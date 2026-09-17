"""Offline synthetic tests only. Never request live models or production data."""
from pathlib import Path
import gzip
import hashlib
import http.client
import io
import json
import tempfile
import unittest
import urllib.error
import urllib.parse

from or_pipeline.collector import (DEFAULT_CONFIG, build_non_catalog_plan, check_config,
                                   collect_catalog, main, model_context, progress_snapshot, request_spec, run_collection)
from or_pipeline.http_capture import CaptureClient, read_captured_body, validate_public_url


class Response(io.BytesIO):
    def __init__(self, body=b'{"data":[]}', status=200, url="https://openrouter.ai/test", headers=None):
        super().__init__(body)
        self.status, self.url = status, url
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(body))}
        self.headers.update(headers or {})

    def geturl(self):
        return self.url

    def getcode(self):
        return self.status


class Clock:
    def __init__(self):
        self.value, self.delays = 0.0, []

    def __call__(self):
        return self.value

    def sleep(self, delay):
        self.delays.append(delay)
        self.value += delay


def config(per_model=None, globals=None):
    return {"schema_version": 1,
            "catalog": {"endpoint": "catalog", "url": "https://openrouter.ai/api/v1/models",
                        "params": {"output_modalities": "all"}, "expected": "json"},
            "http": {"min_interval_seconds": 0, "retries": 0, "workers": 3},
            "global": globals or [],
            "per_model": per_model or [{"endpoint": "model_endpoints", "expected": "json",
                                        "url_template": "https://openrouter.ai/api/v1/models/{model_id_path}/endpoints"}]}


def spec(url="https://openrouter.ai/test"):
    return request_spec({"endpoint": "test", "url": url, "expected": "json"})


class HttpCaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.output = Path(self.temp.name)
        self.clock = Clock()

    def tearDown(self):
        self.temp.cleanup()

    def client(self, transport, retries=0, **options):
        return CaptureClient(self.output, transport=transport, retries=retries, min_interval=0,
                             sleeper=self.clock.sleep, monotonic=self.clock, **options)

    def records(self):
        return [json.loads(x) for x in (self.output / "manifest.jsonl").read_text(encoding="utf8").splitlines()]

    def test_raw_unknown_fields_and_numeric_lexemes_preserved(self):
        body = b'{"data":[],"future":{"decimal":1.000000000000000000001,"escaped":"\\u4e2d"}}'
        result = self.client(lambda *a: Response(body)).capture(spec())
        record = self.records()[0]
        self.assertTrue(result["ok"])
        self.assertEqual(gzip.decompress((self.output / record["path"]).read_bytes()), body)
        self.assertEqual(record["sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(record["bytes"], len(body))

    def test_401_body_retained_no_retry_no_false_success(self):
        result = self.client(lambda *a: Response(b'{"error":"unauthorized"}', 401), retries=2).capture(spec())
        self.assertFalse(result["ok"])
        self.assertEqual(len(self.records()), 1)
        self.assertEqual(self.records()[0]["status"], 401)
        self.assertEqual(read_captured_body(self.output, result["last"]), b'{"error":"unauthorized"}')

    def test_400_not_green(self):
        result = self.client(lambda *a: Response(b'{"error":"invalid selector"}', 400)).capture(spec())
        self.assertFalse(result["ok"])

    def test_HTTPError_body_is_retained(self):
        def transport(*args):
            raise urllib.error.HTTPError(args[0], 404, "Not Found", {"Content-Type": "text/plain"}, io.BytesIO(b"missing"))
        result = self.client(transport).capture(spec())
        self.assertEqual(read_captured_body(self.output, result["last"]), b"missing")
        self.assertFalse(result["ok"])

    def test_retry_keeps_every_body(self):
        responses = iter([Response(b'{"error":"busy"}', 429, headers={"Retry-After": "3"}), Response()])
        result = self.client(lambda *a: next(responses), retries=1).capture(spec())
        self.assertTrue(result["ok"])
        self.assertEqual([r["status"] for r in self.records()], [429, 200])
        self.assertGreaterEqual(sum(self.clock.delays), 3)
        self.assertNotEqual(self.records()[0]["path"], self.records()[1]["path"])

    def test_large_retry_after_pauses_without_silent_truncation(self):
        client = self.client(lambda *a: Response(b'{}', 503, headers={"Retry-After": "3600"}), retries=2, max_retry_after=300)
        first = client.capture(spec())
        second = client.capture(spec("https://openrouter.ai/second"))
        self.assertFalse(first["ok"])
        self.assertFalse(second["attempted"])
        self.assertEqual(len(self.records()), 1)
        self.assertEqual(client.pause_reason["retry_after_seconds"], 3600)

    def test_network_failure_has_empty_original_and_retry(self):
        calls = []
        def transport(*a):
            calls.append(1)
            if len(calls) == 1:
                raise urllib.error.URLError("offline fixture")
            return Response()
        result = self.client(transport, retries=1).capture(spec())
        self.assertTrue(result["ok"])
        self.assertIsNone(self.records()[0]["status"])
        self.assertEqual(self.records()[0]["bytes"], 0)

    def test_content_length_mismatch_not_success(self):
        result = self.client(lambda *a: Response(b'{}', headers={"Content-Length": "100"})).capture(spec())
        self.assertFalse(result["ok"])
        self.assertFalse(result["last"]["body_complete"])
        self.assertEqual(result["last"]["bytes"], 2)

    def test_incomplete_read_keeps_partial_bytes(self):
        class Partial(Response):
            def read(self, n=-1):
                raise http.client.IncompleteRead(b"partial", 20)
        result = self.client(lambda *a: Partial()).capture(spec())
        self.assertFalse(result["ok"])
        self.assertEqual(read_captured_body(self.output, result["last"]), b"partial")

    def test_transport_gzip_is_saved_as_original_encoded_bytes(self):
        encoded = gzip.compress(b'{"data":[],"extra":true}', mtime=0)
        result = self.client(lambda *a: Response(encoded, headers={"Content-Encoding": "gzip"})).capture(spec())
        self.assertTrue(result["ok"])
        self.assertEqual(result["last"]["sha256"], hashlib.sha256(encoded).hexdigest())
        self.assertEqual(gzip.decompress((self.output / result["last"]["path"]).read_bytes()), encoded)

    def test_response_header_whitelist(self):
        result = self.client(lambda *a: Response(headers={"Set-Cookie": "private", "ETag": "abc"})).capture(spec())
        self.assertNotIn("set-cookie", result["last"]["response_headers"])
        self.assertEqual(result["last"]["response_headers"]["etag"], "abc")

    def test_200_application_error_not_success(self):
        result = self.client(lambda *a: Response(b'{"error":{"message":"unavailable"}}')).capture(spec())
        self.assertFalse(result["ok"])

    def test_200_non_json_retained_not_success(self):
        result = self.client(lambda *a: Response(b'<html>blocked</html>')).capture(spec())
        self.assertFalse(result["ok"])

    def test_redirect_bodies_are_separate_attempts(self):
        called = []
        def transport(url, *a):
            called.append(url)
            if len(called) == 1:
                return Response(b"moved", 302, url, {"Location": "/target"})
            return Response(b'{"data":[]}', 200, url)
        result = self.client(transport).capture(spec())
        self.assertTrue(result["ok"])
        self.assertEqual(called, ["https://openrouter.ai/test", "https://openrouter.ai/target"])
        self.assertEqual([r["status"] for r in self.records()], [302, 200])
        self.assertEqual(read_captured_body(self.output, self.records()[0]), b"moved")

    def test_foreign_redirect_is_not_followed(self):
        result = self.client(lambda *a: Response(b"moved", 302, headers={"Location": "https://evil.example/"})).capture(spec())
        self.assertFalse(result["ok"])
        self.assertEqual(len(self.records()), 1)
        self.assertEqual(read_captured_body(self.output, result["last"]), b"moved")

    def test_redirect_cycle_is_bounded(self):
        result = self.client(lambda *a: Response(b"moved", 302, headers={"Location": "/test"})).capture(spec())
        self.assertFalse(result["ok"])
        self.assertEqual(len(self.records()), 1)

    def test_local_or_credential_URL_is_rejected(self):
        for url in ("http://openrouter.ai/x", "https://localhost/x", "https://x@y@openrouter.ai/x", "file:///E:/secret"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                validate_public_url(url)


class CollectorPlanTests(unittest.TestCase):
    def test_progress_separates_request_failures_from_retry_statuses(self):
        outcomes = [
            {"endpoint": "activity", "attempted": True, "ok": False,
             "attempts": [{"status": 401, "body": "must_not_be_logged"}]},
            {"endpoint": "activity", "attempted": True, "ok": True,
             "attempts": [{"status": 503}, {"status": 200}]},
            {"endpoint": "details", "attempted": True, "ok": False,
             "attempts": [{"status": None}]},
            {"endpoint": "details", "attempted": False, "ok": False, "attempts": []},
        ]
        before = json.dumps(outcomes, sort_keys=True)
        snapshot = progress_snapshot(outcomes, 900)
        self.assertEqual(snapshot["completed_requests"], 4)
        self.assertEqual(snapshot["planned_requests"], 900)
        self.assertEqual(snapshot["endpoint_progress"]["activity"], {
            "attempted": 2, "successful": 1, "failed": 1, "not_attempted": 0,
            "HTTP_attempt_status_counts": {"401": 1, "503": 1, "200": 1}})
        self.assertEqual(snapshot["endpoint_progress"]["details"], {
            "attempted": 1, "successful": 0, "failed": 1, "not_attempted": 1,
            "HTTP_attempt_status_counts": {"no_response": 1}})
        self.assertNotIn("must_not_be_logged", json.dumps(snapshot))
        self.assertEqual(json.dumps(outcomes, sort_keys=True), before)

    def test_default_registry_has_no_model_limit(self):
        c = json.loads(DEFAULT_CONFIG.read_text(encoding="utf8"))
        check_config(c)
        models = [{"id": f"fixture/model-{i}"} for i in range(71)]
        plan = build_non_catalog_plan(c, models, True)
        global_count = sum(len(d.get("parameter_sets", [d.get("params", {})])) for d in c["global"])
        self.assertEqual(sum(r["model_id"] is None for r in plan), global_count)
        self.assertEqual(len(plan), global_count + 71 * len(c["per_model"]))
        self.assertEqual(len({r["request_id"] for r in plan}), len(plan))
        self.assertEqual(sum(r["model_id"] is not None for r in plan), 71 * len(c["per_model"]))
        self.assertEqual({r["model_id"] for r in plan if r["model_id"]}, {m["id"] for m in models})

    def test_seven_observed_parameterless_sources_are_independent_requests(self):
        c = json.loads(DEFAULT_CONFIG.read_text(encoding="utf8"))
        expected = {"rankings_apps": "apps", "image_output": "image-output",
                    "rerank_documents": "rerank-documents",
                    "stt_transcript_characters": "stt-transcript-characters",
                    "video_output_hours": "video-output-hours", "session_cost": "session-cost",
                    "task_spend": "task-spend"}
        plan = build_non_catalog_plan(c, [], True)
        for label, suffix in expected.items():
            matches = [r for r in plan if r["endpoint"] == label]
            self.assertEqual(len(matches), 1)
            row = matches[0]
            self.assertEqual(row["url"], "https://openrouter.ai/api/frontend/v1/rankings/" + suffix)
            self.assertEqual(row["params"], {})
            self.assertIsNone(row["model_id"])
            self.assertIn("88b41cf5faa722818186691533fc85e82931db91a475d21082e4e1cc00bf960d", row["endpoint_evidence"])
        self.assertNotIn("modality_models", {r["endpoint"] for r in plan})

    def test_missing_variant_is_only_request_default(self):
        context = model_context({"id": "fixture/model"}, 1)
        self.assertEqual(context["query_variant"], "standard")
        self.assertFalse(context["catalog_variant_present"])
        self.assertIsNone(context["catalog_variant_raw"])
        self.assertFalse(context["query_variant_is_proof_of_native_statistical_variant"])

    def test_explicit_free_id_retained(self):
        context = model_context({"id": "fixture/model:free", "canonical_slug": "fixture/model-2026"}, 1)
        self.assertEqual(context["query_variant"], "free")
        self.assertEqual(context["query_permaslug"], "fixture/model-2026")

    def test_wrong_catalog_scope_rejected(self):
        c = config(); c["catalog"]["params"]["output_modalities"] = "text"
        with self.assertRaises(ValueError):
            check_config(c)

    def test_model_limit_setting_rejected(self):
        c = config(); c["max_models"] = 20
        with self.assertRaises(ValueError):
            check_config(c)

    def test_sampling_CLI_is_not_exposed(self):
        with self.assertRaises(SystemExit):
            main(["--output", "unused", "--max-models", "20"])

    def test_non_ascii_and_reserved_selector_is_encoded_once(self):
        r = request_spec({"endpoint": "language", "url": "https://openrouter.ai/test"}, params={"tag": "C++"})
        self.assertEqual(urllib.parse.parse_qs(urllib.parse.urlsplit(r["url"]).query), {"tag": ["C++"]})


class CatalogAndRunTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.output = Path(self.temp.name) / "run"

    def tearDown(self):
        self.temp.cleanup()

    def client(self, transport):
        return CaptureClient(self.output, transport=transport, retries=0, min_interval=0)

    def test_catalog_all_pages_and_fields_retained(self):
        urls = []
        def transport(url, *a):
            urls.append(url)
            data = {"data": [{"id": "fixture/one", "future": {"x": True}}], "total_count": 2,
                    "links": {"next": "?offset=1&limit=1"}} if len(urls) == 1 else {
                        "data": [{"id": "fixture/two:free"}], "total_count": 2, "links": {"next": None}}
            return Response(json.dumps(data).encode(), url=url)
        cat, plan, _ = collect_catalog(config(), self.client(transport), self.output)
        self.assertTrue(cat["verified"])
        self.assertEqual(len(cat["models"]), 2)
        self.assertTrue(cat["models"][0]["future"]["x"])
        self.assertTrue(all("output_modalities=all" in u for u in urls))
        self.assertEqual(len(plan), 2)

    def test_duplicate_catalog_ids_refuse_completion(self):
        body = json.dumps({"data": [{"id": "fixture/a"}, {"id": "fixture/a"}]}).encode()
        cat, _, _ = collect_catalog(config(), self.client(lambda *a: Response(body)), self.output)
        self.assertFalse(cat["verified"])

    def test_total_count_mismatch_refuse_completion(self):
        body = json.dumps({"data": [{"id": "fixture/a"}], "total_count": 2}).encode()
        cat, _, _ = collect_catalog(config(), self.client(lambda *a: Response(body)), self.output)
        self.assertFalse(cat["verified"])

    def test_scope_change_refuse_completion(self):
        body = json.dumps({"data": [{"id": "fixture/a"}], "links": {"next": "?output_modalities=text"}}).encode()
        cat, _, _ = collect_catalog(config(), self.client(lambda *a: Response(body)), self.output)
        self.assertFalse(cat["verified"])

    def test_cycle_refuse_completion(self):
        body = json.dumps({"data": [{"id": "fixture/a"}], "links": {"next": "?output_modalities=all"}}).encode()
        cat, _, _ = collect_catalog(config(), self.client(lambda *a: Response(body)), self.output)
        self.assertFalse(cat["verified"])

    def test_has_more_without_next_refuse_completion(self):
        body = json.dumps({"data": [{"id": "fixture/a"}], "has_more": True}).encode()
        cat, _, _ = collect_catalog(config(), self.client(lambda *a: Response(body)), self.output)
        self.assertFalse(cat["verified"])

    def test_empty_catalog_refuse_completion(self):
        cat, _, _ = collect_catalog(config(), self.client(lambda *a: Response()), self.output)
        self.assertFalse(cat["verified"])

    def test_foreign_next_refuse_completion(self):
        body = json.dumps({"data": [{"id": "fixture/a"}], "links": {"next": "https://other.example/models"}}).encode()
        cat, _, _ = collect_catalog(config(), self.client(lambda *a: Response(body)), self.output)
        self.assertFalse(cat["verified"])

    def run_fixture(self, fail_model=None, count=43):
        models = [{"id": f"fixture/m{i}"} for i in range(count)]
        def transport(url, *a):
            if urllib.parse.urlsplit(url).path == "/api/v1/models":
                return Response(json.dumps({"data": models, "total_count": count}).encode(), url=url)
            status = 401 if fail_model and f"/{fail_model}/" in url else 200
            return Response(b'{"data":{"endpoints":[]}}', status, url)
        factory = lambda out, **kw: CaptureClient(out, transport=transport, **kw)
        code = run_collection(self.output, config=config(), client_factory=factory)
        return code, json.loads((self.output / "quality.json").read_text()), models

    def test_whole_synthetic_catalog_no_twenty_thirty_sampling(self):
        code, q, models = self.run_fixture()
        self.assertEqual(code, 0)
        self.assertTrue(q["all_catalog_models_planned"])
        self.assertEqual(q["models_with_all_requests_attempted"], len(models))
        self.assertEqual(q["models_with_all_requests_successful"], len(models))
        self.assertEqual(q["planned_requests"], len(models) + 1)
        self.assertEqual(set(q["model_coverage"]), {m["id"] for m in models})
        for name in ("run.json", "catalog.json", "plan.json", "manifest.jsonl", "quality.json"):
            self.assertTrue((self.output / name).is_file())

    def test_failure_keeps_full_plan_and_other_models(self):
        code, q, models = self.run_fixture(fail_model="m3")
        self.assertEqual(code, 2)
        self.assertEqual(q["failed_requests"], 1)
        self.assertEqual(q["models_with_all_requests_attempted"], len(models))
        self.assertEqual(q["not_attempted_requests"], 0)
        self.assertEqual(q["errors"][0]["status"], 401)

    def test_existing_output_not_overwritten(self):
        self.output.mkdir()
        (self.output / "existing").write_text("keep")
        with self.assertRaises(FileExistsError):
            run_collection(self.output, config=config())
        self.assertEqual((self.output / "existing").read_text(), "keep")

    def test_invalid_config_still_has_failure_package(self):
        c = config(); c["max_models"] = 1
        code = run_collection(self.output, config=c)
        self.assertEqual(code, 1)
        for name in ("run.json", "catalog.json", "plan.json", "manifest.jsonl", "quality.json"):
            self.assertTrue((self.output / name).is_file())
        self.assertEqual(json.loads((self.output / "run.json").read_text())["status"], "failed")


if __name__ == "__main__":
    unittest.main()
