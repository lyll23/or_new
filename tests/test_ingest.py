"""Offline synthetic regression tests; never reads a research baseline or network."""
from contextlib import closing, redirect_stdout, redirect_stderr
from decimal import Decimal
import gzip
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zlib

from or_pipeline import ingest as I


def daily(**overrides):
    return {"model_permaslug": "author/model", "date": "2026-08-15T00:00:00", "variant": "standard",
            "count": 5, "total_prompt_tokens": 900719925474099312345, "total_completion_tokens": 2, **overrides}


def event(endpoint="model_activity", **extra):
    return {"request_id": "req1", "attempt": 1, "endpoint": endpoint, "params": {},
            "url": "https://openrouter.ai/api/example", "model_id": "catalog/request-model",
            "status": 200, "ok": True, "body_complete": True, "response_validation": {"valid": True},
            "response_headers": {}, "fetched_at": "2026-08-18T12:00:00Z", "error": None, **extra}


def batch(root, rid="run1", entries=None):
    directory = Path(root) / rid
    (directory / "raw").mkdir(parents=True)
    for name, value in (("run.json", {"run_id": rid, "started_at_utc": "2026-08-18T11:59:00Z"}),
                        ("catalog.json", {}), ("plan.json", {}), ("quality.json", {"status": "provisional"})):
        (directory / name).write_text(I.canonical(value), encoding="utf-8")
    manifest = []
    for n, (e, payload) in enumerate(entries or [(event(), {"data": {"analytics": [daily()], "cachedAt": 1786968000000}})]):
        e = dict(e)
        if payload is not None:
            body = payload if isinstance(payload, bytes) else I.canonical(payload).encode("utf-8")
            path = f"raw/{n}.gz"
            (directory / path).write_bytes(gzip.compress(body, mtime=0))
            e.update(path=path, sha256=hashlib.sha256(body).hexdigest(), bytes=len(body))
        else:
            e.update(path=None, sha256=None, bytes=0)
        manifest.append(I.canonical(e))
    (directory / "manifest.jsonl").write_text("\n".join(manifest) + "\n", encoding="utf-8")
    return directory


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / "observations.sqlite"

    def tearDown(self):
        self.tmp.cleanup()

    def query(self, sql):
        with closing(sqlite3.connect(self.db)) as db:
            return db.execute(sql).fetchall()

    def test_arbitrary_precision_roundtrip(self):
        s = '{"huge":900719925474099312345,"decimal":0.000000000000000000000000000123456789,"zero":0}'
        value = I.loads(s)
        self.assertEqual(I.loads(I.canonical(value)), value)
        self.assertIn("0.000000000000000000000000000123456789", format(value["decimal"], "f"))

    def test_float_dependency_rejected(self):
        with self.assertRaises(I.IngestError):
            I.canonical({"wrong": 0.1})

    def test_duplicate_JSON_key_rejected(self):
        with self.assertRaises(I.IngestError):
            I.loads('{"a":1,"a":2}')

    def test_nonfinite_rejected(self):
        for raw in ("NaN", "Infinity", "-Infinity"):
            with self.assertRaises(I.IngestError):
                I.loads(raw)

    def test_native_missing_not_request_default(self):
        r = daily()
        del r["variant"]
        del r["count"]
        c = I.daily_candidate(r, "/data/analytics/0", {"params": {"variant": "standard"}})
        self.assertEqual(c["variant_state"], "absent")
        self.assertIsNone(c["variant_raw"])
        self.assertIsNone(c["fields"]["count"])
        self.assertEqual(c["raw_record"], r)

    def test_null_empty_zero_different(self):
        self.assertEqual([I.state({}, "a"), I.state({"a": None}, "a"), I.state({"a": ""}, "a"), I.state({"a": 0}, "a")],
                         ["absent", "null", "empty_string", "value"])
        self.assertEqual(I.number(0), "0")
        self.assertIsNone(I.number(False))

    def test_date_no_calendar_shift(self):
        self.assertEqual(I.date_label("2026-08-16T00:00:00+08:00"), "2026-08-16")
        self.assertIsNone(I.date_label("2026-08-16garbage"))
        self.assertIsNone(I.date_label("2026-02-30"))

    def test_full_window_not_two_days(self):
        raw = [daily(date=f"2026-07-{i:02d}T00:00:00") for i in range(1, 32)]
        candidates, issues = I.extract_json({"data": {"analytics": raw}}, event())
        self.assertEqual(len(candidates), 31)
        self.assertEqual(candidates[0]["business_date"], "2026-07-01")

    def test_wrong_rankings_source_not_daily(self):
        cs, _ = I.extract_json({"data": [daily()]}, event("rankings_models", params={"view": "day"}))
        self.assertEqual(cs[0]["table_name"], "rankings_history")
        self.assertIsNone(cs[0]["fields"]["native_rank_raw"])

    def test_ranking_windows_separate(self):
        a, _ = I.extract_json({"data": [daily()]}, event("rankings_models", params={"view": "day"}))
        b, _ = I.extract_json({"data": [daily()]}, event("rankings_models", params={"view": "month"}))
        self.assertNotEqual(a[0]["logical_key"], b[0]["logical_key"])

    def test_variant_case_and_unknown_separate(self):
        a = I.daily_candidate(daily(variant=None), "/x", {})
        b = I.daily_candidate(daily(variant="standard"), "/x", {})
        c = I.daily_candidate(daily(variant="Standard"), "/x", {})
        self.assertEqual(len({a["logical_key"], b["logical_key"], c["logical_key"]}), 3)

    def test_cachedAt_never_borrowed(self):
        cs, _ = I.extract_json({"cachedAt": 1786968000000, "data": {"analytics": [daily()]}}, event())
        self.assertIn("same_owner_cache_missing_or_unconfirmed", cs[0]["quality_flags"])

    def test_cache_milliseconds_not_numeric_string_guess(self):
        c = I.daily_candidate(daily(), "/x", {}, {"cachedAt": "1786968000000"})
        self.assertEqual(c["evidence"]["same_owner_cache"]["unit_status"], "unconfirmed")

    def test_whole_vector_negative_and_Others(self):
        cs, _ = I.extract_json({"data": [{"x": "2026-08-16", "ys": {"Other": 2, "author/model": -1}}]}, event("audio", sha256="sha"))
        self.assertEqual(cs[0]["fields"]["entity_raw"], "Other")
        self.assertEqual(cs[0]["fields"]["entity_key"], "Others")
        self.assertEqual(cs[0]["fields"]["vector_id"], cs[1]["fields"]["vector_id"])
        self.assertTrue(all("whole_vector_invalid_numeric" in c["quality_flags"] for c in cs))

    def test_chart_metric_and_filter_separate(self):
        p = {"data": [{"x": "2026-08-16", "ys": {"m": Decimal("1.1")}}]}
        a, _ = I.extract_json(p, event("categories", params={"category": "Programming"}))
        b, _ = I.extract_json(p, event("categories", params={"category": "programming"}))
        c, _ = I.extract_json(p, event("tools", params={"category": "Programming"}))
        self.assertEqual(len({a[0]["logical_key"], b[0]["logical_key"], c[0]["logical_key"]}), 3)

    def test_chart_cache_change_is_revision_same_key(self):
        a, _ = I.extract_json({"data": {"cachedAt": 1, "data": [{"x": "2026-08-16", "ys": {"m": 1}}]}}, event("leaderboard"))
        b, _ = I.extract_json({"data": {"cachedAt": 2, "data": [{"x": "2026-08-16", "ys": {"m": 2}}]}}, event("leaderboard"))
        self.assertEqual(a[0]["logical_key"], b[0]["logical_key"])

    def test_forecast_config_not_point(self):
        cs, _ = I.extract_json({"data": {"forecast": True, "data": [{"x": "2026-08-16", "ys": {"m": 1}}]}}, event("leaderboard"))
        self.assertNotIn("explicit_forecast_point", cs[0]["quality_flags"])

    def test_explicit_forecast_retained(self):
        cs, _ = I.extract_json({"data": [{"x": "2026-08-16", "ys": {"m": 1}, "isForecast": True}]}, event("tools"))
        self.assertIn("explicit_forecast_point", cs[0]["quality_flags"])

    def test_catalog_nested_flat_null_priority(self):
        r = {"id": "a/b:free", "modality": None, "architecture": {"modality": "text", "tokenizer": "T"},
             "pricing": {"prompt": "0.000000123"}, "top_provider": {"context_length": 1000}, "hugging_face_id": None}
        cs, _ = I.extract_json({"data": [r]}, event("catalog"))
        c = cs[0]
        self.assertIsNone(c["fields"]["arch_modality"])
        self.assertEqual(c["fields"]["tokenizer"], "T")
        self.assertIsNone(c["fields"]["context_length_raw"])
        self.assertEqual(c["fields"]["top_provider_context_length_raw"], "1000")
        self.assertIsNone(c["variant_raw"])
        self.assertEqual(c["raw_record"], r)

    def test_endpoint_parent_not_promoted(self):
        cs, _ = I.extract_json({"data": {"id": "model/parent", "endpoints": [{"id": "uuid", "pricing": {"prompt": "1"}}]}}, event("model_endpoints", params={"variant": "standard"}))
        self.assertEqual(cs[0]["endpoint_id_raw"], "uuid")
        self.assertIsNone(cs[0]["model_id"])
        self.assertIsNone(cs[0]["variant_raw"])

    def test_benchmark_cost_not_score(self):
        cs, _ = I.extract_json({"data": {"weightedInputPrices": ["author/model"], "aaData": {"intelligence": [{"permaslug": "a/b", "score": Decimal("23.01")}]}}}, event("benchmarks"))
        cost = next(c for c in cs if c["fields"]["metric_kind"] == "ordered_cost_list_ordinal")
        self.assertIsNone(cost["fields"]["benchmark_score"])
        self.assertEqual(cost["raw_record"], "author/model")

    def test_performance_not_daily_or_rank(self):
        cs, _ = I.extract_json({"data": [{"permaslug": "a/b", "p50_latency": Decimal("0.000000000001"), "request_count": 12}]}, event("fastest_models"))
        self.assertIsNone(cs[0]["business_date"])
        self.assertIsNone(cs[0]["fields"]["native_rank_raw"])

    def test_unknown_schema_and_empty_not_zero(self):
        cs, issues = I.extract_json({"data": {"analytics": []}}, event())
        self.assertFalse(cs)
        self.assertIn("not_platform_zero", issues[0])
        cs, issues = I.extract_json({"data": [{"tokens": 3}]}, event("modality_chart"))
        self.assertFalse(cs)
        self.assertIn("unsupported", issues[0])

    def test_ingest_exact_numeric_and_idempotence(self):
        p = batch(self.root)
        result = I.ingest(p, self.db)
        self.assertFalse(result["quality_approved"])
        self.assertEqual(result["counts"]["new_candidates"], 1)
        again = I.ingest(p, self.db)
        self.assertEqual(again["counts"]["new_candidates"], 0)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(1,)])
        self.assertEqual(self.query("SELECT total_prompt_tokens,typeof(total_prompt_tokens) FROM v_table1_daily"), [("900719925474099312345", "text")])
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_increment"), [(1,)])

    def test_body_identical_new_capture_link(self):
        I.ingest(batch(self.root, "a"), self.db)
        I.ingest(batch(self.root, "b"), self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(1,)])
        self.assertEqual(self.query("SELECT count(*) FROM event_candidates"), [(2,)])

    def test_revised_daily_held_not_latest(self):
        I.ingest(batch(self.root, "a"), self.db)
        I.ingest(batch(self.root, "b", [(event(), {"data": {"analytics": [daily(count=6)], "cachedAt": 1786968000001}})]), self.db)
        self.assertEqual(self.query("SELECT state_versions FROM v_revisions"), [(2,)])
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_increment"), [(0,)])
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(2,)])

    def test_today_kept_not_completed(self):
        p = batch(self.root, entries=[(event(fetched_at="2026-08-15T12:00:00Z"), {"data": {"analytics": [daily()], "cachedAt": 1786776000000}})])
        I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(1,)])
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_increment"), [(0,)])

    def test_HTTP_encodings(self):
        payload = I.canonical({"data": {"analytics": [daily()], "cachedAt": 1786968000000}}).encode()
        for enc, body in (("gzip", gzip.compress(payload)), ("deflate", zlib.compress(payload))):
            with self.subTest(enc=enc):
                p = batch(self.root, enc, [(event(response_headers={"content-encoding": enc}), body)])
                I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(2,)])

    def test_bad_hash_rolls_back(self):
        p = batch(self.root)
        path = p / "raw/0.gz"
        path.write_bytes(gzip.compress(b"{}"))
        with self.assertRaises(I.IngestError):
            I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM batches"), [(0,)])

    def test_truncated_gzip_rejected(self):
        p = batch(self.root)
        path = p / "raw/0.gz"
        path.write_bytes(path.read_bytes()[:-5])
        with self.assertRaises(I.IngestError):
            I.ingest(p, self.db)

    def test_failed_HTTP_ledger_only(self):
        p = batch(self.root, entries=[(event(status=500, ok=False), {"data": {"analytics": [daily()]}})])
        I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(0,)])
        self.assertEqual(self.query("SELECT count(*) FROM events"), [(1,)])

    def test_missing_validation_no_mapping(self):
        e = event()
        del e["response_validation"]
        I.ingest(batch(self.root, entries=[(e, {"data": {"analytics": [daily()]}})]), self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(0,)])

    def test_200_application_error_not_mapped(self):
        p = batch(self.root, entries=[(event(), {"error": {"code": 401}, "data": {"analytics": [daily()]}})])
        I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(0,)])

    def test_foreign_database_unchanged(self):
        with closing(sqlite3.connect(self.db)) as db:
            db.execute("CREATE TABLE sealed(a)")
            db.commit()
        before = self.db.read_bytes()
        with self.assertRaises(I.IngestError):
            I.ingest(batch(self.root), self.db)
        self.assertEqual(self.db.read_bytes(), before)

    def test_path_escape_rejected(self):
        for name in ("../outside.gz", "raw/../../outside.gz", "E:/outside.gz", "raw\\x.gz", "/raw/x.gz"):
            with self.assertRaises(I.IngestError):
                I.safe_raw_path(self.root, name)

    def test_same_run_changed_rejected(self):
        p = batch(self.root)
        I.ingest(p, self.db)
        (p / "quality.json").write_text('{}', encoding="utf-8")
        with self.assertRaises(I.IngestError):
            I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(1,)])

    def test_duplicate_attempt_rejected(self):
        p = batch(self.root, entries=[(event(), {}), (event(), {})])
        with self.assertRaises(I.IngestError):
            I.ingest(p, self.db)

    def test_import_signature_changes_reparse(self):
        p = batch(self.root)
        I.ingest(p, self.db)
        with patch.object(I, "EXTRACTOR_VERSION", "synthetic-next-version"):
            I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(2,)])
        self.assertEqual(self.query("SELECT count(*) FROM extractions"), [(2,)])

    def test_export_is_new_data_only_and_no_overwrite(self):
        I.ingest(batch(self.root), self.db)
        out = self.root / "exports"
        result = I.export_views(self.db, out)
        self.assertEqual(result["table1_daily"]["rows"], 1)
        text = (out / "table1_daily.csv").read_text(encoding="utf-8-sig")
        self.assertIn("900719925474099312345", text)
        with self.assertRaises(I.IngestError):
            I.export_views(self.db, out)

    def test_cli(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = I.main(["--batch", str(batch(self.root)), "--database", str(self.db)])
        self.assertEqual(status, 0, stderr.getvalue())
        self.assertEqual(I.loads(stdout.getvalue())["status"], "ingestion_complete_candidates_provisional")

    def test_model_HTML_bound_evidence_is_preserved(self):
        from test_native_html import raw, query, html
        r = raw()
        r["exact_extra_decimal"] = Decimal("0.123456789012345678901234567890123456789")
        frames = '1:' + I.canonical(query({"model_chart": [r], "cachedAt": 1789516800000})) + '\n'
        p = batch(self.root, entries=[(event("model_page", fetched_at="2026-09-17T12:00:00Z",
                    response_headers={"content-type": "text/html"}), html(frames).encode("utf-8"))])
        result = I.ingest(p, self.db)
        self.assertEqual(result["counts"]["mapped_candidates"], 1)
        record, evidence = self.query("SELECT raw_record_json,evidence_json FROM candidates")[0]
        self.assertEqual(I.loads(record), r)
        ev = I.loads(evidence)
        self.assertEqual(ev["same_owner_cache"]["raw"], 1789516800000)
        self.assertEqual(ev["native_extractor_evidence"]["query_identity_resolved"]["variant"], "free")
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_increment"), [(1,)])

    def test_direct_Flight_RSC(self):
        from test_native_html import raw, query
        frames = '1:' + I.canonical(query({"model_chart": [raw()], "cachedAt": 1789516800000})) + '\n'
        cs, issues = I.extract_body(frames.encode(), event("model_page", content_type="text/x-component"))
        self.assertEqual(len(cs), 1)
        self.assertFalse(issues)

    def test_database_inside_batch_rejected(self):
        p = batch(self.root)
        with self.assertRaises(I.IngestError):
            I.ingest(p, p / "observations.sqlite")
        self.assertFalse((p / "observations.sqlite").exists())

    def test_cache_future_after_response_held(self):
        p = batch(self.root, entries=[(event(fetched_at="2026-08-16T01:00:00Z"),
             {"data": {"analytics": [daily()], "cachedAt": 1786968000000}})])
        I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_increment"), [(0,)])
        flags = I.loads(self.query("SELECT quality_flags_json FROM event_candidate_qualifications")[0][0])
        self.assertIn("cache_after_response_received", flags)

    def test_database_identity_no_create_stable_instance(self):
        self.assertIsNone(I.database_identity(self.db))
        self.assertFalse(self.db.exists())
        p = batch(self.root)
        result = I.ingest(p, self.db)
        original = I.database_identity(self.db)
        self.assertEqual(result["database_instance_id"], original)
        self.assertEqual(I.ingest(p, self.db)["database_instance_id"], original)
        second = self.root / "different.sqlite"
        self.assertNotEqual(I.ingest(p, second)["database_instance_id"], original)

    def test_database_identity_foreign_rejected(self):
        with closing(sqlite3.connect(self.db)) as db:
            db.execute("CREATE TABLE foreign_data(x)")
        before = self.db.read_bytes()
        with self.assertRaises(I.IngestError):
            I.database_identity(self.db)
        self.assertEqual(self.db.read_bytes(), before)

    def test_provider_directory_objects_not_one_key(self):
        cs, _ = I.extract_json({"data": [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}]}, event("providers"))
        self.assertNotEqual(cs[0]["logical_key"], cs[1]["logical_key"])

    def test_endpoint_stats_objects_keep_explicit_id_context(self):
        cs, _ = I.extract_json({"data": [{"id": "endpoint-a", "p50_latency": 1}, {"id": "endpoint-b", "p50_latency": 2}]}, event("endpoint_stats"))
        self.assertNotEqual(cs[0]["logical_key"], cs[1]["logical_key"])

    def test_shared_body_qualification_is_per_response_both_orders(self):
        payload = {"data": {"analytics": [daily()], "cachedAt": 1786968000000}}
        for n, order in enumerate((("later", "earlier"), ("earlier", "later"))):
            target = self.root / (str(n) + ".sqlite")
            for label in order:
                fetched = "2026-08-18T12:00:00Z" if label == "later" else "2026-08-15T12:00:00Z"
                I.ingest(batch(self.root, f"{n}-{label}", [(event(fetched_at=fetched), payload)]), target)
            with closing(sqlite3.connect(target)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM candidates").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT run_id FROM v_table1_daily_screened").fetchall(), [(f"{n}-later",)])
                self.assertEqual(db.execute("SELECT run_id FROM v_table1_daily_increment").fetchall(), [(f"{n}-later",)])
                self.assertEqual(db.execute("SELECT count(*) FROM event_candidate_qualifications").fetchone()[0], 2)

    def test_upgrade_keeps_versions_but_one_current_increment(self):
        p = batch(self.root)
        I.ingest(p, self.db)
        with patch.object(I, "EXTRACTOR_VERSION", "synthetic-version-v2"):
            result = I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(2,)])
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_increment"), [(1,)])
        self.assertEqual(self.query("SELECT extractor_version FROM v_table1_daily_increment"), [(result["extractor_version"],)])

    def test_API_bad_variant_types_retained_not_screened(self):
        values = [True, ["standard"], {"variant": "free"}, 1, "", "   ", None]
        rows = [daily(variant=v) for v in values]
        p = batch(self.root, entries=[(event(), {"data": {"analytics": rows, "cachedAt": 1786968000000}})])
        I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(len(values),)])
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_screened"), [(0,)])
        saved = [I.loads(x[0])["variant"] for x in self.query("SELECT raw_record_json FROM candidates")]
        self.assertEqual([I.canonical(v) for v in saved], [I.canonical(v) for v in values])

    def test_API_intraday_and_submicrosecond_not_screened(self):
        labels = ["2026-08-15T12:30:00", "2026-08-15T00:00:01", "2026-08-15T00:00:00.0000001Z"]
        p = batch(self.root, entries=[(event(), {"data": {"analytics": [daily(date=d) for d in labels], "cachedAt": 1786968000000}})])
        I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM candidates"), [(3,)])
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_screened"), [(0,)])

    def test_API_date_only_and_midnight_fraction_zero_allowed(self):
        labels = ["2026-08-15", "2026-08-15T00:00:00", "2026-08-15T00:00:00.0000000Z"]
        p = batch(self.root, entries=[(event(), {"data": {"analytics": [daily(date=d) for d in labels], "cachedAt": 1786968000000}})])
        I.ingest(p, self.db)
        self.assertEqual(self.query("SELECT count(*) FROM v_table1_daily_screened"), [(3,)])

    def test_raw_candidate_flags_do_not_include_response_clock(self):
        p = batch(self.root, entries=[(event(fetched_at="2026-08-15T12:00:00Z"),
            {"data": {"analytics": [daily()], "cachedAt": 1786968000000}})])
        I.ingest(p, self.db)
        native = I.loads(self.query("SELECT quality_flags_json FROM candidates")[0][0])
        per_event = I.loads(self.query("SELECT quality_flags_json FROM event_candidate_qualifications")[0][0])
        self.assertNotIn("response_received_before_UTC_day_end", native)
        self.assertIn("response_received_before_UTC_day_end", per_event)


if __name__ == "__main__":
    unittest.main()
