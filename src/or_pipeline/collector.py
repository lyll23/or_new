"""Complete current-catalog capture, with immutable per-attempt response bytes.

Run: python -m or_pipeline.collector --output RUN_DIRECTORY
No model sampling or model-count limit exists in this interface.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import sys
import traceback
import urllib.parse
import uuid

from .http_capture import CaptureClient, read_captured_body, utc_now, validate_public_url
from .configuration import config_file

VERSION = "1.0.0"
DEFAULT_CONFIG = config_file("endpoints.json")
CATALOG_URL = "https://openrouter.ai/api/v1/models"


def save_json(path: Path, value) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def digest_json(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def request_spec(endpoint: dict, *, model_id=None, params=None, context=None, url=None) -> dict:
    address = url or endpoint["url"]
    parameters = dict(params if params is not None else endpoint.get("params", {}))
    parsed = urllib.parse.urlsplit(address)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    if parameters:
        existing = {k for k, _ in query}
        if existing.intersection(parameters):
            raise ValueError("Query parameter supplied twice")
        query.extend((k, str(v)) for k, v in parameters.items())
    address = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path,
                                     urllib.parse.urlencode(query), ""))
    validate_public_url(address)
    spec = {"endpoint": endpoint["endpoint"], "url": address,
            "params": dict(query), "model_id": model_id, "request_context": context or {},
            "expected": endpoint.get("expected", "json"),
            "stability": endpoint.get("stability", "unknown"),
            "endpoint_evidence": endpoint.get("evidence", "unknown")}
    spec["request_id"] = digest_json(spec)
    return spec


def model_context(model: dict, ordinal: int) -> dict:
    identity = model["id"]
    base, sep, suffix = identity.partition(":")
    canonical = model.get("canonical_slug")
    permaslug = canonical if isinstance(canonical, str) and canonical else base
    if sep and suffix:
        query_variant, evidence = suffix, "explicit_suffix_of_catalog_id"
    else:
        query_variant, evidence = "standard", "request_default_only_not_native_variant_evidence"
    return {"catalog_model_ordinal": ordinal, "catalog_model_id": identity,
            "catalog_canonical_slug_raw": canonical,
            "catalog_variant_present": "variant" in model,
            "catalog_variant_raw": model.get("variant"),
            "query_permaslug": permaslug,
            "query_permaslug_evidence": "catalog.canonical_slug" if permaslug == canonical else "catalog.id_before_variant_suffix",
            "query_variant": query_variant, "query_variant_evidence": evidence,
            "query_variant_is_proof_of_native_statistical_variant": False}


def build_non_catalog_plan(config: dict, models: list[dict], catalog_verified: bool) -> list[dict]:
    requests = []
    for definition in config["global"]:
        for params in definition.get("parameter_sets", [definition.get("params", {})]):
            requests.append(request_spec(definition, params=params,
                                         context={"scope": "global", "selector_registry_version": config["schema_version"]}))
    if catalog_verified:
        for ordinal, model in enumerate(models, 1):
            context = model_context(model, ordinal)
            values = {"model_id_path": urllib.parse.quote(model["id"], safe="/"),
                      "permaslug": context["query_permaslug"], "query_variant": context["query_variant"]}
            for definition in config["per_model"]:
                address = definition["url_template"].format(**values)
                parameters = {key: value.format(**values) if isinstance(value, str) else value
                              for key, value in definition.get("params", {}).items()}
                requests.append(request_spec(definition, model_id=model["id"], params=parameters,
                                             context=context, url=address))
    ids = [r["request_id"] for r in requests]
    if len(ids) != len(set(ids)):
        raise ValueError("Configuration produces duplicate request identities")
    return requests


def next_catalog_url(current: str, next_value) -> str | None:
    if next_value in (None, ""):
        return None
    if not isinstance(next_value, str):
        raise ValueError("Catalog pagination next link is not a string")
    address = urllib.parse.urljoin(current, next_value)
    validate_public_url(address)
    parts = urllib.parse.urlsplit(address)
    if parts.path.rstrip("/") != "/api/v1/models":
        raise ValueError("Catalog pagination leaves the models resource")
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    modalities = [v for k, v in query if k == "output_modalities"]
    if modalities and modalities != ["all"]:
        raise ValueError("Catalog pagination changes the all-modalities scope")
    if not modalities:
        query.append(("output_modalities", "all"))
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path,
                                   urllib.parse.urlencode(query), ""))


def collect_catalog(config: dict, client: CaptureClient, output: Path):
    definition = config["catalog"]
    current = request_spec(definition)["url"]
    visited = set()
    model_ids = set()
    models, pages, requests, outcomes, errors = [], [], [], [], []
    declared_totals = set()
    terminal = False
    while current is not None:
        if current in visited:
            errors.append("catalog_pagination_cycle")
            break
        visited.add(current)
        spec = request_spec(dict(definition, params={}), url=current, params={},
                            context={"scope": "catalog", "catalog_page_ordinal": len(pages) + 1,
                                     "output_modalities": "all"})
        requests.append(spec)
        outcome = client.capture(spec)
        outcomes.append(outcome)
        last = outcome["last"]
        page = {"page_ordinal": len(pages) + 1, "request_id": spec["request_id"], "url": spec["url"],
                "ok": outcome["ok"], "source_path": last.get("path") if last else None,
                "source_sha256": last.get("sha256") if last else None,
                "fetched_at": last.get("fetched_at") if last else None}
        pages.append(page)
        if not outcome["ok"]:
            errors.append("catalog_HTTP_or_payload_failed")
            break
        try:
            payload = json.loads(read_captured_body(output, last).decode("utf-8-sig"))
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise ValueError("Catalog must contain a data array")
            data = payload["data"]
            page["raw_model_count"] = len(data)
            for item in data:
                if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
                    raise ValueError("Catalog model entry lacks a literal string id")
                if item["id"] in model_ids:
                    raise ValueError("Duplicate model id across the catalog; no silent deduplication")
                model_ids.add(item["id"])
                models.append(item)
            total = payload.get("total_count")
            if total is not None:
                if not isinstance(total, int) or isinstance(total, bool) or total < 0:
                    raise ValueError("Invalid catalog total_count")
                declared_totals.add(total)
            page["total_count_raw"] = total
            links = payload.get("links")
            if links is not None and not isinstance(links, dict):
                raise ValueError("Unknown catalog links structure")
            next_value = (links or {}).get("next")
            page["next_raw"] = next_value
            page["has_more_raw"] = payload.get("has_more")
            if payload.get("has_more") is True and not next_value:
                raise ValueError("Catalog declares more pages without a usable next link")
            current = next_catalog_url(current, next_value)
            if current is None:
                terminal = True
            elif not data:
                raise ValueError("Empty nonterminal catalog page")
        except (ValueError, TypeError, KeyError, UnicodeError, OSError) as exc:
            page["catalog_validation_error"] = repr(exc)
            errors.append(repr(exc))
            break
    if not models:
        errors.append("empty_catalog_not_accepted_as_complete_current_universe")
    if len(declared_totals) > 1:
        errors.append("catalog_total_count_changed_across_pages")
    if declared_totals and declared_totals != {len(models)}:
        errors.append("catalog_count_does_not_match_total_count")
    verified = bool(terminal and models and not errors)
    catalog = {"schema_version": 1, "verified": verified, "output_modalities": "all",
               "authenticated": False, "models": models, "model_count": len(models),
               "pages": pages, "catalog_errors": errors, "pagination_terminal_reached": terminal,
               "declared_total_counts": sorted(declared_totals),
               "completeness_evidence": "all_returned_pages_followed_and_declared_totals_checked" if verified else "incomplete_or_unverified",
               "model_list_is_a_request_time_public_scope_not_all_historical_models": True,
               "raw_numeric_lexemes_preserved_in_page_source_files": True,
               "catalog_started_at_utc": outcomes[0]["attempts"][0]["started_at"] if outcomes and outcomes[0]["attempts"] else None,
               "catalog_finished_at_utc": utc_now()}
    return catalog, requests, outcomes


def check_config(config):
    if any(key in config for key in ("max_models", "model_limit", "limit", "sample_models")):
        raise ValueError("A model sampling or truncation setting is not permitted")
    if config.get("schema_version") != 1:
        raise ValueError("Unknown endpoint configuration schema")
    c = config.get("catalog", {})
    if c.get("url") != CATALOG_URL or c.get("params") != {"output_modalities": "all"}:
        raise ValueError("A complete all-output-modalities catalog is mandatory")
    if not isinstance(config.get("per_model"), list) or not config["per_model"]:
        raise ValueError("No per-model endpoint plan configured")
    if not isinstance(config.get("global"), list):
        raise ValueError("Missing global endpoint registry")
    labels = [d["endpoint"] for d in config["per_model"]]
    if len(labels) != len(set(labels)):
        raise ValueError("Duplicate per-model endpoint label")


def quality_report(catalog, plan, outcomes, client):
    by_id = {o["request_id"]: o for o in outcomes}
    if len(by_id) != len(outcomes):
        raise ValueError("A planned request has multiple unmerged outcomes")
    expected = {r["request_id"] for r in plan["requests"]}
    if not set(by_id).issubset(expected):
        raise ValueError("Unplanned request outcome")
    grouped = {}
    models = {m["id"]: {"planned": 0, "attempted": 0, "successful": 0, "failed": 0,
                            "not_attempted": 0} for m in catalog["models"]}
    errors = []
    attempted = success = not_attempted = attempts = failed_attempts = 0
    for r in plan["requests"]:
        o = by_id.get(r["request_id"])
        group = grouped.setdefault(r["endpoint"], {"planned": 0, "attempted": 0, "successful": 0,
                                                    "failed": 0, "not_attempted": 0,
                                                    "stability": r.get("stability", "unknown")})
        counters = [group]
        if r.get("model_id") is not None:
            counters.append(models[r["model_id"]])
        for counter in counters:
            counter["planned"] += 1
        if o and o["attempted"]:
            attempted += 1
            attempts += len(o["attempts"])
            failed_attempts += sum(not a["ok"] for a in o["attempts"])
            key = "successful" if o["ok"] else "failed"
            success += int(o["ok"])
            for counter in counters:
                counter["attempted"] += 1
                counter[key] += 1
        else:
            not_attempted += 1
            for counter in counters:
                counter["not_attempted"] += 1
        if not o or not o["ok"]:
            last = o.get("last") if o else None
            errors.append({"request_id": r["request_id"], "endpoint": r["endpoint"],
                           "model_id": r.get("model_id"), "url": r["url"],
                           "status": last.get("status") if last else None,
                           "error": last.get("error") if last else (o or {}).get("not_attempted_reason", "request_not_executed"),
                           "attempted": bool(o and o["attempted"]), "raw_path": last.get("path") if last else None})
    all_models_planned = bool(catalog["verified"] and all(v["planned"] == plan["per_model_endpoint_count"] for v in models.values()))
    complete = bool(all_models_planned and not errors and catalog["verified"])
    return {"schema_version": 1, "status": "complete" if complete else "partial",
            "catalog_verified": catalog["verified"], "catalog_errors": catalog["catalog_errors"],
            "catalog_model_count": len(models), "model_count": len(models),
            "all_catalog_models_planned": all_models_planned,
            "model_plan_complete": all_models_planned,
            "models_with_any_attempt": sum(v["attempted"] > 0 for v in models.values()),
            "models_with_all_requests_attempted": sum(v["attempted"] == plan["per_model_endpoint_count"] for v in models.values()),
            "models_with_all_requests_successful": sum(v["successful"] == plan["per_model_endpoint_count"] for v in models.values()),
            "planned_requests": len(plan["requests"]), "attempted_requests": attempted,
            "successful_requests": success, "failed_requests": attempted - success,
            "not_attempted_requests": not_attempted, "HTTP_attempt_records": attempts,
            "unsuccessful_attempt_records_including_retries": failed_attempts,
            "endpoint_coverage": grouped, "model_coverage": models, "errors": errors,
            "service_pause": client.pause_reason,
            "HTTP_success_is_not_statistical_daily_window_or_finality_proof": True,
            "no_model_sampling": True, "declared_endpoint_scope_only": True,
            "generated_at_utc": utc_now()}


def progress_snapshot(outcomes, planned_requests):
    """Counts only: completed logical requests and every retained HTTP attempt."""
    grouped = {}
    for outcome in outcomes:
        counters = grouped.setdefault(outcome["endpoint"], {
            "attempted": 0, "successful": 0, "failed": 0, "not_attempted": 0,
            "HTTP_attempt_status_counts": Counter(),
        })
        if outcome["attempted"]:
            counters["attempted"] += 1
            counters["successful" if outcome["ok"] else "failed"] += 1
        else:
            counters["not_attempted"] += 1
        for attempt in outcome["attempts"]:
            status = attempt.get("status")
            counters["HTTP_attempt_status_counts"][str(status) if status is not None else "no_response"] += 1
    return {"stage": "collecting", "completed_requests": len(outcomes),
            "planned_requests": planned_requests, "endpoint_progress": grouped}


def run_collection(output, *, config_path=None, config=None, workers=None, client_factory=CaptureClient):
    output = Path(output)
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Output directory must be fresh; existing evidence is never overwritten")
    output.mkdir(parents=True, exist_ok=True)
    (output / "raw").mkdir()
    (output / "manifest.jsonl").touch()
    started = utc_now()
    run = {"schema_version": 1, "run_id": started.replace(":", "").replace("-", "") + "_" + uuid.uuid4().hex[:12],
           "started_at_utc": started, "finished_at_utc": None, "collector_version": VERSION,
           "status": "initializing", "github_run_id": os.environ.get("GITHUB_RUN_ID"),
           "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
           "no_model_sampling": True, "credentials_sent_to_data_source": False}
    catalog = {"verified": False, "models": [], "model_count": 0, "catalog_errors": ["catalog_not_completed"]}
    plan = {"schema_version": 1, "run_id": run["run_id"], "requests": [], "model_plan_complete": False,
            "per_model_endpoint_count": 0, "no_model_sampling": True}
    for name, value in (("run.json", run), ("catalog.json", catalog), ("plan.json", plan),
                        ("quality.json", {"status": "initializing", "catalog_verified": False})):
        save_json(output / name, value)
    outcomes = []
    try:
        if config is None:
            source = Path(config_path or DEFAULT_CONFIG)
            data = source.read_bytes()
            config = json.loads(data)
            run["config_sha256"] = hashlib.sha256(data).hexdigest()
        else:
            run["config_sha256"] = digest_json(config)
        check_config(config)
        settings = config.get("http", {})
        worker_count = workers if workers is not None else settings.get("workers", 4)
        if not isinstance(worker_count, int) or not 1 <= worker_count <= 8:
            raise ValueError("workers must be between 1 and 8; this controls concurrency, not coverage")
        client = client_factory(output, timeout=settings.get("timeout_seconds", 45),
                                retries=settings.get("retries", 2), min_interval=settings.get("min_interval_seconds", 0.35),
                                max_retry_after=settings.get("max_retry_after_seconds", 300))
        run["status"] = "collecting_catalog"
        save_json(output / "run.json", run)
        catalog, catalog_plan, catalog_outcomes = collect_catalog(config, client, output)
        outcomes.extend(catalog_outcomes)
        catalog["run_id"] = run["run_id"]
        save_json(output / "catalog.json", catalog)
        remaining = build_non_catalog_plan(config, catalog["models"], catalog["verified"])
        plan.update(requests=catalog_plan + remaining, catalog_verified=catalog["verified"],
                    model_plan_complete=catalog["verified"], catalog_model_ids=[m["id"] for m in catalog["models"]],
                    per_model_endpoint_count=len(config["per_model"]),
                    endpoint_registry_scope=config.get("scope"), limitations=config.get("limitations", []),
                    frozen_at_utc=utc_now())
        if len({r["request_id"] for r in plan["requests"]}) != len(plan["requests"]):
            raise ValueError("Duplicate request IDs in frozen plan")
        save_json(output / "plan.json", plan)
        run["status"] = "collecting_complete_plan" if catalog["verified"] else "catalog_invalid_collecting_global_only"
        run["planned_requests"] = len(plan["requests"])
        save_json(output / "run.json", run)
        print(json.dumps({"stage": run["status"], "catalog_models": len(catalog["models"]),
                          "catalog_verified": catalog["verified"], "planned_requests": len(plan["requests"])}), flush=True)
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            pending = {pool.submit(client.capture, request): request for request in remaining}
            for count, future in enumerate(as_completed(pending), 1):
                outcomes.append(future.result())
                if count % 50 == 0 or count == len(remaining):
                    print(json.dumps(progress_snapshot(outcomes, len(plan["requests"]))), flush=True)
        quality = quality_report(catalog, plan, outcomes, client)
        run["status"] = quality["status"]
        code = 0 if quality["status"] == "complete" else 2
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = repr(exc)
        quality = {"schema_version": 1, "status": "failed", "catalog_verified": catalog.get("verified", False),
                   "model_count": len(catalog.get("models", [])), "model_plan_complete": False,
                   "planned_requests": len(plan["requests"]), "recorded_outcomes": len(outcomes),
                   "error": repr(exc), "no_model_sampling": True, "raw_evidence_preserved": True,
                   "generated_at_utc": utc_now()}
        traceback.print_exc()
        code = 1
    run["finished_at_utc"] = utc_now()
    run["exit_code"] = code
    save_json(output / "quality.json", quality)
    save_json(output / "run.json", run)
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--workers", type=int, default=None, help="Concurrent requests (1–8); never a model limit")
    args = parser.parse_args(argv)
    try:
        return run_collection(args.output, config_path=args.config, workers=args.workers)
    except (OSError, ValueError) as exc:
        print(f"Collector could not start: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
