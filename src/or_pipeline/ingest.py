"""Append-only, source-bound observations; never writes a sealed research baseline.

All numeric projections are TEXT. Original JSON number tokens use Decimal instead
of binary floats. A mapped candidate is not a selected, final research observation.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import datetime as dt
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sqlite3
import sys
import uuid
import zlib

EXTRACTOR_VERSION = "or-new-native-observations/1.0.0"
DATABASE_KIND = "or-new-append-only-observations-v1"
APPLICATION_ID = 0x4F524E31
CORE = ("count", "total_prompt_tokens", "total_completion_tokens")
CHARTS = {"leaderboard", "market_share", "tools", "images", "audio", "categories",
          "natural_languages", "programming_languages", "context_length"}
REQUIRED_FILES = ("run.json", "catalog.json", "plan.json", "manifest.jsonl", "quality.json")


class IngestError(ValueError):
    pass


def reject_constant(s):
    raise IngestError("Non-finite JSON number: " + s)


def unique_object(pairs):
    result = {}
    for k, v in pairs:
        if k in result:
            raise IngestError("Duplicate JSON key: " + k)
        result[k] = v
    return result


def loads(text):
    return json.loads(text, parse_float=Decimal, parse_int=int,
                      parse_constant=reject_constant, object_pairs_hook=unique_object)


def canonical(value):
    """Lossless numeric JSON serialization (not Decimal converted to a string)."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if type(value) is int:
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise IngestError("Non-finite Decimal")
        return str(value)
    if isinstance(value, list):
        return "[" + ",".join(canonical(x) for x in value) + "]"
    if isinstance(value, dict):
        if not all(isinstance(k, str) for k in value):
            raise IngestError("Non-string JSON key")
        return "{" + ",".join(canonical(k) + ":" + canonical(value[k]) for k in sorted(value)) + "}"
    # A dependency silently converting numbers to float is a hard error.
    raise IngestError("Unsupported value type: " + type(value).__name__)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def sha_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def stat5(path):
    s = Path(path).stat()
    return {k: getattr(s, k) for k in ("st_size", "st_mtime_ns", "st_ctime_ns", "st_ino", "st_dev")}


def pointer(base, key):
    return base + "/" + str(key).replace("~", "~0").replace("/", "~1")


def state(raw, key):
    if key not in raw:
        return "absent"
    v = raw[key]
    return "null" if v is None else "empty_string" if v == "" else "value"


def number(value):
    if isinstance(value, bool) or value is None or value == "":
        return None
    if type(value) is int or isinstance(value, Decimal):
        return str(value) if not isinstance(value, Decimal) or value.is_finite() else None
    # Numeric strings remain raw strings; projection records original field state.
    if isinstance(value, str):
        try:
            n = Decimal(value)
        except Exception:
            return None
        return value if n.is_finite() else None
    return None


def date_label(value):
    """Preserve the calendar label; do not convert to UTC or shift a week."""
    if not isinstance(value, str):
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            return dt.date.fromisoformat(value).isoformat()
        except ValueError:
            return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?", value):
        try:
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass
    return None


def scalar_text(value):
    return value if isinstance(value, str) else canonical(value) if value is not None else None


def identity(raw, keys):
    found = [(k, raw[k]) for k in keys if isinstance(raw.get(k), str) and raw[k]]
    return found[0] if found else (None, None)


def project_fields(raw, paths):
    """First present path wins, even null. Every origin/state is retained."""
    fields, evidence = {}, {}
    for out, options in paths.items():
        alternatives = []
        for path in options:
            node = raw
            for k in path:
                if not isinstance(node, dict) or k not in node:
                    break
                node = node[k]
            else:
                alternatives.append({"path": list(path), "raw": node})
        if alternatives:
            v = alternatives[0]["raw"]
            fields[out] = scalar_text(v)
            evidence[out] = {"state": "null" if v is None else "empty_string" if v == "" else "value",
                             "source_path": alternatives[0]["path"], "all_present_paths": alternatives}
        else:
            fields[out] = None
            evidence[out] = {"state": "absent", "source_path": None, "all_present_paths": []}
    return fields, evidence


ATTRIBUTE_PATHS = {
    "model_slug": [("slug",)], "canonical_slug_raw": [("canonical_slug",)],
    "hf_slug": [("hugging_face_id",)], "name_raw": [("name",)], "description_raw": [("description",)],
    "group_raw": [("group",)], "created_raw": [("created",)], "created_at_raw": [("created_at",)],
    "context_length_raw": [("context_length",)], "arch_modality": [("modality",), ("architecture", "modality")],
    "tokenizer": [("tokenizer",), ("architecture", "tokenizer")],
    "instruct_type": [("instruct_type",), ("architecture", "instruct_type")],
    "input_modalities_raw": [("input_modalities",), ("architecture", "input_modalities")],
    "output_modalities_raw": [("output_modalities",), ("architecture", "output_modalities")],
    "supported_parameters_raw": [("supported_parameters",)], "pricing_object_raw": [("pricing",)],
    "top_provider_object_raw": [("top_provider",)], "per_request_limits_raw": [("per_request_limits",)],
    "top_provider_context_length_raw": [("top_provider", "context_length")],
    "top_provider_max_completion_tokens_raw": [("top_provider", "max_completion_tokens")],
    "is_moderated_raw": [("is_moderated",), ("top_provider", "is_moderated")],
    "max_prompt_tokens_raw": [("max_prompt_tokens",)], "max_completion_tokens_raw": [("max_completion_tokens",)],
    "quantization_raw": [("quantization",)], "provider_name_raw": [("provider_name",)],
    "provider_info_raw": [("provider_info",)], "data_policy_raw": [("data_policy",), ("dataPolicy",)],
}
for _price in ("prompt", "completion", "request", "image", "internal_reasoning", "input_cache_read", "input_cache_write"):
    ATTRIBUTE_PATHS["price_" + _price + "_raw"] = [("price_" + _price,), ("pricing", _price)]


def make_candidate(table, raw, locator, *, model_keys=(), date_key=None, context=None, fields=None, evidence=None, flags=None):
    obj = raw if isinstance(raw, dict) else {}
    identity_field, model = identity(obj, model_keys)
    variant = obj.get("variant")
    label = obj.get(date_key) if date_key else None
    context = context or {}
    warnings = list(flags or [])
    if model_keys and model is None:
        warnings.append("native_model_identity_missing")
    if state(obj, "variant") != "value":
        warnings.append("native_variant_" + state(obj, "variant"))
    if date_key and date_label(label) is None:
        warnings.append("business_date_label_missing_or_invalid")
    # Retrieval clocks/catalog position are evidence, not separate statistical keys.
    def stable_context(value):
        if isinstance(value, dict):
            return {k: stable_context(v) for k, v in value.items() if k not in {
                "cachedAt", "dataUpdatedAt", "dehydratedAt", "request_context", "fetched_at", "queryHash"}}
        if isinstance(value, list):
            return [stable_context(v) for v in value]
        return value
    key = {"table": table, "identity_namespace": identity_field, "model_id": model,
           "variant_state": state(obj, "variant"), "variant_raw": variant,
           "date_label_raw": label, "context": stable_context(context)}
    return {"table_name": table, "raw_record": raw, "source_locator": locator,
            "model_id": model, "identity_namespace": identity_field,
            "variant_raw": variant, "variant_state": state(obj, "variant"),
            "date_label_raw": label, "business_date": date_label(label),
            "endpoint_id_raw": obj.get("id") if table == "provider_endpoints" else None,
            "context": context, "fields": fields or {}, "evidence": evidence or {},
            "quality_flags": sorted(set(warnings)), "logical_key": digest(key),
            "mapping_status": "provisional"}


def daily_candidate(raw, locator, context, owner=None, external_evidence=None):
    owner = owner if isinstance(owner, dict) else {}
    fields, ev = project_fields(raw, {k: [(k,)] for k in (*CORE, "total_native_tokens_reasoning",
        "total_native_tokens_cached", "num_media_prompt", "num_media_completion", "num_audio_prompt",
        "total_tool_calls", "requests_with_tool_call_errors")})
    flags = ["business_day_timezone_unconfirmed", "statistics_finality_unverified",
             "count_success_only_unverified", "media_units_unverified", "tool_error_denominator_not_established"]
    if not isinstance(raw.get("variant"), str) or not raw["variant"].strip():
        flags.append("native_variant_not_nonblank_string")
    if not isinstance(raw.get("model_permaslug"), str) or not raw["model_permaslug"].strip():
        flags.append("native_model_permaslug_not_nonblank_string")
    label = raw.get("date")
    # Do not truncate an intraday timestamp into a daily observation. Inspect the
    # complete fractional token: datetime alone can truncate sub-microseconds.
    midnight_label = isinstance(label, str) and re.fullmatch(
        r"\d{4}-\d{2}-\d{2}(?:[T ]00:00:00(?:\.0+)?(?:Z|[+-]\d{2}:\d{2})?)?", label)
    if not midnight_label or date_label(label) is None:
        flags.append("native_date_not_valid_day_or_midnight_label")
    for k in CORE:
        if type(raw.get(k)) is not int or raw[k] < 0:
            flags.append("core_not_explicit_nonnegative_integer:" + k)
    cache = owner.get("cachedAt")
    cache_evidence = {"field_present": "cachedAt" in owner, "raw": cache,
                      "unit_status": "unconfirmed", "parsed_utc": None,
                      "screen_basis": "UTC_end_of_literal_calendar_day_not_proof_of_business_timezone"}
    # This exact original appStats/analytics field is evidenced as Unix milliseconds;
    # no dataUpdatedAt, response clock, neighboring owner, or numeric string fallback.
    if type(cache) is int and len(str(abs(cache))) == 13:
        try:
            instant = dt.datetime.fromtimestamp(cache // 1000, dt.timezone.utc) + dt.timedelta(milliseconds=cache % 1000)
            cache_evidence.update(unit_status="unix_milliseconds_existing_appStats_field_evidence", parsed_utc=instant.isoformat())
            day = date_label(raw.get("date"))
            if day and instant < dt.datetime.combine(dt.date.fromisoformat(day) + dt.timedelta(days=1), dt.time(), dt.timezone.utc):
                flags.append("cache_precedes_UTC_day_end_screen")
        except (ValueError, OverflowError, OSError):
            flags.append("cache_timestamp_invalid")
    else:
        flags.append("same_owner_cache_missing_or_unconfirmed")
    ev = {"field_projection": ev, "same_owner_cache": cache_evidence, "native_extractor_evidence": external_evidence}
    return make_candidate("table1_daily", raw, locator, model_keys=("model_permaslug",), date_key="date",
                          context=context, fields=fields, evidence=ev, flags=flags)


def extract_json(payload, event):
    """Known paths only. Never search arbitrary nested arrays by similar field names."""
    endpoint = event.get("endpoint")
    request_context = {"endpoint": endpoint, "params": event.get("params", {}),
                       "request_model_id": event.get("model_id"), "request_context": event.get("request_context", {})}
    rows, issues = [], []
    if not isinstance(payload, dict):
        return rows, ["unsupported_top_level_JSON_schema"]
    if any(payload.get(k) not in (None, "", False, {}, []) for k in ("error", "errors")):
        return rows, ["application_error_response"]
    data = payload.get("data")
    if endpoint == "model_activity" and isinstance(data, dict) and isinstance(data.get("analytics"), list):
        context = {**request_context, "native_array": "analytics", "window": "unknown"}
        for i, raw in enumerate(data["analytics"]):
            if isinstance(raw, dict):
                rows.append(daily_candidate(raw, f"/data/analytics/{i}", context, data))
            else:
                issues.append(f"unsupported_analytics_row:{i}")
        return rows, issues or (["explicit_empty_analytics_array_not_platform_zero"] if not rows else [])
    if endpoint in CHARTS:
        owner = data if isinstance(data, dict) else payload
        series = data.get("data") if isinstance(data, dict) else data
        if isinstance(series, list):
            base = "/data/data" if isinstance(data, dict) else "/data"
            meta = {k: v for k, v in owner.items() if k != "data"}
            for i, period in enumerate(series):
                if not isinstance(period, dict) or "x" not in period or not isinstance(period.get("ys"), dict):
                    issues.append(f"unsupported_chart_period:{i}")
                    continue
                context = {**request_context, "metric_series": endpoint, "unit": "unknown", "window": "unknown",
                           "tokenizer": "unknown", "owner_metadata": meta,
                           "period_metadata": {k: v for k, v in period.items() if k not in ("x", "ys")}}
                vector_id = digest({"response_sha256": event.get("sha256"), "pointer": base + "/" + str(i),
                                    "context": context, "period": period})
                vector_flags = ["metric_unit_unverified", "window_unverified", "point_finality_unverified"]
                if endpoint in {"categories", "natural_languages", "programming_languages", "context_length"}:
                    vector_flags.append("requested_filter_not_response_verified")
                if period.get("isForecast") is True or period.get("is_forecast") is True:
                    vector_flags.append("explicit_forecast_point")
                if any(number(v) is None or Decimal(number(v)) < 0 for v in period["ys"].values()):
                    vector_flags.append("whole_vector_invalid_numeric")
                for entity, value in period["ys"].items():
                    f = {"entity_raw": entity, "entity_key": "Others" if entity in ("Other", "Others") else entity,
                         "value_numeric": number(value), "value_raw_json": canonical(value),
                         "metric_series": endpoint, "metric_unit": "unknown", "window_key": "unknown",
                         "select_raw": canonical(event.get("params", {})), "vector_id": vector_id,
                         "vector_entity_count": str(len(period["ys"])), "forecast_configuration_raw": canonical(meta)}
                    # Original whole period is retained once per candidate for traceability,
                    # without joining top-N or Others from a different vector.
                    c = make_candidate(endpoint, {"date": period["x"], "entity": entity, "value": value},
                        pointer(base + "/" + str(i) + "/ys", entity), date_key="date",
                        context={**context, "entity_raw": entity}, fields=f,
                        evidence={"original_period": period, "period_locator": base + "/" + str(i)}, flags=vector_flags)
                    rows.append(c)
            return rows, issues or (["empty_chart_series_not_platform_zero"] if not series else [])
    if endpoint == "catalog" and isinstance(data, list):
        for i, raw in enumerate(data):
            if not isinstance(raw, dict):
                issues.append(f"unsupported_catalog_row:{i}")
                continue
            fields, evidence = project_fields(raw, ATTRIBUTE_PATHS)
            rows.append(make_candidate("model_attributes", raw, f"/data/{i}", model_keys=("id",),
                        context={**request_context, "scope": "catalog_model_object"}, fields=fields,
                        evidence=evidence, flags=["snapshot_observation_not_model_created_date", "HF_identifier_not_license_classification"]))
        return rows, issues or (["empty_catalog_not_platform_zero"] if not data else [])
    if endpoint == "model_endpoints" and isinstance(data, dict) and isinstance(data.get("endpoints"), list):
        for i, raw in enumerate(data["endpoints"]):
            if not isinstance(raw, dict):
                issues.append(f"unsupported_endpoint_row:{i}")
                continue
            fields, evidence = project_fields(raw, ATTRIBUTE_PATHS)
            evidence["parent_response_identity"] = {k: data[k] for k in ("id", "name", "slug") if k in data}
            evidence["endpoint_object_role"] = "data.endpoints[]"
            rows.append(make_candidate("provider_endpoints", raw, f"/data/endpoints/{i}", model_keys=("model_id", "model_permaslug"),
                        context={**request_context, "native_endpoint_identity": raw.get("id"), "scope": "model_endpoint"},
                        fields=fields, evidence=evidence, flags=["price_unit_and_endpoint_scope_preserved_not_global_model_price"]))
        return rows, issues or (["empty_endpoint_array_not_platform_zero"] if not rows else [])
    if endpoint in {"rankings_models", "fastest_models", "endpoint_stats", "providers", "zdr"} and isinstance(data, list):
        table = {"rankings_models": "rankings_history", "fastest_models": "fastest_models",
                 "endpoint_stats": "endpoint_performance_aux", "providers": "provider_directory", "zdr": "zdr_aux"}[endpoint]
        projection = {k: [(k,)] for k in (*CORE, "rank", "p50_latency", "p50_throughput", "request_count",
                      "best_latency_provider", "best_throughput_provider", "best_latency_price", "best_throughput_price", "provider_count")}
        vector_id = digest([event.get("sha256"), request_context, table, data])
        for i, raw in enumerate(data):
            if not isinstance(raw, dict):
                issues.append(f"unsupported_native_row:{i}")
                continue
            fields, ev = project_fields(raw, projection)
            fields.update(native_rank_raw=scalar_text(raw.get("rank")), array_position_1based=str(i + 1), vector_id=vector_id,
                          vector_entity_count=str(len(data)))
            flags = ["array_position_not_official_rank", "measurement_window_unverified"]
            if endpoint == "rankings_models":
                flags.append("ranking_view_not_daily_analytics")
            if endpoint in {"fastest_models", "endpoint_stats"}:
                flags.append("performance_units_and_measurement_window_unverified")
            rows.append(make_candidate(table, raw, f"/data/{i}",
                        model_keys=("model_permaslug", "permaslug", "model_id") if endpoint not in {"providers", "zdr"} else (),
                        date_key="date" if endpoint == "rankings_models" else None,
                        context={**request_context, "view": event.get("params", {}).get("view"),
                                 "native_array": "/data", "native_endpoint_id": raw.get("endpoint_id"),
                                 "native_directory_identity_raw": {k: raw[k] for k in ("id", "slug", "name") if k in raw}
                                      if endpoint in {"providers", "zdr"} else None,
                                 "native_stats_endpoint_id_raw": raw.get("id") if endpoint == "endpoint_stats" else None,
                                 "object_position_for_unidentified_rows": i if not any(raw.get(k) for k in ("model_permaslug", "permaslug", "model_id", "id")) else None},
                        fields=fields, evidence=ev, flags=flags))
        return rows, issues or (["empty_native_array_not_platform_zero"] if not rows else [])
    if endpoint == "effective_pricing" and isinstance(data, dict) and isinstance(data.get("providerSummaries"), (list, dict)):
        summaries = data["providerSummaries"]
        items = enumerate(summaries) if isinstance(summaries, list) else summaries.items()
        for key, raw in items:
            rows.append(make_candidate("effective_pricing_aux", raw, pointer("/data/providerSummaries", key),
                        context={**request_context, "provider_summary_key": key, "scope": "effective_pricing_summary"},
                        flags=["effective_price_not_catalog_or_endpoint_quote", "unit_and_measurement_window_unverified"]))
        return rows, issues or (["empty_provider_summaries_not_platform_zero"] if not rows else [])
    if endpoint == "benchmarks" and isinstance(data, dict):
        arrays = []
        for family in ("aaData", "daData"):
            if isinstance(data.get(family), dict):
                arrays.extend((f"/data/{family}/" + k.replace("~", "~0").replace("/", "~1"), family, k, v)
                              for k, v in data[family].items() if isinstance(v, list))
        for k in ("weightedInputPrices", "costPerRequest"):
            if isinstance(data.get(k), list):
                arrays.append(("/data/" + k, "cost_order", k, data[k]))
        for base, family, metric, values in arrays:
            for i, raw in enumerate(values):
                obj = raw if isinstance(raw, dict) else {}
                cost = family == "cost_order"
                fields = {"benchmark_score": None if cost else number(obj.get("score")), "native_rank_raw": scalar_text(obj.get("rank")),
                          "metric_key": metric, "metric_kind": "ordered_cost_list_ordinal" if cost else "reported_benchmark_score",
                          "source_order_ordinal": str(i + 1), "win_rate": number(obj.get("win_rate")),
                          "avg_generation_time_ms": number(obj.get("avg_generation_time_ms"))}
                rows.append(make_candidate("benchmarks", raw, base + "/" + str(i), model_keys=("permaslug",),
                            context={**request_context, "family": family, "metric": metric, "window": "unknown",
                                     "native_model_name": obj.get("name"), "cost_model_raw": raw if cost else None},
                            fields=fields, flags=["evaluation_date_unverified", "score_scale_and_version_unverified",
                                                 "cost_list_position_not_price" if cost else "original_metric_not_cross_metric_comparable"]))
        if arrays:
            return rows, issues or (["empty_benchmark_arrays_not_platform_zero"] if not rows else [])
    return rows, ["unsupported_JSON_schema_for_endpoint:" + str(endpoint)]


def extract_body(body, event):
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        return [], ["unsupported_non_UTF8_body"]
    if text.lstrip().startswith(("{", "[")):
        try:
            return extract_json(loads(text), event)
        except (ValueError, RecursionError) as exc:
            return [], ["invalid_JSON:" + str(exc)]
    if event.get("endpoint") == "model_page":
        try:
            from .native_html import extract_model_daily
        except ImportError:
            return [], ["native_HTML_extractor_unavailable_raw_preserved"]
        result = extract_model_daily(text, content_type=event.get("content_type", ""))
        rows = []
        for item in result.get("rows", []):
            ev = item.get("evidence", {})
            raw = item["raw"]
            # Native extractor evidence is retained verbatim; request/parent variant
            # is never copied into a row with a missing native field.
            owner = {"cachedAt": ev["cachedAt_raw"]} if "cachedAt_raw" in ev else {}
            rows.append(daily_candidate(raw, item["locator"],
                        {"endpoint": "model_page", "native_array": ev.get("statistics_property_name"),
                         "request_model_id": event.get("model_id"), "params": event.get("params", {}),
                         "query_identity_resolved": ev.get("query_identity_resolved"), "window": "unknown"}, owner, ev))
        return rows, result.get("issues", []) or ([] if rows else ["native_HTML_no_mapped_daily_rows_not_platform_zero"])
    return [], ["unsupported_non_JSON_body_raw_preserved"]


def safe_raw_path(batch, name):
    if not isinstance(name, str) or "\\" in name or ":" in name:
        raise IngestError("Invalid raw relative path")
    parts = PurePosixPath(name).parts
    if not parts or parts[0] != "raw" or any(x in ("..", ".") for x in parts) or not name.endswith(".gz"):
        raise IngestError("Raw path must be beneath raw/ and end in .gz")
    path = (batch / name).resolve()
    if not path.is_relative_to(batch.resolve()):
        raise IngestError("Raw path escapes batch")
    return path


def verify_body(batch, event):
    path = safe_raw_path(batch, event.get("path"))
    before = stat5(path)
    try:
        with gzip.open(path, "rb") as f:
            body = f.read()  # gzip EOF/CRC is checked by the full read.
    except (OSError, EOFError) as exc:
        raise IngestError("Invalid gzip: " + str(path)) from exc
    if before != stat5(path):
        raise IngestError("Raw file changed during read")
    actual = hashlib.sha256(body).hexdigest()
    if event.get("sha256") != actual or type(event.get("bytes")) is not int or event["bytes"] != len(body):
        raise IngestError("Raw body SHA/length mismatch: " + str(path))
    return body, {"absolute_path": str(path), "body_sha256": actual, "bytes": len(body), "stat": before}


def decode_http_entity(entity, event):
    encoding = event.get("response_headers", {}).get("content-encoding", "identity").strip().lower()
    if encoding in ("", "identity"):
        return entity
    if encoding in ("gzip", "x-gzip"):
        return gzip.decompress(entity)
    if encoding == "deflate":
        try:
            return zlib.decompress(entity)
        except zlib.error:
            return zlib.decompress(entity, -zlib.MAX_WBITS)
    raise IngestError("Unsupported HTTP Content-Encoding: " + encoding)


SCHEMA = """
CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE batches(run_id TEXT PRIMARY KEY,batch_sha256 TEXT NOT NULL,run_json TEXT NOT NULL,
 catalog_path TEXT NOT NULL,plan_path TEXT NOT NULL,quality_json TEXT NOT NULL,input_hashes_json TEXT NOT NULL);
CREATE TABLE events(event_id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES batches,
 request_id TEXT NOT NULL,attempt INTEGER NOT NULL,manifest_line INTEGER NOT NULL,
 endpoint TEXT,url TEXT,fetched_at TEXT,request_model_id TEXT,body_sha256 TEXT,raw_path TEXT,
 source_binding_json TEXT,manifest_json TEXT NOT NULL,extraction_issues_json TEXT NOT NULL,
 UNIQUE(run_id,request_id,attempt));
CREATE TABLE candidates(candidate_id TEXT PRIMARY KEY,extractor_version TEXT NOT NULL,table_name TEXT NOT NULL,
 logical_key TEXT NOT NULL,state_sha256 TEXT NOT NULL,body_sha256 TEXT NOT NULL,source_locator TEXT NOT NULL,
 model_id TEXT,identity_namespace TEXT,variant_raw_json TEXT,variant_state TEXT NOT NULL,
 business_date TEXT,date_label_raw_json TEXT,endpoint_id_raw_json TEXT,context_json TEXT NOT NULL,
 raw_record_json TEXT NOT NULL,fields_json TEXT NOT NULL,evidence_json TEXT NOT NULL,
 quality_flags_json TEXT NOT NULL,mapping_status TEXT NOT NULL);
CREATE TABLE event_candidates(event_id TEXT NOT NULL REFERENCES events,candidate_id TEXT NOT NULL REFERENCES candidates,
 PRIMARY KEY(event_id,candidate_id));
CREATE INDEX candidate_logical_key ON candidates(table_name,logical_key);
CREATE INDEX candidate_model_date ON candidates(table_name,model_id,business_date);
CREATE TABLE extractions(event_id TEXT NOT NULL REFERENCES events,extractor_version TEXT NOT NULL,
 code_hashes_json TEXT NOT NULL,issues_json TEXT NOT NULL,candidate_count INTEGER NOT NULL,
 PRIMARY KEY(event_id,extractor_version));
"""


def database_identity(path):
    """Read the stable instance UUID without creating or hashing a database."""
    path = Path(path).resolve()
    if not path.exists():
        return None
    ro = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        if ro.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID:
            raise IngestError("Refusing foreign or sealed database; choose a NEW observations.sqlite")
        if ro.execute("SELECT value FROM metadata WHERE key='database_kind'").fetchone() != (DATABASE_KIND,):
            raise IngestError("Database kind mismatch")
        value = ro.execute("SELECT value FROM metadata WHERE key='database_instance_id'").fetchone()
        return value[0] if value else None
    except sqlite3.DatabaseError as exc:
        raise IngestError("Invalid observation database metadata") from exc
    finally:
        ro.close()


def initialize_database(path):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        # Inspect read-only first. A sealed/foreign DB is never opened for writing.
        database_identity(path)
    db = sqlite3.connect(path)
    db.execute("PRAGMA foreign_keys=ON")
    if db.execute("PRAGMA application_id").fetchone()[0] == 0:
        db.executescript(SCHEMA)
        db.execute("PRAGMA application_id=" + str(APPLICATION_ID))
        db.execute("INSERT INTO metadata VALUES('database_kind',?)", (DATABASE_KIND,))
        db.commit()
    db.execute("INSERT OR IGNORE INTO metadata VALUES('database_instance_id',?)", (str(uuid.uuid4()),))
    db.commit()
    # Qualification clocks belong to a particular response event, not a shared
    # content-addressed candidate. Old candidates are retained; old unassessed
    # event links cannot enter the current screened view until reingested.
    db.execute("""CREATE TABLE IF NOT EXISTS event_candidate_qualifications(
        event_id TEXT NOT NULL REFERENCES events,candidate_id TEXT NOT NULL REFERENCES candidates,
        quality_flags_json TEXT NOT NULL,PRIMARY KEY(event_id,candidate_id))""")
    # These views expose candidates, never a greatest/latest automatic winner.
    tables = sorted(CHARTS | {"table1_daily", "model_attributes", "provider_endpoints", "rankings_history", "benchmarks",
                              "fastest_models", "provider_directory", "endpoint_performance_aux", "effective_pricing_aux", "zdr_aux"})
    fields = set(ATTRIBUTE_PATHS) | set(CORE) | {"num_media_prompt", "num_media_completion", "num_audio_prompt",
        "total_native_tokens_reasoning", "total_native_tokens_cached", "total_tool_calls", "requests_with_tool_call_errors",
        "entity_raw", "entity_key", "value_numeric", "value_raw_json", "metric_series", "metric_unit", "window_key", "select_raw",
        "vector_id", "vector_entity_count", "forecast_configuration_raw", "native_rank_raw", "array_position_1based",
        "p50_latency", "p50_throughput", "request_count", "best_latency_provider", "best_throughput_provider",
        "best_latency_price", "best_throughput_price", "provider_count", "benchmark_score", "metric_key", "metric_kind",
        "source_order_ordinal", "win_rate", "avg_generation_time_ms"}
    # Refresh only this application's view definitions on a code upgrade.
    view_names = ["v_observations", "v_revisions", "v_model_attributes_history", "v_table1_daily_screened", "v_table1_daily_increment"]
    view_names.extend("v_" + name for name in tables)
    for view in view_names:
        db.execute("DROP VIEW IF EXISTS " + view)
    db.execute("""CREATE VIEW v_revisions AS SELECT table_name,logical_key,extractor_version,
        COUNT(DISTINCT state_sha256) state_versions,COUNT(*) source_candidates FROM candidates
        GROUP BY table_name,logical_key,extractor_version HAVING COUNT(DISTINCT state_sha256)>1""")
    db.execute("""CREATE VIEW v_observations AS SELECT c.*,e.run_id,e.event_id,e.fetched_at observation_time,e.raw_path,
        e.url source_url,e.endpoint request_endpoint,e.request_model_id,0 AS actual_archive_capture_verified,
        'live_response_received_at_not_business_day' AS observation_time_role,
        q.quality_flags_json AS event_quality_flags_json
        FROM candidates c JOIN event_candidates ec USING(candidate_id) JOIN events e USING(event_id)
        LEFT JOIN event_candidate_qualifications q ON q.event_id=e.event_id AND q.candidate_id=c.candidate_id""")
    cols = ",".join('json_extract(fields_json,\'$.' + k + '\') AS "' + k + '"' for k in sorted(fields))
    for table in tables:
        db.execute(f"CREATE VIEW IF NOT EXISTS v_{table} AS SELECT *,{cols} FROM v_observations WHERE table_name='{table}'")
    db.execute("CREATE VIEW IF NOT EXISTS v_model_attributes_history AS SELECT * FROM v_model_attributes")
    db.execute("""CREATE VIEW IF NOT EXISTS v_table1_daily_screened AS
        SELECT * FROM v_table1_daily d WHERE d.variant_state='value' AND d.model_id IS NOT NULL
        AND d.business_date IS NOT NULL
        AND d.event_quality_flags_json IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM json_each(d.event_quality_flags_json) f WHERE
            f.value LIKE 'core_not_explicit_nonnegative_integer:%'
            OR f.value IN ('same_owner_cache_missing_or_unconfirmed','cache_timestamp_invalid',
              'cache_precedes_UTC_day_end_screen','response_received_before_UTC_day_end',
              'response_received_time_unconfirmed','cache_after_response_received',
              'native_variant_not_nonblank_string','native_model_permaslug_not_nonblank_string',
              'native_date_not_valid_day_or_midnight_label'))
        AND NOT EXISTS(SELECT 1 FROM v_revisions r WHERE r.table_name=d.table_name
            AND r.logical_key=d.logical_key AND r.extractor_version=d.extractor_version)
        """)
    db.execute("""CREATE VIEW IF NOT EXISTS v_table1_daily_increment AS
        SELECT * FROM v_table1_daily_screened d
        WHERE d.extractor_version=(SELECT value FROM metadata WHERE key='active_extractor_version')
        AND d.candidate_id=(
          SELECT MIN(c.candidate_id) FROM v_table1_daily_screened c
          WHERE c.logical_key=d.logical_key AND c.extractor_version=d.extractor_version)
        AND d.event_id=(SELECT MIN(c.event_id) FROM v_table1_daily_screened c
          WHERE c.candidate_id=d.candidate_id)""")
    db.commit()
    return db


def code_hashes():
    from .configuration import config_file
    paths = [Path(__file__), Path(__file__).with_name("native_html.py"),
             Path(__file__).with_name("configuration.py"), config_file("table_mappings.json")]
    return {p.name: sha_file(p) if p.is_file() else "missing" for p in paths}


def import_signature():
    """Stable code/mapping fingerprint used by sync receipts to trigger reparsing."""
    return digest({"version": EXTRACTOR_VERSION, "files": code_hashes()})


def ingest(batch, database):
    batch = Path(batch).resolve()
    hashes, documents, input_stats = {}, {}, {}
    for name in REQUIRED_FILES:
        p = batch / name
        input_stats[name] = stat5(p)
        hashes[name] = sha_file(p)
        if name != "manifest.jsonl":
            documents[name] = loads(p.read_text(encoding="utf-8-sig"))
    run = documents["run.json"]
    run_id = run.get("run_id") if isinstance(run, dict) else None
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", run_id):
        raise IngestError("Invalid or absent run_id")
    if Path(database).resolve().is_relative_to(batch):
        raise IngestError("Database must be outside the immutable batch directory")
    # Frozen batch content defines idempotence; changing quality/catalog is a new run.
    batch_hash = digest(hashes)
    codes = code_hashes()
    signature = import_signature()
    effective_version = EXTRACTOR_VERSION + "+" + signature
    db = initialize_database(database)
    stats = Counter()
    try:
        with db:
            existing = db.execute("SELECT batch_sha256 FROM batches WHERE run_id=?", (run_id,)).fetchone()
            if existing and existing[0] != batch_hash:
                raise IngestError("run_id reused with different batch bytes")
            db.execute("INSERT OR IGNORE INTO batches VALUES(?,?,?,?,?,?,?)", (run_id, batch_hash, canonical(run),
                       str(batch / "catalog.json"), str(batch / "plan.json"), canonical(documents["quality.json"]), canonical(hashes)))
            seen = set()
            with (batch / "manifest.jsonl").open(encoding="utf-8-sig") as f:
                for line_no, line in enumerate(f, 1):
                    if not line.strip():
                        raise IngestError("Blank manifest line")
                    event = loads(line)
                    if not isinstance(event, dict) or not isinstance(event.get("request_id"), str) or type(event.get("attempt")) is not int:
                        raise IngestError("Invalid manifest event identity")
                    key = (event["request_id"], event["attempt"])
                    if key in seen:
                        raise IngestError("Duplicate request_id/attempt in manifest")
                    seen.add(key)
                    stats["events"] += 1
                    event_id = digest([run_id, *key])
                    binding, candidates, issues = None, [], []
                    if event.get("path") is not None:
                        body, binding = verify_body(batch, event)
                        stats["raw_bodies_verified"] += 1
                        valid = (type(event.get("status")) is int and 200 <= event["status"] < 300
                                 and not event.get("error") and event.get("ok") is True
                                 and event.get("body_complete") is True
                                 and event.get("response_validation", {}).get("valid") is True)
                        if valid:
                            try:
                                decoded = decode_http_entity(body, event)
                            except (IngestError, OSError, EOFError, zlib.error) as exc:
                                issues = ["HTTP_encoding_unparsed_raw_preserved:" + str(exc)]
                            else:
                                binding["decoded_body_sha256"] = hashlib.sha256(decoded).hexdigest()
                                binding["decoded_body_bytes"] = len(decoded)
                                candidates, issues = extract_body(decoded, dict(event, content_type=event.get("response_headers", {}).get("content-type", "")))
                        else:
                            issues = ["failed_HTTP_or_transport_response_not_extracted"]
                    else:
                        if type(event.get("status")) is int and 200 <= event["status"] < 300:
                            raise IngestError("Successful event missing raw body")
                        issues = ["no_response_body_transport_failure"]
                    # Type-check dependency results before any numeric text projection.
                    canonical(candidates)
                    db.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                        event_id, run_id, event["request_id"], event["attempt"], line_no, event.get("endpoint"),
                        event.get("url"), event.get("fetched_at"), event.get("model_id"), event.get("sha256"),
                        binding["absolute_path"] if binding else None, canonical(binding), canonical(event), canonical(issues)))
                    # Explicit columns prevent a future schema change silently moving fields.
                    for c in candidates:
                        event_flags = list(c["quality_flags"])
                        if c["table_name"] == "table1_daily" and c["business_date"]:
                            received = event.get("fetched_at")
                            try:
                                clock = dt.datetime.fromisoformat(received.replace("Z", "+00:00"))
                                if clock.tzinfo is None:
                                    raise ValueError("timezone absent")
                                cutoff = dt.datetime.combine(dt.date.fromisoformat(c["business_date"]) + dt.timedelta(days=1), dt.time(), dt.timezone.utc)
                                if clock < cutoff:
                                    event_flags.append("response_received_before_UTC_day_end")
                                cache_utc = c["evidence"].get("same_owner_cache", {}).get("parsed_utc")
                                if cache_utc and dt.datetime.fromisoformat(cache_utc) > clock:
                                    event_flags.append("cache_after_response_received")
                            except (AttributeError, ValueError):
                                event_flags.append("response_received_time_unconfirmed")
                        state_hash = digest(c["raw_record"])
                        cid = digest([effective_version, event["sha256"], c["table_name"], c["source_locator"], c["context"], c["raw_record"]])
                        vals = (cid, effective_version, c["table_name"], c["logical_key"], state_hash,
                                event["sha256"], c["source_locator"], c["model_id"], c["identity_namespace"],
                                canonical(c["variant_raw"]), c["variant_state"], c["business_date"], canonical(c["date_label_raw"]),
                                canonical(c["endpoint_id_raw"]), canonical(c["context"]), canonical(c["raw_record"]),
                                canonical(c["fields"]), canonical(c["evidence"]), canonical(c["quality_flags"]), c["mapping_status"])
                        cur = db.execute("INSERT OR IGNORE INTO candidates VALUES(" + ",".join("?" for _ in vals) + ")", vals)
                        stats["new_candidates"] += cur.rowcount
                        stats["mapped_candidates"] += 1
                        stats["table:" + c["table_name"]] += 1
                        db.execute("INSERT OR IGNORE INTO event_candidates VALUES(?,?)", (event_id, cid))
                        db.execute("INSERT OR IGNORE INTO event_candidate_qualifications VALUES(?,?,?)",
                                   (event_id, cid, canonical(sorted(set(event_flags)))))
                    db.execute("INSERT OR IGNORE INTO extractions VALUES(?,?,?,?,?)", (event_id, effective_version, canonical(codes), canonical(issues), len(candidates)))
                    if issues:
                        stats["events_with_extraction_issues"] += 1
                    if not candidates:
                        stats["events_without_mapped_candidates"] += 1
            for name in REQUIRED_FILES:
                if input_stats[name] != stat5(batch / name) or hashes[name] != sha_file(batch / name):
                    raise IngestError("Batch input changed during ingestion: " + name)
            if codes != code_hashes():
                raise IngestError("Extractor or mapping changed during ingestion")
            db.execute("INSERT INTO metadata(key,value) VALUES('active_extractor_version',?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (effective_version,))
        return {"status": "ingestion_complete_candidates_provisional", "run_id": run_id, "batch_sha256": batch_hash,
                "database": str(Path(database).resolve()), "database_instance_id": database_identity(database),
                "extractor_version": effective_version, "import_signature": signature,
                "counts": dict(stats), "quality_approved": False, "sealed_baseline_modified": False,
                "scope": "append_only_native_candidates_not_selected_research_master"}
    finally:
        db.close()


def export_views(database, output):
    """Explicit export to a NEW directory; never opens or merges a baseline."""
    output = Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise IngestError("Export directory must be new or empty; existing tables are not overwritten")
    output.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        if db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID:
            raise IngestError("Export only accepts an or-new observations database")
        names = sorted(CHARTS | {"table1_daily", "table1_daily_increment", "model_attributes", "model_attributes_history",
             "provider_endpoints", "rankings_history", "benchmarks", "fastest_models", "provider_directory",
             "endpoint_performance_aux", "effective_pricing_aux", "zdr_aux"})
        results = {}
        for name in names:
            cursor = db.execute("SELECT * FROM v_" + name)
            columns = [x[0] for x in cursor.description]
            path = output / (name + ".csv")
            count = 0
            with path.open("w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                for row in cursor:
                    writer.writerow(row)
                    count += 1
            results[name] = {"rows": count, "sha256": sha_file(path), "file": path.name,
                             "scope": "new_collector_observations_only_no_sealed_baseline_join"}
        (output / "EXPORT.json").write_text(canonical({"status": "export_complete_provisional", "views": results,
            "baseline_joined": False, "quality_approved": False}), encoding="utf-8")
        return results
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--export-dir", type=Path, help="Optional NEW directory for per-table provisional CSVs")
    args = parser.parse_args(argv)
    try:
        result = ingest(args.batch, args.database)
        if args.export_dir:
            result["exports"] = export_views(args.database, args.export_dir)
    except (IngestError, OSError, sqlite3.Error, ValueError) as exc:
        print(canonical({"status": "ingestion_failed", "error": str(exc), "sealed_baseline_modified": False}), file=sys.stderr)
        return 2
    print(canonical(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
