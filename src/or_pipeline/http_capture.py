"""Public HTTP GET capture. Every attempt is retained, including failed bodies."""
from __future__ import annotations

import datetime as dt
import email.utils
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

ALLOWED_HOSTS = frozenset({"openrouter.ai", "www.openrouter.ai"})
RESPONSE_HEADERS = frozenset({
    "content-type", "content-length", "content-encoding", "transfer-encoding",
    "date", "etag", "last-modified", "cache-control", "age", "expires", "vary",
    "cf-cache-status", "cf-ray", "retry-after", "location", "server", "x-cache",
    "x-request-id", "request-id", "content-range", "accept-ranges",
})
RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate_public_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS
            or parsed.username is not None or parsed.password is not None
            or parsed.port not in (None, 443) or parsed.fragment):
        raise ValueError("Only credential-free HTTPS requests to the configured official hosts are allowed")
    return url


def selected_headers(headers) -> dict:
    return {str(k).lower(): str(v) for k, v in headers.items() if str(k).lower() in RESPONSE_HEADERS}


class PublicRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        self.history = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        resolved = urllib.parse.urljoin(req.full_url, newurl)
        self.history.append({"url": req.full_url, "status": code, "location": resolved,
                             "response_headers": selected_headers(headers)})
        # Follow in CaptureClient, after retaining this redirect's own body.
        return None


def read_captured_body(output: Path, record: dict) -> bytes:
    """Verify the retained entity bytes; decode HTTP Content-Encoding only for parsing."""
    with gzip.open(output / record["path"], "rb") as f:
        body = f.read()
    if hashlib.sha256(body).hexdigest() != record["sha256"] or len(body) != record["bytes"]:
        raise ValueError("Captured body does not match its manifest")
    encoding = record.get("response_headers", {}).get("content-encoding", "identity").strip().lower()
    if encoding in ("", "identity"):
        return body
    if encoding in ("gzip", "x-gzip"):
        return gzip.decompress(body)
    if encoding == "deflate":
        try:
            return zlib.decompress(body)
        except zlib.error:
            return zlib.decompress(body, -zlib.MAX_WBITS)
    raise ValueError(f"Unsupported HTTP Content-Encoding retained unchanged: {encoding}")


def validate_payload(output: Path, record: dict, expected: str) -> dict:
    if record.get("status") != 200 or not record.get("body_complete"):
        return {"valid": False, "reason": "HTTP_or_body_not_complete"}
    try:
        body = read_captured_body(output, record)
        if expected == "json":
            payload = json.loads(body.decode("utf-8-sig"))
            if not isinstance(payload, (dict, list)):
                raise ValueError("JSON response is not an object or array")
            if isinstance(payload, dict) and payload.get("error") not in (None, "", False, {}):
                return {"valid": False, "reason": "application_error_in_HTTP_200", "error": payload["error"]}
            return {"valid": True, "format": "json", "empty_data":
                    isinstance(payload, dict) and payload.get("data") in ([], {})}
        if expected == "html":
            mime = record.get("response_headers", {}).get("content-type", "").lower()
            if "html" not in mime and not body.lstrip().lower().startswith((b"<!doctype html", b"<html")):
                return {"valid": False, "reason": "expected_HTML_response_structure_unknown"}
            if not body:
                raise ValueError("Empty HTML body")
            return {"valid": True, "format": "html", "page_semantics_verified": False}
        return {"valid": True, "format": "uninterpreted", "page_semantics_verified": False}
    except (ValueError, OSError, EOFError, UnicodeError, zlib.error) as exc:
        return {"valid": False, "reason": "response_validation_failed", "error": repr(exc)}


class CaptureClient:
    """Thread-safe manifest and bounded retries. No credentials, truncation or model limit."""
    def __init__(self, output: Path, *, timeout: float = 45, retries: int = 2,
                 min_interval: float = 0.35, max_retry_after: float = 300,
                 transport=None, sleeper=time.sleep, monotonic=time.monotonic):
        self.output = Path(output)
        (self.output / "raw").mkdir(parents=True, exist_ok=True)
        self.manifest = self.output / "manifest.jsonl"
        self.manifest.touch(exist_ok=True)
        self.timeout, self.retries = timeout, retries
        self.min_interval, self.max_retry_after = min_interval, max_retry_after
        self.transport, self.sleep = transport, sleeper
        self.monotonic = monotonic
        self.lock = threading.Lock()
        self.rate_lock = threading.Lock()
        self.next_request = 0.0
        self.cooldown_until = 0.0
        self.paused = threading.Event()
        self.pause_reason = None

    def _wait(self):
        with self.rate_lock:
            moment = self.monotonic()
            due = max(moment, self.next_request, self.cooldown_until)
            self.next_request = due + self.min_interval
        while True:
            if self.paused.is_set():
                return
            with self.rate_lock:
                due = max(due, self.cooldown_until)
            delay = due - self.monotonic()
            if delay <= 0:
                return
            self.sleep(delay)

    def _write(self, record):
        with self.lock, self.manifest.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _retry_seconds(self, headers, attempt):
        value = headers.get("retry-after")
        delay = min(60.0, 2.0 ** attempt)
        if value:
            try:
                delay = max(delay, float(value))
            except (TypeError, ValueError):
                try:
                    when = email.utils.parsedate_to_datetime(value)
                    if when.tzinfo is not None:
                        delay = max(delay, (when - dt.datetime.now(dt.timezone.utc)).total_seconds())
                except (TypeError, ValueError, OverflowError):
                    pass
        return max(0.0, delay)

    def capture(self, request: dict) -> dict:
        validate_public_url(request["url"])
        attempts = []
        attempt, retries_used, redirects_used = 0, 0, 0
        current_url = request["url"]
        seen_redirect_urls = {current_url}
        while True:
            if self.paused.is_set():
                break
            self._wait()
            if self.paused.is_set():
                break
            attempt += 1
            active = dict(request, url=current_url,
                          params=dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(current_url).query, keep_blank_values=True)))
            rec = self._attempt(active, attempt)
            rec["plan_url"] = request["url"]
            rec["plan_params"] = request.get("params", {})
            follow = None
            if rec["status"] in (301, 302, 303, 307, 308) and rec["body_complete"]:
                try:
                    location = rec["response_headers"].get("location")
                    if not location:
                        raise ValueError("Redirect has no Location")
                    follow = urllib.parse.urljoin(current_url, location)
                    validate_public_url(follow)
                    if follow in seen_redirect_urls or redirects_used >= 5:
                        raise ValueError("Redirect cycle or bounded redirect count exceeded")
                    rec["redirect_follow_url"] = follow
                except ValueError as exc:
                    rec["error"] = repr(exc)
                    follow = None
            validation = validate_payload(self.output, rec, request.get("expected", "json"))
            rec["response_validation"] = validation
            rec["ok"] = bool(rec.get("status") == 200 and rec.get("body_complete") and validation["valid"])
            if not rec["ok"] and not rec.get("error"):
                rec["error"] = validation.get("reason") if rec.get("status") == 200 else f"HTTP_{rec.get('status')}"
            retryable = rec["status"] in RETRY_STATUSES or rec["status"] is None or not rec["body_complete"]
            delay = self._retry_seconds(rec["response_headers"], attempt)
            rec["retry_planned"] = bool(not rec["ok"] and retryable and retries_used < self.retries)
            if rec["status"] in (429, 503):
                with self.rate_lock:
                    self.cooldown_until = max(self.cooldown_until, self.monotonic() + delay)
            if rec["status"] in (429, 503) and delay > self.max_retry_after:
                self.pause_reason = {"request_id": request["request_id"], "status": rec["status"],
                                     "retry_after_seconds": delay, "observed_at_utc": utc_now()}
                self.paused.set()
                rec["retry_planned"] = False
                rec["retry_deferred_service_pause"] = self.pause_reason
            self._write(rec)
            attempts.append(rec)
            if follow:
                seen_redirect_urls.add(follow)
                current_url = follow
                redirects_used += 1
                continue
            if rec["ok"] or not rec["retry_planned"]:
                break
            retries_used += 1
            self.sleep(delay)
        return {"request_id": request["request_id"], "endpoint": request["endpoint"],
                "model_id": request.get("model_id"), "attempted": bool(attempts),
                "ok": bool(attempts and attempts[-1]["ok"]), "attempts": attempts,
                "last": attempts[-1] if attempts else None,
                "not_attempted_reason": None if attempts else "service_pause_full_plan_preserved"}

    def _attempt(self, request: dict, attempt: int) -> dict:
        started = utc_now()
        filename = f"{request['request_id']}_{attempt:02d}.bin.gz"
        path = self.output / "raw" / filename
        if path.exists():
            raise FileExistsError("Capture output is not fresh; refusing to overwrite an attempt")
        temp = path.with_suffix(".part")
        digest = hashlib.sha256()
        total = 0
        status = None
        headers = {}
        final_url = request["url"]
        error = None
        complete = False
        response = None
        redirects = PublicRedirectHandler()
        request_headers = {"User-Agent": "OpenRouterResearchArchive/1.0 (+https://github.com/lyll23/or_new)",
                           "Accept": "text/html" if request.get("expected") == "html" else "application/json",
                           "Accept-Encoding": "identity"}
        with temp.open("xb") as raw_file:
            with gzip.GzipFile(fileobj=raw_file, mode="wb", mtime=0, filename="") as compressed:
                def retain(block):
                    nonlocal total
                    compressed.write(block)
                    digest.update(block)
                    total += len(block)
                try:
                    try:
                        if self.transport is not None:
                            response = self.transport(request["url"], request_headers, self.timeout)
                        else:
                            opener = urllib.request.build_opener(redirects)
                            response = opener.open(urllib.request.Request(request["url"], headers=request_headers), timeout=self.timeout)
                    except urllib.error.HTTPError as exc:
                        response = exc  # HTTP errors still carry a response body worth retaining.
                    status = getattr(response, "status", None) or response.getcode()
                    headers = selected_headers(response.headers)
                    final_url = response.geturl()
                    validate_public_url(final_url)
                    while True:
                        block = response.read(1024 * 1024)
                        if not block:
                            break
                        retain(block)
                    content_length = headers.get("content-length")
                    if content_length and content_length.isdecimal() and "transfer-encoding" not in headers:
                        if total != int(content_length):
                            raise ValueError("Response entity length differs from Content-Length")
                    complete = True
                except http.client.IncompleteRead as exc:
                    if exc.partial:
                        retain(exc.partial)
                    error = repr(exc)
                except Exception as exc:
                    error = repr(exc)
                finally:
                    if response is not None:
                        response.close()
            raw_file.flush()
            os.fsync(raw_file.fileno())
        temp.replace(path)
        return {"request_id": request["request_id"], "attempt": attempt, "method": "GET",
                "url": request["url"], "final_url": final_url, "model_id": request.get("model_id"),
                "endpoint": request["endpoint"], "params": request.get("params", {}),
                "request_context": request.get("request_context", {}),
                "endpoint_stability": request.get("stability", "unknown"),
                "expected_format": request.get("expected", "json"),
                "status": status, "http_status": status, "started_at": started,
                "fetched_at": utc_now(), "path": path.relative_to(self.output).as_posix(),
                "sha256": digest.hexdigest(), "bytes": total, "gzip_bytes": path.stat().st_size,
                "body_complete": complete, "error": error, "response_headers": headers,
                "request_headers": request_headers, "redirects": redirects.history,
                "body_representation": "HTTP_entity_bytes_before_Content_Encoding_decode",
                "credentials_sent": False}
