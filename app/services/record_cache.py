import hashlib
import json
import os
import time
from typing import Any, Dict, Tuple

from app.services.data_source_client import build_request_payloads, normalize_remote_body


_CACHE_TTL_SECONDS = float(os.getenv("REPORT_FILTERS_CACHE_TTL", "30"))
_CACHE_MAX_ITEMS = int(os.getenv("REPORT_FILTERS_CACHE_MAX", "20"))
_STORE: Dict[str, Tuple[float, Any]] = {}


def get_cached_records(key: str) -> Any | None:
    if not key:
        return None
    entry = _STORE.get(key)
    if not entry:
        return None
    created_at, value = entry
    if time.time() - created_at > _CACHE_TTL_SECONDS:
        _STORE.pop(key, None)
        return None
    return value


def set_cached_records(key: str, value: Any) -> None:
    if not key:
        return
    if len(_STORE) >= _CACHE_MAX_ITEMS:
        oldest_key = min(_STORE.items(), key=lambda item: item[1][0])[0]
        _STORE.pop(oldest_key, None)
    _STORE[key] = (time.time(), value)


def _safe_json_payload(value: Any) -> Any:
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return value
    return str(value)


def build_records_cache_key(
    template_id: str,
    remote_source: Any,
    joins: Any,
) -> str:
    cache_template_id = (
        template_id
        or getattr(remote_source, "id", None)
        or getattr(remote_source, "remoteId", None)
        or ""
    )
    body = normalize_remote_body(remote_source) if remote_source else {}
    url = getattr(remote_source, "url", None) if remote_source else None
    method = getattr(remote_source, "method", None) if remote_source else None
    remote_meta = getattr(remote_source, "remoteMeta", None) if remote_source else None
    remote_meta_key = None
    if isinstance(remote_meta, dict):
        keys = ("jobId", "batchJobId", "batchId", "resultsFileRef", "results_file_ref")
        remote_meta_key = {
            key: remote_meta.get(key)
            for key in keys
            if remote_meta.get(key) is not None
        }
    request_payloads = build_request_payloads(body)
    request_params = [payload.params for payload in request_payloads if payload.params is not None]
    payload = {
        "templateId": cache_template_id,
        "url": _safe_json_payload(url),
        "method": _safe_json_payload(method),
        "remoteMetaKey": _safe_json_payload(remote_meta_key),
        "body": _safe_json_payload(body),
        "requestParams": _safe_json_payload(request_params),
        "joins": _safe_json_payload(joins),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
