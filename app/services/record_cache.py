import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, Tuple

import redis.asyncio as redis

from app.services.data_source_client import build_request_payloads, normalize_remote_body


logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = float(os.getenv("REPORT_FILTERS_CACHE_TTL", "30"))
_CACHE_MAX_ITEMS = int(os.getenv("REPORT_FILTERS_CACHE_MAX", "20"))
_STORE: Dict[str, Tuple[float, Any]] = {}
_REDIS_CLIENT: redis.Redis | None = None
_REDIS_URL: str | None = None


def _get_redis_client() -> redis.Redis | None:
    global _REDIS_CLIENT, _REDIS_URL
    url = os.getenv("REDIS_URL")
    if not url:
        return None
    if _REDIS_CLIENT is None or url != _REDIS_URL:
        _REDIS_URL = url
        _REDIS_CLIENT = redis.from_url(url)
    return _REDIS_CLIENT


def _serialize_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _deserialize_value(payload: Any) -> Any | None:
    if payload is None:
        return None
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


async def get_cached_records(key: str) -> Any | None:
    if not key:
        return None
    client = _get_redis_client()
    if client is not None:
        try:
            payload = await client.get(key)
        except Exception as exc:
            logger.warning("Record cache redis get failed", extra={"error": str(exc)})
        else:
            value = _deserialize_value(payload)
            if value is not None:
                logger.info("Record cache hit", extra={"backend": "redis", "key": key[:12]})
                return value
            logger.info("Record cache miss", extra={"backend": "redis", "key": key[:12]})
            return None

    entry = _STORE.get(key)
    if not entry:
        logger.info("Record cache miss", extra={"backend": "memory", "key": key[:12]})
        return None
    created_at, value = entry
    if time.time() - created_at > _CACHE_TTL_SECONDS:
        _STORE.pop(key, None)
        logger.info("Record cache miss", extra={"backend": "memory", "key": key[:12]})
        return None
    logger.info("Record cache hit", extra={"backend": "memory", "key": key[:12]})
    return value


async def set_cached_records(key: str, value: Any) -> None:
    if not key:
        return
    client = _get_redis_client()
    if client is not None:
        try:
            payload = _serialize_value(value)
            ttl_seconds = int(_CACHE_TTL_SECONDS) if _CACHE_TTL_SECONDS > 0 else 0
            if ttl_seconds > 0:
                await client.setex(key, ttl_seconds, payload)
            else:
                await client.set(key, payload)
            return
        except Exception as exc:
            logger.warning("Record cache redis set failed", extra={"error": str(exc)})

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
