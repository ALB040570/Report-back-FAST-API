import ipaddress
import logging
import os
import re
import time
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Iterable, List

import json
import httpx
from app.observability.metrics import record_pushdown_request
from app.services.pushdown import (
    build_body_with_pushdown,
    host_allowed,
    parse_pushdown,
)
from app.services.upstream_client import UpstreamHTTPError, async_request_json, build_full_url, request_json


from app.models.filters import Filters
from app.models.remote_source import RemoteSource

SERVICE360_BASE_URL = os.getenv("SERVICE360_BASE_URL", "http://45.8.116.32")
logger = logging.getLogger(__name__)
_RESULTS_DIR = os.path.join(os.getcwd(), "batch_results")

_CAMEL_SPLIT_RE = re.compile(r"[^0-9A-Za-z]+")


@dataclass(frozen=True)
class RequestPayload:
    body: Any
    params: Dict[str, Any] | None


def _build_mock_records(remote_source: RemoteSource, filters: Filters) -> List[Dict[str, Any]]:
    """
    Генератор тестовых данных, если remoteSource.url начинается с mock://
    Это позволяет тестировать pivot без реального бэкенда.
    """
    # Можно варьировать структуру в зависимости от id/url, если захочется
    records: List[Dict[str, Any]] = [
        {"cls": "A", "year": 2024, "value": 10, "count": 1},
        {"cls": "A", "year": 2024, "value": 20, "count": 2},
        {"cls": "B", "year": 2024, "value": 5,  "count": 3},
        {"cls": "B", "year": 2023, "value": 15, "count": 4},
    ]
    return records


def get_records_limit() -> int | None:
    value = os.getenv("REPORT_MAX_RECORDS")
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def normalize_remote_body(remote_source: RemoteSource) -> Any:
    body: Any = remote_source.body or {}

    if (not body) and remote_source.rawBody:
        try:
            body = json.loads(remote_source.rawBody)
        except Exception:
            body = remote_source.rawBody
    if isinstance(body, dict):
        body = {**body}
        body.pop("__joins", None)
    return body


def build_request_payloads(body: Any) -> List[RequestPayload]:
    if not isinstance(body, dict):
        return [RequestPayload(body=body, params=None)]

    split_params = bool(body.get("splitParams"))
    cleaned_body = {**body}
    cleaned_body.pop("splitParams", None)

    requests = cleaned_body.get("requests")
    if isinstance(requests, list):
        return _build_request_payloads_from_requests(cleaned_body, requests)

    params_list = cleaned_body.get("params")
    if isinstance(params_list, list):
        if any(not isinstance(entry, dict) for entry in params_list):
            return [RequestPayload(body=cleaned_body, params=None)]
        if split_params:
            return _build_request_payloads_from_params(cleaned_body, params_list)
        return [RequestPayload(body=cleaned_body, params=None)]

    params = cleaned_body.get("params")
    request_params = params if isinstance(params, dict) else None
    return [RequestPayload(body=cleaned_body, params=request_params)]


def _build_request_payloads_from_params(
    body: Dict[str, Any],
    params_list: List[Any],
) -> List[RequestPayload]:
    if not params_list:
        return []
    base_body = {**body}
    base_body.pop("params", None)
    base_body.pop("requests", None)
    base_body.pop("splitParams", None)

    payloads: List[RequestPayload] = []
    for entry in params_list:
        if not isinstance(entry, dict):
            continue
        request_body = {**base_body, "params": [entry]}
        payloads.append(RequestPayload(body=request_body, params=entry))
    return payloads


def _build_request_payloads_from_requests(
    body: Dict[str, Any],
    requests_list: List[Any],
) -> List[RequestPayload]:
    if not requests_list:
        return []
    base_body = {**body}
    base_body.pop("requests", None)
    base_body.pop("splitParams", None)

    payloads: List[RequestPayload] = []
    for entry in requests_list:
        if not isinstance(entry, dict):
            continue
        request_body = {**base_body}
        request_params: Dict[str, Any] | None = None

        entry_body = entry.get("body")
        if isinstance(entry_body, dict):
            cleaned_body = {**entry_body}
            cleaned_body.pop("__joins", None)
            cleaned_body.pop("splitParams", None)
            request_body.update(cleaned_body)
            params_from_body = cleaned_body.get("params")
            if isinstance(params_from_body, dict):
                request_params = params_from_body

        if "params" in entry:
            entry_params = entry.get("params")
            request_body["params"] = entry_params
            if isinstance(entry_params, dict):
                request_params = entry_params

        if request_params is None and isinstance(request_body.get("params"), dict):
            request_params = request_body.get("params")

        payloads.append(RequestPayload(body=request_body, params=request_params))
    return payloads


def _to_camel_case(value: str) -> str:
    parts = [part for part in _CAMEL_SPLIT_RE.split(value) if part]
    if not parts:
        return ""
    if len(parts) == 1:
        return f"{parts[0][:1].upper()}{parts[0][1:]}"
    return "".join(f"{part[:1].upper()}{part[1:]}" for part in parts)


def _get_remote_allowlist() -> str | None:
    return os.getenv("REPORT_REMOTE_ALLOWLIST") or os.getenv("UPSTREAM_ALLOWLIST")


def _get_paging_allowlist() -> str | None:
    return os.getenv("REPORT_PAGING_ALLOWLIST")


def _get_paging_max_pages() -> int:
    value = os.getenv("REPORT_PAGING_MAX_PAGES")
    if not value:
        return 2000
    try:
        parsed = int(value)
    except ValueError:
        return 2000
    return parsed if parsed > 0 else 2000


def _get_upstream_paging_enabled() -> bool:
    value = os.getenv("REPORT_UPSTREAM_PAGING")
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_pushdown_enabled() -> bool:
    value = os.getenv("REPORT_UPSTREAM_PUSHDOWN")
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_pushdown_allowlist() -> str | None:
    return os.getenv("REPORT_PUSHDOWN_ALLOWLIST")


def _get_pushdown_max_filters() -> int:
    value = os.getenv("REPORT_PUSHDOWN_MAX_FILTERS")
    if not value:
        return 50
    try:
        parsed = int(value)
    except ValueError:
        return 50
    return parsed if parsed > 0 else 50


def _get_pushdown_max_in_values() -> int:
    value = os.getenv("REPORT_PUSHDOWN_MAX_IN_VALUES")
    if not value:
        return 200
    try:
        parsed = int(value)
    except ValueError:
        return 200
    return parsed if parsed > 0 else 200


def _get_pushdown_safe_only() -> bool:
    value = os.getenv("REPORT_PUSHDOWN_SAFE_ONLY")
    if value is None:
        return True
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_pushdown_override() -> bool:
    value = os.getenv("REPORT_PUSHDOWN_OVERRIDE")
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_pushdown_state(
    full_url: str,
    remote_source: RemoteSource,
) -> tuple[Any | None, bool, str, str]:
    pushdown_cfg = parse_pushdown(remote_source)
    pushdown_env_enabled = _get_pushdown_enabled()
    pushdown_allowlist = _get_pushdown_allowlist()
    pushdown_override = _get_pushdown_override()
    allowlist_allowed = host_allowed(full_url, pushdown_allowlist) if full_url else False
    pushdown_allowed_for_host = bool(pushdown_override or allowlist_allowed)
    override_used = bool(pushdown_override and not allowlist_allowed)
    if not pushdown_env_enabled:
        pushdown_reason = "disabled_env"
    elif not pushdown_cfg:
        pushdown_reason = "no_config"
    elif not pushdown_allowed_for_host:
        pushdown_reason = "not_allowlisted"
    elif override_used:
        pushdown_reason = "override_enabled"
    else:
        pushdown_reason = "applied"
    pushdown_active = bool(pushdown_env_enabled and pushdown_cfg and pushdown_allowed_for_host)
    pushdown_result = "override_enabled" if override_used else "applied"
    return pushdown_cfg, pushdown_active, pushdown_reason, pushdown_result


def _is_full_url_allowed(url: str, allowlist: str | None) -> bool:
    if not allowlist:
        return False
    target = urlparse(url)
    target_host = target.netloc.lower()
    entries = [item.strip() for item in allowlist.split(",") if item.strip()]
    for entry in entries:
        if entry.startswith("http://") or entry.startswith("https://"):
            parsed = urlparse(entry)
            if parsed.scheme == target.scheme and parsed.netloc.lower() == target_host:
                return True
        else:
            if entry.lower() == target_host:
                return True
    return False


def _is_host_allowed(url: str, allowlist: str | None) -> bool:
    if not allowlist:
        return False
    parsed_url = urlparse(url)
    host = parsed_url.hostname.lower() if parsed_url.hostname else ""
    netloc = parsed_url.netloc.lower()
    entries = [item.strip() for item in allowlist.split(",") if item.strip()]
    for entry in entries:
        if entry.startswith("http://") or entry.startswith("https://"):
            parsed = urlparse(entry)
            if parsed.scheme and parsed.netloc.lower() == netloc:
                return True
        else:
            entry_lower = entry.lower()
            if entry_lower == netloc or entry_lower == host:
                return True
    return False


def _is_private_host(host: str | None) -> bool:
    if not host:
        return False
    lowered = host.lower()
    if lowered in {"localhost"} or lowered.endswith(".local"):
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_multicast
    )


def _build_request_metadata(params: Any) -> Dict[str, Any]:
    if not params:
        return {}
    if isinstance(params, list):
        if params and isinstance(params[0], dict):
            params = params[0]
        else:
            return {}
    if not isinstance(params, dict):
        return {}
    metadata: Dict[str, Any] = {}
    for key, value in params.items():
        camel = _to_camel_case(str(key))
        if not camel:
            continue
        metadata[f"request{camel}"] = value
    return metadata


def _apply_request_metadata(records: List[Dict[str, Any]], params: Any) -> None:
    metadata = _build_request_metadata(params)
    if not metadata:
        return
    for record in records:
        if not isinstance(record, dict):
            continue
        for key, value in metadata.items():
            record.setdefault(key, value)


def _extract_paging_config(payload: Any) -> Dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    paging = payload.get("paging")
    if isinstance(paging, dict):
        limit = paging.get("limit")
        offset = paging.get("offset", 0)
        try:
            limit_val = int(limit)
            offset_val = int(offset)
        except (TypeError, ValueError):
            limit_val = 0
            offset_val = 0
        if limit_val > 0:
            return {"mode": "offset", "limit": limit_val, "offset": offset_val}
    cursor = payload.get("cursor")
    if isinstance(cursor, dict):
        field = cursor.get("field")
        if field:
            return {
                "mode": "cursor",
                "field": str(field),
                "value": cursor.get("value"),
            }
    return None


def _update_paging_payload(payload: Dict[str, Any], config: Dict[str, Any], value: Any) -> Dict[str, Any]:
    updated = dict(payload)
    if config.get("mode") == "offset":
        updated["paging"] = {
            "limit": config.get("limit"),
            "offset": value,
        }
    else:
        updated["cursor"] = {
            "field": config.get("field"),
            "value": value,
        }
    return updated


def _extract_records(data: Any, full_url: str) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        result = data.get("result") or data.get("data") or data
        if isinstance(result, dict):
            records = result.get("records")
            if isinstance(records, list):
                print(f"[load_records] URL={full_url}, records={len(records)}")
                return records
        if isinstance(result, list):
            print(f"[load_records] URL={full_url}, records={len(result)}")
            return result
        records = data.get("records")
        if isinstance(records, list):
            print(f"[load_records] URL={full_url}, records={len(records)}")
            return records

    if isinstance(data, list):
        print(f"[load_records] URL={full_url}, records={len(data)}")
        return data

    print(f"[load_records] URL={full_url}, records=0")
    return []


def _prepare_request(
    method: str,
    headers: Dict[str, Any],
    payload: RequestPayload,
    full_url: str,
) -> tuple[str, Dict[str, Any], Dict[str, Any] | None, Any | None]:
    json_body = payload.body if isinstance(payload.body, (dict, list)) else None
    params = payload.body if (method == "GET" and isinstance(payload.body, dict)) else None
    request_method = method
    request_headers = dict(headers)
    if method == "GET" and json_body is not None:
        request_method = "POST"
        params = None
        request_headers.setdefault("X-HTTP-Method-Override", "GET")
        request_headers.setdefault("Content-Type", "application/json")
        logger.info("Using method override for GET with body", extra={"url": full_url})
    return request_method, request_headers, params, json_body if request_method != "GET" else None


async def _async_fetch_records(
    client: httpx.AsyncClient,
    method: str,
    headers: Dict[str, Any],
    payload: RequestPayload,
    full_url: str,
    *,
    is_mock: bool,
    remote_source: RemoteSource,
) -> List[Dict[str, Any]]:
    if is_mock:
        return _build_mock_records(remote_source, Filters())
    request_method, request_headers, params, json_body = _prepare_request(
        method,
        headers,
        payload,
        full_url,
    )
    try:
        data, _status_code = await async_request_json(
            client,
            request_method,
            full_url,
            headers=request_headers,
            params=params,
            json_body=json_body,
        )
    except UpstreamHTTPError as exc:
        logger.warning(
            "Upstream HTTP error",
            extra={"url": full_url, "status_code": exc.status_code},
        )
        raise
    return _extract_records(data, full_url)


async def _async_fetch_with_pushdown_retry(
    client: httpx.AsyncClient,
    method: str,
    headers: Dict[str, Any],
    request_payload: RequestPayload,
    *,
    base_payload: RequestPayload,
    full_url: str,
    is_mock: bool,
    remote_source: RemoteSource,
    pushdown_attempted: bool,
    pushdown_applied: bool,
    pushdown_result: str,
) -> List[Dict[str, Any]]:
    try:
        records = await _async_fetch_records(
            client,
            method,
            headers,
            request_payload,
            full_url,
            is_mock=is_mock,
            remote_source=remote_source,
        )
        if pushdown_attempted:
            record_pushdown_request(True, pushdown_result)
        return records
    except UpstreamHTTPError as exc:
        if pushdown_attempted and pushdown_applied:
            logger.warning(
                "pushdown_failed_fallback",
                extra={"url": full_url, "status_code": exc.status_code},
            )
            record_pushdown_request(True, "failed_fallback")
            try:
                records = await _async_fetch_records(
                    client,
                    method,
                    headers,
                    base_payload,
                    full_url,
                    is_mock=is_mock,
                    remote_source=remote_source,
                )
                record_pushdown_request(True, "retry_succeeded")
                return records
            except UpstreamHTTPError:
                record_pushdown_request(True, "retry_failed")
                raise
        raise


def _looks_like_request_payload(candidate: Any) -> bool:
    if not isinstance(candidate, dict):
        return False
    return any(key in candidate for key in ("method", "params", "requests"))


def _looks_like_batch_results(candidate: Any) -> bool:
    if not isinstance(candidate, list) or not candidate:
        return False
    return all(
        isinstance(item, dict) and ("ok" in item) and ("data" in item)
        for item in candidate
    )


def _load_results_file(path_value: str) -> Any | None:
    if not path_value:
        return None
    path = path_value
    if not os.path.isabs(path):
        path = os.path.join(os.getcwd(), path)
    base_dir = os.path.abspath(_RESULTS_DIR)
    target = os.path.abspath(path)
    if target != base_dir and not target.startswith(base_dir + os.sep):
        return None
    try:
        with open(target, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def _extract_batch_records(results: List[Any], label: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        if not item.get("ok"):
            continue
        data = item.get("data")
        if data is None:
            continue
        extracted = _extract_records(data, label)
        params = item.get("params") if isinstance(item.get("params"), dict) else None
        if params:
            _apply_request_metadata(extracted, params)
        records.extend(extracted)
    return records


def _extract_local_records(remote_source: RemoteSource) -> tuple[bool, List[Dict[str, Any]]]:
    candidates: List[Any] = []
    body = normalize_remote_body(remote_source)
    if isinstance(body, (dict, list)):
        candidates.append(body)
    if isinstance(remote_source.remoteMeta, dict):
        candidates.append(remote_source.remoteMeta)

    for candidate in candidates:
        if isinstance(candidate, dict):
            results_file_ref = candidate.get("resultsFileRef") or candidate.get("results_file_ref")
            if isinstance(results_file_ref, str):
                file_payload = _load_results_file(results_file_ref)
                if file_payload is not None:
                    found, records = _extract_local_records_from_payload(file_payload)
                    if found:
                        return True, records

            results = candidate.get("results")
            if isinstance(results, list):
                return True, _extract_batch_records(results, "batch")

            if "records" in candidate:
                return True, _extract_records(candidate, "local")
            if ("result" in candidate or "data" in candidate) and not _looks_like_request_payload(candidate):
                return True, _extract_records(candidate, "local")

        if _looks_like_batch_results(candidate):
            return True, _extract_batch_records(candidate, "batch")

    return False, []


def _extract_local_records_from_payload(payload: Any) -> tuple[bool, List[Dict[str, Any]]]:
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            return True, _extract_batch_records(results, "batch")
        if "records" in payload or "result" in payload or "data" in payload:
            return True, _extract_records(payload, "local")
    if _looks_like_batch_results(payload):
        return True, _extract_batch_records(payload, "batch")
    return False, []


def _resolve_full_url(url: str, base_url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        allowlist = _get_remote_allowlist()
        if not _is_full_url_allowed(url, allowlist):
            parsed = urlparse(url)
            host = parsed.hostname
            if not allowlist and _is_private_host(host):
                raise ValueError("remoteSource.url points to a private host; allowlist is required")
            raise ValueError("remoteSource.url is not allowed")
        return url
    return build_full_url(base_url, url)


def _enforce_records_limit(records: List[Dict[str, Any]], limit: int | None) -> None:
    if limit is None:
        return
    if len(records) > limit:
        raise ValueError(f"Records limit exceeded: {len(records)} > {limit}")


def _iter_chunks(records: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    size = chunk_size if chunk_size > 0 else len(records)
    if size <= 0:
        return []
    for idx in range(0, len(records), size):
        yield records[idx : idx + size]


def load_records(remote_source: RemoteSource) -> List[Dict[str, Any]]:
    """
    Загружает сырые записи из удалённого источника,
    используя поля remoteSource (url, method, body, headers).

    Ожидаемый формат ответа такой же, как в Service360:

        {
          "result": {
            "records": [ ... ]
          }
        }

    или, как fallback:

        [ ... ]  # если API сразу возвращает список записей
    """

    local_found, local_records = _extract_local_records(remote_source)
    if local_found:
        _enforce_records_limit(local_records, get_records_limit())
        return local_records

    # 1. Базовые поля источника
    method = (remote_source.method or "POST").upper()
    url = (remote_source.url or "").strip()
    base_url = SERVICE360_BASE_URL.rstrip("/")

    if not url:
        return []
    is_mock = url.startswith("mock://")
    if is_mock:
        full_url = url
    else:
        full_url = _resolve_full_url(url, base_url)
    headers = remote_source.headers or {}

    # 2. Формируем тело запроса
    body = normalize_remote_body(remote_source)
    request_payloads = build_request_payloads(body)
    if not request_payloads:
        return []

    records_all: List[Dict[str, Any]] = []
    start = time.monotonic()
    for payload in request_payloads:
        if is_mock:
            records = _build_mock_records(remote_source, Filters())
        else:
            request_method, request_headers, params, json_body = _prepare_request(
                method,
                headers,
                payload,
                full_url,
            )
            data = request_json(
                request_method,
                full_url,
                headers=request_headers,
                params=params,
                json_body=json_body,
                timeout=30.0,
            )
            records = _extract_records(data, full_url)

        _apply_request_metadata(records, payload.params)
        records_all.extend(records)

    if is_mock:
        print(f"[load_records] URL={full_url}, records={len(records_all)}")
    _enforce_records_limit(records_all, get_records_limit())
    duration_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "load_records",
        extra={"url": full_url, "records": len(records_all), "duration_ms": duration_ms},
    )
    return records_all


async def _async_load_records_with_client(
    remote_source: RemoteSource,
    client: httpx.AsyncClient,
    payload_filters: Filters | Dict[str, Any] | None = None,
    stats: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    local_found, local_records = _extract_local_records(remote_source)
    if local_found:
        _enforce_records_limit(local_records, get_records_limit())
        return local_records

    method = (remote_source.method or "POST").upper()
    url = (remote_source.url or "").strip()
    base_url = SERVICE360_BASE_URL.rstrip("/")

    if not url:
        return []
    is_mock = url.startswith("mock://")
    if is_mock:
        full_url = url
    else:
        full_url = _resolve_full_url(url, base_url)
    headers = remote_source.headers or {}

    body = normalize_remote_body(remote_source)
    request_payloads = build_request_payloads(body)
    if not request_payloads:
        return []

    if stats is None:
        stats = {}
    stats.setdefault("pushdown_enabled", False)
    stats.setdefault("pushdown_filters_applied", 0)
    stats.setdefault("pushdown_paging_applied", False)

    pushdown_cfg, pushdown_active, pushdown_reason, pushdown_result = _get_pushdown_state(
        full_url,
        remote_source,
    )
    pushdown_safe_only = _get_pushdown_safe_only()
    pushdown_max_filters = _get_pushdown_max_filters()
    pushdown_max_in_values = _get_pushdown_max_in_values()

    records_all: List[Dict[str, Any]] = []
    start = time.monotonic()
    for payload in request_payloads:
        request_payload = payload
        pushdown_attempted = False
        pushdown_applied = False
        if pushdown_active and pushdown_cfg:
            try:
                request_body, applied_filters, paging_applied = build_body_with_pushdown(
                    payload.body,
                    payload_filters,
                    pushdown_cfg,
                    None,
                    max_filters=pushdown_max_filters,
                    max_in_values=pushdown_max_in_values,
                    safe_only=pushdown_safe_only,
                )
                request_payload = RequestPayload(body=request_body, params=payload.params)
                pushdown_attempted = True
                pushdown_applied = applied_filters > 0 or paging_applied
                stats["pushdown_enabled"] = True
                stats["pushdown_filters_applied"] = applied_filters
                stats["pushdown_paging_applied"] = paging_applied
            except Exception as exc:
                logger.warning(
                    "pushdown_failed_fallback",
                    extra={"url": full_url, "error": str(exc)},
                )
                record_pushdown_request(True, "fallback_error")
                request_payload = payload
        else:
            record_pushdown_request(False, pushdown_reason)
        records = await _async_fetch_with_pushdown_retry(
            client,
            method,
            headers,
            request_payload,
            base_payload=payload,
            full_url=full_url,
            is_mock=is_mock,
            remote_source=remote_source,
            pushdown_attempted=pushdown_attempted,
            pushdown_applied=pushdown_applied,
            pushdown_result=pushdown_result,
        )

        _apply_request_metadata(records, payload.params)
        records_all.extend(records)

    if is_mock:
        print(f"[load_records] URL={full_url}, records={len(records_all)}")
    _enforce_records_limit(records_all, get_records_limit())
    duration_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "async_load_records",
        extra={"url": full_url, "records": len(records_all), "duration_ms": duration_ms},
    )
    return records_all


async def _async_iter_records_with_client(
    remote_source: RemoteSource,
    client: httpx.AsyncClient,
    chunk_size: int,
    *,
    payload_filters: Filters | Dict[str, Any] | None = None,
    paging_allowlist: str | None = None,
    paging_max_pages: int | None = None,
    paging_force: bool = False,
    stats: Dict[str, Any] | None = None,
) -> AsyncIterator[List[Dict[str, Any]]]:
    if stats is None:
        stats = {}
    local_found, local_records = _extract_local_records(remote_source)
    if local_found:
        _enforce_records_limit(local_records, get_records_limit())
        for chunk in _iter_chunks(local_records, chunk_size):
            yield chunk
        return

    method = (remote_source.method or "POST").upper()
    url = (remote_source.url or "").strip()
    base_url = SERVICE360_BASE_URL.rstrip("/")

    if not url:
        return
    is_mock = url.startswith("mock://")
    if is_mock:
        full_url = url
    else:
        full_url = _resolve_full_url(url, base_url)
    headers = remote_source.headers or {}

    body = normalize_remote_body(remote_source)
    request_payloads = build_request_payloads(body)
    if not request_payloads:
        return

    stats.setdefault("pushdown_enabled", False)
    stats.setdefault("pushdown_filters_applied", 0)
    stats.setdefault("pushdown_paging_applied", False)

    pushdown_cfg, pushdown_active, pushdown_reason, pushdown_result = _get_pushdown_state(
        full_url,
        remote_source,
    )
    pushdown_safe_only = _get_pushdown_safe_only()
    pushdown_max_filters = _get_pushdown_max_filters()
    pushdown_max_in_values = _get_pushdown_max_in_values()

    total_records = 0
    paging_enabled = False
    paging_pages = 0
    paging_allow = paging_allowlist or _get_paging_allowlist()
    paging_max_pages = paging_max_pages or _get_paging_max_pages()
    paging_force = paging_force or _get_upstream_paging_enabled()
    paging_allowed_for_host = paging_force or _is_host_allowed(full_url, paging_allow)
    start = time.monotonic()
    for payload in request_payloads:
        paging_config = _extract_paging_config(payload.body)
        paging_pushdown_allowed = bool(
            pushdown_active and pushdown_cfg and pushdown_cfg.paging and paging_allowed_for_host
        )
        if paging_config is None and paging_pushdown_allowed:
            paging_config = {"mode": "offset", "limit": chunk_size, "offset": 0}
        if paging_config and paging_allowed_for_host:
            paging_enabled = True
            page_value = paging_config.get("offset", 0)
            for _page in range(paging_max_pages):
                paging_pages += 1
                request_body = _update_paging_payload(payload.body, paging_config, page_value)
                page_payload = RequestPayload(body=request_body, params=payload.params)
                request_payload = page_payload
                pushdown_attempted = False
                pushdown_applied = False
                if pushdown_active and pushdown_cfg:
                    page_state = None
                    if paging_config.get("mode") == "offset":
                        page_state = {
                            "limit": paging_config.get("limit"),
                            "offset": page_value,
                        }
                    try:
                        request_body, applied_filters, paging_applied = build_body_with_pushdown(
                            page_payload.body,
                            payload_filters,
                            pushdown_cfg,
                            page_state,
                            max_filters=pushdown_max_filters,
                            max_in_values=pushdown_max_in_values,
                            safe_only=pushdown_safe_only,
                        )
                        request_payload = RequestPayload(body=request_body, params=page_payload.params)
                        pushdown_attempted = True
                        pushdown_applied = applied_filters > 0 or paging_applied
                        stats["pushdown_enabled"] = True
                        stats["pushdown_filters_applied"] = applied_filters
                        stats["pushdown_paging_applied"] = paging_applied
                    except Exception as exc:
                        logger.warning(
                            "pushdown_failed_fallback",
                            extra={"url": full_url, "error": str(exc)},
                        )
                        record_pushdown_request(True, "fallback_error")
                        request_payload = page_payload
                else:
                    record_pushdown_request(False, pushdown_reason)
                records = await _async_fetch_with_pushdown_retry(
                    client,
                    method,
                    headers,
                    request_payload,
                    base_payload=page_payload,
                    full_url=full_url,
                    is_mock=is_mock,
                    remote_source=remote_source,
                    pushdown_attempted=pushdown_attempted,
                    pushdown_applied=pushdown_applied,
                    pushdown_result=pushdown_result,
                )

                if not records:
                    break

                _apply_request_metadata(records, request_payload.params)
                total_records += len(records)
                for chunk in _iter_chunks(records, chunk_size):
                    yield chunk

                if paging_config.get("mode") == "offset":
                    page_value += paging_config.get("limit", 0)
                else:
                    field = paging_config.get("field")
                    if not field:
                        break
                    cursor_value = records[-1].get(field)
                    if cursor_value is None:
                        break
                    page_value = cursor_value
            else:
                raise ValueError(f"Paging max pages exceeded: {paging_max_pages}")
            continue

        request_payload = payload
        pushdown_attempted = False
        pushdown_applied = False
        if pushdown_active and pushdown_cfg:
            try:
                request_body, applied_filters, paging_applied = build_body_with_pushdown(
                    payload.body,
                    payload_filters,
                    pushdown_cfg,
                    None,
                    max_filters=pushdown_max_filters,
                    max_in_values=pushdown_max_in_values,
                    safe_only=pushdown_safe_only,
                )
                request_payload = RequestPayload(body=request_body, params=payload.params)
                pushdown_attempted = True
                pushdown_applied = applied_filters > 0 or paging_applied
                stats["pushdown_enabled"] = True
                stats["pushdown_filters_applied"] = applied_filters
                stats["pushdown_paging_applied"] = paging_applied
            except Exception as exc:
                logger.warning(
                    "pushdown_failed_fallback",
                    extra={"url": full_url, "error": str(exc)},
                )
                record_pushdown_request(True, "fallback_error")
                request_payload = payload
        else:
            record_pushdown_request(False, pushdown_reason)
        records = await _async_fetch_with_pushdown_retry(
            client,
            method,
            headers,
            request_payload,
            base_payload=payload,
            full_url=full_url,
            is_mock=is_mock,
            remote_source=remote_source,
            pushdown_attempted=pushdown_attempted,
            pushdown_applied=pushdown_applied,
            pushdown_result=pushdown_result,
        )

        _apply_request_metadata(records, request_payload.params)
        total_records += len(records)
        for chunk in _iter_chunks(records, chunk_size):
            yield chunk

    if is_mock:
        print(f"[load_records] URL={full_url}, records={total_records}")
    duration_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "async_iter_records",
        extra={
            "url": full_url,
            "records": total_records,
            "duration_ms": duration_ms,
            "paging_enabled": paging_enabled,
            "paging_pages": paging_pages,
        },
    )
    stats["paging_enabled"] = paging_enabled
    stats["paging_pages"] = paging_pages
    stats["records"] = total_records


async def async_load_records(
    remote_source: RemoteSource,
    client: httpx.AsyncClient | None = None,
    *,
    timeout: float = 30.0,
    payload_filters: Filters | Dict[str, Any] | None = None,
    stats: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    if client is not None:
        return await _async_load_records_with_client(
            remote_source,
            client,
            payload_filters=payload_filters,
            stats=stats,
        )
    async with httpx.AsyncClient(timeout=timeout) as async_client:
        return await _async_load_records_with_client(
            remote_source,
            async_client,
            payload_filters=payload_filters,
            stats=stats,
        )


async def async_iter_records(
    remote_source: RemoteSource,
    chunk_size: int,
    client: httpx.AsyncClient | None = None,
    *,
    timeout: float = 30.0,
    payload_filters: Filters | Dict[str, Any] | None = None,
    paging_allowlist: str | None = None,
    paging_max_pages: int | None = None,
    paging_force: bool = False,
    stats: Dict[str, Any] | None = None,
) -> AsyncIterator[List[Dict[str, Any]]]:
    if client is not None:
        async for chunk in _async_iter_records_with_client(
            remote_source,
            client,
            chunk_size,
            payload_filters=payload_filters,
            paging_allowlist=paging_allowlist,
            paging_max_pages=paging_max_pages,
            paging_force=paging_force,
            stats=stats,
        ):
            yield chunk
        return
    async with httpx.AsyncClient(timeout=timeout) as async_client:
        async for chunk in _async_iter_records_with_client(
            remote_source,
            async_client,
            chunk_size,
            payload_filters=payload_filters,
            paging_allowlist=paging_allowlist,
            paging_max_pages=paging_max_pages,
            paging_force=paging_force,
            stats=stats,
        ):
            yield chunk
