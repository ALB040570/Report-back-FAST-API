import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List

import json
from app.services.upstream_client import build_full_url, request_json


from app.models.filters import Filters
from app.models.remote_source import RemoteSource

SERVICE360_BASE_URL = os.getenv("SERVICE360_BASE_URL", "http://45.8.116.32")
logger = logging.getLogger(__name__)

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

    requests = body.get("requests")
    if isinstance(requests, list):
        return _build_request_payloads_from_requests(body, requests)

    params_list = body.get("params")
    if isinstance(params_list, list):
        if any(not isinstance(entry, dict) for entry in params_list):
            return [RequestPayload(body=body, params=None)]
        return _build_request_payloads_from_params(body, params_list)

    params = body.get("params")
    request_params = params if isinstance(params, dict) else None
    return [RequestPayload(body=body, params=request_params)]


def _build_request_payloads_from_params(
    body: Dict[str, Any],
    params_list: List[Any],
) -> List[RequestPayload]:
    if not params_list:
        return []
    base_body = {**body}
    base_body.pop("params", None)
    base_body.pop("requests", None)

    payloads: List[RequestPayload] = []
    for entry in params_list:
        if not isinstance(entry, dict):
            continue
        request_body = {**base_body, "params": entry}
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


def _build_request_metadata(params: Dict[str, Any] | None) -> Dict[str, Any]:
    if not params:
        return {}
    metadata: Dict[str, Any] = {}
    for key, value in params.items():
        camel = _to_camel_case(str(key))
        if not camel:
            continue
        metadata[f"request{camel}"] = value
    return metadata


def _apply_request_metadata(records: List[Dict[str, Any]], params: Dict[str, Any] | None) -> None:
    metadata = _build_request_metadata(params)
    if not metadata:
        return
    for record in records:
        if not isinstance(record, dict):
            continue
        for key, value in metadata.items():
            record.setdefault(key, value)


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
        full_url = build_full_url(base_url, url)
    headers = remote_source.headers or {}

    # 2. Формируем тело запроса
    body = normalize_remote_body(remote_source)
    request_payloads = build_request_payloads(body)
    if not request_payloads:
        return []

    records_all: List[Dict[str, Any]] = []
    for payload in request_payloads:
        if is_mock:
            records = _build_mock_records(remote_source, Filters())
        else:
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
            data = request_json(
                request_method,
                full_url,
                headers=request_headers,
                params=params,
                json_body=json_body if request_method != "GET" else None,
                timeout=30.0,
            )
            records = _extract_records(data, full_url)

        _apply_request_metadata(records, payload.params)
        records_all.extend(records)

    if is_mock:
        print(f"[load_records] URL={full_url}, records={len(records_all)}")
    return records_all
