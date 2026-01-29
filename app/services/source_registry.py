import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.services.upstream_client import build_full_url, request_json

SERVICE360_BASE_URL = os.getenv("SERVICE360_BASE_URL", "http://77.245.107.213")
_SOURCE_CACHE_TTL = 300.0
_SOURCE_CACHE: Dict[str, Any] = {"loaded_at": 0.0, "records": []}


@dataclass
class SourceConfig:
    source_id: str
    url: str
    method: str
    body: Any
    raw_body: Optional[str]
    headers: Dict[str, Any]


def _parse_method_body(raw_body: Any) -> tuple[Any, Optional[str]]:
    if raw_body is None:
        return {}, None
    if isinstance(raw_body, (dict, list)):
        return raw_body, None
    if isinstance(raw_body, str):
        trimmed = raw_body.strip()
        if not trimmed:
            return {}, raw_body
        try:
            parsed = json.loads(trimmed)
            if isinstance(parsed, dict):
                parsed = {**parsed}
                parsed.pop("__joins", None)
            return parsed, raw_body
        except (TypeError, ValueError):
            return raw_body, raw_body
    return raw_body, None


def _fetch_source_records() -> List[Dict[str, Any]]:
    url = build_full_url(SERVICE360_BASE_URL, "/dtj/api/report")
    payload = {"method": "report/loadReportSource", "params": [0]}
    data = request_json("POST", url, json_body=payload, timeout=30.0)
    if isinstance(data, dict):
        result = data.get("result") or data.get("data") or data
        if isinstance(result, dict):
            records = result.get("records")
            if isinstance(records, list):
                return records
    return []


def _get_source_records_cached() -> List[Dict[str, Any]]:
    now = time.time()
    if _SOURCE_CACHE["records"] and (now - _SOURCE_CACHE["loaded_at"] < _SOURCE_CACHE_TTL):
        return _SOURCE_CACHE["records"]
    records = _fetch_source_records()
    _SOURCE_CACHE["records"] = records
    _SOURCE_CACHE["loaded_at"] = now
    return records


async def get_source_config(source_id: str | int) -> Optional[SourceConfig]:
    records = _get_source_records_cached()
    target = str(source_id)
    record = next(
        (item for item in records if str(item.get("id")) == target),
        None,
    )
    if not record:
        return None
    url = record.get("URL") or record.get("url") or ""
    method = (record.get("Method") or record.get("method") or "POST").upper()
    raw_body = record.get("MethodBody") or record.get("methodBody") or record.get("body")
    body, raw_body_str = _parse_method_body(raw_body)
    headers = {"Content-Type": "application/json"}
    return SourceConfig(
        source_id=target,
        url=url,
        method=method,
        body=body,
        raw_body=raw_body_str,
        headers=headers,
    )
