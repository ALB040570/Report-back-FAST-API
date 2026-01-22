from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from app.models.filters import Filters
from app.models.remote_source import RemoteSource


class PushdownPathError(ValueError):
    pass


@dataclass(frozen=True)
class PushdownPaging:
    strategy: str
    limit_path: str
    offset_path: str


@dataclass(frozen=True)
class PushdownFilter:
    filter_key: str
    op: str
    target_path: str


@dataclass(frozen=True)
class PushdownConfig:
    enabled: bool
    mode: str
    paging: Optional[PushdownPaging]
    filters: List[PushdownFilter]


def parse_pushdown(remote_source: RemoteSource) -> Optional[PushdownConfig]:
    raw = getattr(remote_source, "pushdown", None)
    if not isinstance(raw, dict):
        return None
    if not raw.get("enabled"):
        return None
    mode = raw.get("mode")
    if mode != "jsonrpc_params":
        return None

    paging_cfg = None
    paging = raw.get("paging")
    if isinstance(paging, dict) and paging.get("strategy") == "offset":
        limit_path = paging.get("limitPath")
        offset_path = paging.get("offsetPath")
        if isinstance(limit_path, str) and isinstance(offset_path, str):
            paging_cfg = PushdownPaging(
                strategy="offset",
                limit_path=limit_path,
                offset_path=offset_path,
            )

    filters: List[PushdownFilter] = []
    raw_filters = raw.get("filters")
    if isinstance(raw_filters, list):
        for item in raw_filters:
            if not isinstance(item, dict):
                continue
            filter_key = item.get("filterKey")
            op = item.get("op")
            target_path = item.get("targetPath")
            if not isinstance(filter_key, str) or not isinstance(target_path, str):
                continue
            if op not in {"eq", "in", "range"}:
                continue
            filters.append(PushdownFilter(filter_key=filter_key, op=op, target_path=target_path))

    return PushdownConfig(enabled=True, mode=mode, paging=paging_cfg, filters=filters)


def host_allowed(url: str, allowlist: Optional[str]) -> bool:
    if not allowlist:
        return False
    target = urlparse(url)
    target_host = target.netloc.lower()
    for entry in [item.strip() for item in allowlist.split(",") if item.strip()]:
        if entry.startswith("http://") or entry.startswith("https://"):
            parsed = urlparse(entry)
            if parsed.netloc.lower() == target_host:
                return True
        else:
            if entry.lower() == target_host or entry.lower() == target.hostname:
                return True
    return False


def set_by_dot_path(obj: Any, path: str, value: Any) -> None:
    if not path:
        raise PushdownPathError("Empty path")
    parts = [part for part in path.split(".") if part]
    if not parts:
        raise PushdownPathError("Empty path")

    current = obj
    for idx, part in enumerate(parts):
        is_last = idx == len(parts) - 1
        is_index = part.isdigit()
        next_is_index = (not is_last) and parts[idx + 1].isdigit()

        if is_index:
            if not isinstance(current, list):
                raise PushdownPathError(f"Expected list at {part}")
            index = int(part)
            if index < 0:
                raise PushdownPathError(f"Negative index at {part}")
            while len(current) <= index:
                current.append(None)
            if is_last:
                current[index] = value
                return
            if current[index] is None:
                current[index] = [] if next_is_index else {}
            if not isinstance(current[index], (dict, list)):
                raise PushdownPathError(f"Invalid path segment at {part}")
            current = current[index]
        else:
            if not isinstance(current, dict):
                raise PushdownPathError(f"Expected dict at {part}")
            if is_last:
                current[part] = value
                return
            if part not in current or current[part] is None:
                current[part] = [] if next_is_index else {}
            if not isinstance(current[part], (dict, list)):
                raise PushdownPathError(f"Invalid path segment at {part}")
            current = current[part]


def _strip_body_prefix(path: str) -> str:
    if path == "body":
        return ""
    if path.startswith("body."):
        return path[5:]
    return path


def _normalize_filters(filters: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(filters, Filters):
        payload = filters.dict()
    elif isinstance(filters, dict):
        payload = filters
    else:
        return {}
    merged: Dict[str, Dict[str, Any]] = {}
    global_filters = payload.get("globalFilters")
    if isinstance(global_filters, dict):
        merged.update(global_filters)
    container_filters = payload.get("containerFilters")
    if isinstance(container_filters, dict):
        merged.update(container_filters)
    return merged


def _extract_values(values: Any) -> Optional[List[Any]]:
    if values is None:
        return None
    if isinstance(values, list):
        return values
    return [values]


def _extract_range(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        if "from" in value or "to" in value:
            return {"from": value.get("from"), "to": value.get("to")}
        if "gte" in value or "lte" in value:
            return {"from": value.get("gte"), "to": value.get("lte")}
        if "min" in value or "max" in value:
            return {"from": value.get("min"), "to": value.get("max")}
        return None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return {"from": value[0], "to": value[1]}
    return None


def build_body_with_pushdown(
    original_body: Any,
    payload_filters: Any,
    pushdown_cfg: PushdownConfig,
    page_state: Optional[Dict[str, Any]] = None,
    *,
    max_filters: int = 50,
    max_in_values: int = 200,
    safe_only: bool = True,
) -> tuple[Any, int, bool]:
    body = copy.deepcopy(original_body)
    filters_payload = _normalize_filters(payload_filters)
    applied_filters = 0
    paging_applied = False

    for entry in pushdown_cfg.filters[:max_filters]:
        filter_key = entry.filter_key
        if safe_only and "." in filter_key:
            continue
        filter_payload = filters_payload.get(filter_key)
        if not isinstance(filter_payload, dict):
            continue

        values = _extract_values(filter_payload.get("values"))
        range_value = _extract_range(filter_payload.get("range"))

        if entry.op == "eq":
            if not values:
                continue
            target_value = values[0]
        elif entry.op == "in":
            if not values:
                continue
            if len(values) > max_in_values:
                continue
            target_value = values
        elif entry.op == "range":
            if not range_value:
                continue
            target_value = range_value
        else:
            continue

        target_path = _strip_body_prefix(entry.target_path)
        if not target_path:
            raise PushdownPathError("Empty target path")
        set_by_dot_path(body, target_path, target_value)
        applied_filters += 1

    if pushdown_cfg.paging and page_state:
        limit = page_state.get("limit")
        offset = page_state.get("offset")
        limit_path = _strip_body_prefix(pushdown_cfg.paging.limit_path)
        offset_path = _strip_body_prefix(pushdown_cfg.paging.offset_path)
        if limit_path and limit is not None:
            set_by_dot_path(body, limit_path, limit)
            paging_applied = True
        if offset_path and offset is not None:
            set_by_dot_path(body, offset_path, offset)
            paging_applied = True

    return body, applied_filters, paging_applied
