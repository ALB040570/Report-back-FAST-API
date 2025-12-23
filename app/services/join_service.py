import json
from typing import Any, Dict, List, Optional, Tuple

from app.models.remote_source import RemoteSource
from app.services.data_source_client import load_records
from app.services.source_registry import get_source_config


def _normalize_join_fields(value: Any) -> Optional[List[str]]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return None


def _project_join_fields(
    record: Dict[str, Any],
    fields: Optional[List[str]],
    prefix: str,
) -> Dict[str, Any]:
    entries = fields if fields else list(record.keys())
    projected: Dict[str, Any] = {}
    for key in entries:
        target_key = f"{prefix}.{key}" if prefix else key
        projected[target_key] = record.get(key)
    return projected


def _apply_join(
    base_rows: List[Dict[str, Any]],
    join_rows: List[Dict[str, Any]],
    join: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int]:
    primary_key = join.get("primaryKey") or join.get("primary_key")
    foreign_key = join.get("foreignKey") or join.get("foreign_key")
    if not primary_key or not foreign_key:
        return base_rows, 0

    lookup: Dict[Any, List[Dict[str, Any]]] = {}
    for row in join_rows:
        key = row.get(foreign_key)
        if key is None:
            continue
        lookup.setdefault(key, []).append(row)

    join_type = "inner" if str(join.get("joinType")).lower() == "inner" else "left"
    prefix = str(join.get("resultPrefix") or "").strip()
    fields = _normalize_join_fields(join.get("fields"))

    merged: List[Dict[str, Any]] = []
    matched_rows = 0
    for row in base_rows:
        key = row.get(primary_key)
        matches = lookup.get(key)
        if not matches:
            if join_type == "inner":
                continue
            merged.append({**row})
            continue
        matched_rows += len(matches)
        for match in matches:
            merged.append({**row, **_project_join_fields(match, fields, prefix)})
    return merged, matched_rows


def _extract_joins_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    joins = payload.get("__joins")
    if isinstance(joins, list):
        return joins
    join_config = payload.get("joinConfig")
    if isinstance(join_config, list):
        return join_config
    if isinstance(join_config, str):
        try:
            parsed = json.loads(join_config)
        except (TypeError, ValueError):
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def _resolve_source_id(remote_source: RemoteSource) -> Optional[str]:
    if remote_source.id:
        return str(remote_source.id)
    if remote_source.remoteId:
        return str(remote_source.remoteId)
    if isinstance(remote_source.remoteMeta, dict) and remote_source.remoteMeta.get("id"):
        return str(remote_source.remoteMeta.get("id"))
    return None


async def _resolve_joins(remote_source: RemoteSource) -> List[Dict[str, Any]]:
    if isinstance(remote_source.joins, list) and remote_source.joins:
        return list(remote_source.joins)
    joins = _extract_joins_from_payload(remote_source.body)
    if not joins and remote_source.rawBody:
        try:
            parsed = json.loads(remote_source.rawBody)
        except (TypeError, ValueError):
            parsed = None
        joins = _extract_joins_from_payload(parsed)
    if joins:
        return joins

    source_id = _resolve_source_id(remote_source)
    if not source_id:
        return []
    config = await get_source_config(source_id)
    if not config:
        return []
    joins = _extract_joins_from_payload(config.body)
    if joins:
        return joins
    if config.raw_body:
        try:
            parsed = json.loads(config.raw_body)
        except (TypeError, ValueError):
            parsed = None
        joins = _extract_joins_from_payload(parsed)
    return joins


async def resolve_joins(remote_source: RemoteSource) -> List[Dict[str, Any]]:
    return await _resolve_joins(remote_source)


async def apply_joins(
    base_rows: List[Dict[str, Any]],
    remote_source: RemoteSource,
    joins_override: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    joins = joins_override if joins_override is not None else await _resolve_joins(remote_source)
    debug: Dict[str, Any] = {
        "joinsApplied": [],
        "sampleKeys": {
            "beforeJoin": list(base_rows[0].keys()) if base_rows else [],
            "afterJoin": [],
        },
    }
    if not joins:
        return base_rows, debug

    rows = base_rows
    for join in joins:
        target_source_id = join.get("targetSourceId")
        join_rows: List[Dict[str, Any]] = []
        if target_source_id:
            config = await get_source_config(target_source_id)
            if config:
                join_source = RemoteSource(
                    id=str(target_source_id),
                    method=config.method,
                    url=config.url,
                    body=config.body,
                    headers=config.headers,
                    rawBody=config.raw_body,
                )
                join_rows = load_records(join_source)

        base_before = len(rows)
        rows, matched_rows = _apply_join(rows, join_rows, join)
        debug["joinsApplied"].append(
            {
                "joinId": join.get("id"),
                "targetSourceId": target_source_id,
                "baseBefore": base_before,
                "baseAfter": len(rows),
                "matchedRows": matched_rows,
            }
        )

    debug["sampleKeys"]["afterJoin"] = list(rows[0].keys()) if rows else []
    return rows, debug
