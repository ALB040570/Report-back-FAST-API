from typing import Any, Dict, List, Tuple

from app.models.filters import Filters
from app.models.snapshot import Snapshot
from app.services.filter_service import (
    apply_filters,
    _normalize_filter_value,
    _resolve_field_label,
    _resolve_meta_type,
    _resolve_record_value,
)


def _snapshot_to_dict(snapshot: Snapshot | Dict[str, Any]) -> Dict[str, Any]:
    if hasattr(snapshot, "model_dump"):
        return snapshot.model_dump()
    if hasattr(snapshot, "dict"):
        return snapshot.dict()
    return snapshot


def _unique_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _parse_dimension_key(key: str | None) -> Tuple[List[str], List[str]]:
    if not key or not isinstance(key, str):
        return [], []
    base = key.split("::", 1)[0]
    if not base:
        return [], []
    fields: List[str] = []
    values: List[str] = []
    for segment in base.split("|"):
        if ":" not in segment:
            continue
        field, value = segment.split(":", 1)
        if not field:
            continue
        fields.append(field)
        values.append(value)
    return fields, values


def _normalize_constraint_value(value: Any) -> str:
    return _normalize_filter_value(value)


def _matches_constraints(
    record: Dict[str, Any],
    fields: List[str],
    values: List[Any],
) -> bool:
    if not fields or not values:
        return True
    for field, expected in zip(fields, values):
        record_value = _resolve_record_value(record, field)
        if _normalize_constraint_value(record_value) != _normalize_constraint_value(expected):
            return False
    return True


def _resolve_cell_constraints(payload: Dict[str, Any]) -> Dict[str, List[Any]]:
    cell = payload.get("cell") if isinstance(payload.get("cell"), dict) else {}
    row_fields = cell.get("rowFields") or []
    row_values = cell.get("rowValues") or []
    column_fields = cell.get("columnFields") or []
    column_values = cell.get("columnValues") or []

    if row_fields and row_values:
        return {
            "rowFields": list(row_fields),
            "rowValues": list(row_values),
            "columnFields": list(column_fields),
            "columnValues": list(column_values),
        }

    row_key_fields, row_key_values = _parse_dimension_key(payload.get("rowKey"))
    col_key_fields, col_key_values = _parse_dimension_key(payload.get("columnKey"))
    return {
        "rowFields": row_key_fields,
        "rowValues": row_key_values,
        "columnFields": col_key_fields,
        "columnValues": col_key_values,
    }


def _build_detail_fields(
    snapshot_dict: Dict[str, Any],
    metric: Dict[str, Any] | None,
    detail_fields: List[str] | None,
) -> List[str]:
    if detail_fields:
        return _unique_preserve_order([str(item) for item in detail_fields if str(item)])

    pivot = snapshot_dict.get("pivot") or {}
    rows = pivot.get("rows") or []
    columns = pivot.get("columns") or []
    metric_field = None
    if isinstance(metric, dict):
        metric_field = metric.get("fieldKey") or metric.get("field") or metric.get("sourceKey")
    fields = [str(item) for item in rows + columns if str(item)]
    if metric_field:
        fields.append(str(metric_field))
    return _unique_preserve_order(fields)


def _build_field_meta(
    fields: List[str],
    entries: List[Dict[str, Any]],
    snapshot_dict: Dict[str, Any],
) -> List[Dict[str, Any]]:
    header_overrides = (snapshot_dict.get("options") or {}).get("headerOverrides") or {}
    filters_meta = snapshot_dict.get("filtersMeta") or []
    field_meta = snapshot_dict.get("fieldMeta") or {}

    result: List[Dict[str, Any]] = []
    for field in fields:
        values = [entry.get(field) for entry in entries]
        field_type, _ = _resolve_meta_type(field_meta, field, values)
        label = _resolve_field_label(field, header_overrides, field_meta, filters_meta)
        result.append(
            {
                "key": field,
                "label": label,
                "type": field_type,
            }
        )
    return result


def build_details(
    records: List[Dict[str, Any]],
    snapshot: Snapshot | Dict[str, Any],
    filters: Filters | Dict[str, Any] | None,
    payload: Dict[str, Any],
    limit: int = 200,
    offset: int = 0,
    debug: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    snapshot_dict = _snapshot_to_dict(snapshot)
    cell_constraints = _resolve_cell_constraints(payload)
    metric = payload.get("metric") if isinstance(payload.get("metric"), dict) else None
    detail_fields = payload.get("detailFields") if isinstance(payload.get("detailFields"), list) else None

    filtered_records, filter_debug = apply_filters(records, snapshot, filters)

    row_fields = cell_constraints.get("rowFields") or []
    row_values = cell_constraints.get("rowValues") or []
    column_fields = cell_constraints.get("columnFields") or []
    column_values = cell_constraints.get("columnValues") or []

    constrained_records = [
        record
        for record in filtered_records
        if _matches_constraints(record, row_fields, row_values)
        and _matches_constraints(record, column_fields, column_values)
    ]

    total = len(constrained_records)
    if offset < 0:
        offset = 0
    if limit <= 0:
        limit = 200
    paged = constrained_records[offset : offset + limit]

    fields = _build_detail_fields(snapshot_dict, metric, detail_fields)
    entries = [
        {field: _resolve_record_value(record, field) for field in fields}
        for record in paged
    ]

    response = {
        "total": total,
        "limit": limit,
        "offset": offset,
        "fields": _build_field_meta(fields, entries, snapshot_dict),
        "entries": entries,
    }

    debug_payload: Dict[str, Any] = {}
    if debug:
        effective_filters = filter_debug.get("effectiveFilters", {})
        values_map = effective_filters.get("values", {}) or {}
        ranges_map = effective_filters.get("ranges", {}) or {}
        selected_keys = [
            key
            for key, selection in values_map.items()
            if isinstance(selection, dict) and selection.get("items")
        ]
        range_keys = [
            key
            for key, selection in ranges_map.items()
            if isinstance(selection, dict)
            and (selection.get("start") is not None or selection.get("end") is not None)
        ]
        debug_payload = {
            "recordsBeforeFilter": len(records),
            "recordsAfterEffectiveFilters": len(filtered_records),
            "recordsAfterCellFilter": total,
            "effectiveFiltersKeysSummary": {
                "values": selected_keys,
                "ranges": range_keys,
            },
            "cellConstraints": cell_constraints,
        }
    return response, debug_payload
