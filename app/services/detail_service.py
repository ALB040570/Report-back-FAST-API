from typing import Any, Dict, List, Tuple

from app.models.filters import Filters
from app.models.snapshot import Snapshot
from app.services.date_utils import parse_date_input
from app.services.filter_service import (
    apply_filters,
    _is_date_type,
    _normalize_filter_value,
    _resolve_field_label,
    _resolve_meta_type,
    _resolve_record_value,
    _to_ms,
    _to_number,
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


def _normalize_detail_metric_filters(raw: Any) -> List[Dict[str, Any]]:
    if not raw:
        return []
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _normalize_detail_metric_op(op: Any) -> str | None:
    if op is None:
        return None
    normalized = str(op).strip().lower()
    mapping = {
        "=": "eq",
        "==": "eq",
        "eq": "eq",
        "!=": "neq",
        "<>": "neq",
        "ne": "neq",
        "neq": "neq",
        "<": "lt",
        "lt": "lt",
        "<=": "lte",
        "lte": "lte",
        ">": "gt",
        "gt": "gt",
        ">=": "gte",
        "gte": "gte",
        "in": "in",
        "not in": "nin",
        "nin": "nin",
        "contains": "contains",
        "startswith": "starts_with",
        "starts_with": "starts_with",
        "endswith": "ends_with",
        "ends_with": "ends_with",
    }
    return mapping.get(normalized)


def _is_date_like(value: Any) -> bool:
    return isinstance(value, str) and parse_date_input(value) is not None


def _values_equal(
    record_value: Any,
    filter_value: Any,
    field_meta: Dict[str, Any],
    key: str,
) -> bool:
    left_num = _to_number(record_value)
    right_num = _to_number(filter_value)
    if left_num is not None and right_num is not None:
        return left_num == right_num
    is_date = _is_date_type(field_meta, key)
    if not is_date:
        is_date = _is_date_like(record_value) or _is_date_like(filter_value)
    if is_date:
        left_ms = _to_ms(record_value)
        right_ms = _to_ms(filter_value)
        if left_ms is not None and right_ms is not None:
            return left_ms == right_ms
    return _normalize_filter_value(record_value) == _normalize_filter_value(filter_value)


def _coerce_ordered_values(
    record_value: Any,
    filter_value: Any,
    field_meta: Dict[str, Any],
    key: str,
) -> tuple[Any | None, Any | None, str]:
    is_date = _is_date_type(field_meta, key)
    if not is_date:
        is_date = _is_date_like(record_value) or _is_date_like(filter_value)
    if is_date:
        return _to_ms(record_value), _to_ms(filter_value), "date"
    left_num = _to_number(record_value)
    right_num = _to_number(filter_value)
    if left_num is not None and right_num is not None:
        return left_num, right_num, "number"
    return str(record_value), str(filter_value), "string"


def _record_passes_detail_metric_filters(
    record: Dict[str, Any],
    filters: List[Dict[str, Any]],
    field_meta: Dict[str, Any],
) -> bool:
    if not filters:
        return True
    for entry in filters:
        field_key = entry.get("fieldKey") or entry.get("field") or entry.get("key")
        if not field_key:
            continue
        op = _normalize_detail_metric_op(entry.get("op") or entry.get("operator"))
        if not op:
            continue
        target = entry.get("value") if "value" in entry else entry.get("values")
        record_value = _resolve_record_value(record, str(field_key))

        if op in {"in", "nin"}:
            items = target if isinstance(target, list) else [target]
            matched = any(_values_equal(record_value, item, field_meta, str(field_key)) for item in items)
            if op == "nin":
                matched = not matched
            if not matched:
                return False
            continue

        if op in {"contains", "starts_with", "ends_with"}:
            if record_value is None:
                return False
            haystack = str(record_value).casefold()
            needle = "" if target is None else str(target).casefold()
            if op == "contains":
                matched = needle in haystack
            elif op == "starts_with":
                matched = haystack.startswith(needle)
            else:
                matched = haystack.endswith(needle)
            if not matched:
                return False
            continue

        if op in {"eq", "neq"}:
            matched = _values_equal(record_value, target, field_meta, str(field_key))
            if op == "neq":
                matched = not matched
            if not matched:
                return False
            continue

        if op in {"lt", "lte", "gt", "gte"}:
            if record_value is None or target is None:
                return False
            left, right, kind = _coerce_ordered_values(record_value, target, field_meta, str(field_key))
            if left is None or right is None:
                return False
            if kind == "string":
                left = str(left).casefold()
                right = str(right).casefold()
            if op == "lt" and not left < right:
                return False
            if op == "lte" and not left <= right:
                return False
            if op == "gt" and not left > right:
                return False
            if op == "gte" and not left >= right:
                return False
            continue

    return True


def _build_detail_fields(
    snapshot_dict: Dict[str, Any],
    metric: Dict[str, Any] | None,
    detail_fields: List[str] | None,
) -> List[str]:
    if detail_fields:
        return _unique_preserve_order([str(item) for item in detail_fields if str(item)])

    metric_field = _resolve_metric_field(snapshot_dict, metric)
    pivot = snapshot_dict.get("pivot") or {}
    rows = pivot.get("rows") or []
    columns = pivot.get("columns") or []
    fields = [str(item) for item in rows + columns if str(item)]
    if metric_field:
        fields.append(str(metric_field))
    return _unique_preserve_order(fields)


def _resolve_metric_field(snapshot_dict: Dict[str, Any], metric: Dict[str, Any] | None) -> str | None:
    if not isinstance(metric, dict):
        return None
    direct = metric.get("fieldKey") or metric.get("field") or metric.get("sourceKey")
    if direct:
        return str(direct)
    metric_id = metric.get("id") or metric.get("key")
    if not metric_id:
        return None
    for entry in snapshot_dict.get("metrics") or []:
        if not isinstance(entry, dict):
            continue
        if entry.get("id") == metric_id or entry.get("key") == metric_id:
            resolved = entry.get("fieldKey") or entry.get("field") or entry.get("sourceKey")
            if resolved:
                return str(resolved)
    return None


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
    detail_metric_filters = _normalize_detail_metric_filters(payload.get("detailMetricFilter"))

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
    field_meta = snapshot_dict.get("fieldMeta") or {}
    detail_filtered_records = (
        [
            record
            for record in constrained_records
            if _record_passes_detail_metric_filters(record, detail_metric_filters, field_meta)
        ]
        if detail_metric_filters
        else constrained_records
    )

    total = len(detail_filtered_records)
    if offset < 0:
        offset = 0
    if limit <= 0:
        limit = 200
    paged = detail_filtered_records[offset : offset + limit]

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
            "recordsAfterCellFilter": len(constrained_records),
            "recordsAfterDetailMetricFilter": total,
            "effectiveFiltersKeysSummary": {
                "values": selected_keys,
                "ranges": range_keys,
            },
            "cellConstraints": cell_constraints,
            "detailMetricFilterApplied": bool(detail_metric_filters),
        }
    return response, debug_payload
