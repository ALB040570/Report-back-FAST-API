import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

from app.models.filters import Filters, FilterValue
from app.models.snapshot import Snapshot

DATE_PART_MARKER = "__date_part__"
MONTH_LABELS = [
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]
DATE_PART_LABELS = {
    "year": "Год",
    "month": "Месяц",
    "day": "День",
}
SYSTEM_FILTER_KEYS = {
    "id",
    "cls",
    "obj",
    "pv",
    "fv",
    "objUser",
    "pvUser",
    "fvUser",
    "idUser",
    "idCreatedAt",
    "idUpdatedAt",
}


def _snapshot_to_dict(snapshot: Snapshot | Dict[str, Any]) -> Dict[str, Any]:
    if hasattr(snapshot, "model_dump"):
        return snapshot.model_dump()
    if hasattr(snapshot, "dict"):
        return snapshot.dict()
    return snapshot


def _normalize_mode(value: Any) -> str | None:
    if value in ("values", "ranges"):
        return value
    return None


def _normalize_values_list(value: Any) -> List[Any] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    return [value]


def _coerce_values_selection(value: Any) -> Dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict) and ("items" in value or "mode" in value):
        mode = str(value.get("mode") or "include").lower()
        mode = "exclude" if mode == "exclude" else "include"
        items = value.get("items")
        if isinstance(items, list):
            items_list = items
        elif items is None:
            items_list = []
        else:
            items_list = [items]
        return {"mode": mode, "items": items_list}
    if isinstance(value, list):
        return {"mode": "include", "items": value}
    return {"mode": "include", "items": [value]}


def _normalize_range(value: Any) -> Dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    start = value.get("start") if "start" in value else value.get("from")
    end = value.get("end") if "end" in value else value.get("to")
    if start is None and end is None:
        return None
    return {"start": start, "end": end}


def _extract_filter_map(filters: Filters | Dict[str, Any] | None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if not filters:
        return {}, {}
    if isinstance(filters, Filters):
        return filters.globalFilters or {}, filters.containerFilters or {}
    return (
        filters.get("globalFilters") or {},
        filters.get("containerFilters") or {},
    )


def _extract_nested_group(group: Any) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]] | None:
    if not isinstance(group, dict):
        return None
    values_map = group.get("values")
    ranges_map = group.get("ranges") or group.get("range")
    modes_map = group.get("modes") or group.get("mode")
    if not isinstance(values_map, dict) and not isinstance(ranges_map, dict) and not isinstance(modes_map, dict):
        return None
    return (
        values_map if isinstance(values_map, dict) else {},
        ranges_map if isinstance(ranges_map, dict) else {},
        modes_map if isinstance(modes_map, dict) else {},
    )


def _extract_filter_values_ranges(value: Any) -> Tuple[Dict[str, Any] | None, Dict[str, Any] | None, str | None]:
    if value is None:
        return None, None, None
    if isinstance(value, FilterValue):
        values = _coerce_values_selection(value.values)
        ranges = _normalize_range(value.range)
        return values, ranges, None
    if isinstance(value, dict):
        if "items" in value or "mode" in value:
            return _coerce_values_selection(value), None, None
        values = _coerce_values_selection(value.get("values"))
        ranges = _normalize_range(value.get("range") or value.get("ranges"))
        mode = _normalize_mode(value.get("mode"))
        return values, ranges, mode
    return None, None, None


def _merge_filters(snapshot: Dict[str, Any], filters: Filters | Dict[str, Any] | None) -> Tuple[
    Dict[str, Dict[str, Any] | None],
    Dict[str, Dict[str, Any] | None],
    Dict[str, str | None],
]:
    merged_values: Dict[str, Dict[str, Any] | None] = {}
    merged_ranges: Dict[str, Dict[str, Any] | None] = {}
    merged_modes: Dict[str, str | None] = {}

    snapshot_values = snapshot.get("filterValues") or {}
    snapshot_ranges = snapshot.get("filterRanges") or {}
    snapshot_modes = snapshot.get("filterModes") or {}

    for key, value in snapshot_values.items():
        merged_values[key] = _coerce_values_selection(value)
    for key, value in snapshot_ranges.items():
        merged_ranges[key] = _normalize_range(value)
    for key, value in snapshot_modes.items():
        mode = _normalize_mode(value)
        if mode:
            merged_modes[key] = mode

    global_filters, container_filters = _extract_filter_map(filters)

    def apply_override(source: Dict[str, Any]) -> None:
        nested = _extract_nested_group(source)
        if nested:
            values_map, ranges_map, modes_map = nested
            for key, value in values_map.items():
                merged_values[key] = _coerce_values_selection(value)
            for key, value in ranges_map.items():
                merged_ranges[key] = _normalize_range(value)
            for key, value in modes_map.items():
                mode = _normalize_mode(value)
                if mode:
                    merged_modes[key] = mode
            source = {
                key: value
                for key, value in source.items()
                if key not in {"values", "ranges", "range", "modes", "mode"}
            }
        for key, entry in (source or {}).items():
            values, ranges, mode = _extract_filter_values_ranges(entry)
            merged_values[key] = values
            merged_ranges[key] = ranges
            if mode:
                merged_modes[key] = mode
            elif key in merged_modes:
                merged_modes.pop(key, None)

    apply_override(global_filters)
    apply_override(container_filters)

    return merged_values, merged_ranges, merged_modes


def _normalize_filter_value(value: Any) -> str:
    if value is None or value == "":
        return "__BLANK__"
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return "__BLANK__"
    return str(value)


def _parse_date_part_key(key: str | None) -> Dict[str, str] | None:
    if not key or not isinstance(key, str):
        return None
    index = key.rfind(DATE_PART_MARKER)
    if index == -1:
        return None
    field_key = key[:index]
    part = key[index + len(DATE_PART_MARKER) :]
    if not field_key or part not in {"year", "month", "day"}:
        return None
    return {"field_key": field_key, "part": part}


def _parse_date_input(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        try:
            return datetime.fromisoformat(f"{text}T00:00:00+00:00")
        except ValueError:
            return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _resolve_date_part_value(value: Any, part: str) -> str | None:
    parsed = _parse_date_input(value)
    if not parsed:
        return None
    if part == "year":
        return str(parsed.year)
    if part == "month":
        month_index = parsed.month - 1
        numeric = f"{parsed.month:02d}"
        label = MONTH_LABELS[month_index] if 0 <= month_index < len(MONTH_LABELS) else ""
        return f"{numeric} — {label}" if label else numeric
    if part == "day":
        return f"{parsed.day:02d}"
    return None


def _resolve_record_value(record: Dict[str, Any], key: str | None) -> Any:
    if not record or not key:
        return None
    if key in record:
        return record.get(key)
    meta = _parse_date_part_key(key)
    if meta:
        base_value = record.get(meta["field_key"])
        return _resolve_date_part_value(base_value, meta["part"])
    if "." in key:
        current: Any = record
        for part in key.split("."):
            if isinstance(current, dict):
                if part not in current:
                    return None
                current = current.get(part)
                continue
            return None
        return current
    return None


def _format_filter_option_value(value: Any) -> Any:
    if value is None or value == "":
        return "__BLANK__"
    if isinstance(value, (dict, list)):
        return _normalize_filter_value(value)
    return value


def _resolve_field_type_from_meta(field_meta: Dict[str, Any], key: str) -> str | None:
    entry = field_meta.get(key) or {}
    field_type = str(entry.get("type") or "").lower()
    if field_type in {"number", "int", "float", "decimal"}:
        return "number"
    if field_type in {"date", "datetime", "timestamp"}:
        return "date"
    if field_type:
        return "string"
    return None


def _infer_field_type(values: List[Any]) -> str:
    cleaned = [
        value
        for value in values
        if value not in (None, "", "__BLANK__")
    ]
    if not cleaned:
        return "string"

    has_string = any(isinstance(value, str) for value in cleaned)
    all_numeric = True
    all_dates = True

    for value in cleaned:
        if isinstance(value, str):
            if _parse_date_input(value) is None:
                all_dates = False
            if _to_number(value) is None:
                all_numeric = False
        else:
            all_dates = False
            if _to_number(value) is None:
                all_numeric = False

    if all_dates:
        return "date"
    if all_numeric:
        return "number"
    return "string"


def _resolve_meta_type(field_meta: Dict[str, Any], key: str, values: List[Any]) -> tuple[str, str]:
    if key and DATE_PART_MARKER in key:
        return "string", "default"
    meta_type = _resolve_field_type_from_meta(field_meta, key)
    if meta_type:
        if meta_type == "date":
            inferred = _infer_field_type(values)
            if inferred != "date":
                return inferred, "inferred"
        return meta_type, "fieldMeta"
    inferred = _infer_field_type(values)
    source = "inferred" if values else "default"
    return inferred, source


def _find_meta_label(filters_meta: List[Dict[str, Any]], key: str) -> str | None:
    for entry in filters_meta:
        if not isinstance(entry, dict):
            continue
        if entry.get("key") == key and isinstance(entry.get("label"), str):
            label = entry.get("label", "").strip()
            if label:
                return label
    return None


def _format_date_part_field_label(base_label: str, part: str) -> str:
    part_label = DATE_PART_LABELS.get(part) or ""
    cleaned_base = (base_label or "").strip()
    if not cleaned_base:
        return part_label or ""
    if not part_label:
        return cleaned_base
    return f"{cleaned_base} • {part_label}"


def _resolve_field_label(
    key: str,
    header_overrides: Dict[str, Any],
    field_meta: Dict[str, Any],
    filters_meta: List[Dict[str, Any]],
) -> str:
    override = header_overrides.get(key)
    if isinstance(override, str) and override.strip():
        return override.strip()

    meta = _parse_date_part_key(key)
    base_key = meta["field_key"] if meta else key

    base_override = header_overrides.get(base_key)
    if isinstance(base_override, str) and base_override.strip():
        label = base_override.strip()
        return _format_date_part_field_label(label, meta["part"]) if meta else label

    base_label = _find_meta_label(filters_meta, base_key)
    if not base_label and isinstance(field_meta.get(base_key), dict):
        base_label = field_meta.get(base_key, {}).get("label")

    if not base_label:
        base_label = base_key

    if meta:
        return _format_date_part_field_label(str(base_label), meta["part"])
    return str(base_label)


def _sort_filter_values(values: List[Any], field_type: str) -> List[Any]:
    def sort_key(value: Any) -> Any:
        if field_type == "number":
            num = _to_number(value)
            return (0, num) if num is not None else (1, str(value))
        if field_type == "date":
            ms = _to_ms(value)
            return (0, ms) if ms is not None else (1, str(value))
        return str(value).casefold()

    return sorted(values, key=sort_key)


def _record_passes_filters(
    record: Dict[str, Any],
    filter_keys: Iterable[str],
    values_map: Dict[str, Dict[str, Any] | None],
    ranges_map: Dict[str, Dict[str, Any] | None],
    modes_map: Dict[str, str | None],
    field_meta: Dict[str, Any],
    exclude_key: str | None = None,
) -> bool:
    for key in filter_keys:
        if exclude_key and key == exclude_key:
            continue
        mode = modes_map.get(key)
        values = values_map.get(key)
        ranges = ranges_map.get(key)
        if mode == "values":
            ranges = None
        elif mode == "ranges":
            values = None
        if key and DATE_PART_MARKER in key:
            ranges = None

        if not values and not ranges:
            continue
        value = _resolve_record_value(record, key)
        if values and not _passes_values_filter(value, values):
            return False
        if ranges and not _passes_range_filter(value, ranges, field_meta, key):
            return False
    return True


def _determine_filter_keys(snapshot_dict: Dict[str, Any]) -> List[str]:
    # filterKeys должны приходить из snapshot.pivot.filters (единый источник для builder/pages).
    pivot = snapshot_dict.get("pivot") or {}
    if isinstance(pivot, dict):
        keys = [str(key) for key in (pivot.get("filters") or []) if str(key)]
    else:
        keys = []
    if keys:
        return keys
    filters_meta = snapshot_dict.get("filtersMeta") or []
    meta_keys = [entry.get("key") for entry in filters_meta if isinstance(entry, dict) and entry.get("key")]
    if meta_keys:
        return [str(key) for key in meta_keys if str(key)]
    return []


def _unique_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _is_system_like_key(key: str) -> bool:
    if key in SYSTEM_FILTER_KEYS:
        return True
    lowered = key.lower()
    if lowered in SYSTEM_FILTER_KEYS:
        return True
    for prefix in ("id", "cls", "obj", "pv", "fv"):
        if lowered.startswith(prefix):
            return True
    return False


def _is_pivot_filters_suspect(keys: List[str]) -> bool:
    if len(keys) > 50:
        return True
    if not keys:
        return False
    system_count = sum(1 for key in keys if _is_system_like_key(key))
    return system_count >= max(5, int(len(keys) * 0.3))


def _resolve_filter_keys(snapshot_dict: Dict[str, Any]) -> Dict[str, Any]:
    pivot = snapshot_dict.get("pivot") or {}
    pivot_filters = []
    if isinstance(pivot, dict):
        pivot_filters = [str(key) for key in (pivot.get("filters") or []) if str(key)]

    filters_meta = snapshot_dict.get("filtersMeta") or []
    filters_meta_keys = [
        str(entry.get("key"))
        for entry in filters_meta
        if isinstance(entry, dict) and entry.get("key")
    ]

    pivot_filters = _unique_preserve_order(pivot_filters)
    filters_meta_keys = _unique_preserve_order(filters_meta_keys)
    suspect = _is_pivot_filters_suspect(pivot_filters)

    if filters_meta_keys:
        keys_used = filters_meta_keys
        source = "filtersMeta"
    elif pivot_filters:
        keys_used = pivot_filters
        source = "pivot.filters"
    else:
        keys_used = []
        source = "none"

    return {
        "input": pivot_filters,
        "meta": filters_meta_keys,
        "used": keys_used,
        "source": source,
        "suspect": suspect,
    }


def collect_filter_options(
    records: List[Dict[str, Any]],
    snapshot: Snapshot | Dict[str, Any],
    filters: Filters | Dict[str, Any] | None,
    max_unique: int = 200,
    filter_keys: List[str] | None = None,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Dict[str, Any]], Dict[str, bool], Dict[str, List[Any]], Dict[str, Any]]:
    snapshot_dict = _snapshot_to_dict(snapshot)
    field_meta = snapshot_dict.get("fieldMeta") or {}
    header_overrides = (snapshot_dict.get("options") or {}).get("headerOverrides") or {}
    filters_meta = snapshot_dict.get("filtersMeta") or []

    values_map, ranges_map, modes_map = _merge_filters(snapshot_dict, filters)
    normalized_values_map = _normalize_values_selection_map(values_map)
    keys_info = _resolve_filter_keys(snapshot_dict)
    keys = filter_keys or keys_info["used"]

    all_filter_keys = set(values_map.keys()) | set(ranges_map.keys()) | set(keys)

    options_result: Dict[str, List[Dict[str, Any]]] = {}
    meta_result: Dict[str, Dict[str, Any]] = {}
    truncated_result: Dict[str, bool] = {}
    selected_pruned: Dict[str, List[Any]] = {}
    meta_type_source: Dict[str, str] = {}

    for key in keys:
        filtered_records = [
            record
            for record in records
            if _record_passes_filters(
                record,
                all_filter_keys,
                normalized_values_map,
                ranges_map,
                modes_map,
                field_meta,
                exclude_key=key,
            )
        ]
        counts: Dict[str, Dict[str, Any]] = {}
        for record in filtered_records:
            value = _resolve_record_value(record, key)
            value = _format_filter_option_value(value)
            normalized = _normalize_filter_value(value)
            entry = counts.get(normalized)
            if not entry:
                entry = {"value": value, "count": 0}
                if value == "__BLANK__":
                    entry["label"] = "(Blank)"
                counts[normalized] = entry
            entry["count"] += 1
        values_for_type = [entry["value"] for entry in counts.values()]
        field_type, type_source = _resolve_meta_type(field_meta, key, values_for_type)
        meta_type_source[key] = type_source
        values_sorted = _sort_filter_values(values_for_type, field_type)
        options: List[Dict[str, Any]] = []
        for value in values_sorted:
            normalized = _normalize_filter_value(value)
            entry = counts.get(normalized)
            if not entry:
                continue
            options.append(entry)
        truncated = False
        if max_unique and len(options) > max_unique:
            truncated = True
            options = options[:max_unique]
        options_result[key] = options
        truncated_result[key] = truncated

        selection = values_map.get(key)
        if selection and selection.get("items"):
            available = {_normalize_filter_value(option["value"]) for option in options}
            pruned = []
            for item in selection.get("items", []):
                normalized_item = _normalize_filter_value(item)
                if normalized_item not in available:
                    pruned.append(item if normalized_item != "__BLANK__" else "__BLANK__")
            if pruned:
                selected_pruned[key] = pruned

        label = _resolve_field_label(key, header_overrides, field_meta, filters_meta)
        meta_entry = field_meta.get(key) if isinstance(field_meta, dict) else {}
        meta_result[key] = {
            "type": field_type,
            "label": label or (meta_entry.get("label") if isinstance(meta_entry, dict) else None) or key,
        }

    effective_values = {key: values_map.get(key) for key in keys if key in values_map}
    effective_ranges = {key: ranges_map.get(key) for key in keys if key in ranges_map}
    effective_modes = {key: modes_map.get(key) for key in keys if key in modes_map}

    debug = {
        "filterKeysInput": {
            "count": len(keys_info["input"]),
            "sample": keys_info["input"][:20],
            "suspect": keys_info["suspect"],
            "source": keys_info["source"],
        },
        "filterKeysUsed": keys,
        "effectiveFilters": {
            "values": effective_values,
            "ranges": effective_ranges,
            "modes": effective_modes,
        },
        "optionsCountPerKey": {key: len(options_result.get(key, [])) for key in keys},
        "metaTypeSource": meta_type_source,
    }
    return options_result, meta_result, truncated_result, selected_pruned, debug

def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        num = int(value)
        if abs(num) < 10_000_000_000:
            return num * 1000
        return num
    parsed = _parse_date_input(value)
    if not parsed:
        return None
    return int(parsed.timestamp() * 1000)


def _is_date_type(field_meta: Dict[str, Any], key: str) -> bool:
    entry = field_meta.get(key) or {}
    field_type = str(entry.get("type") or "").lower()
    return field_type in {"date", "datetime", "timestamp"}


def _passes_values_filter(value: Any, selection: Dict[str, Any] | None) -> bool:
    if not selection:
        return True
    items = selection.get("items") or []
    if not items:
        return True
    normalized_value = _normalize_filter_value(value)
    allowed_set = selection.get("normalized") or {_normalize_filter_value(item) for item in items}
    mode = selection.get("mode") or "include"
    if mode == "exclude":
        return normalized_value not in allowed_set
    return normalized_value in allowed_set


def _passes_range_filter(
    value: Any,
    range_filter: Dict[str, Any],
    field_meta: Dict[str, Any],
    key: str,
) -> bool:
    if not range_filter:
        return True
    start = range_filter.get("start")
    end = range_filter.get("end")
    if start is None and end is None:
        return True

    is_date = _is_date_type(field_meta, key)
    if not is_date and isinstance(value, str):
        is_date = _parse_date_input(value) is not None

    if is_date:
        val_ms = _to_ms(value)
        if val_ms is None:
            return False
        start_ms = _to_ms(start) if start is not None else None
        end_ms = _to_ms(end) if end is not None else None
        if start_ms is not None and val_ms < start_ms:
            return False
        if end_ms is not None and val_ms > end_ms:
            return False
        return True

    val_num = _to_number(value)
    if val_num is None:
        return False
    start_num = _to_number(start) if start is not None else None
    end_num = _to_number(end) if end is not None else None
    if start_num is not None and val_num < start_num:
        return False
    if end_num is not None and val_num > end_num:
        return False
    return True


def _normalize_values_selection_map(values_map: Dict[str, Dict[str, Any] | None]) -> Dict[str, Dict[str, Any]]:
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, entry in values_map.items():
        if not entry:
            continue
        items = entry.get("items") or []
        normalized[key] = {
            "mode": entry.get("mode") or "include",
            "items": items,
            "normalized": {_normalize_filter_value(item) for item in items},
        }
    return normalized


def apply_filters(
    records: List[Dict[str, Any]],
    snapshot: Snapshot | Dict[str, Any],
    filters: Filters | Dict[str, Any] | None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    snapshot_dict = _snapshot_to_dict(snapshot)
    field_meta = snapshot_dict.get("fieldMeta") or {}
    values_map, ranges_map, modes_map = _merge_filters(snapshot_dict, filters)
    normalized_values_map = _normalize_values_selection_map(values_map)

    filter_keys = sorted(set(values_map.keys()) | set(ranges_map.keys()))
    applied_values = []
    applied_ranges = []
    for key in filter_keys:
        if values_map.get(key) and (values_map.get(key) or {}).get("items"):
            applied_values.append(key)
        if ranges_map.get(key):
            applied_ranges.append(key)

    if not filter_keys or (not applied_values and not applied_ranges):
        return records, {
            "counts": {"beforeFilters": len(records), "afterFilters": len(records)},
            "effectiveFilters": {
                "values": values_map,
                "ranges": ranges_map,
                "modes": modes_map,
            },
            "appliedKeys": {"values": [], "ranges": []},
            "sampleRecordKeys": {"beforeFilters": list(records[0].keys()) if records else []},
        }

    filtered: List[Dict[str, Any]] = []
    dropped_examples: List[Dict[str, Any]] = []

    for record in records:
        passed = True
        fail_reason = None
        for key in filter_keys:
            mode = modes_map.get(key)
            values = normalized_values_map.get(key)
            ranges = ranges_map.get(key)
            if mode == "values":
                ranges = None
            elif mode == "ranges":
                values = None
            if key and DATE_PART_MARKER in key:
                ranges = None

            if not values and not ranges:
                continue
            value = _resolve_record_value(record, key)
            if values and not _passes_values_filter(value, values):
                passed = False
                fail_reason = {"key": key, "type": "values", "value": value}
                break
            if ranges and not _passes_range_filter(value, ranges, field_meta, key):
                passed = False
                fail_reason = {"key": key, "type": "ranges", "value": value}
                break
        if passed:
            filtered.append(record)
        elif fail_reason and len(dropped_examples) < 2:
            dropped_examples.append(fail_reason)

    debug = {
        "counts": {
            "beforeFilters": len(records),
            "afterFilters": len(filtered),
        },
        "effectiveFilters": {
            "values": values_map,
            "ranges": ranges_map,
            "modes": modes_map,
        },
        "appliedKeys": {
            "values": applied_values,
            "ranges": applied_ranges,
        },
        "sampleRecordKeys": {
            "beforeFilters": list(records[0].keys()) if records else [],
            "afterFilters": list(filtered[0].keys()) if filtered else [],
        },
    }
    if dropped_examples:
        debug["reasonsDropped"] = dropped_examples
    return filtered, debug
