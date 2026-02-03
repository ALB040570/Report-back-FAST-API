import ast
import functools
import json
from typing import Any, Dict, List, Tuple

from app.services.date_utils import parse_date_input, parse_date_part_key, resolve_date_part_value


def _normalize_value_for_key(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return ""
    return str(value)


def _format_label_value(value: Any) -> str:
    if value is None or value == "":
        return "—"
    return str(value)




def _resolve_record_value_base(record: Dict[str, Any], key: str | None) -> Any:
    if not record or not key:
        return None
    return record.get(key)


def _build_dimension_label(values: Tuple[Any, ...], has_fields: bool) -> str:
    if not has_fields:
        return "Все записи"
    return " / ".join(_format_label_value(value) for value in values)


def _build_dimension_key(values: Tuple[Any, ...], field_keys: List[str]) -> str:
    if not field_keys:
        return "__all__"
    parts = []
    for field_key, value in zip(field_keys, values):
        normalized = _normalize_value_for_key(value)
        parts.append(f"{field_key}:{normalized}")
    return "|".join(parts) if parts else "__all__"


def _resolve_record_value(record: Dict[str, Any], key: str | None) -> Any:
    if not record or not key:
        return None
    meta = parse_date_part_key(key)
    if meta:
        base_value = _resolve_record_value_base(record, meta["field_key"])
        return resolve_date_part_value(base_value, meta["part"])
    return _resolve_record_value_base(record, key)


def _create_bucket() -> Dict[str, Any]:
    return {
        "count": 0,
        "numeric_count": 0,
        "sum": 0.0,
        "last": None,
    }


def _update_bucket(bucket: Dict[str, Any], value: Any) -> None:
    bucket["count"] += 1
    bucket["last"] = value
    try:
        num = float(value)
    except (TypeError, ValueError):
        num = None
    if num is None:
        return
    bucket["numeric_count"] += 1
    bucket["sum"] += num


def _finalize_bucket(bucket: Dict[str, Any] | None, aggregator: str | None) -> Any:
    if not bucket or not aggregator:
        return None
    agg = aggregator.lower()
    if agg == "value":
        return bucket["last"]
    if agg == "count":
        return bucket["count"]
    if agg == "sum":
        return bucket["sum"] if bucket["numeric_count"] else None
    if agg == "avg":
        if not bucket["numeric_count"]:
            return None
        return bucket["sum"] / bucket["numeric_count"]
    return None


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError("Non-numeric value")


def _apply_binary_op(op: ast.operator, left: Any, right: Any) -> Any:
    left_num = _coerce_number(left)
    right_num = _coerce_number(right)
    if left_num is None or right_num is None:
        return None
    if isinstance(op, ast.Add):
        return left_num + right_num
    if isinstance(op, ast.Sub):
        return left_num - right_num
    if isinstance(op, ast.Mult):
        return left_num * right_num
    if isinstance(op, ast.Div):
        return left_num / right_num
    if isinstance(op, ast.FloorDiv):
        return left_num // right_num
    if isinstance(op, ast.Mod):
        return left_num % right_num
    if isinstance(op, ast.Pow):
        return left_num**right_num
    raise ValueError("Unsupported operator")


def _apply_unary_op(op: ast.unaryop, value: Any) -> Any:
    numeric = _coerce_number(value)
    if numeric is None:
        return None
    if isinstance(op, ast.UAdd):
        return +numeric
    if isinstance(op, ast.USub):
        return -numeric
    raise ValueError("Unsupported unary operator")


def _eval_node(node: ast.AST, context: Dict[str, Any]) -> Any:
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, context)
        right = _eval_node(node.right, context)
        return _apply_binary_op(node.op, left, right)
    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, context)
        return _apply_unary_op(node.op, operand)
    if isinstance(node, ast.Name):
        if node.id not in context:
            raise KeyError(node.id)
        return context[node.id]
    if isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float)):
            return float(node.value)
        if node.value is None:
            return None
        raise ValueError("Unsupported literal")
    if isinstance(node, ast.Num):
        return float(node.n)
    raise ValueError("Unsupported expression")


def _safe_eval(expression: str | None, context: Dict[str, Any]) -> Any:
    if not expression:
        return None
    tree = ast.parse(expression, mode="eval")
    return _eval_node(tree.body, context)


def _normalize_direction(value: Any) -> str | None:
    if value in ("asc", "desc"):
        return value
    return None


def _normalize_sort_config(dimensions: List[str], state: Any) -> Dict[str, Dict[str, str]]:
    config: Dict[str, Dict[str, str]] = {}
    if not isinstance(state, dict):
        return config
    for key in dimensions:
        entry = state.get(key)
        if not isinstance(entry, dict):
            continue
        value_dir = _normalize_direction(entry.get("value"))
        metric_dir = _normalize_direction(entry.get("metric"))
        if value_dir or metric_dir:
            config[key] = {
                "value": value_dir,
                "metric": metric_dir,
            }
    return config


def _compare_strings(left: Any, right: Any, direction: str) -> int:
    a = "" if left is None else str(left)
    b = "" if right is None else str(right)
    a = a.casefold()
    b = b.casefold()
    if a == b:
        return 0
    if direction == "desc":
        return -1 if a > b else 1
    return -1 if a < b else 1


def _compare_numbers(left: Any, right: Any, direction: str) -> int:
    def to_number(value: Any) -> float | None:
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

    a = to_number(left)
    b = to_number(right)
    if a is None and b is None:
        return 0
    if a is None:
        return 1 if direction == "asc" else -1
    if b is None:
        return -1 if direction == "asc" else 1
    if a == b:
        return 0
    return -1 if (a < b) ^ (direction == "desc") else 1


def _finalize_prefix_totals(
    prefix_buckets: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]],
    metrics: List[Dict[str, Any]],
) -> Dict[Tuple[Any, ...], Dict[str, Any]]:
    totals: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for prefix, metric_buckets in prefix_buckets.items():
        context: Dict[str, Any] = {}
        for metric in metrics:
            if metric["type"] == "formula":
                continue
            bucket = metric_buckets.get(metric["key"])
            context[metric["key"]] = _finalize_bucket(bucket, metric["op"])
        for metric in metrics:
            if metric["type"] != "formula":
                continue
            try:
                value = _safe_eval(metric["expression"], context)
            except Exception:
                value = None
            context[metric["key"]] = value
        totals[prefix] = context
    return totals


def _ensure_row_node(
    nodes: Dict[Tuple[Any, ...], Dict[str, Any]],
    roots: List[Dict[str, Any]],
    prefix: Tuple[Any, ...],
    field_key: str,
    value: Any,
    depth: int,
) -> Dict[str, Any]:
    if prefix in nodes:
        return nodes[prefix]
    node = {
        "path": prefix,
        "field_key": field_key,
        "value": value,
        "label": _format_label_value(value),
        "depth": depth,
        "children": [],
        "order": len(nodes),
        "totals": {},
    }
    nodes[prefix] = node
    if depth == 0:
        roots.append(node)
    else:
        parent_prefix = prefix[:-1]
        parent = nodes.get(parent_prefix)
        if parent is not None:
            parent["children"].append(node)
    return node


def _sort_dimension_order(
    order: List[Tuple[Any, ...]],
    dimensions: List[str],
    config: Dict[str, Dict[str, str]],
    prefix_totals: Dict[Tuple[Any, ...], Dict[str, Any]],
    metric_key: str | None,
) -> None:
    if not order or not config:
        return
    ordered_keys = [key for key in dimensions if key in config]
    if not ordered_keys:
        return
    index_map = {key: idx for idx, key in enumerate(dimensions)}

    def compare(left: Tuple[Any, ...], right: Tuple[Any, ...]) -> int:
        for field_key in ordered_keys:
            sort_entry = config.get(field_key, {})
            idx = index_map[field_key]
            if sort_entry.get("metric") and metric_key:
                left_prefix = left[: idx + 1]
                right_prefix = right[: idx + 1]
                left_metric = prefix_totals.get(left_prefix, {}).get(metric_key)
                right_metric = prefix_totals.get(right_prefix, {}).get(metric_key)
                metric_cmp = _compare_numbers(left_metric, right_metric, sort_entry["metric"])
                if metric_cmp != 0:
                    return metric_cmp
            if sort_entry.get("value"):
                left_value = left[idx] if idx < len(left) else None
                right_value = right[idx] if idx < len(right) else None
                value_cmp = _compare_strings(left_value, right_value, sort_entry["value"])
                if value_cmp != 0:
                    return value_cmp
        return 0

    order.sort(key=functools.cmp_to_key(compare))


def _sort_row_tree_by_config(
    nodes: List[Dict[str, Any]],
    sort_config: Dict[str, Dict[str, str]],
    metric_key: str | None,
) -> None:
    if not nodes or not sort_config:
        return
    field_key = nodes[0].get("field_key")
    config = sort_config.get(field_key) if field_key else None

    def compare(left: Dict[str, Any], right: Dict[str, Any]) -> int:
        if config:
            if config.get("metric") and metric_key:
                left_metric = left.get("totals", {}).get(metric_key)
                right_metric = right.get("totals", {}).get(metric_key)
                metric_cmp = _compare_numbers(left_metric, right_metric, config["metric"])
                if metric_cmp != 0:
                    return metric_cmp
            if config.get("value"):
                value_cmp = _compare_strings(left.get("label"), right.get("label"), config["value"])
                if value_cmp != 0:
                    return value_cmp
        return left.get("order", 0) - right.get("order", 0)

    nodes.sort(key=functools.cmp_to_key(compare))
    for node in nodes:
        children = node.get("children") or []
        if children:
            _sort_row_tree_by_config(children, sort_config, metric_key)


def _flatten_row_tree(nodes: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    result: List[Tuple[Any, ...]] = []

    def walk(node: Dict[str, Any]) -> None:
        children = node.get("children") or []
        if not children:
            result.append(node.get("path", tuple()))
            return
        for child in children:
            walk(child)

    for node in nodes:
        walk(node)
    return result


def _normalize_rules(rules: Any) -> List[Dict[str, Any]]:
    if not rules:
        return []
    normalized: List[Dict[str, Any]] = []
    for rule in rules:
        if hasattr(rule, "model_dump"):
            normalized.append(rule.model_dump())
        elif hasattr(rule, "dict"):
            normalized.append(rule.dict())
        elif isinstance(rule, dict):
            normalized.append(rule)
    return normalized


def _rule_target_key(rule: Dict[str, Any]) -> str | None:
    for key in ("metricKey", "targetKey", "fieldKey", "key", "id", "metricId"):
        value = rule.get(key)
        if value is None:
            continue
        text = str(value)
        if text:
            return text
    return None


def _collect_column_rules(column: Dict[str, Any], rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rules:
        return []
    targets = {
        column.get("metric_key"),
        column.get("key"),
        column.get("base_key"),
    }
    targets = {target for target in targets if target}
    matched: List[Dict[str, Any]] = []
    for rule in rules:
        target = _rule_target_key(rule)
        if not target or target in targets:
            matched.append(rule)
    return matched


def _extract_metric(metric: Dict[str, Any]) -> Dict[str, Any]:
    metric_type = metric.get("type") or "base"
    metric_key = metric.get("key") or metric.get("id")
    source_key = (
        metric.get("sourceKey")
        or metric.get("fieldKey")
        or metric.get("field")
        or metric.get("source_key")
    )
    op = (metric.get("op") or metric.get("aggregator") or metric.get("agg") or "sum")
    if metric_type == "formula":
        op = None
    if not metric_key:
        metric_key = f"{source_key}__{op}" if source_key and op else ""
    label = metric.get("label") or metric_key
    return {
        "key": metric_key,
        "source_key": source_key,
        "op": op,
        "type": metric_type,
        "expression": metric.get("expression") or "",
        "label": label,
    }


def build_pivot_view(records: list[dict], snapshot: dict) -> dict:
    """
    Minimal pivot implementation with optional formulas.

    Example:
        records = [
            {"cls": "A", "value": 10, "count": 2},
            {"cls": "A", "value": 20, "count": 1},
            {"cls": "B", "value": 5, "count": 7},
        ]
        snapshot = {
            "pivot": {"rows": ["cls"], "columns": [], "filters": []},
            "metrics": [
                {"key": "value__sum", "sourceKey": "value", "op": "sum"},
                {"key": "count__sum", "sourceKey": "count", "op": "sum"},
                {
                    "key": "value_avg_formula",
                    "type": "formula",
                    "expression": "value__sum / count__sum",
                },
            ],
        }
    """
    pivot = snapshot.get("pivot") or {}
    row_fields = pivot.get("rows") or []
    column_fields = pivot.get("columns") or []
    metrics_input = snapshot.get("metrics") or []

    metrics = [_extract_metric(metric) for metric in metrics_input]
    base_metrics = [metric for metric in metrics if metric["type"] != "formula"]
    rules = _normalize_rules(snapshot.get("conditionalFormatting") or [])
    options = snapshot.get("options") or {}
    sorts = options.get("sorts") or {}
    row_sort_config = _normalize_sort_config(row_fields, sorts.get("rows") or {})
    column_sort_config = _normalize_sort_config(column_fields, sorts.get("columns") or {})
    primary_metric_key = metrics[0]["key"] if metrics else None

    row_order: List[Tuple[Any, ...]] = []
    row_index: Dict[Tuple[Any, ...], Dict[str, str]] = {}
    column_order: List[Tuple[Any, ...]] = []
    column_index: Dict[Tuple[Any, ...], Dict[str, str]] = {}

    cell_buckets: Dict[Tuple[Any, ...], Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]]] = {}
    total_buckets: Dict[str, Dict[str, Any]] = {}
    row_nodes: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    row_roots: List[Dict[str, Any]] = []
    row_prefix_buckets: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]] = {}
    column_prefix_buckets: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]] = {}

    if not column_fields and not column_order:
        column_order.append(tuple())
        column_index[tuple()] = {
            "key": _build_dimension_key(tuple(), []),
            "label": _build_dimension_label(tuple(), False),
            "values": [],
        }

    if not row_fields and not row_order:
        row_order.append(tuple())
        row_index[tuple()] = {
            "key": _build_dimension_key(tuple(), []),
            "label": _build_dimension_label(tuple(), False),
            "values": [],
        }

    for record in records or []:
        row_values = [_resolve_record_value(record, field) for field in row_fields]
        row_key = tuple(_normalize_value_for_key(value) for value in row_values)
        if row_key not in row_index:
            row_order.append(row_key)
            row_index[row_key] = {
                "key": _build_dimension_key(row_values, row_fields),
                "label": _build_dimension_label(tuple(row_values), bool(row_fields)),
                "values": list(row_values),
            }

        column_values = [_resolve_record_value(record, field) for field in column_fields]
        column_key = tuple(_normalize_value_for_key(value) for value in column_values)
        if column_key not in column_index:
            column_order.append(column_key)
            column_index[column_key] = {
                "key": _build_dimension_key(column_values, column_fields),
                "label": _build_dimension_label(tuple(column_values), bool(column_fields)),
                "values": list(column_values),
            }

        if row_key not in cell_buckets:
            cell_buckets[row_key] = {}
        if column_key not in cell_buckets[row_key]:
            cell_buckets[row_key][column_key] = {}

        if row_fields:
            for depth, field_key in enumerate(row_fields):
                prefix = row_key[: depth + 1]
                _ensure_row_node(
                    row_nodes,
                    row_roots,
                    prefix,
                    field_key,
                    row_values[depth] if depth < len(row_values) else None,
                    depth,
                )
                if prefix not in row_prefix_buckets:
                    row_prefix_buckets[prefix] = {}
                for metric in base_metrics:
                    bucket = row_prefix_buckets[prefix].get(metric["key"])
                    if bucket is None:
                        bucket = _create_bucket()
                        row_prefix_buckets[prefix][metric["key"]] = bucket
                    _update_bucket(bucket, _resolve_record_value(record, metric["source_key"]))

        if column_fields:
            for depth, _field_key in enumerate(column_fields):
                prefix = column_key[: depth + 1]
                if prefix not in column_prefix_buckets:
                    column_prefix_buckets[prefix] = {}
                for metric in base_metrics:
                    bucket = column_prefix_buckets[prefix].get(metric["key"])
                    if bucket is None:
                        bucket = _create_bucket()
                        column_prefix_buckets[prefix][metric["key"]] = bucket
                    _update_bucket(bucket, _resolve_record_value(record, metric["source_key"]))

        for metric in base_metrics:
            metric_key = metric["key"]
            bucket = cell_buckets[row_key][column_key].get(metric_key)
            if bucket is None:
                bucket = _create_bucket()
                cell_buckets[row_key][column_key][metric_key] = bucket
            _update_bucket(bucket, _resolve_record_value(record, metric["source_key"]))

            total_bucket = total_buckets.get(metric_key)
            if total_bucket is None:
                total_bucket = _create_bucket()
                total_buckets[metric_key] = total_bucket
            _update_bucket(total_bucket, _resolve_record_value(record, metric["source_key"]))

    if row_fields:
        row_prefix_totals = _finalize_prefix_totals(row_prefix_buckets, metrics)
        for prefix, node in row_nodes.items():
            node["totals"] = row_prefix_totals.get(prefix, {})
    else:
        row_prefix_totals = {}

    column_prefix_totals = _finalize_prefix_totals(column_prefix_buckets, metrics)

    if column_order and column_sort_config:
        _sort_dimension_order(
            column_order,
            column_fields,
            column_sort_config,
            column_prefix_totals,
            primary_metric_key,
        )

    if row_roots and row_sort_config:
        _sort_row_tree_by_config(row_roots, row_sort_config, primary_metric_key)

    if row_fields and row_roots:
        row_order = _flatten_row_tree(row_roots)

    columns_result: List[Dict[str, Any]] = []
    column_entries: List[Dict[str, Any]] = []

    for column_key in column_order:
        column_meta = column_index[column_key]
        base_key = column_meta["key"]
        base_label = column_meta["label"]
        for metric in metrics:
            column_key_value = f"{base_key}::{metric['key']}"
            label = metric["label"]
            if base_label != "Все записи":
                label = f"{base_label} - {label}"
            entry = {
                "key": column_key_value,
                "label": label,
                "base_key": base_key,
                "column_key": column_key,
                "metric_key": metric["key"],
                "metric_type": metric["type"],
                "op": metric["op"],
                "expression": metric["expression"],
            }
            column_rules = _collect_column_rules(entry, rules)
            column_payload = {
                "key": entry["key"],
                "label": entry["label"],
                "values": column_meta.get("values", []),
            }
            if column_rules:
                column_payload["formatting"] = column_rules
            columns_result.append(column_payload)
            column_entries.append(entry)

    rows_result_map: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for row_key, row_meta in row_index.items():
        column_contexts: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for column_key in column_order:
            ctx: Dict[str, Any] = {}
            for metric in base_metrics:
                bucket = (
                    cell_buckets.get(row_key, {})
                    .get(column_key, {})
                    .get(metric["key"])
                )
                ctx[metric["key"]] = _finalize_bucket(bucket, metric["op"])
            column_contexts[column_key] = ctx

        cells: List[Dict[str, Any]] = []
        for column in column_entries:
            column_key = column["column_key"]
            metric_key = column["metric_key"]
            if column["metric_type"] == "formula":
                ctx = column_contexts[column_key]
                try:
                    value = _safe_eval(column["expression"], ctx)
                except Exception:
                    value = None
                ctx[metric_key] = value
            else:
                value = column_contexts[column_key].get(metric_key)

            cells.append(
                {
                    "key": f"{row_meta['key']}||{column['base_key']}||{metric_key}",
                    "value": value,
                }
            )

        rows_result_map[row_key] = {
            "key": row_meta["key"],
            "label": row_meta["label"],
            "values": row_meta.get("values", []),
            "cells": cells,
        }

    if not row_order and row_index:
        row_order = list(row_index.keys())

    rows_result = [
        rows_result_map[row_key]
        for row_key in row_order
        if row_key in rows_result_map
    ]

    totals: Dict[str, Any] | None = None
    if metrics:
        totals = {}
        for metric in base_metrics:
            totals[metric["key"]] = _finalize_bucket(
                total_buckets.get(metric["key"]),
                metric["op"],
            )
        for metric in metrics:
            if metric["type"] != "formula":
                continue
            try:
                value = _safe_eval(metric["expression"], totals)
            except Exception:
                value = None
            totals[metric["key"]] = value

    return {
        "columns": columns_result,
        "rows": rows_result,
        "totals": totals,
    }
