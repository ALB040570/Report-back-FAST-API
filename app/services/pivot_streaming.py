from typing import Any, Dict, List, Tuple

from app.services import pivot_core


def _snapshot_to_dict(snapshot: Any) -> Dict[str, Any]:
    if hasattr(snapshot, "model_dump"):
        return snapshot.model_dump()
    if hasattr(snapshot, "dict"):
        return snapshot.dict()
    return snapshot


class StreamingPivotAggregator:
    def __init__(
        self,
        snapshot: Any,
        *,
        max_groups: int | None = None,
        max_unique_values_per_dim: int | None = None,
    ) -> None:
        snapshot_dict = _snapshot_to_dict(snapshot)
        pivot = snapshot_dict.get("pivot") or {}
        self._row_fields = pivot.get("rows") or []
        self._column_fields = pivot.get("columns") or []
        metrics_input = snapshot_dict.get("metrics") or []

        self._metrics = [pivot_core._extract_metric(metric) for metric in metrics_input]
        self._base_metrics = [metric for metric in self._metrics if metric["type"] != "formula"]
        self._rules = pivot_core._normalize_rules(snapshot_dict.get("conditionalFormatting") or [])
        options = snapshot_dict.get("options") or {}
        sorts = options.get("sorts") or {}
        self._row_sort_config = pivot_core._normalize_sort_config(self._row_fields, sorts.get("rows") or {})
        self._column_sort_config = pivot_core._normalize_sort_config(self._column_fields, sorts.get("columns") or {})
        self._primary_metric_key = self._metrics[0]["key"] if self._metrics else None

        self._row_order: List[Tuple[Any, ...]] = []
        self._row_index: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        self._column_order: List[Tuple[Any, ...]] = []
        self._column_index: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

        self._cell_buckets: Dict[Tuple[Any, ...], Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]]] = {}
        self._total_buckets: Dict[str, Dict[str, Any]] = {}
        self._row_nodes: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        self._row_roots: List[Dict[str, Any]] = []
        self._row_prefix_buckets: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]] = {}
        self._column_prefix_buckets: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]] = {}

        self._max_groups = max_groups if max_groups and max_groups > 0 else None
        self._max_unique_values_per_dim = (
            max_unique_values_per_dim if max_unique_values_per_dim and max_unique_values_per_dim > 0 else None
        )
        self._group_count = 0
        self._unique_values: Dict[str, set[str]] = {
            key: set() for key in (self._row_fields + self._column_fields)
        }

        if not self._column_fields and not self._column_order:
            self._column_order.append(tuple())
            self._column_index[tuple()] = {
                "key": pivot_core._build_dimension_key(tuple(), []),
                "label": pivot_core._build_dimension_label(tuple(), False),
                "values": [],
            }
        if not self._row_fields and not self._row_order:
            self._row_order.append(tuple())
            self._row_index[tuple()] = {
                "key": pivot_core._build_dimension_key(tuple(), []),
                "label": pivot_core._build_dimension_label(tuple(), False),
                "values": [],
            }

    def _track_unique_values(self, fields: List[str], values: List[Any]) -> None:
        if not self._max_unique_values_per_dim:
            return
        for field_key, value in zip(fields, values):
            normalized = pivot_core._normalize_value_for_key(value)
            seen = self._unique_values.setdefault(field_key, set())
            if normalized in seen:
                continue
            if len(seen) + 1 > self._max_unique_values_per_dim:
                raise ValueError(
                    f"Streaming unique values limit exceeded for {field_key}: "
                    f"{len(seen) + 1} > {self._max_unique_values_per_dim}"
                )
            seen.add(normalized)

    def _increment_group_count(self) -> None:
        if not self._max_groups:
            return
        if self._group_count + 1 > self._max_groups:
            raise ValueError(
                f"Streaming groups limit exceeded: {self._group_count + 1} > {self._max_groups}"
            )
        self._group_count += 1

    def update(self, records: List[Dict[str, Any]]) -> None:
        for record in records or []:
            row_values = [pivot_core._resolve_record_value(record, field) for field in self._row_fields]
            self._track_unique_values(self._row_fields, row_values)
            row_key = tuple(pivot_core._normalize_value_for_key(value) for value in row_values)
            if row_key not in self._row_index:
                self._row_order.append(row_key)
                self._row_index[row_key] = {
                    "key": pivot_core._build_dimension_key(tuple(row_values), self._row_fields),
                    "label": pivot_core._build_dimension_label(tuple(row_values), bool(self._row_fields)),
                    "values": list(row_values),
                }

            column_values = [pivot_core._resolve_record_value(record, field) for field in self._column_fields]
            self._track_unique_values(self._column_fields, column_values)
            column_key = tuple(pivot_core._normalize_value_for_key(value) for value in column_values)
            if column_key not in self._column_index:
                self._column_order.append(column_key)
                self._column_index[column_key] = {
                    "key": pivot_core._build_dimension_key(tuple(column_values), self._column_fields),
                    "label": pivot_core._build_dimension_label(tuple(column_values), bool(self._column_fields)),
                    "values": list(column_values),
                }

            if row_key not in self._cell_buckets:
                self._cell_buckets[row_key] = {}
            if column_key not in self._cell_buckets[row_key]:
                self._increment_group_count()
                self._cell_buckets[row_key][column_key] = {}

            if self._row_fields:
                for depth, field_key in enumerate(self._row_fields):
                    prefix = row_key[: depth + 1]
                    pivot_core._ensure_row_node(
                        self._row_nodes,
                        self._row_roots,
                        prefix,
                        field_key,
                        row_values[depth] if depth < len(row_values) else None,
                        depth,
                    )
                    if prefix not in self._row_prefix_buckets:
                        self._row_prefix_buckets[prefix] = {}
                    for metric in self._base_metrics:
                        bucket = self._row_prefix_buckets[prefix].get(metric["key"])
                        if bucket is None:
                            bucket = pivot_core._create_bucket()
                            self._row_prefix_buckets[prefix][metric["key"]] = bucket
                        pivot_core._update_bucket(
                            bucket, pivot_core._resolve_record_value(record, metric["source_key"])
                        )

            if self._column_fields:
                for depth, _field_key in enumerate(self._column_fields):
                    prefix = column_key[: depth + 1]
                    if prefix not in self._column_prefix_buckets:
                        self._column_prefix_buckets[prefix] = {}
                    for metric in self._base_metrics:
                        bucket = self._column_prefix_buckets[prefix].get(metric["key"])
                        if bucket is None:
                            bucket = pivot_core._create_bucket()
                            self._column_prefix_buckets[prefix][metric["key"]] = bucket
                        pivot_core._update_bucket(
                            bucket, pivot_core._resolve_record_value(record, metric["source_key"])
                        )

            for metric in self._base_metrics:
                metric_key = metric["key"]
                bucket = self._cell_buckets[row_key][column_key].get(metric_key)
                if bucket is None:
                    bucket = pivot_core._create_bucket()
                    self._cell_buckets[row_key][column_key][metric_key] = bucket
                pivot_core._update_bucket(
                    bucket, pivot_core._resolve_record_value(record, metric["source_key"])
                )

                total_bucket = self._total_buckets.get(metric_key)
                if total_bucket is None:
                    total_bucket = pivot_core._create_bucket()
                    self._total_buckets[metric_key] = total_bucket
                pivot_core._update_bucket(
                    total_bucket, pivot_core._resolve_record_value(record, metric["source_key"])
                )

    def finalize(self) -> Dict[str, Any]:
        if self._row_fields:
            row_prefix_totals = pivot_core._finalize_prefix_totals(self._row_prefix_buckets, self._metrics)
            for prefix, node in self._row_nodes.items():
                node["totals"] = row_prefix_totals.get(prefix, {})
        else:
            row_prefix_totals = {}

        column_prefix_totals = pivot_core._finalize_prefix_totals(self._column_prefix_buckets, self._metrics)

        if self._column_order and self._column_sort_config:
            pivot_core._sort_dimension_order(
                self._column_order,
                self._column_fields,
                self._column_sort_config,
                column_prefix_totals,
                self._primary_metric_key,
            )

        if self._row_roots and self._row_sort_config:
            pivot_core._sort_row_tree_by_config(
                self._row_roots, self._row_sort_config, self._primary_metric_key
            )

        if self._row_fields and self._row_roots:
            self._row_order = pivot_core._flatten_row_tree(self._row_roots)

        columns_result: List[Dict[str, Any]] = []
        column_entries: List[Dict[str, Any]] = []

        for column_key in self._column_order:
            column_meta = self._column_index[column_key]
            base_key = column_meta["key"]
            base_label = column_meta["label"]
            for metric in self._metrics:
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
                column_rules = pivot_core._collect_column_rules(entry, self._rules)
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
        for row_key, row_meta in self._row_index.items():
            column_contexts: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
            for column_key in self._column_order:
                ctx: Dict[str, Any] = {}
                for metric in self._base_metrics:
                    bucket = self._cell_buckets.get(row_key, {}).get(column_key, {}).get(metric["key"])
                    ctx[metric["key"]] = pivot_core._finalize_bucket(bucket, metric["op"])
                column_contexts[column_key] = ctx

            cells: List[Dict[str, Any]] = []
            for column in column_entries:
                column_key = column["column_key"]
                metric_key = column["metric_key"]
                if column["metric_type"] == "formula":
                    ctx = column_contexts[column_key]
                    try:
                        value = pivot_core._safe_eval(column["expression"], ctx)
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

        if not self._row_order and self._row_index:
            self._row_order = list(self._row_index.keys())

        rows_result = [
            rows_result_map[row_key]
            for row_key in self._row_order
            if row_key in rows_result_map
        ]

        totals: Dict[str, Any] | None = None
        if self._metrics:
            totals = {}
            for metric in self._base_metrics:
                totals[metric["key"]] = pivot_core._finalize_bucket(
                    self._total_buckets.get(metric["key"]),
                    metric["op"],
                )
            for metric in self._metrics:
                if metric["type"] != "formula":
                    continue
                try:
                    value = pivot_core._safe_eval(metric["expression"], totals)
                except Exception:
                    value = None
                totals[metric["key"]] = value

        return {
            "columns": columns_result,
            "rows": rows_result,
            "totals": totals,
        }
