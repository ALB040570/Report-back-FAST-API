from collections import defaultdict
from typing import Any, Dict, List, Tuple

from app.models.snapshot import Snapshot
from app.models.view import PivotView, ViewColumn, ViewRow


def _make_group_key(record: Dict[str, Any], row_fields: List[str]) -> Tuple[Any, ...]:
    return tuple(record.get(field) for field in row_fields)


def _init_metric_accumulator() -> Dict[str, Dict[str, Any]]:
    """
    Возвращает пустой аккумулятор для метрик:
    {
        field: {
            "sum": ...,
            "count": ...,
            "min": ...,
            "max": ...
        }
    }
    Фактический набор операций задаётся snapshot.metrics.
    """
    return {}


def _update_metric_accumulator(
    acc: Dict[str, Dict[str, Any]],
    record: Dict[str, Any],
    metrics: List,
) -> None:
    for metric in metrics:
        field = metric.field
        agg = metric.agg.lower()
        value = record.get(field)

        if field not in acc:
            acc[field] = {"sum": 0.0, "count": 0, "min": None, "max": None}

        # count считаем всегда, даже если value = None
        if value is not None:
            try:
                num = float(value)
            except (TypeError, ValueError):
                # если значение не число — пропускаем для сумм/среднего/мин/макс
                num = None
        else:
            num = None

        if agg in ("sum", "avg") and num is not None:
            acc[field]["sum"] += num

        if agg in ("count", "avg", "sum", "min", "max"):
            acc[field]["count"] += 1

        if agg in ("min", "max") and num is not None:
            if acc[field]["min"] is None or num < acc[field]["min"]:
                acc[field]["min"] = num
            if acc[field]["max"] is None or num > acc[field]["max"]:
                acc[field]["max"] = num


def _finalize_metric_value(acc_entry: Dict[str, Any], agg: str) -> Any:
    agg = agg.lower()
    if agg == "sum":
        return acc_entry["sum"]
    if agg == "count":
        return acc_entry["count"]
    if agg == "avg":
        if acc_entry["count"] == 0:
            return None
        return acc_entry["sum"] / acc_entry["count"]
    if agg == "min":
        return acc_entry["min"]
    if agg == "max":
        return acc_entry["max"]
    # на всякий случай — если неизвестный агрегат
    return None


def build_pivot(records: List[Dict[str, Any]], snapshot: Snapshot) -> PivotView:
    """
    Реализация pivot:
    - группировка по snapshot.pivot.rows
    - расчёт метрик snapshot.metrics (sum/count/avg/min/max)
    Колонки:
      - по всем полям из rows
      - по всем метрикам (field + agg)
    cols из pivot пока игнорируем (добавим позже).
    """
    row_fields = snapshot.pivot.rows or []
    metrics = snapshot.metrics or []

    if not records:
        return PivotView(columns=[], rows=[], totals=None)

    # Группируем записи по ключу из row_fields
    groups: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]] = defaultdict(_init_metric_accumulator)
    # Также храним "representative" запись для получения исходных значений row-полей
    representatives: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    for record in records:
        key = _make_group_key(record, row_fields)
        if key not in representatives:
            representatives[key] = {field: record.get(field) for field in row_fields}
        _update_metric_accumulator(groups[key], record, metrics)

    # Строим список колонок: сначала row-поля, потом метрики
    columns: List[ViewColumn] = []
    for field in row_fields:
        columns.append(ViewColumn(key=field, label=field))

    metric_column_keys: List[Tuple[str, str]] = []  # (field, agg)
    for metric in metrics:
        col_key = f"{metric.field}__{metric.agg}"
        metric_column_keys.append((metric.field, metric.agg))
        label = metric.label or col_key
        columns.append(ViewColumn(key=col_key, label=label))

    # Строим строки
    rows: List[ViewRow] = []
    for idx, (group_key, metric_acc) in enumerate(groups.items()):
        base_row = representatives[group_key]
        cells: Dict[str, Any] = {}

        # row-поля
        for field in row_fields:
            cells[field] = base_row.get(field)

        # метрики
        for field, agg in metric_column_keys:
            acc_entry = metric_acc.get(field)
            if acc_entry is None:
                cells[f"{field}__{agg}"] = None
            else:
                cells[f"{field}__{agg}"] = _finalize_metric_value(acc_entry, agg)

        rows.append(ViewRow(key=str(idx), cells=cells))

    # totals (пока пустой; позже можно добавить суммирование по метрикам)
    return PivotView(columns=columns, rows=rows, totals=None)
