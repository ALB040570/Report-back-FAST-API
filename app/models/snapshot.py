from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel


class PivotSection(BaseModel):
    # filters, rows, columns — массивы идентификаторов полей
    filters: List[str] = []
    rows: List[str] = []
    columns: List[str] = []


class Metric(BaseModel):
    key: Optional[str] = None
    sourceKey: Optional[str] = None
    op: Optional[Literal["sum", "avg", "count", "value"]] = None
    type: Literal["base", "formula"] = "base"
    expression: Optional[str] = None
    field: Optional[str] = None
    agg: Optional[str] = None  # sum, count, avg, min, max
    fieldKey: Optional[str] = None
    aggregator: Optional[str] = None
    label: Optional[str] = None
    format: Optional[str] = None
    # сюда можно потом добавить expr, если используется формула


class Options(BaseModel):
    headerOverrides: Dict[str, Any] = {}
    sorts: Dict[str, Any] = {}  # например { fieldKey: { direction: 'asc' } }


class ConditionalFormattingRule(BaseModel):
    targetKey: Optional[str] = None
    metricKey: Optional[str] = None
    fieldKey: Optional[str] = None
    scope: Literal["cell", "row", "column"] = "cell"
    operator: Optional[str] = None
    value: Any = None
    valueTo: Any = None
    color: Optional[str] = None
    icon: Optional[str] = None
    style: Optional[Dict[str, Any]] = None

    class Config:
        extra = "allow"


class Snapshot(BaseModel):
    """
    Структура snapshot должна соответствовать тому, что возвращает buildSnapshot(config) на фронте.
    """
    pivot: PivotSection

    metrics: List[Metric] = []

    # сохранённые значения фильтров (по ключу поля)
    filterValues: Dict[str, Any] = {}
    filterRanges: Dict[str, Any] = {}

    # фильтры по строкам / столбцам (dimensionValues)
    dimensionValues: Dict[str, Dict[str, Any]] = {
        "rows": {},
        "columns": {},
    }

    # диапазоны по строкам / столбцам (dimensionRanges)
    dimensionRanges: Dict[str, Dict[str, Any]] = {
        "rows": {},
        "columns": {},
    }

    options: Options = Options()

    # метаданные для фильтров и полей
    filtersMeta: List[Dict[str, Any]] = []
    fieldMeta: Dict[str, Any] = {}

    # режимы фильтров (values/range и т.п.)
    filterModes: Dict[str, Any] = {}

    # дополнительные настройки графика (если есть)
    chartSettings: Optional[Dict[str, Any]] = None

    # условное форматирование (правила приходят с фронта)
    conditionalFormatting: List[ConditionalFormattingRule] = []
