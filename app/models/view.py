from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from app.models.snapshot import ConditionalFormattingRule


class ViewCell(BaseModel):
    key: str
    value: Any


class ViewColumn(BaseModel):
    key: str
    label: Optional[str] = None
    width: Optional[int] = None
    values: Optional[List[Any]] = None
    formatting: Optional[List[ConditionalFormattingRule]] = None


class ViewRow(BaseModel):
    key: str
    label: Optional[str] = None
    values: Optional[List[Any]] = None
    cells: List[ViewCell]


class PivotView(BaseModel):
    """
    Итоговое представление таблицы:
    - список колонок
    - список строк с ячейками
    - totals (если есть)
    """
    columns: List[ViewColumn]
    rows: List[ViewRow]
    totals: Optional[Dict[str, Any]] = None


class ChartConfig(BaseModel):
    type: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    options: Optional[Dict[str, Any]] = None


class ViewResponse(BaseModel):
    """
    Ответ сервиса для одного контейнера:
    - view для таблицы
    - chart для графика (если нужно)
    """
    view: PivotView
    chart: Optional[ChartConfig] = None
    debug: Optional[Dict[str, Any]] = None
