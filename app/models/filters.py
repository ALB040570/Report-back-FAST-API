from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class FilterValue(BaseModel):
    values: Optional[List[Any]] = None
    range: Optional[Dict[str, Any]] = None


class Filters(BaseModel):
    """
    Набор фильтров, которые приходят от фронта.
    globalFilters — фильтры страницы,
    containerFilters — фильтры конкретного контейнера.
    """
    globalFilters: Dict[str, FilterValue] = {}
    containerFilters: Dict[str, FilterValue] = {}
