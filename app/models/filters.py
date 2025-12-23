from typing import Any, Dict, List, Optional

from pydantic import BaseModel, root_validator


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

    @root_validator(pre=True)
    def normalize_filter_groups(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        def normalize_group(group: Any) -> Dict[str, Any]:
            if not isinstance(group, dict):
                return {}
            values_map = group.get("values") if isinstance(group.get("values"), dict) else {}
            ranges_map = group.get("ranges") if isinstance(group.get("ranges"), dict) else {}
            if isinstance(group.get("range"), dict) and not ranges_map:
                ranges_map = group.get("range") or {}
            normalized: Dict[str, Any] = {}
            if values_map or ranges_map:
                for key, item in values_map.items():
                    normalized.setdefault(key, {})["values"] = item
                for key, item in ranges_map.items():
                    normalized.setdefault(key, {})["range"] = item
            for key, item in group.items():
                if key in {"values", "ranges", "range", "modes", "mode"}:
                    continue
                normalized[key] = item
            return normalized

        values = values or {}
        values["globalFilters"] = normalize_group(values.get("globalFilters"))
        values["containerFilters"] = normalize_group(values.get("containerFilters"))
        return values
