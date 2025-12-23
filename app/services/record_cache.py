import os
import time
from typing import Any, Dict, Tuple


_CACHE_TTL_SECONDS = float(os.getenv("REPORT_FILTERS_CACHE_TTL", "30"))
_CACHE_MAX_ITEMS = int(os.getenv("REPORT_FILTERS_CACHE_MAX", "20"))
_STORE: Dict[str, Tuple[float, Any]] = {}


def get_cached_records(key: str) -> Any | None:
    if not key:
        return None
    entry = _STORE.get(key)
    if not entry:
        return None
    created_at, value = entry
    if time.time() - created_at > _CACHE_TTL_SECONDS:
        _STORE.pop(key, None)
        return None
    return value


def set_cached_records(key: str, value: Any) -> None:
    if not key:
        return
    if len(_STORE) >= _CACHE_MAX_ITEMS:
        oldest_key = min(_STORE.items(), key=lambda item: item[1][0])[0]
        _STORE.pop(oldest_key, None)
    _STORE[key] = (time.time(), value)
