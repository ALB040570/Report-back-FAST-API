from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from app.models.computed_field import ComputedField

class RemoteSource(BaseModel):
    """
    Нормализованный источник данных, аналогичный normalizeSource(...) на фронте.
    """
    id: Optional[str] = None
    remoteId: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None

    method: str = "POST"
    url: str

    body: Any = None
    headers: Optional[Dict[str, Any]] = None
    pushdown: Optional[Dict[str, Any]] = None

    joins: Optional[List[Dict[str, Any]]] = None
    computedFields: Optional[List[ComputedField]] = None
    rawBody: Optional[str] = None
    remoteMeta: Optional[Dict[str, Any]] = None
