from typing import Any, Dict, List

from app.models.snapshot import Snapshot
from app.services.pivot_core import build_pivot_view


def _snapshot_to_dict(snapshot: Snapshot | Dict[str, Any]) -> Dict[str, Any]:
    if hasattr(snapshot, "model_dump"):
        return snapshot.model_dump()
    if hasattr(snapshot, "dict"):
        return snapshot.dict()
    return snapshot


def build_view(records: List[Dict[str, Any]], snapshot: Snapshot) -> Dict[str, Any]:
    pivot = build_pivot_view(records, _snapshot_to_dict(snapshot))
    return pivot
