from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class BatchRequest(BaseModel):
    sourceId: Optional[int] = None
    method: Optional[str] = None
    endpoint: Optional[str] = None
    params: List[Dict[str, Any]]
    meta: Optional[Dict[str, Any]] = None


class BatchItemResult(BaseModel):
    ok: bool
    params: Dict[str, Any]
    data: Optional[Any] = None
    error: Optional[str] = None
    status_code: Optional[int] = None


class BatchResponse(BaseModel):
    job_id: str
    status_url: str
    cancel_url: str


class BatchStatusResponse(BaseModel):
    job_id: str
    status: str
    createdAt: str
    startedAt: Optional[str] = None
    finishedAt: Optional[str] = None
    total: int
    done: int
    progress: float
    results: Optional[List[BatchItemResult]] = None
    resultsSummary: Optional[Dict[str, int]] = None
    resultsFileRef: Optional[str] = None
    error: Optional[str] = None
    cancelRequested: Optional[bool] = None
