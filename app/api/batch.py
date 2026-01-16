from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from app.config import get_settings
from app.models.batch import BatchRequest, BatchResponse, BatchStatusResponse
from app.services.batch_service import cancel_job, create_job, is_endpoint_allowed
from app.storage.job_store import get_job_store


router = APIRouter(tags=["batch"])


@router.post("/batch", response_model=BatchResponse)
async def create_batch(payload: BatchRequest) -> BatchResponse:
    settings = get_settings()
    if not payload.endpoint or not payload.endpoint.strip():
        raise HTTPException(status_code=400, detail="endpoint is required")
    endpoint = payload.endpoint.strip()
    if not is_endpoint_allowed(endpoint, settings.upstream_allowlist):
        raise HTTPException(status_code=400, detail="endpoint is not allowed")
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        if not settings.upstream_base_url:
            raise HTTPException(
                status_code=400,
                detail="UPSTREAM_BASE_URL is required for relative endpoint (e.g. http://45.8.116.32)",
            )
    if not payload.params:
        raise HTTPException(status_code=400, detail="params must be non-empty")
    if len(payload.params) > settings.batch_max_items:
        raise HTTPException(
            status_code=400,
            detail=f"params exceeds BATCH_MAX_ITEMS={settings.batch_max_items}",
        )

    store = get_job_store()
    job = await create_job(payload, store)
    job_id = job["job_id"]
    status_url = f"/batch/{job_id}"
    cancel_url = f"/batch/{job_id}"
    return BatchResponse(job_id=job_id, status_url=status_url, cancel_url=cancel_url)


@router.get("/batch/{job_id}", response_model=BatchStatusResponse, name="get_batch_status")
async def get_batch_status(job_id: str) -> BatchStatusResponse:
    store = get_job_store()
    job = await store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return BatchStatusResponse(**_job_to_status(job))


@router.delete("/batch/{job_id}", response_model=BatchStatusResponse, name="cancel_batch")
async def cancel_batch(job_id: str) -> BatchStatusResponse:
    store = get_job_store()
    job = await cancel_job(job_id, store)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return BatchStatusResponse(**_job_to_status(job))


def _job_to_status(job: Dict[str, Any]) -> Dict[str, Any]:
    status = job.get("status")
    include_results = status in {"done", "failed", "cancelled"}
    total = job.get("total", 0) or 0
    done = job.get("done", 0) or 0
    progress = (done / total) if total else 0.0
    return {
        "job_id": job.get("job_id"),
        "status": status,
        "createdAt": job.get("createdAt"),
        "startedAt": job.get("startedAt"),
        "finishedAt": job.get("finishedAt"),
        "total": total,
        "done": done,
        "progress": progress,
        "results": job.get("results") if include_results else None,
        "resultsSummary": job.get("resultsSummary") if include_results else None,
        "resultsFileRef": job.get("resultsFileRef") if include_results else None,
        "error": job.get("error") if status == "failed" else None,
        "cancelRequested": job.get("cancelRequested"),
    }
