import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from uuid import uuid4

import httpx

from app.config import get_settings
from app.models.batch import BatchRequest
from app.services.upstream_client import UpstreamHTTPError, async_request_json, build_full_url
from app.storage.job_store import JobStore


logger = logging.getLogger(__name__)

_RESULTS_INLINE_LIMIT_BYTES = 2 * 1024 * 1024
_RESULTS_DIR = os.path.join(os.getcwd(), "batch_results")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _build_upstream_payload(
    params: Dict[str, Any],
    *,
    method: Optional[str],
    source_id: Optional[int],
    meta: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"params": [params]}
    if method:
        payload["method"] = method
    if source_id is not None:
        payload["sourceId"] = source_id
    if meta is not None:
        payload["meta"] = meta
    return payload


async def create_job(payload: BatchRequest, store: JobStore) -> Dict[str, Any]:
    job_id = str(uuid4())
    job = {
        "job_id": job_id,
        "status": "queued",
        "createdAt": _now_iso(),
        "startedAt": None,
        "finishedAt": None,
        "total": len(payload.params),
        "done": 0,
        "results": None,
        "resultsSummary": None,
        "resultsFileRef": None,
        "error": None,
        "cancelRequested": False,
        "params": payload.params,
        "method": payload.method,
        "sourceId": payload.sourceId,
        "endpoint": payload.endpoint.strip() if payload.endpoint else None,
        "meta": payload.meta,
    }
    await store.set_job(job_id, job)
    await store.enqueue_job(job_id)
    logger.info("Batch job queued", extra={"job_id": job_id, "total": job["total"]})
    return job


async def cancel_job(job_id: str, store: JobStore) -> Optional[Dict[str, Any]]:
    job = await store.get_job(job_id)
    if not job:
        return None
    job["cancelRequested"] = True
    await store.set_job(job_id, job)
    return job


async def process_job(job_id: str, store: JobStore) -> None:
    job = await store.get_job(job_id)
    if not job:
        logger.warning("Batch job not found", extra={"job_id": job_id})
        return

    if job.get("status") in {"done", "failed", "cancelled"}:
        return

    if job.get("cancelRequested"):
        await _finish_job(job_id, store, "cancelled", error=None, results=[])
        return

    settings = get_settings()
    effective_upstream = _resolve_upstream_url(job, settings)
    if job.get("endpoint") and not effective_upstream:
        await _finish_job(
            job_id,
            store,
            "failed",
            error="Endpoint is not allowed or UPSTREAM_BASE_URL is missing",
            results=[],
        )
        return
    if not effective_upstream:
        await _finish_job(
            job_id,
            store,
            "failed",
            error="Upstream endpoint is not configured",
            results=[],
        )
        return

    started_at = time.monotonic()
    await store.update_job(job_id, {"status": "running", "startedAt": _now_iso()})
    logger.info("Batch job started", extra={"job_id": job_id, "total": job.get("total", 0)})

    params_list: List[Dict[str, Any]] = job.get("params", [])
    total = len(params_list)
    concurrency = max(1, min(settings.batch_concurrency, total))
    queue: asyncio.Queue[tuple[int, Dict[str, Any]]] = asyncio.Queue()
    for idx, params in enumerate(params_list):
        queue.put_nowait((idx, params))

    results: List[Optional[Dict[str, Any]]] = [None] * total
    done = 0
    lock = asyncio.Lock()
    cancel_event = asyncio.Event()
    semaphore = asyncio.Semaphore(concurrency)

    async def worker_loop(client: httpx.AsyncClient) -> None:
        nonlocal done
        while not cancel_event.is_set():
            try:
                idx, params = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if cancel_event.is_set():
                queue.task_done()
                break

            async with semaphore:
                item_result = await _process_item(
                    job_id,
                    client,
                    params,
                    upstream_url=effective_upstream,
                    method=job.get("method"),
                    source_id=job.get("sourceId"),
                    meta=job.get("meta"),
                )
            results[idx] = item_result

            async with lock:
                done += 1
                await store.update_job(job_id, {"done": done})
                latest = await store.get_job(job_id)
                if latest and latest.get("cancelRequested"):
                    cancel_event.set()

            queue.task_done()

    async with httpx.AsyncClient(timeout=settings.upstream_timeout) as client:
        workers = [asyncio.create_task(worker_loop(client)) for _ in range(concurrency)]
        await asyncio.gather(*workers, return_exceptions=True)

    if cancel_event.is_set():
        for idx, params in enumerate(params_list):
            if results[idx] is None:
                results[idx] = {
                    "ok": False,
                    "params": params,
                    "error": "cancelled",
                }
        await _finish_job(job_id, store, "cancelled", error=None, results=_compact_results(results))
        logger.info(
            "Batch job cancelled",
            extra={"job_id": job_id, "duration_ms": int((time.monotonic() - started_at) * 1000)},
        )
        return

    safe_results = _compact_results(results)
    error_count = sum(1 for item in safe_results if not item.get("ok"))
    status = "failed" if error_count else "done"
    error_message = None
    if error_count:
        error_message = f"{error_count} items failed"
    await _finish_job(job_id, store, status, error=error_message, results=safe_results)
    logger.info(
        "Batch job completed",
        extra={
            "job_id": job_id,
            "status": status,
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        },
    )


async def _process_item(
    job_id: str,
    client: httpx.AsyncClient,
    params: Dict[str, Any],
    *,
    upstream_url: str,
    method: Optional[str],
    source_id: Optional[int],
    meta: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    payload = _build_upstream_payload(params, method=method, source_id=source_id, meta=meta)
    started_at = time.monotonic()
    try:
        data, status_code = await async_request_json(
            client,
            "POST",
            upstream_url,
            headers={"Content-Type": "application/json"},
            json_body=payload,
        )
        logger.info(
            "Batch item finished",
            extra={
                "job_id": job_id,
                "status_code": status_code,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            },
        )
        return {
            "ok": True,
            "params": params,
            "data": data,
            "status_code": status_code,
        }
    except UpstreamHTTPError as exc:
        logger.warning(
            "Upstream HTTP error",
            extra={"job_id": job_id, "status_code": exc.status_code},
        )
        logger.info(
            "Batch item finished",
            extra={
                "job_id": job_id,
                "status_code": exc.status_code,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            },
        )
        return {
            "ok": False,
            "params": params,
            "error": str(exc),
            "status_code": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Upstream request failed", extra={"job_id": job_id})
        logger.info(
            "Batch item finished",
            extra={"job_id": job_id, "duration_ms": int((time.monotonic() - started_at) * 1000)},
        )
        return {
            "ok": False,
            "params": params,
            "error": str(exc),
        }


def _resolve_upstream_url(job: Dict[str, Any], settings: Any) -> Optional[str]:
    endpoint = job.get("endpoint")
    if endpoint:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            if not _is_full_url_allowed(endpoint, settings.upstream_allowlist):
                logger.warning("Full upstream URL is not allowed", extra={"job_id": job.get("job_id")})
                return None
            return endpoint
        if settings.upstream_base_url:
            return build_full_url(settings.upstream_base_url, endpoint)
        return None

    if settings.upstream_url:
        return settings.upstream_url
    return None


def _is_full_url_allowed(url: str, allowlist: Optional[str]) -> bool:
    if not allowlist:
        # TODO: configure UPSTREAM_ALLOWLIST for full URL overrides.
        return False
    target = urlparse(url)
    target_host = target.netloc.lower()
    entries = [item.strip() for item in allowlist.split(",") if item.strip()]
    for entry in entries:
        if entry.startswith("http://") or entry.startswith("https://"):
            parsed = urlparse(entry)
            if parsed.scheme == target.scheme and parsed.netloc.lower() == target_host:
                return True
        else:
            if entry.lower() == target_host:
                return True
    return False


def is_endpoint_allowed(endpoint: str, allowlist: Optional[str]) -> bool:
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return _is_full_url_allowed(endpoint, allowlist)
    return True


def _compact_results(results: List[Optional[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    compacted: List[Dict[str, Any]] = []
    for item in results:
        if item is None:
            continue
        compacted.append(item)
    return compacted


def _summarize_results(results: List[Dict[str, Any]]) -> Dict[str, int]:
    ok_count = sum(1 for item in results if item.get("ok"))
    fail_count = sum(1 for item in results if not item.get("ok"))
    return {"ok": ok_count, "failed": fail_count, "total": len(results)}


def _cleanup_results_dir(ttl_seconds: int) -> None:
    if ttl_seconds <= 0:
        return
    now = time.time()
    try:
        entries = os.listdir(_RESULTS_DIR)
    except OSError:
        return
    removed = 0
    for name in entries:
        path = os.path.join(_RESULTS_DIR, name)
        try:
            stat = os.stat(path)
        except OSError:
            continue
        if now - stat.st_mtime > ttl_seconds:
            try:
                os.remove(path)
                removed += 1
            except OSError:
                continue
    if removed:
        logger.info("Batch results cleanup", extra={"removed": removed, "ttl_seconds": ttl_seconds})


async def _finish_job(
    job_id: str,
    store: JobStore,
    status: str,
    *,
    error: Optional[str],
    results: List[Dict[str, Any]],
) -> None:
    settings = get_settings()
    finished_at = _now_iso()
    payload = json.dumps(results, ensure_ascii=True, default=str)
    payload_size = len(payload.encode("utf-8"))

    results_summary = _summarize_results(results)
    results_file_ref = None
    inline_results: Optional[List[Dict[str, Any]]] = results

    if payload_size > _RESULTS_INLINE_LIMIT_BYTES:
        os.makedirs(_RESULTS_DIR, exist_ok=True)
        results_file_ref = os.path.join(_RESULTS_DIR, f"{job_id}.json")
        with open(results_file_ref, "w", encoding="utf-8") as handle:
            handle.write(payload)
        inline_results = None

    latest = await store.get_job(job_id)
    current_done = latest.get("done", len(results)) if latest else len(results)
    updates = {
        "status": status,
        "finishedAt": finished_at,
        "results": inline_results,
        "resultsSummary": results_summary,
        "resultsFileRef": results_file_ref,
        "error": error,
        "done": current_done,
    }
    await store.update_job(job_id, updates, ttl_seconds=settings.batch_job_ttl_seconds)
    logger.info(
        "Batch job finished",
        extra={
            "job_id": job_id,
            "status": status,
            "ok": results_summary["ok"],
            "failed": results_summary["failed"],
        },
    )
    _cleanup_results_dir(settings.batch_results_ttl_seconds)
