import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional, Protocol
from uuid import uuid4

import redis.asyncio as redis

from app.config import get_settings
from app.models.view_request import ViewRequest
from app.observability.otel import get_tracer
from app.services.report_view_builder import build_report_view_response


logger = logging.getLogger(__name__)

_REPORT_QUEUE_KEY = "report:queue"
_REPORT_JOB_PREFIX = "report:job:"


class QueueFullError(RuntimeError):
    pass


class ReportJobStore(Protocol):
    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        ...

    async def set_job(self, job_id: str, data: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        ...

    async def update_job(self, job_id: str, updates: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        ...

    async def delete_job(self, job_id: str) -> None:
        ...

    async def enqueue_job(self, job_id: str) -> None:
        ...

    async def dequeue_job(self, timeout_seconds: int = 5) -> Optional[str]:
        ...

    async def queue_size(self) -> int:
        ...


class InMemoryReportJobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._expires_at: Dict[str, float] = {}
        self._queue: asyncio.Queue[str] | None = None
        self._queue_loop: asyncio.AbstractEventLoop | None = None

    def _ensure_queue(self) -> asyncio.Queue[str]:
        loop = asyncio.get_running_loop()
        if self._queue is None or self._queue_loop is not loop:
            self._queue = asyncio.Queue()
            self._queue_loop = loop
        return self._queue

    def _is_expired(self, job_id: str) -> bool:
        expires_at = self._expires_at.get(job_id)
        if expires_at is None:
            return False
        if time.time() >= expires_at:
            self._jobs.pop(job_id, None)
            self._expires_at.pop(job_id, None)
            return True
        return False

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        if self._is_expired(job_id):
            return None
        job = self._jobs.get(job_id)
        return dict(job) if job else None

    async def set_job(self, job_id: str, data: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        self._jobs[job_id] = dict(data)
        if ttl_seconds is not None:
            self._expires_at[job_id] = time.time() + ttl_seconds

    async def update_job(self, job_id: str, updates: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        if self._is_expired(job_id):
            return
        job = self._jobs.get(job_id)
        if not job:
            return
        job.update(updates)
        if ttl_seconds is not None:
            self._expires_at[job_id] = time.time() + ttl_seconds

    async def delete_job(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)
        self._expires_at.pop(job_id, None)

    async def enqueue_job(self, job_id: str) -> None:
        queue = self._ensure_queue()
        await queue.put(job_id)

    async def dequeue_job(self, timeout_seconds: int = 5) -> Optional[str]:
        queue = self._ensure_queue()
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return None

    async def queue_size(self) -> int:
        if self._queue is None:
            return 0
        return self._queue.qsize()


class RedisReportJobStore:
    def __init__(self, url: str) -> None:
        self._client = redis.from_url(url, decode_responses=True)

    def _job_key(self, job_id: str) -> str:
        return f"{_REPORT_JOB_PREFIX}{job_id}"

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        raw = await self._client.get(self._job_key(job_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Failed to decode report job payload", extra={"job_id": job_id})
            return None

    async def set_job(self, job_id: str, data: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        payload = json.dumps(data, ensure_ascii=True, default=str)
        key = self._job_key(job_id)
        if ttl_seconds is not None:
            await self._client.setex(key, ttl_seconds, payload)
        else:
            await self._client.set(key, payload)

    async def update_job(self, job_id: str, updates: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        job = await self.get_job(job_id)
        if not job:
            return
        job.update(updates)
        await self.set_job(job_id, job, ttl_seconds=ttl_seconds)

    async def delete_job(self, job_id: str) -> None:
        await self._client.delete(self._job_key(job_id))

    async def enqueue_job(self, job_id: str) -> None:
        await self._client.rpush(_REPORT_QUEUE_KEY, job_id)

    async def dequeue_job(self, timeout_seconds: int = 5) -> Optional[str]:
        item = await self._client.blpop(_REPORT_QUEUE_KEY, timeout=timeout_seconds)
        if not item:
            return None
        _, job_id = item
        return job_id

    async def queue_size(self) -> int:
        return int(await self._client.llen(_REPORT_QUEUE_KEY))


_job_store: Optional[ReportJobStore] = None
_worker_tasks: list[asyncio.Task] = []
_worker_lock = asyncio.Lock()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _job_dir() -> str:
    settings = get_settings()
    return settings.report_jobs_dir


def _job_file_path(job_id: str) -> str:
    return os.path.join(_job_dir(), f"{job_id}.json")


def _cleanup_results_dir(ttl_seconds: int) -> None:
    if ttl_seconds <= 0:
        return
    now = time.time()
    try:
        entries = os.listdir(_job_dir())
    except OSError:
        return
    removed = 0
    for name in entries:
        path = os.path.join(_job_dir(), name)
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
        logger.info("Report results cleanup", extra={"removed": removed, "ttl_seconds": ttl_seconds})


def get_report_job_store() -> ReportJobStore:
    global _job_store
    if _job_store is not None:
        return _job_store
    settings = get_settings()
    if settings.redis_url:
        _job_store = RedisReportJobStore(settings.redis_url)
    else:
        _job_store = InMemoryReportJobStore()
    return _job_store


async def ensure_report_workers() -> None:
    settings = get_settings()
    if settings.report_job_max_concurrency <= 0:
        return
    async with _worker_lock:
        active_tasks = [task for task in _worker_tasks if not task.done()]
        _worker_tasks.clear()
        _worker_tasks.extend(active_tasks)
        if _worker_tasks:
            return
        for _ in range(settings.report_job_max_concurrency):
            _worker_tasks.append(asyncio.create_task(_worker_loop()))


async def create_report_job(payload: Dict[str, Any], request_id: str | None = None) -> str:
    store = get_report_job_store()
    settings = get_settings()
    queue_size = await store.queue_size()
    if queue_size >= settings.report_job_queue_max_size:
        raise QueueFullError("Report job queue is full")

    job_id = str(uuid4())
    job = {
        "job_id": job_id,
        "status": "queued",
        "createdAt": _now_iso(),
        "startedAt": None,
        "finishedAt": None,
        "payload": payload,
        "result": None,
        "resultFileRef": None,
        "error": None,
        "type": "view",
        "requestId": request_id,
    }
    await store.set_job(job_id, job, ttl_seconds=settings.report_job_ttl_seconds)
    await store.enqueue_job(job_id)
    logger.info(
        "Report job queued",
        extra={"job_id": job_id, "requestId": request_id, "status": "queued"},
    )
    await ensure_report_workers()
    return job_id


async def get_report_job(job_id: str) -> Optional[Dict[str, Any]]:
    store = get_report_job_store()
    job = await store.get_job(job_id)
    if not job:
        return None
    result = job.get("result")
    if result is None and job.get("resultFileRef"):
        path = job.get("resultFileRef")
        try:
            with open(path, "r", encoding="utf-8") as handle:
                result = json.load(handle)
        except (OSError, json.JSONDecodeError):
            result = None
    response = {
        "status": job.get("status"),
        "result": result,
        "error": job.get("error"),
    }
    return response


async def delete_report_job(job_id: str) -> bool:
    store = get_report_job_store()
    job = await store.get_job(job_id)
    if not job:
        return False
    path = job.get("resultFileRef")
    if path:
        try:
            os.remove(path)
        except OSError:
            pass
    await store.delete_job(job_id)
    return True


async def _worker_loop() -> None:
    store = get_report_job_store()
    settings = get_settings()
    while True:
        job_id = await store.dequeue_job(timeout_seconds=1)
        if not job_id:
            await asyncio.sleep(0.05)
            continue
        job = await store.get_job(job_id)
        if not job:
            continue
        request_id = job.get("requestId")
        if job.get("status") == "cancelled":
            logger.info(
                "Report job cancelled",
                extra={
                    "job_id": job_id,
                    "requestId": request_id,
                    "duration_ms": 0,
                    "status": "cancelled",
                },
            )
            continue
        await store.update_job(
            job_id,
            {"status": "running", "startedAt": _now_iso()},
            ttl_seconds=settings.report_job_ttl_seconds,
        )
        logger.info(
            "Report job running",
            extra={"job_id": job_id, "requestId": request_id, "status": "running"},
        )
        started = time.monotonic()
        tracer = get_tracer()
        try:
            with tracer.start_as_current_span("report_job_run") as span:
                span.set_attribute("async_enabled", True)
                span.set_attribute("streaming_enabled", settings.report_streaming)
                payload = job.get("payload") or {}
                view_payload = ViewRequest(**payload)
                response = await build_report_view_response(view_payload, request_id)
                result_payload = response.dict()
                await _persist_job_result(job_id, result_payload)
                await store.update_job(
                    job_id,
                    {"status": "done", "finishedAt": _now_iso()},
                    ttl_seconds=settings.report_job_ttl_seconds,
                )
                logger.info(
                    "Report job done",
                    extra={
                        "job_id": job_id,
                        "requestId": request_id,
                        "duration_ms": int((time.monotonic() - started) * 1000),
                        "status": "done",
                    },
                )
        except Exception as exc:
            await store.update_job(
                job_id,
                {
                    "status": "failed",
                    "finishedAt": _now_iso(),
                    "error": {"code": "failed", "message": str(exc)},
                },
                ttl_seconds=settings.report_job_ttl_seconds,
            )
            logger.warning(
                "Report job failed",
                extra={
                    "job_id": job_id,
                    "requestId": request_id,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                    "status": "failed",
                    "error": str(exc),
                },
            )


async def _persist_job_result(job_id: str, result: Dict[str, Any]) -> None:
    store = get_report_job_store()
    settings = get_settings()
    payload = json.dumps(result, ensure_ascii=False, default=str)
    payload_bytes = len(payload.encode("utf-8"))
    updates: Dict[str, Any] = {}
    if payload_bytes > settings.report_job_max_result_bytes:
        os.makedirs(_job_dir(), exist_ok=True)
        path = _job_file_path(job_id)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(payload)
        updates["resultFileRef"] = path
        updates["result"] = None
    else:
        updates["result"] = result
        updates["resultFileRef"] = None
    await store.update_job(job_id, updates, ttl_seconds=settings.report_job_ttl_seconds)
    _cleanup_results_dir(settings.report_job_ttl_seconds)
