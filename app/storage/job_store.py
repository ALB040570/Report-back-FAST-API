import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional, Protocol

from app.config import get_settings

try:
    import redis.asyncio as redis
except ImportError:  # pragma: no cover - optional dependency
    redis = None


logger = logging.getLogger(__name__)

_QUEUE_KEY = "batch:queue"
_JOB_KEY_PREFIX = "batch:job:"


class JobStore(Protocol):
    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        ...

    async def set_job(self, job_id: str, data: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        ...

    async def update_job(self, job_id: str, updates: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        ...

    async def enqueue_job(self, job_id: str) -> None:
        ...

    async def dequeue_job(self, timeout_seconds: int = 5) -> Optional[str]:
        ...


class InMemoryJobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._expires_at: Dict[str, float] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()

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

    async def enqueue_job(self, job_id: str) -> None:
        await self._queue.put(job_id)

    async def dequeue_job(self, timeout_seconds: int = 5) -> Optional[str]:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return None


class RedisJobStore:
    def __init__(self, url: str) -> None:
        if redis is None:
            raise RuntimeError("redis is not installed")
        self._client = redis.from_url(url, decode_responses=True)

    def _job_key(self, job_id: str) -> str:
        return f"{_JOB_KEY_PREFIX}{job_id}"

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        raw = await self._client.get(self._job_key(job_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Failed to decode job payload", extra={"job_id": job_id})
            return None

    async def set_job(self, job_id: str, data: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        payload = json.dumps(data, ensure_ascii=True)
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

    async def enqueue_job(self, job_id: str) -> None:
        await self._client.rpush(_QUEUE_KEY, job_id)

    async def dequeue_job(self, timeout_seconds: int = 5) -> Optional[str]:
        item = await self._client.blpop(_QUEUE_KEY, timeout=timeout_seconds)
        if not item:
            return None
        _, job_id = item
        return job_id


_job_store: Optional[JobStore] = None


def get_job_store() -> JobStore:
    global _job_store
    if _job_store is not None:
        return _job_store
    settings = get_settings()
    if settings.redis_url:
        if redis is None:
            logger.warning("REDIS_URL set but redis dependency missing, using in-memory store")
            _job_store = InMemoryJobStore()
        else:
            _job_store = RedisJobStore(settings.redis_url)
    else:
        _job_store = InMemoryJobStore()
    return _job_store
