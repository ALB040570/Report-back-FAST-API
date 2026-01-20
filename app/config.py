import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Settings:
    redis_url: Optional[str]
    batch_concurrency: int
    batch_max_items: int
    batch_job_ttl_seconds: int
    batch_results_ttl_seconds: int
    upstream_base_url: str
    upstream_url: str
    upstream_timeout: float
    upstream_allowlist: Optional[str]


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def get_settings() -> Settings:
    batch_job_ttl_seconds = _get_int("BATCH_JOB_TTL_SECONDS", 3600)
    batch_results_ttl_seconds = _get_int("BATCH_RESULTS_TTL_SECONDS", batch_job_ttl_seconds)
    return Settings(
        redis_url=os.getenv("REDIS_URL"),
        batch_concurrency=_get_int("BATCH_CONCURRENCY", 5),
        batch_max_items=_get_int("BATCH_MAX_ITEMS", 100),
        batch_job_ttl_seconds=batch_job_ttl_seconds,
        batch_results_ttl_seconds=batch_results_ttl_seconds,
        upstream_base_url=os.getenv("UPSTREAM_BASE_URL", ""),
        upstream_url=os.getenv("UPSTREAM_URL", ""),
        upstream_timeout=_get_float("UPSTREAM_TIMEOUT", 30.0),
        upstream_allowlist=os.getenv("UPSTREAM_ALLOWLIST"),
    )
