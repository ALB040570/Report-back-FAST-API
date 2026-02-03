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
    async_reports: bool
    report_job_ttl_seconds: int
    report_job_max_result_bytes: int
    report_jobs_dir: str
    report_job_max_concurrency: int
    report_job_queue_max_size: int
    report_job_poll_interval_ms: int
    report_streaming: bool
    report_streaming_on_limit: bool
    report_chunk_size: int
    report_streaming_max_groups: int
    report_streaming_max_unique_values_per_dim: int
    report_streaming_max_records: int
    report_paging_allowlist: Optional[str]
    report_paging_max_pages: int
    report_upstream_paging: bool
    report_join_lookup_max_keys: int
    report_join_max_records: int
    report_join_source_max_records: int
    report_upstream_pushdown: bool
    report_pushdown_allowlist: Optional[str]
    report_pushdown_max_filters: int
    report_pushdown_max_in_values: int
    report_pushdown_safe_only: bool
    report_pushdown_override: bool
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


def _get_int_allow_zero(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return default


def get_settings() -> Settings:
    batch_job_ttl_seconds = _get_int("BATCH_JOB_TTL_SECONDS", 3600)
    batch_results_ttl_seconds = _get_int("BATCH_RESULTS_TTL_SECONDS", batch_job_ttl_seconds)
    return Settings(
        redis_url=os.getenv("REDIS_URL"),
        batch_concurrency=_get_int("BATCH_CONCURRENCY", 5),
        batch_max_items=_get_int("BATCH_MAX_ITEMS", 100),
        batch_job_ttl_seconds=batch_job_ttl_seconds,
        batch_results_ttl_seconds=batch_results_ttl_seconds,
        async_reports=_get_bool("ASYNC_REPORTS", False),
        report_job_ttl_seconds=_get_int("REPORT_JOB_TTL_SECONDS", 3600),
        report_job_max_result_bytes=_get_int("REPORT_JOB_MAX_RESULT_BYTES", 2 * 1024 * 1024),
        report_jobs_dir=os.getenv("REPORT_JOBS_DIR", "./report_results"),
        report_job_max_concurrency=_get_int("REPORT_JOB_MAX_CONCURRENCY", 2),
        report_job_queue_max_size=_get_int("REPORT_JOB_QUEUE_MAX_SIZE", 100),
        report_job_poll_interval_ms=_get_int("REPORT_JOB_POLL_INTERVAL_MS", 200),
        report_streaming=_get_bool("REPORT_STREAMING", False),
        report_streaming_on_limit=_get_bool("REPORT_STREAMING_ON_LIMIT", True),
        report_chunk_size=_get_int("REPORT_CHUNK_SIZE", 1000),
        report_streaming_max_groups=_get_int_allow_zero("REPORT_STREAMING_MAX_GROUPS", 200000),
        report_streaming_max_unique_values_per_dim=_get_int_allow_zero(
            "REPORT_STREAMING_MAX_UNIQUE_VALUES_PER_DIM", 0
        ),
        report_streaming_max_records=_get_int_allow_zero("REPORT_STREAMING_MAX_RECORDS", 0),
        report_paging_allowlist=os.getenv("REPORT_PAGING_ALLOWLIST"),
        report_paging_max_pages=_get_int("REPORT_PAGING_MAX_PAGES", 2000),
        report_upstream_paging=_get_bool("REPORT_UPSTREAM_PAGING", False),
        report_join_lookup_max_keys=_get_int("REPORT_JOIN_LOOKUP_MAX_KEYS", 2_000_000),
        report_join_max_records=_get_int_allow_zero("REPORT_JOIN_MAX_RECORDS", 0),
        report_join_source_max_records=_get_int_allow_zero("REPORT_JOIN_SOURCE_MAX_RECORDS", 0),
        report_upstream_pushdown=_get_bool("REPORT_UPSTREAM_PUSHDOWN", False),
        report_pushdown_allowlist=os.getenv("REPORT_PUSHDOWN_ALLOWLIST"),
        report_pushdown_max_filters=_get_int("REPORT_PUSHDOWN_MAX_FILTERS", 50),
        report_pushdown_max_in_values=_get_int("REPORT_PUSHDOWN_MAX_IN_VALUES", 200),
        report_pushdown_safe_only=_get_bool("REPORT_PUSHDOWN_SAFE_ONLY", True),
        report_pushdown_override=_get_bool("REPORT_PUSHDOWN_OVERRIDE", False),
        upstream_base_url=os.getenv("UPSTREAM_BASE_URL", ""),
        upstream_url=os.getenv("UPSTREAM_URL", ""),
        upstream_timeout=_get_float("UPSTREAM_TIMEOUT", 30.0),
        upstream_allowlist=os.getenv("UPSTREAM_ALLOWLIST"),
    )
