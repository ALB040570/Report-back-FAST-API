from __future__ import annotations

from typing import Any

from prometheus_client import Counter, Gauge, Histogram


HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "route", "status"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "route", "status"],
)

UPSTREAM_REQUEST_TOTAL = Counter(
    "upstream_request_total",
    "Total upstream HTTP requests",
    ["status"],
)
UPSTREAM_REQUEST_DURATION_SECONDS = Histogram(
    "upstream_request_duration_seconds",
    "Upstream HTTP request duration in seconds",
    ["status"],
)

REPORT_VIEW_RECORDS_TOTAL = Counter(
    "report_view_records_total",
    "Total records processed for report view",
)
REPORT_VIEW_PAGES_TOTAL = Counter(
    "report_view_pages_total",
    "Total pages processed for report view",
)
REPORT_VIEW_GROUPS_TOTAL = Counter(
    "report_view_groups_total",
    "Total groups emitted for report view",
)

REPORT_PUSHDOWN_REQUESTS_TOTAL = Counter(
    "report_pushdown_requests_total",
    "Total upstream pushdown attempts",
    ["enabled", "result"],
)

REPORT_JOBS_QUEUE_SIZE = Gauge(
    "report_jobs_queue_size",
    "Current report jobs queue size",
)


def record_http_request(method: str, route: str, status: int, duration_seconds: float) -> None:
    label_status = str(status)
    HTTP_REQUESTS_TOTAL.labels(method=method, route=route, status=label_status).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method=method, route=route, status=label_status).observe(duration_seconds)


def record_upstream_request(status: str, duration_seconds: float) -> None:
    UPSTREAM_REQUEST_TOTAL.labels(status=status).inc()
    UPSTREAM_REQUEST_DURATION_SECONDS.labels(status=status).observe(duration_seconds)


def record_report_view_metrics(
    records_count: int,
    pages_count: int,
    view_payload: dict[str, Any] | None,
) -> None:
    if records_count > 0:
        REPORT_VIEW_RECORDS_TOTAL.inc(records_count)
    if pages_count > 0:
        REPORT_VIEW_PAGES_TOTAL.inc(pages_count)
    rows_count = 0
    cols_count = 0
    if isinstance(view_payload, dict):
        rows = view_payload.get("rows")
        cols = view_payload.get("columns")
        rows_count = len(rows) if isinstance(rows, list) else 0
        cols_count = len(cols) if isinstance(cols, list) else 0
    groups_total = rows_count * (cols_count if cols_count > 0 else 1) if rows_count > 0 else 0
    if groups_total > 0:
        REPORT_VIEW_GROUPS_TOTAL.inc(groups_total)


def record_pushdown_request(enabled: bool, result: str) -> None:
    REPORT_PUSHDOWN_REQUESTS_TOTAL.labels(enabled="1" if enabled else "0", result=result).inc()


def set_report_jobs_queue_size(value: int) -> None:
    REPORT_JOBS_QUEUE_SIZE.set(value)
