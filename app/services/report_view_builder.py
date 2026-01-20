import asyncio
import logging
import os
import time
from typing import Optional

from app.models.view import ChartConfig, ViewResponse
from app.models.view_request import ViewRequest
from app.services.data_source_client import async_load_records, get_records_limit
from app.services.filter_service import apply_filters
from app.services.join_service import apply_joins
from app.services.view_service import build_view


logger = logging.getLogger(__name__)


def _enforce_records_limit(count: int, limit: Optional[int], stage: str) -> None:
    if limit is None:
        return
    if count > limit:
        raise ValueError(f"Records limit exceeded after {stage}: {count} > {limit}")


async def build_report_view_response(
    payload: ViewRequest,
    request_id: str | None = None,
) -> ViewResponse:
    max_records = get_records_limit()

    load_started = time.monotonic()
    records = await async_load_records(payload.remoteSource)
    _enforce_records_limit(len(records), max_records, "load_records")
    logger.info(
        "report.view.load_records",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "records": len(records),
            "duration_ms": int((time.monotonic() - load_started) * 1000),
        },
    )

    joins_started = time.monotonic()
    joined_records, join_debug = await apply_joins(
        records,
        payload.remoteSource,
        max_records=max_records,
    )
    _enforce_records_limit(len(joined_records), max_records, "apply_joins")
    logger.info(
        "report.view.apply_joins",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "recordsBefore": len(records),
            "recordsAfter": len(joined_records),
            "duration_ms": int((time.monotonic() - joins_started) * 1000),
        },
    )

    filters_started = time.monotonic()
    filtered_records, filter_debug = apply_filters(
        joined_records,
        payload.snapshot,
        payload.filters,
    )
    logger.info(
        "report.view.apply_filters",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "recordsBefore": len(joined_records),
            "recordsAfter": len(filtered_records),
            "duration_ms": int((time.monotonic() - filters_started) * 1000),
        },
    )

    pivot_started = time.monotonic()
    pivot_view = await asyncio.to_thread(build_view, filtered_records, payload.snapshot)
    logger.info(
        "report.view.build_pivot",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "rows": len(pivot_view.get("rows", [])),
            "columns": len(pivot_view.get("columns", [])),
            "duration_ms": int((time.monotonic() - pivot_started) * 1000),
        },
    )

    chart_config = ChartConfig(
        type="table",
        data={
            "rowCount": len(pivot_view.get("rows", [])),
            "columnCount": len(pivot_view.get("columns", [])),
        },
        options={},
    )

    debug_payload = None
    if os.getenv("REPORT_DEBUG_FILTERS"):
        filter_debug.setdefault("counts", {})
        filter_debug["counts"]["beforeJoin"] = len(records)
        filter_debug["counts"]["afterJoin"] = len(joined_records)
        filter_debug.setdefault("sampleRecordKeys", {})
        filter_debug["sampleRecordKeys"]["beforeJoin"] = join_debug.get("sampleKeys", {}).get("beforeJoin", [])
        filter_debug["sampleRecordKeys"]["afterJoin"] = join_debug.get("sampleKeys", {}).get("afterJoin", [])
        debug_payload = filter_debug
    if os.getenv("REPORT_DEBUG_JOINS"):
        if debug_payload is None:
            debug_payload = join_debug
        else:
            debug_payload["joins"] = join_debug

    return ViewResponse(
        view=pivot_view,
        chart=chart_config,
        debug=debug_payload,
    )
