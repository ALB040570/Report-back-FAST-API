import asyncio
import logging
import os
import time
from typing import Optional

from app.observability.metrics import record_report_view_metrics
from app.observability.otel import get_tracer
from app.config import get_settings
from app.models.view import ChartConfig, ViewResponse
from app.models.view_request import ViewRequest
from app.services.data_source_client import async_iter_records, async_load_records, get_records_limit
from app.services.filter_service import apply_filters
from app.services.join_service import (
    apply_joins,
    apply_prepared_join_lookups,
    prepare_joins_streaming,
)
from app.services.pivot_streaming import StreamingPivotAggregator
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
    settings = get_settings()
    tracer = get_tracer()
    if settings.report_streaming:
        return await _build_report_view_streaming(payload, request_id)

    max_records = get_records_limit()

    load_started = time.monotonic()
    stats: dict = {}
    with tracer.start_as_current_span("load_records") as span:
        records = await async_load_records(
            payload.remoteSource,
            payload_filters=payload.filters,
            stats=stats,
        )
        span.set_attribute("streaming_enabled", False)
        span.set_attribute("records_count", len(records))
        span.set_attribute("pages_count", 1 if records else 0)
        span.set_attribute("pushdown_enabled", bool(stats.get("pushdown_enabled")))
        span.set_attribute("pushdown_filters_applied", int(stats.get("pushdown_filters_applied") or 0))
        span.set_attribute("pushdown_paging_applied", bool(stats.get("pushdown_paging_applied")))
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
    with tracer.start_as_current_span("apply_joins") as span:
        joined_records, join_debug = await apply_joins(
            records,
            payload.remoteSource,
            max_records=max_records,
        )
        span.set_attribute("streaming_enabled", False)
        span.set_attribute("records_count", len(joined_records))
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
    with tracer.start_as_current_span("apply_filters") as span:
        filtered_records, filter_debug = apply_filters(
            joined_records,
            payload.snapshot,
            payload.filters,
        )
        span.set_attribute("streaming_enabled", False)
        span.set_attribute("records_count", len(filtered_records))
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
    with tracer.start_as_current_span("build_pivot") as span:
        pivot_view = await asyncio.to_thread(build_view, filtered_records, payload.snapshot)
        span.set_attribute("streaming_enabled", False)
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
    record_report_view_metrics(len(records), 1 if records else 0, pivot_view)

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


def _init_join_debug(prepared_joins: list) -> dict:
    return {
        "joinsApplied": [
            {
                "joinId": prepared.join.get("id"),
                "targetSourceId": prepared.target_source_id,
                "baseBefore": 0,
                "baseAfter": 0,
                "matchedRows": 0,
            }
            for prepared in prepared_joins
        ],
        "sampleKeys": {"beforeJoin": [], "afterJoin": []},
    }


def _merge_join_debug(target: dict, source: dict) -> None:
    if not target["sampleKeys"]["beforeJoin"] and source["sampleKeys"].get("beforeJoin"):
        target["sampleKeys"]["beforeJoin"] = source["sampleKeys"]["beforeJoin"]
    if not target["sampleKeys"]["afterJoin"] and source["sampleKeys"].get("afterJoin"):
        target["sampleKeys"]["afterJoin"] = source["sampleKeys"]["afterJoin"]
    for idx, entry in enumerate(source.get("joinsApplied", [])):
        if idx >= len(target["joinsApplied"]):
            break
        target_entry = target["joinsApplied"][idx]
        target_entry["baseBefore"] += entry.get("baseBefore", 0)
        target_entry["baseAfter"] += entry.get("baseAfter", 0)
        target_entry["matchedRows"] += entry.get("matchedRows", 0)


def _merge_filter_debug(target: dict | None, source: dict) -> dict:
    if target is None:
        payload = {
            "counts": dict(source.get("counts") or {}),
            "effectiveFilters": source.get("effectiveFilters"),
            "appliedKeys": source.get("appliedKeys"),
            "sampleRecordKeys": dict(source.get("sampleRecordKeys") or {}),
        }
        if "reasonsDropped" in source:
            payload["reasonsDropped"] = list(source["reasonsDropped"])
        return payload

    target_counts = target.setdefault("counts", {})
    source_counts = source.get("counts") or {}
    target_counts["beforeFilters"] = target_counts.get("beforeFilters", 0) + source_counts.get("beforeFilters", 0)
    target_counts["afterFilters"] = target_counts.get("afterFilters", 0) + source_counts.get("afterFilters", 0)

    target_samples = target.setdefault("sampleRecordKeys", {})
    source_samples = source.get("sampleRecordKeys") or {}
    if not target_samples.get("beforeFilters") and source_samples.get("beforeFilters"):
        target_samples["beforeFilters"] = source_samples.get("beforeFilters")
    if not target_samples.get("afterFilters") and source_samples.get("afterFilters"):
        target_samples["afterFilters"] = source_samples.get("afterFilters")

    if source.get("reasonsDropped"):
        target_reasons = target.setdefault("reasonsDropped", [])
        for reason in source.get("reasonsDropped", []):
            if len(target_reasons) >= 2:
                break
            target_reasons.append(reason)

    return target


async def _build_report_view_streaming(
    payload: ViewRequest,
    request_id: str | None = None,
) -> ViewResponse:
    settings = get_settings()
    tracer = get_tracer()
    max_records = get_records_limit()
    chunk_size = settings.report_chunk_size
    prepared_joins = await prepare_joins_streaming(
        payload.remoteSource,
        chunk_size,
        max_records=max_records,
        lookup_max_keys=settings.report_join_lookup_max_keys,
        paging_allowlist=settings.report_paging_allowlist,
        paging_max_pages=settings.report_paging_max_pages,
        paging_force=settings.report_upstream_paging,
    )
    join_debug = _init_join_debug(prepared_joins)
    filter_debug = None

    aggregator = StreamingPivotAggregator(
        payload.snapshot,
        max_groups=settings.report_streaming_max_groups,
        max_unique_values_per_dim=settings.report_streaming_max_unique_values_per_dim,
    )

    total_records = 0
    total_joined = 0
    total_filtered = 0
    join_duration_ms = 0
    filter_duration_ms = 0
    update_duration_ms = 0
    pipeline_started = time.monotonic()

    paging_stats: dict = {}
    paging_enabled = False
    paging_pages = 0
    pages_count = 0
    with tracer.start_as_current_span("load_records") as load_span:
        async for records_chunk in async_iter_records(
            payload.remoteSource,
            chunk_size,
            payload_filters=payload.filters,
            paging_allowlist=settings.report_paging_allowlist,
            paging_max_pages=settings.report_paging_max_pages,
            paging_force=settings.report_upstream_paging,
            stats=paging_stats,
        ):
            total_records += len(records_chunk)
            _enforce_records_limit(total_records, max_records, "load_records")

            joins_started = time.monotonic()
            with tracer.start_as_current_span("apply_joins") as span:
                if prepared_joins:
                    joined_chunk, chunk_join_debug = apply_prepared_join_lookups(
                        records_chunk,
                        prepared_joins,
                        max_records=max_records,
                    )
                    _merge_join_debug(join_debug, chunk_join_debug)
                else:
                    joined_chunk = records_chunk
                    if not join_debug["sampleKeys"]["beforeJoin"] and records_chunk:
                        join_debug["sampleKeys"]["beforeJoin"] = list(records_chunk[0].keys())
                    if not join_debug["sampleKeys"]["afterJoin"] and records_chunk:
                        join_debug["sampleKeys"]["afterJoin"] = list(records_chunk[0].keys())
                span.set_attribute("streaming_enabled", True)
                span.set_attribute("records_count", len(joined_chunk))
            join_duration_ms += int((time.monotonic() - joins_started) * 1000)

            total_joined += len(joined_chunk)
            _enforce_records_limit(total_joined, max_records, "apply_joins")

            filters_started = time.monotonic()
            with tracer.start_as_current_span("apply_filters") as span:
                filtered_chunk, chunk_filter_debug = apply_filters(
                    joined_chunk,
                    payload.snapshot,
                    payload.filters,
                )
                span.set_attribute("streaming_enabled", True)
                span.set_attribute("records_count", len(filtered_chunk))
            filter_duration_ms += int((time.monotonic() - filters_started) * 1000)
            filter_debug = _merge_filter_debug(filter_debug, chunk_filter_debug)

            total_filtered += len(filtered_chunk)

            update_started = time.monotonic()
            with tracer.start_as_current_span("streaming_pivot") as span:
                aggregator.update(filtered_chunk)
                span.set_attribute("streaming_enabled", True)
                span.set_attribute("records_count", len(filtered_chunk))
            update_duration_ms += int((time.monotonic() - update_started) * 1000)
        paging_enabled = paging_stats.get("paging_enabled", False)
        paging_pages = paging_stats.get("paging_pages", 0)
        pages_count = paging_pages if paging_enabled else (1 if total_records else 0)
        load_span.set_attribute("streaming_enabled", True)
        load_span.set_attribute("records_count", total_records)
        load_span.set_attribute("pages_count", pages_count)
        load_span.set_attribute("pushdown_enabled", bool(paging_stats.get("pushdown_enabled")))
        load_span.set_attribute(
            "pushdown_filters_applied",
            int(paging_stats.get("pushdown_filters_applied") or 0),
        )
        load_span.set_attribute("pushdown_paging_applied", bool(paging_stats.get("pushdown_paging_applied")))

    total_duration_ms = int((time.monotonic() - pipeline_started) * 1000)
    load_duration_ms = max(0, total_duration_ms - join_duration_ms - filter_duration_ms - update_duration_ms)

    logger.info(
        "report.view.load_records",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "records": total_records,
            "duration_ms": load_duration_ms,
            "paging_enabled": paging_enabled,
            "paging_pages": paging_pages,
        },
    )

    logger.info(
        "report.view.apply_joins",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "recordsBefore": total_records,
            "recordsAfter": total_joined,
            "duration_ms": join_duration_ms,
            "join_streaming_enabled": True,
        },
    )

    logger.info(
        "report.view.apply_filters",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "recordsBefore": total_joined,
            "recordsAfter": total_filtered,
            "duration_ms": filter_duration_ms,
        },
    )

    pivot_started = time.monotonic()
    with tracer.start_as_current_span("build_pivot") as span:
        pivot_view = await asyncio.to_thread(aggregator.finalize)
        span.set_attribute("streaming_enabled", True)
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
    record_report_view_metrics(total_records, pages_count, pivot_view)

    debug_payload = None
    if os.getenv("REPORT_DEBUG_FILTERS"):
        if filter_debug is None:
            filter_debug = {
                "counts": {"beforeFilters": total_joined, "afterFilters": total_filtered},
                "effectiveFilters": {},
                "appliedKeys": {"values": [], "ranges": []},
                "sampleRecordKeys": {},
            }
        filter_debug.setdefault("counts", {})
        filter_debug["counts"]["beforeJoin"] = total_records
        filter_debug["counts"]["afterJoin"] = total_joined
        filter_debug.setdefault("sampleRecordKeys", {})
        filter_debug["sampleRecordKeys"].setdefault(
            "beforeJoin", join_debug.get("sampleKeys", {}).get("beforeJoin", [])
        )
        filter_debug["sampleRecordKeys"].setdefault(
            "afterJoin", join_debug.get("sampleKeys", {}).get("afterJoin", [])
        )
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
