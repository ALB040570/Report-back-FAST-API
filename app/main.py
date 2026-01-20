import logging
import os
import time
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from app.api.batch import router as batch_router
from app.models.view_request import ViewRequest
from app.models.view import ChartConfig, ViewResponse
from app.services.data_source_client import async_load_records, get_records_limit
from app.services.detail_service import build_details
from app.services.filter_service import apply_filters, collect_filter_options
from app.services.join_service import apply_joins, resolve_joins
from app.services.record_cache import build_records_cache_key, get_cached_records, set_cached_records
from app.services.view_service import build_view


app = FastAPI(
    title="Report Back FastAPI",
    description="Бэкенд для конструктора дашбордов (Service360)",
    version="0.1.0",
)

logger = logging.getLogger(__name__)


def _enforce_records_limit(count: int, limit: int | None, stage: str) -> None:
    if limit is None:
        return
    if count > limit:
        raise HTTPException(
            status_code=422,
            detail=f"Records limit exceeded after {stage}: {count} > {limit}",
        )

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://192.168.1.81:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(batch_router)


@app.get("/health", tags=["system"])
async def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/api/report/view", response_model=ViewResponse, tags=["report"])
async def build_report_view(payload: ViewRequest, request: Request) -> ViewResponse:
    """
    Основной endpoint для конструктора дашбордов.
    1. Загружает сырые записи из remoteSource.
    2. Строит простое представление (pivot) на их основе.
    3. Возвращает view + простейший chartConfig.
    """
    request_id = request.headers.get("X-Request-ID")
    max_records = get_records_limit()

    try:
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
        pivot_view = build_view(filtered_records, payload.snapshot)
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
    except HTTPException:
        raise
    except ValueError as exc:
        logger.warning(
            "Failed to build report view",
            extra={"templateId": payload.templateId, "requestId": request_id, "error": str(exc)},
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception(
            "Failed to build report view",
            extra={"templateId": payload.templateId, "requestId": request_id},
        )
        raise HTTPException(status_code=502, detail=f"Failed to build report view: {exc}") from exc

    # 3. Строим простейший chartConfig-заглушку
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


@app.post("/api/report/filters", tags=["report"])
async def build_report_filters(payload: ViewRequest, request: Request, limit: int = 200) -> Dict[str, Any]:
    """
    Endpoint для взаимозависимых фильтров (cascading filters).
    Возвращает доступные значения для каждого ключа фильтра.
    """
    request_id = request.headers.get("X-Request-ID")
    max_records = get_records_limit()

    try:
        joins = await resolve_joins(payload.remoteSource)
        cache_key = build_records_cache_key(payload.templateId, payload.remoteSource, joins)
        joined_records = await get_cached_records(cache_key)
        join_debug: Dict[str, Any] = {}
        cache_hit = joined_records is not None
        if joined_records is None:
            load_started = time.monotonic()
            records = await async_load_records(payload.remoteSource)
            _enforce_records_limit(len(records), max_records, "load_records")
            logger.info(
                "report.filters.load_records",
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
                joins_override=joins,
                max_records=max_records,
            )
            _enforce_records_limit(len(joined_records), max_records, "apply_joins")
            logger.info(
                "report.filters.apply_joins",
                extra={
                    "templateId": payload.templateId,
                    "requestId": request_id,
                    "recordsBefore": len(records),
                    "recordsAfter": len(joined_records),
                    "duration_ms": int((time.monotonic() - joins_started) * 1000),
                },
            )
            if joined_records:
                await set_cached_records(cache_key, joined_records)
        else:
            _enforce_records_limit(len(joined_records), max_records, "cache_records")
    except HTTPException:
        raise
    except ValueError as exc:
        logger.warning(
            "Failed to build report filters",
            extra={"templateId": payload.templateId, "requestId": request_id, "error": str(exc)},
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception(
            "Failed to build report filters",
            extra={"templateId": payload.templateId, "requestId": request_id},
        )
        raise HTTPException(status_code=502, detail=f"Failed to build report filters: {exc}") from exc

    env_limit = os.getenv("REPORT_FILTERS_MAX_VALUES")
    if env_limit and env_limit.isdigit() and limit == 200:
        limit = int(env_limit)
    if limit <= 0:
        limit = 200

    filters_started = time.monotonic()
    options, meta, truncated, selected_pruned, debug = collect_filter_options(
        joined_records,
        payload.snapshot,
        payload.filters,
        max_unique=limit,
    )
    logger.info(
        "report.filters.collect_options",
        extra={
            "templateId": payload.templateId,
            "requestId": request_id,
            "records": len(joined_records or []),
            "duration_ms": int((time.monotonic() - filters_started) * 1000),
        },
    )
    response: Dict[str, Any] = {
        "options": options,
        "meta": meta,
        "truncated": truncated,
    }
    if selected_pruned:
        response["selectedPruned"] = selected_pruned
    if os.getenv("REPORT_DEBUG_FILTERS"):
        filtered_records, _ = apply_filters(
            joined_records,
            payload.snapshot,
            payload.filters,
        )
        debug["recordsBeforeFilter"] = len(joined_records)
        debug["recordsAfterFilter"] = len(filtered_records)
        debug["truncated"] = truncated
        debug["cacheHit"] = cache_hit
        if selected_pruned:
            debug["selectedPruned"] = selected_pruned
        if join_debug:
            debug["joins"] = join_debug
        response["debug"] = debug
    return response


@app.post("/api/report/details", tags=["report"])
async def build_report_details(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Details payload must be a JSON object")

    template_id = payload.get("templateId")
    try:
        view_payload = ViewRequest(**payload)
    except ValidationError as exc:
        logger.warning(
            "Invalid details payload",
            extra={"templateId": template_id, "errors": exc.errors()},
        )
        raise HTTPException(
            status_code=400,
            detail={"message": "Invalid details payload", "errors": exc.errors()},
        ) from exc
    limit = payload.get("limit") if isinstance(payload, dict) else None
    offset = payload.get("offset") if isinstance(payload, dict) else None

    try:
        limit = int(limit) if limit is not None else 200
    except (TypeError, ValueError):
        limit = 200
    try:
        offset = int(offset) if offset is not None else 0
    except (TypeError, ValueError):
        offset = 0

    request_id = request.headers.get("X-Request-ID")
    max_records = get_records_limit()

    try:
        joins = await resolve_joins(view_payload.remoteSource)
        cache_key = build_records_cache_key(view_payload.templateId, view_payload.remoteSource, joins)
        joined_records = await get_cached_records(cache_key)
        join_debug: Dict[str, Any] = {}
        cache_hit = joined_records is not None
        if joined_records is None:
            load_started = time.monotonic()
            records = await async_load_records(view_payload.remoteSource)
            _enforce_records_limit(len(records), max_records, "load_records")
            logger.info(
                "report.details.load_records",
                extra={
                    "templateId": view_payload.templateId,
                    "requestId": request_id,
                    "records": len(records),
                    "duration_ms": int((time.monotonic() - load_started) * 1000),
                },
            )
            joins_started = time.monotonic()
            joined_records, join_debug = await apply_joins(
                records,
                view_payload.remoteSource,
                joins_override=joins,
                max_records=max_records,
            )
            _enforce_records_limit(len(joined_records), max_records, "apply_joins")
            logger.info(
                "report.details.apply_joins",
                extra={
                    "templateId": view_payload.templateId,
                    "requestId": request_id,
                    "recordsBefore": len(records),
                    "recordsAfter": len(joined_records),
                    "duration_ms": int((time.monotonic() - joins_started) * 1000),
                },
            )
            if joined_records:
                await set_cached_records(cache_key, joined_records)
        else:
            _enforce_records_limit(len(joined_records), max_records, "cache_records")

        details_started = time.monotonic()
        response, debug_payload = build_details(
            joined_records or [],
            view_payload.snapshot,
            view_payload.filters,
            payload,
            limit=limit,
            offset=offset,
            debug=bool(os.getenv("REPORT_DEBUG_FILTERS")),
        )
        logger.info(
            "report.details.build_details",
            extra={
                "templateId": view_payload.templateId,
                "requestId": request_id,
                "records": len(joined_records or []),
                "entries": len(response.get("entries", [])),
                "duration_ms": int((time.monotonic() - details_started) * 1000),
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Failed to build report details",
            extra={"templateId": view_payload.templateId},
        )
        raise HTTPException(
            status_code=422 if isinstance(exc, ValueError) else 502,
            detail=f"Failed to build report details: {exc}",
        ) from exc

    if os.getenv("REPORT_DEBUG_FILTERS"):
        debug_payload["cacheHit"] = cache_hit
        if join_debug:
            debug_payload["joins"] = join_debug
        response["debug"] = debug_payload

    return response
