import json
import logging
import os
import time
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.batch import router as batch_router
from app.config import get_settings
from app.models.view_request import ViewRequest
from app.models.view import ViewResponse
from app.models.report_job import ReportJobQueuedResponse
from app.observability.metrics import record_http_request, set_report_jobs_queue_size
from app.observability.otel import configure_otel
from app.observability.request_context import set_request_id
from app.services.computed_fields import build_computed_fields_engine
from app.services.data_source_client import async_load_records, get_records_limit
from app.services.detail_service import build_details
from app.services.filter_service import apply_filters, collect_filter_options
from app.services.join_service import apply_joins, resolve_joins
from app.services.record_cache import build_records_cache_key, get_cached_records, set_cached_records
from app.services.report_job_service import (
    QueueFullError,
    create_report_job,
    delete_report_job,
    get_report_job,
    get_report_job_store,
)
from app.services.report_view_builder import build_report_view_response


app = FastAPI(
    title="Report Back FastAPI",
    description="Бэкенд для конструктора дашбордов (Service360)",
    version="0.1.0",
)

logger = logging.getLogger(__name__)
configure_otel(app)


def _get_cors_allow_origins() -> list[str]:
    value = os.getenv("CORS_ALLOW_ORIGINS")
    if value is None or not value.strip():
        return [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://192.168.1.81:5173",
        ]
    if value.strip() == "*":
        return ["*"]
    return [item.strip() for item in value.split(",") if item.strip()]


def _enforce_records_limit(count: int, limit: int | None, stage: str) -> None:
    if limit is None:
        return
    if count > limit:
        raise HTTPException(
            status_code=422,
            detail=f"Records limit exceeded after {stage}: {count} > {limit}",
        )


def _extract_request_id_from_body(body: bytes, content_type: str) -> str | None:
    if not body or "application/json" not in (content_type or ""):
        return None
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return None
    return meta.get("requestId") or meta.get("request_id")


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    started = time.monotonic()
    request_id = request.headers.get("X-Request-ID")
    if request_id is None:
        body_bytes = await request.body()
        if body_bytes:
            request._body = body_bytes
            request_id = _extract_request_id_from_body(body_bytes, request.headers.get("content-type", ""))
    request.state.request_id = request_id
    set_request_id(request_id)
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        duration = time.monotonic() - started
        route = request.scope.get("route")
        route_path = route.path if route else request.url.path
        status_code = response.status_code if response else 500
        record_http_request(request.method, route_path, status_code, duration)
        logger.info(
            "http.request",
            extra={
                "requestId": request_id,
                "status": status_code,
                "duration_ms": int(duration * 1000),
            },
        )
        set_request_id(None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_cors_allow_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(batch_router)


@app.get("/health", tags=["system"])
async def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    queue_size = 0
    try:
        queue_size = await get_report_job_store().queue_size()
    except Exception:
        queue_size = 0
    set_report_jobs_queue_size(queue_size)
    payload = generate_latest()
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


@app.post(
    "/api/report/view",
    response_model=ViewResponse,
    tags=["report"],
    responses={
        202: {
            "model": ReportJobQueuedResponse,
            "description": "Async report queued",
        },
        429: {"description": "Report job queue is full"},
    },
)
async def build_report_view(payload: ViewRequest, request: Request) -> ViewResponse:
    """
    Основной endpoint для конструктора дашбордов.
    1. Загружает сырые записи из remoteSource.
    2. Строит простое представление (pivot) на их основе.
    3. Возвращает view + простейший chartConfig.
    """
    settings = get_settings()
    request_id = getattr(request.state, "request_id", None)
    force_sync = request.query_params.get("sync") == "1" or request.headers.get("X-Report-Sync") == "1"

    if settings.async_reports and not force_sync:
        try:
            job_id = await create_report_job(payload.dict(), request_id=request_id)
        except QueueFullError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        return JSONResponse(status_code=202, content={"job_id": job_id, "status": "queued"})

    try:
        response = await build_report_view_response(payload, request_id=request_id)
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
    return response


@app.post("/api/report/filters", tags=["report"])
async def build_report_filters(payload: ViewRequest, request: Request, limit: int = 200) -> Dict[str, Any]:
    """
    Endpoint для взаимозависимых фильтров (cascading filters).
    Возвращает доступные значения для каждого ключа фильтра.
    """
    request_id = getattr(request.state, "request_id", None)
    max_records = get_records_limit()
    computed_engine = build_computed_fields_engine(payload.remoteSource)

    try:
        joins = await resolve_joins(payload.remoteSource)
        cache_key = build_records_cache_key(payload.templateId, payload.remoteSource, joins)
        joined_records = await get_cached_records(cache_key)
        join_debug: Dict[str, Any] = {}
        cache_hit = joined_records is not None
        if joined_records is None:
            load_started = time.monotonic()
            records = await async_load_records(
                payload.remoteSource,
                payload_filters=None,
                pushdown_enabled=False,
            )
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
            if computed_engine:
                computed_engine.apply(joined_records)
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
            if computed_engine:
                computed_engine.apply(joined_records)
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
    if computed_engine and computed_engine.warnings:
        response["computedWarnings"] = computed_engine.warnings
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
        debug["pushdownDisabled"] = True
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

    request_id = getattr(request.state, "request_id", None)
    max_records = get_records_limit()
    computed_engine = build_computed_fields_engine(view_payload.remoteSource)

    try:
        joins = await resolve_joins(view_payload.remoteSource)
        cache_key = build_records_cache_key(
            view_payload.templateId,
            view_payload.remoteSource,
            joins,
            view_payload.filters,
        )
        joined_records = await get_cached_records(cache_key)
        join_debug: Dict[str, Any] = {}
        cache_hit = joined_records is not None
        if joined_records is None:
            load_started = time.monotonic()
            records = await async_load_records(view_payload.remoteSource, payload_filters=view_payload.filters)
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
            if computed_engine:
                computed_engine.apply(joined_records)
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
            if computed_engine:
                computed_engine.apply(joined_records)

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
            extra={"templateId": view_payload.templateId, "requestId": request_id},
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
    if computed_engine and computed_engine.warnings:
        response["computedWarnings"] = computed_engine.warnings

    return response


@app.get("/api/report/jobs/{job_id}", tags=["report"])
async def get_report_job_status(job_id: str) -> Dict[str, Any]:
    job = await get_report_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.delete("/api/report/jobs/{job_id}", status_code=204, tags=["report"])
async def delete_report_job_status(job_id: str) -> Response:
    deleted = await delete_report_job(job_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="job not found")
    return Response(status_code=204)
