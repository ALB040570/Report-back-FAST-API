import hashlib
import json
import os
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.models.view_request import ViewRequest
from app.models.view import ChartConfig, ViewResponse
from app.services.data_source_client import load_records
from app.services.detail_service import build_details
from app.services.filter_service import apply_filters, collect_filter_options
from app.services.join_service import apply_joins, resolve_joins
from app.services.record_cache import get_cached_records, set_cached_records
from app.services.view_service import build_view


app = FastAPI(
    title="Report Back FastAPI",
    description="Бэкенд для конструктора дашбордов (Service360)",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _safe_json_payload(value: Any) -> Any:
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return value
    return str(value)


def _build_records_cache_key(
    template_id: str,
    remote_source: Any,
    joins: Any,
) -> str:
    cache_template_id = template_id or getattr(remote_source, "id", None) or getattr(remote_source, "remoteId", None) or ""
    payload = {
        "templateId": cache_template_id,
        "body": _safe_json_payload(getattr(remote_source, "body", None) or getattr(remote_source, "rawBody", None)),
        "joins": _safe_json_payload(joins),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@app.get("/health", tags=["system"])
async def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/api/report/view", response_model=ViewResponse, tags=["report"])
async def build_report_view(payload: ViewRequest) -> ViewResponse:
    """
    Основной endpoint для конструктора дашбордов.
    1. Загружает сырые записи из remoteSource.
    2. Строит простое представление (pivot) на их основе.
    3. Возвращает view + простейший chartConfig.
    """
    # 1. Загружаем записи из удалённого источника
    records = load_records(payload.remoteSource)

    # 2. Выполняем join-ы, фильтры и строим pivot-представление
    joined_records, join_debug = await apply_joins(records, payload.remoteSource)
    filtered_records, filter_debug = apply_filters(
        joined_records,
        payload.snapshot,
        payload.filters,
    )
    pivot_view = build_view(filtered_records, payload.snapshot)

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
async def build_report_filters(payload: ViewRequest, limit: int = 200) -> Dict[str, Any]:
    """
    Endpoint для взаимозависимых фильтров (cascading filters).
    Возвращает доступные значения для каждого ключа фильтра.
    """
    joins = await resolve_joins(payload.remoteSource)
    cache_key = _build_records_cache_key(payload.templateId, payload.remoteSource, joins)
    joined_records = get_cached_records(cache_key)
    join_debug: Dict[str, Any] = {}
    cache_hit = joined_records is not None
    if joined_records is None:
        records = load_records(payload.remoteSource)
        joined_records, join_debug = await apply_joins(
            records,
            payload.remoteSource,
            joins_override=joins,
        )
        if joined_records:
            set_cached_records(cache_key, joined_records)

    env_limit = os.getenv("REPORT_FILTERS_MAX_VALUES")
    if env_limit and env_limit.isdigit() and limit == 200:
        limit = int(env_limit)
    if limit <= 0:
        limit = 200

    options, meta, truncated, selected_pruned, debug = collect_filter_options(
        joined_records,
        payload.snapshot,
        payload.filters,
        max_unique=limit,
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
async def build_report_details(payload: Dict[str, Any]) -> Dict[str, Any]:
    view_payload = ViewRequest(**payload)
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

    joins = await resolve_joins(view_payload.remoteSource)
    cache_key = _build_records_cache_key(view_payload.templateId, view_payload.remoteSource, joins)
    joined_records = get_cached_records(cache_key)
    join_debug: Dict[str, Any] = {}
    cache_hit = joined_records is not None
    if joined_records is None:
        records = load_records(view_payload.remoteSource)
        joined_records, join_debug = await apply_joins(
            records,
            view_payload.remoteSource,
            joins_override=joins,
        )
        if joined_records:
            set_cached_records(cache_key, joined_records)

    response, debug_payload = build_details(
        joined_records or [],
        view_payload.snapshot,
        view_payload.filters,
        payload,
        limit=limit,
        offset=offset,
        debug=bool(os.getenv("REPORT_DEBUG_FILTERS")),
    )

    if os.getenv("REPORT_DEBUG_FILTERS"):
        debug_payload["cacheHit"] = cache_hit
        if join_debug:
            debug_payload["joins"] = join_debug
        response["debug"] = debug_payload

    return response
