from typing import Any, Dict

from fastapi import FastAPI

from app.models.view_request import ViewRequest
from app.models.view import ChartConfig, ViewResponse
from app.services.data_source_client import load_records
from app.services.view_service import build_view


app = FastAPI(
    title="Report Back FastAPI",
    description="Бэкенд для конструктора дашбордов (Service360)",
    version="0.1.0",
)


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

    # 2. Строим pivot-представление (пока без агрегатов, черновой вариант)
    pivot_view = build_view(records, payload.snapshot)

    # 3. Строим простейший chartConfig-заглушку
    chart_config = ChartConfig(
        type="table",
        data={
            "rowCount": len(pivot_view.get("rows", [])),
            "columnCount": len(pivot_view.get("columns", [])),
        },
        options={},
    )

    return ViewResponse(
        view=pivot_view,
        chart=chart_config,
    )
