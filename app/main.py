from typing import Any, Dict

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(
    title="Report Back FastAPI",
    description="Бэкенд для конструктора дашбордов (Service360)",
    version="0.1.0",
)


@app.get("/health", tags=["system"])
async def health_check():
    return {"status": "ok"}


class TestViewRequest(BaseModel):
    message: str | None = "test"


class TestViewResponse(BaseModel):
    view: Dict[str, Any]


@app.post("/api/report/view/test", response_model=TestViewResponse, tags=["report"])
async def build_test_view(payload: TestViewRequest):
    """
    Временный тестовый endpoint для проверки, что бэкенд жив.
    Пока возвращает заглушку view.
    """
    return TestViewResponse(
        view={
            "columns": [
                {"key": "col1", "label": "Column 1"},
                {"key": "col2", "label": "Column 2"},
            ],
            "rows": [
                {"key": "row1", "cells": {"col1": "Hello", "col2": "World"}},
                {"key": "row2", "cells": {"col1": "FastAPI", "col2": "is alive"}},
            ],
            "meta": {
                "message": payload.message or "no message",
            },
        }
    )
