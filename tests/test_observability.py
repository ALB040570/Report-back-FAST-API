import asyncio
import logging
import os
import unittest

import httpx
from fastapi import FastAPI

from app.main import app
from app.observability.otel import configure_otel, reset_otel_for_tests


class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class ObservabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._otel_enabled = os.environ.get("OTEL_ENABLED")
        self._otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        self._otel_service = os.environ.get("OTEL_SERVICE_NAME")
        self._no_network = os.environ.get("NO_NETWORK")

    def tearDown(self) -> None:
        if self._otel_enabled is None:
            os.environ.pop("OTEL_ENABLED", None)
        else:
            os.environ["OTEL_ENABLED"] = self._otel_enabled
        if self._otel_endpoint is None:
            os.environ.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)
        else:
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = self._otel_endpoint
        if self._otel_service is None:
            os.environ.pop("OTEL_SERVICE_NAME", None)
        else:
            os.environ["OTEL_SERVICE_NAME"] = self._otel_service
        if self._no_network is None:
            os.environ.pop("NO_NETWORK", None)
        else:
            os.environ["NO_NETWORK"] = self._no_network
        reset_otel_for_tests()

    def test_metrics_endpoint(self) -> None:
        async def _get() -> httpx.Response:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                await client.get("/health")
                return await client.get("/metrics")

        response = asyncio.run(_get())
        self.assertEqual(response.status_code, 200)
        self.assertIn("http_requests_total", response.text)

    def test_otel_disabled_no_network(self) -> None:
        os.environ["OTEL_ENABLED"] = "0"
        os.environ["NO_NETWORK"] = "1"
        reset_otel_for_tests()
        mode = configure_otel(FastAPI())
        self.assertEqual(mode, "disabled")

    def test_otel_otlp_disabled_with_no_network(self) -> None:
        os.environ["OTEL_ENABLED"] = "1"
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://collector:4318"
        os.environ["NO_NETWORK"] = "1"
        reset_otel_for_tests()

        handler = _LogCapture()
        logger = logging.getLogger("app.observability.otel")
        previous_level = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        try:
            mode = configure_otel(FastAPI())
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous_level)

        self.assertEqual(mode, "console")
        self.assertTrue(
            any("OTLP exporter disabled" in record.getMessage() for record in handler.records),
            "expected warning about OTLP exporter disabled",
        )
