import logging
import os
from typing import Optional

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor


logger = logging.getLogger(__name__)

_otel_configured = False
_otel_mode: Optional[str] = None


def configure_otel(app: FastAPI) -> str:
    global _otel_configured
    global _otel_mode

    enabled = os.getenv("OTEL_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        _otel_mode = "disabled"
        return _otel_mode
    if _otel_configured:
        return _otel_mode or "disabled"

    service_name = os.getenv("OTEL_SERVICE_NAME", "report-back-fast-api")
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    no_network = os.getenv("NO_NETWORK") == "1"

    if otlp_endpoint:
        if no_network:
            logger.warning(
                "OTLP exporter disabled because NO_NETWORK=1",
                extra={"otlp_endpoint": otlp_endpoint},
            )
            exporter = ConsoleSpanExporter()
            provider.add_span_processor(SimpleSpanProcessor(exporter))
            _otel_mode = "console"
        else:
            exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            _otel_mode = "otlp"
    else:
        exporter = ConsoleSpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        _otel_mode = "console"

    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()

    _otel_configured = True
    return _otel_mode or "disabled"


def get_tracer() -> trace.Tracer:
    return trace.get_tracer("report-back-fast-api")


def reset_otel_for_tests() -> None:
    global _otel_configured
    global _otel_mode
    _otel_configured = False
    _otel_mode = None
