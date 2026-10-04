"""OpenTelemetry setup for the CDC consumer."""

import os
from typing import Optional

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "cdc-consumer")
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")


_initialized = False


def setup_telemetry() -> None:
    """Configure OpenTelemetry once for traces and metrics."""
    global _initialized

    if _initialized:
        return

    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
            "service.version": os.getenv("OTEL_SERVICE_VERSION", "1.0.0"),
            "deployment.environment": os.getenv("OTEL_ENVIRONMENT", "local"),
        }
    )

    trace_provider = TracerProvider(resource=resource)
    trace_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True)
        )
    )
    trace.set_tracer_provider(trace_provider)

    metric_exporter = OTLPMetricExporter(
        endpoint=OTLP_ENDPOINT,
        insecure=True,
    )
    metric_reader = PeriodicExportingMetricReader(
        metric_exporter,
        export_interval_millis=5000,
    )
    metric_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
    )
    metrics.set_meter_provider(metric_provider)

    _initialized = True


def tracer():
    return trace.get_tracer("cdc-consumer")


def meter():
    return metrics.get_meter("cdc-consumer")
