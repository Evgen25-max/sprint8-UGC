import os

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from core.config import config

_configured = False


def configure_tracer(service_name: str) -> None:
    global _configured
    if _configured:
        return
    provider = TracerProvider(
        resource=Resource(attributes={SERVICE_NAME: service_name}),
        sampler=ALWAYS_ON,
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(
        endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', config.OTEL_EXPORTER_OTLP_ENDPOINT),
        insecure=True,
    )))
    trace.set_tracer_provider(provider)
    propagate.set_global_textmap(TraceContextTextMapPropagator())
    RedisInstrumentor().instrument()
    _configured = True
