# applications/consumer/tracing.py

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

from config import settings


def setup_tracing() -> trace.Tracer:
    """Setup OpenTelemetry tracing"""
    
    if not settings.otel_enabled:
        return trace.get_tracer(settings.otel_service_name)
    
    # Create resource with service info
    resource = Resource.create({
        ResourceAttributes.SERVICE_NAME: settings.otel_service_name,
        ResourceAttributes.SERVICE_VERSION: "2.0.0",
        ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "development",
    })
    
    # Create tracer provider
    provider = TracerProvider(resource=resource)
    
    # Create OTLP exporter
    otlp_exporter = OTLPSpanExporter(
        endpoint=settings.otel_exporter_otlp_endpoint,
        insecure=True
    )
    
    # Add span processor
    provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
    
    # Set global tracer provider
    trace.set_tracer_provider(provider)
    
    return trace.get_tracer(settings.otel_service_name)


# Create global tracer
tracer = setup_tracing()
