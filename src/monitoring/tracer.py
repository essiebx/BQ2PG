"""Distributed tracing support."""

import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
import time
import uuid

from opentelemetry import trace, context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

logger = logging.getLogger(__name__)


class Tracer:
    """Manages distributed tracing."""

    def __init__(
        self,
        service_name: str = "bq2pg",
        jaeger_host: str = "localhost",
        jaeger_port: int = 6831,
        enabled: bool = True,
    ):
        """Initialize tracer.

        Args:
            service_name: Name of the service.
            jaeger_host: Jaeger agent host.
            jaeger_port: Jaeger agent port.
            enabled: Whether tracing is enabled.
        """
        self.service_name = service_name
        self.enabled = enabled
        self.spans: Dict[str, Any] = {}

        if enabled:
            try:
                jaeger_exporter = JaegerExporter(
                    agent_host_name=jaeger_host,
                    agent_port=jaeger_port,
                )

                resource = Resource(attributes={SERVICE_NAME: service_name})

                trace.set_tracer_provider(
                    TracerProvider(resource=resource)
                )

                trace.get_tracer_provider().add_span_processor(
                    SimpleSpanProcessor(jaeger_exporter)
                )

                logger.info(f"Tracing enabled for {service_name}")
            except Exception as e:
                logger.warning(f"Failed to initialize Jaeger tracer: {e}. Tracing disabled.")
                self.enabled = False

    def get_tracer(self) -> Any:
        """Get OpenTelemetry tracer.

        Returns:
            Tracer instance.
        """
        return trace.get_tracer(__name__)

    @contextmanager
    def trace_span(self, span_name: str, attributes: Optional[Dict[str, Any]] = None):
        """Context manager for creating spans.

        Args:
            span_name: Name of the span.
            attributes: Additional span attributes.

        Yields:
            Span object.
        """
        if not self.enabled:
            yield None
            return

        tracer = self.get_tracer()
        span_id = str(uuid.uuid4())

        with tracer.start_as_current_span(span_name) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, str(value))

            span.set_attribute("span_id", span_id)
            self.spans[span_id] = span

            start_time = time.time()
            try:
                yield span
            except Exception as e:
                span.set_attribute("error", True)
                span.set_attribute("error_type", type(e).__name__)
                span.set_attribute("error_message", str(e))
                raise
            finally:
                duration = time.time() - start_time
                span.set_attribute("duration_ms", int(duration * 1000))
                del self.spans[span_id]

    def trace_function(self, func_name: str, **attributes):
        """Decorator to trace function execution.

        Args:
            func_name: Name of the function for tracing.
            **attributes: Additional span attributes.

        Returns:
            Decorator function.
        """

        def decorator(func):
            def wrapper(*args, **kwargs):
                with self.trace_span(func_name, attributes):
                    return func(*args, **kwargs)

            return wrapper

        return decorator

    def add_span_event(self, span_name: str, event_name: str, attributes: Optional[Dict] = None) -> None:
        """Add event to span.

        Args:
            span_name: Span to add event to.
            event_name: Name of the event.
            attributes: Event attributes.
        """
        if not self.enabled:
            return

        try:
            current_span = trace.get_current_span()
            if current_span:
                if attributes:
                    current_span.add_event(event_name, attributes)
                else:
                    current_span.add_event(event_name)
        except Exception as e:
            logger.warning(f"Failed to add span event: {e}")

    def get_spans_summary(self) -> Dict[str, Any]:
        """Get summary of recorded spans.

        Returns:
            Dictionary with spans information.
        """
        return {
            "service_name": self.service_name,
            "enabled": self.enabled,
            "active_spans": len(self.spans),
        }
