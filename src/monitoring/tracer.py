"""Minimal tracing support (No dependencies)."""

import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class Tracer:
    """NOOP tracer (OpenTelemetry disabled)."""

    def __init__(self, service_name: str = "bq2pg", **kwargs):
        self.service_name = service_name
        self.enabled = False

    def get_tracer(self) -> Any:
        return None

    @contextmanager
    def trace_span(self, span_name: str, attributes: Optional[Dict[str, Any]] = None):
        """NOOP span context manager."""
        yield None

    def trace_function(self, func_name: str, **attributes):
        """NOOP function decorator."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def add_span_event(self, span_name: str, event_name: str, attributes: Optional[Dict] = None) -> None:
        pass

    def get_spans_summary(self) -> Dict[str, Any]:
        return {"service_name": self.service_name, "enabled": False}
