"""Monitoring module for structured logging, metrics, tracing,
and instrumentation.
"""

from src.monitoring.structured_logger import StructuredLogger
from src.monitoring.metrics import MetricsCollector
from src.monitoring.tracer import Tracer
from src.monitoring.instrumentation import (
    instrument_function,
    instrument_class,
    monitor_operation,
    monitor_data_flow,
    log_operation_metrics,
    PerformanceTimer
)

# Singleton instances to avoid duplicate Prometheus registration
_metrics_instance = None
_tracer_instance = None


def get_metrics_collector(namespace: str = "bq2pg") -> MetricsCollector:
    """Get or create metrics collector singleton."""
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = MetricsCollector(namespace=namespace)
    return _metrics_instance


def get_tracer(service_name: str = "bq2pg") -> Tracer:
    """Get or create tracer singleton."""
    global _tracer_instance
    if _tracer_instance is None:
        _tracer_instance = Tracer(service_name=service_name)
    return _tracer_instance


__all__ = [
    "StructuredLogger",
    "MetricsCollector",
    "Tracer",
    "instrument_function",
    "instrument_class",
    "monitor_operation",
    "monitor_data_flow",
    "log_operation_metrics",
    "PerformanceTimer",
    "get_metrics_collector",
    "get_tracer",
]
