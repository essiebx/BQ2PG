"""Metrics collection and reporting."""

import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from collections import defaultdict

from prometheus_client import Counter, Histogram, Gauge, generate_latest

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and manages application metrics."""

    def __init__(self, namespace: str = "bq2pg"):
        """Initialize metrics collector.

        Args:
            namespace: Metrics namespace.
        """
        self.namespace = namespace

        # Define prometheus metrics
        self.records_extracted = Counter(
            f"{namespace}_records_extracted_total",
            "Total records extracted from BigQuery",
        )
        self.records_loaded = Counter(
            f"{namespace}_records_loaded_total",
            "Total records loaded to PostgreSQL",
        )
        self.records_failed = Counter(
            f"{namespace}_records_failed_total",
            "Total records that failed to load",
            labelnames=["source"],
        )
        self.extraction_duration = Histogram(
            f"{namespace}_extraction_duration_seconds",
            "Time to extract records from BigQuery",
            buckets=(1, 5, 10, 30, 60, 120, 300),
        )
        self.load_duration = Histogram(
            f"{namespace}_load_duration_seconds",
            "Time to load records to PostgreSQL",
            buckets=(1, 5, 10, 30, 60, 120, 300),
        )
        self.batch_size = Histogram(
            f"{namespace}_batch_size",
            "Number of records per batch",
            buckets=(100, 1000, 5000, 10000, 50000, 100000),
        )
        self.pipeline_duration = Histogram(
            f"{namespace}_pipeline_duration_seconds",
            "Total pipeline execution time",
            buckets=(10, 30, 60, 300, 600, 1800, 3600),
        )
        self.database_pool_connections = Gauge(
            f"{namespace}_database_pool_connections",
            "Current database pool connections",
        )
        self.circuit_breaker_state = Gauge(
            f"{namespace}_circuit_breaker_state",
            "Circuit breaker state (0=closed, 1=open, 2=half_open)",
        )

        # In-memory metrics
        self.custom_metrics: Dict[str, Any] = defaultdict(float)
        self.start_time = datetime.utcnow()

    def record_extraction(self, record_count: int, duration_seconds: float) -> None:
        """Record extraction metrics.

        Args:
            record_count: Number of records extracted.
            duration_seconds: Time taken for extraction.
        """
        self.records_extracted.inc(record_count)
        self.extraction_duration.observe(duration_seconds)
        logger.info(f"Extracted {record_count} records in {duration_seconds:.2f}s")

    def record_load(self, record_count: int, duration_seconds: float, failed: int = 0) -> None:
        """Record load metrics.

        Args:
            record_count: Number of records loaded.
            duration_seconds: Time taken for loading.
            failed: Number of records that failed to load.
        """
        self.records_loaded.inc(record_count - failed)
        if failed > 0:
            self.records_failed.labels(source="load").inc(failed)
        self.load_duration.observe(duration_seconds)
        logger.info(f"Loaded {record_count - failed}/{record_count} records in {duration_seconds:.2f}s")

    def record_batch(self, batch_size: int) -> None:
        """Record batch metrics.

        Args:
            batch_size: Size of the batch.
        """
        self.batch_size.observe(batch_size)

    def record_pipeline_duration(self, duration_seconds: float) -> None:
        """Record total pipeline duration.

        Args:
            duration_seconds: Pipeline execution time.
        """
        self.pipeline_duration.observe(duration_seconds)

    def set_pool_connections(self, count: int) -> None:
        """Set current database pool connections.

        Args:
            count: Number of active connections.
        """
        self.database_pool_connections.set(count)

    def set_circuit_breaker_state(self, state: str) -> None:
        """Set circuit breaker state.

        Args:
            state: State name (closed=0, open=1, half_open=2).
        """
        state_map = {"closed": 0, "open": 1, "half_open": 2}
        self.circuit_breaker_state.set(state_map.get(state, 0))

    def set_custom_metric(self, name: str, value: float) -> None:
        """Set custom metric value.

        Args:
            name: Metric name.
            value: Metric value.
        """
        self.custom_metrics[name] = value

    def increment_custom_metric(self, name: str, amount: float = 1) -> None:
        """Increment custom metric.

        Args:
            name: Metric name.
            amount: Amount to increment.
        """
        self.custom_metrics[name] = self.custom_metrics.get(name, 0) + amount

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics.

        Returns:
            Dictionary with metrics summary.
        """
        uptime = (datetime.utcnow() - self.start_time).total_seconds()

        return {
            "uptime_seconds": uptime,
            "records_extracted": self.records_extracted._value.get(),
            "records_loaded": self.records_loaded._value.get(),
            "records_failed": sum(
                child._value.get() for child in self.records_failed.collect()[0].samples
            ),
            "custom_metrics": dict(self.custom_metrics),
        }

    def export_prometheus(self) -> bytes:
        """Export metrics in Prometheus format.

        Returns:
            Metrics in Prometheus text format.
        """
        return generate_latest()

    def export_json(self) -> Dict[str, Any]:
        """Export metrics as JSON.

        Returns:
            Metrics as dictionary.
        """
        return self.get_metrics_summary()
