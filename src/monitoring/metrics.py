"""Minimal metrics collection (No dependencies)."""

import logging
from typing import Dict, Any
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collects and manages application metrics in-memory."""

    def __init__(self, namespace: str = "bq2pg"):
        self.namespace = namespace
        self.custom_metrics: Dict[str, Any] = defaultdict(float)
        self.start_time = datetime.utcnow()
        self.extracted_count = 0
        self.loaded_count = 0
        self.failed_count = 0

    def record_extraction(self, record_count: int, duration_seconds: float) -> None:
        self.extracted_count += record_count
        logger.info(f"Extracted {record_count} records in {duration_seconds:.2f}s")

    def record_load(self, record_count: int, duration_seconds: float, failed: int = 0) -> None:
        self.loaded_count += (record_count - failed)
        self.failed_count += failed
        logger.info(f"Loaded {record_count - failed}/{record_count} records in {duration_seconds:.2f}s")

    def record_batch(self, batch_size: int) -> None:
        pass

    def record_pipeline_duration(self, duration_seconds: float) -> None:
        pass

    def set_pool_connections(self, count: int) -> None:
        pass

    def set_circuit_breaker_state(self, state: str) -> None:
        pass

    def set_custom_metric(self, name: str, value: float) -> None:
        self.custom_metrics[name] = value

    def increment_custom_metric(self, name: str, amount: float = 1) -> None:
        self.custom_metrics[name] += amount

    def get_metrics_summary(self) -> Dict[str, Any]:
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        return {
            "uptime_seconds": uptime,
            "records_extracted": self.extracted_count,
            "records_loaded": self.loaded_count,
            "records_failed": self.failed_count,
            "custom_metrics": dict(self.custom_metrics),
        }

    def export_prometheus(self) -> bytes:
        return b"# Prometheus metrics disabled in free version"

    def export_json(self) -> Dict[str, Any]:
        return self.get_metrics_summary()
