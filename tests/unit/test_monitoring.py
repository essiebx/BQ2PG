"""Unit tests for monitoring modules."""

import json
from src.monitoring.structured_logger import (
    StructuredLogger,
    StructuredFormatter,
)
from src.monitoring.metrics import MetricsCollector


class TestStructuredLogger:
    """Test StructuredLogger class."""

    def test_logger_init(self, temp_dir):
        """Test logger initialization."""
        logger = StructuredLogger("test_logger", level="DEBUG")
        assert logger.logger.name == "test_logger"
        assert logger.logger.level == 10  # DEBUG level

    def test_structured_formatter(self, temp_dir):
        """Test structured formatter."""
        formatter = StructuredFormatter()

        # Create a log record
        import logging

        logger = logging.getLogger("test")
        record = logger.makeRecord(
            "test",
            logging.INFO,
            "test.py",
            42,
            "Test message",
            (),
            None,
        )

        formatted = formatter.format(record)
        log_obj = json.loads(formatted)

        assert log_obj["level"] == "INFO"
        assert log_obj["message"] == "Test message"
        assert log_obj["line"] == 42

    def test_logger_with_file(self, temp_dir):
        """Test logger with file output."""
        log_file = temp_dir / "test.log"
        logger = StructuredLogger("test_logger", log_file=str(log_file))

        logger.info("Test info message")
        logger.warning("Test warning message")

        assert log_file.exists()

        # Read and verify log file
        with open(log_file) as f:
            logs = [json.loads(line) for line in f if line.strip()]

        assert len(logs) == 2
        assert logs[0]["level"] == "INFO"
        assert logs[1]["level"] == "WARNING"

    def test_logger_with_extra_fields(self, temp_dir):
        """Test logging with extra fields."""
        log_file = temp_dir / "test.log"
        logger = StructuredLogger("test_logger", log_file=str(log_file))

        logger.info("Test message", user_id="123", action="create")

        with open(log_file) as f:
            log_obj = json.loads(f.read())

        assert log_obj.get("user_id") == "123"
        assert log_obj.get("action") == "create"


class TestMetricsCollector:
    """Test MetricsCollector class."""

    def test_collector_init(self):
        """Test metrics collector initialization."""
        collector = MetricsCollector(namespace="test")
        assert collector.namespace == "test"

    def test_record_extraction(self):
        """Test recording extraction metrics."""
        collector = MetricsCollector()

        collector.record_extraction(record_count=1000, duration_seconds=5.5)

        summary = collector.get_metrics_summary()
        assert summary["records_extracted"] >= 1000

    def test_record_load(self):
        """Test recording load metrics."""
        collector = MetricsCollector()

        collector.record_load(
            record_count=500, duration_seconds=2.0, failed=10
        )

        summary = collector.get_metrics_summary()
        assert summary["records_loaded"] >= 490
        assert summary["records_failed"] >= 10

    def test_record_batch(self):
        """Test recording batch metrics."""
        collector = MetricsCollector()

        collector.record_batch(batch_size=5000)

        summary = collector.get_metrics_summary()
        assert "uptime_seconds" in summary

    def test_custom_metrics(self):
        """Test custom metric handling."""
        collector = MetricsCollector()

        collector.set_custom_metric("extraction_time", 10.5)
        collector.increment_custom_metric("retry_count")
        collector.increment_custom_metric("retry_count", 5)

        summary = collector.get_metrics_summary()
        assert summary["custom_metrics"]["extraction_time"] == 10.5
        assert summary["custom_metrics"]["retry_count"] == 6

    def test_circuit_breaker_state(self):
        """Test circuit breaker state tracking."""
        collector = MetricsCollector()

        collector.set_circuit_breaker_state("closed")
        collector.set_circuit_breaker_state("open")
        collector.set_circuit_breaker_state("half_open")

        # Just verify no exceptions are raised
        assert True

    def test_export_json(self):
        """Test JSON export."""
        collector = MetricsCollector()

        collector.record_extraction(100, 1.0)
        json_export = collector.export_json()

        assert "uptime_seconds" in json_export
        assert "records_extracted" in json_export
        assert "custom_metrics" in json_export

    def test_export_prometheus(self):
        """Test Prometheus export."""
        collector = MetricsCollector()

        collector.record_extraction(100, 1.0)
        prometheus_output = collector.export_prometheus()

        assert b"bq2pg_records_extracted_total" in prometheus_output
        assert b"100.0" in prometheus_output
