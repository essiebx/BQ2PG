# src/monitoring/health_check.py
"""
Health check system for pipeline status monitoring.
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import json

from .structured_logger import StructuredLogger
from .metrics import MetricsCollector

logger = StructuredLogger("health_check", level="INFO")
metrics = MetricsCollector(namespace="bq2pg_health")


class HealthStatus(Enum):
    """Health status levels"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check"""
    component: str
    status: HealthStatus
    timestamp: datetime
    message: str
    checks: Dict[str, Any] = field(default_factory=dict)
    latency_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "component": self.component,
            "status": self.status.value,
            "timestamp": self.timestamp.isoformat(),
            "message": self.message,
            "checks": self.checks,
            "latency_ms": self.latency_ms
        }


class HealthChecker:
    """Base class for health checks"""

    def __init__(self, name: str):
        self.name = name
        self.last_check: Optional[HealthCheckResult] = None
        self.check_interval = timedelta(seconds=60)
        self.last_check_time = datetime.min

    def should_check(self) -> bool:
        """Check if health check should run"""
        return datetime.now() - self.last_check_time >= self.check_interval

    def check(self) -> HealthCheckResult:
        """Run health check - should be overridden"""
        self.last_check_time = datetime.now()
        return HealthCheckResult(
            component=self.name,
            status=HealthStatus.UNKNOWN,
            timestamp=datetime.now(),
            message="Not implemented"
        )

    def get_status(self) -> Optional[HealthCheckResult]:
        """Get current status"""
        if self.should_check():
            self.last_check = self.check()
        return self.last_check


class DatabaseHealthChecker(HealthChecker):
    """Check database connectivity and performance"""

    def __init__(self, engine=None):
        super().__init__("database")
        self.engine = engine

    def check(self) -> HealthCheckResult:
        """Check database health"""
        start_time = datetime.now()

        try:
            if not self.engine:
                return HealthCheckResult(
                    component=self.name,
                    status=HealthStatus.UNHEALTHY,
                    timestamp=datetime.now(),
                    message="Database engine not configured"
                )

            # Test connection
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")

            latency = (datetime.now() - start_time).total_seconds() * 1000

            self.last_check_time = datetime.now()
            result = HealthCheckResult(
                component=self.name,
                status=(
                    HealthStatus.HEALTHY if latency < 1000
                    else HealthStatus.DEGRADED
                ),
                timestamp=datetime.now(),
                message=(
                    "Database connection OK" if latency < 1000
                    else "Database connection slow"
                ),
                checks={"latency_ms": latency},
                latency_ms=latency
            )

            metrics.set_custom_metric("database_health_latency_ms", latency)
            return result

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNHEALTHY,
                timestamp=datetime.now(),
                message=f"Database check failed: {str(e)}"
            )


class BigQueryHealthChecker(HealthChecker):
    """Check BigQuery connectivity"""

    def __init__(self, client=None):
        super().__init__("bigquery")
        self.client = client

    def check(self) -> HealthCheckResult:
        """Check BigQuery health"""
        start_time = datetime.now()

        try:
            if not self.client:
                return HealthCheckResult(
                    component=self.name,
                    status=HealthStatus.UNHEALTHY,
                    timestamp=datetime.now(),
                    message="BigQuery client not configured"
                )

            # Test connection
            self.client.list_datasets(max_results=1)

            latency = (datetime.now() - start_time).total_seconds() * 1000

            self.last_check_time = datetime.now()
            result = HealthCheckResult(
                component=self.name,
                status=(
                    HealthStatus.HEALTHY if latency < 2000
                    else HealthStatus.DEGRADED
                ),
                timestamp=datetime.now(),
                message=(
                    "BigQuery connection OK" if latency < 2000
                    else "BigQuery connection slow"
                ),
                checks={"latency_ms": latency},
                latency_ms=latency
            )

            metrics.set_custom_metric("bigquery_health_latency_ms", latency)
            return result

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNHEALTHY,
                timestamp=datetime.now(),
                message=f"BigQuery check failed: {str(e)}"
            )


class MemoryHealthChecker(HealthChecker):
    """Check memory usage health"""

    def check(self) -> HealthCheckResult:
        """Check memory health"""
        import psutil

        try:
            memory_percent = psutil.virtual_memory().percent

            self.last_check_time = datetime.now()

            if memory_percent < 70:
                status = HealthStatus.HEALTHY
                message = "Memory usage normal"
            elif memory_percent < 85:
                status = HealthStatus.DEGRADED
                message = "Memory usage elevated"
            else:
                status = HealthStatus.UNHEALTHY
                message = "Memory usage critical"

            result = HealthCheckResult(
                component=self.name,
                status=status,
                timestamp=datetime.now(),
                message=message,
                checks={"memory_percent": memory_percent}
            )

            metrics.set_custom_metric("memory_health_percent", memory_percent)
            return result

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNKNOWN,
                timestamp=datetime.now(),
                message=f"Memory check failed: {str(e)}"
            )


class PipelineHealthChecker(HealthChecker):
    """Check overall pipeline health"""

    def __init__(
        self, max_failed_records: int = 100, max_dlq_entries: int = 50
    ):
        super().__init__("pipeline")
        self.max_failed_records = max_failed_records
        self.max_dlq_entries = max_dlq_entries
        self.last_error_count = 0

    def set_metrics(self, failed_records: int = 0, dlq_entries: int = 0):
        """Set metric values for health check"""
        self.failed_records = failed_records
        self.dlq_entries = dlq_entries

    def check(self) -> HealthCheckResult:
        """Check pipeline health"""
        try:
            self.last_check_time = datetime.now()

            # Check thresholds
            health_checks = {
                "failed_records": self.getattr(self, 'failed_records', 0),
                "dlq_entries": self.getattr(self, 'dlq_entries', 0),
            }

            if health_checks["failed_records"] > self.max_failed_records:
                status = HealthStatus.UNHEALTHY
                message = (
                    "Too many failed records: "
                    f"{health_checks['failed_records']}"
                )
            elif health_checks["dlq_entries"] > self.max_dlq_entries:
                status = HealthStatus.DEGRADED
                message = f"DLQ has {health_checks['dlq_entries']} entries"
            else:
                status = HealthStatus.HEALTHY
                message = "Pipeline operating normally"

            result = HealthCheckResult(
                component=self.name,
                status=status,
                timestamp=datetime.now(),
                message=message,
                checks=health_checks
            )

            metrics.set_custom_metric(
                "pipeline_health_status",
                1 if status == HealthStatus.HEALTHY else 0
            )
            return result

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNKNOWN,
                timestamp=datetime.now(),
                message=f"Pipeline check failed: {str(e)}"
            )

    def getattr(self, obj, attr, default=None):
        """Safe getattr"""
        return getattr(obj, attr, default)


class HealthCheckManager:
    """Manage multiple health checks"""

    def __init__(self):
        self.checkers: Dict[str, HealthChecker] = {}
        self.results_history: List[HealthCheckResult] = []

    def register_checker(self, checker: HealthChecker) -> None:
        """Register health checker"""
        self.checkers[checker.name] = checker
        logger.info(f"Health checker registered: {checker.name}")

    def run_checks(self) -> Dict[str, HealthCheckResult]:
        """Run all health checks"""
        results = {}

        for name, checker in self.checkers.items():
            result = checker.check()
            results[name] = result
            self.results_history.append(result)

            logger.info(
                f"Health check: {name}",
                status=result.status.value,
                latency_ms=result.latency_ms
            )

        return results

    def get_overall_status(self) -> HealthStatus:
        """Get overall health status"""
        results = self.run_checks()

        if not results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in results.values()]

        if HealthStatus.UNHEALTHY in statuses:
            return HealthStatus.UNHEALTHY
        elif HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        elif all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.UNKNOWN

    def get_health_report(self) -> Dict[str, Any]:
        """Get comprehensive health report"""
        results = self.run_checks()
        overall_status = self.get_overall_status()

        return {
            "timestamp": datetime.now().isoformat(),
            "overall_status": overall_status.value,
            "components": {
                name: result.to_dict() for name, result in results.items()
            },
            "healthy_count": sum(
                1 for r in results.values()
                if r.status == HealthStatus.HEALTHY
            ),
            "degraded_count": sum(
                1 for r in results.values()
                if r.status == HealthStatus.DEGRADED
            ),
            "unhealthy_count": sum(
                1 for r in results.values()
                if r.status == HealthStatus.UNHEALTHY
            ),
        }

    def export_report_json(self) -> str:
        """Export health report as JSON"""
        report = self.get_health_report()
        return json.dumps(report, indent=2)
