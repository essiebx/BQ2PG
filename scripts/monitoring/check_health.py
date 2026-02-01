"""Health check script for pipeline monitoring."""

import os
import sys
import logging
import argparse
import json
import requests
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class HealthChecker:
    """Checks health of various pipeline components."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize health checker.

        Args:
            config: Configuration dictionary.
        """
        self.config = config or {}
        self.checks_passed = 0
        self.checks_failed = 0
        self.results: Dict[str, Any] = {}

    def check_database(self, host: str, port: int, database: str, user: str, password: str) -> bool:
        """Check PostgreSQL database connectivity.

        Args:
            host: Database host.
            port: Database port.
            database: Database name.
            user: Database user.
            password: Database password.

        Returns:
            True if database is accessible.
        """
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=5,
            )
            conn.close()

            self.results["database"] = {
                "status": "healthy",
                "host": host,
                "port": port,
                "database": database,
            }
            self.checks_passed += 1
            logger.info(f"Database check passed: {host}:{port}")
            return True
        except Exception as e:
            self.results["database"] = {
                "status": "unhealthy",
                "error": str(e),
            }
            self.checks_failed += 1
            logger.error(f"Database check failed: {e}")
            return False

    def check_bigquery(self, project_id: Optional[str] = None) -> bool:
        """Check BigQuery connectivity.

        Args:
            project_id: GCP project ID.

        Returns:
            True if BigQuery is accessible.
        """
        try:
            from google.cloud import bigquery

            project = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
            if not project:
                raise ValueError("project_id or GOOGLE_CLOUD_PROJECT must be set")

            client = bigquery.Client(project=project)
            client.list_datasets(max_results=1)

            self.results["bigquery"] = {
                "status": "healthy",
                "project": project,
            }
            self.checks_passed += 1
            logger.info(f"BigQuery check passed: {project}")
            return True
        except Exception as e:
            self.results["bigquery"] = {
                "status": "unhealthy",
                "error": str(e),
            }
            self.checks_failed += 1
            logger.warning(f"BigQuery check failed (this may be expected without credentials): {e}")
            return False

    def check_metrics_server(self, host: str = "localhost", port: int = 8000) -> bool:
        """Check if metrics server is running.

        Args:
            host: Metrics server host.
            port: Metrics server port.

        Returns:
            True if metrics server is accessible.
        """
        try:
            response = requests.get(f"http://{host}:{port}/health", timeout=5)

            if response.status_code == 200:
                self.results["metrics_server"] = {
                    "status": "healthy",
                    "host": host,
                    "port": port,
                }
                self.checks_passed += 1
                logger.info(f"Metrics server check passed: {host}:{port}")
                return True
            else:
                self.results["metrics_server"] = {
                    "status": "unhealthy",
                    "status_code": response.status_code,
                }
                self.checks_failed += 1
                logger.error(f"Metrics server returned status {response.status_code}")
                return False
        except Exception as e:
            self.results["metrics_server"] = {
                "status": "unhealthy",
                "error": str(e),
            }
            self.checks_failed += 1
            logger.error(f"Metrics server check failed: {e}")
            return False

    def check_directories(self, directories: Dict[str, str]) -> bool:
        """Check if required directories exist and are writable.

        Args:
            directories: Dictionary of directory name -> path.

        Returns:
            True if all directories are accessible.
        """
        all_healthy = True

        for name, path in directories.items():
            try:
                if not os.path.exists(path):
                    os.makedirs(path, exist_ok=True)

                # Test write access
                test_file = os.path.join(path, ".health_check")
                with open(test_file, "w") as f:
                    f.write("health_check")
                os.remove(test_file)

                if name not in self.results:
                    self.results[name] = {}
                self.results[name]["status"] = "healthy"
                self.checks_passed += 1
                logger.info(f"Directory check passed: {path}")
            except Exception as e:
                if name not in self.results:
                    self.results[name] = {}
                self.results[name]["status"] = "unhealthy"
                self.results[name]["error"] = str(e)
                self.checks_failed += 1
                logger.error(f"Directory check failed for {path}: {e}")
                all_healthy = False

        return all_healthy

    def get_health_report(self) -> Dict[str, Any]:
        """Get complete health report.

        Returns:
            Dictionary with health status.
        """
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": "healthy" if self.checks_failed == 0 else "unhealthy",
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "checks": self.results,
        }

    def print_report(self) -> None:
        """Print health report to stdout."""
        report = self.get_health_report()

        print("\n" + "=" * 60)
        print("HEALTH CHECK REPORT")
        print("=" * 60)
        print(f"Timestamp: {report['timestamp']}")
        print(f"Overall Status: {report['overall_status'].upper()}")
        print(f"Checks Passed: {report['checks_passed']}")
        print(f"Checks Failed: {report['checks_failed']}")
        print("\nDetailed Results:")
        print("-" * 60)

        for check_name, result in report["checks"].items():
            status = result.get("status", "unknown").upper()
            print(f"\n{check_name}: {status}")
            for key, value in result.items():
                if key != "status":
                    print(f"  {key}: {value}")

        print("\n" + "=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Check pipeline health")
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="bq2pg", help="Database name")
    parser.add_argument("--db-user", default="postgres", help="Database user")
    parser.add_argument("--db-password", help="Database password (from env if not provided)")
    parser.add_argument("--gcp-project", help="GCP project ID")
    parser.add_argument("--metrics-host", default="localhost", help="Metrics server host")
    parser.add_argument("--metrics-port", type=int, default=8000, help="Metrics server port")
    parser.add_argument("--output", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--loglevel", default="INFO", help="Logging level")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.loglevel),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Get database password
    db_password = args.db_password or os.getenv("DB_PASSWORD", "")

    # Create checker
    checker = HealthChecker()

    # Run checks
    checker.check_database(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=db_password,
    )

    checker.check_bigquery(project_id=args.gcp_project)

    checker.check_metrics_server(host=args.metrics_host, port=args.metrics_port)

    checker.check_directories({
        "checkpoints": "checkpoints",
        "dlq": "dlq",
        "logs": "logs",
    })

    # Output results
    report = checker.get_health_report()

    if args.output == "json":
        print(json.dumps(report, indent=2))
    else:
        checker.print_report()

    # Exit with appropriate code
    sys.exit(0 if report["overall_status"] == "healthy" else 1)


if __name__ == "__main__":
    main()
