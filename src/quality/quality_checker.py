"""Quality checker with metrics reporting."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class QualityChecker:
    """Tracks and reports data quality metrics."""

    def __init__(self):
        """Initialize quality checker."""
        self.checks_history = []
        self.quality_score = 100.0

    def record_check(
        self,
        check_name: str,
        passed: bool,
        records_total: int,
        records_valid: int,
        records_invalid: int,
        issues: Optional[List[str]] = None,
    ) -> None:
        """Record a quality check result.

        Args:
            check_name: Name of the check.
            passed: Whether the check passed.
            records_total: Total records checked.
            records_valid: Number of valid records.
            records_invalid: Number of invalid records.
            issues: List of issue descriptions.
        """
        check_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "check_name": check_name,
            "passed": passed,
            "records_total": records_total,
            "records_valid": records_valid,
            "records_invalid": records_invalid,
            "valid_percentage": (
                (records_valid / records_total * 100)
                if records_total > 0 else 0
            ),
            "issues": issues or [],
        }

        self.checks_history.append(check_record)
        logger.info(
            f"Quality check '{check_name}': "
            f"{records_valid}/{records_total} valid "
            f"({check_record['valid_percentage']:.1f}%)"
        )

    def get_quality_score(self) -> float:
        """Calculate overall quality score.

        Returns:
            Quality score (0-100).
        """
        if not self.checks_history:
            return 100.0

        total_records = 0
        total_valid = 0

        for check in self.checks_history:
            total_records += check["records_total"]
            total_valid += check["records_valid"]

        score = (
            (total_valid / total_records * 100)
            if total_records > 0 else 100.0
        )
        self.quality_score = score
        return score

    def get_quality_report(self) -> Dict[str, Any]:
        """Get comprehensive quality report.

        Returns:
            Dictionary with quality metrics and history.
        """
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_score": self.get_quality_score(),
            "total_checks": len(self.checks_history),
            "passed_checks": sum(
                1 for c in self.checks_history if c["passed"]
            ),
            "failed_checks": sum(
                1 for c in self.checks_history if not c["passed"]
            ),
            "total_records_checked": sum(
                c["records_total"] for c in self.checks_history
            ),
            "total_valid_records": sum(
                c["records_valid"] for c in self.checks_history
            ),
            "total_invalid_records": sum(
                c["records_invalid"] for c in self.checks_history
            ),
            "checks": self.checks_history,
        }

    def export_report_json(self, filepath: str) -> None:
        """Export quality report to JSON file.

        Args:
            filepath: Path to save report.
        """
        report = self.get_quality_report()
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Quality report exported to {filepath}")

    def export_report_csv(self, filepath: str) -> None:
        """Export quality checks to CSV file.

        Args:
            filepath: Path to save CSV.
        """
        import csv

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "timestamp",
                    "check_name",
                    "passed",
                    "records_total",
                    "records_valid",
                    "records_invalid",
                    "valid_percentage",
                ],
            )
            writer.writeheader()

            for check in self.checks_history:
                writer.writerow({
                    "timestamp": check["timestamp"],
                    "check_name": check["check_name"],
                    "passed": check["passed"],
                    "records_total": check["records_total"],
                    "records_valid": check["records_valid"],
                    "records_invalid": check["records_invalid"],
                    "valid_percentage": f"{check['valid_percentage']:.2f}%",
                })

        logger.info(f"Quality report exported to {filepath}")

    def print_report(self) -> None:
        """Print quality report to stdout."""
        report = self.get_quality_report()

        print("\n" + "=" * 70)
        print("DATA QUALITY REPORT")
        print("=" * 70)
        print(f"Timestamp: {report['timestamp']}")
        print(f"Overall Score: {report['overall_score']:.1f}%")
        print(f"Total Checks: {report['total_checks']}")
        print(f"  [DONE] Passed: {report['passed_checks']}")
        print(f"  ✗ Failed: {report['failed_checks']}")
        print("\nRecords Summary:")
        print(f"  Total Checked: {report['total_records_checked']:,}")
        print(f"  Valid: {report['total_valid_records']:,}")
        print(f"  Invalid: {report['total_invalid_records']:,}")

        print("\nDetailed Checks:")
        print("-" * 70)
        for check in report["checks"]:
            status = "[DONE] PASS" if check["passed"] else "✗ FAIL"
            print(
                f"{status} | {check['check_name']}: "
                f"{check['valid_percentage']:.1f}% valid"
            )
            if check["issues"]:
                for issue in check["issues"]:
                    print(f"       └─ {issue}")

        print("=" * 70 + "\n")
