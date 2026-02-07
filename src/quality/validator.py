"""Data validation engine."""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd


logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data for quality issues."""

    def __init__(self):
        """Initialize data validator."""
        self.validation_results = []

    def validate_nulls(
        self, df: pd.DataFrame, nullable_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Check for null values.

        Args:
            df: DataFrame to validate.
            nullable_columns: Columns that can have nulls (optional).

        Returns:
            Validation result dictionary.
        """
        result = {
            "check": "null_values",
            "passed": True,
            "issues": [],
        }

        nullable_set = set(nullable_columns or [])

        for column in df.columns:
            null_count = df[column].isnull().sum()
            null_pct = (null_count / len(df)) * 100

            if null_count > 0:
                if column not in nullable_set and null_count > 0:
                    result["passed"] = False
                    result["issues"].append({
                        "column": column,
                        "type": "unexpected_nulls",
                        "count": null_count,
                        "percentage": null_pct,
                    })
                elif null_pct > 50:  # Warning for high null percentage
                    result["issues"].append({
                        "column": column,
                        "type": "high_null_percentage",
                        "count": null_count,
                        "percentage": null_pct,
                        "severity": "warning",
                    })

        return result

    def validate_duplicates(
        self, df: pd.DataFrame, key_columns: List[str]
    ) -> Dict[str, Any]:
        """Check for duplicate rows.

        Args:
            df: DataFrame to validate.
            key_columns: Columns to check for uniqueness.

        Returns:
            Validation result dictionary.
        """
        result = {
            "check": "duplicates",
            "passed": True,
            "issues": [],
        }

        if key_columns:
            duplicate_mask = df.duplicated(subset=key_columns, keep=False)
            duplicate_count = duplicate_mask.sum()

            if duplicate_count > 0:
                result["passed"] = False
                result["issues"].append({
                    "type": "duplicate_rows",
                    "count": duplicate_count,
                    "key_columns": key_columns,
                })

        return result

    def validate_types(
        self, df: pd.DataFrame, expected_types: Dict[str, str]
    ) -> Dict[str, Any]:
        """Check data types.

        Args:
            df: DataFrame to validate.
            expected_types: Dictionary of column -> expected type.

        Returns:
            Validation result dictionary.
        """
        result = {
            "check": "data_types",
            "passed": True,
            "issues": [],
        }

        for column, expected_type in expected_types.items():
            if column not in df.columns:
                result["issues"].append({
                    "column": column,
                    "type": "missing_column",
                    "expected_type": expected_type,
                })
                continue

            actual_type = str(df[column].dtype)

            if (
                expected_type == "numeric" and
                not pd.api.types.is_numeric_dtype(df[column])
            ):
                result["passed"] = False
                result["issues"].append({
                    "column": column,
                    "type": "type_mismatch",
                    "expected": expected_type,
                    "actual": actual_type,
                })
            elif expected_type == "string" and not (
                df[column].dtype == "object"
            ):
                result["issues"].append({
                    "column": column,
                    "type": "type_mismatch",
                    "expected": expected_type,
                    "actual": actual_type,
                    "severity": "warning",
                })

        return result

    def validate_ranges(
        self, df: pd.DataFrame, ranges: Dict[str, tuple]
    ) -> Dict[str, Any]:
        """Check numeric ranges.

        Args:
            df: DataFrame to validate.
            ranges: Dictionary of column -> (min, max).

        Returns:
            Validation result dictionary.
        """
        result = {
            "check": "ranges",
            "passed": True,
            "issues": [],
        }

        for column, (min_val, max_val) in ranges.items():
            if column not in df.columns:
                continue

            out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]

            if len(out_of_range) > 0:
                result["issues"].append({
                    "column": column,
                    "type": "out_of_range",
                    "count": len(out_of_range),
                    "percentage": (len(out_of_range) / len(df)) * 100,
                    "min": min_val,
                    "max": max_val,
                })

        return result

    def validate_string_patterns(
        self, df: pd.DataFrame, patterns: Dict[str, str]
    ) -> Dict[str, Any]:
        """Check string patterns.

        Args:
            df: DataFrame to validate.
            patterns: Dictionary of column -> regex pattern.

        Returns:
            Validation result dictionary.
        """
        import re

        result = {
            "check": "string_patterns",
            "passed": True,
            "issues": [],
        }

        for column, pattern in patterns.items():
            if column not in df.columns:
                continue

            non_matching = 0
            for value in df[column].dropna():
                if not re.match(pattern, str(value)):
                    non_matching += 1

            if non_matching > 0:
                result["issues"].append({
                    "column": column,
                    "type": "pattern_mismatch",
                    "count": non_matching,
                    "percentage": (non_matching / len(df)) * 100,
                    "pattern": pattern,
                })

        return result

    def validate_all(
        self,
        df: pd.DataFrame,
        nullable_columns: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
        expected_types: Optional[Dict[str, str]] = None,
        ranges: Optional[Dict[str, tuple]] = None,
        patterns: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Run all validation checks.

        Args:
            df: DataFrame to validate.
            nullable_columns: Columns that can be null.
            key_columns: Columns to check for uniqueness.
            expected_types: Expected column types.
            ranges: Numeric ranges to validate.
            patterns: String patterns to validate.

        Returns:
            Dictionary with all validation results.
        """
        logger.info(
            f"Starting validation of {len(df)} rows, {len(df.columns)} columns"
        )

        results = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "checks": [],
            "passed": True,
        }

        # Run all checks
        checks = [
            self.validate_nulls(df, nullable_columns),
            self.validate_duplicates(df, key_columns or []),
            self.validate_types(df, expected_types or {}),
            self.validate_ranges(df, ranges or {}),
            self.validate_string_patterns(df, patterns or {}),
        ]

        for check in checks:
            results["checks"].append(check)
            if not check["passed"]:
                results["passed"] = False

        logger.info(
            f"Validation complete: "
            f"{'PASSED' if results['passed'] else 'FAILED'}"
        )
        return results
