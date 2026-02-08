# src/transform.py
"""
Data transformation pipeline with quality validation and resilience.
"""

import pandas as pd
from typing import Callable, List, Dict, Any

from .app_config import config
from .utils import timer
from .resilience import RetryPolicy, CircuitBreaker, DeadLetterQueue
from .monitoring import StructuredLogger, get_metrics_collector, get_tracer
from .pipeline import CheckpointManager
from .quality import DataValidator, RuleSet, QualityChecker
from .performance import MemoryOptimizer

# Initialize resilience and monitoring
retry_policy = RetryPolicy(max_retries=3, initial_delay=2)
circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
dlq = DeadLetterQueue(dlq_dir="dlq")
structured_logger = StructuredLogger("transform", level="INFO")
metrics = get_metrics_collector(namespace="bq2pg")
tracer = get_tracer(service_name="bq2pg_transformer")
checkpoint_mgr = CheckpointManager()
memory_optimizer = MemoryOptimizer()


class DataTransformer:
    """Transform data with quality checks and resilience"""

    def __init__(self):
        self.validator = DataValidator()
        self.quality_checker = QualityChecker()
        self.transformed_count = 0
        self.failed_transforms = 0
        self.transformations = {}

    def register_transformation(self, name: str, func: Callable):
        """
        Register a transformation function.

        Args:
            name: Transformation name
            func: Transformation function
        """
        self.transformations[name] = func
        structured_logger.info(f"Registered transformation: {name}")

    @timer
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data: handle nulls, duplicates, type conversions.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        with tracer.trace_span(
            "clean_data", {"rows": len(df), "columns": len(df.columns)}
        ):
            try:
                structured_logger.info("Starting data cleaning")
                original_rows = len(df)

                # Remove complete duplicates
                df_clean = df.drop_duplicates()
                duplicates_removed = original_rows - len(df_clean)

                structured_logger.info(
                    "Removed duplicate rows",
                    duplicates_removed=duplicates_removed
                )

                # Handle nulls: fill with sensible defaults or drop
                for col in df_clean.columns:
                    if df_clean[col].isna().any():
                        null_count = df_clean[col].isna().sum()

                        # For numeric columns, fill with median
                        if df_clean[col].dtype in ['int64', 'float64']:
                            df_clean[col].fillna(
                                df_clean[col].median(), inplace=True
                            )
                        # For string columns, fill with empty string
                        elif df_clean[col].dtype == 'object':
                            df_clean[col].fillna('', inplace=True)

                        structured_logger.info(
                            f"Filled nulls in {col}",
                            null_count=null_count,
                            strategy="median" if df_clean[col].dtype in [
                                'int64', 'float64'
                            ] else "empty"
                        )

                # Type conversions
                for col, dtype in config.TYPE_MAPPINGS.items():
                    if col in df_clean.columns:
                        try:
                            df_clean[col] = df_clean[col].astype(dtype)
                        except Exception as e:
                            structured_logger.warning(
                                f"Type conversion failed for {col}",
                                column=col,
                                target_type=dtype,
                                error=str(e)
                            )

                structured_logger.info(
                    "Data cleaning complete",
                    rows_remaining=len(df_clean),
                    rows_removed=original_rows - len(df_clean)
                )

                metrics.set_custom_metric(
                    "cleaning_rows_removed", duplicates_removed
                )

                return df_clean

            except Exception as e:
                self.failed_transforms += 1
                structured_logger.error(
                    f"Data cleaning failed: {e}",
                    error_type=type(e).__name__,
                    rows=len(df)
                )
                metrics.increment_custom_metric("cleaning_failures")
                dlq.enqueue(
                    {"operation": "clean_data", "rows": len(df)},
                    str(e),
                    source="transform_clean",
                    retry_count=0
                )
                raise

    @timer
    def validate_data(
        self, df: pd.DataFrame, rules: RuleSet = None
    ) -> Dict[str, Any]:
        """
        Validate data quality.

        Args:
            df: DataFrame to validate
            rules: RuleSet with validation rules

        Returns:
            Validation report
        """
        with tracer.trace_span("validate_data", {"rows": len(df)}):
            try:
                structured_logger.info("Starting data validation")

                validation_results = {
                    "null_checks": self.validator.validate_nulls(df),
                    "duplicate_checks": self.validator.validate_duplicates(df),
                    "type_checks": self.validator.validate_types(df),
                }

                # Apply custom rules if provided
                if rules:
                    for i, row in df.iterrows():
                        rule_violations = rules.validate_row(row)
                        if rule_violations:
                            validation_results[
                                f"row_{i}_violations"
                            ] = rule_violations

                # Record quality checks
                valid_rows = len(df)
                for violation_type, results in validation_results.items():
                    if isinstance(results, dict) and "failed" in results:
                        valid_rows -= results["failed"]

                quality_score = (
                    (valid_rows / len(df)) * 100 if len(df) > 0 else 100
                )
                self.quality_checker.record_check(
                    check_name="full_validation",
                    passed_records=valid_rows,
                    failed_records=len(df) - valid_rows,
                    metadata={"columns": len(df.columns)}
                )

                structured_logger.info(
                    "Validation complete",
                    quality_score=quality_score,
                    valid_rows=valid_rows,
                    total_rows=len(df)
                )

                metrics.set_custom_metric(
                    "validation_quality_score", quality_score
                )

                return {
                    "validation_results": validation_results,
                    "quality_score": quality_score,
                    "valid_rows": valid_rows,
                    "total_rows": len(df)
                }

            except Exception as e:
                self.failed_transforms += 1
                structured_logger.error(
                    f"Validation failed: {e}",
                    error_type=type(e).__name__
                )
                metrics.increment_custom_metric("validation_failures")
                dlq.enqueue(
                    {"operation": "validate_data"},
                    str(e),
                    source="transform_validate",
                    retry_count=0
                )
                raise

    @timer
    def transform(
        self, df: pd.DataFrame, transformations: List[str] = None
    ) -> pd.DataFrame:
        """
        Apply registered transformations.

        Args:
            df: Input DataFrame
            transformations: List of transformation names to apply

        Returns:
            Transformed DataFrame
        """
        with tracer.trace_span(
            "transform",
            {"rows": len(df), "transformations": len(transformations or [])}
        ):
            try:
                df_transformed = df.copy()
                transformations = transformations or list(
                    self.transformations.keys()
                )

                for transform_name in transformations:
                    if transform_name not in self.transformations:
                        structured_logger.warning(
                            f"Transformation not found: {transform_name}",
                            available=list(self.transformations.keys())
                        )
                        continue

                    with tracer.trace_span(
                        f"apply_transformation_{transform_name}",
                        {"rows": len(df_transformed)}
                    ):
                        try:
                            transform_func = self.transformations[
                                transform_name
                            ]
                            df_transformed = transform_func(df_transformed)

                            structured_logger.info(
                                f"Applied transformation: {transform_name}",
                                rows_after=len(df_transformed)
                            )

                            metrics.increment_custom_metric(
                                f"transformations_applied_{transform_name}"
                            )

                        except Exception as e:
                            structured_logger.error(
                                f"Transformation {transform_name} failed: {e}",
                                error_type=type(e).__name__
                            )
                            metrics.increment_custom_metric(
                                f"transformation_failures_{transform_name}"
                            )
                            # Continue with next transformation

                self.transformed_count += len(df_transformed)

                structured_logger.info(
                    "All transformations complete",
                    rows_transformed=len(df_transformed),
                    total_transformed=self.transformed_count
                )

                checkpoint_mgr.save_checkpoint(
                    "transformation",
                    {"rows_transformed": len(df_transformed),
                     "transformations": transformations},
                    metadata={"timestamp": pd.Timestamp.now().isoformat()}
                )

                return df_transformed

            except Exception as e:
                self.failed_transforms += 1
                structured_logger.error(
                    f"Transform failed: {e}",
                    error_type=type(e).__name__,
                    rows=len(df)
                )
                metrics.increment_custom_metric("transform_failures")
                dlq.enqueue(
                    {
                        "operation": "transform",
                        "transformations": transformations
                    },
                    str(e),
                    source="transform",
                    retry_count=0
                )
                raise

    @timer
    def process_pipeline(
        self,
        df: pd.DataFrame,
        clean: bool = True,
        validate: bool = True,
        transformations: List[str] = None,
        rules: RuleSet = None
    ) -> Dict[str, Any]:
        """
        Run complete transformation pipeline.

        Args:
            df: Input DataFrame
            clean: Whether to clean data
            validate: Whether to validate data
            transformations: Transformations to apply
            rules: Quality rules to check

        Returns:
            Result dictionary with transformed data and metrics
        """
        with tracer.trace_span("process_pipeline", {"rows": len(df)}):
            try:
                structured_logger.info("Starting transformation pipeline")

                result = {
                    "original_rows": len(df),
                    "steps": []
                }

                df_processed = df.copy()

                if not memory_optimizer.check_memory_usage():
                    structured_logger.warning(
                        "Memory usage high, optimizing..."
                    )
                    memory_optimizer.cleanup()

                # Clean
                if clean:
                    with tracer.trace_span("pipeline_clean"):
                        df_processed = self.clean_data(df_processed)
                        result["steps"].append("clean")
                        result["rows_after_clean"] = len(df_processed)

                # Validate
                if validate:
                    with tracer.trace_span("pipeline_validate"):
                        validation_report = self.validate_data(
                            df_processed, rules
                        )
                        result["validation"] = validation_report
                        result["steps"].append("validate")

                # Transform
                if transformations or self.transformations:
                    with tracer.trace_span("pipeline_transform"):
                        df_processed = self.transform(
                            df_processed, transformations
                        )
                        result["steps"].append("transform")
                        result["rows_after_transform"] = len(df_processed)

                result["final_rows"] = len(df_processed)
                result["data"] = df_processed

                structured_logger.info(
                    "Transformation pipeline complete",
                    original_rows=result["original_rows"],
                    final_rows=result["final_rows"],
                    steps=result["steps"]
                )

                metrics.set_custom_metric(
                    "pipeline_rows_processed", result["final_rows"]
                )

                return result

            except Exception as e:
                structured_logger.error(
                    f"Pipeline failed: {e}",
                    error_type=type(e).__name__
                )
                metrics.increment_custom_metric("pipeline_failures")
                dlq.enqueue(
                    {"operation": "process_pipeline"},
                    str(e),
                    source="transform_pipeline",
                    retry_count=0
                )
                raise

    def get_quality_report(self) -> Dict[str, Any]:
        """Get quality metrics report"""
        return self.quality_checker.get_quality_report()


# Standard transformation functions
def normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize text columns (lowercase, strip whitespace)"""
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        df[col] = df[col].str.lower().str.strip()
    return df


def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize date columns"""
    date_cols = [
        col for col in df.columns
        if 'date' in col.lower() or 'time' in col.lower()
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def deduplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows based on specific columns"""
    key_cols = [
        col for col in df.columns
        if col not in ['created_at', 'updated_at']
    ]
    return df.drop_duplicates(subset=key_cols, keep='first')


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values by forward filling"""
    return df.fillna(method='ffill').fillna(method='bfill')
