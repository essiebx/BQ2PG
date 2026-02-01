"""
Comprehensive tests for Phase 2 components (Quality, Transform, Integration).
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from src.quality import ValidationRule, RuleSet, DataValidator, QualityChecker
from src.transform import DataTransformer, normalize_text, standardize_dates
from src.extract import BigQueryExtractor
from src.load import PostgresLoader


class TestQualityModule:
    """Tests for quality validation module"""
    
    def test_validation_rule_not_null(self):
        """Test NOT_NULL rule"""
        rule = ValidationRule.not_null("id", "ID cannot be null")
        assert rule.rule_type.value == "NOT_NULL"
        
        # Valid row
        valid = rule.validate({"id": 1, "name": "test"})
        assert valid
        
        # Invalid row
        invalid = rule.validate({"id": None, "name": "test"})
        assert not invalid
    
    def test_validation_rule_range(self):
        """Test RANGE rule"""
        rule = ValidationRule.range("age", 0, 150, "Age out of range")
        
        assert rule.validate({"age": 25})
        assert rule.validate({"age": 0})
        assert rule.validate({"age": 150})
        assert not rule.validate({"age": -1})
        assert not rule.validate({"age": 151})
    
    def test_validation_rule_pattern(self):
        """Test PATTERN rule"""
        rule = ValidationRule.pattern("email", r"^[^@]+@[^@]+\.[^@]+$", "Invalid email")
        
        assert rule.validate({"email": "test@example.com"})
        assert not rule.validate({"email": "invalid-email"})
    
    def test_ruleset_operations(self):
        """Test RuleSet add and validate operations"""
        rules = RuleSet()
        rules.add_not_null_rule("id")
        rules.add_range_rule("age", 0, 150)
        rules.add_pattern_rule("email", r"^[^@]+@[^@]+\.[^@]+$")
        
        assert len(rules.rules) == 3
        
        # Valid row
        valid_row = {"id": 1, "age": 25, "email": "test@example.com"}
        assert rules.validate_row(valid_row) == []
        
        # Invalid row
        invalid_row = {"id": None, "age": 200, "email": "invalid"}
        violations = rules.validate_row(invalid_row)
        assert len(violations) == 3
    
    def test_data_validator_nulls(self):
        """Test null value validation"""
        validator = DataValidator()
        df = pd.DataFrame({
            "id": [1, None, 3],
            "name": ["a", "b", None]
        })
        
        result = validator.validate_nulls(df)
        assert result["total_nulls"] == 2
        assert result["null_percentage"] > 0
    
    def test_data_validator_duplicates(self):
        """Test duplicate detection"""
        validator = DataValidator()
        df = pd.DataFrame({
            "id": [1, 2, 1, 3],
            "name": ["a", "b", "a", "c"]
        })
        
        result = validator.validate_duplicates(df)
        assert result["duplicate_rows"] == 2
    
    def test_data_validator_types(self):
        """Test type validation"""
        validator = DataValidator()
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "age": [25, 30, 35]
        })
        
        result = validator.validate_types(df)
        assert "column_types" in result
        assert "int64" in str(result["column_types"]["id"])
    
    def test_data_validator_ranges(self):
        """Test range validation"""
        validator = DataValidator()
        df = pd.DataFrame({
            "age": [25, 150, 30],
            "score": [50, 100, 75]
        })
        
        range_rules = {"age": (0, 120), "score": (0, 100)}
        result = validator.validate_ranges(df, range_rules)
        
        assert "out_of_range_count" in result
    
    def test_quality_checker(self):
        """Test quality metrics tracking"""
        checker = QualityChecker()
        
        checker.record_check("test_check", passed_records=95, failed_records=5)
        report = checker.get_quality_report()
        
        assert "test_check" in report
        assert report["test_check"]["passed"] == 95
        assert report["test_check"]["failed"] == 5
        assert report["test_check"]["score"] == 95.0


class TestTransformModule:
    """Tests for data transformation module"""
    
    def test_clean_data_removes_duplicates(self):
        """Test duplicate removal"""
        transformer = DataTransformer()
        df = pd.DataFrame({
            "id": [1, 1, 2],
            "name": ["a", "a", "b"]
        })
        
        df_clean = transformer.clean_data(df)
        assert len(df_clean) == 2
    
    def test_clean_data_handles_nulls(self):
        """Test null handling"""
        transformer = DataTransformer()
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "numeric": [10.0, np.nan, 30.0],
            "text": ["a", None, "c"]
        })
        
        df_clean = transformer.clean_data(df)
        assert not df_clean["numeric"].isna().any()
        assert not df_clean["text"].isna().any()
    
    def test_normalize_text(self):
        """Test text normalization"""
        df = pd.DataFrame({
            "name": ["JOHN  ", "  jane", "BOB"]
        })
        
        df_norm = normalize_text(df)
        assert df_norm["name"].iloc[0] == "john"
        assert df_norm["name"].iloc[1] == "jane"
        assert df_norm["name"].iloc[2] == "bob"
    
    def test_standardize_dates(self):
        """Test date standardization"""
        df = pd.DataFrame({
            "created_date": ["2024-01-01", "2024-01-02", "invalid"],
            "date_field": ["2024/01/01", "2024/01/02", "2024/01/03"]
        })
        
        df_std = standardize_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(df_std["created_date"])
    
    def test_register_transformation(self):
        """Test custom transformation registration"""
        transformer = DataTransformer()
        
        def custom_transform(df):
            df["new_col"] = df.iloc[:, 0] * 2
            return df
        
        transformer.register_transformation("custom", custom_transform)
        assert "custom" in transformer.transformations
    
    def test_validate_data(self):
        """Test data validation in transformer"""
        transformer = DataTransformer()
        df = pd.DataFrame({
            "id": [1, 2, None],
            "value": [10, 20, 30]
        })
        
        result = transformer.validate_data(df)
        assert "validation_results" in result
        assert "quality_score" in result
    
    def test_transform_pipeline_full(self):
        """Test complete transformation pipeline"""
        transformer = DataTransformer()
        transformer.register_transformation("normalize_text", normalize_text)
        transformer.register_transformation("standardize_dates", standardize_dates)
        
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["  JOHN  ", "  JANE  ", "  BOB  "],
            "created_date": ["2024-01-01", "2024-01-02", "2024-01-03"]
        })
        
        result = transformer.process_pipeline(
            df,
            clean=True,
            validate=True,
            transformations=["normalize_text", "standardize_dates"]
        )
        
        assert "data" in result
        assert "steps" in result
        assert len(result["steps"]) > 0
        assert result["final_rows"] > 0


class TestIntegration:
    """Integration tests for ETL pipeline"""
    
    def test_extract_transform_load_flow(self):
        """Test complete ETL flow (mocked)"""
        # Create sample data
        df_raw = pd.DataFrame({
            "patent_id": ["US1234567", "US7654321", "US1111111"],
            "title": ["  PATENT TITLE  ", "  ANOTHER PATENT  ", "  THIRD PATENT  "],
            "filing_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "filing_year": [2024, 2024, 2024]
        })
        
        # Transform
        transformer = DataTransformer()
        transformer.register_transformation("normalize_text", normalize_text)
        transformer.register_transformation("standardize_dates", standardize_dates)
        
        rules = RuleSet()
        rules.add_not_null_rule("patent_id")
        rules.add_range_rule("filing_year", 1800, 2100)
        
        result = transformer.process_pipeline(
            df_raw,
            clean=True,
            validate=True,
            transformations=["normalize_text", "standardize_dates"],
            rules=rules
        )
        
        # Verify results
        assert result["final_rows"] <= result["original_rows"]
        assert "validation" in result
        assert result["validation"]["quality_score"] > 0
    
    def test_quality_metrics_accumulation(self):
        """Test quality metrics accumulation across multiple checks"""
        checker = QualityChecker()
        
        # Simulate multiple checks
        for i in range(5):
            checker.record_check(
                f"check_{i}",
                passed_records=100-i*5,
                failed_records=i*5
            )
        
        report = checker.get_quality_report()
        assert len(report) == 5
        
        # Export to JSON
        json_report = checker.export_report_json()
        assert isinstance(json_report, str)
        assert "check_" in json_report


class TestErrorHandling:
    """Tests for error handling and resilience"""
    
    def test_validate_data_with_invalid_rules(self):
        """Test validation with invalid rule combinations"""
        rules = RuleSet()
        rules.add_range_rule("age", 0, 150)
        
        df = pd.DataFrame({
            "age": ["not a number", "25", "30"]
        })
        
        # Should handle gracefully
        violations = rules.validate_row({"age": "invalid"})
        # Validation should catch type mismatch
        assert isinstance(violations, list)
    
    def test_transform_handles_exceptions(self):
        """Test transform error handling"""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            "id": [1, 2, 3]
        })
        
        # Register a transformation that might fail
        def risky_transform(df):
            df["result"] = df["nonexistent_col"] * 2
            return df
        
        transformer.register_transformation("risky", risky_transform)
        
        # The transform should handle this gracefully
        # (depends on implementation - might log warning and continue)
        result = transformer.process_pipeline(df, transformations=["risky"])
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
