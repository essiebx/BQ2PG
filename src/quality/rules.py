"""Validation rules and rule sets for data quality."""

import logging
from typing import Callable, Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class RuleType(Enum):
    """Types of validation rules."""

    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    PATTERN = "pattern"
    CUSTOM = "custom"
    REFERENTIAL = "referential"


@dataclass
class ValidationRule:
    """Represents a single validation rule."""

    name: str
    rule_type: RuleType
    column: str
    condition: Any
    error_message: str
    severity: str = "error"  # "error", "warning"

    def validate(self, value: Any) -> tuple[bool, Optional[str]]:
        """Validate a value against the rule.

        Args:
            value: Value to validate.

        Returns:
            Tuple of (is_valid, error_message).
        """
        if self.rule_type == RuleType.NOT_NULL:
            if value is None or (
                isinstance(value, str) and value.strip() == ""
            ):
                return False, f"{self.error_message} (value: {value})"
            return True, None

        elif self.rule_type == RuleType.UNIQUE:
            # This is handled at dataset level
            return True, None

        elif self.rule_type == RuleType.RANGE:
            min_val, max_val = self.condition
            if not (min_val <= value <= max_val):
                return False, (
                    f"{self.error_message} (value: {value}, "
                    f"range: {min_val}-{max_val})"
                )
            return True, None

        elif self.rule_type == RuleType.PATTERN:
            import re

            if not re.match(self.condition, str(value)):
                return False, (
                    f"{self.error_message} (value: {value}, "
                    f"pattern: {self.condition})"
                )
            return True, None

        elif self.rule_type == RuleType.CUSTOM:
            # Custom validation function
            try:
                if callable(self.condition):
                    result = self.condition(value)
                    if not result:
                        return False, self.error_message
                return True, None
            except Exception as e:
                return False, f"{self.error_message} (error: {str(e)})"

        return True, None


class RuleSet:
    """Collection of validation rules."""

    def __init__(self, name: str = "default"):
        """Initialize rule set.

        Args:
            name: Name of the rule set.
        """
        self.name = name
        self.rules: Dict[str, List[ValidationRule]] = {}

    def add_rule(self, rule: ValidationRule) -> None:
        """Add a rule to the set.

        Args:
            rule: ValidationRule to add.
        """
        if rule.column not in self.rules:
            self.rules[rule.column] = []
        self.rules[rule.column].append(rule)
        logger.debug(f"Added rule '{rule.name}' for column '{rule.column}'")

    def add_not_null_rule(
        self, column: str, error_message: str = None
    ) -> None:
        """Add a not-null rule.

        Args:
            column: Column name.
            error_message: Custom error message.
        """
        error_msg = error_message or f"Column '{column}' cannot be null"
        rule = ValidationRule(
            name=f"{column}_not_null",
            rule_type=RuleType.NOT_NULL,
            column=column,
            condition=None,
            error_message=error_msg,
        )
        self.add_rule(rule)

    def add_range_rule(
        self,
        column: str,
        min_val: float,
        max_val: float,
        error_message: str = None
    ) -> None:
        """Add a range validation rule.

        Args:
            column: Column name.
            min_val: Minimum value.
            max_val: Maximum value.
            error_message: Custom error message.
        """
        error_msg = error_message or (
            f"Column '{column}' must be between {min_val} and {max_val}"
        )
        rule = ValidationRule(
            name=f"{column}_range_{min_val}_{max_val}",
            rule_type=RuleType.RANGE,
            column=column,
            condition=(min_val, max_val),
            error_message=error_msg,
        )
        self.add_rule(rule)

    def add_pattern_rule(
        self, column: str, pattern: str, error_message: str = None
    ) -> None:
        """Add a pattern validation rule.

        Args:
            column: Column name.
            pattern: Regex pattern.
            error_message: Custom error message.
        """
        error_msg = error_message or (
            f"Column '{column}' does not match pattern '{pattern}'"
        )
        rule = ValidationRule(
            name=f"{column}_pattern_{pattern[:20]}",
            rule_type=RuleType.PATTERN,
            column=column,
            condition=pattern,
            error_message=error_msg,
        )
        self.add_rule(rule)

    def add_custom_rule(
        self, column: str, validator_func: Callable, error_message: str
    ) -> None:
        """Add a custom validation rule.

        Args:
            column: Column name.
            validator_func: Function that returns True if valid.
            error_message: Error message if validation fails.
        """
        rule = ValidationRule(
            name=f"{column}_custom_{validator_func.__name__}",
            rule_type=RuleType.CUSTOM,
            column=column,
            condition=validator_func,
            error_message=error_message,
        )
        self.add_rule(rule)

    def get_rules_for_column(self, column: str) -> List[ValidationRule]:
        """Get all rules for a column.

        Args:
            column: Column name.

        Returns:
            List of ValidationRules.
        """
        return self.rules.get(column, [])

    def get_all_rules(self) -> List[ValidationRule]:
        """Get all rules in the set.

        Returns:
            List of all ValidationRules.
        """
        all_rules = []
        for rules in self.rules.values():
            all_rules.extend(rules)
        return all_rules

    def validate_row(self, row: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate a single row against all rules.

        Args:
            row: Dictionary of column -> value.

        Returns:
            Tuple of (is_valid, list_of_error_messages).
        """
        errors = []

        for column, rules in self.rules.items():
            if column not in row:
                errors.append(f"Column '{column}' not found in row")
                continue

            value = row[column]
            for rule in rules:
                is_valid, error_msg = rule.validate(value)
                if not is_valid:
                    errors.append(error_msg)

        return len(errors) == 0, errors

    def validate_dataframe(self, df) -> Dict[str, Any]:
        """Validate a DataFrame against all rules.

        Args:
            df: Pandas DataFrame.

        Returns:
            Dictionary with validation results.
        """
        results = {
            "valid_rows": 0,
            "invalid_rows": 0,
            "errors": [],
        }

        for idx, row in df.iterrows():
            is_valid, errors = self.validate_row(row.to_dict())
            if is_valid:
                results["valid_rows"] += 1
            else:
                results["invalid_rows"] += 1
                results["errors"].append({
                    "row": idx,
                    "errors": errors,
                })

        return results
