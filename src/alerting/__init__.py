# src/alerting/__init__.py
"""Alert rules and notification system for pipeline monitoring."""

from src.alerting.rules import (
    AlertSeverity,
    AlertStatus,
    Alert,
    AlertRule,
    AlertRuleEngine,
    create_extraction_failure_rule,
    create_load_failure_rule,
    create_quality_degradation_rule,
    create_high_memory_rule,
    create_slow_processing_rule,
    create_circuit_breaker_open_rule,
)

from src.alerting.notifiers import (
    Notifier,
    EmailNotifier,
    SlackNotifier,
    WebhookNotifier,
    LogFileNotifier,
    NotificationManager,
)

__all__ = [
    "AlertSeverity",
    "AlertStatus",
    "Alert",
    "AlertRule",
    "AlertRuleEngine",
    "create_extraction_failure_rule",
    "create_load_failure_rule",
    "create_quality_degradation_rule",
    "create_high_memory_rule",
    "create_slow_processing_rule",
    "create_circuit_breaker_open_rule",
    "Notifier",
    "EmailNotifier",
    "SlackNotifier",
    "WebhookNotifier",
    "LogFileNotifier",
    "NotificationManager",
]
