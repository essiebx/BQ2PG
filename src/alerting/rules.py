# src/alerting/rules.py
"""
Alert rules engine for pipeline monitoring and notifications.
"""

from typing import Callable, Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import json

from ..monitoring import StructuredLogger, MetricsCollector

logger = StructuredLogger("alerting_rules", level="INFO")
metrics = MetricsCollector(namespace="bq2pg_alerts")


class AlertSeverity(Enum):
    """Alert severity levels"""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertStatus(Enum):
    """Alert status"""
    TRIGGERED = "triggered"
    RESOLVED = "resolved"
    ACKNOWLEDGED = "acknowledged"


@dataclass
class Alert:
    """Alert event"""
    rule_id: str
    rule_name: str
    severity: AlertSeverity
    message: str
    timestamp: datetime
    value: float
    threshold: float
    status: AlertStatus = AlertStatus.TRIGGERED
    context: Dict[str, Any] = field(default_factory=dict)
    acknowledgment: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "severity": self.severity.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "threshold": self.threshold,
            "status": self.status.value,
            "context": self.context,
            "acknowledgment": self.acknowledgment
        }
    
    def to_json(self) -> str:
        """Convert to JSON"""
        return json.dumps(self.to_dict())


@dataclass
class AlertRule:
    """Alert rule definition"""
    rule_id: str
    name: str
    description: str
    metric_name: str
    condition: Callable[[float], bool]
    severity: AlertSeverity
    enabled: bool = True
    cooldown_minutes: int = 5
    last_triggered: Optional[datetime] = None
    
    def evaluate(self, value: float, threshold: float) -> Optional[Alert]:
        """
        Evaluate rule against metric value.
        
        Args:
            value: Metric value to evaluate
            threshold: Alert threshold
            
        Returns:
            Alert if condition met, None otherwise
        """
        if not self.enabled:
            return None
        
        # Check cooldown
        if self.last_triggered:
            minutes_since = (datetime.now() - self.last_triggered).total_seconds() / 60
            if minutes_since < self.cooldown_minutes:
                return None
        
        # Evaluate condition
        if self.condition(value):
            self.last_triggered = datetime.now()
            metrics.increment_custom_metric(f"alert_triggered_{self.rule_id}")
            
            return Alert(
                rule_id=self.rule_id,
                rule_name=self.name,
                severity=self.severity,
                message=self.description,
                timestamp=datetime.now(),
                value=value,
                threshold=threshold
            )
        
        return None


class AlertRuleEngine:
    """Rule engine for managing and evaluating alerts"""
    
    def __init__(self):
        self.rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
    
    def register_rule(self, rule: AlertRule) -> None:
        """Register alert rule"""
        self.rules[rule.rule_id] = rule
        logger.info(f"Rule registered: {rule.name}", rule_id=rule.rule_id)
    
    def unregister_rule(self, rule_id: str) -> None:
        """Unregister alert rule"""
        if rule_id in self.rules:
            del self.rules[rule_id]
            logger.info(f"Rule unregistered: {rule_id}")
    
    def enable_rule(self, rule_id: str) -> None:
        """Enable alert rule"""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = True
            logger.info(f"Rule enabled: {rule_id}")
    
    def disable_rule(self, rule_id: str) -> None:
        """Disable alert rule"""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = False
            logger.info(f"Rule disabled: {rule_id}")
    
    def evaluate_metric(self, metric_name: str, value: float, threshold: float) -> List[Alert]:
        """
        Evaluate metric against all applicable rules.
        
        Args:
            metric_name: Metric name
            value: Metric value
            threshold: Alert threshold
            
        Returns:
            List of triggered alerts
        """
        alerts = []
        
        for rule in self.rules.values():
            if rule.metric_name == metric_name:
                alert = rule.evaluate(value, threshold)
                if alert:
                    alerts.append(alert)
                    self._track_alert(alert)
        
        return alerts
    
    def acknowledge_alert(self, alert_id: str, acknowledgment: str) -> None:
        """Acknowledge alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].status = AlertStatus.ACKNOWLEDGED
            self.active_alerts[alert_id].acknowledgment = acknowledgment
            logger.info(f"Alert acknowledged: {alert_id}")
    
    def resolve_alert(self, alert_id: str) -> None:
        """Resolve alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].status = AlertStatus.RESOLVED
            logger.info(f"Alert resolved: {alert_id}")
    
    def _track_alert(self, alert: Alert) -> None:
        """Track alert in active and history"""
        alert_id = f"{alert.rule_id}_{alert.timestamp.timestamp()}"
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        metrics.set_custom_metric(f"active_alerts", len(self.active_alerts))
    
    def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get active alerts"""
        alerts = list(self.active_alerts.values())
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary"""
        active = self.get_active_alerts()
        critical = [a for a in active if a.severity == AlertSeverity.CRITICAL]
        warnings = [a for a in active if a.severity == AlertSeverity.WARNING]
        
        return {
            "total_active": len(active),
            "critical": len(critical),
            "warnings": len(warnings),
            "total_history": len(self.alert_history),
            "rules_enabled": sum(1 for r in self.rules.values() if r.enabled),
            "rules_total": len(self.rules)
        }


# Predefined alert rules
def create_extraction_failure_rule() -> AlertRule:
    """Create rule for extraction failures"""
    return AlertRule(
        rule_id="extraction_failure",
        name="Extraction Failure Rate",
        description="Extraction failure rate exceeded threshold",
        metric_name="extraction_failures_total",
        condition=lambda v: v > 10,
        severity=AlertSeverity.CRITICAL,
        cooldown_minutes=5
    )


def create_load_failure_rule() -> AlertRule:
    """Create rule for load failures"""
    return AlertRule(
        rule_id="load_failure",
        name="Load Failure Rate",
        description="Load failure rate exceeded threshold",
        metric_name="load_failures_total",
        condition=lambda v: v > 20,
        severity=AlertSeverity.CRITICAL,
        cooldown_minutes=5
    )


def create_quality_degradation_rule() -> AlertRule:
    """Create rule for quality score degradation"""
    return AlertRule(
        rule_id="quality_degradation",
        name="Quality Score Degradation",
        description="Quality score dropped below threshold",
        metric_name="validation_quality_score",
        condition=lambda v: v < 80,
        severity=AlertSeverity.WARNING,
        cooldown_minutes=10
    )


def create_high_memory_rule() -> AlertRule:
    """Create rule for high memory usage"""
    return AlertRule(
        rule_id="high_memory",
        name="High Memory Usage",
        description="Memory usage exceeded threshold",
        metric_name="memory_usage_percent",
        condition=lambda v: v > 85,
        severity=AlertSeverity.WARNING,
        cooldown_minutes=5
    )


def create_slow_processing_rule() -> AlertRule:
    """Create rule for slow processing"""
    return AlertRule(
        rule_id="slow_processing",
        name="Slow Processing",
        description="Processing duration exceeded threshold",
        metric_name="pipeline_duration_seconds",
        condition=lambda v: v > 3600,  # 1 hour
        severity=AlertSeverity.WARNING,
        cooldown_minutes=30
    )


def create_circuit_breaker_open_rule() -> AlertRule:
    """Create rule for circuit breaker open"""
    return AlertRule(
        rule_id="circuit_breaker_open",
        name="Circuit Breaker Open",
        description="Circuit breaker triggered",
        metric_name="circuit_breaker_open",
        condition=lambda v: v > 0,
        severity=AlertSeverity.CRITICAL,
        cooldown_minutes=1
    )
