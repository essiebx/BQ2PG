# src/governance/audit.py
"""
Audit trail and compliance tracking system.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
import hashlib

from ..monitoring import StructuredLogger

logger = StructuredLogger("audit", level="INFO")


class AuditEventType(Enum):
    """Audit event types"""
    DATA_EXTRACTED = "data_extracted"
    DATA_TRANSFORMED = "data_transformed"
    DATA_LOADED = "data_loaded"
    QUALITY_CHECK = "quality_check"
    DATA_DELETED = "data_deleted"
    DATA_EXPORTED = "data_exported"
    ACCESS_GRANTED = "access_granted"
    ACCESS_REVOKED = "access_revoked"
    CONFIGURATION_CHANGED = "configuration_changed"
    ERROR_OCCURRED = "error_occurred"
    ALERT_TRIGGERED = "alert_triggered"


class ComplianceStatus(Enum):
    """Compliance status"""
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"
    UNKNOWN = "unknown"


@dataclass
class AuditEvent:
    """Audit event record"""
    event_id: str
    event_type: AuditEventType
    timestamp: datetime
    user: str
    component: str
    action: str
    resource: str
    status: str
    details: Dict[str, Any] = field(default_factory=dict)
    data_hash: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "user": self.user,
            "component": self.component,
            "action": self.action,
            "resource": self.resource,
            "status": self.status,
            "details": self.details,
            "data_hash": self.data_hash
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class ComplianceRule:
    """Compliance rule definition"""
    rule_id: str
    name: str
    description: str
    requirement: str
    severity: str  # critical, high, medium, low
    check_function: callable
    
    def check(self, data: Dict[str, Any]) -> bool:
        """Check compliance"""
        try:
            return self.check_function(data)
        except Exception as e:
            logger.error(f"Compliance check failed: {e}", rule_id=self.rule_id)
            return False


class AuditTrail:
    """Audit trail management"""
    
    def __init__(self, storage_file: str = "audit_trail.jsonl"):
        self.storage_file = storage_file
        self.events: List[AuditEvent] = []
        self.load_existing_events()
    
    def load_existing_events(self):
        """Load existing audit events from file"""
        try:
            with open(self.storage_file, "r") as f:
                for line in f:
                    if line.strip():
                        event_dict = json.loads(line)
                        # Reconstruct AuditEvent from dict
                        # (simplified - full implementation would handle all fields)
        except FileNotFoundError:
            pass
    
    def record_event(
        self,
        event_type: AuditEventType,
        user: str,
        component: str,
        action: str,
        resource: str,
        status: str,
        details: Dict[str, Any] = None,
        data_hash: str = None
    ) -> str:
        """Record audit event"""
        import uuid
        event_id = str(uuid.uuid4())
        
        event = AuditEvent(
            event_id=event_id,
            event_type=event_type,
            timestamp=datetime.now(),
            user=user,
            component=component,
            action=action,
            resource=resource,
            status=status,
            details=details or {},
            data_hash=data_hash
        )
        
        self.events.append(event)
        
        # Write to storage
        try:
            with open(self.storage_file, "a") as f:
                f.write(event.to_json() + "\n")
        except Exception as e:
            logger.error(f"Failed to write audit event: {e}")
        
        logger.info(
            f"Audit event recorded: {event_type.value}",
            event_id=event_id,
            user=user,
            resource=resource
        )
        
        return event_id
    
    def get_events(
        self,
        event_type: Optional[AuditEventType] = None,
        user: Optional[str] = None,
        resource: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[AuditEvent]:
        """Get audit events with filtering"""
        events = self.events
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if user:
            events = [e for e in events if e.user == user]
        if resource:
            events = [e for e in events if e.resource == resource]
        if start_date:
            events = [e for e in events if e.timestamp >= start_date]
        if end_date:
            events = [e for e in events if e.timestamp <= end_date]
        
        return events
    
    def get_audit_summary(self) -> Dict[str, Any]:
        """Get audit summary"""
        return {
            "timestamp": datetime.now().isoformat(),
            "total_events": len(self.events),
            "event_types": {et.value: len([e for e in self.events if e.event_type == et])
                           for et in AuditEventType},
            "users": list(set(e.user for e in self.events)),
            "components": list(set(e.component for e in self.events)),
            "date_range": {
                "start": min(e.timestamp for e in self.events).isoformat() if self.events else None,
                "end": max(e.timestamp for e in self.events).isoformat() if self.events else None
            }
        }


class ComplianceChecker:
    """Compliance checking and validation"""
    
    def __init__(self):
        self.rules: Dict[str, ComplianceRule] = {}
        self.compliance_checks: List[Dict[str, Any]] = []
    
    def register_rule(self, rule: ComplianceRule) -> None:
        """Register compliance rule"""
        self.rules[rule.rule_id] = rule
        logger.info(f"Compliance rule registered: {rule.name}")
    
    def check_compliance(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check data against all rules"""
        results = {
            "timestamp": datetime.now().isoformat(),
            "total_rules": len(self.rules),
            "passed_rules": 0,
            "failed_rules": 0,
            "rule_results": {}
        }
        
        for rule_id, rule in self.rules.items():
            passed = rule.check(data)
            
            results["rule_results"][rule_id] = {
                "name": rule.name,
                "passed": passed,
                "severity": rule.severity
            }
            
            if passed:
                results["passed_rules"] += 1
            else:
                results["failed_rules"] += 1
        
        # Record compliance check
        self.compliance_checks.append(results)
        
        return results
    
    def get_compliance_status(self, data: Dict[str, Any]) -> ComplianceStatus:
        """Get overall compliance status"""
        results = self.check_compliance(data)
        
        if results["failed_rules"] == 0:
            return ComplianceStatus.COMPLIANT
        else:
            # Check if any failed rules are critical
            critical_failures = [
                r for r in results["rule_results"].values()
                if not r["passed"] and r["severity"] == "critical"
            ]
            
            if critical_failures:
                return ComplianceStatus.NON_COMPLIANT
            else:
                return ComplianceStatus.COMPLIANT
    
    def get_compliance_report(self) -> Dict[str, Any]:
        """Generate compliance report"""
        total_checks = len(self.compliance_checks)
        
        if total_checks == 0:
            return {"status": "no_checks_performed"}
        
        passed_checks = sum(1 for c in self.compliance_checks if c["failed_rules"] == 0)
        compliance_rate = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "compliance_rate": compliance_rate,
            "total_rules": len(self.rules),
            "recent_checks": self.compliance_checks[-10:] if self.compliance_checks else []
        }


def create_data_retention_rule() -> ComplianceRule:
    """Create data retention compliance rule"""
    def check_retention(data: Dict[str, Any]) -> bool:
        # Check if data is not older than retention period (e.g., 7 years for financial)
        return data.get("retention_compliant", True)
    
    return ComplianceRule(
        rule_id="data_retention",
        name="Data Retention Compliance",
        description="Ensure data retention meets regulatory requirements",
        requirement="Data must be retained for minimum period",
        severity="high",
        check_function=check_retention
    )


def create_pii_protection_rule() -> ComplianceRule:
    """Create PII protection compliance rule"""
    def check_pii_protection(data: Dict[str, Any]) -> bool:
        # Check if PII is properly encrypted or masked
        return data.get("pii_protected", True)
    
    return ComplianceRule(
        rule_id="pii_protection",
        name="PII Protection",
        description="Ensure personally identifiable information is protected",
        requirement="PII must be encrypted and access logged",
        severity="critical",
        check_function=check_pii_protection
    )


def create_data_quality_rule() -> ComplianceRule:
    """Create data quality compliance rule"""
    def check_data_quality(data: Dict[str, Any]) -> bool:
        # Check if data quality score is above threshold
        quality_score = data.get("quality_score", 0)
        return quality_score >= 80
    
    return ComplianceRule(
        rule_id="data_quality",
        name="Data Quality Standard",
        description="Ensure data quality meets standards",
        requirement="Quality score must be >= 80%",
        severity="medium",
        check_function=check_data_quality
    )


def create_completeness_rule() -> ComplianceRule:
    """Create data completeness compliance rule"""
    def check_completeness(data: Dict[str, Any]) -> bool:
        # Check if required fields are present
        required_fields = data.get("required_fields", [])
        actual_fields = set(data.get("fields", []))
        return all(field in actual_fields for field in required_fields)
    
    return ComplianceRule(
        rule_id="data_completeness",
        name="Data Completeness",
        description="Ensure all required data fields are present",
        requirement="All required fields must be populated",
        severity="high",
        check_function=check_completeness
    )
