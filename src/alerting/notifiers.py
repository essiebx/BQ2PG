# src/alerting/notifiers.py
"""
Alert notification system for sending alerts via various channels.
"""

from typing import List, Optional, Dict, Any
from abc import ABC, abstractmethod
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import requests

from .rules import Alert, AlertSeverity
from ..monitoring import StructuredLogger

logger = StructuredLogger("alerting_notifiers", level="INFO")


class Notifier(ABC):
    """Base class for alert notifiers"""
    
    @abstractmethod
    def notify(self, alert: Alert) -> bool:
        """Send alert notification"""
        pass
    
    @abstractmethod
    def notify_batch(self, alerts: List[Alert]) -> bool:
        """Send batch alert notifications"""
        pass


class EmailNotifier(Notifier):
    """Send alerts via email"""
    
    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        from_email: str,
        from_password: str,
        to_emails: List[str]
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.from_email = from_email
        self.from_password = from_password
        self.to_emails = to_emails
    
    def notify(self, alert: Alert) -> bool:
        """Send alert via email"""
        try:
            subject = f"[{alert.severity.value.upper()}] {alert.rule_name}"
            body = self._format_alert(alert)
            
            msg = MIMEMultipart()
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "html"))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.from_password)
                server.send_message(msg)
            
            logger.info(
                "Alert sent via email",
                alert_id=alert.rule_id,
                recipients=len(self.to_emails)
            )
            return True
            
        except Exception as e:
            logger.error(
                "Email notification failed",
                error=str(e),
                alert_id=alert.rule_id
            )
            return False
    
    def notify_batch(self, alerts: List[Alert]) -> bool:
        """Send batch alerts via email"""
        if not alerts:
            return True
        
        try:
            subject = f"Alert Batch: {len(alerts)} alerts from BQ2PG Pipeline"
            body = self._format_batch(alerts)
            
            msg = MIMEMultipart()
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "html"))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.from_password)
                server.send_message(msg)
            
            logger.info(
                "Batch alerts sent via email",
                alert_count=len(alerts),
                recipients=len(self.to_emails)
            )
            return True
            
        except Exception as e:
            logger.error("Batch email notification failed", error=str(e))
            return False
    
    def _format_alert(self, alert: Alert) -> str:
        """Format alert as HTML email"""
        color = "red" if alert.severity == AlertSeverity.CRITICAL else "orange"
        return f"""
        <html>
            <body style="font-family: Arial, sans-serif;">
                <h2 style="color: {color};">{alert.rule_name}</h2>
                <p><strong>Severity:</strong> {alert.severity.value.upper()}</p>
                <p><strong>Timestamp:</strong> {alert.timestamp.isoformat()}</p>
                <p><strong>Message:</strong> {alert.message}</p>
                <p><strong>Current Value:</strong> {alert.value:.2f}</p>
                <p><strong>Threshold:</strong> {alert.threshold:.2f}</p>
                <hr>
                <p>BQ2PG Pipeline Alert System</p>
            </body>
        </html>
        """
    
    def _format_batch(self, alerts: List[Alert]) -> str:
        """Format batch alerts as HTML email"""
        alert_rows = "".join([
            f"<tr><td>{a.rule_name}</td><td>{a.severity.value}</td><td>{a.value:.2f}</td></tr>"
            for a in alerts
        ])
        return f"""
        <html>
            <body style="font-family: Arial, sans-serif;">
                <h2>Pipeline Alert Summary</h2>
                <p><strong>Total Alerts:</strong> {len(alerts)}</p>
                <table border="1" style="border-collapse: collapse; margin-top: 20px;">
                    <tr style="background-color: #f0f0f0;">
                        <th>Rule</th>
                        <th>Severity</th>
                        <th>Value</th>
                    </tr>
                    {alert_rows}
                </table>
                <hr>
                <p>BQ2PG Pipeline Alert System</p>
            </body>
        </html>
        """


class SlackNotifier(Notifier):
    """Send alerts via Slack"""
    
    def __init__(self, webhook_url: str, channel: str = "#alerts"):
        self.webhook_url = webhook_url
        self.channel = channel
    
    def notify(self, alert: Alert) -> bool:
        """Send alert via Slack"""
        try:
            color = "danger" if alert.severity == AlertSeverity.CRITICAL else "warning"
            payload = {
                "channel": self.channel,
                "attachments": [{
                    "color": color,
                    "title": alert.rule_name,
                    "text": alert.message,
                    "fields": [
                        {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
                        {"title": "Value", "value": str(alert.value), "short": True},
                        {"title": "Threshold", "value": str(alert.threshold), "short": True},
                        {"title": "Timestamp", "value": alert.timestamp.isoformat(), "short": False}
                    ]
                }]
            }
            
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            
            logger.info("Alert sent via Slack", alert_id=alert.rule_id)
            return True
            
        except Exception as e:
            logger.error("Slack notification failed", error=str(e), alert_id=alert.rule_id)
            return False
    
    def notify_batch(self, alerts: List[Alert]) -> bool:
        """Send batch alerts via Slack"""
        if not alerts:
            return True
        
        try:
            fields = []
            for alert in alerts:
                fields.append({
                    "title": alert.rule_name,
                    "value": f"{alert.severity.value.upper()} - {alert.value:.2f}",
                    "short": True
                })
            
            payload = {
                "channel": self.channel,
                "attachments": [{
                    "color": "danger",
                    "title": f"Alert Batch: {len(alerts)} alerts",
                    "text": f"Summary of alerts from BQ2PG Pipeline",
                    "fields": fields[:20]  # Slack limit
                }]
            }
            
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            
            logger.info("Batch alerts sent via Slack", alert_count=len(alerts))
            return True
            
        except Exception as e:
            logger.error("Batch Slack notification failed", error=str(e))
            return False


class WebhookNotifier(Notifier):
    """Send alerts to custom webhook"""
    
    def __init__(self, webhook_url: str, headers: Optional[Dict[str, str]] = None):
        self.webhook_url = webhook_url
        self.headers = headers or {"Content-Type": "application/json"}
    
    def notify(self, alert: Alert) -> bool:
        """Send alert to webhook"""
        try:
            payload = alert.to_dict()
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info("Alert sent to webhook", alert_id=alert.rule_id)
            return True
            
        except Exception as e:
            logger.error("Webhook notification failed", error=str(e), alert_id=alert.rule_id)
            return False
    
    def notify_batch(self, alerts: List[Alert]) -> bool:
        """Send batch alerts to webhook"""
        try:
            payload = {
                "timestamp": datetime.now().isoformat(),
                "alert_count": len(alerts),
                "alerts": [a.to_dict() for a in alerts]
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info("Batch alerts sent to webhook", alert_count=len(alerts))
            return True
            
        except Exception as e:
            logger.error("Batch webhook notification failed", error=str(e))
            return False


class LogFileNotifier(Notifier):
    """Write alerts to log file"""
    
    def __init__(self, log_file: str = "alerts.jsonl"):
        self.log_file = log_file
    
    def notify(self, alert: Alert) -> bool:
        """Write alert to log file"""
        try:
            with open(self.log_file, "a") as f:
                f.write(alert.to_json() + "\n")
            
            logger.info("Alert written to log file", alert_id=alert.rule_id)
            return True
            
        except Exception as e:
            logger.error("Log file notification failed", error=str(e))
            return False
    
    def notify_batch(self, alerts: List[Alert]) -> bool:
        """Write batch alerts to log file"""
        try:
            with open(self.log_file, "a") as f:
                for alert in alerts:
                    f.write(alert.to_json() + "\n")
            
            logger.info("Batch alerts written to log file", alert_count=len(alerts))
            return True
            
        except Exception as e:
            logger.error("Batch log file notification failed", error=str(e))
            return False


class NotificationManager:
    """Manage multiple notifiers"""
    
    def __init__(self):
        self.notifiers: List[Notifier] = []
    
    def add_notifier(self, notifier: Notifier) -> None:
        """Add notifier"""
        self.notifiers.append(notifier)
        logger.info(f"Notifier added: {type(notifier).__name__}")
    
    def notify(self, alert: Alert) -> None:
        """Send alert via all notifiers"""
        for notifier in self.notifiers:
            try:
                notifier.notify(alert)
            except Exception as e:
                logger.error(
                    f"Notifier failed: {type(notifier).__name__}",
                    error=str(e)
                )
    
    def notify_batch(self, alerts: List[Alert]) -> None:
        """Send batch alerts via all notifiers"""
        for notifier in self.notifiers:
            try:
                notifier.notify_batch(alerts)
            except Exception as e:
                logger.error(
                    f"Batch notifier failed: {type(notifier).__name__}",
                    error=str(e)
                )
