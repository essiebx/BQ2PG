# scripts/monitoring/api_server.py
"""
REST API server for monitoring and control endpoints.
"""

from flask import Flask, jsonify, request
from datetime import datetime
import logging
import os

# Add parent directories to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.monitoring import StructuredLogger
from src.monitoring.health_check import (
    HealthCheckManager, DatabaseHealthChecker, BigQueryHealthChecker,
    MemoryHealthChecker, PipelineHealthChecker, HealthStatus
)
from src.alerting import AlertRuleEngine, create_extraction_failure_rule, create_load_failure_rule
from src.dashboards.grafana import DashboardExporter

app = Flask(__name__)
logger = StructuredLogger("api_server", level="INFO")

# Initialize managers
health_manager = HealthCheckManager()
alert_engine = AlertRuleEngine()

# Register health checkers
health_manager.register_checker(MemoryHealthChecker("memory"))
health_manager.register_checker(PipelineHealthChecker("pipeline"))

# Register alert rules
alert_engine.register_rule(create_extraction_failure_rule())
alert_engine.register_rule(create_load_failure_rule())


@app.route('/health', methods=['GET'])
def get_health():
    """Get overall health status"""
    report = health_manager.get_health_report()
    
    status_code = 200
    if report['overall_status'] == HealthStatus.UNHEALTHY.value:
        status_code = 503
    elif report['overall_status'] == HealthStatus.DEGRADED.value:
        status_code = 200  # Still OK but degraded
    
    return jsonify(report), status_code


@app.route('/health/<component>', methods=['GET'])
def get_component_health(component: str):
    """Get specific component health"""
    if component not in health_manager.checkers:
        return jsonify({"error": f"Unknown component: {component}"}), 404
    
    checker = health_manager.checkers[component]
    result = checker.check()
    
    return jsonify(result.to_dict()), 200


@app.route('/alerts', methods=['GET'])
def get_alerts():
    """Get active alerts"""
    severity = request.args.get('severity')
    alerts = alert_engine.get_active_alerts()
    
    if severity:
        alerts = [a for a in alerts if a.severity.value == severity]
    
    return jsonify({
        "total": len(alerts),
        "alerts": [a.to_dict() for a in alerts]
    }), 200


@app.route('/alerts/summary', methods=['GET'])
def get_alerts_summary():
    """Get alerts summary"""
    summary = alert_engine.get_alert_summary()
    return jsonify(summary), 200


@app.route('/alerts/<alert_id>/acknowledge', methods=['POST'])
def acknowledge_alert(alert_id: str):
    """Acknowledge alert"""
    data = request.get_json()
    acknowledgment = data.get('acknowledgment', '')
    
    alert_engine.acknowledge_alert(alert_id, acknowledgment)
    logger.info(f"Alert acknowledged: {alert_id}")
    
    return jsonify({"status": "acknowledged"}), 200


@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get metrics in Prometheus format"""
    # This would integrate with MetricsCollector
    return "", 200


@app.route('/dashboards/<dashboard_type>', methods=['GET'])
def get_dashboard(dashboard_type: str):
    """Get dashboard JSON"""
    try:
        dashboard_json = DashboardExporter.get_dashboard_json(dashboard_type)
        return dashboard_json, 200, {"Content-Type": "application/json"}
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route('/dashboards', methods=['GET'])
def list_dashboards():
    """List available dashboards"""
    dashboards = [
        "extraction",
        "load",
        "quality",
        "system",
        "overview"
    ]
    return jsonify({"dashboards": dashboards}), 200


@app.route('/pipeline/status', methods=['GET'])
def get_pipeline_status():
    """Get pipeline status"""
    health_report = health_manager.get_health_report()
    alerts_summary = alert_engine.get_alert_summary()
    
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "health": health_report,
        "alerts": alerts_summary
    }), 200


@app.route('/pipeline/resume', methods=['POST'])
def resume_pipeline():
    """Resume pipeline (if paused)"""
    logger.info("Resume pipeline requested")
    return jsonify({"status": "resumed"}), 200


@app.route('/pipeline/pause', methods=['POST'])
def pause_pipeline():
    """Pause pipeline"""
    logger.info("Pause pipeline requested")
    return jsonify({"status": "paused"}), 200


@app.route('/rules/<rule_id>/enable', methods=['POST'])
def enable_rule(rule_id: str):
    """Enable alert rule"""
    alert_engine.enable_rule(rule_id)
    logger.info(f"Rule enabled: {rule_id}")
    return jsonify({"status": "enabled"}), 200


@app.route('/rules/<rule_id>/disable', methods=['POST'])
def disable_rule(rule_id: str):
    """Disable alert rule"""
    alert_engine.disable_rule(rule_id)
    logger.info(f"Rule disabled: {rule_id}")
    return jsonify({"status": "disabled"}), 200


@app.route('/rules', methods=['GET'])
def list_rules():
    """List alert rules"""
    rules = []
    for rule_id, rule in alert_engine.rules.items():
        rules.append({
            "rule_id": rule_id,
            "name": rule.name,
            "enabled": rule.enabled,
            "metric": rule.metric_name,
            "severity": rule.severity.value
        })
    return jsonify({"rules": rules}), 200


@app.route('/info', methods=['GET'])
def get_info():
    """Get API info"""
    return jsonify({
        "name": "BQ2PG Monitoring API",
        "version": "3.0",
        "endpoints": [
            "/health",
            "/health/<component>",
            "/alerts",
            "/alerts/summary",
            "/metrics",
            "/dashboards",
            "/dashboards/<type>",
            "/pipeline/status",
            "/rules",
            "/info"
        ]
    }), 200


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    logger.info("Starting BQ2PG Monitoring API Server")
    app.run(host='0.0.0.0', port=5000, debug=False)
