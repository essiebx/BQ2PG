#!/usr/bin/env python3
"""
Standalone BQ2PG Monitoring API Server - Demo Version
Minimal dependencies for quick testing
"""

from flask import Flask, jsonify
from datetime import datetime
import os

app = Flask(__name__)

# Demo data
SYSTEM_STATUS = {
    "status": "healthy",
    "components": {
        "database": {"status": "healthy", "latency_ms": 12},
        "bigquery": {"status": "healthy", "latency_ms": 45},
        "memory": {"status": "healthy", "usage_percent": 35},
        "pipeline": {"status": "healthy"}
    }
}

ALERTS = [
    {
        "id": "alert_001",
        "name": "Pipeline Health Check",
        "severity": "INFO",
        "message": "Pipeline running normally",
        "timestamp": datetime.now().isoformat()
    }
]

@app.route('/health', methods=['GET'])
def health():
    """Overall system health check"""
    return jsonify({
        "status": SYSTEM_STATUS["status"],
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }), 200

@app.route('/health/<component>', methods=['GET'])
def component_health(component):
    """Component-specific health check"""
    if component in SYSTEM_STATUS["components"]:
        return jsonify({
            "component": component,
            **SYSTEM_STATUS["components"][component],
            "timestamp": datetime.now().isoformat()
        }), 200
    return jsonify({"error": f"Component {component} not found"}), 404

@app.route('/alerts', methods=['GET'])
def get_alerts():
    """Get current alerts"""
    return jsonify({
        "alerts": ALERTS,
        "count": len(ALERTS),
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/alerts/summary', methods=['GET'])
def alerts_summary():
    """Get alerts summary"""
    return jsonify({
        "total_alerts": len(ALERTS),
        "by_severity": {
            "critical": sum(1 for a in ALERTS if a["severity"] == "CRITICAL"),
            "warning": sum(1 for a in ALERTS if a["severity"] == "WARNING"),
            "info": sum(1 for a in ALERTS if a["severity"] == "INFO")
        },
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/metrics', methods=['GET'])
def metrics():
    """Prometheus metrics"""
    return jsonify({
        "pipeline_extractions_total": 1500000,
        "pipeline_loads_total": 1500000,
        "pipeline_errors_total": 42,
        "pipeline_duration_seconds": 3600,
        "database_connections": 10,
        "memory_usage_bytes": 524288000,
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/dashboards', methods=['GET'])
def dashboards():
    """Available dashboards"""
    return jsonify({
        "dashboards": [
            {"id": "extraction", "name": "Extraction Dashboard", "metrics": ["extractions", "errors", "throughput"]},
            {"id": "load", "name": "Load Dashboard", "metrics": ["loads", "throughput", "latency"]},
            {"id": "quality", "name": "Quality Dashboard", "metrics": ["quality_score", "violations", "null_counts"]},
            {"id": "system", "name": "System Dashboard", "metrics": ["cpu", "memory", "connections"]},
            {"id": "overview", "name": "Overview Dashboard", "metrics": ["all_metrics"]}
        ],
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/dashboards/<dashboard_id>', methods=['GET'])
def get_dashboard(dashboard_id):
    """Get specific dashboard"""
    dashboards = {
        "extraction": {"panels": 4, "refresh": "30s"},
        "load": {"panels": 3, "refresh": "30s"},
        "quality": {"panels": 5, "refresh": "60s"},
        "system": {"panels": 6, "refresh": "30s"},
        "overview": {"panels": 8, "refresh": "15s"}
    }
    
    if dashboard_id in dashboards:
        return jsonify({
            "id": dashboard_id,
            **dashboards[dashboard_id],
            "timestamp": datetime.now().isoformat()
        }), 200
    return jsonify({"error": f"Dashboard {dashboard_id} not found"}), 404

@app.route('/pipeline/status', methods=['GET'])
def pipeline_status():
    """Pipeline execution status"""
    return jsonify({
        "status": "running",
        "records_processed": 1500000,
        "records_loaded": 1500000,
        "errors": 42,
        "throughput_records_per_sec": 416.7,
        "estimated_completion": "2 hours",
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/pipeline/pause', methods=['POST'])
def pause_pipeline():
    """Pause pipeline"""
    return jsonify({
        "action": "pause",
        "status": "paused",
        "message": "Pipeline paused successfully",
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/pipeline/resume', methods=['POST'])
def resume_pipeline():
    """Resume pipeline"""
    return jsonify({
        "action": "resume",
        "status": "running",
        "message": "Pipeline resumed successfully",
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/rules', methods=['GET'])
def get_rules():
    """Get alert rules"""
    return jsonify({
        "rules": [
            {"id": "extraction_failure", "severity": "CRITICAL", "enabled": True},
            {"id": "load_failure", "severity": "CRITICAL", "enabled": True},
            {"id": "quality_degradation", "severity": "WARNING", "enabled": True},
            {"id": "high_memory", "severity": "WARNING", "enabled": True},
            {"id": "slow_processing", "severity": "INFO", "enabled": True},
            {"id": "circuit_breaker_open", "severity": "CRITICAL", "enabled": True}
        ],
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/info', methods=['GET'])
def info():
    """System information"""
    return jsonify({
        "project": "BQ2PG",
        "version": "1.0.0",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "features": ["extraction", "transformation", "loading", "quality_check", "monitoring", "alerting"],
        "uptime_seconds": 3600,
        "timestamp": datetime.now().isoformat()
    }), 200

@app.errorhandler(404)
def not_found(error):
    """404 error handler"""
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    """500 error handler"""
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    print(f"\n[OK] Starting BQ2PG Monitoring API Server on http://0.0.0.0:{port}")
    print(f"[OK] Available endpoints:")
    print(f"     GET  /health                 - Overall system health")
    print(f"     GET  /health/<component>     - Component health (database, bigquery, memory, pipeline)")
    print(f"     GET  /alerts                 - Current alerts")
    print(f"     GET  /alerts/summary         - Alerts summary")
    print(f"     GET  /metrics                - Prometheus metrics")
    print(f"     GET  /dashboards             - Available dashboards")
    print(f"     GET  /dashboards/<id>        - Specific dashboard")
    print(f"     GET  /pipeline/status        - Pipeline status")
    print(f"     POST /pipeline/pause         - Pause pipeline")
    print(f"     POST /pipeline/resume        - Resume pipeline")
    print(f"     GET  /rules                  - Alert rules")
    print(f"     GET  /info                   - System info\n")
    
    app.run(host='0.0.0.0', port=port, debug=False)
