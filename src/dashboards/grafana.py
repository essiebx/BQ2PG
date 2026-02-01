# src/dashboards/grafana.py
"""
Grafana dashboard definitions and templates.
"""

import json
from typing import Dict, Any, List


class GrafanaDashboard:
    """Grafana dashboard definition"""
    
    def __init__(self, title: str, description: str):
        self.title = title
        self.description = description
        self.panels: List[Dict[str, Any]] = []
        self.templating = {"list": []}
    
    def add_panel(self, panel: Dict[str, Any]) -> None:
        """Add panel to dashboard"""
        panel["id"] = len(self.panels) + 1
        self.panels.append(panel)
    
    def to_json(self) -> str:
        """Convert to Grafana JSON"""
        dashboard = {
            "dashboard": {
                "title": self.title,
                "description": self.description,
                "tags": ["bq2pg", "pipeline"],
                "timezone": "browser",
                "panels": self.panels,
                "templating": self.templating,
                "refresh": "10s"
            },
            "overwrite": True
        }
        return json.dumps(dashboard, indent=2)


def create_metrics_panel(
    title: str,
    metric: str,
    unit: str = "short",
    x: int = 0,
    y: int = 0
) -> Dict[str, Any]:
    """Create Grafana metrics panel"""
    return {
        "title": title,
        "type": "stat",
        "gridPos": {"x": x, "y": y, "w": 12, "h": 8},
        "targets": [
            {
                "expr": f'bq2pg_{metric}',
                "refId": "A"
            }
        ],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "hideFrom": {
                        "tooltip": False,
                        "viz": False,
                        "legend": False
                    }
                }
            }
        }
    }


def create_graph_panel(
    title: str,
    metrics: List[str],
    x: int = 0,
    y: int = 0
) -> Dict[str, Any]:
    """Create Grafana time series graph panel"""
    targets = [
        {"expr": f'bq2pg_{metric}', "refId": chr(65 + i)}
        for i, metric in enumerate(metrics)
    ]
    
    return {
        "title": title,
        "type": "timeseries",
        "gridPos": {"x": x, "y": y, "w": 24, "h": 8},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "line",
                    "fillOpacity": 10,
                    "pointSize": 5
                }
            }
        }
    }


def create_extraction_dashboard() -> GrafanaDashboard:
    """Create extraction metrics dashboard"""
    dashboard = GrafanaDashboard(
        title="BQ2PG - Extraction Metrics",
        description="BigQuery extraction performance and reliability"
    )
    
    dashboard.add_panel(create_metrics_panel(
        "Extraction Success Rate",
        "extraction_success_total",
        unit="percent"
    ))
    
    dashboard.add_panel(create_metrics_panel(
        "Extraction Failures",
        "extraction_failures_total",
        unit="short",
        x=12
    ))
    
    dashboard.add_panel(create_graph_panel(
        "Extraction Throughput (rows/sec)",
        ["extraction_throughput", "extraction_bytes_per_sec"],
        y=8
    ))
    
    return dashboard


def create_load_dashboard() -> GrafanaDashboard:
    """Create load metrics dashboard"""
    dashboard = GrafanaDashboard(
        title="BQ2PG - Load Metrics",
        description="PostgreSQL load performance and reliability"
    )
    
    dashboard.add_panel(create_metrics_panel(
        "Load Success Rate",
        "load_success_total",
        unit="percent"
    ))
    
    dashboard.add_panel(create_metrics_panel(
        "Load Failures",
        "load_failures_total",
        x=12
    ))
    
    dashboard.add_panel(create_graph_panel(
        "Load Throughput (rows/sec)",
        ["load_throughput"],
        y=8
    ))
    
    return dashboard


def create_quality_dashboard() -> GrafanaDashboard:
    """Create data quality dashboard"""
    dashboard = GrafanaDashboard(
        title="BQ2PG - Data Quality",
        description="Data quality metrics and validation results"
    )
    
    dashboard.add_panel(create_metrics_panel(
        "Average Quality Score",
        "validation_quality_score",
        unit="percent"
    ))
    
    dashboard.add_panel(create_metrics_panel(
        "Validation Failures",
        "validation_failures_total",
        x=12
    ))
    
    dashboard.add_panel(create_graph_panel(
        "Quality Score Over Time",
        ["validation_quality_score"],
        y=8
    ))
    
    return dashboard


def create_system_dashboard() -> GrafanaDashboard:
    """Create system health dashboard"""
    dashboard = GrafanaDashboard(
        title="BQ2PG - System Health",
        description="System resource usage and health status"
    )
    
    dashboard.add_panel(create_metrics_panel(
        "Memory Usage %",
        "memory_usage_percent",
        unit="percent"
    ))
    
    dashboard.add_panel(create_metrics_panel(
        "CPU Usage %",
        "cpu_usage_percent",
        unit="percent",
        x=12
    ))
    
    dashboard.add_panel(create_graph_panel(
        "System Resources",
        ["memory_usage_percent", "cpu_usage_percent"],
        y=8
    ))
    
    return dashboard


def create_pipeline_overview_dashboard() -> GrafanaDashboard:
    """Create overall pipeline overview dashboard"""
    dashboard = GrafanaDashboard(
        title="BQ2PG - Pipeline Overview",
        description="Overall pipeline health and performance"
    )
    
    dashboard.add_panel(create_metrics_panel(
        "Total Rows Extracted",
        "extraction_total_rows",
        unit="short"
    ))
    
    dashboard.add_panel(create_metrics_panel(
        "Total Rows Loaded",
        "load_total_rows",
        unit="short",
        x=12
    ))
    
    dashboard.add_panel(create_metrics_panel(
        "Active Alerts",
        "alert_active_count",
        unit="short",
        y=8
    ))
    
    dashboard.add_panel(create_metrics_panel(
        "Pipeline Health",
        "pipeline_health_status",
        unit="short",
        x=12,
        y=8
    ))
    
    dashboard.add_panel(create_graph_panel(
        "Pipeline Throughput",
        ["extraction_throughput", "load_throughput"],
        y=16
    ))
    
    return dashboard


class DashboardExporter:
    """Export dashboard definitions"""
    
    @staticmethod
    def export_all_dashboards(output_dir: str = "dashboards") -> None:
        """Export all dashboard definitions"""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        dashboards = [
            ("extraction", create_extraction_dashboard()),
            ("load", create_load_dashboard()),
            ("quality", create_quality_dashboard()),
            ("system", create_system_dashboard()),
            ("overview", create_pipeline_overview_dashboard()),
        ]
        
        for name, dashboard in dashboards:
            file_path = os.path.join(output_dir, f"{name}_dashboard.json")
            with open(file_path, "w") as f:
                f.write(dashboard.to_json())
            print(f"Exported: {file_path}")
    
    @staticmethod
    def get_dashboard_json(dashboard_type: str) -> str:
        """Get dashboard JSON by type"""
        dashboards = {
            "extraction": create_extraction_dashboard(),
            "load": create_load_dashboard(),
            "quality": create_quality_dashboard(),
            "system": create_system_dashboard(),
            "overview": create_pipeline_overview_dashboard(),
        }
        
        if dashboard_type not in dashboards:
            raise ValueError(f"Unknown dashboard type: {dashboard_type}")
        
        return dashboards[dashboard_type].to_json()
