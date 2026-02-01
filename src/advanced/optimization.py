# src/advanced/optimization.py
"""
Advanced performance optimization and auto-tuning.
"""

from typing import Dict, List, Any, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import json

from ..monitoring import StructuredLogger

logger = StructuredLogger("optimization", level="INFO")


@dataclass
class PerformanceMetric:
    """Performance metric data point"""
    metric_name: str
    value: float
    timestamp: datetime
    context: Dict[str, Any]


class PerformanceTuner:
    """Automatic performance tuning"""
    
    def __init__(self):
        self.metrics_history: List[PerformanceMetric] = []
        self.optimization_rules: Dict[str, Callable] = {}
        self.current_config: Dict[str, Any] = {
            "chunk_size": 10000,
            "batch_size": 1000,
            "num_workers": 4,
            "cache_enabled": True,
            "compression": "gzip"
        }
    
    def record_metric(self, name: str, value: float, context: Dict[str, Any] = None) -> None:
        """Record performance metric"""
        metric = PerformanceMetric(
            metric_name=name,
            value=value,
            timestamp=datetime.now(),
            context=context or {}
        )
        self.metrics_history.append(metric)
        
        logger.info(f"Metric recorded: {name}={value}")
    
    def register_optimization_rule(self, rule_name: str, rule_func: Callable) -> None:
        """Register optimization rule"""
        self.optimization_rules[rule_name] = rule_func
        logger.info(f"Optimization rule registered: {rule_name}")
    
    def analyze_performance(self, window_size: int = 100) -> Dict[str, Any]:
        """Analyze recent performance"""
        if len(self.metrics_history) < window_size:
            window = self.metrics_history
        else:
            window = self.metrics_history[-window_size:]
        
        analysis = {
            "sample_count": len(window),
            "metrics": {}
        }
        
        # Group by metric name
        metrics_by_name: Dict[str, List[float]] = {}
        for metric in window:
            if metric.metric_name not in metrics_by_name:
                metrics_by_name[metric.metric_name] = []
            metrics_by_name[metric.metric_name].append(metric.value)
        
        # Calculate statistics
        for name, values in metrics_by_name.items():
            analysis["metrics"][name] = {
                "avg": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "trend": "improving" if values[-1] < values[0] else "degrading"
            }
        
        return analysis
    
    def apply_optimizations(self) -> Dict[str, Any]:
        """Apply automatic optimizations"""
        analysis = self.analyze_performance()
        changes = {}
        
        for rule_name, rule_func in self.optimization_rules.items():
            try:
                result = rule_func(analysis, self.current_config)
                if result:
                    self.current_config.update(result)
                    changes[rule_name] = result
                    logger.info(f"Optimization applied: {rule_name}")
            except Exception as e:
                logger.error(f"Failed to apply optimization {rule_name}: {e}")
        
        return changes
    
    def get_recommendations(self) -> List[str]:
        """Get optimization recommendations"""
        recommendations = []
        analysis = self.analyze_performance()
        
        for name, stats in analysis.get("metrics", {}).items():
            if stats["trend"] == "degrading":
                recommendations.append(f"Performance degrading for {name}: {stats['max']:.2f} max")
            if stats["max"] > stats["avg"] * 1.5:
                recommendations.append(f"High variability in {name}")
        
        return recommendations


class CacheOptimizer:
    """Intelligent caching for optimization"""
    
    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, tuple] = {}  # (value, timestamp)
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Any:
        """Get cached value"""
        if key in self.cache:
            value, timestamp = self.cache[key]
            # Check if expired (1 hour TTL)
            if datetime.now() - timestamp < timedelta(hours=1):
                self.hits += 1
                return value
            else:
                del self.cache[key]
                self.misses += 1
                return None
        
        self.misses += 1
        return None
    
    def put(self, key: str, value: Any) -> None:
        """Cache value"""
        if len(self.cache) >= self.max_size:
            # Remove oldest entry
            oldest_key = min(self.cache.keys(),
                           key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]
        
        self.cache[key] = (value, datetime.now())
    
    def get_hit_rate(self) -> float:
        """Get cache hit rate"""
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.hits / total
    
    def clear(self) -> None:
        """Clear cache"""
        self.cache.clear()
        logger.info("Cache cleared")


class QueryOptimizer:
    """Optimize SQL queries"""
    
    def __init__(self):
        self.query_stats: Dict[str, Dict[str, Any]] = {}
    
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze query for optimization opportunities"""
        recommendations = []
        
        # Check for common issues
        query_upper = query.upper()
        
        if "SELECT *" in query_upper:
            recommendations.append("Use specific columns instead of SELECT *")
        
        if query_upper.count("JOIN") > 3:
            recommendations.append("Query has many JOINs, consider denormalization")
        
        if "LIKE '%" in query_upper:
            recommendations.append("Leading wildcard may be inefficient")
        
        if "NOT IN" in query_upper:
            recommendations.append("Consider NOT EXISTS instead of NOT IN")
        
        return {
            "query": query[:100] + "..." if len(query) > 100 else query,
            "issues_found": len(recommendations),
            "recommendations": recommendations
        }
    
    def track_query(self, query: str, execution_time: float) -> None:
        """Track query performance"""
        query_hash = hash(query)
        
        if query_hash not in self.query_stats:
            self.query_stats[query_hash] = {
                "count": 0,
                "total_time": 0,
                "min_time": float('inf'),
                "max_time": 0
            }
        
        stats = self.query_stats[query_hash]
        stats["count"] += 1
        stats["total_time"] += execution_time
        stats["min_time"] = min(stats["min_time"], execution_time)
        stats["max_time"] = max(stats["max_time"], execution_time)
    
    def get_slow_queries(self, threshold: float = 1.0) -> List[Dict[str, Any]]:
        """Get slow queries"""
        slow = []
        
        for query_hash, stats in self.query_stats.items():
            avg_time = stats["total_time"] / stats["count"]
            if avg_time > threshold:
                slow.append({
                    "query_hash": query_hash,
                    "avg_time": avg_time,
                    "execution_count": stats["count"]
                })
        
        return sorted(slow, key=lambda x: x["avg_time"], reverse=True)


class ResourcePlanner:
    """Plan resource allocation"""
    
    @staticmethod
    def calculate_optimal_chunk_size(
        total_rows: int,
        available_memory: int,
        row_size: int = 1000  # bytes per row
    ) -> int:
        """Calculate optimal chunk size"""
        # Use 80% of available memory, leave 20% for safety
        safe_memory = int(available_memory * 0.8)
        chunk_size = safe_memory // row_size
        
        # Min 1000, max 100000
        return max(1000, min(chunk_size, 100000))
    
    @staticmethod
    def calculate_optimal_workers(
        total_tasks: int,
        cpu_cores: int
    ) -> int:
        """Calculate optimal worker count"""
        # Use 80% of CPU cores
        max_workers = int(cpu_cores * 0.8)
        
        # At least 1, at most tasks
        return max(1, min(max_workers, total_tasks))
    
    @staticmethod
    def estimate_runtime(
        total_rows: int,
        throughput_rows_per_sec: float = 1000
    ) -> Dict[str, Any]:
        """Estimate pipeline runtime"""
        seconds = total_rows / throughput_rows_per_sec
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        return {
            "total_seconds": seconds,
            "hours": hours,
            "minutes": minutes,
            "seconds": secs,
            "formatted": f"{hours}h {minutes}m {secs}s"
        }


def create_optimization_rules() -> Dict[str, Callable]:
    """Create default optimization rules"""
    
    def rule_increase_workers(analysis: Dict, config: Dict) -> Dict:
        """Increase workers if CPU-bound"""
        cpu_utilization = analysis.get("metrics", {}).get("cpu_util", {}).get("avg", 0)
        if cpu_utilization < 50 and config["num_workers"] < 16:
            return {"num_workers": config["num_workers"] + 1}
        return {}
    
    def rule_increase_chunk_size(analysis: Dict, config: Dict) -> Dict:
        """Increase chunk size if memory available"""
        memory_util = analysis.get("metrics", {}).get("memory_util", {}).get("avg", 0)
        if memory_util < 70 and config["chunk_size"] < 100000:
            return {"chunk_size": int(config["chunk_size"] * 1.2)}
        return {}
    
    def rule_enable_compression(analysis: Dict, config: Dict) -> Dict:
        """Enable compression if I/O is bottleneck"""
        io_time = analysis.get("metrics", {}).get("io_time", {}).get("avg", 0)
        if io_time > 1.0 and not config["compression"]:
            return {"compression": "gzip"}
        return {}
    
    return {
        "increase_workers": rule_increase_workers,
        "increase_chunk_size": rule_increase_chunk_size,
        "enable_compression": rule_enable_compression
    }
