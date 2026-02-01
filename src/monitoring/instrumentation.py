# src/monitoring/instrumentation.py
"""
Comprehensive instrumentation for all pipeline components.
Decorators and context managers for automatic monitoring.
"""

import functools
import time
import traceback
from typing import Callable, Any, Dict
from contextlib import contextmanager

from .structured_logger import StructuredLogger
from .metrics import MetricsCollector
from .tracer import Tracer

logger = StructuredLogger("instrumentation", level="INFO")
metrics = MetricsCollector(namespace="bq2pg")
tracer = Tracer(service_name="bq2pg_instrumentation")


def instrument_function(
    operation_name: str,
    component: str,
    track_memory: bool = False,
    track_errors: bool = True
):
    """
    Decorator to automatically instrument functions with logging, metrics, and tracing.
    
    Args:
        operation_name: Name of the operation for logging
        component: Component name for metrics
        track_memory: Whether to track memory usage
        track_errors: Whether to track errors
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            
            with tracer.trace_span(operation_name, {
                "function": func.__name__,
                "component": component,
                "args_count": len(args),
                "kwargs_count": len(kwargs)
            }):
                try:
                    logger.info(
                        f"Operation started: {operation_name}",
                        function=func.__name__,
                        component=component
                    )
                    
                    result = func(*args, **kwargs)
                    
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Operation completed: {operation_name}",
                        function=func.__name__,
                        duration_seconds=elapsed,
                        component=component
                    )
                    
                    metrics.set_custom_metric(
                        f"{component}_{operation_name}_duration_seconds",
                        elapsed
                    )
                    metrics.increment_custom_metric(f"{component}_{operation_name}_success")
                    
                    return result
                    
                except Exception as e:
                    elapsed = time.time() - start_time
                    
                    error_info = {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "duration_seconds": elapsed,
                        "component": component,
                        "function": func.__name__,
                        "traceback": traceback.format_exc()
                    }
                    
                    logger.error(
                        f"Operation failed: {operation_name}",
                        **error_info
                    )
                    
                    if track_errors:
                        metrics.increment_custom_metric(f"{component}_{operation_name}_failures")
                    
                    raise
        
        return wrapper
    return decorator


def instrument_class(component: str):
    """
    Class decorator to instrument all public methods of a class.
    
    Args:
        component: Component name for metrics
    """
    def decorator(cls):
        for attr_name in dir(cls):
            # Skip private and special methods
            if attr_name.startswith('_'):
                continue
            
            attr = getattr(cls, attr_name)
            if callable(attr) and not isinstance(attr, type):
                # Wrap the method
                wrapped = instrument_function(
                    operation_name=attr_name,
                    component=component
                )(attr)
                setattr(cls, attr_name, wrapped)
        
        return cls
    
    return decorator


@contextmanager
def monitor_operation(
    operation_name: str,
    component: str,
    metadata: Dict[str, Any] = None
):
    """
    Context manager for monitoring a block of code.
    
    Args:
        operation_name: Name of the operation
        component: Component name
        metadata: Additional metadata to log
    """
    start_time = time.time()
    metadata = metadata or {}
    
    with tracer.trace_span(operation_name, metadata):
        try:
            logger.info(
                f"Operation started: {operation_name}",
                component=component,
                **metadata
            )
            
            yield
            
            elapsed = time.time() - start_time
            logger.info(
                f"Operation completed: {operation_name}",
                component=component,
                duration_seconds=elapsed
            )
            
            metrics.set_custom_metric(
                f"{component}_{operation_name}_duration_seconds",
                elapsed
            )
            metrics.increment_custom_metric(f"{component}_{operation_name}_success")
            
        except Exception as e:
            elapsed = time.time() - start_time
            
            logger.error(
                f"Operation failed: {operation_name}",
                component=component,
                error_type=type(e).__name__,
                error_message=str(e),
                duration_seconds=elapsed
            )
            
            metrics.increment_custom_metric(f"{component}_{operation_name}_failures")
            raise


@contextmanager
def monitor_data_flow(
    stage: str,
    rows_in: int,
    component: str
):
    """
    Context manager for monitoring data flow through pipeline.
    
    Args:
        stage: Pipeline stage name
        rows_in: Input row count
        component: Component name
    """
    start_time = time.time()
    
    with tracer.trace_span(f"data_flow_{stage}", {"rows_in": rows_in}):
        logger.info(
            f"Data flow started: {stage}",
            component=component,
            rows_in=rows_in
        )
        
        try:
            yield
            
            elapsed = time.time() - start_time
            throughput = rows_in / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"Data flow completed: {stage}",
                component=component,
                rows_in=rows_in,
                duration_seconds=elapsed,
                throughput_rows_per_second=throughput
            )
            
            metrics.set_custom_metric(f"data_flow_{stage}_throughput", throughput)
            
        except Exception as e:
            logger.error(
                f"Data flow failed: {stage}",
                component=component,
                rows_in=rows_in,
                error_type=type(e).__name__
            )
            metrics.increment_custom_metric(f"data_flow_{stage}_failures")
            raise


def log_operation_metrics(
    operation_name: str,
    component: str,
    **metrics_data
):
    """
    Log custom metrics for an operation.
    
    Args:
        operation_name: Operation name
        component: Component name
        **metrics_data: Custom metrics to log
    """
    logger.info(
        f"Operation metrics: {operation_name}",
        component=component,
        **metrics_data
    )
    
    for key, value in metrics_data.items():
        if isinstance(value, (int, float)):
            metrics.set_custom_metric(f"{component}_{key}", value)


class PerformanceTimer:
    """Context manager for measuring performance"""
    
    def __init__(self, operation_name: str, component: str = "unknown"):
        self.operation_name = operation_name
        self.component = component
        self.start_time = None
        self.elapsed = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.time() - self.start_time
        
        if exc_type:
            logger.error(
                f"Performance timer failed: {self.operation_name}",
                component=self.component,
                duration_seconds=self.elapsed,
                error_type=exc_type.__name__
            )
        else:
            logger.info(
                f"Performance measurement: {self.operation_name}",
                component=self.component,
                duration_seconds=self.elapsed
            )
            
            metrics.set_custom_metric(
                f"{self.component}_{self.operation_name}_seconds",
                self.elapsed
            )
