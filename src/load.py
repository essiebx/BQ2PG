# src/load.py
"""
Load data into PostgreSQL with resilience and optimization.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from io import StringIO
import logging

from .config import config
from .utils import logger, timer
from .schema_mapper import generate_create_table_sql
from .resilience import RetryPolicy, CircuitBreaker, DeadLetterQueue
from .monitoring import StructuredLogger, MetricsCollector, Tracer
from .performance import ConnectionPool, MemoryOptimizer
from .pipeline import CheckpointManager

# Initialize resilience and monitoring
retry_policy = RetryPolicy(max_retries=3, initial_delay=2)
circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
dlq = DeadLetterQueue(dlq_dir="dlq")
structured_logger = StructuredLogger("load", level="INFO")
metrics = MetricsCollector(namespace="bq2pg")
tracer = Tracer(service_name="bq2pg_loader")
memory_optimizer = MemoryOptimizer()
checkpoint_mgr = CheckpointManager()


class PostgresLoader:
    """Load data into PostgreSQL with resilience patterns"""
    
    def __init__(self):
        self.engine = None
        self.connection_pool = None
        self.load_count = 0
        self.failed_rows = 0
        self.connect()
    
    def connect(self):
        """Connect to PostgreSQL with circuit breaker and retry"""
        @circuit_breaker
        def _connect():
            try:
                self.engine = create_engine(config.postgres_connection_string)
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                structured_logger.info(
                    "Connected to PostgreSQL",
                    component="load"
                )
                metrics.set_custom_metric("postgres_connection_status", 1)
                return True
            except Exception as e:
                structured_logger.error(
                    f"PostgreSQL connection failed: {e}",
                    error_type=type(e).__name__,
                    component="load"
                )
                metrics.set_custom_metric("postgres_connection_status", 0)
                raise
        
        try:
            return retry_policy.retry(_connect)
        except Exception as e:
            logger.error(f"[ERROR] PostgreSQL connection failed after retries: {e}")
            return False
    
    def create_table(self, table_name: str = 'patents'):
        """Create patents table with retry"""
        @retry_policy
        def _create():
            try:
                with self.engine.begin() as conn:
                    sql = generate_create_table_sql(table_name)
                    conn.execute(text(sql))
                structured_logger.info(f"Created table: {table_name}")
                return True
            except Exception as e:
                structured_logger.error(f"Failed to create table: {e}")
                dlq.enqueue(
                    {"table": table_name},
                    str(e),
                    source="load_create_table",
                    retry_count=0
                )
                raise
        
        try:
            return _create()
        except Exception as e:
            logger.error(f"[ERROR] Failed to create table: {e}")
            return False
    
    @timer
    def load_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """
        Load DataFrame to PostgreSQL with memory optimization.
        
        Args:
            df: DataFrame to load
            table_name: Target table
            if_exists: 'fail', 'replace', or 'append'
        """
        with tracer.trace_span("load_dataframe", {"table": table_name, "rows": len(df)}):
            try:
                # Check memory
                if not memory_optimizer.check_memory_usage():
                    structured_logger.warning(
                        "Memory usage high, optimizing...",
                        memory_pct=memory_optimizer.get_memory_stats().percent
                    )
                    memory_optimizer.cleanup()
                
                # Prepare data
                df = df.copy()
                df = df.replace({np.nan: None})
                
                # Convert arrays to PostgreSQL format
                array_cols = [col for col in df.columns if col.endswith('_names') or col.endswith('_codes')]
                for col in array_cols:
                    if col in df.columns:
                        df[col] = df[col].apply(
                            lambda x: '{}' if x is None or (isinstance(x, list) and len(x) == 0) else str(x)
                        )
                
                # Load with retry
                @retry_policy
                @circuit_breaker
                def _load():
                    df.to_sql(
                        table_name,
                        self.engine,
                        if_exists=if_exists,
                        index=False,
                        chunksize=10000,
                        method='multi'
                    )
                
                _load()
                
                self.load_count += len(df)
                structured_logger.info(
                    f"Loaded {len(df):,} rows",
                    table=table_name,
                    total_loaded=self.load_count
                )
                metrics.record_load(len(df), 0)
                
                # Save checkpoint
                if self.load_count % 100000 == 0:
                    checkpoint_mgr.save_checkpoint(
                        "loading",
                        {"rows_loaded": self.load_count, "table": table_name},
                        metadata={"timestamp": pd.Timestamp.now().isoformat()}
                    )
                
                return True
                
            except Exception as e:
                self.failed_rows += len(df)
                structured_logger.error(
                    f"Load failed: {e}",
                    error_type=type(e).__name__,
                    table=table_name,
                    rows_failed=len(df)
                )
                metrics.record_load(0, len(df))
                metrics.increment_custom_metric("load_failures")
                
                # Send failed records to DLQ
                dlq.enqueue(
                    {"rows": len(df), "table": table_name},
                    str(e),
                    source="load",
                    retry_count=0
                )
                
                return False
    
    def load_in_chunks(self, data_generator, table_name: str):
        """Load data from generator in chunks with monitoring and resilience"""
        with tracer.trace_span("load_in_chunks", {"table": table_name}):
            total_rows = 0
            failed_chunks = 0
            
            try:
                for chunk_num, df_chunk in data_generator:
                    with tracer.trace_span("load_chunk", {"chunk": chunk_num, "rows": len(df_chunk)}):
                        structured_logger.info(
                            f"Loading chunk",
                            chunk_num=chunk_num,
                            rows=len(df_chunk),
                            table=table_name
                        )
                        
                        # Check memory before loading chunk
                        if not memory_optimizer.check_memory_usage():
                            structured_logger.warning(
                                "Memory high, optimizing...",
                                memory_pct=memory_optimizer.get_memory_stats().percent
                            )
                            memory_optimizer.cleanup()
                        
                        success = self.load_dataframe(df_chunk, table_name, if_exists='append')
                        if not success:
                            structured_logger.error(
                                f"Chunk {chunk_num} load failed",
                                chunk_num=chunk_num,
                                rows=len(df_chunk)
                            )
                            failed_chunks += 1
                            metrics.increment_custom_metric("chunk_failures")
                        else:
                            total_rows += len(df_chunk)
                            metrics.set_custom_metric("chunks_processed", chunk_num + 1)
                
                structured_logger.info(
                    f"Chunk loading complete",
                    total_rows=total_rows,
                    failed_chunks=failed_chunks,
                    table=table_name
                )
                
                checkpoint_mgr.save_checkpoint(
                    "loading_chunks",
                    {"rows_loaded": total_rows, "chunks_failed": failed_chunks, "table": table_name},
                    metadata={"timestamp": pd.Timestamp.now().isoformat()}
                )
                
                return total_rows
                
            except Exception as e:
                structured_logger.error(
                    f"Chunk load failed: {e}",
                    error_type=type(e).__name__,
                    table=table_name
                )
                metrics.increment_custom_metric("chunked_load_failures")
                
                dlq.enqueue(
                    {"table": table_name, "operation": "load_in_chunks"},
                    str(e),
                    source="load",
                    retry_count=0
                )
                
                raise