# src/extract.py
"""
BigQuery data extraction for patents with resilience and monitoring.
"""

import pandas as pd
from google.cloud import bigquery
import logging

from .config import config
from .utils import logger, timer
from .schema_mapper import generate_extraction_query
from .resilience import RetryPolicy, CircuitBreaker, DeadLetterQueue
from .monitoring import StructuredLogger, MetricsCollector, Tracer
from .pipeline import CheckpointManager

# Initialize resilience and monitoring
retry_policy = RetryPolicy(max_retries=3, initial_delay=2)
circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
dlq = DeadLetterQueue(dlq_dir="dlq")
structured_logger = StructuredLogger("extract", level="INFO")
metrics = MetricsCollector(namespace="bq2pg")
tracer = Tracer(service_name="bq2pg_extractor")
checkpoint_mgr = CheckpointManager()


class BigQueryExtractor:
    """Extract data from BigQuery with resilience patterns"""
    
    def __init__(self):
        self.client = None
        self.extraction_count = 0
        self.failed_extractions = 0
    
    def connect(self):
        """Connect to BigQuery with circuit breaker"""
        @circuit_breaker
        def _connect():
            try:
                self.client = bigquery.Client.from_service_account_json(
                    config.GOOGLE_APPLICATION_CREDENTIALS,
                    project=config.GOOGLE_CLOUD_PROJECT
                )
                structured_logger.info(
                    "Connected to BigQuery",
                    project_id=config.GOOGLE_CLOUD_PROJECT,
                    component="extract"
                )
                metrics.set_custom_metric("bigquery_connection_status", 1)
                return True
            except Exception as e:
                structured_logger.error(
                    f"BigQuery connection failed: {e}",
                    error_type=type(e).__name__,
                    component="extract"
                )
                metrics.set_custom_metric("bigquery_connection_status", 0)
                dlq.enqueue(
                    {"operation": "connect"},
                    str(e),
                    source="extract_connect",
                    retry_count=0
                )
                raise
        
        try:
            return retry_policy.retry(_connect)
        except Exception as e:
            logger.error(f"[ERROR] BigQuery connection failed after retries: {e}")
            return False
    
    @timer
    def extract(self, query: str, chunk_size: int = None):
        """
        Extract data from BigQuery with resilience and monitoring.
        
        Args:
            query: SQL query
            chunk_size: Rows per chunk (None for all at once)
        """
        if not self.client:
            self.connect()
        
        chunk_size = chunk_size or config.DEFAULT_CHUNK_SIZE
        
        with tracer.trace_span("extract_bigquery", {"query_length": len(query), "chunk_size": chunk_size}):
            structured_logger.info(f"Starting extraction (chunk_size: {chunk_size:,})")
            
            try:
                # Use pandas-gbq with retry
                @retry_policy
                def _extract_with_retry():
                    return pd.read_gbq(
                        query,
                        project_id=config.GOOGLE_CLOUD_PROJECT,
                        credentials=self.client._credentials,
                        chunksize=chunk_size,
                        progress_bar_type='tqdm'
                    )
                
                extraction_iterator = _extract_with_retry()
                
                for chunk_num, df_chunk in enumerate(extraction_iterator):
                    structured_logger.info(
                        f"Extracted chunk {chunk_num}",
                        chunk_size=len(df_chunk),
                        total_chunks=chunk_num + 1
                    )
                    
                    self.extraction_count += len(df_chunk)
                    metrics.record_extraction(len(df_chunk), 0)
                    
                    # Save checkpoint every 100k rows
                    if self.extraction_count % 100000 == 0:
                        checkpoint_mgr.save_checkpoint(
                            "extraction",
                            {"rows_extracted": self.extraction_count, "chunk": chunk_num},
                            metadata={"timestamp": pd.Timestamp.now().isoformat()}
                        )
                    
                    yield chunk_num, df_chunk
                    
            except Exception as e:
                self.failed_extractions += 1
                structured_logger.error(
                    f"Extraction failed: {e}",
                    error_type=type(e).__name__,
                    rows_attempted=self.extraction_count
                )
                metrics.increment_custom_metric("extraction_failures")
                dlq.enqueue(
                    {"query_snippet": query[:200]},
                    str(e),
                    source="extract",
                    retry_count=0
                )
                raise
    
    def extract_dataframe(self, query: str):
        """
        Extract as single DataFrame with monitoring and resilience.
        
        Args:
            query: SQL query to extract data
            
        Returns:
            DataFrame with extracted data
        """
        if not self.client:
            self.connect()
        
        with tracer.trace_span("extract_dataframe", {"query_length": len(query)}):
            structured_logger.info("Starting single DataFrame extraction")
            
            try:
                # Extract with retry
                @retry_policy
                @circuit_breaker
                def _extract():
                    return pd.read_gbq(
                        query,
                        project_id=config.GOOGLE_CLOUD_PROJECT,
                        credentials=self.client._credentials
                    )
                
                df = _extract()
                
                self.extraction_count += len(df)
                structured_logger.info(
                    f"DataFrame extraction complete",
                    rows=len(df),
                    columns=len(df.columns)
                )
                metrics.record_extraction(len(df), 0)
                
                # Save checkpoint
                checkpoint_mgr.save_checkpoint(
                    "extraction_dataframe",
                    {"rows_extracted": len(df), "columns": len(df.columns)},
                    metadata={"timestamp": pd.Timestamp.now().isoformat()}
                )
                
                return df
                
            except Exception as e:
                self.failed_extractions += 1
                structured_logger.error(
                    f"DataFrame extraction failed: {e}",
                    error_type=type(e).__name__,
                    query_length=len(query)
                )
                metrics.increment_custom_metric("extraction_failures")
                dlq.enqueue(
                    {"operation": "extract_dataframe", "query_snippet": query[:200]},
                    str(e),
                    source="extract_dataframe",
                    retry_count=0
                )
                raise
    
    def estimate_cost(self, query: str):
        """
        Estimate query cost with monitoring.
        
        Args:
            query: SQL query to estimate
            
        Returns:
            Tuple of (bytes_processed, estimated_cost_usd)
        """
        if not self.client:
            self.connect()
        
        with tracer.trace_span("estimate_cost", {"query_length": len(query)}):
            try:
                structured_logger.info("Estimating query cost")
                
                @retry_policy
                def _estimate():
                    job_config = bigquery.QueryJobConfig(dry_run=True)
                    query_job = self.client.query(query, job_config=job_config)
                    return query_job.total_bytes_processed
                
                bytes_processed = _estimate()
                cost_usd = (bytes_processed / (1024**4)) * 5  # $5 per TB
                
                structured_logger.info(
                    f"Query cost estimated",
                    bytes_processed=bytes_processed,
                    cost_usd=cost_usd
                )
                
                metrics.set_custom_metric("estimated_bytes_processed", bytes_processed)
                metrics.set_custom_metric("estimated_cost_usd", cost_usd)
                
                return bytes_processed, cost_usd
                
            except Exception as e:
                structured_logger.error(
                    f"Cost estimation failed: {e}",
                    error_type=type(e).__name__
                )
                metrics.increment_custom_metric("cost_estimation_failures")
                dlq.enqueue(
                    {"operation": "estimate_cost"},
                    str(e),
                    source="estimate_cost",
                    retry_count=0
                )
                raise