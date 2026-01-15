# src/extract.py
"""
BigQuery data extraction for patents.
"""

import pandas as pd
from google.cloud import bigquery

from .config import config
from .utils import logger, timer
from .schema_mapper import generate_extraction_query

class BigQueryExtractor:
    """Extract data from BigQuery"""
    
    def __init__(self):
        self.client = None
    
    def connect(self):
        """Connect to BigQuery"""
        try:
            self.client = bigquery.Client.from_service_account_json(
                config.GOOGLE_APPLICATION_CREDENTIALS,
                project=config.GOOGLE_CLOUD_PROJECT
            )
            logger.info(f"✅ Connected to BigQuery project: {config.GOOGLE_CLOUD_PROJECT}")
            return True
        except Exception as e:
            logger.error(f"❌ BigQuery connection failed: {e}")
            return False
    
    @timer
    def extract(self, query: str, chunk_size: int = None):
        """
        Extract data from BigQuery.
        
        Args:
            query: SQL query
            chunk_size: Rows per chunk (None for all at once)
        """
        if not self.client:
            self.connect()
        
        chunk_size = chunk_size or config.DEFAULT_CHUNK_SIZE
        
        logger.info(f"📥 Extracting data (chunk_size: {chunk_size:,})...")
        
        try:
            # Use pandas-gbq for chunked loading
            for chunk_num, df_chunk in enumerate(
                pd.read_gbq(
                    query,
                    project_id=config.GOOGLE_CLOUD_PROJECT,
                    credentials=self.client._credentials,
                    chunksize=chunk_size,
                    progress_bar_type='tqdm'
                )
            ):
                logger.info(f"📦 Chunk {chunk_num}: {len(df_chunk):,} rows")
                yield chunk_num, df_chunk
                
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            raise
    
    def extract_dataframe(self, query: str):
        """Extract as single DataFrame (for small queries)"""
        if not self.client:
            self.connect()
        
        logger.info("📥 Extracting data...")
        df = pd.read_gbq(
            query,
            project_id=config.GOOGLE_CLOUD_PROJECT,
            credentials=self.client._credentials
        )
        
        logger.info(f"✅ Extracted {len(df):,} rows")
        return df
    
    def estimate_cost(self, query: str):
        """Estimate query cost"""
        if not self.client:
            self.connect()
        
        job_config = bigquery.QueryJobConfig(dry_run=True)
        query_job = self.client.query(query, job_config=job_config)
        
        bytes_processed = query_job.total_bytes_processed
        cost_usd = (bytes_processed / (1024**4)) * 5  # $5 per TB
        
        logger.info(f"💰 Estimated cost: ${cost_usd:.6f} ({bytes_processed:,} bytes)")
        return bytes_processed, cost_usd