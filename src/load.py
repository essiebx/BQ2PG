# src/load.py
"""
Load data into PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from io import StringIO

from .config import config
from .utils import logger, timer
from .schema_mapper import generate_create_table_sql

class PostgresLoader:
    """Load data into PostgreSQL"""
    
    def __init__(self):
        self.engine = None
        self.connect()
    
    def connect(self):
        """Connect to PostgreSQL"""
        try:
            self.engine = create_engine(config.postgres_connection_string)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            return False
    
    def create_table(self, table_name: str = 'patents'):
        """Create patents table"""
        try:
            with self.engine.begin() as conn:
                sql = generate_create_table_sql(table_name)
                conn.execute(text(sql))
            logger.info(f"✅ Created table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create table: {e}")
            return False
    
    @timer
    def load_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """
        Load DataFrame to PostgreSQL.
        
        Args:
            df: DataFrame to load
            table_name: Target table
            if_exists: 'fail', 'replace', or 'append'
        """
        try:
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
            
            # Load
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=10000,
                method='multi'
            )
            
            logger.info(f"✅ Loaded {len(df):,} rows to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Load failed: {e}")
            return False
    
    def load_in_chunks(self, data_generator, table_name: str):
        """Load data from generator in chunks"""
        total_rows = 0
        
        for chunk_num, df_chunk in data_generator:
            logger.info(f"📦 Loading chunk {chunk_num} ({len(df_chunk):,} rows)...")
            
            success = self.load_dataframe(df_chunk, table_name, if_exists='append')
            if not success:
                logger.error(f"❌ Failed to load chunk {chunk_num}")
                break
            
            total_rows += len(df_chunk)
            logger.info(f"📊 Total loaded: {total_rows:,} rows")
        
        logger.info(f"🎉 Finished loading. Total: {total_rows:,} rows")
        return total_rows