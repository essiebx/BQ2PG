"""
Main pipeline script
"""

import argparse
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from src.config import config
from src.utils import logger
from src.schema_mapper import generate_extraction_query
from src.extract import BigQueryExtractor
from src.load import PostgresLoader

def main():
    parser = argparse.ArgumentParser(description='BigQuery to PostgreSQL Patents Pipeline')
    parser.add_argument('--limit', type=int, help='Number of rows to extract')
    parser.add_argument('--year', type=int, help='Filter by filing year')
    parser.add_argument('--recent-days', type=int, help='Last N days of patents')
    parser.add_argument('--drop-tables', action='store_true', help='Drop tables before loading')
    parser.add_argument('--test', action='store_true', help='Run test mode (100 rows)')
    
    args = parser.parse_args()
    
    # Test mode
    if args.test:
        args.limit = 100
        args.drop_tables = True
        logger.info("🧪 Running in test mode (100 rows)")
    
    logger.info("🚀 Starting Patents Pipeline")
    logger.info(f"📊 Parameters: limit={args.limit}, year={args.year}, recent_days={args.recent_days}")
    
    try:
        # Validate configuration
        config.validate()
        
        # Generate query
        query = generate_extraction_query(
            limit=args.limit,
            year=args.year,
            recent_days=args.recent_days
        )
        
        # Initialize components
        extractor = BigQueryExtractor()
        loader = PostgresLoader()
        
        # Estimate cost
        bytes_processed, cost = extractor.estimate_cost(query)
        logger.info(f"💰 Estimated cost: ${cost:.6f}")
        
        if cost > 0.01 and not args.test:
            confirm = input(f"Estimated cost: ${cost:.6f}. Continue? (y/n): ")
            if confirm.lower() != 'y':
                logger.info("⏹️  Pipeline cancelled")
                return 0
        
        # Create table
        if args.drop_tables:
            logger.info("🗑️  Dropping existing tables...")
        
        if not loader.create_table('patents'):
            logger.error("❌ Failed to create table")
            return 1
        
        # Extract and load
        data_generator = extractor.extract(query)
        total_rows = loader.load_in_chunks(data_generator, 'patents')
        
        logger.info(f"🎉 Pipeline completed successfully!")
        logger.info(f"📈 Total rows loaded: {total_rows:,}")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())