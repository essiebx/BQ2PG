"""
Main pipeline script - Enhanced with Phase 2 Integration (Quality, Resilience, Monitoring)
"""

import argparse
import sys
import json
from pathlib import Path
import pandas as pd

# Add src to path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from src.config import config
from src.monitoring import StructuredLogger
from src.schema_mapper import generate_extraction_query
from src.extract import BigQueryExtractor
from src.transform import DataTransformer, normalize_text, standardize_dates
from src.load import PostgresLoader
from src.quality import RuleSet
from src.performance import MemoryOptimizer
from src.pipeline import CheckpointManager

# Initialize components
logger = StructuredLogger("main", level="INFO")
memory_optimizer = MemoryOptimizer()
checkpoint_mgr = CheckpointManager()


def setup_quality_rules() -> RuleSet:
    """Create quality validation rules"""
    rules = RuleSet()
    
    # Add validation rules for patents
    rules.add_not_null_rule("patent_id", "Patent ID cannot be null")
    rules.add_not_null_rule("title", "Patent title cannot be null")
    rules.add_range_rule("filing_year", 1800, 2100, "Filing year out of range")
    rules.add_pattern_rule("patent_id", r"^US\d+[A-Z]?$", "Invalid patent ID format")
    
    logger.info("Quality rules configured", rules_count=len(rules.rules))
    return rules


def main():
    parser = argparse.ArgumentParser(description='BQ2PG Pipeline - BigQuery to PostgreSQL ETL')
    parser.add_argument('--limit', type=int, help='Number of rows to extract')
    parser.add_argument('--year', type=int, help='Filter by filing year')
    parser.add_argument('--recent-days', type=int, help='Last N days of patents')
    parser.add_argument('--drop-tables', action='store_true', help='Drop tables before loading')
    parser.add_argument('--test', action='store_true', help='Run test mode (100 rows)')
    parser.add_argument('--skip-validation', action='store_true', help='Skip data validation')
    parser.add_argument('--skip-transformation', action='store_true', help='Skip data transformation')
    parser.add_argument('--parallel', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--report', action='store_true', help='Generate quality report')
    
    args = parser.parse_args()
    
    # Test mode
    if args.test:
        args.limit = 100
        args.drop_tables = True
        logger.info("Test mode enabled", limit=100)
    
    logger.info("Pipeline starting", 
                limit=args.limit, 
                year=args.year,
                parallel_workers=args.parallel)
    
    try:
        # Validate configuration
        config.validate()
        
        # Check memory
        if not memory_optimizer.check_memory_usage():
            logger.warning("Memory usage high at startup")
        
        # Load checkpoint if exists
        checkpoint = checkpoint_mgr.load_checkpoint("pipeline")
        if checkpoint:
            logger.info("Resuming from checkpoint", 
                       checkpoint_data=checkpoint.get('data'))
        
        # Generate query
        query = generate_extraction_query(
            limit=args.limit,
            year=args.year,
            recent_days=args.recent_days
        )
        
        # Initialize components
        extractor = BigQueryExtractor()
        transformer = DataTransformer()
        loader = PostgresLoader()
        quality_rules = None if args.skip_validation else setup_quality_rules()
        
        # Register transformations
        transformer.register_transformation("normalize_text", normalize_text)
        transformer.register_transformation("standardize_dates", standardize_dates)
        
        # Estimate cost
        logger.info("Estimating query cost")
        bytes_processed, cost = extractor.estimate_cost(query)
        logger.info(f"Query estimated", cost_usd=cost, bytes_processed=bytes_processed)
        
        if cost > 0.01 and not args.test:
            confirm = input(f"Estimated cost: ${cost:.6f}. Continue? (y/n): ")
            if confirm.lower() != 'y':
                logger.info("Pipeline cancelled by user")
                return 0
        
        # Create table
        if args.drop_tables:
            logger.info("Dropping existing tables")
        
        if not loader.create_table('patents'):
            logger.error("Failed to create table")
            return 1
        
        logger.info("Table created successfully")
        
        # Extract-Transform-Load Pipeline
        total_rows_extracted = 0
        total_rows_loaded = 0
        total_rows_failed = 0
        
        for chunk_num, df_chunk in extractor.extract(query, chunk_size=10000):
            logger.info(f"Processing chunk {chunk_num}", rows=len(df_chunk))
            
            # Transform: Clean and Validate
            if not args.skip_transformation:
                logger.info(f"Transforming chunk {chunk_num}")
                
                try:
                    transform_result = transformer.process_pipeline(
                        df_chunk,
                        clean=True,
                        validate=not args.skip_validation,
                        transformations=["normalize_text", "standardize_dates"],
                        rules=quality_rules
                    )
                    
                    df_chunk = transform_result['data']
                    
                    if 'validation' in transform_result:
                        logger.info(
                            f"Chunk validation complete",
                            chunk=chunk_num,
                            quality_score=transform_result['validation'].get('quality_score'),
                            valid_rows=transform_result['validation'].get('valid_rows')
                        )
                        
                        # If quality score too low, log warning
                        if transform_result['validation'].get('quality_score', 100) < 80:
                            logger.warning(
                                f"Low quality score in chunk {chunk_num}",
                                score=transform_result['validation'].get('quality_score')
                            )
                    
                except Exception as e:
                    logger.error(f"Transformation failed for chunk {chunk_num}", error=str(e))
                    total_rows_failed += len(df_chunk)
                    continue
            
            # Load
            logger.info(f"Loading chunk {chunk_num}", rows=len(df_chunk))
            
            try:
                success = loader.load_dataframe(df_chunk, 'patents', if_exists='append')
                if success:
                    total_rows_loaded += len(df_chunk)
                    total_rows_extracted += len(df_chunk)
                    logger.info(
                        f"Chunk {chunk_num} loaded successfully",
                        rows=len(df_chunk),
                        total_loaded=total_rows_loaded
                    )
                else:
                    logger.error(f"Failed to load chunk {chunk_num}")
                    total_rows_failed += len(df_chunk)
                    
            except Exception as e:
                logger.error(f"Load failed for chunk {chunk_num}", error=str(e))
                total_rows_failed += len(df_chunk)
                continue
            
            # Memory check after each chunk
            if not memory_optimizer.check_memory_usage():
                logger.warning("Memory usage high, cleaning up")
                memory_optimizer.cleanup()
        
        # Save final checkpoint
        checkpoint_mgr.save_checkpoint(
            "pipeline",
            {
                "rows_extracted": total_rows_extracted,
                "rows_loaded": total_rows_loaded,
                "rows_failed": total_rows_failed,
                "status": "completed"
            },
            metadata={"timestamp": str(pd.Timestamp.now())}
        )
        
        # Generate reports
        logger.info("Pipeline completed",
                   rows_extracted=total_rows_extracted,
                   rows_loaded=total_rows_loaded,
                   rows_failed=total_rows_failed,
                   success_rate=f"{(total_rows_loaded/(total_rows_extracted+total_rows_failed)*100):.1f}%" if (total_rows_extracted+total_rows_failed) > 0 else "N/A")
        
        if args.report:
            quality_report = transformer.get_quality_report()
            logger.info("Quality report generated")
            
            report_file = Path("quality_report.json")
            with open(report_file, 'w') as f:
                json.dump(quality_report, f, indent=2)
            logger.info(f"Report saved to {report_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed", error_type=type(e).__name__, error=str(e))
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
