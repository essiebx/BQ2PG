#!/usr/bin/env python3
"""Export patents_enhanced to CSV for ML workflows"""

import os
import csv
import logging
from datetime import datetime
import psycopg2

def export_to_csv():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_name = os.getenv("DB_NAME", "patents_db")
    db_user = os.getenv("DB_USER", "pipeline_user")
    db_pass = os.getenv("DB_PASS")
    
    conn_kwargs = {k: v for k, v in {
        "host": db_host,
        "dbname": db_name,
        "user": db_user,
        "password": db_pass
    }.items() if v is not None}
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/patents_enhanced_ml_{timestamp}.csv"
    
    os.makedirs("data", exist_ok=True)
    
    logger.info("Exporting patents_enhanced to %s", output_file)
    
    with psycopg2.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            # Get total count
            cur.execute("SELECT COUNT(*) FROM public.patents_enhanced")
            total = cur.fetchone()[0]
            logger.info("Exporting %d rows...", total)
            
            # Export query
            cur.execute("""
                SELECT 
                    publication_number,
                    application_number,
                    country_code,
                    kind_code,
                    filing_date,
                    publication_date,
                    grant_date,
                    title,
                    abstract,
                    assignee,
                    inventors::text as inventors,
                    cpc_codes::text as cpc_codes,
                    loaded_at
                FROM public.patents_enhanced
                ORDER BY filing_date DESC
            """)
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Write header
                writer.writerow([desc[0] for desc in cur.description])
                # Write rows
                count = 0
                for row in cur:
                    writer.writerow(row)
                    count += 1
                    if count % 50000 == 0:
                        logger.info("Exported %d rows...", count)
    
    logger.info("[OK] Export complete: %s (%d rows)", output_file, count)
    return output_file

if __name__ == "__main__":
    export_to_csv()