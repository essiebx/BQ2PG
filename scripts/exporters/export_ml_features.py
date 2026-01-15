#!/usr/bin/env python3
"""Export ML-ready feature set with text lengths, inventor counts, etc."""

import os
import csv
import logging
from datetime import datetime
import psycopg2

def export_ml_features():
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
    output_file = f"data/patents_ml_features_{timestamp}.csv"
    
    os.makedirs("data", exist_ok=True)
    
    logger.info("Exporting ML features to %s", output_file)
    
    with psycopg2.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    publication_number,
                    country_code,
                    filing_date,
                    publication_date,
                    grant_date,
                    EXTRACT(YEAR FROM filing_date) as filing_year,
                    EXTRACT(YEAR FROM publication_date) as publication_year,
                    LENGTH(title) as title_length,
                    LENGTH(abstract) as abstract_length,
                    CASE WHEN assignee IS NOT NULL THEN 1 ELSE 0 END as has_assignee,
                    jsonb_array_length(inventors) as inventor_count,
                    jsonb_array_length(cpc_codes) as cpc_count,
                    (publication_date - filing_date) as days_to_publication,
                    CASE WHEN grant_date IS NOT NULL 
                         THEN (grant_date - filing_date) 
                         ELSE NULL END as days_to_grant
                FROM public.patents_enhanced
                WHERE filing_date IS NOT NULL 
                  AND publication_date IS NOT NULL
                ORDER BY filing_date DESC
            """)
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([desc[0] for desc in cur.description])
                count = 0
                for row in cur:
                    writer.writerow(row)
                    count += 1
                    if count % 50000 == 0:
                        logger.info("Exported %d rows...", count)
    
    logger.info("✅ ML features export complete: %s (%d rows)", output_file, count)
    return output_file

if __name__ == "__main__":
    export_ml_features()