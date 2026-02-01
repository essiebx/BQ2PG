#!/usr/bin/env python3
"""
Scaled Pipeline - Loads 100K+ patents with proper schema
Based on your working simple_pipeline.py
"""

import os
import json
import logging
from pathlib import Path
from google.cloud import bigquery
import psycopg2
from psycopg2.extras import execute_batch, Json
from google.api_core import retry as gcp_retry
import time
from datetime import datetime

def _require_env(name):
    val = os.getenv(name)
    if not val:
        logging.error("Missing required environment variable: %s", name)
        raise SystemExit(1)
    return val

def _get_sql_file(filename):
    """Get absolute path to SQL file"""
    base_dir = Path(__file__).parent
    sql_path = base_dir / "sql" / "migrations" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text()

def scaled_pipeline():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    logger.info("SCALED PATENT PIPELINE")

    sa_path = _require_env("GOOGLE_APPLICATION_CREDENTIALS")
    with open(sa_path) as f:
        gcp_key = json.load(f)
    project_id = gcp_key.get("project_id")

    bq_client = bigquery.Client.from_service_account_json(sa_path, project=project_id)

    # allow scaling via env SCALED_LIMIT (default 1000000)
    limit = int(os.getenv("SCALED_LIMIT", "1000000"))

    query = rf"""
    WITH patent_data AS (
      SELECT
        publication_number,
        application_number,
        country_code,
        kind_code,
        CASE WHEN SAFE_CAST(filing_date AS INT64) IS NOT NULL AND LENGTH(CAST(filing_date AS STRING)) = 8
             THEN PARSE_DATE('%Y%m%d', CAST(filing_date AS STRING))
             ELSE NULL END AS filing_date,
        CASE WHEN SAFE_CAST(publication_date AS INT64) IS NOT NULL AND LENGTH(CAST(publication_date AS STRING)) = 8
             THEN PARSE_DATE('%Y%m%d', CAST(publication_date AS STRING))
             ELSE NULL END AS publication_date,
        CASE WHEN SAFE_CAST(grant_date AS INT64) IS NOT NULL AND LENGTH(CAST(grant_date AS STRING)) = 8
             THEN PARSE_DATE('%Y%m%d', CAST(grant_date AS STRING))
             ELSE NULL END AS grant_date,
        (SELECT t.text FROM UNNEST(title_localized) AS t LIMIT 1) AS title,
        (SELECT a.text FROM UNNEST(abstract_localized) AS a LIMIT 1) AS abstract,
        (SELECT
           CASE
             WHEN REGEXP_CONTAINS(TO_JSON_STRING(s), r'^\s*\{{') THEN JSON_EXTRACT_SCALAR(TO_JSON_STRING(s), '$.name')
             ELSE REPLACE(TO_JSON_STRING(s), '"', '')
           END
         FROM UNNEST(assignee_harmonized) AS s LIMIT 1
        ) AS assignee,
        TO_JSON_STRING(ARRAY(
          SELECT
            CASE
              WHEN REGEXP_CONTAINS(TO_JSON_STRING(i), r'^\s*\{{') THEN COALESCE(JSON_EXTRACT_SCALAR(TO_JSON_STRING(i), '$.name'),
                                                                       JSON_EXTRACT_SCALAR(TO_JSON_STRING(i), '$.fullName'))
              ELSE REPLACE(TO_JSON_STRING(i), '"', '')
            END
          FROM UNNEST(inventor) AS i LIMIT 10
        )) AS inventors_json,
        TO_JSON_STRING(ARRAY(
          SELECT AS STRUCT
            CASE
              WHEN REGEXP_CONTAINS(TO_JSON_STRING(c), r'^\s*\{{') THEN COALESCE(JSON_EXTRACT_SCALAR(TO_JSON_STRING(c), '$.code'),
                                                                              JSON_EXTRACT_SCALAR(TO_JSON_STRING(c), '$.classification'))
              ELSE REPLACE(TO_JSON_STRING(c), '"', '')
            END AS code,
            CASE
              WHEN REGEXP_CONTAINS(TO_JSON_STRING(c), r'^\s*\{{') THEN SAFE_CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(c), '$.first') AS BOOL)
              ELSE NULL
            END AS first
          FROM UNNEST(cpc) AS c LIMIT 20
        )) AS cpc_json,
        ROW_NUMBER() OVER (ORDER BY filing_date DESC NULLS LAST) AS row_num
      FROM `patents-public-data.patents.publications`
      WHERE filing_date IS NOT NULL
        AND publication_date IS NOT NULL
        AND country_code IS NOT NULL
    )
    SELECT * EXCEPT(row_num)
    FROM patent_data
    WHERE row_num <= {limit}
    ORDER BY filing_date DESC
    """

    logger.info("Running enhanced query for up to %d patents...", limit)
    query_job = bq_client.query(query, retry=gcp_retry.Retry(deadline=60))
    results_iter = query_job.result()
    total_rows = getattr(results_iter, "total_rows", 0) or 0

    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_name = os.getenv("DB_NAME", "patents_db")
    db_user = os.getenv("DB_USER", "pipeline_user")
    db_pass = os.getenv("DB_PASS")
    if db_host and not db_pass:
        logger.error("DB_PASS required when connecting over TCP")
        raise SystemExit(1)

    insert_sql = """
    INSERT INTO public.patents_enhanced
      (publication_number, application_number, country_code, kind_code,
       filing_date, publication_date, grant_date, title, abstract,
       assignee, inventors, cpc_codes, loaded_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, CURRENT_TIMESTAMP)
    ON CONFLICT (publication_number) DO NOTHING
    """

    conn_kwargs = {k: v for k, v in {
        "host": db_host,
        "dbname": db_name,
        "user": db_user,
        "password": db_pass
    }.items() if v is not None}

    total_loaded = 0
    batch = []
    batch_size = 5000
    start_time = time.time()
    last_log_time = start_time

    with psycopg2.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            # Use helper function to read SQL file
            migration_sql = _get_sql_file("001_create_tables.sql")
            cur.execute(migration_sql)
            conn.commit()  # Commit DDL separately
            
            for r in results_iter:
                try:
                    inventors = json.loads(r.inventors_json) if r.inventors_json else None
                except Exception:
                    inventors = None
                try:
                    cpc = json.loads(r.cpc_json) if r.cpc_json else None
                except Exception:
                    cpc = None
                
                row = (
                    r.publication_number, r.application_number, r.country_code, r.kind_code,
                    r.filing_date, r.publication_date, r.grant_date,
                    r.title, r.abstract, r.assignee, Json(inventors), Json(cpc)
                )
                batch.append(row)
                if len(batch) >= batch_size:
                    execute_batch(cur, insert_sql, batch)
                    total_loaded += len(batch)
                    
                    # Timing metrics
                    elapsed = time.time() - start_time
                    rows_per_sec = total_loaded / elapsed if elapsed > 0 else 0
                    est_remaining = (total_rows - total_loaded) / rows_per_sec if rows_per_sec > 0 else 0
                    
                    logger.info(
                        "Inserted %d rows | Total: %d/%d (%.1f%%) | "
                        "Speed: %.0f rows/sec | ETA: %.0f min",
                        batch_size, total_loaded, total_rows,
                        (total_loaded / total_rows * 100) if total_rows > 0 else 0,
                        rows_per_sec,
                        est_remaining / 60
                    )
                    batch = []
            
            if batch:
                execute_batch(cur, insert_sql, batch)
                total_loaded += len(batch)
            
            conn.commit()
            total_time = time.time() - start_time
            logger.info(
                "[OK] Complete | Loaded %d rows in %.1f sec (%.0f rows/sec)",
                total_loaded, total_time, total_loaded / total_time if total_time > 0 else 0
            )

    logger.info("[OK] Retrieved ~%d rows, Loaded %d rows", total_rows, total_loaded)

if __name__ == "__main__":
    try:
        scaled_pipeline()
    except Exception:
        logging.exception("scaled_pipeline failed")
        raise