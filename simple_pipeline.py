#!/usr/bin/env python3
import os
import json
import logging
from google.cloud import bigquery
import psycopg2
from psycopg2.extras import execute_batch, Json
from pathlib import Path

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

def test_pipeline():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.info("🧪 Simple Pipeline Test")

    sa_path = _require_env("GOOGLE_APPLICATION_CREDENTIALS")
    with open(sa_path) as f:
        gcp_key = json.load(f)
    project_id = gcp_key.get("project_id")

    bq_client = bigquery.Client.from_service_account_json(sa_path, project=project_id)

    query = """
    SELECT
      publication_number,
      application_number,
      country_code,
      PARSE_DATE('%Y%m%d', CAST(filing_date AS STRING)) AS filing_date
    FROM `patents-public-data.patents.publications`
    WHERE filing_date > 20250000
    LIMIT 10000
    """

    logging.info(" Running query...")
    query_job = bq_client.query(query)
    results_iter = query_job.result()

    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_name = os.getenv("DB_NAME", "patents_db")
    db_user = os.getenv("DB_USER", "pipeline_user")
    db_pass = os.getenv("DB_PASS")
    if db_host and not db_pass:
        logging.error("DB_PASS required when connecting over TCP")
        raise SystemExit(1)

    insert_sql = """
    INSERT INTO public.patents_simple
      (publication_number, application_number, country_code, filing_date)
    VALUES (%s,%s,%s,%s)
    ON CONFLICT (publication_number) DO NOTHING
    """

    conn_kwargs = {"host": db_host, "dbname": db_name, "user": db_user}
    if db_pass:
        conn_kwargs["password"] = db_pass

    total_loaded = 0
    with psycopg2.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            migration_sql = _get_sql_file("001_create_tables.sql")
            cur.execute(migration_sql)
            batch = []
            batch_size = 500
            for r in results_iter:
                batch.append((r.publication_number, r.application_number, r.country_code, r.filing_date))
                if len(batch) >= batch_size:
                    execute_batch(cur, insert_sql, batch)
                    total_loaded += len(batch)
                    batch = []
            if batch:
                execute_batch(cur, insert_sql, batch)
                total_loaded += len(batch)
            conn.commit()

    logging.info(" Loaded %d rows", total_loaded)

if __name__ == "__main__":
    try:
        test_pipeline()
    except Exception as e:
        logging.exception("Pipeline failed: %s", e)
        raise