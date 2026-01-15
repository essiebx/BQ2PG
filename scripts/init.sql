-- scripts/init.sql
-- PostgreSQL initialization for patents database

-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For text search

-- Create schema
CREATE SCHEMA IF NOT EXISTS patents;
GRANT ALL ON SCHEMA patents TO postgres;

-- Set search path
ALTER DATABASE patents_db SET search_path TO patents, public;

-- Create read-only user for applications
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'patents_reader') THEN
        CREATE USER patents_reader WITH PASSWORD 'reader123';
    END IF;
END $$;

GRANT CONNECT ON DATABASE patents_db TO patents_reader;
GRANT USAGE ON SCHEMA patents TO patents_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA patents TO patents_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA patents GRANT SELECT ON TABLES TO patents_reader;

-- Create statistics
CREATE TABLE IF NOT EXISTS patents.load_stats (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    rows_loaded INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_seconds INTEGER,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT
);

-- Create index on stats for querying
CREATE INDEX IF NOT EXISTS idx_load_stats_time ON patents.load_stats(start_time);
CREATE INDEX IF NOT EXISTS idx_load_stats_table ON patents.load_stats(table_name);

-- Log initialization
INSERT INTO patents.load_stats (table_name, rows_loaded, start_time, end_time, success)
VALUES ('database_init', 0, NOW(), NOW(), TRUE);

COMMENT ON TABLE patents.load_stats IS 'Pipeline loading statistics';
COMMENT ON COLUMN patents.load_stats.duration_seconds IS 'Load duration in seconds';