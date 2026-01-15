-- filepath: /home/essie/bq2pg-pipeline/sql/migrations/001_create_tables.sql
-- Idempotent migration to create pipeline tables and indexes
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.patents_simple (
  publication_number TEXT PRIMARY KEY,
  application_number TEXT,
  country_code VARCHAR(10),
  filing_date DATE,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.patents_enhanced (
  publication_number TEXT PRIMARY KEY,
  application_number TEXT,
  country_code VARCHAR(10),
  kind_code VARCHAR(10),
  filing_date DATE,
  publication_date DATE,
  grant_date DATE,
  title TEXT,
  abstract TEXT,
  assignee TEXT,
  inventors JSONB,
  cpc_codes JSONB,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.patents_large (
  publication_number TEXT PRIMARY KEY,
  application_number TEXT,
  country_code VARCHAR(10),
  kind_code VARCHAR(10),
  filing_date DATE,
  publication_date DATE,
  grant_date DATE,
  title TEXT,
  abstract TEXT,
  assignee TEXT,
  inventors JSONB,
  cpc_codes JSONB,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for patents_simple
CREATE INDEX IF NOT EXISTS idx_simple_filing_date ON public.patents_simple(filing_date);
CREATE INDEX IF NOT EXISTS idx_simple_country ON public.patents_simple(country_code);

-- Indexes for patents_enhanced
CREATE INDEX IF NOT EXISTS idx_enhanced_filing_date ON public.patents_enhanced(filing_date);
CREATE INDEX IF NOT EXISTS idx_enhanced_country ON public.patents_enhanced(country_code);
CREATE INDEX IF NOT EXISTS idx_enhanced_publication_date ON public.patents_enhanced(publication_date);
CREATE INDEX IF NOT EXISTS idx_enhanced_cpc ON public.patents_enhanced USING GIN(cpc_codes);
CREATE INDEX IF NOT EXISTS idx_enhanced_inventors ON public.patents_enhanced USING GIN(inventors);

-- Indexes for patents_large
CREATE INDEX IF NOT EXISTS idx_large_filing_date ON public.patents_large(filing_date);
CREATE INDEX IF NOT EXISTS idx_large_country ON public.patents_large(country_code);
CREATE INDEX IF NOT EXISTS idx_large_cpc ON public.patents_large USING GIN(cpc_codes);
CREATE INDEX IF NOT EXISTS idx_large_inventors ON public.patents_large USING GIN(inventors);
