-- Migration: Create indexes for performance
-- Description: Creates indexes to optimize query performance
-- Created: 2024-02-01

-- Index for main table
CREATE INDEX IF NOT EXISTS idx_records_created_at ON records(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_records_id ON records(id);
CREATE INDEX IF NOT EXISTS idx_records_source ON records(source);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_records_source_date ON records(source, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_records_status_created ON records(status, created_at DESC);

-- Index for foreign keys (if applicable)
-- CREATE INDEX IF NOT EXISTS idx_records_user_id ON records(user_id);

-- Partial indexes for filtered queries
CREATE INDEX IF NOT EXISTS idx_records_active ON records(id) 
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_records_recent ON records(id, created_at) 
  WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';

-- Statistics for query optimizer
ANALYZE records;

-- Log the migration
INSERT INTO schema_migrations (name, description, applied_at)
VALUES (
  '002_create_indexes',
  'Create indexes for performance optimization',
  NOW()
)
ON CONFLICT DO NOTHING;
