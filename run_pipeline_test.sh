#!/bin/bash
echo "🚀 BQ2PG PIPELINE TEST"
echo "======================"

# Clean up
podman rm -f bq2pg-postgres 2>/dev/null || true

# Start PostgreSQL
echo "1. Starting PostgreSQL..."
podman run -d \
  --name bq2pg-postgres \
  -e POSTGRES_PASSWORD=postgres123 \
  -e POSTGRES_DB=patents_db \
  -p 5433:5432 \
  docker.io/library/postgres:15-alpine

sleep 5
podman exec bq2pg-postgres pg_isready -U postgres || {
    echo "❌ PostgreSQL failed to start"
    exit 1
}
echo "✅ PostgreSQL ready on port 5433"

# Run pipeline with 100 rows
echo "2. Running pipeline (100 rows)..."
podman run --rm \
  --network host \
  -v ./credentials:/app/credentials:ro \
  -v ./src:/app/src:ro \
  -v ./main.py:/app/main.py:ro \
  -v ./docker.env:/app/.env:ro \
  bq2pg-pipeline \
  python /app/main.py --test

# Check results
echo "3. Checking results..."
podman exec bq2pg-postgres psql -U postgres -d patents_db -c "
SELECT 
    COUNT(*) as total_patents,
    MIN(filing_date) as earliest,
    MAX(filing_date) as latest
FROM patents;
" 2>/dev/null || echo "⚠️  Could not query patents table (might not exist yet)"

# Clean up
echo "4. Cleaning up..."
podman stop bq2pg-postgres
podman rm bq2pg-postgres

echo "🎉 Test complete!"
