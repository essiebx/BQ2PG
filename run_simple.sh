#!/bin/bash
echo "🚀 SIMPLE PIPELINE RUN"
echo "====================="

# Start PostgreSQL
podman run -d \
  --name bq2pg-postgres \
  -e POSTGRES_PASSWORD=postgres123 \
  -e POSTGRES_DB=patents_db \
  -p 5433:5432 \
  docker.io/library/postgres:15-alpine

sleep 5

echo "Running pipeline with --test flag..."
podman run --rm \
  --network host \
  -v ./credentials:/app/credentials:ro \
  -v ./src:/app/src:ro \
  -v ./main.py:/app/main.py:ro \
  -v ./docker.env:/app/.env:ro \
  bq2pg-pipeline \
  python -c "
import sys
sys.path.append('/app/src')
sys.argv = ['main.py', '--test']

# Monkey-patch to see what's happening
import logging
logging.basicConfig(level=logging.DEBUG)

exec(open('/app/main.py').read())
"

# Check if table was created
echo -e "\n📊 Checking database..."
podman exec bq2pg-postgres psql -U postgres -d patents_db -c "
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public';
"

# Clean up
podman stop bq2pg-postgres
podman rm bq2pg-postgres
