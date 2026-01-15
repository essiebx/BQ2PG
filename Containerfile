# Containerfile - Podman build configuration
FROM python:3.11-slim

LABEL maintainer="BQ2PG Pipeline"
LABEL description="BigQuery to PostgreSQL Patents Data Pipeline"
LABEL version="1.0"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data/logs directories
RUN mkdir -p data logs outputs && \
    chown -R appuser:appuser /app

USER appuser

# Health check (verify Python can import main modules)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from google.cloud import bigquery; import psycopg2; print('OK')" || exit 1

# Default command (can be overridden)
CMD ["python3", "scaled_pipeline.py"]