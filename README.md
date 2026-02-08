# BigQuery to PostgreSQL Patents Pipeline (BQ2PG) - Complete Reference

**Status**: Production Ready - All 6 Phases Complete | **Version**: 1.0.0 | **Date**: February 2026

A comprehensive, enterprise-grade ETL data pipeline for migrating large-scale datasets from Google BigQuery to PostgreSQL with built-in security, resilience, monitoring, governance, advanced features, and production deployment automation.

## Quick Navigation

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Free Tier Deployment](#free-tier-deployment) [NEW]
- [API & Monitoring](#api--monitoring)
- [Configuration](#configuration)
- [Usage & Examples](#usage--examples)
- [Phases Breakdown](#phases-breakdown)
- [Production Deployment](#production-deployment)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

##  Project Overview

**BQ2PG Pipeline** is a sophisticated ETL (Extract, Transform, Load) data pipeline designed to migrate large-scale patent datasets from Google BigQuery to a local PostgreSQL database. This project demonstrates enterprise-grade data engineering practices including:

- **Large-scale data extraction** (1M+ patent records)
- **Intelligent schema mapping** and data transformation
- **Chunked processing** for memory efficiency
- **Error handling** and data validation
- **Multiple execution modes** (simple, scaled, debug)
- **Comprehensive logging** and monitoring
- **Containerized deployment** with Docker/Podman

### Key Features
Extract millions of patent records from BigQuery  
Handle complex nested data structures (arrays, JSON)  
Intelligent date parsing and format conversion  
Configurable batch processing and chunking  
Multiple pipeline strategies (simple, scaled, debug)  
PostgreSQL schema with optimized indexes  
Full Podman containerization  
Comprehensive error handling and logging  
ML-ready data exports with feature engineering  

---

## Problem Statement

### Background
Organizations often store massive patent datasets in cloud services like Google BigQuery for cost-effectiveness and scalability. However, when working locally or integrating with applications, there's a need to:

1. **Transfer large volumes of data** (100K to 10M+ records) efficiently
2. **Preserve data integrity** during transformation across different platforms
3. **Handle complex nested structures** (inventor arrays, CPC classifications, citations)
4. **Optimize for local analysis** while maintaining referential integrity
5. **Process data in chunks** to avoid memory overflow
6. **Maintain auditability** with logging and error tracking
7. **Adapt to different use cases** (simple testing, scaled production, debugging)

### Challenges
- **Volume**: Patents dataset contains millions of records with complex structures
- **Complexity**: Multiple nested arrays, JSON fields, and standardized data formats
- **Schema Mapping**: BigQuery schema differs from PostgreSQL; requires intelligent conversion
- **Performance**: Direct bulk loading can cause memory exhaustion and connection timeouts
- **Reliability**: Network failures, partial loads, and data corruption risks
- **Flexibility**: Need support for different load sizes, filtering, and testing modes

---

##  Solution Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GOOGLE BIGQUERY                          │
│        (Patents Dataset - Billions of Records)              │
└────────────────────┬────────────────────────────────────────┘
                     │ BigQuery Extractor
                     ▼
┌──────────────────────────────────────────────────────────────┐
│              TRANSFORMATION LAYER                            │
│  • Schema Mapping (BigQuery -> PostgreSQL)                   │
│  • Data Type Conversion                                     │
│  • Date Parsing (YYYYMMDD -> DATE)                           │
│  • Array/JSON Normalization                                 │
│  • Chunked Processing (50K rows per chunk)                  │
└──────────────────┬───────────────────────────────────────────┘
                   │ PostgreSQL Loader
                   ▼
┌──────────────────────────────────────────────────────────────┐
│              LOCAL POSTGRESQL DATABASE                       │
│  • patents_simple (basic records)                           │
│  • patents_enhanced (full feature set)                      │
│  • patents_large (1M+ records scale)                        │
│  • Optimized indexes for queries                            │
└──────────────────────────────────────────────────────────────┘
```

### Data Flow Pipeline

```
START
  │
  ├─► Load Configuration
  │   (BigQuery project, PostgreSQL connection, credentials)
  │
  ├─► BigQuery Extractor
  │   └─► Generate Query (with filters: limit, year, recent_days)
  │   └─► Connect to BigQuery using Service Account
  │   └─► Fetch data in chunks (50K rows default)
  │   └─► Yield chunk_num, DataFrame for each batch
  │
  ├─► Schema Mapper
  │   └─► Transform BigQuery schema to PostgreSQL
  │   └─► Parse dates from YYYYMMDD format
  │   └─► Extract English titles
  │   └─► Normalize arrays (inventors, assignees, CPC codes)
  │   └─► Convert nested data to JSON/JSONB
  │
  ├─► PostgreSQL Loader
  │   └─► Create tables with proper schemas
  │   └─► Handle NULL values and NaN conversions
  │   └─► Batch insert (10K rows per batch)
  │   └─► Create optimized indexes
  │
  └─► Finish
      └─► Log summary statistics
      └─► Record execution time
      └─► Return success/failure status
```

### Core Components

| Component | Purpose | Key Responsibility |
|-----------|---------|-------------------|
| **BigQueryExtractor** | Data source | Connect to BigQuery, execute queries, yield chunks |
| **SchemaMapper** | Transformation | Convert BigQuery schema to PostgreSQL, parse/normalize data |
| **PostgresLoader** | Data sink | Create tables, load data in batches, create indexes |
| **Config** | Configuration | Manage credentials, database connections, pipeline parameters |
| **Utils** | Helpers | Logging, timing, error handling utilities |

---

##  Project Skeleton

```
# bq2pg-pipeline/

## .github/
- **workflows/**
  - `ci-cd.yml`                    # [NEW] NEW: Main CI/CD pipeline
  - `performance.yml`              # [NEW] NEW: Performance tests

## src/
- `__init__.py`
- **config/**                          # [NEW] NEW: Configuration module
  - `__init__.py`
  - `config_manager.py`            # Hierarchical config
- **security/**                        # [NEW] NEW: Security module
  - `__init__.py`
  - `secret_manager.py`            # Google Secret Manager
  - `credential_manager.py`        # Credential rotation
- **resilience/**                      # [NEW] NEW: Resilience patterns
  - `__init__.py`
  - `retry.py`                     # Retry with backoff
  - `circuit_breaker.py`           # Circuit breaker
  - `dead_letter_queue.py`         # DLQ for failed batches
- **pipeline/**                        # [NEW] NEW: Pipeline orchestration
  - `__init__.py`
  - `checkpoint_manager.py`        # Checkpoint recovery
- **monitoring/**                      # [NEW] NEW: Observability
  - `__init__.py`
  - `structured_logger.py`         # JSON logging
  - `metrics.py`                   # Prometheus metrics
  - `tracer.py`                    # OpenTelemetry tracing
- **performance/**                     # [NEW] NEW: Performance optimization
  - `__init__.py`
  - `connection_pool.py`           # Connection pooling
  - `parallel_processor.py`        # Parallel processing
  - `memory_optimizer.py`          # Memory optimization
- `config.py`                        # [WARNING] MODIFIED: Backward compat wrapper
- `utils.py`                         # [WARNING] MODIFIED: Add new utilities
- `schema_mapper.py`                 # [WARNING] MODIFIED: Enhanced mapping
- `extract.py`                       # [WARNING] MODIFIED: Add resilience
- `transform.py`                     # Keep as is
- `load.py`                          # [WARNING] MODIFIED: Performance opts

## tests/
- `__init__.py`
- `conftest.py`                      # [NEW] NEW: Shared fixtures
- **unit/**                            # [NEW] NEW: Unit tests directory
  - `__init__.py`
  - `test_config.py`
  - `test_security.py`
  - `test_resilience.py`
  - `test_monitoring.py`
- **integration/**                     # [NEW] NEW: Integration tests
  - `__init__.py`
  - `test_e2e_pipeline.py`
- **performance/**                     # [NEW] NEW: Performance tests
  - `__init__.py`
  - `test_benchmarks.py`
- **fixtures/**                        # [NEW] NEW: Test fixtures
  - `sample_data.json`
- `test_extract.py`                  # [WARNING] MODIFIED: Expand tests
- `test_load.py`                     # [WARNING] MODIFIED: Expand tests
- `test_integration.py`              # [WARNING] MODIFIED: Comprehensive tests

## config/
- **environments/**                    # [NEW] NEW: Environment configs
  - `production.yaml`
  - `staging.yaml`
  - `development.yaml`
- `settings.yaml`                    # [WARNING] MODIFIED: Enhanced settings

## credentials/
- `key.json`                         # Keep (gitignored)

## scripts/
- **exporters/**                       # Existing
  - `export_ml_data.py`
  - `export_ml_features.py`
- **sql_runners/**                     # Existing
  - `run_sql.py`
- **monitoring/**                      # [NEW] NEW: Monitoring scripts
  - `start_metrics_server.py`
  - `check_health.py`
- `create_local_postgres.sh`
- `setup_environment.sh`
- `run_pipeline.sh`

## sql/
- **migrations/**
  - `001_create_tables.sql`
  - `002_create_indexes.sql`       # [NEW] NEW: Index creation

## data/                                # Existing (gitignored)
- `*.csv`

## logs/                                # [NEW] NEW: Application logs
- `pipeline.log`
- `errors.log`

## dlq/                                 # [NEW] NEW: Dead letter queue
- `failed_batch_*.json`

## checkpoints/                         # [NEW] NEW: Pipeline checkpoints
- `pipeline_*.json`

## metrics/                             # [NEW] NEW: Prometheus metrics
- `prometheus.yml`

## notebooks/                           # Existing
- **analysis/**

## docs/                                # Existing
- `bigquery_setup.md`
- `postgresql_setup.md`
- `setup_guide.md`

## .gitignore                           # [WARNING] MODIFIED: Add new ignores
## .pre-commit-config.yaml              # [NEW] NEW: Pre-commit hooks
## .secrets.baseline                    # Keep
## .env.example                         # [WARNING] MODIFIED: New env vars

## Containerfile                        # Keep
## compose.yaml                         # Keep
## Makefile                             # [WARNING] MODIFIED: New targets

## main.py                              # [WARNING] MODIFIED: Add new features
## simple_pipeline.py                   # Keep for reference
## scaled_pipeline.py                   # [WARNING] MODIFIED: Add checkpoints
## debug_pipeline.py                    # [WARNING] MODIFIED: Enhanced logging

## requirements.txt                     # [WARNING] MODIFIED: New dependencies
## requirements-dev.txt                 # [WARNING] MODIFIED: Testing tools
## constraints.txt                      # Keep

## README.md                            # [WARNING] MODIFIED: Update docs

```

---

## ⚙️ Installation & Setup

### Prerequisites
- Python 3.8+ (3.13 recommended)
- PostgreSQL 12+ (or use Docker)
- Google Cloud Project with BigQuery access
- Service Account credentials (JSON file)

### Step 1: Clone and Environment Setup

```bash
# Clone repository
git clone <repository-url>
cd bq2pg-pipeline

# Create virtual environment
python3 -m venv myenv
source myenv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # Optional: for development
```

### Step 2: Configure Credentials

```bash
# Copy environment template
cp .env.example .env

# Set your environment variables
export GOOGLE_CLOUD_PROJECT="your-project-id"
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials/key.json"
export DB_HOST="127.0.0.1"
export DB_PORT="5432"
export DB_NAME="patents_db"
export DB_USER="pipeline_user"
export DB_PASS="your_password"

# New: For secret manager integration
export SECRET_MANAGER_BACKEND="file"  # or "gcp", "aws", "env"
export SECRET_MANAGER_CONFIG="config/environments/development.yaml"
```

### Step 3: Setup PostgreSQL

**Option A: Local Installation**
```bash
# Run setup script
bash scripts/create_local_postgres.sh

# Or manual setup
sudo apt-get install postgresql
sudo service postgresql start
```

**Option B: Docker**
```bash
# Start PostgreSQL container
docker-compose up -d postgres

# Verify connection
psql -h localhost -U pipeline_user -d patents_db
```

### Step 4: Create Database Schema

```bash
# Run migrations
psql -h $DB_HOST -U $DB_USER -d $DB_NAME < sql/migrations/001_create_tables.sql

# Verify tables created
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "\dt"
```

---

## Free Tier Deployment (GitHub Student Pack)

This project is fully deployable using free services. See [FREE_TIER_QUICKSTART.md](./FREE_TIER_QUICKSTART.md) for detailed setup.

### Free Services Available

| Service | Cost | Purpose |
|---------|------|---------|
| **GitHub Actions** | $0 | CI/CD pipeline (unlimited for public repos) |
| **GHCR** (ghcr.io) | $0 | Docker image hosting |
| **GitHub Pages** | $0 | Documentation & test reports |
| **Prometheus** | $0 | Self-hosted metrics (container included) |
| **Grafana** | $0 | Self-hosted dashboards (container included) |
| **Loki** | $0 | Self-hosted log aggregation |
| **Jaeger** | $0 | Self-hosted distributed tracing |
| **Azure Student** | $0 | 12 months free + $200 initial credits |
| **DigitalOcean Student** | $0 | $50-100 in credits |
| **Railway** | $5/mo | Recommended simple deployment |

### Quick Start (100% FREE)

```bash
# Start full monitoring stack locally
docker-compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d

# Access services
echo "API:       http://localhost:5000"
echo "Grafana:   http://localhost:3000 (admin/admin123)"
echo "Prometheus: http://localhost:9090"
echo "Jaeger:    http://localhost:16686"

# Test API
curl http://localhost:5000/health
```

### Deployment Options

**Option 1: GitHub Pages** (Static Reports)
- Automatic test reports & coverage
- No cost, no infrastructure needed
- Limited to static content

**Option 2: Railway** ($5/month recommended)
- Deploy from docker-compose.yml
- Includes database hosting
- Perfect for portfolio projects

**Option 3: Azure** ($0 for 12 months, students)
- Full Kubernetes support
- $200 free credits per month (first month)
- Production-ready

**Option 4: DigitalOcean** ($0 with student credits)
- $50-100 free credits
- Simple $5/month droplet
- Great for learning infrastructure

See [GITHUB_STUDENT_PACK_SETUP.md](./docs/GITHUB_STUDENT_PACK_SETUP.md) for detailed configuration of each option.

---

## Usage

### Basic Pipeline Execution

```bash
# Test mode (100 rows)
python main.py --test

# Extract with limit
python main.py --limit 10000

# Extract by filing year
python main.py --year 2020 --limit 50000

# Extract recent patents (last N days)
python main.py --recent-days 30

# Drop and recreate tables
python main.py --limit 100000 --drop-tables
```

### Simple Pipeline (Development/Testing)

```bash
export DB_HOST=127.0.0.1
export DB_USER=pipeline_user
export DB_PASS='password'
export DB_NAME=patents_db
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials/key.json"

python3 simple_pipeline.py
```

### Scaled Pipeline (Production - 1M+ Records)

```bash
# Default 1M records
python3 scaled_pipeline.py

# Custom limit
export SCALED_LIMIT=5000000
python3 scaled_pipeline.py
```

### Debug Mode

```bash
# Extra logging and diagnostics
python3 debug_pipeline.py --verbose
```

### Export Data for ML

```bash
# Export raw data
python3 scripts/exporters/export_ml_data.py

# Export with features
python3 scripts/exporters/export_ml_features.py

# Check exports
ls -lh data/*.csv
head -10 data/patents_enhanced_ml_*.csv
```

### Using Make

```bash
# View available commands
make help

# Run pipeline
make run

# Run tests
make test

# Build container
make build

# Run in container
make run-container
```

---

## Skills Gained

### 1. **Data Engineering Fundamentals**
- [OK] ETL pipeline design and implementation
- [OK] Large-scale data processing (1M+ records)
- [OK] Chunked/streaming data processing for memory efficiency
- [OK] Error handling and data validation

### 2. **Cloud Technologies**
- [OK] Google BigQuery API integration and optimization
- [OK] Service account authentication and credential management
- [OK] Cost optimization for cloud queries
- [OK] Working with complex cloud data structures

### 3. **Database Design**
- [OK] Schema mapping between different database systems
- [OK] PostgreSQL optimization (indexes, JSONB, GIN indexes)
- [OK] Relational database best practices
- [OK] Data type conversions and normalization
- [OK] Batch insertion strategies for performance

### 4. **Python Development**
- [OK] Object-oriented design patterns (Extractors, Loaders)
- [OK] Decorator patterns (timing, error handling)
- [OK] Generator functions for memory-efficient processing
- [OK] SQLAlchemy ORM and raw SQL execution
- [OK] pandas DataFrame manipulation and optimization

### 5. **Software Engineering Practices**
- [OK] Configuration management (.env, Config classes)
- [OK] Comprehensive logging and monitoring
- [OK] Error handling and recovery mechanisms
- [OK] Unit and integration testing
- [OK] Code organization and modularity

### 6. **DevOps & Containerization**
- [OK] Docker/Podman containerization
- [OK] Docker Compose for multi-container setups
- [OK] Environment variable management
- [OK] Container networking and persistence
- [OK] Shell scripting for automation

### 7. **SQL & Query Optimization**
- ✅ Complex SQL query generation (dynamic queries)
- ✅ BigQuery SQL syntax and optimization
- ✅ PostgreSQL window functions and CTEs
- ✅ Index creation and query planning
- ✅ Data migration queries

### 8. **Monitoring & Performance**
- [OK] Performance profiling (timing decorators)
- [OK] Memory usage optimization
- [OK] Logging and debugging strategies
- [OK] Error tracking and reporting
- [OK] Batch size tuning for optimal throughput

### 9. **Data Science Integration**
- [OK] Feature engineering from raw data
- [OK] Data export for ML pipelines
- [OK] Jupyter notebook integration
- [OK] CSV data export and transformation

### 10. **Project Management**
- [OK] Version control with Git
- [OK] CI/CD pipeline concepts (GitHub Actions example)
- [OK] Documentation best practices
- [OK] Testing strategy and implementation
- [OK] Requirements management

---

## Future Enhancements

### Phase 1: Immediate Improvements
- [ ] **Incremental Loading**
  - Implement CDC (Change Data Capture) for only new/modified records
  - Track last_loaded_timestamp to avoid duplicate processing
  - Reduce query costs on BigQuery

- [ ] **Data Validation Framework**
  - Row count validation (source vs. target)
  - Data type checking
  - NULL value analysis
  - Duplicate detection

- [ ] **Advanced Error Handling**
  - Retry logic with exponential backoff
  - Dead letter queue for failed records
  - Automatic recovery from transient failures

### Phase 2: Feature Expansion
- [ ] **API Interface**
  - REST API for pipeline execution
  - Query builder UI
  - Real-time progress tracking
  - Historical run statistics

- [ ] **Data Quality Monitoring**
  - Great Expectations framework integration
  - Automated data profiling
  - Anomaly detection
  - Quality dashboards

- [ ] **Advanced Transformations**
  - Patent family relationships
  - Citation network analysis
  - Inventor/assignee deduplication
  - Technology classification enrichment

### Phase 3: Scalability & Performance
- [ ] **Distributed Processing**
  - Apache Airflow orchestration
  - Spark-based transformation for larger datasets
  - Parallel chunk processing
  - Task scheduling and dependencies

- [ ] **Caching & Optimization**
  - Query result caching
  - Materialized views for common queries
  - Connection pooling optimization
  - Partition pruning on date ranges

- [ ] **Multi-Cloud Support**
  - AWS Athena integration
  - Azure SQL Database support
  - Snowflake connector
  - Generic data warehouse abstraction

### Phase 4: Analytics & Insights
- [ ] **BI Integration**
  - Grafana dashboards
  - Metabase analytics
  - Real-time Tableau connections
  - KPI tracking

- [ ] **Advanced Analytics**
  - Patent trend analysis
  - Technology roadmap generation
  - Competitive intelligence reports
  - Citation impact analysis

- [ ] **Machine Learning**
  - Patent classification models
  - Inventor collaboration networks
  - Patent value prediction
  - Novelty scoring

### Phase 5: Enterprise Features
- [ ] **Security Enhancements**
  - Data encryption at rest/transit
  - Row-level security (RLS)
  - Audit logging
  - Sensitive data masking

- [ ] **Performance SLAs**
  - Load time guarantees
  - Data freshness SLAs
  - Automated capacity planning
  - Cost tracking and optimization

- [ ] **Multi-Tenancy**
  - Tenant isolation
  - Per-tenant data access policies
  - Custom transformation pipelines
  - Dedicated resources per tenant

### Technical Roadmap

```
Q1 2026
├── CDC Implementation
├── Data Validation Framework
└── Enhanced Error Handling

Q2 2026
├── REST API Interface
├── Quality Monitoring Dashboards
└── Advanced Transformations

Q3 2026
├── Apache Airflow Integration
├── Query Optimization & Caching
└── AWS/Azure Support

Q4 2026
├── BI Tool Integrations
├── ML Model Integration
└── Enterprise Security Features
```

---

## Architecture Decision Records (ADRs)

### ADR-001: Chunked Processing Strategy
**Decision**: Process data in 50K row chunks rather than loading all at once  
**Rationale**: Prevents memory exhaustion, allows resume on failure, better progress tracking  
**Trade-off**: Slightly slower due to increased SQL roundtrips, but safety is priority

### ADR-002: JSONB for Complex Fields
**Decision**: Store arrays/nested structures as PostgreSQL JSONB  
**Rationale**: Flexibility for evolving schemas, native indexing, query expressiveness  
**Trade-off**: Slightly more query complexity, but gains flexibility

### ADR-003: Service Account Authentication
**Decision**: Use Google Cloud Service Accounts instead of OAuth  
**Rationale**: Better for automated/scheduled pipelines, easier credential management  
**Trade-off**: Single credential point, must be carefully protected

---

## 📞 Support & Troubleshooting

### Common Issues

**Issue**: "Missing GOOGLE_APPLICATION_CREDENTIALS"
```bash
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials/key.json"
# Ensure credentials/key.json exists and is valid
```

**Issue**: "Connection refused" to PostgreSQL
```bash
# Check if PostgreSQL is running
sudo service postgresql status

# Or check Docker container
docker ps | grep postgres
```

**Issue**: "Out of memory" during large loads
```bash
# Reduce chunk size
export DEFAULT_CHUNK_SIZE=10000
python main.py --limit 100000
```

**Issue**: "BigQuery quota exceeded"
```bash
# Check current month usage in BigQuery console
# Reduce query scope:
python main.py --limit 50000  # Instead of millions
```

---

## 📄 License


---

## Contributing

Contributions are welcome! 

---

## 📞 Contact & Support

For issues, questions, or suggestions:
- 📧 Email: [your-email@example.com]
- 🐛 GitHub Issues: [repository-issues-link]
- 💬 Discussions: [repository-discussions-link]

---

**Last Updated**: January 15, 2026  
**Version**: 1.0.0  
**Maintainer**: Data Engineering Team

---

## 🙏 Acknowledgments

- Google Cloud Platform and BigQuery team
- PostgreSQL community
- Open-source libraries: pandas, SQLAlchemy, google-cloud-bigquery
- Contributors and testers

---

> **Made with ❤️ for data engineers and researchers**
