# 🚀 BigQuery to PostgreSQL Patents Pipeline (BQ2PG)

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Problem Statement](#problem-statement)
3. [Solution Architecture](#solution-architecture)
4. [Project Skeleton](#project-skeleton)
5. [Installation & Setup](#installation--setup)
6. [Usage](#usage)
7. [Skills Gained](#skills-gained)
8. [Future Enhancements](#future-enhancements)
9. [Contributing](#contributing)

---

## 🎯 Project Overview

**BQ2PG Pipeline** is a sophisticated ETL (Extract, Transform, Load) data pipeline designed to migrate large-scale patent datasets from Google BigQuery to a local PostgreSQL database. This project demonstrates enterprise-grade data engineering practices including:

- **Large-scale data extraction** (1M+ patent records)
- **Intelligent schema mapping** and data transformation
- **Chunked processing** for memory efficiency
- **Error handling** and data validation
- **Multiple execution modes** (simple, scaled, debug)
- **Comprehensive logging** and monitoring
- **Containerized deployment** with Docker/Podman

### Key Features
✅ Extract millions of patent records from BigQuery  
✅ Handle complex nested data structures (arrays, JSON)  
✅ Intelligent date parsing and format conversion  
✅ Configurable batch processing and chunking  
✅ Multiple pipeline strategies (simple, scaled, debug)  
✅ PostgreSQL schema with optimized indexes  
✅ Full Docker/Podman containerization  
✅ Comprehensive error handling and logging  
✅ ML-ready data exports with feature engineering  

---

## 🔍 Problem Statement

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

## 💡 Solution Architecture

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
│  • Schema Mapping (BigQuery → PostgreSQL)                   │
│  • Data Type Conversion                                     │
│  • Date Parsing (YYYYMMDD → DATE)                           │
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

## 📁 Project Skeleton

```
bq2pg-pipeline/
│
├── 📄 README.md                          # This file - comprehensive project documentation
├── 📄 main.py                            # Main entry point with CLI argument parsing
├── 📄 requirements.txt                   # Python dependencies for production
├── 📄 requirements-dev.txt               # Additional dev dependencies (pytest, jupyter)
├── 📄 Makefile                           # Build automation commands
├── 📄 Containerfile                      # Container image definition (Podman/Docker)
├── 📄 compose.yaml                       # Docker Compose for local PostgreSQL
├── 📄 .env.example                       # Environment variables template
├── 📄 constraints.txt                    # Python version/package constraints
│
├── 📂 src/                               # Core pipeline source code
│   │
│   ├── __init__.py                       # Package initialization
│   │
│   ├── config.py                         # 🔧 CONFIGURATION MANAGEMENT
│   │   └─ Loads environment variables (BigQuery credentials, DB connection)
│   │   └─ Validates required config at startup
│   │   └─ Provides connection strings for PostgreSQL
│   │   └─ Centralizes all configuration constants
│   │
│   ├── utils.py                          # 🛠️ UTILITY FUNCTIONS
│   │   └─ Logger setup with formatting
│   │   └─ Timer decorator for performance monitoring
│   │   └─ Error handling helpers
│   │   └─ Data validation utilities
│   │
│   ├── schema_mapper.py                  # 📊 SCHEMA & QUERY GENERATION
│   │   └─ generate_extraction_query() - Creates BigQuery SQL dynamically
│   │   └─ generate_create_table_sql() - Creates PostgreSQL table DDL
│   │   └─ Field mapping logic (BigQuery → PostgreSQL)
│   │   └─ Type conversion rules (STRUCT → JSONB, ARRAY → TEXT[])
│   │   └─ Supports filtering by limit, year, recent_days
│   │
│   ├── extract.py                        # 🔽 BIGQUERY EXTRACTION
│   │   └─ BigQueryExtractor class
│   │   └─ Manages BigQuery client connection
│   │   └─ Implements chunked extraction for large datasets
│   │   └─ Handles authentication via service account
│   │   └─ Yields data in configurable chunk sizes
│   │   └─ Performance monitoring with decorators
│   │
│   ├── transform.py                      # 🔄 DATA TRANSFORMATION
│   │   └─ Placeholder for future transformations
│   │   └─ Can be extended for data cleaning, validation
│   │   └─ Feature engineering hooks
│   │
│   └── load.py                           # 🔼 POSTGRESQL LOADING
│       └─ PostgresLoader class
│       └─ Database connection management
│       └─ Table creation with proper schemas
│       └─ DataFrame to SQL batch insertion
│       └─ Chunked loading from generator
│       └─ Data type conversion (NaN → NULL, arrays → JSON)
│       └─ Index creation for query optimization
│
├── 📂 config/                            # Configuration files
│   └── settings.yaml                     # YAML configuration (optional overrides)
│
├── 📂 credentials/                       # 🔐 AUTHENTICATION (Git-ignored)
│   └── key.json                          # Google Cloud Service Account JSON
│                                          # DO NOT commit this file!
│
├── 📂 sql/                               # SQL scripts and migrations
│   │
│   ├── migrations/                       # Database migrations
│   │   └── 001_create_tables.sql         # Initial schema with patents tables
│   │                                     # Creates: patents_simple, patents_enhanced, patents_large
│   │                                     # Includes: Indexes on filing_date, country, CPC, inventors
│   │
│   ├── analysis/                         # Analytical queries
│   ├── functions/                        # PostgreSQL stored procedures
│   ├── reports/                          # Report generation queries
│   └── views/                            # PostgreSQL views
│
├── 📂 scripts/                           # Utility and helper scripts
│   │
│   ├── create_local_postgres.sh          # 🐘 Setup local PostgreSQL instance
│   ├── setup_environment.sh              # 🔧 Initialize Python virtual environment
│   ├── init.sql                          # Database initialization script
│   ├── run_pipeline.sh                   # Shell wrapper for pipeline execution
│   │
│   ├── exporters/                        # 📤 Data export utilities
│   │   ├── export_ml_data.py            # Export data for ML training
│   │   └── export_ml_features.py        # Generate ML features and export
│   │
│   ├── sql_runners/                      # 🔍 SQL execution utilities
│   │   └── run_sql.py                   # Execute SQL queries from files
│   │
│   └── monitoring/                       # 📊 Performance monitoring
│
├── 📂 tests/                             # Unit and integration tests
│   ├── test_extract.py                   # BigQuery extraction tests
│   ├── test_load.py                      # PostgreSQL loading tests
│   └── test_integration.py               # End-to-end pipeline tests
│
├── 📂 data/                              # 💾 Data directory (Git-ignored)
│   └── patents_enhanced_ml_*.csv         # Exported CSV files from pipeline
│
├── 📂 logs/                              # 📝 Application logs (Git-ignored)
│   └── scaled_1M_*.log                   # Pipeline execution logs
│
├── 📂 outputs/                           # 📊 Pipeline outputs
│   ├── csv_exports/                      # Exported CSV files
│   ├── reports/                          # Generated reports
│   └── visualizations/                   # Charts and visualizations
│
├── 📂 notebooks/                         # 📓 Jupyter notebooks for analysis
│   ├── analysis/                         # Data analysis notebooks
│   ├── dashboards/                       # Interactive dashboards
│   └── documentation/                    # Documentation notebooks
│
├── 📂 docs/                              # 📚 Additional documentation
│   ├── bigquery_setup.md                 # BigQuery configuration guide
│   ├── postgresql_setup.md               # PostgreSQL setup instructions
│   └── setup_guide.md                    # Complete setup walkthrough
│
├── 📂 myenv/                             # 🐍 Python virtual environment (Git-ignored)
│   └── [Python packages and binaries]
│
├── 📄 simple_pipeline.py                 # 🧪 Minimal working pipeline (100-1K rows)
│                                         # Useful for testing and debugging
│                                         # Direct SQL without advanced features
│
├── 📄 scaled_pipeline.py                 # 🚀 Production-scale pipeline (1M+ rows)
│                                         # Chunked processing for memory efficiency
│                                         # Optimized for large dataset handling
│                                         # Includes retry logic and error recovery
│
├── 📄 debug_pipeline.py                  # 🐛 Debug version with extra logging
│                                         # Helps diagnose issues
│                                         # Verbose output for troubleshooting
│
├── 📄 docker_test.py                     # 🐳 Container-based testing
│                                         # Tests pipeline in container environment
│
├── 📄 run_pipeline.py                    # ⚙️ Alternative pipeline runner
├── 📄 run_simple.sh                      # 🏃 Shell script to run simple pipeline
├── 📄 run_pipeline_test.sh               # 🧪 Test pipeline execution script
│
├── 📄 setup-podman.sh                    # 🐳 Podman container setup script
├── 📄 fix-podman.sh                      # 🔧 Podman troubleshooting script
├── 📄 podman_setup.md                    # 📖 Podman configuration guide
│
├── 📂 .vscode/                           # VS Code settings
│
└── 📄 .gitignore                         # Git ignore patterns
    ├── credentials/                      # Never commit credentials
    ├── myenv/                            # Never commit virtual environment
    ├── logs/                             # Never commit log files
    ├── data/                             # Never commit data files
    ├── *.env                             # Never commit .env files
    └── __pycache__/                      # Never commit Python cache
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

## 🚀 Usage

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

## 📚 Skills Gained

### 1. **Data Engineering Fundamentals**
- ✅ ETL pipeline design and implementation
- ✅ Large-scale data processing (1M+ records)
- ✅ Chunked/streaming data processing for memory efficiency
- ✅ Error handling and data validation

### 2. **Cloud Technologies**
- ✅ Google BigQuery API integration and optimization
- ✅ Service account authentication and credential management
- ✅ Cost optimization for cloud queries
- ✅ Working with complex cloud data structures

### 3. **Database Design**
- ✅ Schema mapping between different database systems
- ✅ PostgreSQL optimization (indexes, JSONB, GIN indexes)
- ✅ Relational database best practices
- ✅ Data type conversions and normalization
- ✅ Batch insertion strategies for performance

### 4. **Python Development**
- ✅ Object-oriented design patterns (Extractors, Loaders)
- ✅ Decorator patterns (timing, error handling)
- ✅ Generator functions for memory-efficient processing
- ✅ SQLAlchemy ORM and raw SQL execution
- ✅ pandas DataFrame manipulation and optimization

### 5. **Software Engineering Practices**
- ✅ Configuration management (.env, Config classes)
- ✅ Comprehensive logging and monitoring
- ✅ Error handling and recovery mechanisms
- ✅ Unit and integration testing
- ✅ Code organization and modularity

### 6. **DevOps & Containerization**
- ✅ Docker/Podman containerization
- ✅ Docker Compose for multi-container setups
- ✅ Environment variable management
- ✅ Container networking and persistence
- ✅ Shell scripting for automation

### 7. **SQL & Query Optimization**
- ✅ Complex SQL query generation (dynamic queries)
- ✅ BigQuery SQL syntax and optimization
- ✅ PostgreSQL window functions and CTEs
- ✅ Index creation and query planning
-  Data migration queries

### 8. **Monitoring & Performance**
- ✅ Performance profiling (timing decorators)
- ✅ Memory usage optimization
- ✅ Logging and debugging strategies
- ✅ Error tracking and reporting
- ✅ Batch size tuning for optimal throughput

### 9. **Data Science Integration**
- ✅ Feature engineering from raw data
- ✅ Data export for ML pipelines
- ✅ Jupyter notebook integration
- ✅ CSV data export and transformation

### 10. **Project Management**
- ✅ Version control with Git
- ✅ CI/CD pipeline concepts (GitHub Actions example)
- ✅ Documentation best practices
- ✅ Testing strategy and implementation
- ✅ Requirements management

---

## 🔮 Future Enhancements

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

## 📖 Architecture Decision Records (ADRs)

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

##  Support & Troubleshooting

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



---

## 👥 Contributing

Contributions are welcome! 

---

## 📞 Contact & Support

For issues, questions, or suggestions:
- https://linkedin.com/in/esthernaisimoi

---

**Last Updated**: January 15, 2026  
**Version**: 1.0.0  
**Maintainer**: essie

---

## 🙏 Acknowledgments

- Google Cloud Platform and BigQuery 
- PostgreSQL
- Open-source libraries: pandas, SQLAlchemy, google-cloud-bigquery

---

> **Made with ❤️ for data engineers , any curious data nerd and researchers**
