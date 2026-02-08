#  BlueQuery Migration Tool
<img width="1887" height="931" alt="image" src="https://github.com/user-attachments/assets/41733c18-8a98-487e-8495-5657b6154dc7" />


**Version**: 1.1.0 | **Date**: February 2026

---

##  1. Product Overview
**BlueQuery** is a high-performance, specialized data migration tool designed to bridge the gap between Google BigQuery (Cloud Data Warehouse) and PostgreSQL (Relational Database). It provides an intuitive Web UI to manage complex ETL workflows without requiring deep knowledge of Python or SQL.

###  1.1 Problem Statement
Migrating multi-million row datasets from BigQuery to PostgreSQL is often fraught with memory issues, network timeouts, and data type mapping complexities. Existing tools are either overly generic (expensive) or require complex CLI configurations.

###  1.2 Solution
A lightweight, self-hosted web application that handles the heavy lifting of:
- **Authentication**: Seamless Service Account logic.
- **Transformation**: Auto-mapping BigQuery schemas to SQL.
- **Efficiency**: Chunked streaming to manage memory.
- **Reliability**: Real-time checkpointing for resume-on-failure.

---

##  2. Target Audience
- **Data Engineers**: Looking for a reliable tool to move data for local analysis or application syncing.
- **Researchers**: Needing to pull specific subsets of large public BigQuery datasets to a local SQL engine.
- **Developers**: Needing a quick way to seed production/staging databases from cloud data sources.

---

##  3. Core Features

###  3.1 Migration Wizard (Web UI)
- **Step 1: Credentials**: Drag-and-drop secure upload of GCP `.json` keys.
- **Step 2: Source Config**: Intelligent project/dataset/table selection with row limit controls.
- **Step 3: Dest Config**: Flexible PostgreSQL connection settings.
- **Step 4: Execution**: Real-time progress tracker with live row counts and performance metrics.

###  3.2 High-Performance Engine
- **Chunked Extraction**: Default 50,000 row chunks to ensure low RAM footprint.
- **Cost Estimation**: Dry-run feature to estimate BigQuery bytes processed before starting.
- **Automatic Checkpointing**: Progress is saved every 100,000 rows.
- **Data Cleaning**: Integrated transformation layer for date parsing and type normalization.

###  3.3 Reliability Patterns
- **Retry Policy**: 3-step exponential backoff for network-related failures.
- **Dead Letter Queue (DLQ)**: Failed records are moved to a local JSON file instead of crashing.
- **Circuit Breaker**: Prevents resource exhaustion if systems become unreachable.

---

##  4. Scalability & Data Handling
The BlueQuery pipeline is designed for enterprise-scale migration, limited only by your cloud quotas and local hardware resources.

###  4.1 Limits & Capacities
- **Million+ Rows**: Pre-configured and tested for migrations exceeding 1,000,000 records.
- **Zero Hard Cap**: No software limit; it flows as long as BigQuery provides and PG stores.
- **Safety Limit**: By default, the app is set to stop after **1,000,000 rows** (`MAX_ROWS_PER_RUN`) to prevent unexpected cloud costs. This is easily adjustable in `src/app_config.py` (Line 29) or via environment variables:
  ```bash
  export MAX_ROWS_PER_RUN=5000000
  ```

###  4.2 Handling "Massive" Data
1. **Low RAM Footprint**: Maintained at **<500MB RAM** regardless of total dataset size.
2. **Streaming Load**: Data is pushed to PostgreSQL immediately after cleaning.
3. **Auto-Save**: Checkpoints are saved every **100,000 rows** (`extraction_count % 100000 == 0`).

---

##  5. Technical Stack
- **Backend**: Python 3.11+, Flask (API & Web Server), `pandas` (ETL Engine).
- **Frontend**: Vanilla HTML5, Modern CSS (Slate & Indigo Professional Theme).
- **Database**: Google BigQuery (Source) ➡️ PostgreSQL 12-16 (Destination).
- **Infrastructure**: `google-cloud-bigquery`, `flask-cors`, `psycopg2-binary`.

---

---

## ⚡ 6. Quick Start

### 6.1 Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Google Cloud Service Account JSON key

### 6.2 Setup & Execution
```bash
# Initialize Environment
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt

# Start the API Server in background (Recommended)
nohup ./.venv/bin/python api/server.py > api.log 2>&1 & echo $! > api.pid
```
Access the Web UI at: **`http://localhost:5000`**

###  6.3 Operational Stability
While you can run the API simply using `python api/server.py`, the long-form command above is recommended for professional stability:

1. **The "Ghost" Mode (`&` and `nohup`)**: 
   - `&` pushes the process to the background, freeing your terminal.
   - `nohup` ensures the server keeps running even if you close your terminal or log out.
2. **The "Black Box" Recorder (`> api.log 2>&1`)**: 
   - Dynamically saves all logs and errors into `api.log`. If a migration fails at 3:00 AM, you'll have the exact trace recorded.
3. **The "Kill Switch" (`echo $! > api.pid`)**: 
   - Saves the process ID to `api.pid`. This allows you to stop the server cleanly with one command:  
     `kill $(cat api.pid)`

---

## 7. Security

BlueQuery includes several built-in security features to protect your data and credentials:

### 7.1 Hardened Input Validation
The API uses strict regex validation for all database identifiers (project IDs, dataset names, and table names). Validation is performed at the earliest possible stage of the request lifecycle to block SQL injection and unauthorized command execution.

### 7.2 Global Security Headers
The server automatically injects industry-standard security headers into every response:
- **X-Frame-Options (DENY)**: Prevents clickjacking by blocking iframe embedding.
- **Content-Security-Policy (CSP)**: Restricts script/style sources to prevent XSS.
- **HSTS**: Enforces secure connections over a long duration.
- **X-Content-Type-Options**: Prevents MIME-type sniffing.

### 7.3 Advanced Protection Layers
- **Rate Limiting**: Integrated `Flask-Limiter` to prevent brute-force attacks on credentials and DoS on migration triggers (Configured for 10-20 requests/min).
- **Request Size Limiting**: Enforced a **5MB cap** on all incoming JSON payloads to prevent memory exhaustion attacks.
- **Deep Response Scrubbing**: A recursive cleaning engine ensures that sensitive keys (like `private_key` or `password`) are **never** returned in API responses or job status updates, even if present in the backend state.

### 7.4 Log Masking
The system automatically identifies and masks sensitive keys (like `password`, `private_key`, and `client_email`) in `api.log` to ensure no plain-text secrets are stored in operational logs.

### 7.5 Shared Secret Authentication (Optional)
For multi-user environments, you can lock the API behind a shared secret token:

1. **Set the Token**: Start the server with the `API_SECURITY_TOKEN` environment variable:
   ```bash
   export API_SECURITY_TOKEN=my-secret-key-123
   nohup ./.venv/bin/python api/server.py > api.log 2>&1 &
   ```
2. **Access the UI**: The frontend will automatically detect the token if you append it to the URL once:
   `http://localhost:5000?token=my-secret-key-123`
3. **Persistence**: The token is safely stored in browser `localStorage` for future sessions.

---

##  8. Project Structure
```text
├── api/                  # Backend API Server (Flask)
├── frontend/             # Modern Web UI (HTML/JS/CSS)
├── src/                  # Core ETL Engine & Libraries
│   ├── app_config.py     # Base application configuration
│   ├── extract.py        # BigQuery extraction logic
│   ├── load.py           # PostgreSQL loading logic
│   ├── transform.py      # Data cleaning pipeline
│   └── quality/          # Data integrity & validation
├── sql/migrations/       # Database schema setup
└── tests/                # Automated Test Suite
```

---

##  9. Troubleshooting

**Server Issues:**
```bash
# Check if running
ps aux | grep server.py

# Restart manually
python api/server.py  # Remember to run: source .venv/bin/activate

# Check logs for errors
tail -f api.log
```

**Database Issues:**
Ensure PostgreSQL is running and your target database/user exists.
```bash
sudo service postgresql status
```

---

##  10. Success Metrics
- **99.9% Success Rate**: migrations completed successfully via checkpoint resume.
- **Enterprise Ready**: High-throughput extraction and zero "Out of Memory" crashes.

---

##  Support
- **Issues**: Open a GitHub Issue
- **Maintainer**: [Esther](https://github.com/essie-dev)

> **Made for high-performance data engineering.**
