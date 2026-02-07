# BQ2PG: Database Migration Engine

**Empowering the Data Community with High-Performance, Zero-Cost Cloud-to-Local Migration**

---

## 📄 Product Requirements Document (PRD)

### 1. Problem Statement
Many data engineers and analysts need to move data from BigQuery to a local or on-premise PostgreSQL instance for cost-efficient analysis, development, or backup. Existing solutions are either:
- **Enterprise-Heavy**: Requiring complex orchestration (Airflow, DBT, Dataflow) and high operational overhead.
- **Cost-Prohibitive**: Paid SaaS tools charge exorbitant fees based on row volume.
- **CLI-Only**: Lacking an accessible interface for non-technical or semi-technical users.

### 2. Target Audience
- **Data Analysts**: Needing quick subsets of cloud data for local exploration.
- **Developers**: Building apps locally using production-like datasets.
- **DevOps Engineers**: Seeking a lightweight, self-hostable ETL tool without massive infra requirements.

### 3. Solution Overview
**BQ2PG** is a high-performance migration engine designed to be "Lite yet Powerful." It provides a premium, web-based experience to move data using Google's free-tier APIs, ensuring zero cost while maintaining enterprise-grade resilience and observability.

---

## 🛠 Functional Requirements

### FR-01: Premium Web Interface
- **Glassmorphism Design**: A sleek, modern UI built with Outfit typography and vibrant aesthetics.
- **Guided Stepper**: A 4-step wizard for credentials, source config, destination config, and execution.
- **Real-Time Instrumentation**: Live progress tracking including extraction rates, load speeds, and error counts.

### FR-02: Resilience & Reliability
- **Circuit Breaker Pattern**: Prevents systemic collapse when downstream databases or cloud APIs are unstable.
- **Retry Policy**: Intelligent backoff strategies for transient network failures.
- **Dead Letter Queue (DLQ)**: Failed records are automatically persisted locally for post-migration analysis.

### FR-03: Zero-Dependency Monitoring
- **Lite Metrics**: Custom in-memory metrics collector (replacing Prometheus) for zero-overhead performance tracking.
- **Structured Logging**: JSON logs optimized for modern log management tools.

---

## 🗼 Technical Specification

### Internal Architecture
BQ2PG follows a modular architecture designed for extensibility:
- **Frontend**: Vanila HTML5/CSS3/JS with a focus on CSS custom properties and micro-animations.
- **API Strategy**: Lightweight Flask-based REST API serving the migration logic.
- **Core Engine**:
    - `src/extract.py`: Highly efficient Google BigQuery client.
    - `src/load.py`: SQLAlchemy-backed PostgreSQL loader with batching support.
    - `src/quality/`: Automated schema mapping and data validation rules.

### Performance Design
- **Memory Optimization**: Chunk-based processing ensures millions of rows can be migrated even on low-resource machines.
- **Parallel Processing**: Multi-threaded extraction and loading pipelines.

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL (Docker or local installation)
- Google Cloud Service Account (JSON)

### One-Command Setup
```bash
# Install the engine
git clone https://github.com/essiebx/BQ2PG.git && cd BQ2PG
pip install -r requirements.txt

# Launch the platform
python api/server.py
```
*Access the dashboard at `http://localhost:5000`*

---

## 📈 Roadmap

- [x] Premium UI/UX Overhaul
- [x] Removal of heavy DevOps dependencies (Lite Refactor)
- [x] Programmatic Credential Injection
- [ ] Support for Incremental Loads
- [ ] Multi-Cloud Source Support (Snowflake, Redshift)
- [ ] Advanced Data Masking/Anonymization

---

## 🤝 Contributing
Contributions are what make the data community amazing. Please read our [CONTRIBUTING.md](CONTRIBUTING.md) to get started.

## 📝 License
Distributed under the MIT License. See `LICENSE` for more information.

---

**Made with ❤️ for the Data Community**
