# FinFlow

Personal finance aggregation and fraud detection pipeline. Ingests synthetic transactions from multiple Indonesian financial sources (BCA, Mandiri, GoPay, OVO, Visa), processes them through a Medallion architecture (Bronze → Silver → Gold), and serves near-real-time fraud alerts and daily cashflow reports via a static dashboard.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│   Generator   │───▶│  FastAPI      │───▶│    Redpanda      │
│  (Faker)      │    │  Ingestion    │    │  (Kafka-compat)  │
└──────────────┘    │  + PII (FPE)  │    └──────┬───────────┘
                    └──────────────┘           │
                                               ▼
                    ┌──────────────────────────────────────────┐
                    │         Spark Structured Streaming        │
                    │                                          │
                    │  ┌──────────┐  ┌────────────┐  ┌──────┐ │
                    │  │  Silver   │  │   Fraud    │  │Alerts│ │
                    │  │  Writer   │  │  Detection │  │ Sink │ │
                    │  └────┬─────┘  └─────┬──────┘  └──┬───┘ │
                    └───────┼──────────────┼────────────┼─────┘
                            ▼              │            ▼
                    ┌──────────────┐       │    ┌──────────────┐
                    │   Iceberg    │       │    │   Iceberg    │
                    │   Silver     │       │    │   Gold       │
                    │  (MinIO S3)  │       │    │  (MinIO S3)  │
                    └──────┬───────┘       │    └──────┬───────┘
                           │               │           │
                           ▼               │           ▼
                    ┌──────────────┐       │    ┌──────────────┐
                    │     dbt      │       │    │    DuckDB     │
                    │  (DuckDB)    │       │    │   Serving     │
                    └──────┬───────┘       │    └──────┬───────┘
                           │               │           │
                           ▼               │           ▼
                    ┌──────────────────────────────────────────┐
                    │          Evidence.dev Dashboard           │
                    │  Cashflow │ Fraud Alerts │ User Summary   │
                    └──────────────────────────────────────────┘
```

## Tech Stack

| Concern | Tool |
|---|---|
| Ingestion API | FastAPI + aiokafka |
| Data Contracts | Pydantic v2 (`schemas/`) |
| Event Broker | Redpanda |
| Object Storage | MinIO (S3-compatible) |
| Table Format | Apache Iceberg |
| Stream Processing | Spark Structured Streaming |
| Transformation | dbt (DuckDB adapter) |
| Data Quality | dbt tests + Great Expectations v1.x |
| Dashboard | Evidence.dev |
| Orchestration | Apache Airflow |
| Secrets | HashiCorp Vault |
| IaC | Terraform |
| Monitoring | Prometheus + Grafana + Loki |
| Lineage | OpenLineage + Marquez |

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Node.js 18+ (for Evidence.dev dashboard)

### Setup

```bash
# Clone and install
git clone https://github.com/<your-user>/finflow.git
cd finflow
pip install -e ".[dev]"

# Start infrastructure
make infra-up

# Start the ingestion API
make dev

# (In another terminal) Run the synthetic data generator
make simulate
```

### Running Tests

```bash
# Unit tests (no Docker needed)
make test

# Integration tests (requires Docker stack)
make test-integration

# E2E tests (full stack)
make test-e2e
```

### dbt & Dashboard

```bash
# Run dbt transformations
make dbt-run

# Start the Evidence.dev dashboard
make dashboard-dev
```

## Project Structure

See [AGENTS.md](AGENTS.md) for the complete project specification, including:
- Repository structure
- Critical rules and constraints
- Data contracts and schema versioning
- Adding new transaction sources
- Architecture Decision Records

## License

MIT
