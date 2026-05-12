# FinFlow

**Personal finance aggregation and fraud detection pipeline.**

Ingests synthetic transactions from multiple Indonesian financial sources (BCA, Mandiri, GoPay, OVO, Visa), processes them through a **Medallion architecture** (Bronze → Silver → Gold), and serves near-real-time fraud alerts and daily cashflow reports via a static dashboard — all running locally via Docker Compose.

> This is a **portfolio project**. All transaction data is synthetic, generated with Faker. No real financial data is used.

---

## Table of Contents

- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [Infrastructure Services](#infrastructure-services)
- [Airflow Orchestration](#airflow-orchestration)
- [Running Individual Components](#running-individual-components)
- [dbt Transformations](#dbt-transformations)
- [Evidence.dev Dashboard](#evidencedev-dashboard)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Security & Secrets](#security--secrets)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌──────────────┐    ┌────────────────────┐    ┌──────────────────┐
│   Generator  │───▶│   FastAPI Gateway  │───▶│    Redpanda      │
│  (Faker)     │    │  + FPE PII masking │    │  (Kafka-compat)  │
└──────────────┘    └────────────────────┘    └──────┬───────────┘
                                                     │
                              ┌──────────────────────┘
                              ▼
                 ┌────────────────────────────────────────────┐
                 │          Spark Structured Streaming         │
                 │                                            │
                 │  ┌────────────┐  ┌────────────┐  ┌──────┐ │
                 │  │   Silver   │  │   Fraud    │  │Alerts│ │
                 │  │   Writer   │  │  Detection │  │ Sink │ │
                 │  └─────┬──────┘  └──────┬─────┘  └──┬───┘ │
                 └────────┼───────────────┼────────────┼─────┘
                          ▼               │            ▼
                 ┌──────────────┐         │   ┌──────────────┐
                 │   Iceberg    │         │   │   Iceberg    │
                 │   Silver     │         │   │   Gold       │
                 │  (MinIO S3)  │         │   │  (MinIO S3)  │
                 └──────┬───────┘         │   └──────┬───────┘
                        │    ┌────────────┘          │
                        ▼    ▼                       ▼
                 ┌──────────────┐          ┌──────────────────┐
                 │     dbt      │          │  DuckDB Serving  │
                 │  (DuckDB)    │          │  (Gold marts)    │
                 └──────┬───────┘          └──────┬───────────┘
                        └──────────┬──────────────┘
                                   ▼
                 ┌──────────────────────────────────────────┐
                 │          Evidence.dev Dashboard           │
                 │  Cashflow │ Fraud Alerts │ User Summary  │
                 └──────────────────────────────────────────┘
```

### Airflow Orchestration

Two DAGs run daily to keep the pipeline healthy:

```
02:00 UTC — finflow_daily_cashflow
            ├── dbt run  (Silver → Gold marts)
            └── Great Expectations validation (Gold fraud_alerts)

03:00 UTC — finflow_iceberg_maintenance
            ├── RewriteDataFiles  (compact small Parquet files)
            └── ExpireSnapshots   (reclaim storage, 7-day retention)
```

---

## Data Flow

| Stage | Location | Format | Written by |
|---|---|---|---|
| **Bronze** | `s3a://finflow-bronze/` | Raw JSON, partitioned `source/date` | FastAPI gateway |
| **Silver** | `s3a://finflow-silver/` | Iceberg (Parquet + metadata) | `silver_writer.py` |
| **Gold — fraud** | `s3a://finflow-gold/` | Iceberg | `alerts_sink.py` |
| **Gold — marts** | `finflow.duckdb` | DuckDB | dbt |

### PII Treatment per Stage

| Field | Ingestion (Bronze) | Silver | Gold |
|---|---|---|---|
| `user_id` | FPE tokenized (FF3-1) | — | Pseudonymized + RLS |
| `no_rekening` | FPE tokenized (FF3-1) | — | Pseudonymized + RLS |
| `email` | — | SHA-256 hashed | Pseudonymized |
| `phone` | — | Last 4 digits only | Pseudonymized |
| `full_name` | — | UUID mapping | Pseudonymized |

> **Important:** FPE keys must never change mid-pipeline. Key rotation requires a full stop + re-tokenization of existing data.

---

## Tech Stack

| Concern | Tool | Notes |
|---|---|---|
| Ingestion API | FastAPI + `aiokafka` | Async; `kafka-python` is banned |
| Data Contracts | Pydantic v2 (`schemas/`) | Single source of truth |
| Event Broker | Redpanda | Kafka-compatible, single binary |
| Object Storage | MinIO | Self-hosted S3; use `s3a://` URIs |
| Table Format | Apache Iceberg | ACID writes at Silver + Gold |
| Stream Processing | Spark Structured Streaming | 5-minute micro-batch trigger |
| Transformation | dbt (DuckDB adapter) | Reads Iceberg via MinIO |
| Data Quality | Great Expectations v1.x | Fluent API only |
| Dashboard | Evidence.dev | Static HTML from Markdown + SQL |
| Orchestration | Apache Airflow 2.11 | LocalExecutor, containerized |
| Secrets | HashiCorp Vault | Vault agent sidecar in prod |
| IaC | Terraform | OCI provider (prod) + Docker Compose (dev) |
| Monitoring | Prometheus + Grafana + Loki | |
| Lineage | OpenLineage + Marquez | Via Airflow 2.7+ and dbt-ol |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker | 24+ | Docker Compose v2 required |
| Python | 3.12+ | Managed via pyproject.toml |
| Java | 17 | Required by PySpark (JDK 17 or 21 only — see note) |
| Node.js | 18+ | For Evidence.dev dashboard only |

> **Java version note:** PySpark 3.5.x requires JDK 17 or JDK 21. JDK 24+ is NOT supported (Hadoop depends on `SecurityManager` which was removed). Install via [SDKMAN](https://sdkman.io/):
> ```bash
> sdk install java 17.0.19-tem
> ```

---

## Quick Start

### 1. Clone & install

```bash
git clone https://github.com/<your-user>/finflow.git
cd finflow
pip install -e ".[dev]"
```

### 2. Configure environment

```bash
# Copy the example .env and fill in values (see Environment Variables section)
cp .env.example .env
```

The defaults in `.env.example` work out of the box for local development. No changes needed unless you want to customize credentials.

### 3. Start infrastructure

```bash
make infra-up
```

This starts: Redpanda, MinIO (+ bucket init), Iceberg REST Catalog, Vault, Prometheus, Grafana, Loki, Marquez, Airflow (scheduler + webserver).

Wait ~30 seconds for all services to become healthy, then verify:

```bash
docker compose ps         # all services should be "healthy"
```

### 4. Initialize Airflow (first time only)

```bash
make airflow-init         # db migrate + create admin user (admin/admin)
```

### 5. Start the ingestion API

```bash
make dev                  # FastAPI at http://localhost:8000
```

### 6. Run the synthetic data generator

```bash
make simulate             # sends transactions to FastAPI for all 5 sources
```

### 7. Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8085 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| FastAPI Docs | http://localhost:8000/docs | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Marquez | http://localhost:5000 | — |

---

## Environment Variables

All secrets are stored in `.env` (gitignored). Copy from the example:

```bash
cp .env.example .env
```

Key variables:

| Variable | Description | Default |
|---|---|---|
| `MINIO_ACCESS_KEY` | MinIO root user / AWS access key ID | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO root password / AWS secret key | `minioadmin` |
| `FPE_KEY` | 32-char hex AES key for FF3-1 PII tokenization | *(test value)* |
| `FPE_TWEAK` | 14-char hex tweak for FF3-1 | *(test value)* |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Airflow metadata DB connection string | *(postgres)* |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Airflow session signing key | *(dev value)* |
| `ICEBERG_REST_URI` | Iceberg REST catalog URL | `http://localhost:8181` |
| `FINFLOW_API_KEY_BCA` | Per-source API key for BCA ingestion | `dev-key` |
| `JAVA_HOME` | Path to JDK 17 install | *(SDKMAN path)* |

> **Production:** All secrets are injected by the Vault agent sidecar — `.env` is never used in production.

---

## Infrastructure Services

All services are defined in `docker-compose.yml` with pinned image versions (no `latest` tags, per AGENTS.md rule #7).

### Redpanda Topics

| Topic | Producer | Consumer |
|---|---|---|
| `finflow.transactions.bca` | FastAPI | `silver_writer.py` |
| `finflow.transactions.mandiri` | FastAPI | `silver_writer.py` |
| `finflow.transactions.gopay` | FastAPI | `silver_writer.py` |
| `finflow.transactions.ovo` | FastAPI | `silver_writer.py` |
| `finflow.transactions.visa` | FastAPI | `silver_writer.py` |
| `finflow.fraud.alerts` | `fraud_detection.py` | `alerts_sink.py` |

### MinIO Buckets

| Bucket | Purpose |
|---|---|
| `finflow-bronze` | Raw JSON events from FastAPI |
| `finflow-silver` | Iceberg data files (Silver layer) |
| `finflow-gold` | Iceberg data files (Gold layer) |
| `finflow-checkpoints` | Spark Streaming checkpoint state |

---

## Airflow Orchestration

Airflow runs **containerized** (Docker-based), using a custom image that includes dbt, PySpark, Great Expectations, and DuckDB.

### Starting Airflow

```bash
make airflow-build        # build custom Docker image (only needed after Dockerfile changes)
make airflow-init         # initialize DB and create admin user (first time only)
make airflow-up           # start webserver + scheduler
make airflow-logs         # tail logs
make airflow-down         # stop Airflow services
```

### DAG Schedules

| DAG | Schedule | What it does |
|---|---|---|
| `finflow_daily_cashflow` | `0 2 * * *` (02:00 UTC) | dbt run → dbt test → GE validation |
| `finflow_iceberg_maintenance` | `0 3 * * *` (03:00 UTC) | Iceberg compaction + snapshot expiry |

### Airflow UI

Access at **http://localhost:8085** with credentials `admin` / `admin`.

Trigger a DAG manually:
1. Open the DAG in the UI
2. Click the ▶ **Trigger DAG** button (top right)

Or via CLI:
```bash
docker exec finflow-airflow-scheduler \
  airflow dags trigger finflow_daily_cashflow
```

### Rebuilding After Code Changes

The `dags/`, `jobs/`, `dbt/`, and `expectations/` directories are bind-mounted into the container at `/opt/finflow`. **Changes to Python files take effect immediately without rebuilding.**

A Docker rebuild is only needed when `docker/airflow/Dockerfile` or `pyproject.toml` changes:
```bash
docker compose build --no-cache airflow-scheduler airflow-webserver
docker compose up -d --force-recreate airflow-scheduler airflow-webserver
```

---

## Running Individual Components

### Spark Streaming Jobs

These jobs run continuously (5-minute micro-batch). Start them in separate terminals:

```bash
make spark-silver         # Redpanda → Bronze + Silver Iceberg
make spark-fraud          # Silver Iceberg → fraud detection → finflow.fraud.alerts
make spark-alerts         # finflow.fraud.alerts → Gold Iceberg
```

### Schema Registry

Register Pydantic schemas to Redpanda Schema Registry:

```bash
make register-schemas
```

---

## dbt Transformations

dbt reads Silver Iceberg tables via DuckDB's Iceberg extension and MinIO S3 API, then writes Gold marts to `finflow.duckdb`.

```bash
make dbt-run              # run all models
make dbt-test             # run dbt tests
make dbt-docs             # generate and serve docs at http://localhost:8080
```

### dbt Models

| Model | Grain | Source |
|---|---|---|
| `mart_monthly_cashflow` | Monthly | Silver transactions |
| `mart_user_summary` | Per-user | Silver transactions |
| `mart_fraud_alerts` | Per-alert | Gold fraud_alerts |

**Conventions:**
- Staging models: `view` materialization
- Mart models: `table` materialization
- All models tagged `config(tags=["v1"])`

---

## Evidence.dev Dashboard

The dashboard reads directly from `finflow.duckdb` and compiles to static HTML.

```bash
make dashboard-dev        # dev server at http://localhost:3000 (live reload)
make dashboard-build      # build static HTML to dashboard/.evidence/build/
```

Pages:
- `/` — Overview
- `/cashflow` — Monthly cashflow from `mart_monthly_cashflow`
- `/fraud` — Fraud alerts from `mart_fraud_alerts`
- `/user` — Per-user summary from `mart_user_summary`

---

## Testing

```bash
# Unit tests — no Docker needed
make test

# Integration tests — requires Docker stack running
make test-integration

# E2E tests — full stack required
make test-e2e

# All tests
make test-all
```

### Key test files

| File | What it tests |
|---|---|
| `tests/ingestion/test_pii.py` | FPE tokenization determinism — run after any `pii.py` change |
| `tests/ingestion/test_auth.py` | Per-source API key verification |
| `tests/ingestion/test_schemas.py` | Pydantic schema validation for all sources |
| `tests/spark/` | PySpark unit tests using shared `spark_session` fixture |
| `tests/e2e/` | Full pipeline via testcontainers |

---

## Project Structure

```
finflow/
├── ingestion/            # FastAPI webhook gateway
│   ├── main.py           # App entrypoint, lifecycle hooks
│   ├── auth.py           # Per-source API key verification
│   ├── pii.py            # FPE tokenization (FF3-1/AES-FFX)
│   └── producer.py       # aiokafka Redpanda producer
├── schemas/              # Data contracts — single source of truth
│   ├── v1/               # BaseTransaction + per-source subclasses
│   └── v2/               # New versions (never modify existing)
├── generator/            # Faker-based synthetic data generator
├── streaming/            # Spark Structured Streaming jobs
│   ├── silver_writer.py  # Redpanda → Iceberg Silver
│   ├── fraud_detection.py# Silver → velocity check → fraud.alerts topic
│   └── alerts_sink.py    # fraud.alerts topic → Iceberg Gold
├── jobs/                 # Batch PySpark jobs
│   └── iceberg_maintenance.py
├── dags/                 # Airflow DAGs
│   ├── finflow_daily_cashflow.py
│   └── finflow_iceberg_maintenance.py
├── dbt/                  # dbt project (DuckDB adapter)
│   ├── models/
│   │   ├── staging/
│   │   ├── marts/
│   │   └── dimensions/
│   └── profiles.yml
├── expectations/         # Great Expectations v1.x suites
│   └── gold_fraud_alerts.py
├── dashboard/            # Evidence.dev project
│   └── pages/
├── docker/
│   └── airflow/
│       └── Dockerfile    # Custom Airflow image (dbt + PySpark + GE + DuckDB)
├── terraform/            # IaC — OCI + local
├── tests/
├── docker-compose.yml
├── Makefile
├── .env.example          # ← commit this
├── .env                  # ← gitignored, never commit
└── AGENTS.md             # Full project spec for AI coding assistants
```

---

## Security & Secrets

- **`.env` is gitignored** — never commit it. Only `.env.example` (with placeholder values) is committed.
- **All credentials in `docker-compose.yml`** are referenced as `${VAR}` and read from `.env`.
- **In production**, secrets are injected by the HashiCorp Vault agent sidecar — `.env` is not used.
- **FPE keys** (`FPE_KEY`, `FPE_TWEAK`) must never change after data has been written to Bronze. Key rotation requires a full pipeline stop and re-tokenization of all existing records.
- **Do not log raw PII.** `user_id` and `no_rekening` are tokenized before they ever reach Redpanda.

If you suspect `.env` was accidentally committed:
```bash
git log --all --full-history -- .env
# If this shows commits, remove the history with git filter-repo
```

---

## Troubleshooting

### Airflow DAG fails immediately

Check the scheduler logs:
```bash
make airflow-logs
# or for a specific DAG run:
docker compose logs airflow-scheduler | grep ERROR
```

### Iceberg maintenance — `NoSuchMethodError` or `ClassNotFoundException`

This is a JAR version mismatch. The project uses a specific pinned stack:
- **PySpark 3.5.5** + **`iceberg-spark-runtime-3.5_2.12:1.8.1`**
- AWS SDK v2 **2.28.3** (needed for `S3FileIO.crossRegionAccessEnabled`)
- AWS SDK v1 **`aws-java-sdk-bundle:1.12.262`** (needed for Hadoop S3A)

Do not change these versions independently. If you update Iceberg, update all three together.

### Spark — `403 Forbidden` from MinIO

Iceberg's `S3FileIO` uses **separate config** from Hadoop S3A:
```python
# These are needed in addition to spark.hadoop.fs.s3a.* keys:
.config("spark.sql.catalog.finflow.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
.config("spark.sql.catalog.finflow.s3.endpoint", "http://minio:9000")
.config("spark.sql.catalog.finflow.s3.access-key-id", "minioadmin")
.config("spark.sql.catalog.finflow.s3.secret-access-key", "minioadmin")
.config("spark.sql.catalog.finflow.s3.path-style-access", "true")
```

### dbt — `Permission denied` on `finflow.duckdb`

The `finflow.duckdb` file is created in the project root. Ensure the Airflow container has write access via the bind mount (`./:/opt/finflow`). Check ownership:
```bash
ls -la finflow.duckdb
```

### Great Expectations — validation fails on empty table

GE validation includes a pre-flight check that skips validation when `mart_fraud_alerts` is empty (expected in a fresh dev environment). If you see validation failures, ensure the streaming pipeline has produced data first.

### MinIO not accessible

```bash
docker compose ps minio          # should be "healthy"
curl http://localhost:9000/minio/health/live  # should return 200
```

If MinIO is unhealthy, check disk space — MinIO requires at least a few GB free.

---

## License

MIT
