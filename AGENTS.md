# AGENTS.md — FinFlow

This file provides guidance for AI coding assistants (Claude, Cursor, Copilot, etc.) working on this codebase. Read this before making any changes.

---

## Project Overview

**FinFlow** is a personal finance aggregation and fraud detection pipeline. It ingests synthetic transactions from multiple sources (bank transfers, e-wallets, credit cards), processes them through a Medallion architecture (Bronze → Silver → Gold), and serves near-real-time fraud alerts and daily cashflow reports via a static dashboard.

This is a **portfolio project**. The codebase is pre-implementation — architecture is designed, code scaffolds are being built. Prioritize correctness and clarity over brevity.

---

## Repository Structure

```
finflow/
├── ingestion/                  # FastAPI webhook gateway
│   ├── main.py                 # App entrypoint, lifecycle hooks
│   ├── auth.py                 # Per-source API key verification
│   ├── pii.py                  # FPE tokenization (FF3-1/AES-FFX)
│   └── producer.py             # aiokafka Redpanda producer
│
├── schemas/                    # Data contracts — single source of truth for payload shape
│   ├── v1/
│   │   ├── base.py             # BaseTransaction — shared fields across all sources
│   │   ├── bca.py              # BCATransaction(BaseTransaction)
│   │   ├── mandiri.py          # MandiriTransaction(BaseTransaction)
│   │   ├── gopay.py            # GoPayTransaction(BaseTransaction)
│   │   ├── ovo.py              # OVOTransaction(BaseTransaction)
│   │   └── visa.py             # VisaTransaction(BaseTransaction)
│   └── v2/                     # Add new versions here — never modify existing ones
│       └── bca.py              # Example: BCATransactionV2(BCATransaction) with new fields
│
├── generator/                  # Faker-based synthetic data
│   ├── simulate_sources.py     # Async multi-source simulation runner
│   └── templates/              # Per-source Faker generators — import from schemas/
│
├── streaming/                  # Spark Structured Streaming jobs
│   ├── silver_writer.py        # Redpanda → Bronze → Silver (Iceberg)
│   ├── fraud_detection.py      # Silver → velocity check → finflow.fraud.alerts
│   └── alerts_sink.py          # finflow.fraud.alerts → Gold Iceberg table
│
├── jobs/                       # Batch PySpark jobs
│   └── iceberg_maintenance.py  # RewriteDataFiles + ExpireSnapshots
│
├── dags/                       # Airflow DAGs
│   ├── finflow_daily_cashflow.py       # dbt + GE, runs 02:00 daily
│   └── finflow_iceberg_maintenance.py  # Iceberg compaction, runs 03:00 daily
│
├── dbt/                        # dbt project (DuckDB adapter)
│   ├── models/
│   │   ├── staging/            # Silver Iceberg → staging views
│   │   ├── marts/
│   │   │   ├── mart_monthly_cashflow.sql
│   │   │   ├── mart_user_summary.sql
│   │   │   └── mart_fraud_alerts.sql   # Gold reporting layer
│   │   └── dimensions/         # dim_user, dim_source, dim_time
│   └── tests/
│
├── expectations/               # Great Expectations v1.x suites
│   └── gold_fraud_alerts.py
│
├── dashboard/                  # Evidence.dev project
│   ├── evidenceproject.json    # Evidence.dev project config (auto-generated, commit it)
│   ├── sources/
│   │   └── finflow.duckdb.yaml # DuckDB source connection config
│   └── pages/
│       ├── index.md            # Landing page / overview
│       ├── cashflow.md         # Monthly cashflow mart
│       ├── fraud.md            # Fraud alerts mart
│       └── user.md             # Per-user summary
│
├── features/                   # ML feature extraction (future scope)
│   └── fraud_features.py
│
├── terraform/                  # IaC — OCI + local
│   ├── modules/
│   │   ├── networking/
│   │   ├── storage/            # MinIO, Iceberg REST catalog, buckets
│   │   ├── streaming/          # Redpanda, topics, schema registry
│   │   ├── compute/            # Airflow, Spark, FastAPI, DuckDB
│   │   ├── security/           # Vault, OPA, TLS
│   │   └── observability/      # Prometheus, Grafana, Loki, Marquez
│   └── environments/
│       ├── local/
│       ├── dev/
│       └── prod/
│
├── tests/
│   ├── ingestion/              # FastAPI + PII unit tests
│   ├── spark/                  # PySpark unit tests (pytest + SparkSession fixture)
│   └── e2e/                    # testcontainers-based end-to-end tests
│
├── alerts/
│   └── finflow-alerts.yml      # Prometheus alerting rules
│
├── docs/
│   └── adr/                    # Architecture Decision Records
│
├── docker-compose.yml
└── Makefile
```

---

## Tech Stack — Quick Reference

| Concern | Tool | Notes |
|---|---|---|
| Ingestion API | FastAPI | Async; uses `aiokafka` — never `kafka-python` |
| Data contracts | Pydantic v2 + `schemas/` | Single source of truth; imported by `ingestion/` and `generator/` |
| Event broker | Redpanda | Kafka-compatible; single binary; no ZooKeeper |
| Object storage | MinIO | Self-hosted S3; not AWS S3 — use `s3a://` URIs |
| Table format | Apache Iceberg | Silver + Gold fraud_alerts; REST catalog |
| Stream processing | Spark Structured Streaming | Trigger: `processingTime="5 minutes"` |
| Batch processing | PySpark | Backfill and maintenance jobs |
| Transformation | dbt (DuckDB adapter) | Reads Iceberg via MinIO S3 API |
| Serving | DuckDB (dev) / MotherDuck (prod) | Gold mart `.duckdb` file |
| Orchestration | Apache Airflow | Two DAGs: cashflow (02:00) + maintenance (03:00) |
| Data quality | dbt tests + Great Expectations v1.x | GE v1.x API — not v0.x |
| Dashboard | Evidence.dev | Lives in `dashboard/`; reads from `.duckdb`; static HTML output |
| Secrets | HashiCorp Vault | Never hardcode credentials |
| IaC | Terraform | OCI provider for prod; Docker Compose for local |
| Monitoring | Prometheus + Grafana + Loki | |
| Lineage | OpenLineage + Marquez | Integrated via Airflow >2.7 and dbt-ol |

---

## Critical Rules

These rules exist because of known failure modes in this stack. Do not bypass them.

### 1. Always use `aiokafka` — never `kafka-python` or `confluent-kafka` directly

`kafka-python` and `confluent-kafka` are synchronous and blocking. Using them inside FastAPI's `async def` handlers will block the event loop under concurrent load.

```python
# ✅ CORRECT
from aiokafka import AIOKafkaProducer

# ❌ WRONG — blocks the event loop
from kafka import KafkaProducer
```

The producer must be started in `@app.on_event("startup")` and stopped in `@app.on_event("shutdown")`. Never instantiate it per-request.

### 2. FPE tokenization must be deterministic — never reload the key mid-run

The `user_id` and `no_rekening` fields are tokenized using FF3-1 (AES-FFX) before entering Redpanda. The streaming fraud detection jobs aggregate by `user_id` using sliding window `GROUP BY`. If the cipher key changes between messages, the same user will produce different tokens and velocity checks will silently fail.

```python
# ✅ CORRECT — load once at startup, cache forever
@lru_cache(maxsize=1)
def _get_cipher() -> FF3Cipher:
    key = os.environ["FPE_KEY"]
    tweak = os.environ["FPE_TWEAK"]
    return FF3Cipher.withCustomAlphabet(key, tweak, alphabet="0123456789")

# ❌ WRONG — creates a new cipher per call, potentially with a different key
def tokenize(value):
    cipher = FF3Cipher(get_key_from_vault(), ...)
    return cipher.encrypt(value)
```

Key rotation requires a full pipeline stop + re-tokenization of existing data. See `docs/pii-key-lifecycle.yml`.

### 3. Never write raw Parquet to Silver — always write to Iceberg

Concurrent Spark Streaming writes to plain Parquet will corrupt data. Iceberg provides ACID transactions at the Silver layer.

```python
# ✅ CORRECT
.writeStream.format("iceberg")...

# ❌ WRONG
.writeStream.format("parquet")...
```

### 4. Always use the MinIO S3-based checkpoint manager for Spark Streaming jobs

POSIX-emulated checkpoints on MinIO cause emulation overhead and can corrupt state on restart.

```python
# ✅ CORRECT — include this config in every SparkSession for streaming jobs
.config(
    "spark.sql.streaming.checkpointFileManagerClass",
    "io.minio.spark.checkpoint.S3BasedCheckpointFileManager"
)
```

### 5. Fraud alerts must flow through `alerts_sink.py` — not read directly from Redpanda into DuckDB

DuckDB does not efficiently consume Kafka/Redpanda topics for dashboarding. The canonical data path for fraud alerts is:

```
fraud_detection.py → Redpanda (finflow.fraud.alerts)
    → alerts_sink.py → Iceberg (finflow.gold.fraud_alerts)
    → DuckDB → Evidence.dev
```

Never attempt to wire DuckDB or dbt directly to the Redpanda topic.

### 6. Iceberg maintenance must run daily

The Silver writer produces 288 Iceberg commits per day. Without compaction, dbt + DuckDB query performance degrades significantly within days. The `finflow_iceberg_maintenance` DAG handles this. If you add a new Iceberg table that is written to via streaming, add it to the maintenance job.

```python
# jobs/iceberg_maintenance.py — add new tables here
TABLES_TO_MAINTAIN = [
    "finflow.silver.transactions",
    "finflow.gold.fraud_alerts",
    # add new streaming-target tables here
]
```

### 7. Never use `latest` tag for Docker images in Terraform

Always pin to a specific release tag. Example: `minio/minio:RELEASE.2024-03-15T01-07-19Z`. Unpinned images break reproducibility across environments.

### 8. All credentials come from Vault — never from environment files or source code

Secrets (MinIO credentials, FPE keys, DB passwords) are injected by the Vault agent sidecar at pod startup as environment variables. Do not create `.env` files with real credentials and do not hardcode anything.

---

## Iceberg Catalog Configuration

All Spark jobs must use the REST catalog. The catalog name is `finflow`.

```python
spark = SparkSession.builder \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.finflow",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.finflow.type", "rest") \
    .config("spark.sql.catalog.finflow.uri", "http://iceberg-rest-catalog:8181") \
    ...
```

Table naming convention: `finflow.<layer>.<table_name>`

| Layer | Example |
|---|---|
| Silver | `finflow.silver.transactions` |
| Gold — fraud | `finflow.gold.fraud_alerts` |

---

## Redpanda Topics

| Topic | Producer | Consumer(s) |
|---|---|---|
| `finflow.transactions.bca` | FastAPI ingestion | `silver_writer.py` |
| `finflow.transactions.mandiri` | FastAPI ingestion | `silver_writer.py` |
| `finflow.transactions.gopay` | FastAPI ingestion | `silver_writer.py` |
| `finflow.transactions.ovo` | FastAPI ingestion | `silver_writer.py` |
| `finflow.transactions.visa` | FastAPI ingestion | `silver_writer.py` |
| `finflow.fraud.alerts` | `fraud_detection.py` | `alerts_sink.py` |

When adding a new source, add the topic to both `terraform/modules/streaming/topics.tf` and the `subscribe` option in `silver_writer.py`.

---

## MinIO Buckets & S3 Paths

| Bucket | Purpose |
|---|---|
| `finflow-bronze` | Raw JSON, partitioned by `source/date` |
| `finflow-silver` | Iceberg data files for Silver tables |
| `finflow-gold` | Iceberg data files for Gold tables |
| `finflow-checkpoints` | Spark Streaming checkpoint state |

Checkpoint paths within `finflow-checkpoints`:

| Job | Checkpoint path |
|---|---|
| `silver_writer.py` | `s3a://finflow-checkpoints/silver-transactions/` |
| `fraud_detection.py` | `s3a://finflow-checkpoints/fraud-alerts/` |
| `alerts_sink.py` | `s3a://finflow-checkpoints/gold-fraud-alerts/` |

---

## Airflow DAG Schedule

| DAG | Schedule | Depends on |
|---|---|---|
| `finflow_daily_cashflow` | `0 2 * * *` (02:00 UTC) | Silver Iceberg data |
| `finflow_iceberg_maintenance` | `0 3 * * *` (03:00 UTC) | `finflow_daily_cashflow` completes |

The 1-hour gap between cashflow (02:00) and maintenance (03:00) is intentional. If the cashflow DAG runtime grows beyond 60 minutes, adjust the maintenance schedule accordingly.

---

## dbt Conventions

- Adapter: DuckDB (dev), MotherDuck (prod)
- Source: Iceberg Silver tables read via MinIO S3 API
- Model tags: `config(tags=["v1"])` on all models in this version
- Materialization defaults: `view` for staging, `table` for marts
- All mart models must have a corresponding dbt test file

```sql
-- Naming: mart_<domain>_<grain>.sql
-- Example:
--   mart_monthly_cashflow.sql    ← monthly grain
--   mart_user_summary.sql        ← per-user grain
--   mart_fraud_alerts.sql        ← per-alert grain (Gold reporting layer)
```

---

## Great Expectations Version

This project uses **GE v1.x (Fluent API)**. The v0.x API (`DataContext`, `suite.expect_*`, `SimpleCheckpoint`) is incompatible and must not be used.

```python
# ✅ v1.x — correct
import great_expectations as gx
context = gx.get_context()
data_source = context.data_sources.add_sql(...)

# ❌ v0.x — wrong
from great_expectations.data_context import DataContext
context = DataContext()
```

---

## PII Field Reference

| Field | Layer | Treatment |
|---|---|---|
| `no_rekening` | Ingestion | FPE tokenized (FF3-1) before Redpanda |
| `user_id` | Ingestion | FPE tokenized (FF3-1) before Redpanda |
| `email` | Silver | SHA-256 hashed |
| `phone` | Silver | Masked — last 4 digits only |
| `full_name` | Silver | Pseudonymized (UUID mapping) |
| Any PII field | Gold | Pseudonymized + RLS-controlled |

Never log raw PII values. Never pass raw PII in exception messages.

---

## Testing

### Running tests locally

```bash
# Unit tests (no external services needed)
pytest tests/ingestion/ tests/spark/ -v

# Integration tests (requires Docker Compose stack)
docker compose up -d
pytest tests/ -m integration -v

# E2E tests (full stack)
pytest tests/e2e/ -v
```

### Spark tests

Use the shared `spark_session` pytest fixture in `tests/conftest.py`. Do not create `SparkSession` instances inside individual test functions.

```python
# tests/conftest.py provides:
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()
```

### FPE determinism test

Always run `tests/ingestion/test_pii.py` when modifying `ingestion/pii.py`. The test verifies that the same input always produces the same token — a regression here would silently break fraud detection aggregations.

---

## OCI Free Tier Constraints (Dev Environment)

When writing or modifying configuration for the `dev` environment, respect these constraints:

| Component | Dev config | Why |
|---|---|---|
| MinIO | Standalone (1 node) | No erasure coding — not HA |
| Redpanda | `--smp=1 --memory=1G` | Single node, no replication |
| Spark | `--executor-memory 2g` | Limited RAM on Ampere A1 |
| Airflow | `LocalExecutor` | No Redis/Celery dependency |

Do not use `distributed_mode = true` in `terraform/environments/dev/terraform.tfvars`.

All Docker images must have an `arm64` / `linux/arm64` build available. Verify before adding a new image dependency. MinIO, Redpanda, and official Spark images all support ARM64.

---

## Adding a New Transaction Source

1. Create `schemas/v1/<source>.py` — subclass `BaseTransaction`, add source-specific fields
2. Add the new model to `schemas/registry.py` → `AnyTransaction` union
3. Register the JSON Schema export to Redpanda Schema Registry
4. Add the source name to `valid_sources` in `ingestion/main.py`
5. Create a Faker generator in `generator/templates/<source>.py` — import model from `schemas/`
6. Add the topic `finflow.transactions.<source>` in `terraform/modules/streaming/topics.tf`
7. Add the topic to the `subscribe` list in `streaming/silver_writer.py`
8. Add source-specific schema normalization in the PySpark Silver writer
9. Add the source to `SOURCE_CONFIGS` in `generator/simulate_sources.py`
10. Add schema tests in `tests/ingestion/test_schemas.py`
11. Run the full E2E test suite to confirm the new source flows end-to-end

---

## Data Contracts (`schemas/`)

The `schemas/` directory is the **single source of truth** for payload shape. Both `ingestion/` (gatekeeper) and `generator/` (compliant producer) import from here. Never define payload structure inline in either module.

### Inheritance pattern

Every source model inherits from `BaseTransaction`. Only source-specific fields go in the subclass.

```python
# schemas/v1/base.py
from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import datetime

class BaseTransaction(BaseModel):
    transaction_id: str
    user_id: str                          # FPE-tokenized before this reaches the gateway
    amount: Decimal = Field(ge=0, decimal_places=2)
    transaction_timestamp: datetime
    location_city: str
    source: str
    schema_version: str                   # Required — used for version dispatch

# schemas/v1/bca.py
from schemas.v1.base import BaseTransaction
from typing import Literal

class BCATransaction(BaseTransaction):
    source: Literal["bca"] = "bca"
    schema_version: Literal["1.0"] = "1.0"   # Discriminator field
    bca_terminal_id: str | None = None
    transaction_type: str                     # e.g. "TRANSFER", "PAYMENT"

# schemas/v2/bca.py — extend v1, never modify v1
from schemas.v1.bca import BCATransaction as BCATransactionV1

class BCATransaction(BCATransactionV1):
    schema_version: Literal["2.0"] = "2.0"
    merchant_category: str | None = None      # New field in v2
```

### Version dispatch in FastAPI (Pydantic v2 Discriminated Union)

The ingestion endpoint accepts a single `AnyTransaction` union type. Pydantic reads `schema_version` from the payload and dispatches validation to the correct model automatically — no `if/else` needed.

```python
# schemas/registry.py
from typing import Annotated, Union
from pydantic import Field
from schemas.v1.bca import BCATransaction as BCAV1
from schemas.v1.gopay import GoPayTransaction as GoPayV1
from schemas.v1.visa import VisaTransaction as VisaV1
from schemas.v2.bca import BCATransaction as BCAV2

# Pydantic v2 discriminated union — dispatches on schema_version field
AnyTransaction = Annotated[
    Union[BCAV2, BCAV1, GoPayV1, VisaV1],  # More specific versions first
    Field(discriminator="schema_version")
]
```

```python
# ingestion/main.py
from schemas.registry import AnyTransaction

@app.post("/ingest/{source}")
async def ingest_transaction(
    source: str,
    payload: AnyTransaction,              # Pydantic dispatches to correct model
    _: None = Depends(verify_source_token)
):
    # payload is already validated and typed at this point
    sanitized = tokenize_pii(payload.model_dump(), source=source)
    await producer.send(
        topic=f"finflow.transactions.{source}",
        value=sanitized,
        key=sanitized.get("transaction_id")
    )
    return {"status": "accepted", "transaction_id": sanitized.get("transaction_id")}
```

### Schema versioning rules

- **Never modify** an existing version's model after data has been written to Bronze. Bronze is immutable.
- **Always create a new version** (`v2/`) for any field addition, rename, or type change.
- New versions inherit from the previous version's model and only override what changed.
- The `schema_version` field in the payload is the source of truth — it travels all the way to Bronze and is used by PySpark for schema-aware normalization.
- Register the exported JSON Schema to Redpanda Schema Registry for broker-level awareness:

```bash
# Export Pydantic model to JSON Schema and register to Redpanda Schema Registry
python -c "
from schemas.v1.bca import BCATransaction
import json
print(json.dumps(BCATransaction.model_json_schema(), indent=2))
" | curl -X POST http://redpanda:8081/subjects/finflow.transactions.bca-value/versions \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "{\"schema\": $(cat -)}"
```

### Adding a new schema version

1. Create `schemas/v<N>/<source>.py`, inheriting from the previous version
2. Update `schemas/registry.py` — add the new model to `AnyTransaction` union (more specific first)
3. Add a test in `tests/ingestion/test_schemas.py` covering the new fields
4. Register the new JSON Schema to Redpanda Schema Registry
5. Update the PySpark Silver writer if the new fields need to be stored in Silver

---

## Evidence.dev Dashboard (`dashboard/`)

The `dashboard/` directory is an Evidence.dev project. Evidence compiles `.md` files with embedded SQL into a static HTML site.

### How it works

Evidence queries DuckDB directly at build time. Pages are Markdown files with SQL code fences.

```markdown
<!-- dashboard/pages/fraud.md -->
# Fraud Alerts

```sql fraud_summary
SELECT
    DATE_TRUNC('day', detected_at) AS alert_date,
    COUNT(*) AS alert_count,
    AVG(risk_score) AS avg_risk_score
FROM mart_fraud_alerts
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30
```

<LineChart data={fraud_summary} x="alert_date" y="alert_count" />
```

### DuckDB source config

```yaml
# dashboard/sources/finflow.duckdb.yaml
name: finflow
type: duckdb
filename: ../../finflow.duckdb   # Relative path to Gold mart file
```

### Local development

```bash
cd dashboard
npm install
npm run dev       # http://localhost:3000 — live reload on SQL/MD changes
```

### Build and deploy

```bash
# Dev — builds to dashboard/.evidence/build/, deploys to GitHub Pages
npm run build

# Prod — output goes to OCI Object Storage + CDN (via CI/CD)
npm run build
# Upload .evidence/build/ to OCI bucket via Terraform or GitHub Actions
```

### Conventions

- One page per mart: `cashflow.md` → `mart_monthly_cashflow`, `fraud.md` → `mart_fraud_alerts`
- SQL query names (the label after the fence) must be unique per page — they become JS variable names
- Do not put business logic in dashboard SQL — transformations belong in dbt models
- `evidenceproject.json` is auto-generated by Evidence on first run. Commit it — it pins the Evidence version

---

## Architecture Decision Records

Significant decisions are documented in `/docs/adr/`. Before proposing a change to a core component (broker, table format, serving layer), check whether an ADR already covers it.

Key ADRs to be written (not yet created):
- `adr-001-redpanda-over-kafka.md`
- `adr-002-iceberg-over-delta-lake.md`
- `adr-003-aiokafka-over-kafka-python.md`
- `adr-004-alerts-sink-pattern.md`
- `adr-005-pydantic-contracts-over-avro.md`

---

## What Not to Do

- **Do not use `kafka-python` or `confluent-kafka` in async FastAPI code** — use `aiokafka`
- **Do not write Parquet directly at Silver** — always Iceberg
- **Do not skip Iceberg maintenance** when adding streaming-target tables
- **Do not hardcode secrets** — everything from Vault
- **Do not use GE v0.x API** — this project is on v1.x
- **Do not pin Docker images to `latest`** in Terraform
- **Do not rotate the FPE key mid-pipeline** — requires full re-tokenization
- **Do not read from `finflow.fraud.alerts` Redpanda topic in dbt or DuckDB directly** — use `alerts_sink.py` → Iceberg Gold path
- **Do not define payload shape inline in `ingestion/` or `generator/`** — always import from `schemas/`
- **Do not modify an existing schema version** after data has landed in Bronze — create a new version
- **Do not put transformation logic in `dashboard/` SQL** — belongs in dbt models