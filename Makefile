# FinFlow Makefile
# Dev workflow commands for the FinFlow pipeline.

.PHONY: help install dev test lint format \
        infra-up infra-down infra-logs \
        simulate ingest-api \
        dbt-run dbt-test dashboard-dev \
        spark-silver spark-fraud spark-alerts spark-maintenance \
        airflow-build airflow-init airflow-up airflow-down airflow-logs

# ── Spark Configuration ──────────────────────────────────────────
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1"

# ── Help ─────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'

# ── Python ───────────────────────────────────────────────────────
install: ## Install Python dependencies
	pip install -e ".[dev]"

dev: ## Start the FastAPI ingestion server (dev mode)
	set -a && source .env && set +a && uvicorn ingestion.main:app --reload --host 0.0.0.0 --port 8000

# ── Testing ──────────────────────────────────────────────────────
test: ## Run unit tests (no Docker needed)
	pytest tests/ingestion/ tests/spark/ -v

test-integration: ## Run integration tests (requires Docker stack)
	pytest tests/ -m integration -v

test-e2e: ## Run E2E tests (full Docker stack required)
	pytest tests/e2e/ -v

test-all: ## Run all tests
	pytest tests/ -v

# ── Code Quality ─────────────────────────────────────────────────
lint: ## Run linters
	ruff check .
	mypy ingestion/ schemas/ generator/ streaming/ jobs/

format: ## Auto-format code
	ruff check --fix .
	ruff format .

# ── Infrastructure ───────────────────────────────────────────────
infra-up: ## Start all infrastructure services
	docker compose up -d

infra-down: ## Stop all infrastructure services
	docker compose down

infra-logs: ## Tail infrastructure logs
	docker compose logs -f

infra-reset: ## Reset all infrastructure (WARNING: deletes data)
	docker compose down -v
	docker compose up -d

# ── Data Generation ──────────────────────────────────────────────
simulate: ## Run synthetic data generator
	set -a && source .env && set +a && python -m generator.simulate_sources

# ── Ingestion ────────────────────────────────────────────────────
ingest-api: dev ## Alias for starting the ingestion API

# ── Spark Streaming ──────────────────────────────────────────────
spark-silver: ## Run Silver writer streaming job
	spark-submit \
	  --packages $(SPARK_PACKAGES) \
	  streaming/silver_writer.py

spark-fraud: ## Run fraud detection streaming job
	spark-submit \
	  --packages $(SPARK_PACKAGES) \
	  streaming/fraud_detection.py

spark-alerts: ## Run alerts sink streaming job
	spark-submit \
	  --packages $(SPARK_PACKAGES) \
	  streaming/alerts_sink.py

spark-maintenance: ## Run Iceberg maintenance job
	spark-submit \
	  --packages $(SPARK_PACKAGES) \
	  jobs/iceberg_maintenance.py

# ── dbt ──────────────────────────────────────────────────────────
dbt-run: ## Run dbt transformations
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

dbt-docs: ## Generate and serve dbt docs
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ── Dashboard ────────────────────────────────────────────────────
dashboard-dev: ## Start Evidence.dev dashboard (dev mode)
	bash -c "cd dashboard && source ~/.nvm/nvm.sh && nvm use && npm install && npm run sources && npm run dev"

dashboard-build: ## Build Evidence.dev dashboard (static HTML)
	bash -c "cd dashboard && source ~/.nvm/nvm.sh && nvm use && npm install && npm run sources && npm run build"

# ── Schema Registry ──────────────────────────────────────────────
register-schemas: ## Export and register JSON schemas to Redpanda
	@echo "Registering schemas to Redpanda Schema Registry..."
	@for source in bca mandiri gopay ovo visa; do \
		echo "Registering $$source..."; \
		python -c "from schemas.v1.$${source} import *; import json; \
			cls = [c for c in dir() if c.endswith('Transaction') and c != 'BaseTransaction'][0]; \
			print(json.dumps(eval(cls).model_json_schema(), indent=2))" | \
		curl -s -X POST http://localhost:18081/subjects/finflow.transactions.$${source}-value/versions \
			-H 'Content-Type: application/vnd.schemaregistry.v1+json' \
			-d "{\"schema\": $$(cat -)}"; \
		echo ""; \
	done

# ── Airflow ──────────────────────────────────────────────────────
airflow-build: ## Build Airflow Docker image
	docker compose build airflow-init airflow-webserver airflow-scheduler

airflow-init: airflow-build ## Initialize Airflow (db migrate + create admin user)
	docker compose up airflow-init

airflow-up: ## Start Airflow webserver + scheduler
	docker compose up -d airflow-webserver airflow-scheduler
	@echo "Airflow UI: http://localhost:8085  (admin / admin)"

airflow-down: ## Stop Airflow services
	docker compose stop airflow-webserver airflow-scheduler

airflow-logs: ## Tail Airflow logs
	docker compose logs -f airflow-webserver airflow-scheduler

# ── Observability ─────────────────────────────────────────────────
obs-up: ## Start observability stack (Prometheus + Grafana + Loki + Promtail)
	docker compose up -d prometheus grafana loki promtail
	@echo ""
	@echo "  Grafana   →  http://localhost:3000  (admin / admin)"
	@echo "  Prometheus →  http://localhost:9090"
	@echo "  Loki      →  http://localhost:3100"

obs-down: ## Stop observability stack
	docker compose stop prometheus grafana loki promtail

obs-logs: ## Tail observability stack logs
	docker compose logs -f prometheus grafana loki promtail

obs-reload-prometheus: ## Hot-reload Prometheus config without restart
	curl -s -X POST http://localhost:9090/-/reload && echo "Prometheus reloaded"
