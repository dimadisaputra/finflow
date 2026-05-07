# FinFlow Makefile
# Dev workflow commands for the FinFlow pipeline.

.PHONY: help install dev test lint format \
        infra-up infra-down infra-logs \
        simulate ingest-api \
        dbt-run dbt-test dashboard-dev \
        spark-silver spark-fraud spark-alerts spark-maintenance

# ── Help ─────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'

# ── Python ───────────────────────────────────────────────────────
install: ## Install Python dependencies
	pip install -e ".[dev]"

dev: ## Start the FastAPI ingestion server (dev mode)
	uvicorn ingestion.main:app --reload --host 0.0.0.0 --port 8000

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
	python -m generator.simulate_sources

# ── Ingestion ────────────────────────────────────────────────────
ingest-api: dev ## Alias for starting the ingestion API

# ── Spark Streaming ──────────────────────────────────────────────
spark-silver: ## Run Silver writer streaming job
	spark-submit streaming/silver_writer.py

spark-fraud: ## Run fraud detection streaming job
	spark-submit streaming/fraud_detection.py

spark-alerts: ## Run alerts sink streaming job
	spark-submit streaming/alerts_sink.py

spark-maintenance: ## Run Iceberg maintenance job
	spark-submit jobs/iceberg_maintenance.py

# ── dbt ──────────────────────────────────────────────────────────
dbt-run: ## Run dbt transformations
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

dbt-docs: ## Generate and serve dbt docs
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ── Dashboard ────────────────────────────────────────────────────
dashboard-dev: ## Start Evidence.dev dashboard (dev mode)
	cd dashboard && npm install && npm run dev

dashboard-build: ## Build Evidence.dev dashboard (static HTML)
	cd dashboard && npm run build

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
