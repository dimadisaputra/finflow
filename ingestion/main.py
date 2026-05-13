"""FastAPI ingestion gateway — app entrypoint and lifecycle hooks.

This module defines the ``/ingest/{source}`` endpoint that receives
transaction payloads, validates them against ``schemas.registry.AnyTransaction``,
applies PII tokenization, and forwards them to the appropriate Redpanda topic
via ``aiokafka``.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException
from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator

from ingestion.auth import VALID_SOURCES, verify_source_token
from ingestion.pii import tokenize_pii
from ingestion.producer import get_producer, start_producer, stop_producer
from schemas.registry import AnyTransaction

logger = logging.getLogger(__name__)

# ── Custom Prometheus metrics ─────────────────────────────────────────────────
# These supplement the auto-generated metrics from instrumentator and power
# the per-source breakdown panels in the Grafana dashboard.

INGESTION_REQUESTS = Counter(
    "finflow_ingestion_requests_total",
    "Total ingestion requests received",
    labelnames=["source", "status"],
)

INGESTION_ERRORS = Counter(
    "finflow_ingestion_errors_total",
    "Total ingestion errors",
    labelnames=["source", "error_type"],
)

INGESTION_DURATION = Histogram(
    "finflow_ingestion_duration_seconds",
    "Ingestion request processing time",
    labelnames=["source"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)



@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifecycle — start/stop the Kafka producer."""
    await start_producer()
    logger.info("Kafka producer started")
    yield
    await stop_producer()
    logger.info("Kafka producer stopped")


app = FastAPI(
    title="FinFlow Ingestion Gateway",
    description="Receives transaction webhooks and forwards them to Redpanda.",
    version="0.1.0",
    lifespan=lifespan,
)

# Mount /metrics endpoint — scraped by Prometheus every 15s
Instrumentator().instrument(app).expose(app, include_in_schema=False)


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Basic health check endpoint."""
    return {"status": "healthy"}


@app.post("/ingest/{source}")
async def ingest_transaction(
    source: str,
    payload: AnyTransaction,
    _: None = Depends(verify_source_token),
) -> dict[str, str]:
    """Ingest a single transaction from a given source.

    1. Validates the source name.
    2. Applies FPE tokenization to PII fields.
    3. Publishes the sanitized payload to the source's Redpanda topic.
    """
    if source not in VALID_SOURCES:
        INGESTION_ERRORS.labels(source=source, error_type="unknown_source").inc()
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source '{source}'. Valid sources: {sorted(VALID_SOURCES)}",
        )

    start = time.perf_counter()
    try:
        # Tokenize PII fields before sending to Redpanda
        sanitized = tokenize_pii(payload.model_dump(mode="json"), source=source)

        # Publish to Redpanda
        producer = get_producer()
        topic = f"finflow.transactions.{source}"
        await producer.send_and_wait(
            topic=topic,
            value=sanitized,
            key=sanitized["transaction_id"],
        )

        logger.info(
            "Transaction %s ingested to %s",
            sanitized["transaction_id"],
            topic,
        )

        INGESTION_REQUESTS.labels(source=source, status="success").inc()
        return {
            "status": "accepted",
            "transaction_id": sanitized["transaction_id"],
        }

    except Exception as exc:
        INGESTION_ERRORS.labels(source=source, error_type=type(exc).__name__).inc()
        INGESTION_REQUESTS.labels(source=source, status="error").inc()
        logger.exception("Failed to ingest transaction for source '%s'", source)
        raise HTTPException(status_code=500, detail="Ingestion failed") from exc

    finally:
        INGESTION_DURATION.labels(source=source).observe(time.perf_counter() - start)
