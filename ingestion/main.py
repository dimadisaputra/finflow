"""FastAPI ingestion gateway — app entrypoint and lifecycle hooks.

This module defines the ``/ingest/{source}`` endpoint that receives
transaction payloads, validates them against ``schemas.registry.AnyTransaction``,
applies PII tokenization, and forwards them to the appropriate Redpanda topic
via ``aiokafka``.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException

from ingestion.auth import verify_source_token
from ingestion.pii import tokenize_pii
from ingestion.producer import get_producer, start_producer, stop_producer
from schemas.registry import AnyTransaction

logger = logging.getLogger(__name__)

# Valid source names — must match Redpanda topic suffixes.
VALID_SOURCES: set[str] = {"bca", "mandiri", "gopay", "ovo", "visa"}


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
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source '{source}'. Valid sources: {sorted(VALID_SOURCES)}",
        )

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

    return {
        "status": "accepted",
        "transaction_id": sanitized["transaction_id"],
    }
