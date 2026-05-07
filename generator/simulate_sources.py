"""Async multi-source simulation runner.

Generates synthetic transactions across all configured sources and
sends them to the FinFlow ingestion API.  Imports models from
``schemas/`` to ensure generated data is always contract-compliant.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any

import httpx

from generator.templates.bca import generate_bca_transaction
from generator.templates.gopay import generate_gopay_transaction
from generator.templates.mandiri import generate_mandiri_transaction
from generator.templates.ovo import generate_ovo_transaction
from generator.templates.visa import generate_visa_transaction

logger = logging.getLogger(__name__)

# Source configuration — maps source name to its generator function and API key env var.
SOURCE_CONFIGS: dict[str, dict[str, Any]] = {
    "bca": {"generator": generate_bca_transaction, "api_key_env": "FINFLOW_API_KEY_BCA"},
    "mandiri": {"generator": generate_mandiri_transaction, "api_key_env": "FINFLOW_API_KEY_MANDIRI"},
    "gopay": {"generator": generate_gopay_transaction, "api_key_env": "FINFLOW_API_KEY_GOPAY"},
    "ovo": {"generator": generate_ovo_transaction, "api_key_env": "FINFLOW_API_KEY_OVO"},
    "visa": {"generator": generate_visa_transaction, "api_key_env": "FINFLOW_API_KEY_VISA"},
}

DEFAULT_INGESTION_URL = "http://localhost:8000"
DEFAULT_TPS = 5  # Transactions per second (total across all sources)
DEFAULT_DURATION_SECONDS = 60


async def send_transaction(
    client: httpx.AsyncClient,
    base_url: str,
    source: str,
    payload: dict[str, Any],
    api_key: str,
) -> None:
    """Send a single transaction to the ingestion API."""
    try:
        response = await client.post(
            f"{base_url}/ingest/{source}",
            json=payload,
            headers={"X-API-Key": api_key},
        )
        response.raise_for_status()
        logger.debug("Sent %s transaction: %s", source, payload.get("transaction_id"))
    except httpx.HTTPError:
        logger.exception("Failed to send %s transaction", source)


async def simulate(
    base_url: str = DEFAULT_INGESTION_URL,
    tps: int = DEFAULT_TPS,
    duration_seconds: int = DEFAULT_DURATION_SECONDS,
) -> None:
    """Run the multi-source simulation loop.

    Generates ``tps`` transactions per second, randomly distributed
    across all configured sources, for ``duration_seconds``.
    """
    import os

    sources = list(SOURCE_CONFIGS.keys())
    api_keys = {
        source: os.environ.get(config["api_key_env"], "dev-key")
        for source, config in SOURCE_CONFIGS.items()
    }

    total_transactions = tps * duration_seconds
    interval = 1.0 / tps

    logger.info(
        "Starting simulation: %d TPS × %ds = %d transactions",
        tps,
        duration_seconds,
        total_transactions,
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(total_transactions):
            source = random.choice(sources)
            generator = SOURCE_CONFIGS[source]["generator"]
            payload = generator()

            await send_transaction(client, base_url, source, payload, api_keys[source])
            await asyncio.sleep(interval)

            if (i + 1) % 100 == 0:
                logger.info("Progress: %d/%d transactions sent", i + 1, total_transactions)

    logger.info("Simulation complete: %d transactions sent", total_transactions)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(simulate())
