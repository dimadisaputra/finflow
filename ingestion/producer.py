"""aiokafka Redpanda producer — lifecycle management.

The producer is started once during FastAPI's lifespan startup and stopped
during shutdown.  It must NEVER be instantiated per-request.

Uses ``aiokafka`` exclusively — never ``kafka-python`` or ``confluent-kafka``
in async FastAPI code.  See AGENTS.md critical rule #1.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


def _json_serializer(value: Any) -> bytes:
    """Serialize a Python dict to JSON bytes for Kafka."""
    return json.dumps(value, default=str).encode("utf-8")


def _key_serializer(key: Any) -> bytes | None:
    """Serialize a message key to bytes."""
    if key is None:
        return None
    return str(key).encode("utf-8")


async def start_producer() -> None:
    """Start the global Kafka producer.  Called during app startup."""
    global _producer

    bootstrap_servers = os.environ.get("REDPANDA_BROKERS", "localhost:9092")

    _producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=_json_serializer,
        key_serializer=_key_serializer,
        acks="all",
        enable_idempotence=True,
    )
    await _producer.start()
    logger.info("Kafka producer connected to %s", bootstrap_servers)


async def stop_producer() -> None:
    """Stop the global Kafka producer.  Called during app shutdown."""
    global _producer

    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped")


def get_producer() -> AIOKafkaProducer:
    """Return the global producer instance.

    Raises
    ------
    RuntimeError
        If the producer has not been started yet (startup not complete).
    """
    if _producer is None:
        raise RuntimeError(
            "Kafka producer is not initialized. "
            "Ensure the app lifespan startup has completed."
        )
    return _producer
