# ADR-003: aiokafka over kafka-python

## Status
Accepted

## Date
2026-01-01

## Context
The FinFlow ingestion gateway uses FastAPI with async request handlers.
The Kafka producer library must be compatible with Python's asyncio event loop.

## Decision
Use **aiokafka** as the sole Kafka client library in the ingestion gateway.

## Rationale
1. **Async-native** — does not block the FastAPI event loop
2. **kafka-python is synchronous** — using it inside `async def` handlers blocks the event loop under concurrent load
3. **confluent-kafka is also synchronous** — same blocking issue
4. **Production-proven** — aiokafka is widely used in async Python Kafka applications

## Consequences
- `kafka-python` and `confluent-kafka` must NEVER be imported in async code
- The producer must be started in the app lifespan startup and stopped in shutdown
- Producer must never be instantiated per-request
