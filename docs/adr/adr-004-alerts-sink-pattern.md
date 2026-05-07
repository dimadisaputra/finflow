# ADR-004: Alerts Sink Pattern

## Status
Accepted

## Date
2026-01-01

## Context
Fraud alerts produced by `fraud_detection.py` need to reach the Evidence.dev
dashboard via DuckDB.  DuckDB cannot efficiently consume Kafka/Redpanda topics
for real-time dashboarding.

## Decision
Route fraud alerts through a dedicated **alerts sink** streaming job that
writes from the Redpanda topic to a Gold Iceberg table.

## Data Path
```
fraud_detection.py → Redpanda (finflow.fraud.alerts)
    → alerts_sink.py → Iceberg (finflow.gold.fraud_alerts)
    → DuckDB → Evidence.dev
```

## Rationale
1. **DuckDB limitations** — DuckDB is not designed for streaming consumption
2. **Iceberg benefits** — provides ACID writes, time-travel, and efficient reads
3. **Decoupling** — separates detection logic from serving logic
4. **Auditability** — Gold Iceberg table provides full history of alerts

## Consequences
- Never attempt to wire DuckDB or dbt directly to the Redpanda topic
- `alerts_sink.py` must be added to Iceberg maintenance job
- Adds ~5 minutes of latency between alert detection and dashboard visibility
