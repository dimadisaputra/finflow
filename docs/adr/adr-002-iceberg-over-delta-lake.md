# ADR-002: Apache Iceberg over Delta Lake

## Status
Accepted

## Date
2026-01-01

## Context
FinFlow requires a lakehouse table format for Silver and Gold layers that supports:
- ACID transactions for concurrent Spark Streaming writes
- Efficient time-travel and snapshot management
- DuckDB read compatibility for the serving layer

## Decision
Use **Apache Iceberg** as the table format for Silver and Gold layers.

## Rationale
1. **ACID transactions** — prevents data corruption from concurrent Spark Streaming writes
2. **REST catalog** — vendor-neutral catalog interface; no Hive Metastore dependency
3. **DuckDB support** — DuckDB can read Iceberg tables directly via the Iceberg extension
4. **Open format** — no vendor lock-in (vs. Delta Lake's Databricks affinity)
5. **Snapshot management** — built-in expiration and compaction procedures

## Consequences
- Must run daily Iceberg maintenance (compaction + snapshot expiration) to prevent query degradation
- Iceberg REST catalog adds one more service to manage
- Must ensure `spark.sql.streaming.checkpointFileManagerClass` uses MinIO S3-based checkpoint manager
