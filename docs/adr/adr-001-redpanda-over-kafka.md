# ADR-001: Redpanda over Apache Kafka

## Status
Accepted

## Date
2026-01-01

## Context
FinFlow needs a Kafka-compatible event broker for real-time transaction ingestion.
Apache Kafka is the industry standard but requires:
- ZooKeeper (or KRaft) for coordination
- Significant memory and disk resources
- Complex multi-node setup for development

## Decision
Use **Redpanda** as the event broker instead of Apache Kafka.

## Rationale
1. **Kafka API compatible** — all existing Kafka client libraries (including aiokafka) work without modification
2. **Single binary** — no ZooKeeper dependency, simpler deployment
3. **Lower resource footprint** — critical for OCI free tier dev environment (1 CPU, 1GB RAM)
4. **Built-in Schema Registry** — no need for Confluent Schema Registry
5. **ARM64 support** — required for OCI Ampere A1 instances

## Consequences
- Must verify Redpanda compatibility for any new Kafka client library
- Some Kafka-specific features (e.g. Kafka Streams) may not be available
- Team must document Redpanda-specific configuration quirks
