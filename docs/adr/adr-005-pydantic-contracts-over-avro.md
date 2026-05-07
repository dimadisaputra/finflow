# ADR-005: Pydantic Contracts over Avro

## Status
Accepted

## Date
2026-01-01

## Context
FinFlow needs a data contract system for transaction payloads that is:
- Shared between the ingestion gateway and the synthetic data generator
- Version-aware with backward compatibility
- Easy to validate and serialize

## Decision
Use **Pydantic v2 models** in the `schemas/` directory as the single source
of truth for payload shape, with Pydantic's discriminated union for version
dispatch.

## Rationale
1. **Python-native** — no code generation step; models are plain Python classes
2. **Pydantic v2 performance** — Rust-based validation core; fast enough for hot-path ingestion
3. **Discriminated unions** — automatic version dispatch based on `schema_version` field
4. **JSON Schema export** — Pydantic models can export JSON Schema for Redpanda Schema Registry
5. **Shared codebase** — both `ingestion/` and `generator/` import from `schemas/`

## Consequences
- Schema evolution requires creating new version directories (`v2/`, `v3/`)
- Existing versions must never be modified after data has been written to Bronze
- JSON Schema must be registered to Redpanda Schema Registry for broker-level awareness
- Avro could be reconsidered if cross-language producers are needed in the future
