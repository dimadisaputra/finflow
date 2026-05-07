"""FinFlow data contracts — single source of truth for payload shape.

All transaction models are defined here and imported by both
`ingestion/` (gatekeeper) and `generator/` (compliant producer).
Never define payload structure inline in either module.
"""
