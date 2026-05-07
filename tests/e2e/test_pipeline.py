"""End-to-end pipeline test using testcontainers.

Requires Docker Compose stack to be running.  Tests the full data path:
  Generator → Ingestion API → Redpanda → Silver Writer → Iceberg

Run with:
    docker compose up -d
    pytest tests/e2e/ -v
"""

from __future__ import annotations

import pytest


@pytest.mark.e2e
class TestPipeline:
    """End-to-end pipeline tests."""

    @pytest.mark.skip(reason="Requires Docker Compose stack — run manually")
    def test_transaction_flows_to_silver(self) -> None:
        """Verify a transaction flows from ingestion to Silver Iceberg.

        TODO: Implement with testcontainers:
        1. Start Redpanda + MinIO + Iceberg REST catalog containers
        2. Send a BCA transaction to the ingestion API
        3. Run the silver_writer for one micro-batch
        4. Query the Silver Iceberg table and verify the record exists
        """
        raise NotImplementedError("E2E test not yet implemented")

    @pytest.mark.skip(reason="Requires Docker Compose stack — run manually")
    def test_fraud_alert_flows_to_gold(self) -> None:
        """Verify fraud alerts flow from detection to Gold Iceberg.

        TODO: Implement with testcontainers:
        1. Insert velocity-triggering transactions into Silver
        2. Run fraud_detection for one micro-batch
        3. Run alerts_sink for one micro-batch
        4. Query the Gold Iceberg table and verify the alert exists
        """
        raise NotImplementedError("E2E test not yet implemented")
