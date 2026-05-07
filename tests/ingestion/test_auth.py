"""Authentication tests for the ingestion gateway."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client() -> TestClient:
    """Create a test client with mock environment variables."""
    with patch.dict(os.environ, {
        "FINFLOW_API_KEY_BCA": "test-bca-key",
        "FINFLOW_API_KEY_MANDIRI": "test-mandiri-key",
        "FINFLOW_API_KEY_GOPAY": "test-gopay-key",
        "FINFLOW_API_KEY_OVO": "test-ovo-key",
        "FINFLOW_API_KEY_VISA": "test-visa-key",
        "REDPANDA_BROKERS": "localhost:9092",
        "FPE_KEY": "EF4359D8D580AA4F7F036D6F04FC6A94",
        "FPE_TWEAK": "D8E7920AFA330A73",
    }):
        from ingestion.main import app
        yield TestClient(app, raise_server_exceptions=False)


class TestHealthCheck:
    """Tests for the health check endpoint."""

    def test_health_returns_200(self, client: TestClient) -> None:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestAuthentication:
    """Tests for API key authentication."""

    def test_missing_api_key_returns_422(self, client: TestClient) -> None:
        """Request without X-API-Key header should be rejected."""
        response = client.post(
            "/ingest/bca",
            json={
                "transaction_id": "txn-001",
                "user_id": "1234567890",
                "amount": "100.00",
                "transaction_timestamp": "2026-01-01T00:00:00Z",
                "location_city": "Jakarta",
                "source": "bca",
                "schema_version": "1.0",
                "transaction_type": "TRANSFER",
            },
        )
        assert response.status_code == 422

    def test_invalid_api_key_returns_401(self, client: TestClient) -> None:
        """Request with wrong API key should be rejected."""
        response = client.post(
            "/ingest/bca",
            json={
                "transaction_id": "txn-001",
                "user_id": "1234567890",
                "amount": "100.00",
                "transaction_timestamp": "2026-01-01T00:00:00Z",
                "location_city": "Jakarta",
                "source": "bca",
                "schema_version": "1.0",
                "transaction_type": "TRANSFER",
            },
            headers={"X-API-Key": "wrong-key"},
        )
        assert response.status_code == 401

    def test_unknown_source_returns_400(self, client: TestClient) -> None:
        """Request to unknown source should return 400."""
        response = client.post(
            "/ingest/unknown_bank",
            json={
                "transaction_id": "txn-001",
                "user_id": "1234567890",
                "amount": "100.00",
                "transaction_timestamp": "2026-01-01T00:00:00Z",
                "location_city": "Jakarta",
                "source": "unknown_bank",
                "schema_version": "1.0",
                "transaction_type": "TRANSFER",
            },
            headers={"X-API-Key": "test-key"},
        )
        assert response.status_code == 400
