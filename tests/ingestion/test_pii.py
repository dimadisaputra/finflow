"""FPE determinism tests for PII tokenization.

Always run these tests when modifying ``ingestion/pii.py``.
A regression here would silently break fraud detection aggregations
because the same user would produce different tokens.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest


# Test FPE key and tweak (safe for testing only — never use in production)
TEST_FPE_KEY = "EF4359D8D580AA4F7F036D6F04FC6A94"
TEST_FPE_TWEAK = "D8E7920AFA330A73"


@pytest.fixture(autouse=True)
def _set_fpe_env() -> None:
    """Set FPE environment variables for testing."""
    with patch.dict(os.environ, {
        "FPE_KEY": TEST_FPE_KEY,
        "FPE_TWEAK": TEST_FPE_TWEAK,
    }):
        # Clear the cached cipher so each test gets fresh env vars
        from ingestion.pii import _get_cipher
        _get_cipher.cache_clear()
        yield
        _get_cipher.cache_clear()


class TestFPEDeterminism:
    """Verify that FPE tokenization is deterministic."""

    def test_same_input_produces_same_output(self) -> None:
        """The same user_id must always produce the same token."""
        from ingestion.pii import tokenize_pii

        payload1 = {"user_id": "1234567890", "no_rekening": "0987654321", "other": "data"}
        payload2 = {"user_id": "1234567890", "no_rekening": "0987654321", "other": "data"}

        result1 = tokenize_pii(payload1, source="bca")
        result2 = tokenize_pii(payload2, source="bca")

        assert result1["user_id"] == result2["user_id"]
        assert result1["no_rekening"] == result2["no_rekening"]

    def test_different_input_produces_different_output(self) -> None:
        """Different user_ids must produce different tokens."""
        from ingestion.pii import tokenize_pii

        payload1 = {"user_id": "1234567890", "other": "data"}
        payload2 = {"user_id": "0987654321", "other": "data"}

        result1 = tokenize_pii(payload1, source="bca")
        result2 = tokenize_pii(payload2, source="bca")

        assert result1["user_id"] != result2["user_id"]

    def test_non_pii_fields_unchanged(self) -> None:
        """Fields not in the PII list should pass through unchanged."""
        from ingestion.pii import tokenize_pii

        payload = {
            "user_id": "1234567890",
            "transaction_id": "txn-001",
            "amount": "150000.00",
            "source": "bca",
        }

        result = tokenize_pii(payload, source="bca")

        assert result["transaction_id"] == "txn-001"
        assert result["amount"] == "150000.00"
        assert result["source"] == "bca"

    def test_none_pii_fields_skipped(self) -> None:
        """None-valued PII fields should not be tokenized."""
        from ingestion.pii import tokenize_pii

        payload = {"user_id": "1234567890", "no_rekening": None}

        result = tokenize_pii(payload, source="bca")

        assert result["no_rekening"] is None
        assert result["user_id"] != "1234567890"  # Should be tokenized

    def test_empty_string_pii_fields_skipped(self) -> None:
        """Empty string PII fields should not be tokenized."""
        from ingestion.pii import tokenize_pii

        payload = {"user_id": "1234567890", "no_rekening": ""}

        result = tokenize_pii(payload, source="bca")

        assert result["no_rekening"] == ""
