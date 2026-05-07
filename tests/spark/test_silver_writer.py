"""Silver writer PySpark unit tests.

Uses the shared ``spark_session`` fixture from ``tests/conftest.py``.
Do NOT create SparkSession instances inside individual test functions.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from streaming.silver_writer import SILVER_SCHEMA


class TestSilverSchema:
    """Tests for the Silver schema definition."""

    def test_schema_has_required_fields(self) -> None:
        """Verify the Silver schema contains all expected fields."""
        field_names = {f.name for f in SILVER_SCHEMA.fields}

        # Base fields
        assert "transaction_id" in field_names
        assert "user_id" in field_names
        assert "amount" in field_names
        assert "transaction_timestamp" in field_names
        assert "location_city" in field_names
        assert "source" in field_names
        assert "schema_version" in field_names
        assert "ingested_at" in field_names

        # Source-specific fields
        assert "bca_terminal_id" in field_names
        assert "mandiri_channel" in field_names
        assert "gopay_merchant_id" in field_names
        assert "ovo_merchant_name" in field_names
        assert "card_last_four" in field_names

    def test_required_fields_are_non_nullable(self) -> None:
        """Core fields should be non-nullable in the schema."""
        field_map = {f.name: f for f in SILVER_SCHEMA.fields}

        assert field_map["transaction_id"].nullable is False
        assert field_map["user_id"].nullable is False
        assert field_map["amount"].nullable is False
        assert field_map["source"].nullable is False

    def test_source_specific_fields_are_nullable(self) -> None:
        """Source-specific fields should be nullable."""
        field_map = {f.name: f for f in SILVER_SCHEMA.fields}

        assert field_map["bca_terminal_id"].nullable is True
        assert field_map["mandiri_channel"].nullable is True
        assert field_map["gopay_merchant_id"].nullable is True
        assert field_map["ovo_merchant_name"].nullable is True
        assert field_map["card_last_four"].nullable is True


class TestSilverDataFrame:
    """Tests for Silver DataFrame creation using the shared SparkSession."""

    def test_create_silver_dataframe(self, spark_session: SparkSession) -> None:
        """Verify we can create a DataFrame with the Silver schema."""
        data = [
            (
                "txn-001", "usr-001", Decimal("150000.00"),
                datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc),
                "Jakarta", "bca", "1.0", "TRANSFER",
                "0987654321", "BCA-TERM-0001",
                None, None, None, None, None, None, None, None,
                datetime.now(tz=timezone.utc),
            ),
        ]
        df = spark_session.createDataFrame(data, schema=SILVER_SCHEMA)
        assert df.count() == 1
        assert df.schema == SILVER_SCHEMA
