"""Shared pytest fixtures for FinFlow tests.

Provides a session-scoped SparkSession fixture for PySpark tests.
Do NOT create SparkSession instances inside individual test functions —
always use this fixture.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Create a shared SparkSession for all tests in the session."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("finflow-test")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.finflow", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.finflow.type", "hadoop")
        .config("spark.sql.catalog.finflow.warehouse", "/tmp/finflow-test-warehouse")
        .getOrCreate()
    )
    yield spark
    spark.stop()
