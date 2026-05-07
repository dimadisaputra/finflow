"""Iceberg Maintenance — RewriteDataFiles + ExpireSnapshots.

The Silver writer produces ~288 Iceberg commits per day (5-min trigger × 24h).
Without compaction, dbt + DuckDB query performance degrades significantly
within days.  This job runs daily via the ``finflow_iceberg_maintenance``
Airflow DAG.

When adding a new Iceberg table that is written to via streaming,
add it to ``TABLES_TO_MAINTAIN`` (AGENTS.md rule #6).
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# All Iceberg tables that receive streaming writes and need daily maintenance.
# Add new streaming-target tables here.
TABLES_TO_MAINTAIN: list[str] = [
    "finflow.silver.transactions",
    "finflow.gold.fraud_alerts",
]

# Maintenance parameters
EXPIRE_OLDER_THAN_MS = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds
TARGET_FILE_SIZE_BYTES = 512 * 1024 * 1024  # 512 MB target file size


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Iceberg maintenance."""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    return (
        SparkSession.builder
        .appName("finflow-iceberg-maintenance")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.finflow", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.finflow.type", "rest")
        .config(
            "spark.sql.catalog.finflow.uri",
            os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest-catalog:8181"),
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def rewrite_data_files(spark: SparkSession, table: str) -> None:
    """Compact small data files into larger ones."""
    logger.info("Compacting data files for %s", table)
    spark.sql(f"""
        CALL finflow.system.rewrite_data_files(
            table => '{table}',
            options => map(
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'min-file-size-bytes', '{TARGET_FILE_SIZE_BYTES // 2}',
                'max-file-size-bytes', '{TARGET_FILE_SIZE_BYTES * 2}'
            )
        )
    """)
    logger.info("Compaction complete for %s", table)


def expire_snapshots(spark: SparkSession, table: str) -> None:
    """Expire old snapshots to reclaim storage."""
    logger.info("Expiring snapshots for %s (older than %d ms)", table, EXPIRE_OLDER_THAN_MS)
    spark.sql(f"""
        CALL finflow.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{_ms_ago_timestamp()}'
        )
    """)
    logger.info("Snapshot expiration complete for %s", table)


def _ms_ago_timestamp() -> str:
    """Return a timestamp string for ``EXPIRE_OLDER_THAN_MS`` ago."""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(tz=timezone.utc) - timedelta(milliseconds=EXPIRE_OLDER_THAN_MS)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def run() -> None:
    """Run maintenance on all registered Iceberg tables."""
    spark = create_spark_session()

    for table in TABLES_TO_MAINTAIN:
        try:
            rewrite_data_files(spark, table)
            expire_snapshots(spark, table)
        except Exception:
            logger.exception("Maintenance failed for table %s", table)
            raise

    spark.stop()
    logger.info("All maintenance tasks complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
