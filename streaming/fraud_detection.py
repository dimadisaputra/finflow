"""Fraud Detection — Silver → velocity check → finflow.fraud.alerts.

Reads from the Silver Iceberg table using Spark Structured Streaming,
applies velocity-based fraud detection rules (e.g. multiple transactions
from the same user in a short window), and writes fraud alerts to the
``finflow.fraud.alerts`` Redpanda topic.

Alerts must flow through ``alerts_sink.py`` before reaching DuckDB —
never read directly from Redpanda into DuckDB (AGENTS.md rule #5).
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

logger = logging.getLogger(__name__)

# Velocity detection parameters
WINDOW_DURATION = "10 minutes"
SLIDE_DURATION = "1 minute"
MAX_TRANSACTIONS_PER_WINDOW = 5  # Flag if user exceeds this
HIGH_AMOUNT_THRESHOLD = 10_000_000  # Flag single transactions above this amount (IDR)


def _is_local_dev() -> bool:
    """Detect if we are running in local development mode (outside Docker)."""
    return os.environ.get("FINFLOW_ENV", "local") == "local"


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Iceberg + MinIO + Redpanda."""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    local_dev = _is_local_dev()

    builder = (
        SparkSession.builder
        .appName("finflow-fraud-detection")
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
        .config("spark.sql.catalog.finflow.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.finflow.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.finflow.s3.secret-access-key", minio_secret_key)
        .config("spark.sql.catalog.finflow.s3.path-style-access", "true")
        .config("spark.sql.catalog.finflow.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    if not local_dev:
        builder = builder.config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "io.minio.spark.checkpoint.S3BasedCheckpointFileManager",
        )

    return builder.getOrCreate()


def run() -> None:
    """Run the fraud detection streaming job."""
    spark = create_spark_session()

    # Read from Silver Iceberg table as a stream
    silver_stream = (
        spark.readStream
        .format("iceberg")
        .load("finflow.silver.transactions")
    )

    # Velocity check — count transactions per user in a sliding window
    velocity_alerts = (
        silver_stream
        .withWatermark("transaction_timestamp", "15 minutes")
        .groupBy(
            F.window("transaction_timestamp", WINDOW_DURATION, SLIDE_DURATION),
            "user_id",
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.collect_list("transaction_id").alias("transaction_ids"),
            F.collect_list("source").alias("sources"),
            F.collect_list("location_city").alias("cities"),
        )
        .filter(F.col("txn_count") > MAX_TRANSACTIONS_PER_WINDOW)
        .select(
            F.col("user_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("txn_count"),
            F.col("total_amount"),
            F.col("transaction_ids"),
            F.col("sources"),
            F.col("cities"),
            F.lit("velocity").alias("alert_type"),
            F.current_timestamp().alias("detected_at"),
        )
    )

    # Write alerts to Redpanda topic
    bootstrap_servers = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
    checkpoint_location = "s3a://finflow-checkpoints/fraud-alerts/"

    query = (
        velocity_alerts.selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", "finflow.fraud.alerts")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="5 minutes")
        .start()
    )

    logger.info("Fraud detection started — awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
