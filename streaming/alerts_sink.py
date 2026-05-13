"""Alerts Sink — finflow.fraud.alerts → Gold Iceberg table.
Consumes fraud alerts from the ``finflow.fraud.alerts`` Redpanda topic
and writes them to the Gold Iceberg table (``finflow.gold.fraud_alerts``).
This is the canonical data path for fraud alerts reaching the dashboard.
DuckDB must read from the Gold Iceberg table, never directly from
the Redpanda topic (AGENTS.md rule #5).
"""
from __future__ import annotations
import logging
import os
from streaming.metrics import attach_query_listener, mark_query_active, start_metrics_server
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
logger = logging.getLogger(__name__)
# Schema for fraud alert payloads from the Redpanda topic.
ALERT_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("txn_count", IntegerType(), nullable=False),
    StructField("total_amount", DecimalType(18, 2), nullable=False),
    StructField("transaction_ids", ArrayType(StringType()), nullable=True),
    StructField("sources", ArrayType(StringType()), nullable=True),
    StructField("cities", ArrayType(StringType()), nullable=True),
    StructField("alert_type", StringType(), nullable=False),
    StructField("detected_at", TimestampType(), nullable=False),
])
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
        .appName("finflow-alerts-sink")
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
    """Run the alerts sink streaming job."""
    spark = create_spark_session()
    
    # Start Prometheus metrics server on port 8003
    start_metrics_server(port=8003, job_name="alerts-sink")
    # Ensure table exists in the correct bucket (AGENTS.md rule)
    table_name = "finflow.gold.fraud_alerts"
    location = "s3a://finflow-gold/fraud_alerts"
    if not spark.catalog.tableExists(table_name):
        logger.info(f"Creating table {table_name} at {location}")
        spark.createDataFrame([], ALERT_SCHEMA).writeTo(table_name) \
            .tableProperty("location", location) \
            .create()
    bootstrap_servers = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
    # Read from Redpanda fraud alerts topic
    raw_alerts = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "finflow.fraud.alerts")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    # Parse JSON payloads
    parsed = (
        raw_alerts
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json(F.col("json_payload"), ALERT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Attach Prometheus listener BEFORE query starts to capture onQueryStarted
    attach_query_listener(spark, job_name="alerts-sink")

    # Write to Gold Iceberg table — NEVER raw Parquet
    checkpoint_location = "s3a://finflow-checkpoints/gold-fraud-alerts/"
    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="5 minutes")
        .option("checkpointLocation", checkpoint_location)
        .toTable("finflow.gold.fraud_alerts")
    )

    # Mark active after query starts (belt-and-suspenders with onQueryStarted)
    mark_query_active(job_name="alerts-sink")

    logger.info("Alerts sink started — awaiting termination")
    query.awaitTermination()
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
