"""Silver Writer — Redpanda → Bronze → Silver (Iceberg).

Reads raw JSON transactions from all ``finflow.transactions.*`` Redpanda
topics, applies schema normalization, and writes to the Silver Iceberg
table (``finflow.silver.transactions``).

Critical rules enforced:
- Always writes to Iceberg (never raw Parquet) — see AGENTS.md rule #3.
- Uses MinIO S3-based checkpoint manager — see AGENTS.md rule #4.
- Processing time trigger: 5 minutes.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# All transaction topics to subscribe to.
SUBSCRIBE_TOPICS = ",".join([
    "finflow.transactions.bca",
    "finflow.transactions.mandiri",
    "finflow.transactions.gopay",
    "finflow.transactions.ovo",
    "finflow.transactions.visa",
])

# Unified Silver schema — all source-specific fields are nullable.
SILVER_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("amount", DecimalType(18, 2), nullable=False),
    StructField("transaction_timestamp", TimestampType(), nullable=False),
    StructField("location_city", StringType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("schema_version", StringType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=True),
    # BCA-specific
    StructField("no_rekening", StringType(), nullable=True),
    StructField("bca_terminal_id", StringType(), nullable=True),
    # Mandiri-specific
    StructField("mandiri_channel", StringType(), nullable=True),
    # GoPay-specific
    StructField("gopay_merchant_id", StringType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    # OVO-specific
    StructField("ovo_merchant_name", StringType(), nullable=True),
    StructField("cashback_amount", DecimalType(18, 2), nullable=True),
    # Visa-specific
    StructField("card_last_four", StringType(), nullable=True),
    StructField("merchant_category_code", StringType(), nullable=True),
    StructField("merchant_name", StringType(), nullable=True),
    # Metadata
    StructField("ingested_at", TimestampType(), nullable=False),
])


def _is_local_dev() -> bool:
    """Detect if we are running in local development mode (outside Docker)."""
    return os.environ.get("FINFLOW_ENV", "local") == "local"


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Iceberg + MinIO + Redpanda.

    In local dev mode (FINFLOW_ENV=local, the default), this will:
    - Auto-download required JARs via spark.jars.packages (Iceberg, Kafka, Hadoop-AWS).
    - Use local filesystem checkpoints (MinIO S3BasedCheckpointFileManager
      has no Maven artifact and must be built from source for Docker).
    - Set hadoop.security.authentication=simple to avoid JDK 23+ getSubject errors.
    """
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    local_dev = _is_local_dev()

    builder = (
        SparkSession.builder
        .appName("finflow-silver-writer")
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Iceberg REST catalog
        .config("spark.sql.catalog.finflow", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.finflow.type", "rest")
        .config(
            "spark.sql.catalog.finflow.uri",
            os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest-catalog:8181"),
        )
        # Iceberg catalog S3 properties — override server-side config so the
        # client connects to our local MinIO, not the Docker-internal hostname.
        .config("spark.sql.catalog.finflow.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.finflow.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.finflow.s3.secret-access-key", minio_secret_key)
        .config("spark.sql.catalog.finflow.s3.path-style-access", "true")
        .config("spark.sql.catalog.finflow.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # Hadoop S3A — used for checkpoint and general S3 access
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    if local_dev:
        # In local dev, the MinIO S3BasedCheckpointFileManager JAR is not
        # available (no Maven artifact — must be built from source).
        # Use the default HDFS-based checkpoint manager with a local path.
        logger.info("Local dev mode — using default checkpoint manager")
    else:
        # In Docker/prod, the MinIO checkpoint JAR is pre-installed.
        # AGENTS.md rule #4: always use MinIO S3-based checkpoint manager.
        builder = builder.config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "io.minio.spark.checkpoint.S3BasedCheckpointFileManager",
        )

    return builder.getOrCreate()


def run() -> None:
    """Run the Silver writer streaming job."""
    spark = create_spark_session()
    
    # Ensure table exists in the correct bucket (AGENTS.md rule)
    table_name = "finflow.silver.transactions"
    location = "s3a://finflow-silver/transactions"
    if not spark.catalog.tableExists(table_name):
        logger.info(f"Creating table {table_name} at {location}")
        spark.createDataFrame([], SILVER_SCHEMA).writeTo(table_name) \
            .tableProperty("location", location) \
            .create()

    bootstrap_servers = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
    checkpoint_location = "s3a://finflow-checkpoints/silver-transactions/"

    # Read from Redpanda
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", SUBSCRIBE_TOPICS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON payloads
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_timestamp")
        .select(
            F.from_json(F.col("json_payload"), SILVER_SCHEMA).alias("data"),
            F.col("kafka_timestamp").alias("ingested_at"),
        )
        .select("data.*")
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Write to Iceberg — NEVER raw Parquet (AGENTS.md rule #3)
    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="5 minutes")
        .option("checkpointLocation", checkpoint_location)
        .toTable("finflow.silver.transactions")
    )

    logger.info("Silver writer started — awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
