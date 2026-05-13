"""Prometheus metrics exporter for Spark Structured Streaming jobs.

Starts a lightweight HTTP server on a configurable port that exposes
Prometheus metrics scraped from Spark's StreamingQueryListener API.

Metrics exposed:
- finflow_spark_streaming_status        (gauge)  — 1 = active, 0 = inactive
- finflow_spark_streaming_input_rows    (counter) — total input rows processed
- finflow_spark_streaming_processed_rows_per_second (gauge)
- finflow_spark_streaming_input_rows_per_second     (gauge)
- finflow_spark_streaming_batch_duration_seconds     (histogram)
- finflow_spark_streaming_last_batch_timestamp       (gauge)

Usage in a streaming job:
    from streaming.metrics import start_metrics_server, attach_query_listener

    spark = create_spark_session()
    start_metrics_server(port=8001, job_name="silver-writer")
    attach_query_listener(spark, job_name="silver-writer")   # BEFORE query start
    ...
    query = parsed.writeStream.format("iceberg")...start()
    mark_query_active(job_name="silver-writer")              # AFTER query start
    query.awaitTermination()
"""

from __future__ import annotations

import logging
import time
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

logger = logging.getLogger(__name__)

# ── Shared registry (one per process) ─────────────────────────────
REGISTRY = CollectorRegistry()

# Use "spark_job" as label name to avoid conflict with Prometheus's
# built-in "job" label (which is set to the scrape job name).
_LABEL = "spark_job"

# ── Metrics ───────────────────────────────────────────────────────

STREAMING_STATUS = Gauge(
    "finflow_spark_streaming_status",
    "Whether the streaming query is active (1) or inactive (0)",
    labelnames=[_LABEL],
    registry=REGISTRY,
)

INPUT_ROWS_TOTAL = Counter(
    "finflow_spark_streaming_input_rows_total",
    "Total number of input rows processed across all batches",
    labelnames=[_LABEL],
    registry=REGISTRY,
)

PROCESSED_ROWS_PER_SEC = Gauge(
    "finflow_spark_streaming_processed_rows_per_second",
    "Current rate of rows being processed",
    labelnames=[_LABEL],
    registry=REGISTRY,
)

INPUT_ROWS_PER_SEC = Gauge(
    "finflow_spark_streaming_input_rows_per_second",
    "Current rate of rows arriving at the source",
    labelnames=[_LABEL],
    registry=REGISTRY,
)

BATCH_DURATION = Histogram(
    "finflow_spark_streaming_batch_duration_seconds",
    "Duration of each micro-batch in seconds",
    labelnames=[_LABEL],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
    registry=REGISTRY,
)

LAST_BATCH_TIMESTAMP = Gauge(
    "finflow_spark_streaming_last_batch_timestamp",
    "Unix timestamp of the last completed micro-batch",
    labelnames=[_LABEL],
    registry=REGISTRY,
)

NUM_INPUT_ROWS = Gauge(
    "finflow_spark_streaming_batch_input_rows",
    "Number of input rows in the last micro-batch",
    labelnames=[_LABEL],
    registry=REGISTRY,
)

BATCH_ID = Gauge(
    "finflow_spark_streaming_batch_id",
    "ID of the last completed micro-batch",
    labelnames=[_LABEL],
    registry=REGISTRY,
)


def start_metrics_server(port: int, job_name: str) -> None:
    """Start the Prometheus HTTP metrics server in a background thread.

    Args:
        port: TCP port to expose /metrics on. Convention:
              - silver_writer:    8001
              - fraud_detection:  8002
              - alerts_sink:      8003
        job_name: Label value for the ``spark_job`` metric label.
    """
    # Initialize status to 0 (not yet active)
    STREAMING_STATUS.labels(**{_LABEL: job_name}).set(0)

    start_http_server(port, registry=REGISTRY)
    logger.info("Prometheus metrics server started on port %d (spark_job=%s)", port, job_name)


def mark_query_active(job_name: str) -> None:
    """Manually mark a streaming query as active.

    Call this AFTER the streaming query is started, since the
    StreamingQueryListener may have been attached before the query
    started and missed the onQueryStarted event, or in PySpark
    the listener may not fire onQueryStarted reliably.
    """
    STREAMING_STATUS.labels(**{_LABEL: job_name}).set(1)
    logger.info("Streaming query marked active (spark_job=%s)", job_name)


class FinFlowQueryListener(StreamingQueryListener):
    """StreamingQueryListener that pushes micro-batch stats to Prometheus.

    Spark fires onQueryProgress after each micro-batch completes, giving
    us access to inputRowsPerSecond, processedRowsPerSecond, batchDuration,
    numInputRows, etc.
    """

    def __init__(self, job_name: str) -> None:
        super().__init__()
        self.job_name = job_name

    def onQueryStarted(self, event: Any) -> None:
        """Called when the streaming query starts."""
        STREAMING_STATUS.labels(**{_LABEL: self.job_name}).set(1)
        logger.info(
            "[metrics] Query started: id=%s name=%s",
            event.id,
            event.name,
        )

    def onQueryProgress(self, event: Any) -> None:
        """Called after each micro-batch completes."""
        progress = event.progress

        # Core throughput metrics
        input_rows_per_sec = progress.inputRowsPerSecond or 0.0
        processed_rows_per_sec = progress.processedRowsPerSecond or 0.0
        num_input_rows = progress.numInputRows or 0
        batch_id = progress.batchId

        kw = {_LABEL: self.job_name}
        INPUT_ROWS_PER_SEC.labels(**kw).set(input_rows_per_sec)
        PROCESSED_ROWS_PER_SEC.labels(**kw).set(processed_rows_per_sec)
        INPUT_ROWS_TOTAL.labels(**kw).inc(num_input_rows)
        NUM_INPUT_ROWS.labels(**kw).set(num_input_rows)
        BATCH_ID.labels(**kw).set(batch_id)
        LAST_BATCH_TIMESTAMP.labels(**kw).set(time.time())

        # Batch duration — durationMs is a dict with keys like
        # 'addBatch', 'triggerExecution', etc.
        duration_ms = progress.durationMs
        if duration_ms:
            # Use 'triggerExecution' as the overall batch time
            total_ms = duration_ms.get("triggerExecution", 0)
            BATCH_DURATION.labels(**kw).observe(total_ms / 1000.0)

        logger.info(
            "[metrics] batch=%d rows=%d in=%.1f/s proc=%.1f/s",
            batch_id,
            num_input_rows,
            input_rows_per_sec,
            processed_rows_per_sec,
        )

    def onQueryTerminated(self, event: Any) -> None:
        """Called when the streaming query terminates."""
        STREAMING_STATUS.labels(**{_LABEL: self.job_name}).set(0)
        logger.info(
            "[metrics] Query terminated: id=%s",
            event.id,
        )


def attach_query_listener(spark: SparkSession, job_name: str) -> None:
    """Attach the FinFlow metrics listener to the Spark session.

    Call this BEFORE starting the streaming query so that the listener
    can capture the onQueryStarted event.

    Args:
        spark: The active SparkSession.
        job_name: Label value for the ``spark_job`` metric label.
    """
    listener = FinFlowQueryListener(job_name=job_name)
    spark.streams.addListener(listener)
    logger.info("FinFlowQueryListener attached (spark_job=%s)", job_name)
