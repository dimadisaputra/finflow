"""Great Expectations v1.x validation suite for Gold fraud alerts.

Uses the GE v1.x Fluent API — NOT the v0.x API.  See AGENTS.md for details.

Validates the Gold fraud alerts table after dbt transformations to ensure
data quality before serving to the Evidence.dev dashboard.
"""

from __future__ import annotations

import logging
import os

import duckdb
import great_expectations as gx

logger = logging.getLogger(__name__)


def run_validation() -> None:
    """Run GE validation on the Gold fraud alerts table.

    Uses the v1.x Fluent API:
    - ``gx.get_context()`` (not ``DataContext()``)
    - ``context.data_sources.add_sql(...)`` (not ``suite.expect_*``)

    In dev environments, the fraud alerts table may be empty (no streaming
    pipeline data yet).  The validation skips gracefully in that case.
    """
    # Resolve DuckDB path — works both locally and inside Docker
    finflow_home = os.environ.get("FINFLOW_HOME", ".")
    duckdb_path = os.path.join(finflow_home, "finflow.duckdb")

    # Pre-flight check: skip if table is empty (expected in dev before
    # the streaming pipeline has produced fraud alerts).
    conn = duckdb.connect(duckdb_path, read_only=True)
    try:
        result = conn.sql("SELECT COUNT(*) FROM mart_fraud_alerts").fetchone()
        row_count = result[0] if result is not None else 0
    except duckdb.CatalogException:
        logger.warning("Table mart_fraud_alerts does not exist yet — skipping validation")
        return
    finally:
        conn.close()

    if row_count == 0:
        logger.warning(
            "mart_fraud_alerts is empty — skipping GE validation "
            "(expected in dev before streaming pipeline produces fraud alerts)"
        )
        return

    logger.info("mart_fraud_alerts has %d rows — running GE validation", row_count)

    context = gx.get_context()

    # Add DuckDB data source
    data_source = context.data_sources.add_sql(
        name="finflow_gold",
        connection_string=f"duckdb:///{duckdb_path}",
    )

    # Add the fraud alerts table asset
    fraud_alerts_asset = data_source.add_table_asset(
        name="mart_fraud_alerts",
        table_name="mart_fraud_alerts",
    )

    # Create a batch definition (GE v1.x requires BatchDefinition, not BatchRequest)
    batch_definition = fraud_alerts_asset.add_batch_definition_whole_table(
        name="mart_fraud_alerts_batch",
    )

    # Define expectations
    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="gold_fraud_alerts_suite")
    )

    # user_id should never be null
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id")
    )

    # alert_type should be one of known types
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="alert_type",
            value_set=["velocity", "high_amount", "geo_anomaly"],
        )
    )

    # txn_count should always be positive
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="txn_count",
            min_value=1,
        )
    )

    # detected_at should never be null
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="detected_at")
    )

    # Create and run validation definition
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="gold_fraud_alerts_validation",
            data=batch_definition,
            suite=expectation_suite,
        )
    )

    # Run validation
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="gold_fraud_alerts_checkpoint",
            validation_definitions=[validation_definition],
        )
    )

    result = checkpoint.run()

    if not result.success:
        logger.error("Gold fraud alerts validation FAILED")
        raise RuntimeError("Great Expectations validation failed for Gold fraud alerts")

    logger.info("Gold fraud alerts validation PASSED")
