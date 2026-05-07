"""Great Expectations v1.x validation suite for Gold fraud alerts.

Uses the GE v1.x Fluent API — NOT the v0.x API.  See AGENTS.md for details.

Validates the Gold fraud alerts table after dbt transformations to ensure
data quality before serving to the Evidence.dev dashboard.
"""

from __future__ import annotations

import logging

import great_expectations as gx

logger = logging.getLogger(__name__)


def run_validation() -> None:
    """Run GE validation on the Gold fraud alerts table.

    Uses the v1.x Fluent API:
    - ``gx.get_context()`` (not ``DataContext()``)
    - ``context.data_sources.add_sql(...)`` (not ``suite.expect_*``)
    """
    context = gx.get_context()

    # Add DuckDB data source
    data_source = context.data_sources.add_sql(
        name="finflow_gold",
        connection_string="duckdb:///finflow.duckdb",
    )

    # Add the fraud alerts table asset
    fraud_alerts_asset = data_source.add_table_asset(
        name="mart_fraud_alerts",
        table_name="mart_fraud_alerts",
    )

    # Create a batch request
    batch_request = fraud_alerts_asset.build_batch_request()

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
            data=batch_request,
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
