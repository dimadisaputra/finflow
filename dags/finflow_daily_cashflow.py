"""FinFlow Daily Cashflow DAG — dbt + Great Expectations.

Runs daily at 02:00 UTC.  Executes dbt transformations to build Gold
mart tables, followed by Great Expectations validation on the results.

The 1-hour gap between this DAG (02:00) and the Iceberg maintenance
DAG (03:00) is intentional.  If this DAG's runtime grows beyond
60 minutes, adjust the maintenance schedule accordingly.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "finflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finflow_daily_cashflow",
    default_args=default_args,
    description="Daily dbt transformations + Great Expectations validation",
    schedule="0 2 * * *",  # 02:00 UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finflow", "dbt", "great_expectations", "v1"],
) as dag:

    # Step 1: Run dbt transformations
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/finflow/dbt && dbt run --profiles-dir . --target prod",
    )

    # Step 2: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/finflow/dbt && dbt test --profiles-dir . --target prod",
    )

    # Step 3: Run Great Expectations validation
    def run_great_expectations() -> None:
        """Execute GE v1.x validation suite for Gold fraud alerts."""
        from expectations.gold_fraud_alerts import run_validation

        run_validation()

    ge_validate = PythonOperator(
        task_id="great_expectations_validate",
        python_callable=run_great_expectations,
    )

    # DAG task dependencies
    dbt_run >> dbt_test >> ge_validate
