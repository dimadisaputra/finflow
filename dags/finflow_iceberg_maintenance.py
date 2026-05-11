"""FinFlow Iceberg Maintenance DAG.

Runs daily at 03:00 UTC — 1 hour after the cashflow DAG (02:00 UTC).
Performs RewriteDataFiles (compaction) and ExpireSnapshots on all
streaming-target Iceberg tables.

If the cashflow DAG runtime grows beyond 60 minutes, adjust this
schedule accordingly to maintain the gap.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "finflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="finflow_iceberg_maintenance",
    default_args=default_args,
    description="Daily Iceberg compaction and snapshot expiration",
    schedule="0 3 * * *",  # 03:00 UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finflow", "iceberg", "maintenance", "v1"],
) as dag:

    def run_iceberg_maintenance() -> None:
        """Execute Iceberg maintenance (compaction + snapshot expiration)."""
        from jobs.iceberg_maintenance import run

        run()

    maintenance = PythonOperator(
        task_id="iceberg_maintenance",
        python_callable=run_iceberg_maintenance,
    )
