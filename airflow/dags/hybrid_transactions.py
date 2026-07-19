from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task


PROJECT = Path(os.environ.get("HYBRID_PROJECT_DIR", Path(__file__).resolve().parents[2]))


@dag(
    dag_id="hybrid_transactions",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    # Backfill SCD2 dates deliberately and in order; do not enqueue history on unpause.
    catchup=False,
    # SCD2 changes must commit in effective-date order.
    max_active_runs=1,
    tags=["trino", "delta", "scd2", "dbt"],
)
def hybrid_transactions():
    task_defaults = dict(
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=6),
        pool=os.getenv("HYBRID_AIRFLOW_POOL", "default_pool"),
    )

    def run_stage(stage: str, business_date: str) -> None:
        subprocess.run(
            [
                os.environ.get("PYTHON", "python"),
                str(PROJECT / "tools" / "run_partition.py"),
                "--date", business_date,
                "--stage", stage,
            ],
            cwd=PROJECT,
            check=True,
        )

    @task(task_id="customer", **task_defaults)
    def customer(business_date: str) -> None:
        run_stage("customer", business_date)

    @task(task_id="account", **task_defaults)
    def account(business_date: str) -> None:
        run_stage("account", business_date)

    @task(task_id="transactions", **task_defaults)
    def transactions(business_date: str) -> None:
        run_stage("transactions", business_date)

    customer_result = customer("{{ ds }}")
    account_result = account("{{ ds }}")
    transaction_result = transactions("{{ ds }}")
    customer_result >> account_result >> transaction_result


dag = hybrid_transactions()
