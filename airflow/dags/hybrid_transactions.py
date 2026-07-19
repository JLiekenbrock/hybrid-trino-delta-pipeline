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
    catchup=True,
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

    def run_stage(stage: str, ds: str) -> None:
        subprocess.run(
            [
                os.environ.get("PYTHON", "python"),
                str(PROJECT / "tools" / "run_partition.py"),
                "--date", ds,
                "--stage", stage,
            ],
            cwd=PROJECT,
            check=True,
        )

    @task(task_id="customer", **task_defaults)
    def customer(ds: str) -> None:
        run_stage("customer", ds)

    @task(task_id="account", **task_defaults)
    def account(ds: str) -> None:
        run_stage("account", ds)

    @task(task_id="transactions", **task_defaults)
    def transactions(ds: str) -> None:
        run_stage("transactions", ds)

    customer_result = customer("{{ ds }}")
    account_result = account("{{ ds }}")
    transaction_result = transactions("{{ ds }}")
    customer_result >> account_result >> transaction_result


dag = hybrid_transactions()
