from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, dag, task


PROJECT = Path(os.environ.get("HYBRID_PROJECT_DIR", Path(__file__).resolve().parents[2]))


@dag(
    dag_id="hybrid_transactions",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    # Backfill SCD2 dates deliberately and in order; do not enqueue history on unpause.
    catchup=False,
    # SCD2 changes must commit in effective-date order.
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "customer_segment": Param(None, type=["null", "string"], title="Customer segment"),
        "account_status": Param(None, type=["null", "string"], title="Account status"),
        "minimum_transaction_amount": Param(0, type="number", minimum=0, title="Minimum transaction amount"),
        "tenant_ids": Param(
            [],
            type="array",
            items={"type": "string", "minLength": 1},
            maxItems=400,
            title="Tenant IDs",
            description="Empty processes all tenants; otherwise only the listed tenants are read and written.",
        ),
        "fetch_size": Param(100_000, type="integer", minimum=1, maximum=1_000_000, title="Trino fetch size"),
        "window": Param(
            "daily",
            type="string",
            enum=["daily", "weekly", "monthly"],
            title="Processing window",
            description="Weekly windows start Monday; monthly windows start on day one.",
        ),
        "run_dbt_tests": Param(True, type="boolean", title="Run post-publication dbt tests"),
    },
    tags=["trino", "delta", "scd2", "dbt"],
)
def hybrid_transactions():
    task_defaults = dict(
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=6),
        pool=os.getenv("HYBRID_AIRFLOW_POOL", "default_pool"),
    )

    def run_stage(
        stage: str, business_date: str, query_parameters: dict, fetch_size: int, window: str
    ) -> dict:
        completed = subprocess.run(
            [
                os.environ.get("PYTHON", "python"),
                str(PROJECT / "tools" / "run_partition.py"),
                "--date", business_date,
                "--stage", stage,
                "--query-params", json.dumps(query_parameters),
                "--fetch-size", str(fetch_size),
                "--window", window,
            ],
            cwd=PROJECT,
            check=True,
            capture_output=True,
            text=True,
        )
        print(completed.stdout)
        return json.loads(completed.stdout)

    @task(task_id="customer", **task_defaults)
    def customer(business_date: str, query_parameters: dict, fetch_size: int, window: str) -> dict:
        return run_stage("customer", business_date, query_parameters, fetch_size, window)

    @task(task_id="account", **task_defaults)
    def account(business_date: str, query_parameters: dict, fetch_size: int, window: str) -> dict:
        return run_stage("account", business_date, query_parameters, fetch_size, window)

    @task(task_id="transactions", **task_defaults)
    def transactions(business_date: str, query_parameters: dict, fetch_size: int, window: str) -> dict:
        return run_stage("transactions", business_date, query_parameters, fetch_size, window)

    @task(
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=1),
        pool=os.getenv("HYBRID_AIRFLOW_POOL", "default_pool"),
    )
    def dbt_tests(model: str, stage_result: dict, enabled: bool) -> None:
        if not enabled:
            print("Post-publication dbt tests disabled for this run")
            return
        subprocess.run(
            [
                os.environ.get("PYTHON", "python"),
                str(PROJECT / "tools" / "validate_and_rollback.py"),
                "--model", model,
                "--stage-result", json.dumps(stage_result),
                "--project-dir", str(PROJECT),
                "--profiles-dir", str(PROJECT),
            ],
            cwd=PROJECT,
            check=True,
        )

    query_parameters = {
        "customer_segment": "{{ params.customer_segment }}",
        "account_status": "{{ params.account_status }}",
        "minimum_transaction_amount": "{{ params.minimum_transaction_amount }}",
        "tenant_ids": "{{ params.tenant_ids }}",
    }
    fetch_size = "{{ params.fetch_size }}"
    window = "{{ params.window }}"
    customer_result = customer("{{ ds }}", query_parameters, fetch_size, window)
    account_result = account("{{ ds }}", query_parameters, fetch_size, window)
    transaction_result = transactions("{{ ds }}", query_parameters, fetch_size, window)
    customer_validation = dbt_tests.override(task_id="customer_dbt_tests")(
        "customer", customer_result, "{{ params.run_dbt_tests }}"
    )
    account_validation = dbt_tests.override(task_id="account_dbt_tests")(
        "account", account_result, "{{ params.run_dbt_tests }}"
    )
    transaction_validation = dbt_tests.override(task_id="transaction_dbt_tests")(
        "transactions", transaction_result, "{{ params.run_dbt_tests }}"
    )
    (
        customer_result
        >> customer_validation
        >> account_result
        >> account_validation
        >> transaction_result
        >> transaction_validation
    )


dag = hybrid_transactions()
