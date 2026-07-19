"""Production example: run only the hybrid ETL stages in the pipeline image."""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Param, dag
from kubernetes.client import models as k8s


PIPELINE_IMAGE = os.environ.get(
    "HYBRID_PIPELINE_IMAGE", "registry.example.com/data/hybrid-pipeline:1.0.0"
)
VALIDATION_IMAGE = os.environ.get(
    "HYBRID_VALIDATION_IMAGE", "registry.example.com/data/hybrid-validation:1.0.0"
)
NAMESPACE = os.environ.get("HYBRID_PIPELINE_NAMESPACE", "data-jobs")

QUERY_PARAMETERS = """{
  "customer_segment": {{ params.customer_segment | tojson }},
  "account_status": {{ params.account_status | tojson }},
  "minimum_transaction_amount": {{ params.minimum_transaction_amount | tojson }}
  ,"tenant_ids": {{ params.tenant_ids | tojson }}
}"""


def stage_pod(stage: str, cpu: str, memory: str) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=stage,
        name=f"hybrid-{stage}",
        namespace=NAMESPACE,
        image=PIPELINE_IMAGE,
        arguments=[
            "--stage", stage,
            "--date", "{{ ds }}",
            "--window", "{{ params.window }}",
            "--fetch-size", "{{ params.fetch_size }}",
            "--query-params", QUERY_PARAMETERS,
            "--xcom-output", "/airflow/xcom/return.json",
        ],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="hybrid-pipeline-secrets")
            )
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu, "memory": memory},
            limits={"cpu": cpu, "memory": memory},
        ),
        service_account_name="hybrid-pipeline",
        do_xcom_push=True,
        get_logs=True,
        log_events_on_failure=True,
        reattach_on_restart=True,
        on_finish_action="delete_pod",
        startup_timeout_seconds=600,
    )


def validation_pod(model: str, upstream_task_id: str) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=f"{model.rstrip('s')}_dbt_tests",
        name=f"hybrid-{model}-dbt-tests",
        namespace=NAMESPACE,
        image=VALIDATION_IMAGE,
        arguments=[
            "--model", model,
            "--stage-result", "{{ ti.xcom_pull(task_ids='" + upstream_task_id + "') | tojson }}",
            "--project-dir", "/opt/dbt",
            "--profiles-dir", "/opt/dbt",
        ],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="hybrid-pipeline-secrets")
            )
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "1", "memory": "2Gi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
        service_account_name="hybrid-pipeline",
        do_xcom_push=False,
        get_logs=True,
        log_events_on_failure=True,
        reattach_on_restart=True,
        on_finish_action="delete_pod",
        startup_timeout_seconds=600,
    )


@dag(
    dag_id="hybrid_transactions_kubernetes",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params={
        "customer_segment": Param(None, type=["null", "string"]),
        "account_status": Param(None, type=["null", "string"]),
        "minimum_transaction_amount": Param(0, type="number", minimum=0),
        "tenant_ids": Param(
            [], type="array", items={"type": "string", "minLength": 1}, maxItems=400
        ),
        "fetch_size": Param(100_000, type="integer", minimum=1, maximum=1_000_000),
        "window": Param("daily", type="string", enum=["daily", "weekly", "monthly"]),
    },
    tags=["trino", "delta", "kubernetes", "scd2"],
)
def hybrid_transactions_kubernetes():
    customer = stage_pod("customer", cpu="1", memory="2Gi")
    account = stage_pod("account", cpu="2", memory="4Gi")
    transactions = stage_pod("transactions", cpu="4", memory="8Gi")
    customer_tests = validation_pod("customer", "customer")
    account_tests = validation_pod("account", "account")
    transaction_tests = validation_pod("transactions", "transactions")
    customer >> customer_tests >> account >> account_tests >> transactions >> transaction_tests


hybrid_transactions_kubernetes()
