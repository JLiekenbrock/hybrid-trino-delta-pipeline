"""Graph-only draft for the future asynchronous transaction categorization flow.

The tasks intentionally perform no data or Kafka operations. This DAG exists so the proposed
boundaries can be reviewed in Airflow before the integration contracts are implemented.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Param, dag, task


DRAFT_MESSAGE = (
    "DESIGN PLACEHOLDER ONLY: no Delta, Kafka, Trino, or dbt operation was executed. "
    "See docs/transaction-categorization-draft.md."
)


@dag(
    dag_id="transaction_categorization_draft",
    description="Draft graph: prepare transactions, wait asynchronously, then publish curated transactions",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        "tenant_ids": Param(
            [],
            type="array",
            items={"type": "string", "minLength": 1},
            maxItems=400,
            title="Tenant IDs",
            description="Empty means all tenants; otherwise scope every draft phase to this list.",
        )
    },
    tags=["draft", "kafka", "delta", "categorization"],
    doc_md="""
    # Draft only

    This DAG is a reviewable graph of the proposed asynchronous categorization design. Its tasks
    are harmless placeholders and **do not process or publish data**. The production implementation
    will split preparation and finalization into independently scheduled DAGs so a long-running
    categorization does not occupy a worker pod.
    """,
)
def transaction_categorization_draft():
    @task
    def write_transactions_base() -> None:
        """Write normalized Delta rows and the exact expected request-ID set atomically."""
        print(DRAFT_MESSAGE)

    @task
    def publish_requests_to_kafka() -> None:
        """Let the checkpointed Spark Structured Streaming publisher submit requests."""
        print(DRAFT_MESSAGE)

    @task
    def mark_run_submitted() -> None:
        """Persist submission state, then allow the preparation phase to finish."""
        print(DRAFT_MESSAGE)

    @task
    def poll_result_table_without_worker() -> None:
        """Future scheduled/deferrable check; never a loop in a worker or Spark job."""
        print(DRAFT_MESSAGE)

    @task
    def reconcile_expected_ids() -> None:
        """Anti-join expected and terminal result IDs; reject missing or duplicate IDs."""
        print(DRAFT_MESSAGE)

    @task
    def write_transactions_curated_candidate() -> None:
        """Join base rows to results by request_id and write a candidate Delta snapshot."""
        print(DRAFT_MESSAGE)

    @task
    def transaction_dbt_tests() -> None:
        """Validate completeness, uniqueness, types, and transaction relationships."""
        print(DRAFT_MESSAGE)

    @task
    def promote_or_rollback() -> None:
        """Expose validated curated data or perform a version-checked rollback."""
        print(DRAFT_MESSAGE)

    base = write_transactions_base()
    publish = publish_requests_to_kafka()
    submitted = mark_run_submitted()
    poll = poll_result_table_without_worker()
    reconcile = reconcile_expected_ids()
    curated = write_transactions_curated_candidate()
    tests = transaction_dbt_tests()
    publish_result = promote_or_rollback()

    base >> publish >> submitted >> poll >> reconcile >> curated >> tests >> publish_result


dag = transaction_categorization_draft()
