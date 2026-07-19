# Draft: asynchronous transaction categorization

> Status: design draft. The current pipeline still writes directly to `transactions`; none of
> the table names or DAGs below are active yet.

Categorization is an asynchronous boundary rather than an inline transaction transformation.
The categorization service can take much longer than extraction and is operated independently
from this ETL. The pipeline therefore should not keep a Spark job, Airflow worker, or Kubernetes
pod alive while waiting.

## Dataset decision

Keep two transaction Delta tables with different guarantees:

| Dataset | Purpose | Consumer guarantee |
| --- | --- | --- |
| `transactions_base` | Normalized, type-safe transaction and durable replay source before categorization | Available early; category is not present |
| `transactions_curated` | Base transaction joined to the terminal categorization response | Published only after completeness checks and dbt tests |

The external service's `categorization_results` table is a narrow integration table, not a copy
of the transaction. A small `categorization_requests` control table records the exact expected
IDs for each submitted run. `transactions_curated` is the official replacement for today's
consumer-facing `transactions`; `transactions_base` is an internal silver/staging dataset with a
deliberate replay and audit retention period.

Suggested logical columns:

```text
transactions_base
  tenant_id, transaction_id, request_id, run_id, business_date, ...transaction attributes

categorization_requests
  run_id, tenant_id, request_id, submitted_at, submission_status

categorization_results                 # owned by the external service
  request_id, status, category, model_version, processed_at, error_code

transactions_curated
  ...transactions_base columns, category, model_version, categorized_at
```

`request_id` must be deterministic and globally unambiguous, for example a stable hash of
`run_id`, `tenant_id`, and `transaction_id`. It is the Kafka message key. Retries may publish a
request more than once, so both the service and final join must be idempotent by this ID.

Both the current ETL DAG and this draft use the same `tenant_ids` trigger contract: an empty array
means all tenants and a non-empty array selects up to 400 tenants. Preparation persists the tenant
scope with the run. Finalization reads that persisted scope rather than accepting a new selection,
so it cannot accidentally reconcile or publish a different tenant set.

## Proposed orchestration

Use two independently retryable DAGs instead of holding the main DAG open for an unknown amount
of time:

```text
Preparation DAG
  account dbt tests
    -> write transactions_base + categorization_requests atomically for the run
    -> Spark Structured Streaming publishes requests to Kafka
    -> mark run SUBMITTED
    -> finish

Finalization DAG (short scheduled readiness check)
  find submitted runs
    -> compare expected IDs with the service result table through Trino
    -> if incomplete: finish without starting a worker-heavy task
    -> if complete: join base + results
    -> write candidate transactions_curated Delta version
    -> transaction dbt tests
    -> promote, or version-checked rollback on failure
```

A periodically scheduled finalization DAG is preferred when categorization can take hours. A
deferrable or `reschedule` Airflow sensor is also valid, but it leaves a long-running DAG instance.
Never implement the wait as a polling loop inside a KubernetesPodOperator or Spark job.

## Readiness contract

Counts are useful metrics but are not proof of completeness. Persist the exact expected ID set
before publishing and make the anti-join authoritative:

```sql
SELECT count(*) AS missing_count
FROM categorization_requests AS expected
LEFT JOIN categorization_results AS actual
  ON actual.request_id = expected.request_id
WHERE expected.run_id = :run_id
  AND actual.request_id IS NULL;
```

The run is ready only when `missing_count = 0`, every expected ID has exactly one terminal result,
and no unexpected or duplicate result is selected. Report readiness per tenant as well as for the
whole run so skew among up to 400 tenants is visible.

The service must persist a terminal `SUCCESS` or `FAILED` row for every accepted request. Without
terminal failures, a missing ID could mean queued, lost, or permanently failed, so Airflow cannot
know that the batch is finished. A service-produced run completion marker is a useful optimization,
but the ID-level comparison remains the correctness check.

## Finalization and publication

Finalization reads only terminal results and performs an ID join:

```sql
SELECT
  base.*,
  result.category,
  result.model_version,
  result.processed_at AS categorized_at
FROM transactions_base AS base
JOIN categorization_results AS result
  ON result.request_id = base.request_id
WHERE base.run_id = :run_id
  AND result.status = 'SUCCESS';
```

Define an explicit policy for terminal `FAILED` results before implementation: either fail the
whole run, publish a governed fallback category, or quarantine those transactions. Do not silently
drop them. Write and validate a candidate curated snapshot before exposing it, following the
project's existing version-checked rollback/WAP decision.

## Spark and Kafka responsibilities

The existing Spark Structured Streaming publisher can remain. It should checkpoint progress,
use `request_id` as the Kafka key, attach `run_id`, tolerate at-least-once delivery, and expose
consumer lag/backlog metrics. Kafka retention must exceed the worst expected categorization delay.
Kafka topic write permission does not imply read permission; the ETL does not need to consume the
request topic because readiness is checked against the persisted result table.

## Implementation cutover checklist

1. Agree the result schema and terminal failure behavior with the service owner.
2. Add contracts and Delta writers for `transactions_base` and `transactions_curated`.
3. Persist expected IDs before Kafka publication and make publication restartable.
4. Add the lightweight readiness/finalization DAG and duplicate/unexpected-ID checks.
5. Add dbt lineage and tests for both transaction layers.
6. Run both paths in parallel, reconcile results, then move consumers from `transactions` to
   `transactions_curated`.
