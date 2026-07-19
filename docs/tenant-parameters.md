# Tenant parameters

The active `hybrid_transactions` DAG accepts `tenant_ids` as a run parameter. It controls the
tenant scope consistently across customer, account, and transaction stages.

## Airflow trigger values

`tenant_ids` is an array of unique tenant ID strings. The maximum accepted input is 400 items.

| Intent | Trigger value |
| --- | --- |
| Process all tenants | `[]` |
| Process one tenant | `["tenant-a"]` |
| Process a subset | `["tenant-a", "tenant-b", "tenant-large"]` |

An empty array deliberately means **all tenants**. Tenant IDs must be non-empty strings. The
runner trims surrounding whitespace, removes duplicates while preserving order, and safely quotes
values before adding them to SQL.

In the Airflow UI, open `hybrid_transactions`, choose **Trigger**, and enter the array in the
`Tenant IDs` field. The selection applies to the whole DAG run; customer, account, and transactions
cannot receive different tenant lists within one run.

## What the parameter scopes

The same validated tenant selection is used at both sides of the hybrid pipeline:

```text
Airflow tenant_ids
  -> Trino source filters
  -> customer/account SCD2 source and current-target scans
  -> transaction source filters
  -> Delta transaction replaceWhere tenant predicate
```

This last step is important. A retry for `["tenant-a"]` replaces only `tenant-a` transaction
partitions in the selected daily, weekly, or monthly window. It cannot delete or replace rows for
`tenant-b`. Customer and account writes are SCD2 merges keyed by `(tenant_id, business_id)` and
their Trino scans use the same tenant restriction.

The committed stage result records both `tenant_ids` and `tenant_scope` (`selected` or `all`) for
logs, XCom metadata, validation, and rollback diagnostics.

## Command-line equivalent

The runner accepts the identical contract in `--query-params`:

```powershell
python tools/run_partition.py --date 2026-07-18 --stage transactions `
  --query-params '{"tenant_ids":["tenant-a","tenant-b"]}'
```

```shell
python3 tools/run_partition.py --date 2026-07-18 --stage transactions \
  --query-params '{"tenant_ids":["tenant-a","tenant-b"]}'
```

Use `--print-query` to inspect the rendered Trino filter without executing or writing data.

## Scheduling and performance guidance

Use `[]` for the normal scheduled run. Trino can process shared-catalog tenants in parallel, and a
single Delta writer avoids hundreds of pod starts, small commits, and Delta-log conflicts.

Use a selected list for:

- retrying tenants affected by a source or data-quality problem;
- controlled onboarding or validation;
- deliberately isolating a measured large-tenant outlier;
- targeted backfills within a date window.

Do not launch many selected-tenant DAG runs concurrently against the same Delta tables. The active
DAG uses `max_active_runs=1`, preserving SCD2 effective-date order and the version-checked rollback
guard. If production orchestration later permits parallel tenant batches, give them separate
candidate locations and consolidate with one writer, or replace table-level rollback assumptions
with an explicitly concurrency-safe publication design.

Tenant size does not need to be balanced in the trigger list. Keep normal runs all-tenant and use
observed Trino duration, bytes, and Delta file metrics to identify actual outliers. Physical table
partitioning remains:

```text
customer:     tenant_id
account:      tenant_id
transactions: tenant_id, business_date
```

## Categorization draft

The `transaction_categorization_draft` DAG exposes the same `tenant_ids` input for design review.
In the eventual asynchronous implementation, preparation will persist the selected tenant scope
with `run_id`. Finalization must reuse that persisted scope rather than accepting a different list,
ensuring that completeness checks and curated publication cover exactly the submitted tenants.

