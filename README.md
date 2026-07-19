# Read-only Trino to Delta SCD2 pipeline

This project is optimized for a source environment where Trino can query every input but
cannot write. Trino performs all large joins, filtering, change detection and type casts.
Python streams the final result through a fixed Arrow schema into delta-rs, which owns Delta
transactions on a writable S3 prefix.

## Runtime graph

```text
customer (Trino -> Arrow -> Delta SCD2 MERGE)
    -> account (Trino -> Arrow -> Delta SCD2 MERGE)
        -> transactions (Trino -> Arrow -> Delta replaceWhere)
```

Each Airflow task is a separate retry and KubernetesExecutor pod boundary. The Delta tables
are durable handoffs, and Trino reads their latest committed snapshots. DAG runs are serialized
(`max_active_runs=1`) so daily SCD2 changes cannot commit out of effective-date order.

## Publishing behavior

- `customer`: SCD2, partitioned into 256 stable `entity_bucket` values.
- `account`: SCD2, partitioned into 256 stable `entity_bucket` values.
- `transactions`: immutable facts, partitioned by `business_date`; a retry transactionally
  replaces only that date using Delta `replaceWhere`.

Customer and account SQL emit staged SCD2 input. Changed existing keys produce one row with
`merge_key=<business key>` to close the current version plus one row with `merge_key=NULL` to
insert the replacement. New keys produce only the insert row. delta-rs applies both actions in
one `MERGE` commit. The supplied SQL keeps the latest source state per key in each daily window;
adapt it if every intraday version must be retained.

## Schema contracts

`contracts/*.json` defines exact Arrow and Delta types, nullability, business keys and
partition columns. The Trino cursor column names and order must match. Every batch is converted
with the explicit Arrow schema; inference from the first batch is never used. Delta rejects
incompatible writes.

The included contracts are examples. Update them together with the matching projections in:

```text
sql/customer.sql
sql/customer_bootstrap.sql
sql/account.sql
sql/account_bootstrap.sql
sql/transactions.sql
```

The bootstrap query is selected automatically when a customer/account Delta table does not yet
exist. Replace the example `analytics.*` and `published.*` relations with your Trino relations.
Trino must be able to read the published Delta tables before the downstream task starts.

## dbt's role

dbt is not invoked by Airflow tasks. The Docker image runs `dbt parse` at build time to validate
the published-table lineage. The dbt models scan the three committed Delta tables and provide
documentation plus post-publication tests for null keys, transaction uniqueness, exactly one
current SCD2 version and non-overlapping validity windows.

Run those tests separately after publication when desired:

```powershell
dbt build --project-dir . --profiles-dir . --vars "output_uri: 's3://bucket/prefix'"
```

Critical transport/schema checks remain inline in the writer, so dbt is not on the hot path.

## Configure and run

Install the project:

```powershell
python -m pip install -e .
```

Set the Trino variables plus `OUTPUT_URI` and the S3 credentials shown in `.env.example`.
Then run stages in order:

```powershell
.\run.ps1 -Date 2026-07-18 -Stage customer
.\run.ps1 -Date 2026-07-18 -Stage account
.\run.ps1 -Date 2026-07-18 -Stage transactions
```

`DELTA_TARGET_FILE_SIZE` defaults to 512 MiB. Run compaction/checkpoint maintenance separately
instead of adding it to every ingestion task.

## Local Airflow UI

The local UI uses the official Airflow 3.3.0 image:

```powershell
.\airflow-local.ps1 start
.\airflow-local.ps1 logs
```

Open <http://localhost:8080> and select `hybrid_transactions`. Stop the environment with
`.\airflow-local.ps1 stop`. This standalone deployment is for local development only.
