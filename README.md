# Read-only Trino to Delta SCD2 pipeline

This project is optimized for a source environment where Trino can query every input but
cannot write. Trino performs all large joins, filtering, change detection and type casts.
Python streams the final result through a fixed Arrow schema into delta-rs, which owns Delta
transactions on a writable S3 prefix.

## Project setup at a glance

| Component | Responsibility |
| --- | --- |
| Trino | Read-only source access and pushdown of joins, filters, deduplication, change detection and casts |
| PyArrow | Bounded streaming transport with explicit schemas; no whole-result DataFrame is required |
| delta-rs | Transactional S3 writes, SCD2 `MERGE` operations and partition-scoped transaction replacement |
| Delta Lake on S3 | Durable handoff between independently retryable customer, account and transaction tasks |
| Airflow | Orders the three tasks, supplies the logical date and provides retry/observability boundaries |
| DuckDB + dbt | Reads the published Delta tables for contracts, tests, documentation and dataset lineage; it is not on the ingestion hot path |
| KubernetesExecutor (production) | Runs each Airflow task in a separate pod while Delta provides the cross-pod handoff |

The local demonstration supplies the same shape without external infrastructure: Trino's TPCH
connector is the read-only source, MinIO provides S3-compatible storage, and Airflow runs the
pipeline. Production replaces those endpoints through environment variables and the production
SQL profile; the execution design remains the same.

For production on Stackable Airflow, keep the Stackable Airflow runtime separate from the pure
ETL image and launch only the three data stages with KubernetesPodOperator. Unrelated Airflow
tasks continue to use the normal executor image. See
[`docs/stackable-airflow.md`](docs/stackable-airflow.md) for the image, DAG, RBAC and secret
examples.

## Runtime graph

```text
customer (Trino -> Arrow -> Delta SCD2 MERGE)
    -> account (Trino -> Arrow -> Delta SCD2 MERGE)
        -> transactions (Trino -> Arrow -> Delta replaceWhere)
```

Each Airflow task is a separate retry and KubernetesExecutor pod boundary. The Delta tables
are durable handoffs, and Trino reads their latest committed snapshots. DAG runs are serialized
(`max_active_runs=1`) so daily SCD2 changes cannot commit out of effective-date order. Automatic
catch-up is disabled; trigger historical dates deliberately in ascending order.

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

## dbt lineage, tests and documentation

dbt is deliberately outside the ingestion hot path. The Docker image runs `dbt parse` at build
time, while an explicit documentation/test command scans the three committed Delta tables with
DuckDB. Trino inputs are declared as read-only dbt `source()` nodes; `ref()` dependencies connect
the published customer, account and transaction models. Their metadata records Trino as the
source query engine, delta-rs as the writer, DuckDB as the dbt adapter, Delta/S3 as storage and
Airflow as the orchestrator.

This produces dataset lineage rather than pretending that execution engines are datasets:

```text
Trino customer ------------------------> customer Delta
Trino orders ----+---------------------> account Delta
customer Delta --+                           |
Trino lineitem --+---------------------------+--> transactions Delta
Trino orders ----+
```

The dbt tests cover null keys, transaction uniqueness, exactly one current SCD2 version and
non-overlapping validity windows.

Run those tests separately after publication when desired:

```powershell
dbt build --project-dir . --profiles-dir . --vars "output_uri: 's3://bucket/prefix'"
```

Critical transport and schema checks remain inline in the writer.

## Configure and run

Windows and macOS/Linux are both supported. Use Python 3.10-3.12.

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
```

macOS/Linux:

```shell
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

On macOS, install and start Docker Desktop before using the local stack. Apple Silicon is
supported by the Python and container dependencies; the Compose file intentionally has no
architecture override, so Docker selects native `arm64` images. Rosetta is only needed if a
private image is published for `amd64` alone.

Set the Trino variables plus `OUTPUT_URI` and the S3 credentials shown in `.env.example`.
Then run stages in order:

```powershell
.\run.ps1 -Date 2026-07-18 -Stage customer
.\run.ps1 -Date 2026-07-18 -Stage account
.\run.ps1 -Date 2026-07-18 -Stage transactions
```

macOS/Linux:

```shell
./run.sh 2026-07-18 customer
./run.sh 2026-07-18 account
./run.sh 2026-07-18 transactions
```

`DELTA_TARGET_FILE_SIZE` defaults to 512 MiB. Run compaction/checkpoint maintenance separately
instead of adding it to every ingestion task.

## Local Airflow UI

The local stack includes:

- Airflow at <http://localhost:8080>
- read-only Trino TPCH at <http://localhost:8081>
- MinIO S3 API at <http://localhost:9000>
- MinIO console at <http://localhost:9001> (`minioadmin` / `minioadmin`)

The default demo maps TPCH `customer -> orders -> lineitem` onto the three pipeline stages,
uses the `tiny` schema, and publishes Delta tables under `s3://hybrid/delta` in MinIO. Start it:

```powershell
.\airflow-local.ps1 start
.\airflow-local.ps1 logs
```

macOS/Linux:

```shell
./airflow-local.sh start
./airflow-local.sh logs
```

Open <http://localhost:8080> and select `hybrid_transactions`. Stop the environment with
`.\airflow-local.ps1 stop` on Windows or `./airflow-local.sh stop` on macOS/Linux. This
standalone deployment is for local development only.

Trigger the paused DAG for any date. Customer and account are stable SCD2 snapshots, so reruns
are idempotent; lineitem is transactionally replaced in the triggered `business_date` partition.
The trigger form also exposes validated query parameters for customer segment, account status,
minimum transaction amount and Trino fetch size. Airflow passes these to every task; the runner
escapes strings and validates numeric values before rendering the SQL profile. Preview a rendered
query without executing or writing it:

```powershell
python tools/run_partition.py --date 2026-07-18 --stage transactions --print-query `
  --query-params '{"minimum_transaction_amount": 1000}'
```

macOS/Linux:

```shell
python3 tools/run_partition.py --date 2026-07-18 --stage transactions --print-query \
  --query-params '{"minimum_transaction_amount": 1000}'
```

The `window` parameter accepts `daily`, `weekly`, or `monthly`. A weekly trigger aligns the
logical date to Monday and replaces all transaction date partitions in the seven-day interval;
a monthly trigger aligns to the first day and replaces that calendar month. Customer and account
remain physically partitioned by stable entity buckets because date partitioning performs poorly
for current-state SCD2 lookups. Transactions remain physically partitioned by `business_date`, so
weekly/monthly replacement only touches the included daily partitions.

The TPCH profile is in `sql/profiles/tpch/`. Production remains in `sql/`; set
`TRINO_SQL_PROFILE=production` and the values from `.env.example` to use it.

## Generate dbt documentation

Generate a reproducible dbt catalog and lineage graph against the local Delta
tables:

```powershell
.\dbt-docs.ps1
```

macOS/Linux:

```shell
./dbt-docs.sh
```

Open `target/index.html` after the command finishes. The graph models the Trino
tables as read-only dbt sources and the Delta datasets as dbt models. Node
metadata records the actual engine roles: Trino reads, delta-rs writes, DuckDB
exposes/tests the Delta outputs, and Airflow orchestrates the tasks.
