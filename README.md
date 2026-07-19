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
| KubernetesPodOperator (production) | Runs only the ETL stages in the shared pipeline image while unrelated Airflow tasks retain their normal runtime |

## Why these technologies

- **Trino** is the source query engine because it is the only common access layer for all foreign
  inputs. It executes the expensive joins, filtering, deduplication and projection close to the
  sources, so only the reduced final result crosses the network. It is intentionally read-only in
  this design because the available Trino identity cannot write.
- **Python** provides a small control layer around the Trino cursor, schema enforcement and Delta
  transaction APIs. It avoids implementing orchestration or data processing logic in shell code.
- **PyArrow** is the transport because Trino results can be converted in bounded batches with an
  explicit schema. This avoids loading an entire result into a pandas or Polars DataFrame and
  preserves decimals, timestamps and nullability across the engine boundary.
- **delta-rs** is the writer because it can commit Delta transactions directly to the writable S3
  prefix without requiring Trino write permissions or a Spark cluster. It supports the SCD2 merge
  and partition-scoped overwrite behavior required here.
- **Delta Lake on S3** is both the table format and durable handoff. Its transaction log makes
  retries atomic and gives downstream tasks a committed snapshot, while S3 remains accessible
  across short-lived Kubernetes pods. Plain Parquet alone would not provide transactional merge,
  replacement or snapshot semantics.
- **Airflow** supplies scheduling, logical dates, typed run parameters, retries and operational
  visibility. Customer, account and transactions remain separate tasks because each published
  Delta table is a useful retry and observability boundary.
- **KubernetesPodOperator** is used for production ETL stages so they can share one purpose-built
  pipeline image without forcing unrelated Airflow tasks to use that image. The Stackable Airflow
  control-plane and normal executor images remain separate.
- **DuckDB** directly scans the published Delta/S3 data for fast local validation and analytical
  SQL. It stays outside the ingestion hot path because the result reduction depends on joins that
  Trino should perform before transferring data.
- **dbt** documents dataset lineage and runs post-publication tests using DuckDB. It is not used as
  the hybrid execution orchestrator because one dbt invocation has one adapter and the actual job
  spans Trino reads, Arrow transport and delta-rs writes.
- **MinIO and TPCH** make the architecture runnable locally without AWS credentials or production
  source access. They are development substitutes for S3 and foreign Trino catalogs, not required
  production components.

**Polars is not required** because this pipeline does not need an in-memory DataFrame layer after
Trino has completed the transformations. **Redis is not a data handoff or cache**: Delta on S3 is
durable and transactional. Redis is needed only if the chosen Airflow deployment uses
CeleryExecutor as its task queue.

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
customer write -> customer dbt tests
    -> account write -> account dbt tests
        -> transactions write -> transaction dbt tests
```

Each Airflow stage is a separate retry and, in production, KubernetesPodOperator pod boundary.
The Delta tables are durable handoffs, and Trino reads their latest committed snapshots. DAG runs are serialized
(`max_active_runs=1`) so daily SCD2 changes cannot commit out of effective-date order. Automatic
catch-up is disabled; trigger historical dates deliberately in ascending order.

### Draft asynchronous categorization extension

The current implementation still publishes one `transactions` table. A proposed extension splits
it into an early, replayable `transactions_base` Delta table and a complete
`transactions_curated` table after an external Kafka categorization service responds. Because
categorization may take hours, preparation and finalization are separate DAGs; no Spark job or
Kubernetes worker waits for the service. Airflow determines readiness by comparing the exact set
of persisted request IDs with the service's result table, not by trusting counts alone.

See [`docs/transaction-categorization-draft.md`](docs/transaction-categorization-draft.md) for the
table contracts, polling ownership, completion rules, failure policy decisions and cutover plan.
Airflow also loads `transaction_categorization_draft` as a paused, manual, graph-only DAG. Its
tasks are harmless placeholders so the proposed boundaries can be reviewed in the UI; they do
not read, publish, poll, test or write any data.

## Publishing behavior

- `customer`: tenant-aware SCD2, physically partitioned by `tenant_id`.
- `account`: tenant-aware SCD2, physically partitioned by `tenant_id`.
- `transactions`: immutable facts, partitioned by `tenant_id, business_date`; a retry
  transactionally replaces the selected date window using Delta `replaceWhere`.

Customer and account SQL emit staged SCD2 input. Changed existing keys produce one row with
`merge_key=<business key>` to close the current version plus one row with `merge_key=NULL` to
insert the replacement. New keys produce only the insert row. delta-rs applies both actions in
one `MERGE` commit. The supplied SQL keeps the latest source state per key in each daily window;
adapt it if every intraday version must be retained.

## Multi-tenant execution decision

By default, the production design processes all tenants in one Trino query and one Delta commit
per dataset stage. Airflow's `tenant_ids` parameter can instead select any subset (up to 400 IDs);
an empty list means all tenants. There are at most roughly 400 tenants, they share credentials and catalogs, and Trino can
parallelize their source partitions across its workers. This avoids hundreds of Kubernetes pod
starts, small commits and concurrent Delta-log conflicts. It also keeps the version-checked
rollback safe because only one writer owns a table during a stage.

`tenant_id` is mandatory throughout the Arrow and Delta contracts. Keys are composite:

```text
customer:    (tenant_id, customer_id)
account:     (tenant_id, account_id)
transaction: (tenant_id, transaction_id)
```

Every source deduplication, customer/account/transaction join, SCD2 merge predicate and dbt test
uses that tenant boundary. The TPCH demonstration supplies the constant tenant `tpch`.

Customer and account are physically partitioned by `tenant_id`; transactions use
`tenant_id, business_date`. The stable `entity_bucket` remains a column used in SCD2 merge
predicates, but is not also a physical partition. Combining 400 tenant partitions with 256 bucket
partitions could produce 102,400 sparse dimension partitions and excessive small files.

Tenant sizes may be highly skewed. Start with the single all-tenant stage and use Trino query
metrics to identify genuine outliers. If later needed, isolate only the largest tenants or create
size-balanced tenant batches. Do not let parallel batches commit independently to the same Delta
table while table-version rollback is enabled; use separate table locations or a single
consolidating writer for that design.

A large tenant partition is not inherently a problem: delta-rs writes multiple target-sized
Parquet files inside it. Optimize for file sizes, pruning and observed query patterns rather than
trying to make every tenant partition equally large. If a dimension partition becomes too large
for acceptable SCD2 merge performance, introduce adaptive `tenant_shard` as a second partition
column using a governed tenant-size table:

```text
small tenant  -> shard_count 1
medium tenant -> shard_count 8
large tenant  -> shard_count 32 or 64
tenant_shard  = hash(composite business key) % shard_count
```

Small tenants then retain one partition while only measured outliers fan out. The shard count must
be stable for existing records or changed through a deliberate table rewrite. Avoid using all 256
`entity_bucket` values as physical partitions for every tenant. For transactions, add a shard only
when a single `tenant_id, business_date` partition is demonstrably too large; multiple Parquet files
inside that partition are normally sufficient.

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
non-overlapping validity windows. Airflow runs model-scoped tests immediately after each atomic
Delta write. A failure marks the DAG run failed and prevents the next dataset from being built.
Disable these tasks for an exceptional run with the typed `run_dbt_tests` trigger parameter.

### Failed-test rollback decision

Every table is initialized with an empty Delta version 0 before its first data commit. Each ETL
task returns only small version metadata through XCom: the previous version, its committed version
and the table URI. If the immediately following dbt task fails, it checks that the table is still
at exactly that committed version and then uses Delta `RESTORE` to create a compensating commit
whose snapshot matches the previous version. The validation task still fails, so the next dataset
does not run.

Rollback is intentionally refused when the current version differs from the failed job's committed
version. This optimistic-concurrency guard prevents the cleanup task from erasing a newer external
writer's commit. `max_active_runs=1` serializes this DAG, but production should also ensure that
other writers coordinate ownership of these tables. Delta history retains both the rejected commit
and the compensating restore for auditability until normal retention and vacuum policies remove old
files.

This is a performance-oriented alternative to Write-Audit-Publish (WAP): it avoids copying a
candidate table from S3 to S3, but a bad snapshot can be visible briefly between commit and restore.
Use candidate locations plus a metadata-only catalog/pointer swap when absolutely no unvalidated
snapshot may ever be visible to readers.

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
uses the `tiny` schema, and publishes tenant-aware Delta tables under
`s3://hybrid/delta-multitenant` in MinIO. The prior single-tenant demo prefix is left untouched
because Delta partition specifications cannot be migrated in place. Start it:

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
minimum transaction amount, tenant IDs and Trino fetch size. Airflow passes these to every task; the runner
escapes strings and validates numeric values before rendering the SQL profile. Preview a rendered
query without executing or writing it:

```powershell
python tools/run_partition.py --date 2026-07-18 --stage transactions --print-query `
  --query-params '{"tenant_ids":["tenant-a"],"minimum_transaction_amount":1000}'
```

macOS/Linux:

```shell
python3 tools/run_partition.py --date 2026-07-18 --stage transactions --print-query \
  --query-params '{"tenant_ids":["tenant-a"],"minimum_transaction_amount":1000}'
```

The `window` parameter accepts `daily`, `weekly`, or `monthly`. A weekly trigger aligns the
logical date to Monday and replaces all transaction date partitions in the seven-day interval;
a monthly trigger aligns to the first day and replaces that calendar month. Customer and account
remain physically partitioned by tenant. Transactions remain physically partitioned by tenant and
`business_date`, so weekly/monthly replacement only touches the included daily partitions.
When `tenant_ids` is supplied, the same validated selection scopes both the Trino source query
and the Delta `replaceWhere` predicate. A tenant-specific retry therefore cannot replace another
tenant's transaction partitions. Customer/account SCD2 merges are naturally key-scoped and their
source and current-target scans receive the same tenant filter.

See [`docs/tenant-parameters.md`](docs/tenant-parameters.md) for trigger examples, validation
rules, scoped-retry guarantees, command-line usage and performance guidance.

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
