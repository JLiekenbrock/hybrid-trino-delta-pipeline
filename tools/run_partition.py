"""Stream a Trino stage into a schema-enforced Delta Lake transaction."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import trino
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from contracts import Contract, load_contract


ROOT = Path(__file__).resolve().parents[1]
SCD2_STAGES = {"customer", "account"}


def trino_connection():
    required = ("TRINO_HOST", "TRINO_USER", "TRINO_CATALOG", "TRINO_SCHEMA")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
    return trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.getenv("TRINO_PORT", "443")),
        user=os.environ["TRINO_USER"],
        catalog=os.environ["TRINO_CATALOG"],
        schema=os.environ["TRINO_SCHEMA"],
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "https"),
    )


def sql_string(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def processing_window(day: date, mode: str) -> tuple[date, date]:
    if mode == "daily":
        start = day
        end = start + timedelta(days=1)
    elif mode == "weekly":
        start = day - timedelta(days=day.weekday())
        end = start + timedelta(days=7)
    elif mode == "monthly":
        start = day.replace(day=1)
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    else:
        raise ValueError(f"unsupported processing window: {mode}")
    return start, end


def query_template_values(
    day: date, parameters: dict[str, object], window: str = "daily"
) -> dict[str, str]:
    try:
        minimum_amount = Decimal(str(parameters.get("minimum_transaction_amount", 0)))
    except InvalidOperation as exc:
        raise ValueError("minimum_transaction_amount must be numeric") from exc
    if minimum_amount < 0:
        raise ValueError("minimum_transaction_amount must be non-negative")
    tenant_ids = parameters.get("tenant_ids", [])
    if tenant_ids is None:
        tenant_ids = []
    if not isinstance(tenant_ids, list) or any(
        not isinstance(tenant_id, str) or not tenant_id.strip() for tenant_id in tenant_ids
    ):
        raise ValueError("tenant_ids must be an array of non-empty strings")
    if len(tenant_ids) > 400:
        raise ValueError("tenant_ids accepts at most 400 tenants")
    tenant_ids = list(dict.fromkeys(tenant_id.strip() for tenant_id in tenant_ids))
    start_day, end_day = processing_window(day, window)
    start = datetime.combine(start_day, datetime_time.min)
    end = datetime.combine(end_day, datetime_time.min)
    return {
        "business_date": start_day.isoformat(),
        "window_start_date": start_day.isoformat(),
        "window_end_date": end_day.isoformat(),
        "start_ts": start.isoformat(sep=" "),
        "end_ts": end.isoformat(sep=" "),
        "customer_segment": sql_string(parameters.get("customer_segment")),
        "account_status": sql_string(parameters.get("account_status")),
        "minimum_transaction_amount": format(minimum_amount, "f"),
        "all_tenants": "true" if not tenant_ids else "false",
        # NULL keeps the generated IN clause syntactically valid when all tenants are selected.
        "tenant_values": ", ".join(sql_string(tenant_id) for tenant_id in tenant_ids) or "NULL",
    }


def render_query(
    stage: str,
    day: date,
    parameters: dict[str, object],
    bootstrap: bool = False,
    window: str = "daily",
) -> str:
    profile = os.getenv("TRINO_SQL_PROFILE", "production")
    directory = ROOT / "sql" if profile == "production" else ROOT / "sql" / "profiles" / profile
    suffix = "_bootstrap" if bootstrap else ""
    path = directory / f"{stage}{suffix}.sql"
    if not path.exists():
        path = directory / f"{stage}.sql"
    return path.read_text(encoding="utf-8").format(
        **query_template_values(day, parameters, window)
    )


def arrow_stream(sql: str, schema: pa.Schema, fetch_size: int) -> pa.RecordBatchReader:
    """Lazily convert Trino DBAPI batches using a fixed, non-inferred Arrow schema."""
    cursor = trino_connection().cursor()
    cursor.execute(sql)
    names = [column[0] for column in cursor.description]
    expected = schema.names
    if names != expected:
        cursor.close()
        raise RuntimeError(f"Trino columns do not match contract. expected={expected}, actual={names}")

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            while rows := cursor.fetchmany(fetch_size):
                records = [dict(zip(names, row)) for row in rows]
                yield pa.RecordBatch.from_pylist(records, schema=schema)
        finally:
            cursor.close()

    return pa.RecordBatchReader.from_batches(schema, batches())


def peek(reader: pa.RecordBatchReader) -> tuple[pa.RecordBatch | None, pa.RecordBatchReader]:
    try:
        first = reader.read_next_batch()
    except StopIteration:
        return None, pa.RecordBatchReader.from_batches(reader.schema, [])

    def batches() -> Iterator[pa.RecordBatch]:
        yield first
        yield from reader

    return first, pa.RecordBatchReader.from_batches(reader.schema, batches())


def storage_options() -> dict[str, str]:
    names = (
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
        "AWS_REGION", "AWS_ENDPOINT_URL", "AWS_ALLOW_HTTP",
        "AWS_FORCE_PATH_STYLE", "AWS_S3_ALLOW_UNSAFE_RENAME",
    )
    return {name: os.environ[name] for name in names if os.getenv(name)}


def open_table(uri: str) -> DeltaTable | None:
    try:
        return DeltaTable(uri, storage_options=storage_options())
    except TableNotFoundError:
        return None


def initialize_empty_table(uri: str, contract: Contract) -> DeltaTable:
    """Create version 0 so every data commit has a rollback target."""
    empty = pa.RecordBatchReader.from_batches(contract.target_schema, [])
    write_deltalake(
        uri,
        empty,
        mode="error",
        partition_by=contract.partition_by,
        storage_options=storage_options(),
    )
    return DeltaTable(uri, storage_options=storage_options())


def target_only_reader(reader: pa.RecordBatchReader, contract: Contract) -> pa.RecordBatchReader:
    names = contract.target_schema.names

    def batches() -> Iterator[pa.RecordBatch]:
        for batch in reader:
            merge_key = batch.column(batch.schema.get_field_index("merge_key"))
            if merge_key.null_count != len(batch):
                raise RuntimeError("SCD2 bootstrap input must contain insert rows only")
            yield batch.select(names)

    return pa.RecordBatchReader.from_batches(contract.target_schema, batches())


def write_scd2(stage: str, uri: str, reader: pa.RecordBatchReader, contract: Contract) -> dict:
    table = open_table(uri)
    first, reader = peek(reader)
    if first is None:
        return {"operation": "noop", "num_source_rows": 0}

    if table is None:
        # Bootstrap queries must emit insert rows only (merge_key is null).
        merge_key = first.column(first.schema.get_field_index("merge_key"))
        if merge_key.null_count != len(first):
            raise RuntimeError(f"{stage} bootstrap received close rows for a table that does not exist")
        write_deltalake(
            uri,
            target_only_reader(reader, contract),
            mode="error",
            partition_by=contract.partition_by,
            storage_options=storage_options(),
            target_file_size=int(os.getenv("DELTA_TARGET_FILE_SIZE", str(512 * 1024 * 1024))),
        )
        return {"operation": "create"}

    tenant_key, key = contract.business_keys
    insert_values = {name: f"source.{name}" for name in contract.target_schema.names}
    metrics = (
        table.merge(
            source=reader,
            predicate=(
                f"target.{tenant_key} = source.{tenant_key} AND "
                "target.entity_bucket = source.entity_bucket AND target.is_current = true AND ("
                f"(target.{key} = source.merge_key AND target.attribute_hash <> source.attribute_hash) OR "
                f"(source.merge_key IS NULL AND target.{key} = source.{key} "
                "AND target.attribute_hash = source.attribute_hash))"
            ),
            source_alias="source",
            target_alias="target",
            error_on_type_mismatch=True,
            streamed_exec=True,
        )
        .when_matched_update(
            predicate="source.merge_key IS NOT NULL",
            updates={"valid_to": "source.valid_to", "is_current": "false"},
        )
        .when_not_matched_insert(
            predicate="source.merge_key IS NULL",
            updates=insert_values,
        )
        .execute()
    )
    return {"operation": "merge", **metrics}


def write_transactions(
    uri: str,
    window_start: date,
    window_end: date,
    reader: pa.RecordBatchReader,
    contract: Contract,
    tenant_ids: list[str] | None = None,
) -> dict:
    table = open_table(uri)
    first, reader = peek(reader)
    predicate = (
        f"business_date >= '{window_start.isoformat()}' AND "
        f"business_date < '{window_end.isoformat()}'"
    )
    if tenant_ids:
        tenant_values = ", ".join(sql_string(tenant_id) for tenant_id in tenant_ids)
        predicate += f" AND tenant_id IN ({tenant_values})"
    if first is None:
        metrics = table.delete(predicate=predicate) if table else {}
        return {"operation": "delete_empty_partition", **metrics}

    write_deltalake(
        table or uri,
        reader,
        mode="overwrite" if table else "error",
        partition_by=contract.partition_by if table is None else None,
        predicate=predicate if table else None,
        storage_options=storage_options(),
        target_file_size=int(os.getenv("DELTA_TARGET_FILE_SIZE", str(512 * 1024 * 1024))),
    )
    return {"operation": "replace_partition", "partition_predicate": predicate}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--stage", required=True, choices=("customer", "account", "transactions"))
    parser.add_argument("--output-uri", default=os.getenv("OUTPUT_URI"))
    parser.add_argument("--fetch-size", type=int, default=100_000)
    parser.add_argument("--query-params", type=json.loads, default={})
    parser.add_argument("--window", choices=("daily", "weekly", "monthly"), default="daily")
    parser.add_argument("--print-query", action="store_true")
    parser.add_argument("--xcom-output", type=Path)
    args = parser.parse_args()
    if not isinstance(args.query_params, dict):
        parser.error("--query-params must be a JSON object")
    if args.fetch_size < 1:
        parser.error("--fetch-size must be at least 1")
    window_start, window_end = processing_window(args.date, args.window)
    # Validate and normalize tenant selection before querying or constructing replaceWhere.
    query_template_values(args.date, args.query_params, args.window)
    tenant_ids = args.query_params.get("tenant_ids") or []
    tenant_ids = list(dict.fromkeys(tenant_id.strip() for tenant_id in tenant_ids))
    rendered_query = render_query(
        args.stage, args.date, args.query_params, bootstrap=False, window=args.window
    )
    if args.print_query:
        print(rendered_query)
        return
    if not args.output_uri:
        parser.error("set --output-uri or OUTPUT_URI")

    started = time.perf_counter()
    uri = f"{args.output_uri.rstrip('/')}/{args.stage}"
    contract = load_contract(ROOT / "contracts" / f"{args.stage}.json")
    existing_table = open_table(uri)
    bootstrap = args.stage in SCD2_STAGES and existing_table is None
    table_before_write = existing_table or initialize_empty_table(uri, contract)
    previous_version = table_before_write.version()
    reader = arrow_stream(
        render_query(
            args.stage, args.date, args.query_params, bootstrap=bootstrap, window=args.window
        ),
        contract.source_schema,
        args.fetch_size,
    )
    if args.stage in SCD2_STAGES:
        metrics = write_scd2(args.stage, uri, reader, contract)
    else:
        metrics = write_transactions(
            uri, window_start, window_end, reader, contract, tenant_ids=tenant_ids
        )
    committed_version = DeltaTable(uri, storage_options=storage_options()).version()
    result = {
        "business_date": args.date.isoformat(),
        "window": args.window,
        "window_start": window_start.isoformat(),
        "window_end_exclusive": window_end.isoformat(),
        "stage": args.stage,
        "tenant_ids": tenant_ids,
        "tenant_scope": "selected" if tenant_ids else "all",
        "delta_uri": uri, "total_seconds": round(time.perf_counter() - started, 3),
        "previous_version": previous_version,
        "committed_version": committed_version,
        "metrics": metrics,
    }
    rendered_result = json.dumps(result, indent=2, default=str)
    print(rendered_result)
    if args.xcom_output:
        args.xcom_output.parent.mkdir(parents=True, exist_ok=True)
        args.xcom_output.write_text(json.dumps(result, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
