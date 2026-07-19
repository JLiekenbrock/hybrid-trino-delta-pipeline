"""Load explicit Arrow/Delta schemas shared by transport and write validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa


def arrow_type(spec: str) -> pa.DataType:
    fixed = {
        "string": pa.string(),
        "bool": pa.bool_(),
        "date32": pa.date32(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "timestamp_us": pa.timestamp("us"),
        "timestamp_us_utc": pa.timestamp("us", tz="UTC"),
    }
    if spec in fixed:
        return fixed[spec]
    if spec.startswith("decimal128(") and spec.endswith(")"):
        precision, scale = (int(value) for value in spec[11:-1].split(","))
        return pa.decimal128(precision, scale)
    raise ValueError(f"Unsupported contract type: {spec}")


def schema(fields: list[dict]) -> pa.Schema:
    return pa.schema([
        pa.field(field["name"], arrow_type(field["type"]), nullable=field.get("nullable", True))
        for field in fields
    ])


@dataclass(frozen=True)
class Contract:
    business_key: str
    partition_by: list[str]
    target_schema: pa.Schema
    source_schema: pa.Schema


def load_contract(path: Path) -> Contract:
    value = json.loads(path.read_text(encoding="utf-8"))
    target = schema(value["fields"])
    source_fields = value.get("source_fields", value["fields"])
    return Contract(
        business_key=value["business_key"],
        partition_by=value["partition_by"],
        target_schema=target,
        source_schema=schema(source_fields),
    )
