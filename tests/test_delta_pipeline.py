from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable


PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "tools"))

from contracts import load_contract  # noqa: E402
from run_partition import write_scd2, write_transactions  # noqa: E402


def reader(schema: pa.Schema, rows: list[dict]) -> pa.RecordBatchReader:
    batch = pa.RecordBatch.from_pylist(rows, schema=schema)
    return pa.RecordBatchReader.from_batches(schema, [batch])


class DeltaPipelineTest(unittest.TestCase):
    def test_customer_scd2_create_and_change(self) -> None:
        contract = load_contract(PROJECT / "contracts" / "customer.json")
        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "customer")
            initial = [{
                "merge_key": None,
                "tenant_id": "tenant-a",
                "customer_id": "c1",
                "customer_segment": "standard",
                "valid_from": datetime(2026, 1, 1),
                "valid_to": datetime(9999, 12, 31, 23, 59, 59, 999999),
                "is_current": True,
                "attribute_hash": "hash-1",
                "entity_bucket": 1,
            }]
            write_scd2("customer", uri, reader(contract.source_schema, initial), contract)

            same_id_other_tenant = [{
                **initial[0],
                "tenant_id": "tenant-b",
                "attribute_hash": "hash-tenant-b",
            }]
            write_scd2(
                "customer",
                uri,
                reader(contract.source_schema, same_id_other_tenant),
                contract,
            )

            changed = [
                {
                    "merge_key": "c1",
                    "tenant_id": "tenant-a",
                    "customer_id": "c1",
                    "customer_segment": "premium",
                    "valid_from": datetime(2026, 1, 2),
                    "valid_to": datetime(2026, 1, 2),
                    "is_current": False,
                    "attribute_hash": "hash-2",
                    "entity_bucket": 1,
                },
                {
                    "merge_key": None,
                    "tenant_id": "tenant-a",
                    "customer_id": "c1",
                    "customer_segment": "premium",
                    "valid_from": datetime(2026, 1, 2),
                    "valid_to": datetime(9999, 12, 31, 23, 59, 59, 999999),
                    "is_current": True,
                    "attribute_hash": "hash-2",
                    "entity_bucket": 1,
                },
            ]
            write_scd2("customer", uri, reader(contract.source_schema, changed), contract)
            # A retry using a stale Trino target snapshot must be a no-op.
            write_scd2("customer", uri, reader(contract.source_schema, changed), contract)
            rows = DeltaTable(uri).to_pyarrow_table().to_pylist()
            self.assertEqual(len(rows), 3)
            self.assertEqual(sum(row["is_current"] for row in rows), 2)
            old = next(
                row
                for row in rows
                if row["tenant_id"] == "tenant-a" and row["attribute_hash"] == "hash-1"
            )
            self.assertEqual(old["valid_to"], datetime(2026, 1, 2))
            tenant_b = next(row for row in rows if row["tenant_id"] == "tenant-b")
            self.assertTrue(tenant_b["is_current"])

    def test_transaction_partition_retry_replaces_rows(self) -> None:
        contract = load_contract(PROJECT / "contracts" / "transactions.json")
        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "transactions")
            business_date = date(2026, 1, 1)
            window_end = business_date + timedelta(days=1)
            base = {
                "tenant_id": "tenant-a",
                "transaction_id": "t1",
                "account_id": "a1",
                "customer_id": "c1",
                "transaction_ts": datetime(2026, 1, 1, 12),
                "business_date": business_date,
                "amount": Decimal("10.00"),
                "currency": "EUR",
            }
            write_transactions(
                uri, business_date, window_end, reader(contract.source_schema, [base]), contract
            )
            replacement = {**base, "amount": Decimal("12.00")}
            write_transactions(
                uri, business_date, window_end, reader(contract.source_schema, [replacement]), contract
            )
            rows = DeltaTable(uri).to_pyarrow_table().to_pylist()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["amount"], Decimal("12.00"))


if __name__ == "__main__":
    unittest.main()
