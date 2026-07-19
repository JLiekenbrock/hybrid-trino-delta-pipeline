from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "tools"))

from contracts import load_contract  # noqa: E402
from run_partition import initialize_empty_table  # noqa: E402
from validate_and_rollback import rollback  # noqa: E402


class RollbackTest(unittest.TestCase):
    def test_restore_and_refuse_stale_retry(self) -> None:
        contract = load_contract(PROJECT / "contracts" / "customer.json")
        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "customer")
            initialize_empty_table(uri, contract)
            row = {
                "tenant_id": "tenant-a",
                "customer_id": "c1",
                "customer_segment": "standard",
                "valid_from": datetime(2026, 1, 1),
                "valid_to": datetime(9999, 12, 31, 23, 59, 59, 999999),
                "is_current": True,
                "attribute_hash": "hash-1",
                "entity_bucket": 1,
            }
            write_deltalake(
                uri,
                pa.Table.from_pylist([row], schema=contract.target_schema),
                mode="append",
            )
            stage_result = {
                "stage": "customer",
                "delta_uri": uri,
                "previous_version": 0,
                "committed_version": 1,
            }

            result = rollback(stage_result)

            self.assertEqual(result["rollback_commit_version"], 2)
            self.assertEqual(DeltaTable(uri).count(), 0)
            with self.assertRaisesRegex(RuntimeError, "Refusing to roll back"):
                rollback(stage_result)


if __name__ == "__main__":
    unittest.main()
