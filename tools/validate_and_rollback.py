"""Run model-scoped dbt tests and restore the prior Delta version on failure."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from deltalake import DeltaTable


def storage_options() -> dict[str, str]:
    names = (
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
        "AWS_REGION", "AWS_ENDPOINT_URL", "AWS_ALLOW_HTTP",
        "AWS_FORCE_PATH_STYLE", "AWS_S3_ALLOW_UNSAFE_RENAME",
    )
    return {name: os.environ[name] for name in names if os.getenv(name)}


def rollback(stage_result: dict) -> dict:
    uri = stage_result["delta_uri"]
    expected = int(stage_result["committed_version"])
    previous = int(stage_result["previous_version"])
    table = DeltaTable(uri, storage_options=storage_options())
    actual = table.version()
    if actual != expected:
        raise RuntimeError(
            f"Refusing to roll back {uri}: expected version {expected}, current version is {actual}"
        )
    metrics = table.restore(previous)
    rollback_commit_version = DeltaTable(uri, storage_options=storage_options()).version()
    return {
        "stage": stage_result["stage"],
        "delta_uri": uri,
        "failed_version": expected,
        "restored_from_version": previous,
        "rollback_commit_version": rollback_commit_version,
        "metrics": metrics,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=("customer", "account", "transactions"))
    parser.add_argument("--stage-result", required=True, type=json.loads)
    parser.add_argument("--project-dir", required=True, type=Path)
    parser.add_argument("--profiles-dir", type=Path)
    args = parser.parse_args()
    profiles_dir = args.profiles_dir or args.project_dir
    try:
        subprocess.run(
            [
                "dbt", "build",
                "--project-dir", str(args.project_dir),
                "--profiles-dir", str(profiles_dir),
                "--vars", json.dumps({"output_uri": os.environ["OUTPUT_URI"]}),
                "--select", args.model,
            ],
            check=True,
        )
    except subprocess.CalledProcessError:
        rollback_result = rollback(args.stage_result)
        print("dbt validation failed; committed a compensating Delta restore:")
        print(json.dumps(rollback_result, indent=2, default=str))
        raise


if __name__ == "__main__":
    main()
