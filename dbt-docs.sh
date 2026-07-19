#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$project_dir"

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker Desktop is required and docker was not found on PATH." >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "Docker Desktop is installed but its engine is not running." >&2
    exit 1
fi

docker compose up -d airflow
docker compose exec -T airflow dbt build \
    --project-dir /opt/project --profiles-dir /opt/project \
    --vars "{output_uri: 's3://hybrid/delta'}"
docker compose exec -T airflow dbt docs generate \
    --project-dir /opt/project --profiles-dir /opt/project \
    --vars "{output_uri: 's3://hybrid/delta'}"
echo "dbt documentation generated at target/index.html"
