#!/bin/sh
set -eu

action="${1:-start}"
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker Desktop is required and docker was not found on PATH." >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "Docker Desktop is installed but its engine is not running." >&2
    exit 1
fi

cd "$project_dir"
case "$action" in
    start)
        docker compose up --build --detach
        echo "Airflow is starting at http://localhost:8080"
        echo "Run ./airflow-local.sh logs to view the generated admin password."
        ;;
    stop) docker compose down ;;
    logs) docker compose logs --follow airflow ;;
    status) docker compose ps ;;
    *) echo "Usage: $0 [start|stop|logs|status]" >&2; exit 2 ;;
esac
