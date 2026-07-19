#!/bin/sh
set -eu

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 DATE {customer|account|transactions} [additional arguments]" >&2
    exit 2
fi

business_date=$1
stage=$2
shift 2
case "$stage" in
    customer|account|transactions) ;;
    *) echo "Stage must be customer, account, or transactions." >&2; exit 2 ;;
esac

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
python_command="${PYTHON:-python3}"
if ! command -v "$python_command" >/dev/null 2>&1; then
    echo "Python command '$python_command' was not found." >&2
    exit 1
fi

exec "$python_command" "$project_dir/tools/run_partition.py" \
    --date "$business_date" --stage "$stage" "$@"
