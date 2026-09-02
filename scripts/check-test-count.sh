#!/usr/bin/env bash
set -euo pipefail

minimum="${1:-520}"
count="$(
  cargo test --workspace --locked -- --list \
    | awk '/ tests, 0 benchmarks$/ { total += $1 } END { print total + 0 }'
)"

echo "test count: ${count}"
if [ "$count" -lt "$minimum" ]; then
  echo "expected at least ${minimum} tests" >&2
  exit 1
fi
