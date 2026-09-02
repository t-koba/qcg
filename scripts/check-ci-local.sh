#!/usr/bin/env bash
set -euo pipefail

run_step() {
  echo "==> $*"
  "$@"
}

run_step cargo fmt --all -- --check
run_step cargo check --workspace --locked
run_step cargo check -p qcg-expr-wasm --target wasm32-unknown-unknown --locked
run_step bash scripts/check-generated-docs.sh
run_step bash scripts/check-third-party-notices.sh
run_step cargo clippy --workspace --all-targets --locked -- -D warnings
run_step cargo test --workspace --locked -- --test-threads=1
run_step bash scripts/check-test-count.sh 520
run_step bash scripts/check-fixtures.sh
run_step bash scripts/e2e-server-smoke.sh
run_step bash scripts/package-dist.sh --dry-run

echo "local core CI audit ok"
