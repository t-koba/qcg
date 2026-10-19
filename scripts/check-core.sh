#!/usr/bin/env bash
# Canonical Rust/API/SDK verification, shared by local validation and OS CI.
set -euo pipefail
cd "$(dirname "$0")/.."
node --test scripts/product.test.mjs scripts/capabilities.test.mjs scripts/spdx.test.mjs scripts/rust-line-budget.test.mjs
node scripts/rust-line-budget.mjs
cargo fmt --all -- --check
cargo check -p expr-wasm --target wasm32-unknown-unknown --locked
cargo check -p mcp --no-default-features --locked
cargo check -p server --no-default-features --locked
bash scripts/check-generated-docs.sh
bash scripts/check-capability-matrix.sh
bash scripts/check-sdk.sh
bash scripts/check-sdk-behavior.sh
python3 scripts/comparison/test_run_cleanup.py
bash scripts/check-third-party-notices.sh
# Clippy covers all targets here; the MSRV job keeps a separate plain workspace check.
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test -p mcp --no-default-features --locked
cargo test -p server --no-default-features --locked
bash scripts/check-fixtures.sh
bash scripts/package-dist.sh --dry-run
if [[ "$(uname -s)" == Linux ]]; then
  cargo test -p mcp --features mcp-test-server --locked
  cargo test -p mcp --no-default-features --features mcp-test-server --locked
  bash scripts/e2e-server-smoke.sh
fi
