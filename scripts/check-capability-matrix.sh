#!/usr/bin/env bash
# Use Cargo's registered tests; source names alone are not proof of coverage.
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
listing="$(mktemp "${TMPDIR:-/tmp}/qcg-test-registry.XXXXXX")"
trap 'rm -f "$listing"' EXIT
cd "$repo_root"
cargo test --workspace -- --list >"$listing"
node scripts/check-capabilities.mjs docs/capability-matrix.md "$listing"
