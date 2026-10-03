#!/usr/bin/env bash
# G07-04: SDK behavior gate, separate from freshness (scripts/check-sdk.sh).
#
# Freshness proves the checked-in clients match the generator; this script
# proves the generated clients actually behave: SSE delivery/framing over a
# live server and chunk matrix (G04), finite consistent redirects (G05), and
# typed response readers. CI runs both gates; neither subsumes the other.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

echo "--- Python SDK behavior (G04 live HTTP + chunk matrix) ---"
python3 "$repo_root/scripts/sdk-behavior/test_sse_behavior.py"

echo "--- TypeScript SDK behavior (G04/G05 via bundled client) ---"
bundle_dir="$(mktemp -d "${TMPDIR:-/tmp}/qcg-ts-behavior.XXXXXX")"
trap 'rm -rf "$bundle_dir"' EXIT
(cd "$repo_root/frontend/generator" && npx --no-install esbuild "$repo_root/clients/ts/client.ts" \
  --format=esm --outfile="$bundle_dir/client.mjs" --log-level=error)
QCG_TS_CLIENT="$bundle_dir/client.mjs" node "$repo_root/scripts/sdk-behavior/test_sse_redirect.mjs"

echo "SDK behavior check passed"
