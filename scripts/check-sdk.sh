#!/usr/bin/env bash
# Fails when the checked-in SDKs are stale relative to docs/openapi.json.
#
# The generator writes clients/ deterministically; CI re-runs it and requires
# a clean diff, so a spec change without a regenerated SDK cannot land.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
node "$repo_root/scripts/generate-sdk.mjs" >/dev/null

if ! git -C "$repo_root" diff --exit-code -- clients >/dev/null; then
  echo "checked-in SDKs are stale; run node scripts/generate-sdk.mjs" >&2
  git -C "$repo_root" diff --stat -- clients >&2
  exit 1
fi
echo "SDK check passed"
