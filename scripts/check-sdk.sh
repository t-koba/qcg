#!/usr/bin/env bash
# Fails when the checked-in SDKs are stale relative to docs/openapi.json.
#
# Generation runs into a temporary directory and the result is diffed against
# clients/, so the check never rewrites the worktree and never depends on a
# clean git index. CI re-runs it and requires a zero diff, so a spec change
# without a regenerated SDK cannot land.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
generated="$(mktemp -d "${TMPDIR:-/tmp}/qcg-sdk-check.XXXXXX")"
trap 'rm -rf "$generated"' EXIT

node "$repo_root/scripts/generate-sdk.mjs" "$generated" >/dev/null

status=0
for relative in README.md ts/client.ts ts/types.ts python/qcg_client.py; do
  if ! diff -u "$repo_root/clients/$relative" "$generated/$relative" >"$generated/diff.txt"; then
    echo "clients/$relative differs from the generator output" >&2
    sed 's/^/  /' "$generated/diff.txt" >&2
    status=1
  fi
done
if [ "$status" -ne 0 ]; then
  echo "checked-in SDKs are stale; run node scripts/generate-sdk.mjs" >&2
  exit "$status"
fi
echo "SDK check passed"
