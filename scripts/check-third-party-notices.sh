#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
node scripts/generate-third-party-notices.mjs "$tmp"
cmp -s "$tmp" THIRD-PARTY-NOTICES || {
  echo "THIRD-PARTY-NOTICES is stale; regenerate it with node scripts/generate-third-party-notices.mjs THIRD-PARTY-NOTICES" >&2
  exit 1
}
echo "third-party notices are current"
