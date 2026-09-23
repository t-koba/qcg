#!/usr/bin/env bash
# Verifies that every capability in the matrix names a live test.
#
# The matrix is the contract-level claim list: each row maps a user-visible
# guarantee to the test that covers it. This check fails when a named test no
# longer exists, so a guarantee cannot be silently dropped while its claim
# stays behind (ADR 0001: no inert promises).
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
matrix="$repo_root/docs/internal/capability-matrix.md"
missing=0

while IFS= read -r test_name; do
  if ! grep -rqE "fn[[:space:]]+${test_name}[[:space:]]*\(" "$repo_root/crates" "$repo_root/tests"; then
    echo "capability matrix names a missing test: ${test_name}" >&2
    missing=1
  fi
done < <(awk -F'|' 'NF >= 3 { print $3 }' "$matrix" | grep -oE '`[a-z0-9_]+`' | tr -d '`' | sort -u)

if [ "$missing" -ne 0 ]; then
  exit 1
fi
echo "capability matrix check passed"
