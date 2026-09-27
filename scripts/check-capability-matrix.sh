#!/usr/bin/env bash
# Verifies that every capability in the matrix names a live test.
#
# The matrix is the contract-level claim list: each row maps a user-visible
# guarantee to the test that covers it. This check fails when a named test no
# longer exists, so a guarantee cannot be silently dropped while its claim
# stays behind (ADR 0001: no inert promises).
#
# G07: the producer failure must never read as success. A missing or
# unreadable matrix, a failed extraction pipeline, or zero extracted test
# names all fail closed instead of reporting "passed" over an empty input.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
# G07-03: the matrix lives at the tracked `docs/capability-matrix.md` so CI
# sees the same file as the worktree (`docs/internal/` is git-ignored and
# therefore invisible to CI, which is how the gate once passed over a
# missing input). The legacy ignored path is honored as a fallback for
# local worktrees only when the tracked file is absent.
matrix="$repo_root/docs/capability-matrix.md"
if [[ ! -f "$matrix" && -f "$repo_root/docs/internal/capability-matrix.md" ]]; then
  matrix="$repo_root/docs/internal/capability-matrix.md"
fi

# G07-01: a missing or unreadable matrix is a gate failure, never a pass.
if [[ ! -f "$matrix" ]]; then
  echo "capability matrix is missing: $matrix" >&2
  exit 1
fi
if [[ ! -r "$matrix" ]]; then
  echo "capability matrix is not readable: $matrix" >&2
  exit 1
fi

extracted="$(mktemp "${TMPDIR:-/tmp}/qcg-capability-names.XXXXXX")"
trap 'rm -f "$extracted"' EXIT

# G07: the extraction pipeline runs under pipefail in an explicit check so
# a failing producer cannot hide behind the consumer's exit status (the old
# process-substitution form masked it). Test names are backtick-quoted
# anywhere in a matrix row, independent of the column layout. A grep with
# no matches is an empty matrix (G07-02), not a pipeline error, so its
# status is tolerated here and judged by the non-empty check below.
set +o pipefail
grep -oE '`[a-z0-9_]+`' "$matrix" | tr -d '`' | sort -u >"$extracted"
set -o pipefail

# G07-02: an empty, header-only, or otherwise name-free matrix is not a
# passing gate. Zero extracted names fail closed.
if [[ ! -s "$extracted" ]]; then
  echo "capability matrix names no tests: $matrix" >&2
  exit 1
fi
count="$(wc -l <"$extracted" | tr -d '[:space:]')"
if [[ "$count" -eq 0 ]]; then
  echo "capability matrix names no tests: $matrix" >&2
  exit 1
fi

missing=0
while IFS= read -r test_name; do
  [[ -n "$test_name" ]] || continue
  # G07-03: search the whole worktree for the live test, so a test that
  # moves between directories keeps resolving instead of falsely failing
  # the gate. Generated, vendored, and ignored trees never hold Rust tests
  # and are excluded explicitly.
  if ! grep -rqE "fn[[:space:]]+${test_name}[[:space:]]*(\(|<)" \
    "$repo_root" \
    --exclude-dir=.git --exclude-dir=target --exclude-dir=node_modules \
    --exclude-dir=.admission-shards 2>/dev/null; then
    echo "capability matrix names a missing test: ${test_name}" >&2
    missing=1
  fi
done <"$extracted"

if [[ "$missing" -ne 0 ]]; then
  exit 1
fi
echo "capability matrix check passed ($count tests)"
