#!/usr/bin/env bash
# G07-01..G07-03 self-test for scripts/check-capability-matrix.sh.
#
# The gate itself is release-blocking, so its fail-closed behavior is pinned
# here: a missing/unreadable matrix, an empty/header-only matrix, and an
# unknown test name must all exit non-zero, while the checked-in matrix
# passes. CI runs this so a future edit cannot silently reintroduce the old
# "empty input reads as passed" shape.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
check="$repo_root/scripts/check-capability-matrix.sh"
matrix="$repo_root/docs/capability-matrix.md"
work="$(mktemp -d "${TMPDIR:-/tmp}/qcg-matrix-gate.XXXXXX")"
trap 'rm -rf "$work"' EXIT

pass=0
fail=0
expect_fail() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "gate self-test FAILED (expected non-zero): $name" >&2
    fail=$((fail + 1))
  else
    echo "gate self-test passed (fails closed): $name"
    pass=$((pass + 1))
  fi
}
expect_pass() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "gate self-test passed: $name"
    pass=$((pass + 1))
  else
    echo "gate self-test FAILED (expected zero): $name" >&2
    fail=$((fail + 1))
  fi
}

# G07-01: missing matrix fails.
cp "$matrix" "$work/good.md"
mv "$matrix" "$work/hidden.md"
expect_fail "G07-01 missing matrix" bash "$check"
mv "$work/hidden.md" "$matrix"

# G07-01b: unreadable matrix fails. Some platforms (Windows ACLs, root)
# cannot enforce mode-based unreadability: after chmod the file may still
# read, in which case the case is skipped instead of asserting a failure
# the platform cannot produce.
chmod 000 "$matrix"
if [[ -r "$matrix" ]]; then
  echo "gate self-test skipped (platform cannot enforce unreadable): G07-01 unreadable matrix"
  chmod 644 "$matrix"
else
  expect_fail "G07-01 unreadable matrix" bash "$check"
  chmod 644 "$matrix"
fi

# G07-02: empty / header-only / name-free matrices fail.
: >"$work/empty.md"
cp "$matrix" "$work/good2.md"
cp "$work/empty.md" "$matrix"
expect_fail "G07-02 empty matrix" bash "$check"
printf '# Title\n\n| ID | Guarantee | Test |\n' >"$matrix"
expect_fail "G07-02 header-only matrix" bash "$check"
printf '# Title\n\nNo backtick names here.\n' >"$matrix"
expect_fail "G07-02 name-free matrix" bash "$check"
cp "$work/good2.md" "$matrix"

# G07-03: unknown test name fails; the checked-in matrix passes.
cp "$matrix" "$work/good3.md"
echo '| X | Y | `nonexistent_test_xyz_123` |' >>"$matrix"
expect_fail "G07-03 unknown test name" bash "$check"
cp "$work/good3.md" "$matrix"
expect_pass "G07-03 valid matrix" bash "$check"

echo "capability gate self-test: $pass passed, $fail failed"
[[ "$fail" -eq 0 ]]
