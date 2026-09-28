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

# H06-01: a producer that emits one valid name then exits 2 must fail
# closed instead of validating the partial prefix.
cp "$matrix" "$work/good4.md"
printf '| Partial read | `real_test_placeholder` |\n| Unread tail | `nonexistent_test_xyz_123` |\n' >"$matrix"
mkdir -p "$work/fakebin"
real_grep="$(command -v grep)"
printf '#!/usr/bin/env bash\nif [[ "${1:-}" == "-oE" ]]; then\n  printf "`real_test_placeholder`\\n"\n  echo "grep: injected I/O failure after partial output" >&2\n  exit 2\nfi\nexec "%s" "$@"\n' "$real_grep" >"$work/fakebin/grep"
printf '%s' "$(cat "$work/fakebin/grep" | sed "s/printf \"\`real_test_placeholder\`\\\\n\"/printf '\`real_test_placeholder\`\\\\n'/")" >"$work/fakebin/grep"
chmod 755 "$work/fakebin/grep"
# Seed a live test name matching the partial output so the gate would pass
# if it validated the prefix.
if ! grep -rq "real_test_placeholder" "$repo_root" 2>/dev/null; then
  # Use a known-live test name instead: rewrite the matrix and wrapper to
  # use an actually existing test discovered from the tree.
  live_name="$(grep -rhoE 'fn[[:space:]]+[a-z0-9_]+[[:space:]]*(\(|<)' "$repo_root" --exclude-dir=.git --exclude-dir=target --exclude-dir=node_modules 2>/dev/null | head -n 1 | grep -oE '[a-z0-9_]+' | head -n 1)"
  if [[ -n "${live_name:-}" ]]; then
    printf '| Partial read | `%s` |\n| Unread tail | `nonexistent_test_xyz_123` |\n' "$live_name" >"$matrix"
    printf '#!/usr/bin/env bash\nif [[ "${1:-}" == "-oE" ]]; then\n  printf '\''`%s`\\n'\''\n  echo "grep: injected I/O failure after partial output" >&2\n  exit 2\nfi\nexec "%s" "$@"\n' "$live_name" "$real_grep" >"$work/fakebin/grep"
    chmod 755 "$work/fakebin/grep"
  fi
fi
PATH="$work/fakebin:$PATH" expect_fail "H06-01 partial producer exit 2" bash "$check"
cp "$work/good4.md" "$matrix"
rm -rf "$work/fakebin"

# H06-03: consumer failures fail closed too.
cp "$matrix" "$work/good5.md"
mkdir -p "$work/fakebin2"
printf '#!/usr/bin/env bash\necho "tr: injected failure" >&2\nexit 1\n' >"$work/fakebin2/tr"
chmod 755 "$work/fakebin2/tr"
PATH="$work/fakebin2:$PATH" expect_fail "H06-03 tr failure" bash "$check"
rm -rf "$work/fakebin2"
mkdir -p "$work/fakebin3"
printf '#!/usr/bin/env bash\ncat > /dev/null\necho "sort: injected failure" >&2\nexit 1\n' >"$work/fakebin3/sort"
chmod 755 "$work/fakebin3/sort"
PATH="$work/fakebin3:$PATH" expect_fail "H06-03 sort failure" bash "$check"
rm -rf "$work/fakebin3"
cp "$work/good5.md" "$matrix"

echo "capability gate self-test: $pass passed, $fail failed"
[[ "$fail" -eq 0 ]]
