#!/usr/bin/env bash
# Canonical Rust/API/SDK verification, shared by local validation and OS CI.
set -euo pipefail
cd "$(dirname "$0")/.."

# Minimal in-CI visibility for existing Rust test invocation wall time.
# Uses only the portable Bash builtin SECONDS (whole-second resolution);
# stdout/stderr of the wrapped command pass through unchanged.
_qcg_test_time_header_written=0
_qcg_test_time_artifact="target/qcg-test-time-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}.md"
rm -f "$_qcg_test_time_artifact"
qcg_test_time() {
  local label="$1"
  shift
  local start=$SECONDS
  local status=0
  local result=passed
  "$@" || { status=$?; result=failed; }
  local elapsed=$((SECONDS - start))
  printf 'qcg-test-time label="%s" result=%s elapsed_s=%s\n' "$label" "$result" "$elapsed"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    if [[ $_qcg_test_time_header_written -eq 0 ]]; then
      {
        printf '### qcg test invocation wall time\n\n'
        printf 'context: RUNNER_OS=%s rustc=%s\n\n' "${RUNNER_OS:-$(uname -s)}" "$(rustc --version)"
        printf 'cargo_cache_exact_hit=%s restore_bracket_elapsed_s=%s (whole-second pre-test bracket; runner overhead included, cache post-save excluded)\n\n' "${QCG_RUST_CACHE_EXACT_HIT:-unknown}" "${QCG_RUST_CACHE_RESTORE_BRACKET_ELAPSED_S:-unknown}"
        printf 'Values use Bash SECONDS (whole-second resolution; a displayed 0 may mean <1s) and are whole-command invocation wall time, including Cargo/build overhead. '
        printf "Cargo's normal \`test result ... finished in\` lines remain target-harness timing, not per-test timing.\n\n"
        printf '| invocation | result | elapsed_s |\n'
        printf '|---|---|---|\n'
      } >>"$GITHUB_STEP_SUMMARY" || printf 'warning: qcg-test-time: failed to write header to GITHUB_STEP_SUMMARY\n' >&2
      _qcg_test_time_header_written=1
    fi
    printf '| %s | %s | %s |\n' "$label" "$result" "$elapsed" >>"$GITHUB_STEP_SUMMARY" || printf 'warning: qcg-test-time: failed to append to GITHUB_STEP_SUMMARY\n' >&2
    # Keep a machine-readable copy because the CI evidence reader can fetch
    # artifacts, but not GitHub's per-step summary files.
    if ! mkdir -p "$(dirname "$_qcg_test_time_artifact")" ||
      ! cp "$GITHUB_STEP_SUMMARY" "$_qcg_test_time_artifact"; then
      printf 'warning: qcg-test-time: failed to copy summary artifact\n' >&2
    fi
  fi
  return $status
}

node --test scripts/product.test.mjs scripts/capabilities.test.mjs scripts/spdx.test.mjs scripts/rust-line-budget.test.mjs
node scripts/rust-line-budget.mjs
cargo fmt --all -- --check
bash scripts/check-generated-docs.sh
bash scripts/check-capability-matrix.sh
bash scripts/check-sdk.sh
bash scripts/check-sdk-behavior.sh
python3 scripts/comparison/test_run_cleanup.py
bash scripts/check-third-party-notices.sh
# Clippy covers all targets here; the MSRV job keeps a separate plain workspace check.
cargo clippy --workspace --all-targets --locked -- -D warnings
qcg_test_time "default workspace" cargo test --workspace --locked
# These tests compile both the production library and tests for each feature set.
qcg_test_time "MCP no-default" cargo test -p mcp --no-default-features --locked
qcg_test_time "server no-default" cargo test -p server --no-default-features --locked
bash scripts/check-fixtures.sh
bash scripts/package-dist.sh --dry-run
if [[ "$(uname -s)" == Linux ]]; then
  qcg_test_time "MCP live server" cargo test -p mcp --features mcp-test-server --locked
  qcg_test_time "MCP no-default live server" cargo test -p mcp --no-default-features --features mcp-test-server --locked
  bash scripts/e2e-server-smoke.sh
fi
