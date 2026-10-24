#!/usr/bin/env bash
# Canonical Rust/API/SDK verification, shared by local validation and OS CI.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.."

# Minimal in-CI visibility for existing Rust test invocation wall time.
# Uses only the portable Bash builtin SECONDS (whole-second resolution);
# stdout/stderr of the wrapped command pass through unchanged.
_qcg_test_time_header_written=0
_qcg_test_time_artifact="target/qcg-test-time-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}.md"

# Measured invocation budget v1 (Bash SECONDS whole-command invocation
# wall time, including Cargo/build overhead; NOT test-only timing).
# Evidence: exact-cache-hit Linux whole-command observations, n=16 per
# invocation (15 historical plus run 40102552370 / SHA 3ffa8bed). Values
# below are mean/sample-SD/observed-max -> ceiling:
# default workspace 102.50/5.68/111 -> 120s; MCP no-default 12.00/1.26/14
# -> 16s; server no-default 33.81/4.68/41 -> 48s; MCP live server
# 19.94/2.26/24 -> 27s; MCP no-default live server 18.44/1.67/21 -> 24s.
# Each ceiling is ceil(mean+3*sample_SD). Three SD is
# an engineering envelope, not a calibrated false-alarm guarantee; changing
# thresholds requires a new policy version and fresh evidence. Apply only
# with RUNNER_OS=Linux, cargo exact cache hit=true and rustc=1.99.1. Other
# OS/groups, cache/setup brackets, CI job duration and run/day totals remain
# unbudgeted.
# Focused tests synthesize elapsed time through Bash SECONDS; production
# policy values are fixed here and cannot be raised through the environment.
QCG_TEST_TIME_BUDGET_POLICY_VERSION=v1
QCG_TEST_TIME_BUDGET_LINUX_DEFAULT_WORKSPACE_S=120
QCG_TEST_TIME_BUDGET_LINUX_MCP_NO_DEFAULT_S=16
QCG_TEST_TIME_BUDGET_LINUX_SERVER_NO_DEFAULT_S=48
QCG_TEST_TIME_BUDGET_LINUX_MCP_LIVE_SERVER_S=27
QCG_TEST_TIME_BUDGET_LINUX_MCP_NO_DEFAULT_LIVE_SERVER_S=24
QCG_TEST_TIME_BUDGET_RUSTC_VERSION=1.99.1
_qcg_rustc_version_line=""
qcg_rustc_version() {
  if [[ -z "$_qcg_rustc_version_line" ]]; then
    _qcg_rustc_version_line="$(rustc --version 2>/dev/null)" ||
      _qcg_rustc_version_line=unknown
  fi
}
# Prints the whole-command budget in seconds for a label, or nothing when
# the v1 profile does not apply. Never fails.
qcg_test_time_budget_for_label() {
  local label="$1"
  local budget
  case "$label" in
    "default workspace") budget=$QCG_TEST_TIME_BUDGET_LINUX_DEFAULT_WORKSPACE_S ;;
    "MCP no-default") budget=$QCG_TEST_TIME_BUDGET_LINUX_MCP_NO_DEFAULT_S ;;
    "server no-default") budget=$QCG_TEST_TIME_BUDGET_LINUX_SERVER_NO_DEFAULT_S ;;
    "MCP live server") budget=$QCG_TEST_TIME_BUDGET_LINUX_MCP_LIVE_SERVER_S ;;
    "MCP no-default live server") budget=$QCG_TEST_TIME_BUDGET_LINUX_MCP_NO_DEFAULT_LIVE_SERVER_S ;;
    *) return 0 ;;
  esac
  if [[ "${RUNNER_OS:-}" != "Linux" ]]; then
    return 0
  fi
  if [[ "${QCG_RUST_CACHE_EXACT_HIT:-}" != "true" ]]; then
    return 0
  fi
  local rustc_line rustc_ver
  qcg_rustc_version
  rustc_line="$_qcg_rustc_version_line"
  if [[ "$rustc_line" =~ ^rustc[[:space:]]+([^[:space:]]+) ]]; then
    rustc_ver="${BASH_REMATCH[1]}"
  else
    return 0
  fi
  if [[ "$rustc_ver" != "$QCG_TEST_TIME_BUDGET_RUSTC_VERSION" ]]; then
    return 0
  fi
  printf '%s' "$budget"
}
# Checks one elapsed measurement against the v1 profile. Prints an
# actionable failure to stderr and returns 1 only when a budget applies and
# is exceeded; returns 0 for within-budget or no-budget.
qcg_test_time_check_budget() {
  local label="$1" elapsed="$2"
  local budget
  budget="$(qcg_test_time_budget_for_label "$label")"
  if [[ -z "$budget" ]]; then
    return 0
  fi
  if [[ "$elapsed" =~ ^[0-9]+$ ]] && [[ "$budget" =~ ^[0-9]+$ ]] && (( elapsed > budget )); then
    printf 'qcg-test-time-budget exceeded: label="%s" elapsed_s=%s budget_s=%s policy=%s (Bash SECONDS whole-command invocation wall time, including Cargo/build overhead). Action: investigate the slowdown from the qcg-test-time-* timing artifact; do not raise the budget to hide a regression (budgets ratchet down only after speedups with new evidence).\n' "$label" "$elapsed" "$budget" "$QCG_TEST_TIME_BUDGET_POLICY_VERSION" >&2
    return 1
  fi
  return 0
}
qcg_test_time() {
  local label="$1"
  shift
  local start=$SECONDS
  local status=0
  local result=passed
  "$@" || { status=$?; result=failed; }
  local elapsed=$((SECONDS - start))
  printf 'qcg-test-time label="%s" result=%s elapsed_s=%s\n' "$label" "$result" "$elapsed"
  local budget budget_display budget_result=none budget_status=0
  budget="$(qcg_test_time_budget_for_label "$label")"
  if [[ -z "$budget" ]]; then
    budget_display=none
    budget_result=none
    printf 'qcg-test-time-budget policy=%s label="%s" budget_s=none elapsed_s=%s verdict=none\n' "$QCG_TEST_TIME_BUDGET_POLICY_VERSION" "$label" "$elapsed"
  else
    budget_display="$budget"
    if qcg_test_time_check_budget "$label" "$elapsed"; then
      budget_result=within
    else
      budget_result=exceeded
      budget_status=1
    fi
    printf 'qcg-test-time-budget policy=%s label="%s" budget_s=%s elapsed_s=%s verdict=%s\n' "$QCG_TEST_TIME_BUDGET_POLICY_VERSION" "$label" "$budget" "$elapsed" "$budget_result"
  fi
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    if [[ $_qcg_test_time_header_written -eq 0 ]]; then
      {
        printf '### qcg test invocation wall time\n\n'
        qcg_rustc_version
        printf 'context: RUNNER_OS=%s rustc=%s\n\n' "${RUNNER_OS:-$(uname -s)}" "$_qcg_rustc_version_line"
        printf 'cargo_cache_exact_hit=%s restore_bracket_elapsed_s=%s (whole-second pre-test bracket; runner overhead included, cache post-save excluded)\n\n' "${QCG_RUST_CACHE_EXACT_HIT:-unknown}" "${QCG_RUST_CACHE_RESTORE_BRACKET_ELAPSED_S:-unknown}"
        printf 'timing_budget_policy=%s Linux_exact_hit_rustc=%s budgets_s={default workspace:%s; MCP no-default:%s; server no-default:%s; MCP live server:%s; MCP no-default live server:%s}; other profiles unbudgeted. Values are Bash SECONDS whole-command invocation wall time including Cargo/build overhead, not test-only timing.\n\n' "$QCG_TEST_TIME_BUDGET_POLICY_VERSION" "$QCG_TEST_TIME_BUDGET_RUSTC_VERSION" "$QCG_TEST_TIME_BUDGET_LINUX_DEFAULT_WORKSPACE_S" "$QCG_TEST_TIME_BUDGET_LINUX_MCP_NO_DEFAULT_S" "$QCG_TEST_TIME_BUDGET_LINUX_SERVER_NO_DEFAULT_S" "$QCG_TEST_TIME_BUDGET_LINUX_MCP_LIVE_SERVER_S" "$QCG_TEST_TIME_BUDGET_LINUX_MCP_NO_DEFAULT_LIVE_SERVER_S"
        printf 'timing_budget_basis=Each Linux invocation ceiling is ceil(mean+3*sample_sd) from n=16 exact-cache-hit passes (15 historical plus run 40102552370, SHA 3ffa8bed); three SD is an engineering margin, not a calibrated false-alarm guarantee.\n\n'
        printf 'Values use Bash SECONDS (whole-second resolution; a displayed 0 may mean <1s) and are whole-command invocation wall time, including Cargo/build overhead. '
        printf "Cargo's normal \`test result ... finished in\` lines remain target-harness timing, not per-test timing.\n\n"
        printf '| invocation | result | elapsed_s | budget_s | budget_result |\n'
        printf '|---|---|---|---|---|\n'
      } >>"$GITHUB_STEP_SUMMARY" || printf 'warning: qcg-test-time: failed to write header to GITHUB_STEP_SUMMARY\n' >&2
      _qcg_test_time_header_written=1
    fi
    printf '| %s | %s | %s | %s | %s |\n' "$label" "$result" "$elapsed" "$budget_display" "$budget_result" >>"$GITHUB_STEP_SUMMARY" || printf 'warning: qcg-test-time: failed to append to GITHUB_STEP_SUMMARY\n' >&2
    if [[ "$budget_result" == "exceeded" ]]; then
      printf '**qcg-test-time-budget exceeded**: label="%s" elapsed_s=%s budget_s=%s policy=%s; investigate the slowdown from the qcg-test-time-* timing artifact; do not raise the budget to hide a regression.\n\n' "$label" "$elapsed" "$budget_display" "$QCG_TEST_TIME_BUDGET_POLICY_VERSION" >>"$GITHUB_STEP_SUMMARY" || printf 'warning: qcg-test-time: failed to append budget verdict to GITHUB_STEP_SUMMARY\n' >&2
    fi
    # Keep a machine-readable copy because the CI evidence reader can fetch
    # artifacts, but not GitHub's per-step summary files.
    if ! mkdir -p "$(dirname "$_qcg_test_time_artifact")" ||
      ! cp "$GITHUB_STEP_SUMMARY" "$_qcg_test_time_artifact"; then
      printf 'warning: qcg-test-time: failed to copy summary artifact\n' >&2
    fi
  fi
  if [[ "$budget_status" -ne 0 && "$status" -eq 0 ]]; then
    status=1
  fi
  return $status
}

# Focused timing-budget tests source the helpers above without running the
# verification below.
if [[ "${BASH_SOURCE[0]:-}" != "${0}" ]]; then
  return 0
fi
rm -f "$_qcg_test_time_artifact"

node --test scripts/product.test.mjs scripts/capabilities.test.mjs scripts/spdx.test.mjs scripts/rust-line-budget.test.mjs scripts/check-core-timing-budget.test.mjs
node scripts/rust-line-budget.mjs
cargo fmt --all -- --check
bash scripts/check-generated-docs.sh
bash scripts/check-capability-matrix.sh
bash scripts/check-sdk.sh
bash scripts/check-sdk-behavior.sh
python3 scripts/comparison/test_run_cleanup.py
python3 scripts/test-ci-job-minutes.py
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
