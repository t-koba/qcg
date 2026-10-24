import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const RUSTC_PINNED = "1.99.1";
const BUDGETED_ENV = {
  RUNNER_OS: "Linux",
  QCG_RUST_CACHE_EXACT_HIT: "true",
  TEST_RUSTC_VERSION: RUSTC_PINNED,
};

function bash(script, env = {}) {
  const sourceLine = "source scripts/check-core.sh";
  const rustcStub =
    "qcg_rustc_version() { _qcg_rustc_version_line=\"rustc ${TEST_RUSTC_VERSION:-1.99.1} (test)\"; }";
  script = script.replace(sourceLine, `${sourceLine}\n${rustcStub}`);
  return spawnSync("bash", ["-c", script], {
    encoding: "utf8",
    env: { ...process.env, ...env },
  });
}

function budgetFor(label, env = {}) {
  const r = bash(`source scripts/check-core.sh
printf '%s' "$(qcg_test_time_budget_for_label ${JSON.stringify(label)})"`, {
    ...BUDGETED_ENV,
    ...env,
  });
  assert.equal(r.status, 0, r.stderr);
  return r.stdout;
}

test("v1 profile budgets only Linux exact-cache-hit rustc-1.99.1 default workspace", () => {
  assert.equal(budgetFor("default workspace"), "120");
  assert.equal(
    budgetFor("default workspace", {
      QCG_TEST_TIME_BUDGET_LINUX_DEFAULT_WORKSPACE_S: "9999",
    }),
    "120",
  );
  assert.equal(budgetFor("MCP no-default"), "16");
  assert.equal(budgetFor("server no-default"), "48");
  assert.equal(budgetFor("MCP live server"), "27");
  assert.equal(budgetFor("MCP no-default live server"), "24");
  assert.equal(budgetFor("unknown invocation"), "");
  assert.equal(budgetFor("default workspace ", BUDGETED_ENV), "");
  assert.equal(
    budgetFor("default workspace", { RUNNER_OS: "macOS" }),
    "",
  );
  assert.equal(
    budgetFor("default workspace", { RUNNER_OS: "Windows" }),
    "",
  );
  assert.equal(budgetFor("default workspace", { RUNNER_OS: "" }), "");
  assert.equal(
    budgetFor("default workspace", { QCG_RUST_CACHE_EXACT_HIT: "false" }),
    "",
  );
  assert.equal(
    budgetFor("default workspace", { QCG_RUST_CACHE_EXACT_HIT: "" }),
    "",
  );
  assert.equal(
    budgetFor("default workspace", {
      TEST_RUSTC_VERSION: "1.99.0",
    }),
    "",
  );
  assert.equal(
    budgetFor("default workspace", {
      TEST_RUSTC_VERSION: "1.99.10",
    }),
    "",
  );
  assert.equal(
    budgetFor("default workspace", { TEST_RUSTC_VERSION: "unknown" }),
    "",
  );
});

test("budget verdict is synthetic: within at the bound, exceeded above it", () => {
  let r = bash(
    `source scripts/check-core.sh
set +e
qcg_test_time_check_budget "default workspace" 0
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /rc=0/);

  r = bash(
    `source scripts/check-core.sh
set +e
qcg_test_time_check_budget "default workspace" 120
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /rc=0/);

  r = bash(
    `source scripts/check-core.sh
set +e
qcg_test_time_check_budget "default workspace" 121
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /rc=1/);
  assert.match(r.stderr, /qcg-test-time-budget exceeded/);
  assert.match(r.stderr, /label="default workspace"/);
  assert.match(r.stderr, /budget_s=120/);
  assert.match(r.stderr, /policy=v1/);

  // Unbudgeted labels never exceed, even far above the budget.
  r = bash(
    `source scripts/check-core.sh
set +e
qcg_test_time_check_budget "unknown invocation" 10000
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /rc=0/);
  assert.doesNotMatch(r.stderr, /exceeded/);
});

test("timed wrapper keeps output, fails on exceed, preserves prior failure", () => {
  // Within budget: instant command passes with the v1 budget line.
  let r = bash(
    `source scripts/check-core.sh
unset GITHUB_STEP_SUMMARY
set +e
qcg_test_time "default workspace" true
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /qcg-test-time label="default workspace" result=passed elapsed_s=/);
  assert.match(
    r.stdout,
    /qcg-test-time-budget policy=v1 label="default workspace" budget_s=120 elapsed_s=\d+ verdict=within/,
  );

  // Unbudgeted label reports no budget and still passes.
  r = bash(
    `source scripts/check-core.sh
unset GITHUB_STEP_SUMMARY
set +e
qcg_test_time "unknown invocation" true
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /budget_s=none elapsed_s=\d+ verdict=none/);

  // Synthetic Bash SECONDS avoids sleeping while exercising the wrapper.
  r = bash(
    `source scripts/check-core.sh
unset GITHUB_STEP_SUMMARY
fake_over_budget() { SECONDS=$((start + 121)); }
set +e
qcg_test_time "default workspace" fake_over_budget
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /result=passed/);
  assert.match(r.stdout, /budget_s=120 elapsed_s=121 verdict=exceeded/);
  assert.match(r.stderr, /qcg-test-time-budget exceeded/);
  assert.match(r.stdout, /rc=1/);

  // A preexisting command failure keeps its own exit code on exceed.
  r = bash(
    `source scripts/check-core.sh
unset GITHUB_STEP_SUMMARY
fake_failed_over_budget() { SECONDS=$((start + 121)); return 42; }
set +e
qcg_test_time "default workspace" fake_failed_over_budget
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /result=failed/);
  assert.match(r.stdout, /budget_s=120 elapsed_s=121 verdict=exceeded/);
  assert.match(r.stderr, /qcg-test-time-budget exceeded/);
  assert.match(r.stdout, /rc=42/);
});

test("timing report carries policy version, budget columns, and exceed note", () => {
  const dir = mkdtempSync(join(tmpdir(), "qcg-timing-budget-"));
  const summary = join(dir, "summary.md");
  const runId = `timing-budget-${process.pid}`;
  const artifact = `target/qcg-test-time-${runId}-0.md`;
  try {
    const r = bash(
      `source scripts/check-core.sh
rm -f "$GITHUB_STEP_SUMMARY"
set +e
qcg_test_time "default workspace" true
qcg_test_time "MCP no-default" true
fake_over_budget() { SECONDS=$((start + 121)); }
qcg_test_time "default workspace" fake_over_budget
echo "rc=$?"`,
      {
        ...BUDGETED_ENV,
        GITHUB_STEP_SUMMARY: summary,
        GITHUB_RUN_ID: runId,
        GITHUB_RUN_ATTEMPT: "0",
      },
    );
    assert.equal(r.status, 0, r.stderr);
    const text = readFileSync(summary, "utf8");
    assert.match(text, /timing_budget_policy=v1/);
    assert.match(text, /budgets_s=\{default workspace:120; MCP no-default:16; server no-default:48;/);
    assert.match(text, /MCP live server:27; MCP no-default live server:24\}/);
    assert.match(text, /timing_budget_basis=Each Linux invocation ceiling is ceil\(mean\+3\*sample_sd\) from n=16/);
    assert.match(
      text,
      /\| invocation \| result \| elapsed_s \| budget_s \| budget_result \|/,
    );
    assert.match(text, /\| default workspace \| passed \| \d+ \| 120 \| within \|/);
    assert.match(text, /\| MCP no-default \| passed \| \d+ \| 16 \| within \|/);
    assert.match(text, /\| default workspace \| passed \| 121 \| 120 \| exceeded \|/);
    assert.match(text, /\*\*qcg-test-time-budget exceeded\*\*/);
    const artifactText = readFileSync(artifact, "utf8");
    assert.match(artifactText, /timing_budget_policy=v1/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
    rmSync(artifact, { force: true });
  }
});
