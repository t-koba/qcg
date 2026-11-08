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

test("v3 profile budgets only Linux exact-cache-hit rustc-1.99.1 default workspace", () => {
  assert.equal(budgetFor("default workspace"), "151");
  assert.equal(
    budgetFor("default workspace", {
      QCG_TEST_TIME_BUDGET_LINUX_DEFAULT_WORKSPACE_S: "9999",
    }),
    "151",
  );
  assert.equal(budgetFor("MCP no-default"), "16");
  assert.equal(budgetFor("server no-default"), "16");
  assert.equal(budgetFor("MCP live server"), "27");
  assert.equal(budgetFor("MCP no-default live server"), "24");
  assert.equal(budgetFor("server no-default build"), "");
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

test("every timing ceiling follows its recorded evidence", () => {
  // Each ceiling line records the sample statistics it came from, so the
  // recorded statistics, the documented ceiling and the constant must agree.
  const policy = readFileSync("scripts/check-core.sh", "utf8");
  const lines = [
    ...policy.matchAll(
      /^#   (.+): mean ([\d.]+), sample SD ([\d.]+), max (\d+) -> (\d+)s$/gm,
    ),
  ];
  assert.equal(
    lines.length,
    4,
    "policy comment must retain four statistical ceilings",
  );
  const server = policy.match(
    /^#   server no-default: harness sample \(([\d., ]+) s\), margin ([\d.]+) -> (\d+)s$/m,
  );
  assert.ok(server, "policy must record the post-build budget basis");
  const harnessSample = server[1].split(", ").map(Number);
  assert.deepEqual(harnessSample, [10.23, 10.40]);
  assert.equal(Number(server[2]), 1.5);
  assert.equal(
    Math.ceil(Number(server[2]) * Math.max(...harnessSample)),
    Number(server[3]),
  );
  assert.equal(Number(server[3]), Number(budgetFor("server no-default")));
  assert.ok(
    policy.includes(
      'qcg_test_time "server no-default build" cargo test -p server --no-default-features --locked --no-run\n' +
        'qcg_test_time "server no-default" cargo test -p server --no-default-features --locked\n',
    ),
    "build must precede the unchanged feature-off test command",
  );
  const sampleMatch = policy.match(/\(([\d, ]+) s\)\./);
  assert.ok(sampleMatch, "policy comment must record the default-workspace sample");
  const sample = sampleMatch[1].split(", ").map(Number);
  const sampleMean = sample.reduce((sum, value) => sum + value, 0) / sample.length;
  const sampleSd = Math.sqrt(
    sample.reduce((sum, value) => sum + (value - sampleMean) ** 2, 0) /
      (sample.length - 1),
  );
  for (const [, label, mean, sd, max, ceiling] of lines) {
    if (label === "default workspace") {
      assert.equal(Number(mean).toFixed(2), sampleMean.toFixed(2));
      assert.equal(Number(sd).toFixed(2), sampleSd.toFixed(2));
      assert.equal(Number(max), Math.max(...sample));
    }
    assert.equal(
      Math.ceil(Number(mean) + 3 * Number(sd)),
      Number(ceiling),
      `${label}: ceiling must be ceil(mean+3*sample_SD)`,
    );
    assert.ok(Number(max) <= Number(ceiling), `${label}: max must fit the ceiling`);
    assert.equal(
      Number(ceiling),
      Number(budgetFor(label)),
      `${label}: recorded ceiling must equal the configured budget`,
    );
  }
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
qcg_test_time_check_budget "default workspace" 151
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /rc=0/);

  r = bash(
    `source scripts/check-core.sh
set +e
qcg_test_time_check_budget "default workspace" 152
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /rc=1/);
  assert.match(r.stderr, /qcg-test-time-budget exceeded/);
  assert.match(r.stderr, /label="default workspace"/);
  assert.match(r.stderr, /budget_s=151/);
  assert.match(r.stderr, /policy=v3/);

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
  // Within budget: instant command passes with the budget line.
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
    /qcg-test-time-budget policy=v3 label="default workspace" budget_s=151 elapsed_s=\d+ verdict=within/,
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
fake_over_budget() { SECONDS=$((start + 152)); }
set +e
qcg_test_time "default workspace" fake_over_budget
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /result=passed/);
  assert.match(r.stdout, /budget_s=151 elapsed_s=152 verdict=exceeded/);
  assert.match(r.stderr, /qcg-test-time-budget exceeded/);
  assert.match(r.stdout, /rc=1/);

  // A preexisting command failure keeps its own exit code on exceed.
  r = bash(
    `source scripts/check-core.sh
unset GITHUB_STEP_SUMMARY
fake_failed_over_budget() { SECONDS=$((start + 152)); return 42; }
set +e
qcg_test_time "default workspace" fake_failed_over_budget
echo "rc=$?"`,
    BUDGETED_ENV,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /result=failed/);
  assert.match(r.stdout, /budget_s=151 elapsed_s=152 verdict=exceeded/);
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
qcg_test_time "server no-default build" true
qcg_test_time "server no-default" true
fake_over_budget() { SECONDS=$((start + 152)); }
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
    assert.match(text, /timing_budget_policy=v3/);
    assert.match(text, /budgets_s=\{default workspace:151; MCP no-default:16; server no-default:16;/);
    assert.match(text, /MCP live server:27; MCP no-default live server:24\}/);
    assert.ok(
      text.includes(
        "timing_budget_basis=server no-default: ceil(1.5 * max(10.23, 10.40))=16s from observed harness durations, with room for Cargo overhead; other four ceilings retain ceil(mean+3*sample_sd) from their exact-cache-hit samples. Engineering margins, not calibrated false-alarm guarantees. Prior same-source whole invocations were 54s (43.75s build + 10.23s harness) and 51s (39.94s build + 10.40s harness), versus a prior same-image 42s; 54s/48s failed v2 despite all 195 tests passing.",
      ),
      "timing summary must carry the complete basis sentence",
    );
    assert.match(
      text,
      /\| invocation \| result \| elapsed_s \| budget_s \| budget_result \|/,
    );
    assert.match(text, /\| default workspace \| passed \| \d+ \| 151 \| within \|/);
    assert.match(text, /\| MCP no-default \| passed \| \d+ \| 16 \| within \|/);
    assert.match(text, /\| default workspace \| passed \| 152 \| 151 \| exceeded \|/);
    assert.match(text, /server no-default measures the post-build Cargo test invocation/);
    assert.match(text, /server no-default build is a separate visible, unbudgeted diagnostic/);
    assert.match(text, /\| server no-default build \| passed \| \d+ \| none \| none \|/);
    assert.match(text, /\| server no-default \| passed \| \d+ \| 16 \| within \|/);
    assert.match(text, /\*\*qcg-test-time-budget exceeded\*\*/);
    const artifactText = readFileSync(artifact, "utf8");
    assert.match(artifactText, /timing_budget_policy=v3/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
    rmSync(artifact, { force: true });
  }
});
