import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { budgetErrors, collectStats, isTestPath, splitSrcFile } from "./rust-line-budget.mjs";

function write(root, path, text) {
  const file = join(root, path);
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, text);
}

test("test paths and inline test-only cfg modules split physical lines", () => {
  assert.equal(isTestPath("crates/demo/src/tests/mod.rs"), true);
  assert.equal(isTestPath("crates/demo/src/test_support.rs"), true);
  assert.equal(isTestPath("crates/demo/src/lib.rs"), false);
  const source = [
    "fn before() {}", "#[cfg(test)]", "mod tests {",
    '    let raw = r#"{ }"#;', '    let text = "}";',
    "    /* { /* } */ */", "    let brace = '}';", "}", "fn after() {}", "",
  ].join("\n");
  assert.deepEqual(splitSrcFile(source, "crates/demo/src/lib.rs"), { product: 2, test: 7 });
  assert.deepEqual(splitSrcFile("#[cfg(all(test, unix))]\nmod tests {\n}\n", "crates/demo/src/lib.rs"), { product: 0, test: 3 });
  assert.deepEqual(splitSrcFile("#[cfg(all(unix, test))]\nmod tests {\n}\n", "crates/demo/src/lib.rs"), { product: 0, test: 3 });
  assert.deepEqual(splitSrcFile("#[cfg(all(not(test), unix))]\nmod production {\n}\n", "crates/demo/src/lib.rs"), { product: 3, test: 0 });
  assert.deepEqual(splitSrcFile("#[cfg(test)]\nfn helper() {}\n", "crates/demo/src/lib.rs"), { product: 2, test: 0 });
});

test("recursive budget counts test locations uncapped and caps only product", () => {
  const root = mkdtempSync(join(tmpdir(), "rust-line-budget-"));
  try {
    write(root, "crates/demo/src/lib.rs", "fn product() {}\n#[cfg(test)]\nmod tests {\nfn case() {}\n}\n");
    write(root, "crates/demo/src/tests.rs", "test\ntest\n");
    write(root, "crates/demo/src/test_support.rs", "helper\n");
    write(root, "crates/demo/tests/integration.rs", "test\n");
    write(root, "crates/demo/test_support/helper.rs", "test\n");
    write(root, "tests/workspace.rs", "test\n");
    write(root, "scripts/test-support/fixture.rs", "test\n");
    const stats = collectStats(root), budget = { totalMax: 1, crates: { demo: 1 } };
    assert.deepEqual(stats.crates.demo, { product: 1, test: 9 });
    assert.equal(stats.totals.test, 11);
    assert.deepEqual(budgetErrors(budget, stats), []);
    write(root, "crates/demo/tests/extra.rs", "test\n".repeat(100));
    assert.deepEqual(budgetErrors(budget, collectStats(root)), []);
    write(root, "crates/demo/src/lib.rs", "fn product() {}\nfn growth() {}\n");
    const grown = collectStats(root);
    assert.notDeepEqual(budgetErrors(budget, grown), []);
    assert.notDeepEqual(budgetErrors({ totalMax: 2, crates: { demo: 1 } }, grown), []);
    assert.notDeepEqual(budgetErrors({ totalMax: 1, crates: { demo: 2 } }, grown), []);
    assert.notDeepEqual(budgetErrors({ totalMax: 1, crates: {} }, stats), []);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
