import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  physicalLineCount,
  isTestOnlySrcFile,
  cfgImpliesTest,
  findTestModuleSpans,
  maskRustNoise,
  splitSrcFile,
  listRsFilesRecursive,
  sumPhysicalLines,
  checkBudgetPure,
} from "./rust-line-budget.mjs";

test("physical lines drop only the empty trailing split; blanks count", () => {
  assert.equal(physicalLineCount(""), 0);
  assert.equal(physicalLineCount("a\n"), 1);
  assert.equal(physicalLineCount("a\nb\n"), 2);
  assert.equal(physicalLineCount("a\nb"), 2);
  assert.equal(physicalLineCount("a\n\n"), 2);
  assert.equal(physicalLineCount("\n"), 1);
  assert.equal(physicalLineCount("// comment\n\nfn f() {}\n"), 3);
});

test("production vs test-only file classification is path-only", () => {
  assert.equal(isTestOnlySrcFile("crates/foo/src/lib.rs"), false);
  assert.equal(isTestOnlySrcFile("crates/foo/src/engine.rs"), false);
  assert.equal(isTestOnlySrcFile("crates/foo/src/bin/tool.rs"), false);
  assert.equal(isTestOnlySrcFile("crates/service/src/tests/mod.rs"), true);
  assert.equal(isTestOnlySrcFile("crates/service/src/tests/support.rs"), true);
  assert.equal(isTestOnlySrcFile("crates/engine/src/engine/tests.rs"), true);
  assert.equal(isTestOnlySrcFile("crates/llm/src/tests.rs"), true);
  assert.equal(isTestOnlySrcFile("crates/engine/src/test_support.rs"), true);
  // Basename rule applies even without a tests/ component.
  assert.equal(isTestOnlySrcFile("crates/foo/src/tests.rs"), true);
  // A file merely containing \"tests\" in its stem is still product.
  assert.equal(isTestOnlySrcFile("crates/foo/src/latest.rs"), false);
});

test("test-only files are all test lines", () => {
  const src = "use super::*;\n#[test]\nfn t() {}\n";
  const split = splitSrcFile(src, "crates/foo/src/tests/mod.rs");
  assert.equal(split.physical, 3);
  assert.equal(split.product, 0);
  assert.equal(split.test, 3);
});

test("cfg(test) module exclusion ignores braces in comments/strings/chars", () => {
  const src = [
    "fn product_before() {}",
    "#[cfg(test)]",
    "mod tests {",
    '    fn brace_in_string() { let s = "{"; let t = "}"; }',
    "    fn raw() { let r = r#\"{\"#; let q = r##\"}\"##; }",
    "    fn ch() { let a = '{'; let b = '}'; let life = foo(&'a x); }",
    "    // } stray brace in line comment",
    "    /* } brace in block comment */",
    "    /* outer /* nested } */ still comment { */",
    '    fn escaped() { let s = "a\\"{"; }',
    "}",
    "fn product_after() {}",
    "",
  ].join("\n");
  const spans = findTestModuleSpans(src);
  assert.deepEqual(spans, [[1, 10]]);
  const split = splitSrcFile(src, "crates/foo/src/lib.rs");
  assert.equal(split.physical, 12);
  assert.equal(split.test, 10);
  assert.equal(split.product, 2);
});

test("non-module cfg(test) items stay product; non-test cfgs stay product", () => {
  const src = [
    "#[cfg(test)]",
    "pub(crate) fn helper() {}",
    "#[cfg(any(test, unix))]",
    "mod maybe_prod {",
    "    fn f() {}",
    "}",
    "#[cfg(not(test))]",
    "mod prod_only {",
    "}",
    "#[cfg(unix)]",
    "mod platform {",
    "}",
    "#[cfg(feature = \"test-support\")]",
    "mod feat {",
    "}",
    "fn product() {}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(src), []);
  const split = splitSrcFile(src, "crates/foo/src/lib.rs");
  assert.equal(split.product, split.physical);
  assert.equal(split.test, 0);
});

test("cfg predicates implying test start spans; others do not", () => {
  const withMod = (cfg) =>
    ["fn a() {}", cfg, "mod t {", "    fn f() {}", "}", "fn b() {}", ""].join("\n");
  const implying = [
    "#[cfg(test)]",
    "#[cfg( test )]",
    "#[cfg(all(test, unix))]",
    "#[cfg(all(test, windows))]",
    "#[cfg(all(unix, test))]",
    "#[cfg(all(test, any(unix, windows)))]",
    "#[cfg(any(all(test, unix), all(test, windows)))]",
    "#[cfg(any(test, all(test, unix)))]",
    "#[cfg(not(not(test)))]",
    "#[cfg(all(test, not(feature = \"x\")))]",
  ];
  for (const cfg of implying) {
    assert.equal(cfgImpliesTest(cfg.slice("#[cfg(".length, -2).trim()), true, cfg);
    assert.deepEqual(findTestModuleSpans(withMod(cfg)), [[1, 4]], cfg);
  }
  const producing = [
    "#[cfg(any(test, unix))]",
    "#[cfg(any(test, not(test)))]",
    "#[cfg(not(test))]",
    "#[cfg(all(not(test), unix))]",
    "#[cfg(unix)]",
    "#[cfg(windows)]",
    "#[cfg(feature = \"test-support\")]",
    "#[cfg(all(unix, windows))]",
    "#[cfg(testable)]",
    "#[cfg()]",
    "#[cfg(all(test,)]",
  ];
  for (const cfg of producing) {
    assert.equal(cfgImpliesTest(cfg.slice("#[cfg(".length, -2).trim()), false, cfg);
    assert.deepEqual(findTestModuleSpans(withMod(cfg)), [], cfg);
  }
});

test("real-world spellings: all(test, unix) modules are test; any(test, ..) fns stay product", () => {
  const src = [
    "#[cfg(all(test, unix))]",
    "mod daemon_policy_tests {",
    "    fn f() {}",
    "}",
    "#[cfg(all(test, unix))]",
    "#[allow(dead_code)]",
    'mod daemon_policy_test_support {',
    "}",
    "#[cfg(any(test, not(target_os = \"linux\")))]",
    "pub(crate) fn check_no_replace_target() {}",
    "fn product() {}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(src), [
    [0, 3],
    [4, 7],
  ]);
  const split = splitSrcFile(src, "crates/foo/src/session.rs");
  assert.equal(split.test, 8);
  assert.equal(split.product, split.physical - 8);
});

test("char literals mask \\xNN and \\u{...} escapes including underscores", () => {
  const src = "let a = '\\x41';\nlet b = '\\u{1_F600}';\nlet c = '\\u{7d}';\nlet d = b'\\x00';\n";
  const masked = maskRustNoise(src);
  assert.equal(masked.length, src.length);
  assert.ok(!masked.includes("{") && !masked.includes("}"), `escapes must be blanked: ${JSON.stringify(masked)}`);
  // Newlines survive masking so line numbers align.
  assert.equal(masked.split("\n").length, src.split("\n").length);
  // Lifetimes stay code; ordinary chars stay blanked.
  const life = maskRustNoise("foo(&'a x)\n");
  assert.ok(life.includes("'"), "lifetime tick stays code");
});

test("braces inside \\u{...} escapes never close a test module early", () => {
  const src = [
    "fn product_before() {}",
    "#[cfg(test)]",
    "mod tests {",
    "    const OPEN: char = '\\x7b';",
    "    const CLOSE: char = '\\u{7d}';",
    "    const EMOJI: char = '\\u{1_F600}';",
    "    fn f() { let q = '\\''; let life = foo(&'a x); }",
    "}",
    "fn product_after() {}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(src), [[1, 7]]);
  const split = splitSrcFile(src, "crates/foo/src/lib.rs");
  assert.equal(split.physical, 9);
  assert.equal(split.test, 7);
  assert.equal(split.product, 2);
});

test("source discovery fails closed; missing optional test dirs are fine", () => {
  const root = mkdtempSync(join(tmpdir(), "line-budget-"));
  try {
    const missing = join(root, "does-not-exist");
    assert.deepEqual(listRsFilesRecursive(missing, { optional: true }), []);
    assert.throws(() => listRsFilesRecursive(missing));
    assert.throws(() => sumPhysicalLines([join(missing, "x.rs")]));
    // A directory is not a readable source file.
    mkdirSync(join(root, "sub"), { recursive: true });
    assert.throws(() => sumPhysicalLines([join(root, "sub")]));
    // Present files are counted.
    writeFileSync(join(root, "a.rs"), "fn a() {}\n");
    assert.equal(sumPhysicalLines([join(root, "a.rs")]), 1);
    assert.deepEqual(listRsFilesRecursive(join(root, "sub"), { optional: true }), []);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("intervening attributes allowed; out-of-line mod has no span", () => {
  const withAttr = [
    "fn a() {}",
    "#[cfg(test)]",
    '#[allow(dead_code)]',
    "mod tests {",
    "    fn f() {}",
    "}",
    "fn b() {}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(withAttr), [[1, 5]]);
  const outOfLine = [
    "fn a() {}",
    "#[cfg(test)]",
    '#[path = "tests/mod.rs"]',
    "mod tests;",
    "fn b() {}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(outOfLine), []);
  const split = splitSrcFile(outOfLine, "crates/foo/src/lib.rs");
  assert.equal(split.test, 0);
  assert.equal(split.product, split.physical);
});

test("nested test modules union without double counting", () => {
  const src = [
    "#[cfg(test)]",
    "mod outer {",
    "    fn f() {}",
    "    #[cfg(test)]",
    "    mod inner {",
    "        fn g() {}",
    "    }",
    "}",
    "fn product() {}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(src), [[0, 7]]);
  const split = splitSrcFile(src, "crates/foo/src/lib.rs");
  assert.equal(split.test, 8);
  assert.equal(split.product, 1);
});

test("budget enforces exact crate set and product caps; tests uncapped", () => {
  const stats = {
    crates: { a: { product: 10, test: 1000 }, b: { product: 5, test: 0 } },
    totals: { product: 15, test: 1000 },
  };
  const ok = { version: 1, totalMax: 15, crates: { a: 10, b: 5 } };
  assert.deepEqual(checkBudgetPure(ok, stats), []);
  // Huge test lines never fail the gate.
  const hugeTests = {
    crates: { a: { product: 10, test: 999999 }, b: { product: 5, test: 0 } },
    totals: { product: 15, test: 999999 },
  };
  assert.deepEqual(checkBudgetPure(ok, hugeTests), []);
  // Per-crate overrun.
  const over = {
    crates: { a: { product: 11, test: 0 }, b: { product: 5, test: 0 } },
    totals: { product: 16, test: 0 },
  };
  const v1 = checkBudgetPure(ok, over);
  assert.ok(v1.some((v) => v.includes('"a"')), `expected crate a violation: ${v1}`);
  // Total overrun with per-crate caps met.
  const tight = { version: 1, totalMax: 14, crates: { a: 10, b: 5 } };
  const v2 = checkBudgetPure(tight, stats);
  assert.ok(v2.some((v) => v.includes("total")), `expected total violation: ${v2}`);
  // Exact crate set: missing and unknown.
  const missing = { version: 1, totalMax: 15, crates: { a: 10 } };
  assert.ok(
    checkBudgetPure(missing, stats).some((v) => v.includes("missing")),
    "expected missing-crate violation",
  );
  const extra = { version: 1, totalMax: 15, crates: { a: 10, b: 5, c: 1 } };
  assert.ok(
    checkBudgetPure(extra, stats).some((v) => v.includes("unknown")),
    "expected unknown-crate violation",
  );
});

test("cfg string values keep identity; trivia comments are skipped", () => {
  const withMod = (cfg) =>
    ["fn a() {}", cfg, "mod t {", "    fn f() {}", "}", "fn b() {}", ""].join("\n");
  // Distinct string values are distinct atoms: satisfiable without `test`.
  assert.equal(cfgImpliesTest('all(feature="x", not(feature="y"))'), false);
  assert.deepEqual(
    findTestModuleSpans(withMod('#[cfg(all(feature="x", not(feature="y")))]')),
    [],
  );
  // A conjunction requiring `test` still implies it, even with a string atom.
  assert.equal(cfgImpliesTest('all(test, feature="x")'), true);
  assert.deepEqual(
    findTestModuleSpans(withMod('#[cfg(all(test, feature="x"))]')),
    [[1, 4]],
  );
  // Identical spellings share one atom (unsatisfiable without `test`);
  // different spellings stay distinct (conservative: never conflated).
  assert.equal(cfgImpliesTest('all(feature="x", not(feature="x"))'), true);
  assert.equal(cfgImpliesTest('all(feature="a\\"b", not(feature="a\\"b"))'), true);
  assert.equal(cfgImpliesTest('all(feature="a\\"b", not(feature="a\\"c"))'), false);
  assert.deepEqual(
    findTestModuleSpans(withMod('#[cfg(all(feature="a\\"b", not(feature="a\\"c")))]')),
    [],
  );
  // Whitespace and line/nested-block comments are trivia between tokens.
  assert.equal(cfgImpliesTest("all(test /* outer /* nested */ still */, unix)"), true);
  assert.equal(cfgImpliesTest("all(test // trailing\n, unix)"), true);
  assert.equal(cfgImpliesTest("any(test /* c */, unix)"), false);
  assert.deepEqual(
    findTestModuleSpans(
      withMod("#[cfg(all(test /* outer /* nested */ still */, unix))]"),
    ),
    [[1, 4]],
  );
});

test("string parens/comments do not end cfg early; long trivia still finds mod", () => {
  const withMod = (cfg) =>
    ["fn a() {}", cfg, "mod t {", "    fn f() {}", "}", "fn b() {}", ""].join("\n");
  // A paren or comment marker inside a cfg string must not close the cfg.
  assert.deepEqual(
    findTestModuleSpans(withMod('#[cfg(all(test, feature=")"))]')),
    [[1, 4]],
  );
  assert.deepEqual(
    findTestModuleSpans(withMod('#[cfg(all(test, feature="/*"))]')),
    [[1, 4]],
  );
  assert.equal(cfgImpliesTest('all(test, feature=")")'), true);
  assert.equal(cfgImpliesTest('all(test, feature="/*")'), true);
  // Intervening trivia longer than any fixed window still classifies.
  const longComment = "/* " + "x".repeat(600) + " */";
  const srcLongComment = [
    "#[cfg(test)]",
    longComment,
    "mod t {",
    "    fn f() {}",
    "}",
    "fn b() {}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(srcLongComment), [[0, 4]]);
  const longAttr = "#[allow(" + "x,".repeat(200) + ")]";
  const srcLongAttr = [
    "#[cfg(test)]",
    longAttr,
    "mod t {",
    "    fn f() {}",
    "}",
    "",
  ].join("\n");
  assert.deepEqual(findTestModuleSpans(srcLongAttr), [[0, 4]]);
});
