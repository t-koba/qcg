// Rust product-line budget gate.
//
// Metric (reproducible, lexical — not a compiler AST metric):
// - Physical Rust source lines: `text.split("\n")` with only the empty
//   trailing split after a final newline removed; an empty file is 0 lines.
//   Blank lines and comment lines count consistently (they are physical
//   lines). No other normalization (no trimming, no dedup).
// - Product lines: lines in files under `crates/<crate>/src/**/*.rs` that
//   are NOT test-only and NOT covered by an inline `#[cfg(test)] mod` span
//   (see below). Only product lines are capped.
// - Test lines: lines in test-only src files plus lines covered by inline
//   `#[cfg(test)] mod` spans, plus every `.rs` line under workspace
//   `tests/**/*.rs`, `scripts/test-support/**/*.rs`, and per-crate
//   `crates/<crate>/tests/**/*.rs` / `crates/<crate>/test_support/**/*.rs`
//   (crate-scoped test helpers outside `src/`). Test lines are reported per
//   crate and in totals but never capped (uncapped by design).
//
// Test-only src file rule (lexical, path-only): a file under
// `crates/<crate>/src/` is test-only when any `/`-separated path component
// equals `tests`, or its basename is `tests.rs` or `test_support.rs`
// (e.g. `src/engine/tests.rs`, `src/tests/mod.rs`, `src/test_support.rs`).
// Test-only files contribute 0 product lines and all physical lines as test.
//
// Inline test module rule: within a non-test-only src file, every physical
// line from a test-only `#[cfg(predicate)]` attribute through the matching
// closing brace of the `mod NAME { ... }` it introduces is a test line.
// Details:
// - A `#[cfg(predicate)]` attribute starts a span exactly when its predicate
//   logically implies `test`: the predicate can only hold when `test` holds,
//   treating every other cfg atom as an independent flag. Direct `test` and
//   conjunctions such as `all(test, unix)`, `all(test, windows)`, or nested
//   forms like `all(test, any(unix, windows))` and
//   `any(all(test, unix), all(test, windows))` are test-only. Predicates that
//   can also hold in production builds — `any(test, unix)`, `not(test)`,
//   `unix`, `feature = "test-support"`, `all(unix, windows)` — stay product
//   lines. Decided by a small cfg-expression evaluator (`all`/`any`/`not`
//   over `key` / `key = value` atoms, exhaustive over up to 24 distinct
//   non-test atoms; larger or unparseable predicates conservatively remain
//   product lines. This keeps the metric lexical (never a compiler query).
// - Optional intervening attributes between the `#[cfg(...)]` and `mod` are
//   allowed (e.g. `#[cfg(test)]` + `#[path = "..."]` + `mod NAME {`).
// - Only `mod NAME {` (inline body) creates a span. `mod NAME;`
//   (out-of-line, e.g. `#[path = "tests/mod.rs"] mod tests;`) creates no
//   span; its single declaration line stays a product line while the
//   referenced file is already test-only by path.
// - Inline test-`cfg` items that are NOT modules (functions, statics,
//   `thread_local!`, `use`, etc.) remain product lines.
// - Nested test-only `mod` spans inside an outer span are unioned, never
//   double counted.
// - Brace matching uses a lexical mask that ignores line comments (`//`),
//   nested block comments (`/* ... */`), normal strings (`"..."` with
//   escapes, optional `b` prefix), raw strings (`r"..."`, `r#"..."#`,
//   optional `br` prefix), and char literals (`'a'`, `'\n'`, `'\''`,
//   `'\x41'`, `'\u{1F600}'` with underscores allowed, optional `b` prefix;
//   lifetimes such as `'a` left as code). Braces inside those regions never
//   affect the module end.
// - Source discovery and reads fail closed: unreadable source paths throw
//   and fail the gate. Only test-only directories that may legitimately not
//   exist (`tests/`, `test_support/`, `scripts/test-support/`) tolerate a
//   missing directory; anything present must be readable.
//
// Budget JSON (`scripts/rust-line-budget.json`): `{ "version": 1,
// "totalMax": <product total cap>, "crates": { "<crate>": <product cap> } }`.
// The gate enforces the exact crate set (discovered `crates/*/src` dirs must
// equal the JSON keys), each per-crate product maximum, and the overall
// product maximum. There are no test caps. Thresholds are initialized to the
// observed baseline so current sources pass without allowing growth.
//
// Node standard library only.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, dirname, posix, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = join(HERE, "..");
const DEFAULT_BUDGET = join(HERE, "rust-line-budget.json");

// Physical lines: split on "\n", drop only the empty trailing split after a
// final newline; empty file is 0. Blank/comment lines count.
export function physicalLineCount(text) {
  if (text === "") return 0;
  const parts = text.split("\n");
  if (text.endsWith("\n")) parts.pop();
  return parts.length;
}

export function lineOfIndex(text, index) {
  let line = 0;
  for (let i = 0; i < index && i < text.length; i++) {
    if (text[i] === "\n") line++;
  }
  return line;
}

// Path-only test-only rule. Accepts a repo-relative posix path (e.g.
// "crates/foo/src/tests/mod.rs") or any posix path; returns true when the
// file is test-only by location/basename.
export function isTestOnlySrcFile(repoRelPosix) {
  const normalized = String(repoRelPosix).replace(/\\/g, "/");
  const parts = normalized.split("/");
  if (parts.includes("tests")) return true;
  const base = parts[parts.length - 1];
  return base === "tests.rs" || base === "test_support.rs";
}

// Lexical mask: same length as source, noise (comments/strings/chars)
// replaced with spaces except newlines (preserved so line numbers align).
// Handles line comments, nested block comments, normal/raw strings
// (optional `b` prefix), and char literals (lifetimes left as code).
export function maskRustNoise(source) {
  const n = source.length;
  const out = new Array(n);
  let i = 0;
  const charRe = /^b?'(?:\\(?:x[0-9a-fA-F_]{2}|u\{[0-9a-fA-F_]+\}|.)|[^'\\\n])'/;
  while (i < n) {
    const c = source[i];
    const d = i + 1 < n ? source[i + 1] : "";
    if (c === "/" && d === "/") {
      out[i] = " ";
      out[i + 1] = " ";
      i += 2;
      while (i < n && source[i] !== "\n") {
        out[i] = " ";
        i++;
      }
      continue;
    }
    if (c === "/" && d === "*") {
      out[i] = " ";
      out[i + 1] = " ";
      i += 2;
      let depth = 1;
      while (i < n && depth > 0) {
        if (source[i] === "/" && source[i + 1] === "*") {
          out[i] = " ";
          out[i + 1] = " ";
          i += 2;
          depth++;
        } else if (source[i] === "*" && source[i + 1] === "/") {
          out[i] = " ";
          out[i + 1] = " ";
          i += 2;
          depth--;
        } else {
          out[i] = source[i] === "\n" ? "\n" : " ";
          i++;
        }
      }
      continue;
    }
    // Raw string: optional `b`, then `r`, then k hashes, then `"`.
    // (Plain `r#ident` raw identifiers do not match: no opening `"`.)
    let rawHashes = -1;
    let rawStartLen = 0;
    {
      let j = i;
      if (source[j] === "b" && source[j + 1] === "r") j += 1;
      if (source[j] === "r") {
        let k = j + 1;
        while (source[k] === "#") k++;
        if (source[k] === '"') {
          rawHashes = k - (j + 1);
          rawStartLen = k - i + 1; // includes opening `"`
        }
      }
    }
    if (rawHashes >= 0) {
      for (let k = 0; k < rawStartLen; k++) {
        out[i + k] = source[i + k] === "\n" ? "\n" : " ";
      }
      i += rawStartLen;
      const close = '"' + "#".repeat(rawHashes);
      const end = source.indexOf(close, i);
      const stop = end === -1 ? n : end + close.length;
      while (i < stop) {
        out[i] = source[i] === "\n" ? "\n" : " ";
        i++;
      }
      continue;
    }
    // Normal string: optional `b` prefix + `"`, with `\` escapes.
    if (c === '"' || (c === "b" && d === '"')) {
      const startLen = c === '"' ? 1 : 2;
      for (let k = 0; k < startLen; k++) {
        out[i + k] = " ";
      }
      i += startLen;
      while (i < n) {
        if (source[i] === "\\") {
          out[i] = " ";
          if (i + 1 < n) out[i + 1] = source[i + 1] === "\n" ? "\n" : " ";
          i += 2;
        } else if (source[i] === '"') {
          out[i] = " ";
          i++;
          break;
        } else {
          out[i] = source[i] === "\n" ? "\n" : " ";
          i++;
        }
      }
      continue;
    }
    // Char literal (or lifetime, left as code when the regex misses).
    if (c === "'" || (c === "b" && d === "'")) {
      const m = charRe.exec(source.slice(i, i + 24));
      if (m) {
        for (let k = 0; k < m[0].length; k++) {
          out[i + k] = source[i + k] === "\n" ? "\n" : " ";
        }
        i += m[0].length;
        continue;
      }
      out[i] = c;
      i++;
      continue;
    }
    out[i] = c;
    i++;
  }
  return out.join("");
}

// Minimal `cfg(predicate)` expression parser. Understands `all(...)` /
// `any(...)` / `not(...)` over `key` / `key = value` atoms (values are bare
// identifiers or double-quoted strings with raw token identity). Unknown
// predicates with arguments parse as opaque atoms so they never imply
// `test`. The predicate slice is the original (unmasked) source text, so
// string values keep their identity (`feature="x"` and `feature="y"` are
// distinct atoms) while trivia (whitespace, `//` line comments, nested
// `/* ... */` block comments) is skipped between tokens.
function parseCfgPredicate(text) {
  let pos = 0;
  const fail = (msg) => {
    throw new Error(`invalid cfg predicate (${msg}): ${text}`);
  };
  const skipTrivia = () => {
    for (;;) {
      while (pos < text.length && /\s/.test(text[pos])) pos++;
      if (text.startsWith("//", pos)) {
        pos += 2;
        while (pos < text.length && text[pos] !== "\n") pos++;
        continue;
      }
      if (text.startsWith("/*", pos)) {
        pos += 2;
        let depth = 1;
        while (pos < text.length && depth > 0) {
          if (text.startsWith("/*", pos)) {
            depth++;
            pos += 2;
          } else if (text.startsWith("*/", pos)) {
            depth--;
            pos += 2;
          } else {
            pos++;
          }
        }
        if (depth !== 0) fail("unterminated block comment");
        continue;
      }
      break;
    }
  };
  const parseString = () => {
    // Assumes text[pos] === '"'. Returns the raw token including quotes and
    // escapes so identical spellings share an atom key while different
    // values (or spellings) stay distinct (conservative: never conflates).
    const start = pos;
    pos++;
    while (pos < text.length && text[pos] !== '"') {
      if (text[pos] === "\\") pos++;
      pos++;
    }
    if (text[pos] !== '"') fail("unterminated string");
    pos++;
    return text.slice(start, pos);
  };
  const parseIdent = () => {
    const found = /^[A-Za-z_][A-Za-z0-9_]*/.exec(text.slice(pos));
    if (!found) fail("expected identifier");
    pos += found[0].length;
    return found[0];
  };
  const parseExpr = () => {
    skipTrivia();
    const name = parseIdent();
    skipTrivia();
    if (text[pos] === "=") {
      pos++;
      skipTrivia();
      if (text[pos] === '"') {
        const raw = parseString();
        return { kind: "atom", key: `${name}=${raw}` };
      }
      const value = /^[A-Za-z_][A-Za-z0-9_]*/.exec(text.slice(pos));
      if (value) {
        pos += value[0].length;
        return { kind: "atom", key: `${name}=${value[0]}` };
      }
      fail("expected cfg value");
    }
    if (text[pos] === "(") {
      pos++;
      const items = [];
      skipTrivia();
      if (text[pos] === ")") {
        pos++;
      } else {
        for (;;) {
          items.push(parseExpr());
          skipTrivia();
          if (text[pos] === ",") {
            pos++;
            continue;
          }
          if (text[pos] === ")") {
            pos++;
            break;
          }
          fail("expected ',' or ')'");
        }
      }
      if (name === "all") return { kind: "all", items };
      if (name === "any") return { kind: "any", items };
      if (name === "not") {
        if (items.length !== 1) fail("not() takes exactly one argument");
        return { kind: "not", item: items[0] };
      }
      return { kind: "atom", key: `${name}(...)` };
    }
    return { kind: "atom", key: name };
  };
  const root = parseExpr();
  skipTrivia();
  if (pos !== text.length) fail("trailing characters");
  return root;
}

function evalCfgPredicate(node, env) {
  switch (node.kind) {
    case "atom":
      return node.key === "test" ? !!env.test : !!env[node.key];
    case "all":
      return node.items.every((it) => evalCfgPredicate(it, env));
    case "any":
      return node.items.some((it) => evalCfgPredicate(it, env));
    case "not":
      return !evalCfgPredicate(node.item, env);
    /* c8 ignore next */
    default:
      return false;
  }
}

function collectCfgAtoms(node, into) {
  if (node.kind === "atom") into.add(node.key);
  else if (node.kind === "not") collectCfgAtoms(node.item, into);
  else for (const it of node.items) collectCfgAtoms(it, into);
  return into;
}

// True when a `cfg(...)` predicate argument can only hold while `test` holds,
// treating every other cfg atom as an independent flag. Decided by
// exhausting assignments with `test` unset: if none satisfies the
// predicate, it implies `test`. Unparseable predicates or predicates with
// more than 24 distinct non-test atoms conservatively stay product.
export function cfgImpliesTest(predicateText) {
  let root;
  try {
    root = parseCfgPredicate(predicateText);
  } catch {
    return false;
  }
  const others = [...collectCfgAtoms(root, new Set())].filter((k) => k !== "test");
  if (others.length > 24) return false;
  const total = 1 << others.length;
  for (let mask = 0; mask < total; mask++) {
    const env = { test: false };
    for (let b = 0; b < others.length; b++) env[others[b]] = !!(mask & (1 << b));
    if (evalCfgPredicate(root, env)) return false;
  }
  return true;
}

// Find inclusive [startLine, endLine] spans (0-based) for inline
// test-only `mod NAME { ... }` bodies. See header for exact rules.
export function findTestModuleSpans(source) {
  const masked = maskRustNoise(source);
  const spans = [];
  const cfgRe = /#\s*\[\s*cfg\s*\(/g;
  let m;
  while ((m = cfgRe.exec(masked)) !== null) {
    const cfgStart = m.index;
    // Extract the balanced `cfg(...)` argument on the masked text (parens
    // inside strings/comments are blanked, so only real parens count; the
    // mask preserves length, hence offsets align with `source`). Evaluate
    // the original predicate slice so string values keep their identity.
    let parenDepth = 1;
    let pos = cfgStart + m[0].length;
    while (pos < masked.length && parenDepth > 0) {
      if (masked[pos] === "(") parenDepth++;
      else if (masked[pos] === ")") parenDepth--;
      pos++;
    }
    if (parenDepth !== 0) continue; // unbalanced: ignore rather than misattribute.
    const predicate = source.slice(cfgStart + m[0].length, pos - 1);
    let after = pos;
    while (after < masked.length && /\s/.test(masked[after])) after++;
    if (masked[after] !== "]") continue;
    if (!cfgImpliesTest(predicate)) continue; // production cfg: stays product.
    pos = after + 1;
    const skipWs = () => {
      while (pos < masked.length && /\s/.test(masked[pos])) pos++;
    };
    skipWs();
    // Skip optional intervening attributes `#[...]`.
    while (masked[pos] === "#" && masked[pos + 1] === "[") {
      const close = masked.indexOf("]", pos + 2);
      if (close === -1) break;
      pos = close + 1;
      skipWs();
    }
    const rest = masked.slice(pos);
    const modMatch =
      /^(?:pub\s*(?:\(\s*(?:crate|self|super|in\s+[^)]+)\s*\)\s*)?)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*([;{])/.exec(
        rest,
      );
    if (!modMatch) continue; // non-module #[cfg(test)] item: stays product.
    if (modMatch[2] === ";") continue; // out-of-line `mod NAME;`: no body.
    const braceOpen = pos + modMatch[0].lastIndexOf("{");
    let depth = 0;
    let end = -1;
    for (let k = braceOpen; k < masked.length; k++) {
      if (masked[k] === "{") depth++;
      else if (masked[k] === "}") {
        depth--;
        if (depth === 0) {
          end = k;
          break;
        }
      }
    }
    if (end === -1) continue; // unbalanced: ignore rather than misattribute.
    const startLine = lineOfIndex(source, cfgStart);
    const endLine = lineOfIndex(source, end);
    spans.push([startLine, endLine]);
  }
  // Union overlapping/nested spans so nested test modules never double count.
  spans.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const merged = [];
  for (const s of spans) {
    const last = merged[merged.length - 1];
    if (last && s[0] <= last[1] + 0) {
      if (s[1] > last[1]) last[1] = s[1];
    } else {
      merged.push([s[0], s[1]]);
    }
  }
  // Adjacent spans (end+1 === start) are distinct modules; the `+ 0` above
  // keeps them separate while overlapping/nested spans merge.
  return merged;
}

// Split one src file's physical lines into product/test. Test-only files are
// all test; otherwise test lines are the union of inline module spans.
export function splitSrcFile(source, repoRelPosix) {
  const physical = physicalLineCount(source);
  if (isTestOnlySrcFile(repoRelPosix)) {
    return { physical, product: 0, test: physical, spans: [] };
  }
  const spans = findTestModuleSpans(source);
  let test = 0;
  for (const [a, b] of spans) test += b - a + 1;
  // Clamp defensively; spans are within the file so this only guards bugs.
  if (test > physical) test = physical;
  return { physical, product: physical - test, test, spans };
}

// List `.rs` files under `dir`, failing closed on unreadable paths: every
// I/O error throws and fails the gate, except that a missing (`ENOENT`)
// directory is tolerated when `optional` is set. Only test-only directories
// that may legitimately not exist pass `optional: true`; required source
// directories and every file or entry encountered must be readable.
export function listRsFilesRecursive(dir, { optional = false } = {}) {
  let entries;
  try {
    entries = readdirSync(dir);
  } catch (err) {
    if (optional && err?.code === "ENOENT") return [];
    throw err;
  }
  const found = [];
  for (const e of entries.sort()) {
    const p = join(dir, e);
    const st = statSync(p); // throws on unreadable entries: fail closed.
    if (st.isDirectory()) found.push(...listRsFilesRecursive(p));
    else if (st.isFile() && e.endsWith(".rs")) found.push(p);
  }
  return found;
}

// Sum physical lines, failing closed: unreadable files throw.
export function sumPhysicalLines(files) {
  let total = 0;
  for (const f of files) {
    total += physicalLineCount(readFileSync(f, "utf8"));
  }
  return total;
}

export function discoverCrates(rootDir = DEFAULT_ROOT) {
  const cratesDir = join(rootDir, "crates");
  const names = [];
  for (const e of readdirSync(cratesDir).sort()) {
    const entry = join(cratesDir, e);
    if (!statSync(entry).isDirectory()) continue;
    let srcStat;
    try {
      srcStat = statSync(join(entry, "src"));
    } catch (err) {
      if (err?.code === "ENOENT") continue; // not a crate directory.
      throw err;
    }
    if (srcStat.isDirectory()) names.push(e);
  }
  return names;
}

// Measure per-crate product/test lines plus totals. `rootDir` is the repo
// root. Returns `{ root, crates: {name: {product, test}}, totals }`.
export function collectStats(rootDir = DEFAULT_ROOT) {
  const crates = {};
  let productTotal = 0;
  let testTotal = 0;
  for (const name of discoverCrates(rootDir)) {
    const srcDir = join(rootDir, "crates", name, "src");
    let product = 0;
    let test = 0;
    for (const f of listRsFilesRecursive(srcDir)) {
      const rel = posix.join("crates", name, "src", f.slice(srcDir.length + 1).split(sep).join("/"));
      const split = splitSrcFile(readFileSync(f, "utf8"), rel);
      product += split.product;
      test += split.test;
    }
    // Crate-scoped test dirs outside `src/`: fully test lines for the crate.
    // A missing dir is fine (not every crate has one); anything present must
    // be readable.
    const crateTests = listRsFilesRecursive(join(rootDir, "crates", name, "tests"), { optional: true });
    const crateSupport = listRsFilesRecursive(join(rootDir, "crates", name, "test_support"), {
      optional: true,
    });
    test += sumPhysicalLines(crateTests) + sumPhysicalLines(crateSupport);
    crates[name] = { product, test };
    productTotal += product;
    testTotal += test;
  }
  // Workspace-wide test locations contribute to totals only (uncapped).
  const workspaceTests = listRsFilesRecursive(join(rootDir, "tests"), { optional: true });
  const scriptSupport = listRsFilesRecursive(join(rootDir, "scripts", "test-support"), {
    optional: true,
  });
  testTotal += sumPhysicalLines(workspaceTests) + sumPhysicalLines(scriptSupport);
  return { root: rootDir, crates, totals: { product: productTotal, test: testTotal } };
}

export function loadBudget(budgetPath = DEFAULT_BUDGET) {
  return JSON.parse(readFileSync(budgetPath, "utf8"));
}

// Pure budget check for unit tests: returns violation strings (empty = pass).
// Enforces exact crate set, per-crate product maxima, and total maximum.
// Test lines are never capped.
export function checkBudgetPure(budget, stats) {
  const violations = [];
  if (!budget || typeof budget !== "object" || !budget.crates || typeof budget.crates !== "object") {
    return ["budget must define an object \"crates\" map"];
  }
  if (typeof budget.totalMax !== "number" || !Number.isInteger(budget.totalMax) || budget.totalMax < 0) {
    violations.push("budget \"totalMax\" must be a non-negative integer");
  }
  const expected = Object.keys(budget.crates).sort();
  const actual = Object.keys(stats.crates).sort();
  const missing = actual.filter((c) => !expected.includes(c));
  const extra = expected.filter((c) => !actual.includes(c));
  if (missing.length || extra.length) {
    if (missing.length) violations.push(`budget missing crates: ${missing.join(", ")}`);
    if (extra.length) violations.push(`budget has unknown crates: ${extra.join(", ")}`);
  }
  for (const [name, cap] of Object.entries(budget.crates)) {
    if (typeof cap !== "number" || !Number.isInteger(cap) || cap < 0) {
      violations.push(`budget cap for crate "${name}" must be a non-negative integer`);
      continue;
    }
    const got = stats.crates[name]?.product;
    if (got === undefined) continue; // already reported as unknown.
    if (got > cap) {
      violations.push(`crate "${name}" product lines ${got} exceed maximum ${cap}`);
    }
  }
  if (typeof budget.totalMax === "number" && stats.totals.product > budget.totalMax) {
    violations.push(
      `total product lines ${stats.totals.product} exceed maximum ${budget.totalMax}`,
    );
  }
  for (const [name, cap] of Object.entries(budget.crates)) {
    if (typeof cap !== "number") continue;
    const got = stats.crates[name]?.product;
    if (got !== undefined && got < 0) violations.push(`negative count for "${name}"`);
  }
  return violations;
}

export function formatStats(stats) {
  const names = Object.keys(stats.crates).sort();
  const lines = ["rust product-line budget (physical lines; only product is capped):"];
  for (const name of names) {
    const c = stats.crates[name];
    lines.push(`  ${name}: product=${c.product} test=${c.test}`);
  }
  lines.push(`  total: product=${stats.totals.product} test=${stats.totals.test}`);
  return lines.join("\n");
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2);
  let root = DEFAULT_ROOT;
  let budgetPath = DEFAULT_BUDGET;
  let json = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--root") root = args[++i];
    else if (args[i] === "--budget") budgetPath = args[++i];
    else if (args[i] === "--json") json = true;
    else if (args[i] === "--help" || args[i] === "-h") {
      console.log("usage: node scripts/rust-line-budget.mjs [--root DIR] [--budget PATH] [--json]");
      process.exit(0);
    } else {
      console.error(`unknown argument: ${args[i]}`);
      process.exit(2);
    }
  }
  // Fail closed: unreadable sources throw and fail the gate.
  let stats;
  try {
    stats = collectStats(root);
  } catch (err) {
    console.error(`rust-line-budget: cannot collect stats under ${root}: ${err.message}`);
    process.exit(1);
  }
  if (json) {
    console.log(JSON.stringify({ budget: budgetPath, ...stats }, null, 2));
  } else {
    console.log(formatStats(stats));
  }
  // Always enforce the budget gate (both human and --json modes).
  let budget;
  try {
    budget = loadBudget(budgetPath);
  } catch (err) {
    console.error(`rust-line-budget: cannot load budget ${budgetPath}: ${err.message}`);
    process.exit(2);
  }
  const violations = checkBudgetPure(budget, stats);
  if (violations.length) {
    for (const v of violations) console.error(`rust-line-budget: ${v}`);
    process.exit(1);
  }
  if (!json) console.log(`rust-line-budget: pass (total product ${stats.totals.product} <= ${budget.totalMax})`);
}
