// Test paths/files and inline #[cfg(test)] modules are uncapped; other src lines are product.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
const here = dirname(fileURLToPath(import.meta.url)), root = join(here, ".."), budgetFile = join(here, "rust-line-budget.json");
const lines = (s) => s === "" ? 0 : s.split("\n").length - Number(s.endsWith("\n"));
export const isTestPath = (p) => {
  const a = String(p).replace(/\\/g, "/").split("/");
  return a.includes("tests") || a.includes("test_support") || ["tests.rs", "test_support.rs"].includes(a[a.length - 1]);
};
function braceDelta(s, q) {
  let n = 0;
  for (let i = 0; i < s.length;) {
    if (q.block) { if (s.startsWith("/*", i)) { q.block++; i += 2; } else if (s.startsWith("*/", i)) { q.block--; i += 2; } else i++; continue; }
    if (q.raw !== null) { const e = s.indexOf('"' + "#".repeat(q.raw), i); if (e < 0) break; i = e + q.raw + 1; q.raw = null; continue; }
    if (q.quote) { if (s[i] === "\\") i += 2; else if (s[i] === q.quote) { q.quote = ""; i++; } else i++; continue; }
    if (s.startsWith("//", i)) break;
    if (s.startsWith("/*", i)) { q.block = 1; i += 2; continue; }
    const raw = /^(?:br|r)(#+)?"/.exec(s.slice(i)), ch = /^'(?:\\.|[^'\\])'/.exec(s.slice(i));
    if (raw) { q.raw = (raw[1] || "").length; i += raw[0].length; }
    else if (ch) i += ch[0].length;
    else if (s[i] === '"') { q.quote = '"'; i++; }
    else { n += s[i] === "{" ? 1 : s[i] === "}" ? -1 : 0; i++; }
  }
  return n;
}
export function splitSrcFile(s, path) {
  const physical = lines(s), a = s.split("\n");
  if (isTestPath(path)) return { product: 0, test: physical };
  let test = 0;
  for (let i = 0; i < a.length; i++) {
    if (!/^\s*#\[cfg\(test\)\]\s*$/.test(a[i])) continue;
    let m = i + 1;
    while (m < a.length && (!a[m].trim() || /^\s*(?:#\s*\[|\/\/|\/\*|\*)/.test(a[m]))) m++;
    if (!/^(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\b/.test(a[m] || "")) continue;
    let start = m;
    if (!a[m].includes("{")) { if (a[m].trim().endsWith(";") || !a[m + 1]?.trim().startsWith("{")) continue; start++; }
    const q = { block: 0, raw: null, quote: "" }; let depth = 0;
    for (let end = start; end < a.length; end++) if ((depth += braceDelta(a[end], q)) === 0) { test += end - i + 1; i = end; break; }
  }
  return { product: physical - test, test };
}
function files(dir, optional = false) {
  let names; try { names = readdirSync(dir); } catch (e) { if (optional && e.code === "ENOENT") return []; throw e; }
  return names.sort().flatMap((name) => { const p = join(dir, name), s = statSync(p); return s.isDirectory() ? files(p) : s.isFile() && name.endsWith(".rs") ? [p] : []; });
}
const sum = (dir, optional = false) => files(dir, optional).reduce((n, f) => n + lines(readFileSync(f, "utf8")), 0);
export function collectStats(base = root) {
  const crates = {}; let product = 0, test = 0;
  for (const name of readdirSync(join(base, "crates")).sort()) {
    const crate = join(base, "crates", name); if (!statSync(crate).isDirectory()) continue;
    let src; try { src = statSync(join(crate, "src")); } catch (e) { if (e.code === "ENOENT") continue; throw e; }
    if (!src.isDirectory()) continue;
    let p = 0, t = 0;
    for (const f of files(join(crate, "src"))) { const c = splitSrcFile(readFileSync(f, "utf8"), relative(base, f).split(sep).join("/")); p += c.product; t += c.test; }
    t += sum(join(crate, "tests"), true) + sum(join(crate, "test_support"), true);
    crates[name] = { product: p, test: t }; product += p; test += t;
  }
  test += sum(join(base, "tests"), true) + sum(join(base, "scripts", "test-support"), true);
  return { crates, totals: { product, test } };
}
export function budgetErrors(b, s) {
  if (!b?.crates || typeof b.crates !== "object" || Array.isArray(b.crates)) return ["budget must define crates"];
  const caps = b.crates, names = Object.keys(s.crates).sort(), out = [];
  if (names.join("\n") !== Object.keys(caps).sort().join("\n")) out.push("budget crate set differs");
  for (const name of names) if (!Number.isInteger(caps[name]) || caps[name] < 0) out.push(`invalid cap for ${name}`); else if (s.crates[name].product > caps[name]) out.push(`${name} product lines ${s.crates[name].product} exceed ${caps[name]}`);
  if (!Number.isInteger(b.totalMax) || b.totalMax < s.totals.product) out.push(`total product lines ${s.totals.product} exceed ${b.totalMax}`);
  return out;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const s = collectStats(), b = JSON.parse(readFileSync(budgetFile, "utf8")), e = budgetErrors(b, s);
  console.log("rust product-line budget (physical lines; tests uncapped):");
  for (const [name, c] of Object.entries(s.crates)) console.log(`  ${name}: product=${c.product} test=${c.test}`);
  console.log(`  total: product=${s.totals.product} test=${s.totals.test}`);
  for (const error of e) console.error(`rust-line-budget: ${error}`);
  console.log(e.length ? "rust-line-budget: FAIL" : `rust-line-budget: pass (total product ${s.totals.product} <= ${b.totalMax})`);
  if (e.length) process.exitCode = 1;
}
