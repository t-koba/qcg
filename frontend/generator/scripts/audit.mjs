import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

// Exact-ID exceptions per project policy, not broad suppressions. Each entry
// is a time-boxed, reviewable record: once its review date passes the gate
// fails closed and forces re-triage, so a suppression cannot quietly become
// permanent. Every other moderate-or-higher advisory always fails the gate.
const exceptions = [{
  id: "GHSA-fc5q-39xc-vjpf",
  reason: "picomatch <=4.0.7 is dev-only in this private demo generator with zero runtime deps; as of 2026-11-18 there is no patched release and npm's only proposed fix is a rejected Vitest 4 -> 3 downgrade",
  owner: "qcg maintainers",
  reviewBy: "2027-05-18",
}];
const severities = ["info", "low", "moderate", "high", "critical"];

function isPlainObject(value) {
  if (value === null || typeof value !== "object") return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function advisoryId(url, advisory) {
  // Only a canonical GitHub Advisory URL can match an exception. A CVE id in
  // free text is not an identity: a different advisory that merely cites the
  // excepted CVE must stay blocking. Noncanonical URLs are classified by their
  // raw URL, which cannot equal an exact GHSA id.
  const ghsa = url.origin === "https://github.com" &&
    /^\/advisories\/GHSA-([a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4})\/?$/i.exec(url.pathname);
  return ghsa ? `GHSA-${ghsa[1].toLowerCase()}` : advisory.url;
}

export function evaluateAudit(report, exceptionEntries = exceptions, auditStatus = 0, now = new Date()) {
  const invalid = (error) => ({ ok: false, blocking: [], ignored: [], error });
  if (!isPlainObject(report)) return invalid("expected a JSON object");
  if (!Array.isArray(exceptionEntries)) return invalid("expected an exception list");
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) return invalid("invalid current date");
  const activeExceptions = new Set();
  for (const entry of exceptionEntries) {
    if (!isPlainObject(entry) || typeof entry.id !== "string" ||
        !/^GHSA-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}$/i.test(entry.id) ||
        typeof entry.reason !== "string" || !entry.reason.trim() ||
        typeof entry.owner !== "string" || !entry.owner.trim() ||
        typeof entry.reviewBy !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(entry.reviewBy)) {
      return invalid("invalid audit exception entry");
    }
    const reviewBy = new Date(`${entry.reviewBy}T00:00:00Z`);
    if (Number.isNaN(reviewBy.getTime()) || reviewBy.toISOString().slice(0, 10) !== entry.reviewBy) {
      return invalid(`invalid review date for audit exception ${entry.id}`);
    }
    if (now >= reviewBy) {
      return invalid(`audit exception ${entry.id} expired on ${entry.reviewBy}; re-triage required`);
    }
    activeExceptions.add(`GHSA-${entry.id.slice(5).toLowerCase()}`);
  }
  if (Object.hasOwn(report, "error")) return invalid("npm audit reported an error");
  if (report.auditReportVersion !== 2) return invalid("expected audit report version 2");
  if (!isPlainObject(report.vulnerabilities)) return invalid("expected a vulnerabilities object");

  const blocking = new Map();
  const ignored = new Set();
  const dependents = new Map();
  const resolved = new Set();
  const threshold = severities.indexOf("moderate");
  let hasThresholdAdvisory = false;
  for (const [name, vulnerability] of Object.entries(report.vulnerabilities)) {
    if (!isPlainObject(vulnerability) || !Array.isArray(vulnerability.via) || !vulnerability.via.length) {
      return invalid(`expected a non-empty via array for ${name}`);
    }
    for (const advisory of vulnerability.via) {
      if (typeof advisory === "string" && advisory.trim()) {
        if (!Object.hasOwn(report.vulnerabilities, advisory)) {
          return invalid(`missing via target ${advisory} for ${name}`);
        }
        if (!dependents.has(advisory)) dependents.set(advisory, []);
        dependents.get(advisory).push(name);
        continue;
      }
      if (!isPlainObject(advisory)) return invalid(`invalid via entry for ${name}`);
      const rank = severities.indexOf(advisory.severity);
      const url = typeof advisory.url === "string" && URL.parse(advisory.url);
      if (rank < 0 || !url || !["https:", "http:"].includes(url.protocol) ||
          typeof advisory.title !== "string" || !advisory.title.trim()) {
        return invalid(`invalid advisory severity, URL, or title for ${name}`);
      }
      resolved.add(name);
      const atThreshold = rank >= threshold;
      hasThresholdAdvisory ||= atThreshold;
      const id = advisoryId(url, advisory);
      if (activeExceptions.has(id)) ignored.add(id);
      else if (atThreshold) {
        blocking.set(id, { id, package: name, title: advisory.title, severity: advisory.severity });
      }
    }
  }
  // Walk references backwards from concrete advisories. Set iteration also
  // visits newly added packages, so cycles resolve only if they reach a seed.
  for (const name of resolved) {
    for (const dependent of dependents.get(name) ?? []) resolved.add(dependent);
  }
  for (const target of dependents.keys()) {
    if (!resolved.has(target)) return invalid(`via target ${target} does not resolve to an advisory`);
  }
  if (auditStatus !== 0 && !hasThresholdAdvisory) {
    return invalid(`npm audit exited with status ${auditStatus} without a moderate-or-higher advisory`);
  }
  return { ok: blocking.size === 0, blocking: [...blocking.values()], ignored: [...ignored] };
}

function selfTest() {
  const TEST_NOW = new Date("2026-11-18T00:00:00Z");
  const excepted = "GHSA-fc5q-39xc-vjpf";
  const fresh = "GHSA-aaaa-bbbb-cccc";
  const advisory = (id, severity) => ({
    severity, url: `https://github.com/advisories/${id}`, title: `Fixture ${id}`,
  });
  const report = (vulnerabilities) => ({ auditReportVersion: 2, vulnerabilities });
  const picomatch = { via: [advisory(excepted, "high")] };
  const high = { ...advisory(fresh, "high"), title: "New issue related to CVE-2026-112366" };
  const fallback = { ...picomatch.via[0], url: "https://example.test/advisory", title: "Issue CVE-2026-112366" };
  const cases = [
    ["clean", report({}), true],
    ["excepted", report({ picomatch }), true, [], [excepted]],
    ["excepted plus transitive high", report({
      picomatch, vite: { via: ["picomatch", "tinyglobby"] }, tinyglobby: { via: [high, high] },
    }), false, [fresh], [excepted]],
    ["new moderate", report({ other: { via: [advisory(fresh, "moderate")] } }), false, [fresh]],
    ["below threshold", report({ other: { via: [advisory(fresh, "info"), advisory(fresh, "low")] } }), true],
    ["CVE in title is not an identity", report({ picomatch: { via: [fallback] } }), false, [fallback.url]],
    ["CVE field is not an identity", report({ picomatch: { via: [{ ...fallback, title: "Issue", cve: "CVE-2026-112366" }] } }), false, [fallback.url]],
    ["different id citing CVE is blocked", report({ picomatch: { via: [{ severity: "high", url: "https://example.test/a", title: "Issue CVE-2026-112366", cve: ["CVE-2026-112366"] }] } }), false, ["https://example.test/a"]],
    ["missing vulnerabilities", { auditReportVersion: 2 }, false, [], [], /vulnerabilities/],
    ["npm error", { ...report({}), error: null }, false, [], [], /reported an error/],
    ["wrong version", { ...report({}), auditReportVersion: 1 }, false, [], [], /version 2/],
    ["malformed via", report({ other: { via: null } }), false, [], [], /via array/],
    ["malformed advisory", report({ other: { via: [advisory(fresh, "unknown")] } }), false, [], [], /invalid advisory/],
    ["clean with zero status", report({}), true, [], [], undefined, 0],
    ["exception explains nonzero status", report({ picomatch }), true, [], [excepted], undefined, 1],
    ["dangling reference", report({ picomatch, vite: { via: ["picomatch", "missing"] } }),
      false, [], [], /missing via target/],
    ["reference cycle without an advisory", report({ vite: { via: ["tinyglobby"] }, tinyglobby: { via: ["vite"] } }),
      false, [], [], /does not resolve/],
    ["reference chain", report({ vite: { via: ["tinyglobby"] }, tinyglobby: { via: ["picomatch"] }, picomatch }),
      true, [], [excepted]],
    ["reference cycle with a path to an advisory", report({
      vite: { via: ["tinyglobby"] }, tinyglobby: { via: ["vite", "picomatch"] }, picomatch,
    }), true, [], [excepted]],
    ["nonzero status with empty report", report({}), false, [], [], /npm audit exited with status 1/, 1],
    ["nonzero status with only a low advisory", report({ other: { via: [advisory(fresh, "low")] } }),
      false, [], [], /npm audit exited with status 1/, 1],
    ["blocking advisory explains nonzero status", report({ other: { via: [high] } }),
      false, [fresh], [], undefined, 1],
  ];
  for (const [name, input, ok, blocking = [], ignored = [], error, status] of cases) {
    const actual = evaluateAudit(input, undefined, status, TEST_NOW);
    assert.equal(actual.ok, ok, name);
    assert.deepEqual(actual.blocking.map(({ id }) => id), blocking, name);
    assert.deepEqual(actual.ignored, ignored, name);
    if (error) assert.match(actual.error, error, name);
    else assert.equal(actual.error, undefined, name);
  }
  const validEntry = exceptions[0];
  const expired = { ...validEntry, reviewBy: "2026-11-17" };
  const malformed = [
    null,
    {},
    { ...validEntry, id: "not-a-ghsa" },
    { ...validEntry, reason: "  " },
    { ...validEntry, owner: "" },
    { ...validEntry, reviewBy: undefined },
    { ...validEntry, reviewBy: "2026-02-30" },
  ];
  assert.equal(evaluateAudit(report({ picomatch }), [validEntry], 0, TEST_NOW).ok, true, "in-date exception");
  assert.match(evaluateAudit(report({ picomatch }), [expired], 0, TEST_NOW).error, /expired/, "expired exception fails closed");
  assert.match(evaluateAudit(report({ picomatch }), [validEntry], 0, new Date("2027-05-18T00:00:00Z")).error, /expired/, "expiry boundary fails closed");
  for (const [index, entry] of malformed.entries()) {
    assert.equal(evaluateAudit(report({ picomatch }), [entry], 0, TEST_NOW).ok, false, `malformed exception ${index}`);
  }
  assert.equal(evaluateAudit(report({ picomatch }), "not-a-list", 0, TEST_NOW).ok, false, "exception list must be an array");
  assert.equal(evaluateAudit(report({ picomatch }), [validEntry], 0, "not-a-date").ok, false, "invalid now fails closed");
  console.log(`audit self-test: ${cases.length} table cases + exception-entry cases passed`);
}

function fail(message) {
  console.error(`audit gate: ${message}`);
  return 1;
}

function main() {
  if (process.argv[2] === "--self-test") {
    selfTest();
    return 0;
  }

  const { error, signal, status, stdout } = spawnSync("npm", ["audit", "--json", "--audit-level=moderate"], { encoding: "utf8" });
  if (error) return fail(`could not run npm audit: ${error.message}`);
  if (signal) return fail(`npm audit terminated by ${signal}`);
  if (!stdout?.trim()) return fail("npm audit returned no JSON output");

  let report;
  try {
    report = JSON.parse(stdout);
  } catch {
    return fail("npm audit returned invalid JSON");
  }
  const result = evaluateAudit(report, undefined, status);
  if (result.error) return fail(result.error);
  if (!result.ok) {
    for (const { id, package: name, title, severity } of result.blocking) {
      console.error(`audit gate: ${id} (${name}, ${severity}): ${title}`);
    }
    return 1;
  }
  const ignored = result.ignored.length ? `; excepted ${result.ignored.join(", ")}` : "";
  console.log(`audit gate: no unexcepted moderate-or-higher advisories${ignored}`);
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = main();
}
