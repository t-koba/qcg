import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

// Exact-ID exception per project policy, not a broad suppression: picomatch
// <=4.0.7 is dev-only in this private demo generator with zero runtime deps.
// As of 2026-11-17 there is no patched release; npm's only proposed fix is a
// Vitest 4 -> 3 downgrade, which we reject. Every other moderate-or-higher
// advisory fails the gate. Remove this exception when patched picomatch
// (>4.0.7) is available.
const allowlist = new Set(["GHSA-fc5q-39xc-vjpf"]);
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

export function evaluateAudit(report, exceptions = allowlist) {
  const invalid = (error) => ({ ok: false, blocking: [], ignored: [], error });
  if (!isPlainObject(report)) return invalid("expected a JSON object");
  if (Object.hasOwn(report, "error")) return invalid("npm audit reported an error");
  if (report.auditReportVersion !== 2) return invalid("expected audit report version 2");
  if (!isPlainObject(report.vulnerabilities)) return invalid("expected a vulnerabilities object");

  const blocking = new Map();
  const ignored = new Set();
  for (const [name, vulnerability] of Object.entries(report.vulnerabilities)) {
    if (!isPlainObject(vulnerability) || !Array.isArray(vulnerability.via) || !vulnerability.via.length) {
      return invalid(`expected a non-empty via array for ${name}`);
    }
    for (const advisory of vulnerability.via) {
      // npm represents transitive references as names; the advisories themselves
      // are checked in their own package entry.
      if (typeof advisory === "string" && advisory.trim()) continue;
      if (!isPlainObject(advisory)) return invalid(`invalid via entry for ${name}`);
      const rank = severities.indexOf(advisory.severity);
      const url = typeof advisory.url === "string" && URL.parse(advisory.url);
      if (rank < 0 || !url || !["https:", "http:"].includes(url.protocol) ||
          typeof advisory.title !== "string" || !advisory.title.trim()) {
        return invalid(`invalid advisory severity, URL, or title for ${name}`);
      }
      const id = advisoryId(url, advisory);
      if (exceptions.has(id)) ignored.add(id);
      else if (rank >= severities.indexOf("moderate")) {
        blocking.set(id, { id, package: name, title: advisory.title, severity: advisory.severity });
      }
    }
  }
  return { ok: blocking.size === 0, blocking: [...blocking.values()], ignored: [...ignored] };
}

function selfTest() {
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
  ];
  for (const [name, input, ok, blocking = [], ignored = [], error] of cases) {
    const actual = evaluateAudit(input);
    assert.equal(actual.ok, ok, name);
    assert.deepEqual(actual.blocking.map(({ id }) => id), blocking, name);
    assert.deepEqual(actual.ignored, ignored, name);
    if (error) assert.match(actual.error, error, name);
    else assert.equal(actual.error, undefined, name);
  }
  console.log(`audit self-test: ${cases.length} passed`);
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

  const { error, signal, stdout } = spawnSync("npm", ["audit", "--json", "--audit-level=moderate"], { encoding: "utf8" });
  if (error) return fail(`could not run npm audit: ${error.message}`);
  if (signal) return fail(`npm audit terminated by ${signal}`);
  if (!stdout?.trim()) return fail("npm audit returned no JSON output");

  // npm exits nonzero for vulnerabilities; decide from the JSON, not its status.
  let report;
  try {
    report = JSON.parse(stdout);
  } catch {
    return fail("npm audit returned invalid JSON");
  }
  const result = evaluateAudit(report);
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
