import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";

const ci = readFileSync(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");
const release = readFileSync(
  new URL("../.github/workflows/release.yml", import.meta.url),
  "utf8",
);

function jobBlock(source, id) {
  const lines = source.split(/\r?\n/);
  const jobLine = /^  ([A-Za-z0-9_-]+):\s*$/;
  const start = lines.findIndex((line) => jobLine.exec(line)?.[1] === id);
  assert.notEqual(start, -1, `workflow must define job ${id}`);
  let end = lines.length;
  for (let index = start + 1; index < lines.length; index += 1) {
    if (jobLine.test(lines[index])) {
      end = index;
      break;
    }
  }
  return lines.slice(start, end).join("\n");
}

test("tag releases wait for local CI on the same commit before bundle publication", () => {
  const triggers = ci.split(/^jobs:\s*$/m, 1)[0];
  const releaseTriggers = release.split(/^jobs:\s*$/m, 1)[0];
  assert.match(releaseTriggers, /^  push:\n    tags:\n      - "v\*"\s*$/m);
  assert.match(triggers, /^  workflow_call:\s*$/m);
  assert.match(triggers, /^  push:\n    branches: \[main\]/m);
  assert.match(triggers, /^  pull_request:\s*$/m);
  assert.match(triggers, /^  workflow_dispatch:\s*$/m);

  const requiredCi = jobBlock(release, "required-ci");
  assert.match(requiredCi, /^    uses: \.\/\.github\/workflows\/ci\.yml\s*$/m);
  assert.doesNotMatch(requiredCi, /^\s+with:\s*$/m);
  const bundle = jobBlock(release, "bundle");
  const publish = jobBlock(release, "github-release");
  assert.match(bundle, /^    needs: required-ci\s*$/m);
  assert.match(publish, /^    needs: bundle\s*$/m);
  assert.doesNotMatch(bundle, /^    if:.*\b(?:always|failure|cancelled)\s*\(/m);
  assert.doesNotMatch(publish, /^    if:.*\b(?:always|failure|cancelled)\s*\(/m);
});

test("called CI jobs cannot skip required checks on tagged releases", () => {
  // Job keys use four spaces; deeper step-level conditions remain allowed.
  assert.doesNotMatch(
    ci,
    /^    if\s*:/m,
    "CI jobs must run unconditionally when called from a tagged release",
  );
});
