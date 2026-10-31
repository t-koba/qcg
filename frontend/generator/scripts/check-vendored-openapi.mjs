import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const root = new URL("../", import.meta.url);
const json = (path) => JSON.parse(readFileSync(new URL(path, root), "utf8"));
const require = createRequire(new URL("package.json", root));
const lock = json("package-lock.json");
const manifest = json("package.json");
const key = "node_modules/@redocly/openapi-core";
const fork = lock.packages[key];
const spec = "file:vendor/qcg-openapi-core-1.34.20.tgz";
assert.equal(manifest.devDependencies["@redocly/openapi-core"], spec);
assert.equal(lock.packages[""].devDependencies["@redocly/openapi-core"], spec);
assert.equal(manifest.overrides, undefined);
assert.equal(fork.name, "@qcg/openapi-core");
assert.equal(fork.version, "1.34.20");
assert.equal(fork.resolved, spec);
assert.equal(fork.license, "MIT");
assert.equal(fork.integrity, "sha512-" + createHash("sha512")
  .update(readFileSync(new URL(spec.slice(5), root))).digest("base64"));
const fromTypescript = createRequire(require.resolve("openapi-typescript"));
assert.equal(fromTypescript.resolve("@redocly/openapi-core"), require.resolve("@redocly/openapi-core"));
const installed = require("@redocly/openapi-core/package.json");
assert.equal(installed.name, fork.name);
assert.equal(installed.version, fork.version);
assert.equal(installed.private, true, "the local fork must remain unpublished");
assert.deepEqual(installed.dependencies, fork.dependencies);
assert.deepEqual(require("@redocly/openapi-core/QCG-UPSTREAM.json"), json("vendor/openapi-core-upstream.json"));
assert.equal(readFileSync(require.resolve("@redocly/openapi-core/QCG-PATCH.diff"), "utf8"),
  readFileSync(new URL("vendor/openapi-core.patch", root), "utf8"));

const closure = [
  ["minimatch", "11.0.3", "BlueOak-1.0.0", "sha512-iny/40PEGkNJQ5/Pv6z3nm6FbRAby32GeZRjCUJk2WnNDbgPSTcdI+DDvLqsgBdD4cD5sMtMheqPPnehCCa8Nw=="],
  ["@isaacs/nonrecursive-glob-match", "1.0.3", "BlueOak-1.0.0", "sha512-SI7kyKHq4XXDRozw+r9GlRZkia1ZkFrpLW98r2IC2Bpr8YkFGE3fvjzmxCG0x78A++69HmDNrDvSMYBZmHGpKA=="],
  ["brace-expansion", "5.0.12", "MIT", "sha512-YovQ3rzhaLMIrDjNDMkNS01tea93qhEhG5xy8f6+R0l+dw3Ki+5sCoIoI942iuLZTHWogWktgwVDhU09iNEimQ=="],
  ["balanced-match", "4.0.4", "MIT", "sha512-BLrgEcRTwX2o6gGxGOCNyMvGSp35YofuYzw9h1IMTRmKqttAZZVU67bdb9Pr2vUHA8+j3i2tJfjO6C6+4myGTA=="],
];
for (const [name, version, license, integrity] of closure) {
  const entry = lock.packages[`node_modules/${name}`];
  assert.deepEqual([entry.version, entry.license, entry.integrity], [version, license, integrity]);
}
assert.deepEqual(Object.entries(lock.packages)
  .filter(([, pkg]) => pkg.dependencies?.minimatch)
  .map(([path, pkg]) => [path, pkg.dependencies.minimatch]), [[key, "11.0.3"]]);
assert.equal(Object.keys(lock.packages).filter((path) => path.endsWith("/minimatch")).length, 1);
assert.equal(lock.packages["node_modules/deepmerge"]?.version, "4.3.1",
  "keep the existing low-severity deepmerge advisory visible");

// Exercise Redocly's actual URL/header path with its customFetch seam, never a network call.
const { readFileFromUrl } = require("@redocly/openapi-core/lib/utils.js");
const url = "https://api.example.test/v1/schema.json";
const cases = [
  [url, true],
  ["api.example.test/v1/schema.json", true],
  ["https://api.example.test/**", true],
  ["api.example.test/**", true],
  ["api.example.test/*", false],
  ["api.example.test/{v1,v2}/*.json", true],
  ["api.example.test/v?/schema.json", true],
  ["api.example.test/v[12]/schema.json", true],
  ["http://api.example.test/**", false],
  ["other.example.test/**", false],
];
for (const [pattern, matches] of cases) {
  const result = await readFileFromUrl(url, {
    headers: [{ matches: pattern, name: "x-test", value: "selected" }],
    customFetch: async (requested, options) => {
      assert.equal(requested, url);
      assert.deepEqual(options.headers, matches ? { "x-test": "selected" } : {});
      return new Response("schema", { headers: { "content-type": "application/json" } });
    },
  });
  assert.deepEqual(result, { body: "schema", mimeType: "application/json" });
}
console.log("fork identity, provenance, locked closure integrity and 10 URL/header cases passed");
