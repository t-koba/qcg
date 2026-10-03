import { test } from "node:test";
import assert from "node:assert/strict";
import { verifySpdx } from "./verify-spdx.mjs";
import { declaredLicense } from "./licenses.mjs";

test("Cargo legacy license alternatives become SPDX expressions", () => {
  assert.equal(declaredLicense("MIT/Apache-2.0", true), "MIT OR Apache-2.0");
  assert.equal(declaredLicense("MIT OR Apache-2.0", true), "MIT OR Apache-2.0");
  assert.equal(declaredLicense("UNLICENSED"), "NOASSERTION");
  assert.equal(declaredLicense(null), "NOASSERTION");
});
const valid = () => ({
  spdxVersion: "SPDX-2.3",
  SPDXID: "SPDXRef-DOCUMENT",
  dataLicense: "CC0-1.0",
  documentNamespace: "https://example.com/revision/target/digest",
  creationInfo: { created: "2026-10-02T00:00:00Z" },
  packages: [
    { SPDXID: "SPDXRef-Product" },
    {
      SPDXID: "SPDXRef-Rust",
      externalRefs: [{ referenceLocator: "pkg:cargo/example@1" }],
    },
    {
      SPDXID: "SPDXRef-Npm",
      externalRefs: [{ referenceLocator: "pkg:npm/example@1" }],
    },
  ],
  relationships: [
    {
      spdxElementId: "SPDXRef-DOCUMENT",
      relationshipType: "DESCRIBES",
      relatedSpdxElement: "SPDXRef-Product",
    },
  ],
});
test("SPDX validation rejects broken identifiers, timestamp, dependency edges and missing ecosystems", () => {
  assert.equal(verifySpdx(valid()), 4);
  for (const mutate of [
    (bom) => bom.packages.push(bom.packages[0]),
    (bom) => (bom.creationInfo.created = "invalid"),
    (bom) => (bom.relationships[0].relatedSpdxElement = "SPDXRef-missing"),
    (bom) => bom.packages.pop(),
  ]) {
    const bom = valid();
    mutate(bom);
    assert.throws(() => verifySpdx(bom));
  }
});
