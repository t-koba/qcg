import { test } from "node:test";
import assert from "node:assert/strict";
import { resolveProduct, verifyVersion } from "./product.mjs";
const pkg = {
  id: "cli",
  name: "cli",
  version: "0.1.0",
  targets: [{ name: "qcg", kind: ["bin"] }],
};
const metadata = {
  workspace_members: ["cli"],
  packages: [
    { id: "mcp", name: "mcp", targets: [{ name: "mcp_test", kind: ["bin"] }] },
    pkg,
  ],
};
test("product resolves the binary owner and validates release tags", () => {
  const product = resolveProduct(metadata);
  assert.equal(product.package, "cli");
  assert.equal(product.name, "qcg");
  verifyVersion(product, "v0.1.0");
  assert.throws(() => verifyVersion(product, "v0.2.0"));
  assert.throws(() => resolveProduct({ ...metadata, packages: [] }));
  assert.throws(() => resolveProduct({ ...metadata, packages: [pkg, pkg] }));
  assert.equal(resolveProduct(metadata, "renamed").binary, "qcg");
  assert.throws(() => resolveProduct(metadata, "../escape"));
});
