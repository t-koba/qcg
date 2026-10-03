import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";

export function resolveProduct(metadata, productId = "qcg") {
  if (!/^[a-z][a-z0-9-]*$/.test(productId))
    throw new Error("invalid product id");
  const members = new Set(metadata.workspace_members);
  const matches = metadata.packages.filter(
    (pkg) =>
      members.has(pkg.id) &&
      pkg.targets.some(
        (target) => target.name === "qcg" && target.kind.includes("bin"),
      ),
  );
  if (matches.length !== 1)
    throw new Error(
      `expected one workspace owner of binary qcg, found ${matches.length}`,
    );
  return {
    name: productId,
    binary: "qcg",
    package: matches[0].name,
    version: matches[0].version,
    packageId: matches[0].id,
  };
}
export function cargoMetadata(full = false, target) {
  return JSON.parse(
    execFileSync(
      "cargo",
      [
        "metadata",
        "--format-version",
        "1",
        "--locked",
        ...(full ? [] : ["--no-deps"]),
        ...(target ? ["--filter-platform", target] : []),
      ],
      { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
    ),
  );
}
export function verifyVersion(product, tag) {
  const version = tag.replace(/^v/, "");
  if (version !== product.version)
    throw new Error(
      `tag version ${version} does not match Cargo version ${product.version}`,
    );
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const product = resolveProduct(
    cargoMetadata(),
    process.env.PRODUCT_ID || "qcg",
  );
  if (process.argv[2] === "verify-version")
    verifyVersion(product, process.argv[3] ?? "");
  else if (process.argv[2]) {
    if (!(process.argv[2] in product))
      throw new Error("unknown product property");
    console.log(product[process.argv[2]]);
  } else console.log(JSON.stringify(product));
}
