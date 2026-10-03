import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { cargoMetadata, resolveProduct } from "./product.mjs";
import { declaredLicense } from "./licenses.mjs";

const outputPath = process.argv[2];
if (!outputPath)
  throw new Error("usage: node scripts/generate-sbom.mjs <OUTPUT_PATH>");
const target =
  process.env.CARGO_BUILD_TARGET ||
  execFileSync("rustc", ["-vV"], { encoding: "utf8" }).match(
    /^host: (.+)$/m,
  )[1];
const metadata = cargoMetadata(true, target);
const wasmMetadata = cargoMetadata(true, "wasm32-unknown-unknown");
const product = resolveProduct(metadata, process.env.PRODUCT_ID || "qcg");
const lock = JSON.parse(
  readFileSync("frontend/generator/package-lock.json", "utf8"),
);
const canonical = new Map(
  metadata.packages.map((pkg) => [
    pkg.id,
    `${pkg.source ?? "workspace"}:${pkg.name}@${pkg.version}`,
  ]),
);
const id = (key) =>
  `SPDXRef-${createHash("sha256")
    .update(canonical.get(key) ?? key)
    .digest("hex")}`;
const packages = [
  {
    SPDXID: "SPDXRef-Product",
    name: product.name,
    versionInfo: product.version,
    downloadLocation: "NOASSERTION",
    filesAnalyzed: false,
    licenseConcluded: "NOASSERTION",
    licenseDeclared: "NOASSERTION",
  },
];
const relationships = [
  {
    spdxElementId: "SPDXRef-DOCUMENT",
    relationshipType: "DESCRIBES",
    relatedSpdxElement: "SPDXRef-Product",
  },
];
const selected = new Set();
const visited = new Set();
const edges = new Map();
function visit(key, graph, platform) {
  const marker = `${platform}:${key}`;
  if (visited.has(marker)) return;
  visited.add(marker);
  selected.add(key);
  for (const dep of graph.get(key)?.deps ?? []) {
    if (!dep.dep_kinds.some((kind) => kind.kind !== "dev")) continue;
    const build = dep.dep_kinds.every(
      (kind) => kind.kind === "build" || kind.kind === "dev",
    );
    const edge = build
      ? {
          spdxElementId: id(dep.pkg),
          relationshipType: "BUILD_DEPENDENCY_OF",
          relatedSpdxElement: id(key),
        }
      : {
          spdxElementId: id(key),
          relationshipType: "DEPENDS_ON",
          relatedSpdxElement: id(dep.pkg),
        };
    edges.set(JSON.stringify(edge), edge);
    visit(dep.pkg, graph, platform);
  }
}
visit(
  product.packageId,
  new Map(metadata.resolve.nodes.map((node) => [node.id, node])),
  target,
);
const wasmNodes = new Map(
  wasmMetadata.resolve.nodes.map((node) => [node.id, node]),
);
for (const pkg of metadata.packages)
  if (pkg.name === "expr-wasm" && metadata.workspace_members.includes(pkg.id)) {
    visit(pkg.id, wasmNodes, "wasm32-unknown-unknown");
    relationships.push({
      spdxElementId: "SPDXRef-Product",
      relationshipType: "DEPENDS_ON",
      relatedSpdxElement: id(pkg.id),
    });
  }
relationships.push(...edges.values());
for (const pkg of metadata.packages
  .filter((pkg) => selected.has(pkg.id))
  .sort((a, b) => a.id.localeCompare(b.id))) {
  packages.push({
    SPDXID: id(pkg.id),
    name: pkg.name,
    versionInfo: pkg.version,
    downloadLocation: "NOASSERTION",
    licenseConcluded: "NOASSERTION",
    licenseDeclared: declaredLicense(pkg.license, true),
    filesAnalyzed: false,
    externalRefs: [
      {
        referenceCategory: "PACKAGE-MANAGER",
        referenceType: "purl",
        referenceLocator: `pkg:cargo/${pkg.name}@${pkg.version}`,
      },
    ],
  });
}
relationships.push({
  spdxElementId: "SPDXRef-Product",
  relationshipType: "DEPENDS_ON",
  relatedSpdxElement: id(product.packageId),
});
const npmEntries = Object.entries(lock.packages ?? {})
  .filter(([path, pkg]) => path && pkg.version)
  .sort(([a], [b]) => a.localeCompare(b));
const npmPaths = new Set(npmEntries.map(([path]) => path));
function npmDependency(path, name) {
  let parent = path;
  while (parent) {
    const candidate = `${parent}/node_modules/${name}`;
    if (npmPaths.has(candidate)) return candidate;
    const index = parent.lastIndexOf("/node_modules/");
    parent = index < 0 ? "" : parent.slice(0, index);
  }
  const top = `node_modules/${name}`;
  return npmPaths.has(top) ? top : null;
}
for (const [path, pkg] of npmEntries) {
  const name = pkg.name ?? path.split("node_modules/").at(-1);
  packages.push({
    SPDXID: id(`npm:${path}`),
    name,
    versionInfo: pkg.version,
    downloadLocation: pkg.resolved ?? "NOASSERTION",
    licenseConcluded: "NOASSERTION",
    licenseDeclared: declaredLicense(pkg.license),
    filesAnalyzed: false,
    externalRefs: [
      {
        referenceCategory: "PACKAGE-MANAGER",
        referenceType: "purl",
        referenceLocator: `pkg:npm/${name.replace("@", "%40")}@${pkg.version}`,
      },
    ],
    ...(pkg.integrity?.startsWith("sha512-")
      ? {
          checksums: [
            {
              algorithm: "SHA512",
              checksumValue: Buffer.from(
                pkg.integrity.slice(7),
                "base64",
              ).toString("hex"),
            },
          ],
        }
      : {}),
  });
  relationships.push({
    spdxElementId: id(`npm:${path}`),
    relationshipType:
      name === "svelte" ? "DEPENDENCY_OF" : "BUILD_DEPENDENCY_OF",
    relatedSpdxElement: "SPDXRef-Product",
  });
  for (const name of Object.keys({
    ...pkg.dependencies,
    ...pkg.optionalDependencies,
  })) {
    const target = npmDependency(path, name);
    if (target)
      relationships.push({
        spdxElementId: id(`npm:${path}`),
        relationshipType: "DEPENDS_ON",
        relatedSpdxElement: id(`npm:${target}`),
      });
  }
}
const revision = execFileSync("git", ["rev-parse", "HEAD"], {
  encoding: "utf8",
}).trim();

const created = new Date(
  Number(
    process.env.SOURCE_DATE_EPOCH ??
      execFileSync("git", ["show", "-s", "--format=%ct", "HEAD"], {
        encoding: "utf8",
      }).trim(),
  ) * 1000,
)
  .toISOString()
  .replace(/\.\d{3}Z$/, "Z");
const sources = createHash("sha256");
for (const file of execFileSync(
  "git",
  ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
  { encoding: "utf8" },
)
  .split("\0")
  .filter(Boolean)
  .sort()) {
  try {
    sources.update(file).update(readFileSync(file));
  } catch {
    /* deleted file is represented by its absence */
  }
}
const sourceDigest = sources.digest("hex");
const contents = JSON.stringify({
  packages,
  relationships,
  revision,
  target,
  created,
  sourceDigest,
});
const digest = createHash("sha256").update(contents).digest("hex");
const sbom = {
  spdxVersion: "SPDX-2.3",
  dataLicense: "CC0-1.0",
  SPDXID: "SPDXRef-DOCUMENT",
  name: `${product.name}-${target}`,
  documentNamespace: `https://github.com/t-koba/qcg/sbom/${revision}/${target}/${digest}`,
  creationInfo: { created, creators: ["Tool: qcg distribution builder"] },
  documentComment: `revision=${revision}; target=${target}; wasm_target=wasm32-unknown-unknown; source_sha256=${sourceDigest}; npm entries include build dependencies`,
  packages,
  relationships,
};
writeFileSync(outputPath, `${JSON.stringify(sbom, null, 2)}\n`);
