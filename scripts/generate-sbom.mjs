import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";

const outputPath = process.argv[2];
if (!outputPath) {
  throw new Error("usage: node scripts/generate-sbom.mjs <OUTPUT_PATH>");
}

const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--format-version", "1", "--locked"], {
  encoding: "utf8",
  maxBuffer: 64 * 1024 * 1024,
}));
const packages = metadata.packages.map((pkg, index) => ({
  SPDXID: `SPDXRef-Package-${index}-${pkg.name}-${pkg.version}`.replaceAll(/[^A-Za-z0-9.-]/g, "-"),
  name: pkg.name,
  versionInfo: pkg.version,
  downloadLocation: "NOASSERTION",
  licenseConcluded: pkg.license ?? "NOASSERTION",
  licenseDeclared: pkg.license ?? "NOASSERTION",
  filesAnalyzed: false,
}));
const sbom = {
  spdxVersion: "SPDX-2.3",
  dataLicense: "CC0-1.0",
  SPDXID: "SPDXRef-DOCUMENT",
  name: "qcg",
  documentNamespace: `https://qcg.dev/sbom/${metadata.workspace_members.length}`,
  creationInfo: { created: new Date().toISOString(), creators: ["Tool: qcg distribution builder"] },
  packages,
};
writeFileSync(outputPath, `${JSON.stringify(sbom, null, 2)}\n`);
