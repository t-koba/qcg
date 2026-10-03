import { cargoMetadata } from "./product.mjs";
import { readFileSync, writeFileSync } from "node:fs";

const lock = JSON.parse(readFileSync("frontend/generator/package-lock.json", "utf8"));
const entries = Object.entries(lock.packages ?? {})
  .filter(([path, value]) => path !== "" && value.version)
  .map(([path, value]) => ({
    name: value.name ?? path.split("node_modules/").at(-1),
    version: value.version,
    license: value.license ?? "SEE PACKAGE LICENSE",
  }))
  .sort((left, right) => `${left.name}@${left.version}`.localeCompare(`${right.name}@${right.version}`));

const lines = [
  "THIRD-PARTY NOTICES",
  "",
  "This file lists the npm packages used to build the bundled frontend/generator application.",
  "Each package remains available under its own license.",
  "",
  "Rust packages (including build and verification dependencies):",
  ...cargoMetadata(true).packages.sort((a,b)=>a.name.localeCompare(b.name)).map(pkg=>`- ${pkg.name}@${pkg.version} — ${pkg.license ?? "NOASSERTION"}`),
  "",
  "npm packages:",
  ...entries.map(({ name, version, license }) => `- ${name}@${version} — ${license}`),
  "",
];

const output = process.argv[2] ?? "THIRD-PARTY-NOTICES";
writeFileSync(output, lines.join("\n"));
