#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
node scripts/generate-third-party-notices.mjs "$tmp"
node --input-type=module - "$tmp" <<'NODE'
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const metadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--format-version", "1", "--locked", "--no-deps"], {
    encoding: "utf8",
  }),
);
const members = new Set(metadata.workspace_members);
const workspacePackages = metadata.packages.filter((pkg) => members.has(pkg.id));
const rustRows = readFileSync(process.argv[2], "utf8")
  .split("\nnpm packages:\n", 1)[0]
  .split("\n")
  .filter((line) => line.startsWith("- "));
for (const pkg of workspacePackages) {
  const row = `- ${pkg.name}@${pkg.version} — `;
  if (rustRows.some((line) => line.startsWith(row)))
    throw new Error(
      `workspace package ${pkg.name}@${pkg.version} appears in third-party Rust notices`,
    );
}
NODE
cmp -s "$tmp" THIRD-PARTY-NOTICES || {
  echo "THIRD-PARTY-NOTICES is stale; regenerate it with node scripts/generate-third-party-notices.mjs THIRD-PARTY-NOTICES" >&2
  exit 1
}
echo "third-party notices are current"
