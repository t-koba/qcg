import { verifySpdx } from "./verify-spdx.mjs";
import { readFileSync, existsSync, readdirSync } from "node:fs";
import { resolve, dirname, join, relative, isAbsolute } from "node:path";
const root = resolve(process.argv[2] ?? ".");
for (const name of [
  "README.md",
  "THIRD-PARTY-NOTICES",
  "SBOM.spdx.json",
  "providers.toml",
  "docs/agent-mcp-verification.md",
  "docs/capability-matrix.md",
  "generators/generator/ui/index.html",
]) {
  if (!existsSync(join(root, name)))
    throw new Error(`missing bundle file: ${name}`);
}
const markdown = [
  join(root, "README.md"),
  ...readdirSync(join(root, "docs"))
    .filter((name) => name.endsWith(".md"))
    .map((name) => join(root, "docs", name)),
];
for (const file of markdown) {
  for (const match of readFileSync(file, "utf8").matchAll(
    /!?\[[^\]]*\]\(([^\s)]+)(?:\s+[^)]*)?\)/g,
  )) {
    const href = match[1];
    if (/^[a-z]+:|^#/i.test(href)) continue;
    const path = resolve(dirname(file), decodeURIComponent(href.split("#")[0]));
    const within = relative(root, path);
    if (
      within === ".." ||
      within.startsWith("../") ||
      within.startsWith("..\\") ||
      isAbsolute(within) ||
      !existsSync(path)
    )
      throw new Error(`broken bundle link: ${file} -> ${href}`);
  }
}
const sbom = JSON.parse(readFileSync(join(root, "SBOM.spdx.json"), "utf8"));
verifySpdx(sbom);
const ui = join(root, "generators/generator/ui");
for (const match of readFileSync(join(ui, "index.html"), "utf8").matchAll(
  /(?:src|href)="([^"#]+)"/g,
)) {
  if (!existsSync(resolve(ui, match[1])))
    throw new Error(`missing SPA asset ${match[1]}`);
}
if (!readdirSync(join(ui, "assets")).some((name) => name.endsWith(".wasm")))
  throw new Error("missing WASM module");
console.log(
  "expanded bundle links, required files, SPA/WASM and SPDX graph verified",
);
