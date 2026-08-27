import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const uiRoot = resolve(root, "frontend/generator");
const openapiPath = resolve(uiRoot, "src/api/openapi.json");
const typesPath = resolve(uiRoot, "src/api/types.d.ts");

mkdirSync(dirname(openapiPath), { recursive: true });
const openapi = execFileSync("cargo", ["run", "-p", "qcg", "--", "docs", "openapi"], {
  cwd: root,
  encoding: "utf8",
});
writeFileSync(openapiPath, openapi);
execFileSync("npx", ["openapi-typescript", openapiPath, "-o", typesPath], {
  cwd: uiRoot,
  stdio: "inherit",
});
