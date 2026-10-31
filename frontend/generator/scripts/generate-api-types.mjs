import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { openapiTypes } from "./openapi-types.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const uiRoot = resolve(root, "frontend/generator");
const outDir = resolve(process.argv[2] ?? resolve(uiRoot, "src/api"));
const openapiPath = resolve(outDir, "openapi.json");
// The root TypeScript SDK owns the single checked-in OpenAPI type declaration.
// The frontend imports it through src/api/types.d.ts; only temporary check
// generations materialize a second copy for byte comparison.
const temporaryOutput = process.argv[2] !== undefined;
const typesPath = temporaryOutput
  ? resolve(outDir, "types.d.ts")
  : resolve(root, "clients/ts/types.ts");

mkdirSync(dirname(openapiPath), { recursive: true });
const openapi = execFileSync("cargo", ["run", "-q", "-p", "cli", "--locked", "--", "docs", "openapi"], {
  cwd: root,
  encoding: "utf8",
});
writeFileSync(openapiPath, openapi);
writeFileSync(typesPath, await openapiTypes(JSON.parse(openapi)));
if (!temporaryOutput) {
  writeFileSync(
    resolve(uiRoot, "src/api/types.d.ts"),
    'export type * from "../../../../clients/ts/types";\n',
  );
}
console.log(`generated ${outDir}`);
