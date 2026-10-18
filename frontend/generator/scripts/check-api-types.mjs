import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const uiRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const checkedIn = resolve(uiRoot, "src/api");
const sharedTypes = resolve(uiRoot, "../../clients/ts/types.ts");
const generated = mkdtempSync(resolve(tmpdir(), "generator-api."));
const frontendTypesReexport = 'export type * from "../../../../clients/ts/types";\n';

function readOrFail(directory, file, description) {
  try {
    return readFileSync(resolve(directory, file), "utf8");
  } catch (error) {
    throw new Error(
      `${description} is unreadable at ${resolve(directory, file)}; run npm run generate:api in frontend/generator: ${error.message}`,
    );
  }
}

try {
  execFileSync(process.execPath, [resolve(uiRoot, "scripts/generate-api-types.mjs"), generated], {
    cwd: uiRoot,
    stdio: "inherit",
  });
  const generatedOpenapi = readOrFail(generated, "openapi.json", "generated openapi.json");
  const checkedInOpenapi = readOrFail(checkedIn, "openapi.json", "checked-in openapi.json");
  if (checkedInOpenapi !== generatedOpenapi) {
    throw new Error(
      `generated openapi.json is stale; run npm run generate:api in frontend/generator (fresh output: ${resolve(generated, "openapi.json")})`,
    );
  }

  const generatedTypes = readOrFail(generated, "types.d.ts", "generated OpenAPI types");
  const canonicalTypes = readOrFail(sharedTypes, "", "shared clients/ts/types.ts");
  if (canonicalTypes !== generatedTypes) {
    throw new Error(
      `shared clients/ts/types.ts is stale; run node scripts/generate-sdk.mjs (fresh frontend output: ${resolve(generated, "types.d.ts")})`,
    );
  }

  const frontendTypes = readOrFail(checkedIn, "types.d.ts", "frontend type re-export");
  if (frontendTypes !== frontendTypesReexport) {
    throw new Error(
      "frontend src/api/types.d.ts must re-export the canonical clients/ts/types.ts; run npm run generate:api in frontend/generator",
    );
  }
} finally {
  rmSync(generated, { recursive: true, force: true });
}
console.log("generated OpenAPI and shared API types match the checked-in copies");
