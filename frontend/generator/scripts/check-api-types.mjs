import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const uiRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const checkedIn = resolve(uiRoot, "src/api");
const generated = mkdtempSync(resolve(tmpdir(), "qcg-generator-api."));

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
  for (const file of ["openapi.json", "types.d.ts"]) {
    const expected = readOrFail(checkedIn, file, `checked-in ${file}`);
    const actual = readOrFail(generated, file, `generated ${file}`);
    if (expected !== actual) {
      throw new Error(
        `generated ${file} is stale; run npm run generate:api in frontend/generator (fresh output: ${resolve(generated, file)})`,
      );
    }
  }
} finally {
  rmSync(generated, { recursive: true, force: true });
}
console.log("generated API types match the checked-in copies");
