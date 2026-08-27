import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";

const tmp = mkdtempSync(resolve(tmpdir(), "qcg-generator-api."));

try {
  const beforeOpenapi = readFileSync("src/api/openapi.json", "utf8");
  const beforeTypes = readFileSync("src/api/types.d.ts", "utf8");
  execFileSync("npm", ["run", "generate:api"], { stdio: "inherit" });
  const afterOpenapi = readFileSync("src/api/openapi.json", "utf8");
  const afterTypes = readFileSync("src/api/types.d.ts", "utf8");
  if (beforeOpenapi !== afterOpenapi || beforeTypes !== afterTypes) {
    throw new Error("generated API types are stale; run npm run generate:api in frontend/generator");
  }
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
