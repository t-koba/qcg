import { readFileSync } from "node:fs";
export function verify(matrix, listing, platform = process.platform) {
  const registered = new Set(
    [...listing.matchAll(/^(.+): test$/gm)].map((m) => m[1].split("::").at(-1)),
  );
  if (!registered.size) throw new Error("test registry is empty");
  const rows = matrix
    .split("\n")
    .filter((row) => row.startsWith("|") && !row.includes("See `scripts/"));
  const names = rows.flatMap((row) =>
    [...row.matchAll(/`([a-z0-9_]+)`/g)].map((m) => m[1]),
  );
  if (!names.length) throw new Error("matrix names no tests");
  for (const name of names) {
    if (platform === "win32" && name.startsWith("g06_")) continue; // Unix file mode guarantee.
    if (!registered.has(name))
      throw new Error(`test is not registered in this build: ${name}`);
  }
  return names.length;
}
if (process.argv[1]?.endsWith("check-capabilities.mjs")) {
  console.log(
    `capability matrix check passed (${verify(readFileSync(process.argv[2], "utf8"), readFileSync(process.argv[3], "utf8"))} tests)`,
  );
}
