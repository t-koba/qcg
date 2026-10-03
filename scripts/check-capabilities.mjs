import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

// Collect test names defined inside `#[cfg(...unix...)]` modules.
// Those tests never register in Windows builds (`cargo test --list`
// omits them), so the win32 gate must not demand them while every other
// platform still does. Scanning is syntactic: a `#[cfg]` attribute naming
// `unix` immediately above `mod NAME {`, brace-matched to the module end,
// with `#[test]` / `#[tokio::test]` functions collected inside. Strings,
// chars, raw strings and comments are stripped before brace counting so
// format strings and TOML fixtures cannot skew the module span.
export function unixGatedTests(rootDir) {
  const found = new Set();
  const walk = (dir) => {
    for (const entry of readdirSync(dir)) {
      const path = join(dir, entry);
      if (statSync(path).isDirectory()) {
        walk(path);
      } else if (entry.endsWith(".rs")) {
        for (const name of unixGatedInFile(readFileSync(path, "utf8")))
          found.add(name);
      }
    }
  };
  walk(rootDir);
  return found;
}

function stripRustNoise(source) {
  let out = "";
  let i = 0;
  const n = source.length;
  while (i < n) {
    const c = source[i];
    if (c === "/" && source[i + 1] === "/") {
      while (i < n && source[i] !== "\n") i++;
    } else if (c === "/" && source[i + 1] === "*") {
      i += 2;
      while (i < n && !(source[i] === "*" && source[i + 1] === "/")) i++;
      i += 2;
    } else if (c === "r") {
      const hashes = /^r(#+)"/.exec(source.slice(i, i + 8));
      if (hashes) {
        const close = `"${hashes[1]}`;
        const end = source.indexOf(close, i + hashes[0].length);
        i = end === -1 ? n : end + close.length;
      } else if (source[i + 1] === '"') {
        i += 2;
        while (i < n) {
          if (source[i] === "\\") i += 2;
          else if (source[i] === '"') {
            i++;
            break;
          } else i++;
        }
      } else {
        out += c;
        i++;
      }
    } else if (c === '"') {
      i++;
      while (i < n) {
        if (source[i] === "\\") i += 2;
        else if (source[i] === '"') {
          i++;
          break;
        } else i++;
      }
    } else if (
      c === "'" &&
      i + 2 < n &&
      (source[i + 2] === "'" || (source[i + 1] === "\\" && i + 3 < n))
    ) {
      if (source[i + 1] === "\\") {
        const end = source.indexOf("'", i + 2);
        i = end === -1 ? n : end + 1;
      } else {
        i += 3;
      }
    } else {
      out += c;
      i++;
    }
  }
  return out;
}

export function unixGatedInFile(source) {
  const clean = stripRustNoise(source);
  const lines = clean.split("\n");
  const found = new Set();
  for (let i = 0; i < lines.length; i++) {
    const attr = lines[i].trim();
    if (!/^#\[cfg\(.*unix.*\)\]$/.test(attr)) continue;
    // Skip blank lines between the attribute and the module it gates.
    let j = i + 1;
    while (j < lines.length && lines[j].trim() === "") j++;
    const modMatch = /^(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z0-9_]+)\s*\{/.exec(
      lines[j]?.trim() ?? "",
    );
    if (!modMatch) {
      // A directly gated test function (no enclosing gated module):
      // `#[cfg(unix)]` above `#[test]` + `fn`, or directly above the fn.
      const rest = lines.slice(j, j + 3).join("\n");
      const direct =
        /#\[(?:tokio::)?test[^\]]*\]\s*(?:async\s+)?fn\s+([A-Za-z0-9_]+)\s*\(/.exec(
          rest,
        );
      if (direct) found.add(direct[1]);
      continue;
    }
    // Brace-match from the opening line to the module end.
    let depth = 0;
    let k = j;
    for (; k < lines.length; k++) {
      for (const ch of lines[k]) {
        if (ch === "{") depth++;
        else if (ch === "}") depth--;
      }
      if (depth === 0) break;
    }
    const span = lines.slice(j, k + 1).join("\n");
    for (const m of span.matchAll(
      /#\[(?:tokio::)?test[^\]]*\]\s*(?:\/\/[^\n]*\n\s*)*async\s+fn\s+([A-Za-z0-9_]+)\s*\(|#\[(?:tokio::)?test[^\]]*\]\s*(?:\/\/[^\n]*\n\s*)*fn\s+([A-Za-z0-9_]+)\s*\(/g,
    ))
      found.add(m[1] ?? m[2]);
    i = k;
  }
  return found;
}

export function verify(
  matrix,
  listing,
  platform = process.platform,
  unixGated = new Set(),
) {
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
    // Unix file-semantics guarantees (F06/G02/G06 rows) live in Unix-only
    // test modules and never register on Windows; every other platform
    // still demands them, and Windows still demands everything else.
    if (platform === "win32" && unixGated.has(name)) continue;
    if (!registered.has(name))
      throw new Error(`test is not registered in this build: ${name}`);
  }
  return names.length;
}
if (process.argv[1]?.endsWith("check-capabilities.mjs")) {
  const cratesDir =
    process.argv[4] ??
    join(dirname(process.argv[2] ?? "docs/capability-matrix.md"), "..", "crates");
  console.log(
    `capability matrix check passed (${verify(readFileSync(process.argv[2], "utf8"), readFileSync(process.argv[3], "utf8"), process.platform, unixGatedTests(cratesDir))} tests)`,
  );
}
