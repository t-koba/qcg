import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { EvalContext } from "./loader";

type TomlObject = Record<string, unknown>;

const fixture = readCorpusFixture(fileURLToPath(new URL("../../../../fixtures/expr-corpus.toml", import.meta.url)));

export const exprCorpusContext = fixture.context as EvalContext;
export const exprCorpus = fixture.cases.map((entry) => [entry.expr, entry.expected] as const);

if (exprCorpus.length < 50) {
  throw new Error(`expression corpus must have 50+ cases, got ${exprCorpus.length}`);
}

function readCorpusFixture(path: string): { context: TomlObject; cases: Array<{ expr: string; expected: boolean }> } {
  const root: TomlObject = {};
  let current: TomlObject = root;
  for (const rawLine of readFileSync(path, "utf8").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }
    if (line === "[[cases]]") {
      const cases = arrayAt(root, "cases");
      const next: TomlObject = {};
      cases.push(next);
      current = next;
      continue;
    }
    if (line.startsWith("[") && line.endsWith("]")) {
      current = objectPath(root, line.slice(1, -1).split("."));
      continue;
    }
    const separator = line.indexOf("=");
    if (separator < 0) {
      throw new Error(`unsupported TOML line: ${line}`);
    }
    const key = line.slice(0, separator).trim();
    current[key] = parseScalar(line.slice(separator + 1).trim());
  }
  return {
    context: objectAt(root, "context"),
    cases: arrayAt(root, "cases").map((entry) => {
      const object = entry as TomlObject;
      if (typeof object.expr !== "string" || typeof object.expected !== "boolean") {
        throw new Error("expression corpus case must contain expr and expected");
      }
      return { expr: object.expr, expected: object.expected };
    }),
  };
}

function objectPath(root: TomlObject, path: string[]): TomlObject {
  let cursor = root;
  for (const segment of path) {
    const value = cursor[segment];
    if (value === undefined) {
      cursor[segment] = {};
    } else if (!isObject(value)) {
      throw new Error(`TOML path segment is not an object: ${segment}`);
    }
    cursor = cursor[segment] as TomlObject;
  }
  return cursor;
}

function objectAt(root: TomlObject, key: string): TomlObject {
  const value = root[key];
  if (!isObject(value)) {
    throw new Error(`TOML key is not an object: ${key}`);
  }
  return value;
}

function arrayAt(root: TomlObject, key: string): unknown[] {
  const value = root[key];
  if (value === undefined) {
    root[key] = [];
    return root[key] as unknown[];
  }
  if (!Array.isArray(value)) {
    throw new Error(`TOML key is not an array: ${key}`);
  }
  return value;
}

function parseScalar(raw: string): unknown {
  if (raw === "true") {
    return true;
  }
  if (raw === "false") {
    return false;
  }
  if (/^-?\d+(\.\d+)?$/.test(raw)) {
    return Number(raw);
  }
  if (raw.startsWith('"') && raw.endsWith('"')) {
    const value = JSON.parse(raw) as string;
    return value === "null" ? null : value;
  }
  throw new Error(`unsupported TOML scalar: ${raw}`);
}

function isObject(value: unknown): value is TomlObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
