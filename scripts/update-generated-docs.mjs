import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

function generated(command) {
  return execFileSync("cargo", ["run", "-q", "-p", "qcg", "--", "docs", command], { encoding: "utf8" });
}

function replaceBlock(path, marker, content) {
  const source = readFileSync(path, "utf8");
  const start = `<!-- qcg-${marker}:start -->`;
  const end = `<!-- qcg-${marker}:end -->`;
  const startIndex = source.indexOf(start);
  const endIndex = source.indexOf(end);
  if (startIndex < 0 || endIndex < startIndex) {
    throw new Error(`generated marker is missing in ${path}: ${marker}`);
  }
  const updated = `${source.slice(0, startIndex + start.length)}\n${content.trim()}\n${source.slice(endIndex)}`;
  writeFileSync(path, updated);
}

replaceBlock("docs/contract-reference.md", "step-schemas", generated("step-schemas"));
replaceBlock("docs/run-event-reference.md", "run-events", generated("run-events"));
writeFileSync("docs/openapi.json", generated("openapi").trimEnd() + "\n");
