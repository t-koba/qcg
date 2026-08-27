import { describe, expect, it } from "vitest";
import { exprCorpus, exprCorpusContext } from "./corpus";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { eval_bool_json, initSync } from "./pkg/qcg_expr_wasm.js";

const wasmPath = resolve(dirname(fileURLToPath(import.meta.url)), "pkg/qcg_expr_wasm_bg.wasm");
initSync({ module: readFileSync(wasmPath) });

describe("qcg expression corpus", () => {
  for (const [expr, expected] of exprCorpus) {
    it(expr, () => {
      expect(eval_bool_json(expr, JSON.stringify(exprCorpusContext))).toBe(expected);
    });
  }
});
