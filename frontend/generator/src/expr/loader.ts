export type EvalContext = {
  inputs: Record<string, unknown>;
  steps?: Record<string, { output: unknown }>;
  item?: unknown;
};

type WasmModule = {
  default?: (input?: unknown) => Promise<unknown>;
  eval_bool_json?: (expr: string, contextJson: string) => boolean;
};

let wasmPromise: Promise<WasmModule | null> | null = null;

export async function evalWhen(expr: string | undefined, context: EvalContext): Promise<boolean> {
  if (!expr) {
    return true;
  }
  const wasm = await loadWasm();
  if (!wasm?.eval_bool_json) {
    return false;
  }
  try {
    return wasm.eval_bool_json(expr, JSON.stringify(context));
  } catch {
    return false;
  }
}

async function loadWasm(): Promise<WasmModule | null> {
  if (!wasmPromise) {
    wasmPromise = import("./pkg/qcg_expr_wasm.js")
      .then(async (loaded) => {
        const module = loaded as WasmModule;
        if (module.default) {
          await module.default();
        }
        return module;
      })
      .catch(() => null);
  }
  return wasmPromise;
}
