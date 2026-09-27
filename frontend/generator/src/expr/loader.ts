export type EvalContext = {
  inputs: Record<string, unknown>;
  steps?: Record<string, { output: unknown }>;
  item?: unknown;
};

type WasmModule = {
  default?: (input?: unknown) => Promise<unknown>;
  eval_bool_json?: (expr: string, contextJson: string) => boolean;
};

type LoadedWasmModule = WasmModule & { eval_bool_json: (expr: string, contextJson: string) => boolean };

let wasmPromise: Promise<LoadedWasmModule> | null = null;

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Evaluates a contract `when` expression.
 *
 * Every failure mode is an error, never `false`: a `when` that cannot be
 * evaluated would otherwise hide input stages and submit a run built from the
 * wrong fields. Only a real `false` result skips a stage.
 */
export async function evalWhen(expr: string | undefined, context: EvalContext): Promise<boolean> {
  if (!expr) {
    return true;
  }
  const wasm = await loadWasm();
  try {
    return wasm.eval_bool_json(expr, JSON.stringify(context));
  } catch (error) {
    throw new Error(`when expression \`${expr}\` failed to evaluate: ${describe(error)}`);
  }
}

async function loadWasm(): Promise<LoadedWasmModule> {
  if (!wasmPromise) {
    // A failed load is never cached. The module is a build artifact
    // (`npm run generate:wasm`), so a cached rejection would keep every later
    // `when` evaluation broken for the rest of the session.
    wasmPromise = import("./pkg/qcg_expr_wasm.js")
      .then(async (loaded) => {
        const module = loaded as WasmModule;
        if (module.default) {
          await module.default();
        }
        if (!module.eval_bool_json) {
          throw new Error("the module loaded without an `eval_bool_json` export");
        }
        return module as LoadedWasmModule;
      })
      .catch((error: unknown) => {
        wasmPromise = null;
        throw new Error(`the qcg expression module failed to load: ${describe(error)}`);
      });
  }
  return wasmPromise;
}
