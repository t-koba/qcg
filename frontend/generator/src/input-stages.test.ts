import { beforeEach, expect, it, vi } from "vitest";
import { evalWhen } from "./expr/loader";
import type { GeneratorDetail, InputField } from "./api/client";
import { RunStore } from "./run-store.svelte";

vi.mock("./expr/loader", () => ({
  evalWhen: vi.fn(async (expr: string | undefined, context: { inputs: Record<string, unknown> }) => {
    if (!expr) return true;
    const key = /^inputs\.([A-Za-z0-9_]+)$/.exec(expr)?.[1];
    return key !== undefined && context.inputs[key] === true;
  }),
}));

const field = (id: string, defaultValue?: unknown): InputField => ({
  id,
  label: null,
  label_i18n: {},
  type: "string",
  required: false,
  default: defaultValue === undefined ? null : defaultValue,
  pattern: null,
  options: [],
  option_labels_i18n: {},
  min_items: null,
  item_type: null,
} as InputField);

function storeWithStages(stages: Array<{ id: string; when?: string; fields: InputField[] }>): RunStore {
  const store = new RunStore();
  store.detail = { inputs: { stages } } as unknown as GeneratorDetail;
  return store;
}

beforeEach(() => {
  vi.mocked(evalWhen).mockReset();
  vi.mocked(evalWhen).mockImplementation(async (expr, context) => {
    if (!expr) return true;
    const key = /^inputs\.([A-Za-z0-9_]+)$/.exec(expr)?.[1];
    return key !== undefined && context.inputs[key] === true;
  });
});

it("uses an active stage's default for a later stage on first render", async () => {
  const evaluatedInputs: Record<string, unknown>[] = [];
  vi.mocked(evalWhen).mockImplementation(async (expr, context) => {
    evaluatedInputs.push({ ...context.inputs });
    if (!expr) return true;
    const key = /^inputs\.([A-Za-z0-9_]+)$/.exec(expr)?.[1];
    return key !== undefined && context.inputs[key] === true;
  });
  const store = storeWithStages([
    { id: "setup", fields: [field("enabled", true)] },
    { id: "details", when: "inputs.enabled", fields: [field("detail", "initial")] },
  ]);

  await store.refreshActiveFields();

  expect(store.activeFields.map(({ id }) => id)).toEqual(["enabled", "detail"]);
  expect(store.values).toEqual({ enabled: true, detail: "initial" });
  expect(evaluatedInputs).toEqual([{}, { enabled: true }]);
});

it("does not apply an inactive stage's default to later conditions", async () => {
  const store = storeWithStages([
    { id: "inactive", when: "inputs.show_hidden", fields: [field("hidden", true)] },
    { id: "dependent", when: "inputs.hidden", fields: [field("dependent")] },
  ]);

  await store.refreshActiveFields();

  expect(store.activeFields).toEqual([]);
  expect(store.values).toEqual({});
});

it("re-evaluates later stages after a user overrides an earlier default", async () => {
  const store = storeWithStages([
    { id: "setup", fields: [field("enabled", true)] },
    { id: "details", when: "inputs.enabled", fields: [field("detail")] },
  ]);
  await store.refreshActiveFields();
  expect(store.activeFields.map(({ id }) => id)).toEqual(["enabled", "detail"]);
  expect(store.values).toEqual({ enabled: true });

  store.setValue("enabled", false);
  await vi.waitFor(() => expect(store.activeFields.map(({ id }) => id)).toEqual(["enabled"]));
  expect(store.values).toEqual({ enabled: false });
});

it("discards stale field refreshes without publishing their defaults or fields", async () => {
  let releaseStale!: (active: boolean) => void;
  let laterStageCalls = 0;
  vi.mocked(evalWhen).mockImplementation(async (expr, context) => {
    if (!expr) return true;
    laterStageCalls += 1;
    if (laterStageCalls === 1) {
      return await new Promise<boolean>((resolve) => { releaseStale = resolve; });
    }
    return context.inputs.enabled === true;
  });
  const store = storeWithStages([
    { id: "setup", fields: [field("enabled", true)] },
    { id: "details", when: "inputs.enabled", fields: [field("detail", "stale")] },
  ]);

  const staleRefresh = store.refreshActiveFields();
  await vi.waitFor(() => expect(releaseStale).toBeTypeOf("function"));
  store.setValue("enabled", false);
  await vi.waitFor(() => expect(laterStageCalls).toBe(2));
  await vi.waitFor(() => expect(store.activeFields.map(({ id }) => id)).toEqual(["enabled"]));
  expect(store.values).toEqual({ enabled: false });

  releaseStale(true);
  await staleRefresh;
  expect(store.activeFields.map(({ id }) => id)).toEqual(["enabled"]);
  expect(store.values).toEqual({ enabled: false });
});
