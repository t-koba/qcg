import { describe, expect, it } from "vitest";
import { evalWhen } from "./loader";

const context = { inputs: { enabled: true, count: 2 } };

describe("evalWhen", () => {
  it("treats a missing expression as always active", async () => {
    expect(await evalWhen(undefined, context)).toBe(true);
    expect(await evalWhen("", context)).toBe(true);
  });

  it("never reports a stage as skipped when the expression cannot run", async () => {
    // The expression module is a generated build artifact that initializes
    // through `fetch`, which node cannot serve for a `file:` URL. Either the
    // module loads and the expression decides, or the failure is reported;
    // resolving `false` without evaluating is the one outcome forbidden here,
    // because it would drop input stages and submit an incomplete run.
    const outcome = await evalWhen("inputs.enabled", context).then(
      (value) => ({ value }),
      (error: unknown) => ({ error: error instanceof Error ? error.message : String(error) }),
    );
    if ("value" in outcome) {
      expect(outcome.value).toBe(true);
    } else {
      expect(outcome.error).toMatch(/expression module failed to load|when expression/);
    }
  });
});
