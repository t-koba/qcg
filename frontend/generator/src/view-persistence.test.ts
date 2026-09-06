import { describe, expect, it } from "vitest";
import { decodeView, encodeView, emptyPersistedView } from "./view-persistence";

describe("decodeView", () => {
  it("returns null for missing or malformed payloads", () => {
    expect(decodeView(null)).toBeNull();
    expect(decodeView("")).toBeNull();
    expect(decodeView("not json")).toBeNull();
    expect(decodeView("[]")).toBeNull();
    expect(decodeView("{}")).toBeNull();
  });

  it("rejects unknown versions", () => {
    expect(decodeView(JSON.stringify({ version: 999 }))).toBeNull();
  });

  it("round-trips a valid view", () => {
    const view = {
      ...emptyPersistedView(),
      selected: "generator",
      tabOrder: ["run-a", "run-b"],
      currentRun: "run-b",
    };
    expect(decodeView(encodeView(view))).toEqual(view);
  });

  it("ignores legacy answer and form maps", () => {
    const decoded = decodeView(JSON.stringify({
      version: 1,
      selected: "generator",
      tabOrder: ["run-a"],
      currentRun: "run-a",
      answersByRun: { "run-a": { "question:name": "secret" } },
      formValuesByGenerator: { generator: { password: "secret" } },
    }));
    expect(decoded).toEqual({
      version: 1,
      selected: "generator",
      tabOrder: ["run-a"],
      currentRun: "run-a",
    });
  });

  it("drops placeholder tabs and caps the tab order", () => {
    const decoded = decodeView(JSON.stringify({
      version: 1,
      tabOrder: ["pending-abc", "run-a", "run-a", "", 42],
    }));
    expect(decoded?.tabOrder).toEqual(["run-a"]);
  });

  it("rejects a current run that is not an open tab", () => {
    expect(decodeView(JSON.stringify({
      version: 1,
      tabOrder: ["run-a"],
      currentRun: "run-gone",
    }))).toBeNull();
  });
});

describe("encodeView", () => {
  it("keeps tab order and selection within quota", () => {
    const view = {
      ...emptyPersistedView(),
      selected: "generator",
      tabOrder: ["run-a"],
      currentRun: "run-a",
    };
    const decoded = decodeView(encodeView(view));
    expect(decoded?.tabOrder).toEqual(["run-a"]);
    expect(decoded?.currentRun).toBe("run-a");
  });
});
