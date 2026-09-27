import { describe, expect, it } from "vitest";
import type { RunEvent } from "./api/client";
import { applyNodeProgress, emptyNodeProgress, materialize } from "./progress";

function event(
  seq: number,
  kind: RunEvent["kind"],
  data: unknown,
  path: string | null = null,
): RunEvent {
  return {
    seq,
    kind,
    data,
    path,
    run_id: "run-1",
    trace_id: "00000000000000000000000000000001",
    span_id: "0000000000000001",
    parent_span_id: null,
    ts: "2026-01-01T00:00:00Z",
  };
}

describe("node progress aggregate", () => {
  it("folds event envelopes by node path", () => {
    const aggregate = emptyNodeProgress();
    for (const entry of [
      event(1, "graph_resolved", { nodes: ["build", "test"] }),
      event(2, "step_started", { type: "command" }, "build"),
      event(3, "step_finished", { status: "success" }, "build"),
      event(4, "step_skipped", { reason: "dependency failed" }, "test"),
      event(5, "run_waiting", {}, "test"),
    ]) {
      applyNodeProgress(aggregate, entry);
    }
    expect(materialize(aggregate)).toEqual([
      { id: "build", status: "succeeded", detail: "success" },
      { id: "test", status: "waiting", detail: "run_waiting" },
    ]);
  });
});
