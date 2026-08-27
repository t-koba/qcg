import { describe, expect, it } from "vitest";
import type { RunEvent } from "./api/client";
import { collectNodeProgress } from "./progress";

function event(seq: number, kind: string, data: unknown, path: string | null = null): RunEvent {
  return { seq, kind, data, path, run_id: "run-1", ts: "2026-01-01T00:00:00Z" };
}

describe("collectNodeProgress", () => {
  it("folds event envelopes by node path", () => {
    const progress = collectNodeProgress([
      event(1, "graph_resolved", { nodes: ["build", "test"] }),
      event(2, "step_started", { type: "command" }, "build"),
      event(3, "step_finished", { status: "success" }, "build"),
      event(4, "step_skipped", { reason: "dependency failed" }, "test"),
    ]);
    expect(progress).toEqual([
      { id: "build", status: "succeeded", detail: "success" },
      { id: "test", status: "skipped", detail: "dependency failed" },
    ]);
  });
});
