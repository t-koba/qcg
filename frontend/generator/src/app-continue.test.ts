import { render } from "svelte/server";
import { describe, expect, it, vi } from "vitest";
import type { GeneratorDetail, RunSnapshot } from "./api/client";
import App from "./App.svelte";
import { currentMessages } from "./messages";

const scenario = vi.hoisted(() => ({ state: "failed", pendingAction: null as string | null }));

vi.mock("./run-store.svelte", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./run-store.svelte")>();
  class ScenarioStore extends actual.RunStore {
    constructor() {
      super();
      this.detail = {
        generator: { id: "demo", name: "Demo" },
        package_sha256: "a".repeat(64),
      } as unknown as GeneratorDetail;
      const snapshot: RunSnapshot = {
        run_id: "test",
        generator_id: "demo",
        seq: 1,
        state: scenario.state as RunSnapshot["state"],
        invalidation: null,
        queue_position: null,
        queue_position_quality: "unavailable",
        priority: 0,
        labels: {},
        contract_sha256: null,
        metrics: null,
        parent_run_id: null,
        queued_at: null,
        step_summaries: {},
      };
      this.currentRun = "test";
      this.applySnapshot(snapshot);
      this.pendingAction = scenario.pendingAction as typeof this.pendingAction;
    }
  }
  return { ...actual, RunStore: ScenarioStore };
});

const messages = currentMessages("en");

function continueButton(body: string): string {
  const buttons = body.match(/<button\b[^>]*>[\s\S]*?<\/button>/g) ?? [];
  const button = buttons.find((candidate) => candidate.includes(messages.continueRun));
  expect(button, "the terminal actions must render a continue button").toBeDefined();
  return button!;
}

function forkButton(body: string): string {
  const buttons = body.match(/<button\b[^>]*>[\s\S]*?<\/button>/g) ?? [];
  const button = buttons.find((candidate) => candidate.includes(messages.forkRun));
  expect(button, "the terminal actions must render a fork button").toBeDefined();
  return button!;
}

describe("terminal continuation action", () => {
  it("offers Continue run for a failed run", () => {
    scenario.state = "failed";
    scenario.pendingAction = null;
    const body = render(App).body;
    expect(continueButton(body)).not.toMatch(/\sdisabled(?:[\s=>])/);
    expect(continueButton(body)).toContain(`>${messages.continueRun}</button>`);
  });

  it.each(["succeeded", "canceled", "interrupted"] as const)(
    "disables Continue run with a reason for %s while keeping Fork",
    (state) => {
      scenario.state = state;
      scenario.pendingAction = null;
      const body = render(App).body;
      expect(continueButton(body)).toMatch(/\sdisabled(?:[\s=>])/);
      expect(continueButton(body)).toContain(messages.continueRunUnavailable);
      expect(forkButton(body)).not.toMatch(/\sdisabled(?:[\s=>])/);
    },
  );

  it("disables Continue run while another action is pending", () => {
    scenario.state = "failed";
    scenario.pendingAction = "starting";
    const body = render(App).body;
    expect(continueButton(body)).toMatch(/\sdisabled(?:[\s=>])/);
    expect(forkButton(body)).toMatch(/\sdisabled(?:[\s=>])/);
  });
});
