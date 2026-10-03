import { render } from "svelte/server";
import { describe, expect, it } from "vitest";
import ConfirmPanel from "./components/ConfirmPanel.svelte";
import { currentMessages } from "./messages";
import { RunStore } from "./run-store.svelte";

const digest = "a".repeat(64);
const invocation = "b".repeat(64);
const messages = currentMessages("en");

function panel(scope: unknown, pending = false, id?: string) {
  const store = new RunStore();
  store.confirm = {
    id: id ?? `publish:http:${digest}${scope === "invocation" ? `:${invocation}` : ""}`,
    scope,
    operation_digest: digest,
    title: "Publish artifact",
    kind: "http",
    target: "https://example.test/publish",
    details: null,
  } as typeof store.confirm;
  store.pendingAction = pending ? "approving" : null;
  return render(ConfirmPanel, { props: { store, messages } }).body;
}

function approveButton(body: string): string {
  const buttons = body.match(/<button\b[^>]*>[\s\S]*?<\/button>/g) ?? [];
  const approve = buttons.find(button => button.includes('class="primary-btn"'));
  expect(approve, "the actual panel must render an approval action").toBeDefined();
  return approve!;
}

describe("confirmation panel", () => {
  it.each(["content", "invocation"])("renders %s binding and enables approval", scope => {
    const body = panel(scope);
    expect(approveButton(body)).not.toMatch(/\sdisabled(?:[\s=>])/);
    expect(body).toContain(scope === "content" ? messages.confirmScopeContent : messages.confirmScopeInvocation);
    expect(body).toContain(`digest ${digest}`);
    if (scope === "invocation") expect(body).toContain(`invocation ${invocation}`);
    else expect(body).not.toContain(`invocation ${invocation}`);
  });

  it.each(["unknown", undefined, null, ""])("blocks approval for scope %s and keeps denial available", scope => {
    const body = panel(scope);
    expect(approveButton(body)).toMatch(/\sdisabled(?:[\s=>])/);
    expect(body).toContain('role="alert"');
    expect(body).toContain(messages.confirmScopeUnknown);
    const deny = body.match(/<button[^>]*class="secondary-btn"[^>]*>/)?.[0];
    expect(deny).toBeDefined();
    expect(deny).not.toMatch(/\sdisabled(?:[\s=>])/);
  });

  it("blocks duplicate decisions while an action is pending", () => {
    const body = panel("content", true);
    expect(approveButton(body)).toMatch(/\sdisabled(?:[\s=>])/);
    expect(body.match(/<button[^>]*class="secondary-btn"[^>]*>/)?.[0]).toMatch(/\sdisabled(?:[\s=>])/);
  });

  it.each([
    ["content", "publish:http:nothex", "invalid content id"],
    ["invocation", `publish:http:${digest}`, "invalid invocation id"],
    ["invocation", `publish:http:${digest}:short`, "invalid invocation id"],
  ])("shows malformed %s binding instead of a trusted digest", (scope, id, error) => {
    expect(panel(scope, false, id)).toContain(error);
  });
});
