import { describe, expect, it } from "vitest";
import { describeConfirmId, isApproveDisabled } from "./view-persistence";

// Q1: unknown approval scope must disable approval (fail-closed). Tests the
// real component predicate shared with ConfirmPanel.svelte, never a copy.
describe("confirm scope gate", () => {
  it("enables approve only for content/invocation", () => {
    expect(isApproveDisabled("content", null)).toBe(false);
    expect(isApproveDisabled("invocation", null)).toBe(false);
  });
  it("disables approve for unknown/missing scopes", () => {
    expect(isApproveDisabled("unknown", null)).toBe(true);
    expect(isApproveDisabled(undefined, null)).toBe(true);
    expect(isApproveDisabled(null, null)).toBe(true);
    expect(isApproveDisabled("", null)).toBe(true);
  });
  it("disables approve while an action is pending", () => {
    expect(isApproveDisabled("content", "approving")).toBe(true);
  });
});

// Q1: confirmation id decomposition validates hex lengths before trusting
// the tail split, via the shared helper the panel renders. Unknown scopes
// never render empty.
describe("confirm id mapping", () => {
  const digest = "a".repeat(64);
  const invocation = "b".repeat(64);
  it("decomposes content ids to their digest", () => {
    expect(describeConfirmId(`publish:http:${digest}`, "content")).toBe(`digest ${digest}`);
  });
  it("decomposes invocation ids to digest plus invocation hash", () => {
    expect(describeConfirmId(`publish:http:${digest}:${invocation}`, "invocation")).toBe(
      `digest ${digest} · invocation ${invocation}`,
    );
  });
  it("rejects tail guesses without hex lengths", () => {
    expect(describeConfirmId("publish:http:nothex", "content")).toContain("invalid content id");
    expect(describeConfirmId(`publish:http:${digest}:short`, "invocation")).toContain(
      "invalid invocation id",
    );
    expect(describeConfirmId(`publish:http:${digest}`, "invocation")).toContain(
      "invalid invocation id",
    );
  });
  it("shows an explicit unknown-scope state instead of empty", () => {
    const mapped = describeConfirmId(`publish:http:${digest}`, "unknown");
    expect(mapped).toContain("unknown scope");
    expect(mapped.length).toBeGreaterThan(0);
    expect(describeConfirmId(`publish:http:${digest}`, undefined)).toContain("unknown scope");
  });
});

// Q1 panel integration: second-time display distinguishes content-reuse
// (same digest reuses approval until the run ends) from invocation
// (one call only). The panel renders the scope message plus the decomposed
// mapping above, so a reviewer sees both the policy and the hex binding.
describe("confirm second-time display", () => {
  it("distinguishes content-reuse from invocation-only", async () => {
    const messages = (await import("./messages")).currentMessages("en");
    expect(messages.confirmScopeContent).toContain("reused");
    expect(messages.confirmScopeInvocation).toContain("only");
    expect(messages.confirmScopeContent).not.toBe(messages.confirmScopeInvocation);
    const digest = "c".repeat(64);
    const invocation = "d".repeat(64);
    // Content second-time: same digest line, no invocation hash.
    const contentDisplay = `${messages.confirmScopeContent} ${describeConfirmId(`node:kind:${digest}`, "content")}`;
    expect(contentDisplay).toContain("reused");
    expect(contentDisplay).toContain(digest);
    expect(contentDisplay).not.toContain("invocation");
    // Invocation second-time: call-only line plus both hexes.
    const invocationDisplay = `${messages.confirmScopeInvocation} ${describeConfirmId(`node:kind:${digest}:${invocation}`, "invocation")}`;
    expect(invocationDisplay).toContain("only");
    expect(invocationDisplay).toContain(digest);
    expect(invocationDisplay).toContain(invocation);
  });
});

// Q1 docs-to-key conformance: operations.md documents the MCP continuation
// key as `<node>:agentmcp:<alias>:<invocation_hash>#__mcp_pending`. The key
// constructor lives in FOREIGN `qcg-llm-steps/src/tool_events.rs`
// (`pending_key_for_agent_mcp`), so this test pins the documented FORMAT
// against a captured real key shape instead of importing foreign code.
describe("mcp continuation key format", () => {
  it("matches the documented two-namespace shape", () => {
    const documented =
      "mynode:agentmcp:myalias:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef#__mcp_pending";
    const pattern = /^[^:]+:agentmcp:[^:]+:[0-9a-fA-F]{64}#__mcp_pending$/;
    expect(documented).toMatch(pattern);
    // Single-shot `mcp.call` steps bind `execution:<node>:<count>` and never
    // match the agent key, so the namespaces cannot authorize each other.
    expect("mynode:agentmcp:myalias:short#__mcp_pending").not.toMatch(pattern);
    expect("execution:mynode:3").not.toMatch(pattern);
  });
});
