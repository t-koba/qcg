import { describe, expect, it } from "vitest";
import { mcpCleanupSettledLabel, toolCallLabel } from "./event-log";
import { currentMessages } from "./messages";

const messages = currentMessages("en-US");

describe("tool call event labels", () => {
  it("renders a user-input wait as waiting instead of failed", () => {
    const label = toolCallLabel({
      agent: "researcher",
      tool: "search",
      status: "needs_user",
      phase: "input_validation",
    }, messages);

    expect(label).toContain("waiting for input");
    expect(label).toContain("phase: input validation");
    expect(label).not.toContain("failed");
  });

  it("renders a confirmation wait as waiting instead of failed", () => {
    const label = toolCallLabel({
      tool: "write_file",
      status: "needs_confirmation",
      phase: "execution",
    }, messages);

    expect(label).toContain("waiting for approval");
    expect(label).toContain("phase: execution");
    expect(label).not.toContain("failed");
  });
});

describe("MCP cleanup event labels", () => {
  it("renders the failure and settlement provenance without raw error details", () => {
    const secretLikeDetail = "token=do-not-display <script>bad()</script>";
    const label = mcpCleanupSettledLabel({
      cleanup_error: { code: "cleanup_failed", message: secretLikeDetail },
      cleanup_settlement: { policy: "preserve_execution", source: "node_parameter" },
    }, messages);

    expect(label).toContain("MCP session cleanup failed");
    expect(label).toContain("cleanup policy: preserve execution");
    expect(label).toContain("configured by: node parameter");
    expect(label).not.toContain(secretLikeDetail);
  });

  it("renders cleanup failure on the tool call without exposing its error message", () => {
    const label = toolCallLabel({
      tool: "mcp:example/action",
      status: "succeeded",
      phase: "completed",
      cleanup_error: { code: "cleanup_failed", message: "private transport detail" },
    }, messages);

    expect(label).toContain("succeeded");
    expect(label).toContain("MCP session cleanup failed");
    expect(label).not.toContain("private transport detail");
  });

  it("localizes cleanup labels and ignores unknown provenance", () => {
    const japanese = mcpCleanupSettledLabel({
      cleanup_settlement: { policy: "unexpected", source: "unexpected" },
    }, currentMessages("ja-JP"));

    expect(japanese).toContain("MCP セッションのクリーンアップに失敗");
    expect(japanese).not.toContain("unexpected");
  });
});
