import { describe, expect, it } from "vitest";
import { toolCallLabel } from "./event-log";
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
