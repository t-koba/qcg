import type { Messages } from "./messages";
import { record } from "./progress";

export function toolCallLabel(data: unknown, messages: Messages): string {
  const value = record(data);
  return [
    messages.eventToolCall,
    typeof value.agent === "string" ? value.agent : "",
    typeof value.tool === "string" ? value.tool : "",
    toolCallStatus(value.status, messages),
    toolCallPhase(value.phase, messages),
    typeof record(value.error).message === "string" ? record(value.error).message : "",
  ].filter(Boolean).join(" · ");
}

function toolCallStatus(status: unknown, messages: Messages): string {
  if (typeof status !== "string") return messages.eventToolFailed;
  switch (status) {
    case "succeeded": return messages.eventToolSucceeded;
    case "failed": return messages.eventToolFailed;
    case "needs_user": return messages.eventToolNeedsUser;
    case "needs_confirmation":
    case "needs_confirm":
      return messages.eventToolNeedsConfirmation;
    default: return status;
  }
}

function toolCallPhase(phase: unknown, messages: Messages): string {
  if (typeof phase !== "string" || phase.length === 0) return "";
  const labels: Record<string, string> = {
    input_validation: messages.eventToolPhaseInputValidation,
    input_guardrail: messages.eventToolPhaseInputGuardrail,
    execution: messages.eventToolPhaseExecution,
    output_guardrail: messages.eventToolPhaseOutputGuardrail,
    completed: messages.eventToolPhaseCompleted,
  };
  return `${messages.eventToolPhase}: ${labels[phase] || phase}`;
}
