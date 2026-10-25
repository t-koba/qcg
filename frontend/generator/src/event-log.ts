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
    record(value.cleanup_error).code === "cleanup_failed"
      ? messages.eventMcpCleanupFailed
      : "",
  ].filter(Boolean).join(" · ");
}

export function mcpCleanupSettledLabel(data: unknown, messages: Messages): string {
  const settlement = record(record(data).cleanup_settlement);
  const policy = cleanupPolicy(settlement.policy, messages);
  const source = cleanupSource(settlement.source, messages);
  return [
    messages.eventMcpCleanupFailed,
    policy ? `${messages.eventMcpCleanupPolicy}: ${policy}` : "",
    source ? `${messages.eventMcpCleanupSource}: ${source}` : "",
  ].filter(Boolean).join(" · ");
}

function cleanupPolicy(policy: unknown, messages: Messages): string {
  switch (policy) {
    case "preserve_execution": return messages.eventMcpCleanupPreserveExecution;
    case "fail_fast": return messages.eventMcpCleanupFailFast;
    default: return "";
  }
}

function cleanupSource(source: unknown, messages: Messages): string {
  switch (source) {
    case "default": return messages.eventMcpCleanupDefaultSource;
    case "node_parameter": return messages.eventMcpCleanupNodeParameterSource;
    default: return "";
  }
}

function toolCallStatus(status: unknown, messages: Messages): string {
  if (typeof status !== "string") return messages.eventToolFailed;
  switch (status) {
    case "succeeded": return messages.eventToolSucceeded;
    case "failed": return messages.eventToolFailed;
    case "degraded": return messages.eventToolDegraded;
    case "needs_user": return messages.eventToolNeedsUser;
    case "needs_confirmation":
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
