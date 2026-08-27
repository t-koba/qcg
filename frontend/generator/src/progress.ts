import type { RunEvent } from "./api/client";

export type NodeProgress = {
  id: string;
  status: "pending" | "running" | "succeeded" | "skipped" | "waiting" | "failed" | "finished";
  detail: string;
};

export function collectNodeProgress(events: RunEvent[]): NodeProgress[] {
  const nodes = new Map<string, NodeProgress>();
  const order: string[] = [];
  const ensure = (id: unknown) => {
    if (typeof id !== "string" || id.length === 0) return undefined;
    if (!nodes.has(id)) {
      nodes.set(id, { id, status: "pending", detail: "pending" });
      order.push(id);
    }
    return nodes.get(id);
  };

  for (const event of events) {
    const data = record(event.data);
    if (event.kind === "graph_resolved" && Array.isArray(data.nodes)) {
      for (const id of data.nodes) ensure(id);
      continue;
    }
    const node = ensure(event.path || data.node);
    if (!node) continue;
    if (event.kind === "step_started") {
      node.status = "running";
      node.detail = string(data.type, "running");
    } else if (event.kind === "step_replayed") {
      node.status = "succeeded";
      node.detail = "replayed";
    } else if (event.kind === "step_skipped") {
      node.status = "skipped";
      node.detail = string(data.reason, "skipped");
    } else if (event.kind === "run_waiting" || event.kind === "confirm_request") {
      node.status = "waiting";
      node.detail = event.kind;
    } else if (event.kind === "step_finished") {
      const status = string(data.status, "finished");
      node.status = statusClass(status);
      node.detail = status;
    }
  }
  return order.flatMap((id) => nodes.get(id) || []);
}

function statusClass(status: string): NodeProgress["status"] {
  if (status === "success" || status === "succeeded" || status === "repaired") return "succeeded";
  if (status === "skipped") return "skipped";
  if (status === "needs_user" || status === "needs_confirm") return "waiting";
  if (["check_failed", "repair_exhausted", "failed"].includes(status)) return "failed";
  return "finished";
}

export function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function string(value: unknown, fallback: string): string {
  if (typeof value === "string" && value.length > 0) return value;
  const object = record(value);
  return typeof object.message === "string" && object.message.length > 0 ? object.message : fallback;
}
