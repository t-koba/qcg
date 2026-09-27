import type { RunEvent } from "./api/client";

export type NodeProgress = {
  id: string;
  status: "pending" | "running" | "succeeded" | "skipped" | "waiting" | "failed" | "finished";
  detail: string;
};

/**
 * Node-status aggregate over the run event stream.
 *
 * Progress is state, not a view of the retained event window: the store keeps
 * only a bounded tail of events for display, so folding them on every render
 * would lose the status of nodes whose events scrolled out. The aggregate is
 * therefore updated one event at a time and never derived from the window.
 */
export type NodeProgressAggregate = {
  nodes: Map<string, NodeProgress>;
  order: string[];
};

export function emptyNodeProgress(): NodeProgressAggregate {
  return { nodes: new Map(), order: [] };
}

export function applyNodeProgress(
  aggregate: NodeProgressAggregate,
  event: RunEvent,
): NodeProgressAggregate {
  const ensure = (id: unknown) => {
    if (typeof id !== "string" || id.length === 0) return undefined;
    if (!aggregate.nodes.has(id)) {
      aggregate.nodes.set(id, { id, status: "pending", detail: "pending" });
      aggregate.order.push(id);
    }
    return aggregate.nodes.get(id);
  };

  const data = record(event.data);
  if (event.kind === "graph_resolved" && Array.isArray(data.nodes)) {
    for (const id of data.nodes) ensure(id);
    return aggregate;
  }
  const node = ensure(event.path || data.node);
  if (!node) return aggregate;
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
  return aggregate;
}

export function materialize(aggregate: NodeProgressAggregate): NodeProgress[] {
  return aggregate.order.flatMap((id) => aggregate.nodes.get(id) || []);
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
