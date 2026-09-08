<script lang="ts">
  import type { RunEvent } from "../api/client";
  import type { Messages } from "../messages";
  import { toolCallLabel } from "../event-log";
  import { record } from "../progress";
  let { events, messages }: { events: RunEvent[]; messages: Messages } = $props();

  function label(event: RunEvent): string {
    const data = typeof event.data === "object" && event.data !== null ? event.data as Record<string, unknown> : {};
    const node = event.path || (typeof data.node === "string" ? data.node : "");
    const reason = record(data.reason);
    const detail = typeof data.reason === "string"
      ? data.reason
      : typeof reason.message === "string"
        ? reason.message
      : typeof data.status === "string"
        ? data.status
        : "";
    switch (event.kind) {
      case "artifact": return `${messages.eventArtifact}: ${String(data.path || "file")}`;
      case "step_finished": return node.startsWith("ask_")
        ? messages.eventUserInteraction
        : [messages.eventStepFinished, node, detail].filter(Boolean).join(" · ");
      case "step_skipped": return [messages.eventStepSkipped, node, detail].filter(Boolean).join(" · ");
      case "llm_call": return [
        messages.eventLlmCall,
        typeof data.agent === "string" ? data.agent : "",
      ].filter(Boolean).join(" · ");
      case "tool_call": return toolCallLabel(data, messages);
      case "agent_delegated": return [
        messages.eventAgentDelegated,
        typeof data.agent === "string" ? data.agent : "",
      ].filter(Boolean).join(" · ");
      case "agent_completed": return [
        messages.eventAgentCompleted,
        typeof data.agent === "string" ? data.agent : "",
      ].filter(Boolean).join(" · ");
      case "agent_failed": return [
        messages.eventAgentFailed,
        typeof data.agent === "string" ? data.agent : "",
        typeof data.message === "string" ? data.message : messages.eventToolFailed,
      ].filter(Boolean).join(" · ");
      case "agent_handoff": return [
        messages.eventAgentHandoff,
        typeof data.agent === "string" ? data.agent : "",
      ].filter(Boolean).join(" · ");
      case "context_compacted": return [
        messages.eventContextCompacted,
        typeof data.scope === "string" ? data.scope : messages.eventPromptContext,
        typeof data.final_bytes === "number" && typeof data.limit_bytes === "number"
          ? `${data.final_bytes}/${data.limit_bytes} B`
          : "",
      ].filter(Boolean).join(" · ");
      case "llm_route_failed": return [
        messages.eventLlmRouteFailed,
        typeof data.provider === "string" ? data.provider : "",
        typeof data.model === "string" ? data.model : "",
        routeFailureKind(data.kind),
      ].filter(Boolean).join(" · ");
      case "user_interaction": return messages.eventUserInteraction;
      case "run_queued": return messages.statusQueued;
      case "run_started": return messages.eventStarted;
      case "run_finished": return messages.eventRunFinished;
      case "run_canceled": return messages.eventRunCanceled;
      case "run_error": return messages.eventRunError;
      case "run_waiting": return messages.eventRunWaiting;
      case "run_interrupted": return messages.eventRunInterrupted;
      case "run_resumed": return messages.eventRunResumed;
      default: return event.kind;
    }
  }

  function sources(event: RunEvent): { url: string; title: string }[] {
    const data = typeof event.data === "object" && event.data !== null ? event.data as Record<string, unknown> : {};
    if (!Array.isArray(data.sources)) return [];
    return data.sources.flatMap((source) => {
      if (!source || typeof source !== "object") return [];
      const item = source as Record<string, unknown>;
      if (typeof item.url !== "string" || !/^https?:\/\//.test(item.url)) return [];
      return [{ url: item.url, title: typeof item.title === "string" && item.title ? item.title : item.url }];
    });
  }

  function routeFailureKind(value: unknown): string {
    if (typeof value === "string") return value;
    const kind = record(value);
    return typeof kind.http_status === "number" ? `HTTP ${kind.http_status}` : "";
  }
</script>

{#if events.length > 0}
  <details class="run-details">
    <summary>{messages.technicalDetails}<span>{events.length}</span></summary>
    <ol class="event-log">
      {#each events as event (event.seq)}
        {@const eventSources = sources(event)}
        <li>
          <span class="event-seq">{event.seq}</span>
          <span>
            {label(event)}
            {#if eventSources.length > 0}
              <span class="event-sources">
                {#each eventSources as source}
                  <a href={source.url} target="_blank" rel="noopener noreferrer">{source.title}</a>
                {/each}
              </span>
            {/if}
          </span>
        </li>
      {/each}
    </ol>
  </details>
{/if}

<style>
  /* Technical run details scoped to the event log. */
  .run-details {
    border-top: 1px solid var(--line);
    margin-top: 28px;
    padding-top: 18px;
  }

  .run-details summary {
    align-items: center;
    color: var(--muted);
    cursor: pointer;
    display: flex;
    font-size: 12px;
    font-weight: 650;
    gap: var(--space-sm);
    list-style: none;
    width: fit-content;
  }

  .run-details summary::-webkit-details-marker {
    display: none;
  }

  .run-details summary::before {
    content: "›";
    font-size: 18px;
    line-height: 1;
    transition: transform .15s ease;
  }

  .run-details[open] summary::before {
    transform: rotate(90deg);
  }

  .run-details summary span {
    background: var(--surface-muted);
    border-radius: 999px;
    font-size: 10px;
    padding: 2px 7px;
  }

  .event-log {
    display: grid;
    gap: 0;
    list-style: none;
    margin: 14px 0 0;
    max-height: 280px;
    overflow: auto;
    padding: 0;
  }

  .event-log li {
    align-items: baseline;
    border-top: 1px solid var(--line);
    color: var(--gray-600);
    display: grid;
    font-size: 12px;
    gap: var(--space-md);
    grid-template-columns: 28px 1fr;
    padding: 8px 2px;
  }

  .event-seq {
    color: var(--gray-400);
    font-variant-numeric: tabular-nums;
    text-align: right;
  }

  .event-sources {
    display: flex;
    flex-wrap: wrap;
    gap: 4px 10px;
    margin-top: 4px;
  }

  .event-sources a {
    color: var(--accent-dark);
    max-width: 360px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
