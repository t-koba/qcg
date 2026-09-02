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
