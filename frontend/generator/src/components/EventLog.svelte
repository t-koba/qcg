<script lang="ts">
  import type { RunEvent } from "../api/client";
  import type { Messages } from "../messages";
  let { events, messages }: { events: RunEvent[]; messages: Messages } = $props();

  function label(event: RunEvent): string {
    const data = (typeof event.data === "object" && event.data !== null) ? event.data as Record<string, unknown> : {};
    const node = typeof data.node === "string" ? data.node : "";
    switch (event.kind) {
      case "artifact": return `${messages.eventArtifact}: ${String(data.path || "file")}`;
      case "step_finished": return node.startsWith("ask_") ? messages.eventUserInteraction : `${messages.eventStepFinished}: ${node}`;
      case "step_skipped": return `${messages.eventStepSkipped}: ${node}`;
      case "llm_call": return messages.eventLlmCall;
      case "user_interaction": return messages.eventUserInteraction;
      case "run_started": return messages.eventStarted;
      case "run_finished": return messages.eventRunFinished;
      case "run_error": return messages.eventRunError;
      default: return event.kind;
    }
  }
</script>

{#if events.length > 0}
<details class="debug-log">
  <summary>{messages.debugDetails}</summary>
  <ol class="event-log">
    {#each events as event (event.seq)}
      <li><code>[{event.kind}]</code> <span>{label(event)}</span></li>
    {/each}
  </ol>
</details>
{/if}
