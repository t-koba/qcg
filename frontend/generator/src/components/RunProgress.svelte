<script lang="ts">
  import { humanizeIdentifier } from "../format";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();

  let active = $derived(["queued", "running", "waiting", "confirming"].includes(store.runState));
  let done = $derived(store.nodeProgress.filter((node) => node.status === "succeeded" || node.status === "skipped").length);
  let total = $derived(store.nodeProgress.length);
  let percent = $derived(total ? Math.round((done / total) * 100) : store.runState === "succeeded" ? 100 : 8);
  let currentNode = $derived(store.nodeProgress.find((node) => node.status === "running" || node.status === "waiting"));

  let statusMessage = $derived((() => {
    switch (store.runState) {
      case "queued": return messages.statusQueued;
      case "running": return messages.statusRunning;
      case "waiting": return messages.statusWaiting;
      case "confirming": return messages.statusConfirming;
      case "succeeded": return messages.statusSucceeded;
      case "failed": return messages.statusFailed;
      case "canceled": return messages.statusCanceled;
      case "interrupted": return messages.statusInterrupted;
      default: return "";
    }
  })());

  let progressLabel = $derived(messages.completedSteps
    .replace("{done}", String(done))
    .replace("{total}", String(total)));
</script>

<div id="run-state" class="run-summary {store.runState}">
  <div class="status-icon" aria-hidden="true">
    {#if store.runState === "queued" || store.runState === "running"}
      <span class="spinner"></span>
    {:else if store.runState === "succeeded"}
      <svg viewBox="0 0 24 24"><path d="m7 12 3 3 7-7"/></svg>
    {:else if store.runState === "waiting" || store.runState === "confirming"}
      <svg viewBox="0 0 24 24"><path d="M12 8v4l2.5 2.5"/><circle cx="12" cy="12" r="9"/></svg>
    {:else}
      <svg viewBox="0 0 24 24"><path d="m8 8 8 8m0-8-8 8"/><circle cx="12" cy="12" r="9"/></svg>
    {/if}
  </div>
  <div class="status-copy">
    <h2>{statusMessage}</h2>
    {#if currentNode}<p>{humanizeIdentifier(currentNode.id)}</p>
    {:else if total > 0}<p>{progressLabel}</p>{/if}
  </div>
  {#if active && store.currentRun}
    <button
      class="cancel-btn"
      type="button"
      disabled={store.pendingAction !== null}
      onclick={() => void store.withError(() => store.cancelRun())}
    >{store.pendingAction === "canceling" ? messages.canceling : messages.cancel}</button>
  {/if}
</div>

{#if total > 0}
  <div class="progress-track" role="progressbar" aria-label={progressLabel} aria-valuemin="0" aria-valuemax="100" aria-valuenow={percent}>
    <span style:width={`${percent}%`}></span>
  </div>
{/if}
