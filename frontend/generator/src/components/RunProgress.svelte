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

<style>
  /* Run status header scoped to the active run. */
  .run-summary {
    align-items: center;
    display: flex;
    gap: var(--space-lg);
  }

  .status-icon {
    align-items: center;
    background: var(--accent-soft);
    border-radius: var(--radius-xl);
    color: var(--accent);
    display: flex;
    flex: 0 0 auto;
    height: 52px;
    justify-content: center;
    width: 52px;
  }

  .status-icon svg {
    fill: none;
    height: 24px;
    stroke: currentColor;
    stroke-linecap: round;
    stroke-linejoin: round;
    stroke-width: 1.8;
    width: 24px;
  }

  .run-summary.succeeded .status-icon {
    background: var(--success-soft);
    color: var(--success);
  }

  .run-summary.failed .status-icon {
    background: var(--danger-soft);
    color: var(--danger);
  }

  .run-summary.canceled .status-icon,
  .run-summary.interrupted .status-icon {
    background: var(--surface-muted);
    color: var(--gray-500);
  }

  .status-copy h2 {
    font-size: 20px;
    letter-spacing: -.025em;
    margin: 0;
  }

  .status-copy p {
    color: var(--muted);
    font-size: 13px;
    margin: 3px 0 0;
  }

  .spinner {
    animation: spin .8s linear infinite;
    border: 2px solid currentColor;
    border-right-color: transparent;
    border-radius: 50%;
    display: block;
    height: 22px;
    width: 22px;
  }

  .progress-track {
    background: var(--track);
    border-radius: 999px;
    height: 6px;
    margin-top: var(--space-xl);
    overflow: hidden;
  }

  .progress-track span {
    background: var(--accent);
    border-radius: inherit;
    display: block;
    height: 100%;
    min-width: 6px;
    transition: width .25s ease;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  @media (max-width: 600px) {
    .run-summary {
      align-items: flex-start;
      flex-wrap: wrap;
    }
  }
</style>
