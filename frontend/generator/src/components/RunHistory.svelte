<script lang="ts">
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();

  const openIds = $derived(new Set(store.tabOrder));
  // Single scan: both the visible slice and the hidden count derive from
  // the same closed-run list instead of filtering twice.
  const closedRuns = $derived(store.runs.filter((run) => !openIds.has(run.run_id)));
  const history = $derived(closedRuns.slice(0, store.historyLimit));
  const hiddenCount = $derived(closedRuns.length - history.length);
  const states = [
    "",
    "queued",
    "running",
    "waiting",
    "confirming",
    "succeeded",
    "failed",
    "canceled",
    "interrupted",
  ];

  function stateLabel(state: string): string {
    switch (state) {
      case "queued": return messages.statusQueued;
      case "running": return messages.statusRunning;
      case "waiting": return messages.statusWaiting;
      case "confirming": return messages.statusConfirming;
      case "succeeded": return messages.statusSucceeded;
      case "failed": return messages.statusFailed;
      case "canceled": return messages.statusCanceled;
      case "cancel_requested": return messages.statusCancelRequested;
      case "interrupted": return messages.statusInterrupted;
      default: return messages.filterAllStates;
    }
  }
</script>

<nav class="generator-nav run-history" aria-label={messages.runHistory}>
  <p class="nav-label">{messages.runHistory}</p>
  <label class="run-history-filter">
    <span class="schema-validity-proxy">{messages.filterRuns}</span>
    <select
      value={store.historyState}
      aria-label={messages.filterRuns}
      onchange={(event) => store.setHistoryFilter(event.currentTarget.value)}
    >
      {#each states as state}<option value={state}>{stateLabel(state)}</option>{/each}
    </select>
  </label>
  {#if history.length === 0}
    <p class="run-history-empty">{messages.noRuns}</p>
  {:else}
    {#each history as run (run.run_id)}
      <button type="button" onclick={() => void store.withError(() => store.openRun(run.run_id))} title={run.run_id}>
        <span>{run.generator_id}</span>
        <small>{run.state} · {run.started_at}</small>
      </button>
    {/each}
    {#if hiddenCount > 0}
      <button type="button" class="secondary-btn" disabled={store.historyLoading} onclick={() => store.loadMoreHistory()}>
        {messages.loadMore} ({hiddenCount})
      </button>
    {/if}
    {#if store.historyHasMore}
      <p class="run-history-empty">More history remains on the server; Load more fetches the next page.</p>
    {/if}
  {/if}
</nav>

<style>
  /* Run history scoped to the sidebar section. */
  .run-history {
    border-top: 1px solid rgba(152, 162, 179, .2);
    margin-top: 16px;
    padding-top: 16px;
  }

  .run-history-empty {
    color: var(--sidebar-faint);
    font-size: 11px;
    margin: 0 10px;
  }

  .run-history-filter {
    display: block;
    margin: 0 0 8px;
    padding: 0 2px;
  }

  .run-history-filter select {
    background: var(--sidebar-hover);
    border: 1px solid rgba(152, 162, 179, .35);
    border-radius: var(--radius-sm);
    color: var(--sidebar-fg);
    font-size: 11px;
    padding: 5px 6px;
    width: 100%;
  }
</style>
