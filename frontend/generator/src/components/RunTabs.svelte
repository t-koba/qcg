<script lang="ts">
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();

  const tabs = $derived(store.orderedTabs());

  function shortId(runId: string): string {
    return runId.length > 14 ? `…${runId.slice(-8)}` : runId;
  }
</script>

{#if tabs.length > 0}
  <div class="run-tabs" role="tablist" aria-label={messages.openRuns}>
    {#each tabs as tab (tab.runId)}
      <div
        class="run-tab"
        class:selected={store.currentRun === tab.runId}
        class:run-active={["queued", "running", "waiting", "confirming"].includes(tab.runState)}
        role="tab"
        aria-selected={store.currentRun === tab.runId}
      >
        <button
          class="run-tab-open"
          type="button"
          title={tab.runId}
          onclick={() => store.selectTab(tab.runId)}
        >
          <span class="run-tab-dot run-tab-dot-{tab.runState}" aria-hidden="true"></span>
          <span class="run-tab-name">{tab.generatorName}</span>
          <small class="run-tab-id">{shortId(tab.runId)}</small>
          {#if tab.runState === "queued" && tab.queuePosition !== null}
            <small class="run-tab-queue">#{tab.queuePosition}</small>
          {/if}
          {#if tab.pendingAction}<span class="spinner tiny" aria-hidden="true"></span>{/if}
        </button>
        <button
          class="run-tab-close"
          type="button"
          aria-label={`${messages.closeRun} ${tab.runId}`}
          title={messages.closeRun}
          onclick={() => store.closeTab(tab.runId)}
        >×</button>
      </div>
    {/each}
    <button class="run-tab-new" type="button" onclick={() => store.showForm()}>+ {messages.newRun}</button>
  </div>
{/if}

<style>
  /* Open-run tabs scoped to the workspace header area. */
  .run-tabs {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-sm);
    margin-bottom: 20px;
  }

  .run-tab {
    align-items: center;
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius-md);
    display: flex;
    max-width: 260px;
    min-width: 0;
  }

  .run-tab.selected {
    background: var(--accent-soft);
    border-color: var(--accent);
  }

  .run-tab-open {
    align-items: center;
    background: transparent;
    border: 0;
    color: var(--text);
    cursor: pointer;
    display: flex;
    gap: var(--space-sm);
    min-width: 0;
    padding: 8px 4px 8px 10px;
    text-align: left;
  }

  .run-tab-name {
    font-size: 12px;
    font-weight: 700;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .run-tab-id,
  .run-tab-queue {
    color: var(--muted);
    font-size: 10px;
    white-space: nowrap;
  }

  .run-tab-dot {
    border-radius: 50%;
    flex: 0 0 auto;
    height: 8px;
    width: 8px;
  }

  .run-tab-dot-queued { background: var(--warning); }
  .run-tab-dot-running { background: var(--accent); }
  .run-tab-dot-waiting,
  .run-tab-dot-confirming { background: var(--warning); }
  .run-tab-dot-succeeded { background: var(--success); }
  .run-tab-dot-failed,
  .run-tab-dot-canceled,
  .run-tab-dot-interrupted { background: var(--danger); }
  .run-tab-dot-idle,
  .run-tab-dot-loading { background: var(--gray-400); }

  .run-tab-close {
    background: transparent;
    border: 0;
    border-radius: 6px;
    color: var(--muted);
    cursor: pointer;
    font-size: 16px;
    line-height: 1;
    padding: 6px 8px;
  }

  .run-tab-close:hover {
    background: var(--danger-soft);
    color: var(--danger);
  }

  .run-tab-new {
    background: transparent;
    border: 1px dashed var(--input-border);
    border-radius: var(--radius-md);
    color: var(--muted);
    cursor: pointer;
    font-size: 12px;
    font-weight: 700;
    padding: 8px 12px;
  }

  .run-tab-new:hover {
    border-color: var(--accent);
    color: var(--accent-dark);
  }

  .spinner.tiny {
    height: 14px;
    width: 14px;
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

  @keyframes spin {
    to { transform: rotate(360deg); }
  }
</style>
