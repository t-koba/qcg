<script lang="ts">
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let active = $derived(["running", "waiting", "confirming", "interrupted"].includes(store.runState));

  let statusMessage = $derived((() => {
    switch (store.runState) {
      case "running": return messages.statusRunning;
      case "waiting": return messages.statusWaiting;
      case "confirming": return messages.statusConfirming;
      case "succeeded": return messages.statusSucceeded;
      case "failed": return messages.statusFailed;
      default: return "";
    }
  })());
</script>

{#if store.nodeProgress.length > 0}
  <div id="node-progress" class="node-progress" aria-label="Flow progress">
    {#each store.nodeProgress as node}
      <div class="node {node.status}" title={node.detail}>
        <span class="node-dot" aria-hidden="true"></span>
        <span>{node.id}</span>
        <span class="hint">{node.detail}</span>
      </div>
    {/each}
  </div>
{/if}

<div class="panel-heading">
  <div><h2>{statusMessage}</h2></div>
  <div class="run-actions">
    {#if active}<button class="secondary-btn" type="button" onclick={() => void store.withError(() => store.cancelRun())}>{messages.cancel}</button>{/if}
    {#if store.currentRun && store.artifacts.length}<a id="zip-link" class="download-btn" href={store.api.zipUrl(store.currentRun)}>{messages.downloadZip}</a>{/if}
  </div>
</div>
