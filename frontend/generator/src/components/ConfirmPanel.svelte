<script lang="ts">
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let details = $derived(typeof store.confirm?.details === "string" ? store.confirm.details : JSON.stringify(store.confirm?.details, null, 2));
</script>

{#if store.confirm}
  <div class="confirm-card" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
    <h3 id="confirm-title">{store.confirm.title || messages.confirm}</h3>
    <p>{[store.confirm.kind, store.confirm.target].filter(Boolean).join(" ")}</p>
    {#if store.confirm.details !== null}<pre>{details}</pre>{/if}
    <div class="confirm-actions">
      <button class="submit-btn" type="button" onclick={() => void store.withError(() => store.decideConfirmation("approve"))}>{messages.approve}</button>
      <button class="secondary-btn" type="button" onclick={() => void store.withError(() => store.decideConfirmation("deny"))}>{messages.deny}</button>
    </div>
  </div>
{/if}
