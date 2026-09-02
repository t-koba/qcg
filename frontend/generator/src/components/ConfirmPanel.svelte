<script lang="ts">
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let heading = $state<HTMLElement>();
  let details = $derived(typeof store.confirm?.details === "string"
    ? store.confirm.details
    : JSON.stringify(store.confirm?.details, null, 2));
  $effect(() => { if (store.confirm) queueMicrotask(() => heading?.focus()); });
</script>

{#if store.confirm}
  <section class="interaction-card confirmation" aria-labelledby="confirm-title">
    <div class="interaction-heading">
      <span class="interaction-index" aria-hidden="true">!</span>
      <h2 id="confirm-title" tabindex="-1" bind:this={heading}>{store.confirm.title || messages.confirm}</h2>
    </div>
    {#if store.confirm.kind || store.confirm.target}
      <p class="confirmation-target">{[store.confirm.kind, store.confirm.target].filter(Boolean).join(" · ")}</p>
    {/if}
    {#if store.confirm.details !== null}<pre>{details}</pre>{/if}
    <div class="confirm-actions">
      <button
        class="primary-btn"
        type="button"
        disabled={store.pendingAction !== null}
        onclick={() => void store.withError(() => store.decideConfirmation("approve"))}
      >{store.pendingAction === "approving" ? messages.approving : messages.approve}</button>
      <button
        class="secondary-btn"
        type="button"
        disabled={store.pendingAction !== null}
        onclick={() => void store.withError(() => store.decideConfirmation("deny"))}
      >{store.pendingAction === "denying" ? messages.denying : messages.deny}</button>
    </div>
  </section>
{/if}
