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

<style>
  /* Confirmation card scoped to approval requests. */
  .interaction-card {
    background: var(--surface-subtle);
    border: 1px solid var(--line);
    border-radius: var(--radius-xl);
    margin-top: 28px;
    padding: 22px;
  }

  .interaction-card.confirmation {
    background: var(--warning-soft);
    border-color: var(--warning);
  }

  .interaction-heading {
    align-items: center;
    display: flex;
    gap: 11px;
    margin-bottom: 20px;
  }

  .interaction-heading h2 {
    font-size: 17px;
    margin: 0;
  }

  .interaction-index {
    align-items: center;
    background: var(--warning);
    border-radius: var(--space-sm);
    color: #1d1407;
    display: inline-flex;
    font-size: 13px;
    font-weight: 800;
    height: 28px;
    justify-content: center;
    width: 28px;
  }

  .confirmation-target {
    color: var(--warning);
    font-size: 13px;
    margin: -8px 0 14px;
  }

  .interaction-card pre {
    background: var(--confirm-code-bg);
    border-radius: var(--radius-md);
    color: var(--confirm-code-fg);
    font-size: 12px;
    margin: 0;
    max-height: 260px;
    overflow: auto;
    padding: 14px;
    white-space: pre-wrap;
  }

  .confirm-actions {
    display: flex;
    gap: 10px;
    margin-top: 18px;
  }

  @media (max-width: 600px) {
    .confirm-actions {
      display: grid;
    }
  }
</style>
