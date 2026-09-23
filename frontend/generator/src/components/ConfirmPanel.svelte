<script lang="ts">
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  import { describeConfirmId, isApproveDisabled } from "../view-persistence";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let heading = $state<HTMLElement>();
  let details = $derived(typeof store.confirm?.details === "string"
    ? store.confirm.details
    : JSON.stringify(store.confirm?.details, null, 2));
  // Digest vs invocation-hash decomposition of the confirmation id (Q1):
  // validated hex lengths before trusting the tail split, explicit
  // unknown-scope state instead of an empty render, via the shared helper
  // so the component and its tests cannot diverge. Shown next to the id so
  // reviewers see which hex is content-bound and which is call-bound.
  let idMapping = $derived.by(() => describeConfirmId(store.confirm?.id, store.confirm?.scope));
  // Title carries both the operation digest and the invocation hash (Q1):
  // digest-only hid the call binding for second-time (content-reuse vs
  // invocation) review.
  let idTitle = $derived.by(() => {
    const id = store.confirm?.id ?? "";
    const digest = store.confirm?.operation_digest ?? "";
    return id && digest && id.includes(digest) ? `ID ${id} · digest ${digest}` : id ? `ID ${id}` : "";
  });
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
    {#if store.confirm.id}
      <p class="confirmation-id" title={idTitle}>ID: {store.confirm.id}</p>
      {#if idMapping}
        <p class="confirmation-id-mapping">{idMapping}</p>
      {/if}
      <p class="confirmation-scope-value">Scope: {store.confirm.scope ?? "unknown"}</p>
    {/if}
    {#if store.confirm.scope === "content"}
      <p class="confirmation-scope">
        {messages.confirmScopeContent}
      </p>
    {:else if store.confirm.scope === "invocation"}
      <p class="confirmation-scope">
        {messages.confirmScopeInvocation}
      </p>
    {:else}
      <p class="confirmation-scope confirmation-scope-unknown" role="alert">
        {messages.confirmScopeUnknown}
      </p>
    {/if}
    {#if store.confirm.details !== null}<pre>{details}</pre>{/if}
    <div class="confirm-actions">
      <button
        class="primary-btn"
        type="button"
        disabled={isApproveDisabled(store.confirm.scope, store.pendingAction)}
        title={store.confirm.scope !== "content" && store.confirm.scope !== "invocation" ? messages.confirmScopeUnknown : undefined}
        onclick={() => void store.withError(() => store.decideConfirmation("approve"))}
      >{store.pendingAction === "approving" ? messages.approving : messages.approve}</button>
      <button
        class="secondary-btn"
        type="button"
        disabled={store.pendingAction !== null}
        title="Deny is always safe and stays available even for unknown scopes."
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

  .confirmation-id {
    color: var(--text-muted);
    font-family: monospace;
    font-size: 11px;
    margin: -8px 0 4px;
    overflow-wrap: anywhere;
  }

  .confirmation-id-mapping {
    color: var(--text-muted);
    font-family: monospace;
    font-size: 11px;
    margin: -4px 0 4px;
    overflow-wrap: anywhere;
  }

  .confirmation-scope-value {
    color: var(--text-muted);
    font-size: 11px;
    margin: -4px 0 14px;
  }

  .confirmation-scope {
    color: var(--text-muted);
    font-size: 12px;
    margin: -8px 0 14px;
  }

  .confirmation-scope-unknown {
    color: var(--danger, #b42318);
    font-weight: 700;
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
