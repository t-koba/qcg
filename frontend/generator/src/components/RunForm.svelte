<script lang="ts">
  import FieldControl from "../FieldControl.svelte";
  import { humanizeIdentifier } from "../format";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages, language }: { store: RunStore; messages: Messages; language: string } = $props();

  let stages = $derived(store.detail?.inputs?.stages || []);
  let activeStages = $derived(stages
    .map((stage) => ({
      ...stage,
      fields: store.activeFields.filter((field) => stage.fields.some((candidate) => candidate.id === field.id)),
    }))
    .filter((stage) => stage.fields.length > 0));
  let hasFields = $derived(activeStages.length > 0);

  function submitRun(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement) || !form.checkValidity()) {
      if (form instanceof HTMLFormElement) form.reportValidity();
      return;
    }
    void store.withError(() => store.startRun());
  }
</script>

<form
  class="run-form"
  aria-busy={store.pendingAction === "starting"}
  onsubmit={submitRun}
>
  {#if hasFields}
    <div class="form-fields">
      {#each activeStages as stage}
        <fieldset class:single-stage={activeStages.length === 1}>
          {#if activeStages.length > 1}<legend>{humanizeIdentifier(stage.id)}</legend>{/if}
          <div class="field-grid">
            {#each stage.fields as field}
              <FieldControl
                {field}
                value={store.values[field.id]}
                valueKey={field.id}
                requiredLabel={messages.required}
                {language}
                onValue={(id, value) => store.setValue(id, value)}
                onFile={(id, file) => store.withError(() => store.setFileValue(id, file))}
              />
            {/each}
          </div>
        </fieldset>
      {/each}
    </div>
    <div class="form-actions">
      <button class="primary-btn" type="submit" disabled={!store.selected || store.pendingAction !== null}>
        {store.pendingAction === "starting" ? messages.starting : messages.run}
      </button>
    </div>
  {:else}
    <div class="zero-input">
      <div class="zero-input-icon" aria-hidden="true">
        <svg viewBox="0 0 24 24"><path d="M8 12h8M12 8v8"/><circle cx="12" cy="12" r="9"/></svg>
      </div>
      <div><strong>{messages.noInputRequired}</strong><p>{messages.interactiveNote}</p></div>
      <button class="primary-btn" type="submit" disabled={!store.selected || store.pendingAction !== null}>
        {store.pendingAction === "starting" ? messages.starting : messages.run}
      </button>
    </div>
  {/if}
</form>
