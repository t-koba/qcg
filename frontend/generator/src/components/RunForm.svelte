<script lang="ts">
  import FieldControl from "../FieldControl.svelte";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();

  let stages = $derived(store.detail?.inputs?.stages || []);
  let hasFields = $derived(stages.some((stage) => stage.fields.length > 0));
</script>

<form class="run-form" onsubmit={(event) => { event.preventDefault(); void store.withError(() => store.startRun()); }}>
  {#if hasFields}
    {#each stages as stage}
      {#if store.activeFields.filter((field) => stage.fields.some((candidate) => candidate.id === field.id)).length > 0}
        <fieldset class="stage">
          <legend>{stage.id}</legend>
          {#each store.activeFields.filter((field) => stage.fields.some((candidate) => candidate.id === field.id)) as field}
            <FieldControl {field} value={store.values[field.id]} valueKey={field.id} onValue={(id, value) => store.setValue(id, value)} onFile={(id, file) => store.setFileValue(id, file)} />
          {/each}
        </fieldset>
      {/if}
    {/each}
    <button class="submit-btn" type="submit" disabled={!store.selected && !store.generators.length}>{messages.run}</button>
  {:else}
    <div class="hero">
      <h2>{messages.readyToGenerate}</h2>
      <p>{messages.interactiveNote}</p>
      <button class="cta-button" type="submit" disabled={!store.selected && !store.generators.length}>{messages.run}</button>
    </div>
  {/if}
</form>
