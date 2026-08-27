<script lang="ts">
  import FieldControl from "../FieldControl.svelte";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let heading = $state<HTMLElement>();
  $effect(() => { if (store.question) queueMicrotask(() => heading?.focus()); });
</script>

{#if store.question}
  <section id="question-panel" class="question-card" aria-labelledby="question-title">
    <h3 id="question-title" tabindex="-1" bind:this={heading}>{store.question.title || store.question.id}</h3>
    <form onsubmit={(event) => { event.preventDefault(); void store.withError(() => store.answerQuestion()); }}>
      {#each store.question.fields as field}
        <FieldControl {field} value={store.values[`question:${field.id}`] ?? field.default} valueKey={`question:${field.id}`} onValue={(id, value) => store.setValue(id, value)} onFile={(id, file) => store.setFileValue(id, file)} />
      {/each}
      <button type="submit">{messages.answer}</button>
    </form>
  </section>
{/if}
