<script lang="ts">
  import FieldControl from "../FieldControl.svelte";
  import { humanizeIdentifier, localizedText } from "../format";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  let { store, messages, language }: { store: RunStore; messages: Messages; language: string } = $props();
  let heading = $state<HTMLElement>();
  $effect(() => { if (store.question) queueMicrotask(() => heading?.focus()); });

  function submitAnswer(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement) || !form.checkValidity()) {
      if (form instanceof HTMLFormElement) form.reportValidity();
      return;
    }
    void store.withError(() => store.answerQuestion());
  }
</script>

{#if store.question}
  <section id="question-panel" class="interaction-card" aria-labelledby="question-title">
    <div class="interaction-heading">
      <span class="interaction-index" aria-hidden="true">?</span>
      <h2 id="question-title" tabindex="-1" bind:this={heading}>
        {localizedText(store.question.title || humanizeIdentifier(store.question.id), store.question.title_i18n, language)}
      </h2>
    </div>
    <form
      class="answer-form"
      aria-busy={store.pendingAction === "answering"}
      onsubmit={submitAnswer}
    >
      <div class="field-grid">
        {#each store.question.fields as field}
          <FieldControl
            {field}
            value={store.values[`question:${field.id}`] ?? field.default}
            valueKey={`question:${field.id}`}
            requiredLabel={messages.required}
            {language}
            onValue={(id, value) => store.setValue(id, value)}
            onFile={(id, file) => store.withError(() => store.setFileValue(id, file))}
          />
        {/each}
      </div>
      <button class="primary-btn" type="submit" disabled={store.pendingAction !== null}>
        {store.pendingAction === "answering" ? messages.answering : messages.answer}
      </button>
    </form>
  </section>
{/if}
