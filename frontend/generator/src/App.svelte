<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import ArtifactList from "./components/ArtifactList.svelte";
  import ConfirmPanel from "./components/ConfirmPanel.svelte";
  import EventLog from "./components/EventLog.svelte";
  import McpConnections from "./components/McpConnections.svelte";
  import QuestionPanel from "./components/QuestionPanel.svelte";
  import RunForm from "./components/RunForm.svelte";
  import RunProgress from "./components/RunProgress.svelte";
  import { currentMessages } from "./messages";
  import { RunStore } from "./run-store.svelte";

  const store = new RunStore();
  const language = navigator.language.toLowerCase().startsWith("ja") ? "ja" : "en";
  const messages = currentMessages(language);

  onMount(() => {
    document.documentElement.lang = language;
    void store.withError(() => store.initialize());
  });
  onDestroy(() => store.destroy());

  let active = $derived(["queued", "running", "waiting", "confirming"].includes(store.runState));
  let terminal = $derived(["succeeded", "failed", "canceled", "interrupted"].includes(store.runState));
  let showRun = $derived(active || terminal);
  let generatorName = $derived(store.detail?.generator?.name || store.selected || messages.selectGenerator);

</script>

<svelte:head><title>{generatorName === messages.selectGenerator ? "qcg" : `${generatorName} · qcg`}</title></svelte:head>

<div class="app-shell">
  <aside class="sidebar">
    <div class="brand" aria-label="qcg">
      <div class="mark" aria-hidden="true">q</div>
      <div><strong>qcg</strong><span>generator workspace</span></div>
    </div>

    <nav class="generator-nav" aria-label={messages.generators}>
      <p class="nav-label">{messages.generators}</p>
      {#each store.generators as generator}
        <button
          class:active={store.selected === generator.id}
          type="button"
          aria-current={store.selected === generator.id ? "page" : undefined}
          disabled={active && store.selected !== generator.id}
          onclick={() => void store.withError(() => store.selectGenerator(generator.id))}
        >
          <span>{generator.name || generator.id}</span>
          {#if generator.description}<small>{generator.description}</small>{/if}
        </button>
      {/each}
    </nav>
    <McpConnections {messages} />
  </aside>

  <main class="workspace">
    <div class="workspace-inner">
      {#if store.errorText}
        <div class="error-banner" role="alert">
          <span>{store.errorText}</span>
          <button type="button" aria-label={messages.dismissError} onclick={() => store.dismissError()}>×</button>
        </div>
      {/if}

      <header class="workspace-header">
        <div>
          <p class="eyebrow">{store.detail?.generator?.id || "qcg"}</p>
          <h1>{generatorName}</h1>
          {#if store.detail?.generator?.description}
            <p class="generator-description">{store.detail.generator.description}</p>
          {/if}
        </div>
      </header>

      {#if store.runState === "loading"}
        <section class="surface loading-surface" aria-label={messages.statusLoading} aria-busy="true">
          <div class="skeleton wide"></div><div class="skeleton"></div><div class="skeleton short"></div>
        </section>
      {:else if !store.detail}
        <section class="surface empty-surface"><p>{messages.noGenerators}</p></section>
      {:else if store.runState === "idle"}
        <section class="surface input-surface"><RunForm {store} {messages} {language} /></section>
      {:else if showRun}
        <section class="surface run-surface" aria-live="polite">
          <RunProgress {store} {messages} />
          <QuestionPanel {store} {messages} {language} />
          <ConfirmPanel {store} {messages} />
          {#if terminal}
            <div class="completion-actions">
              <button class="primary-btn" type="button" onclick={() => store.resetRun()}>{messages.startAgain}</button>
            </div>
          {/if}
          <EventLog events={store.events} {messages} />
        </section>
      {/if}

      {#if store.artifacts.length > 0}
        <section class="surface artifacts-surface">
          <div class="section-heading">
            <div><p class="eyebrow">{store.artifacts.length}</p><h2>{messages.artifacts}</h2></div>
            {#if store.currentRun}<a id="zip-link" class="secondary-btn" href={store.api.zipUrl(store.currentRun)}>{messages.downloadZip}</a>{/if}
          </div>
          <ArtifactList {store} {messages} />
        </section>
      {/if}
    </div>
  </main>
</div>
