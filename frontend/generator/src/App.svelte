<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import ArtifactList from "./components/ArtifactList.svelte";
  import ConfirmPanel from "./components/ConfirmPanel.svelte";
  import EventLog from "./components/EventLog.svelte";
  import QuestionPanel from "./components/QuestionPanel.svelte";
  import RunForm from "./components/RunForm.svelte";
  import RunProgress from "./components/RunProgress.svelte";
  import { currentMessages } from "./messages";
  import { RunStore } from "./run-store.svelte";

  const store = new RunStore();
  const messages = currentMessages();

  onMount(() => void store.initialize());
  onDestroy(() => store.destroy());

  let hasActiveRun = $derived(["running", "waiting", "confirming", "interrupted"].includes(store.runState));
  let showProgress = $derived(hasActiveRun || store.runState === "succeeded" || store.runState === "failed");
</script>

<svelte:head><title>qcg</title></svelte:head>

<div class="topbar">
  <div class="brand"><div class="mark">q</div><div><h1>qcg</h1><p>quick config generator</p></div></div>
  {#if store.generators.length > 1}
  <nav class="gen-nav" aria-label="Generators">
    {#each store.generators as generator}
      <button class:active={store.selected === generator.id} type="button" onclick={() => void store.withError(() => store.selectGenerator(generator.id))}>{generator.name || generator.id}</button>
    {/each}
  </nav>
  {/if}
</div>

<main class="main">
  {#if store.errorText}<div class="error-banner">{store.errorText}</div>{/if}

  <section class="card">
    <div class="card-header">
      <h2>{store.detail?.generator?.name || messages.selectGenerator}</h2>
      <span id="run-state" class="state-badge {store.runState}">{store.runState}</span>
    </div>
    <div class="card-body">
      {#if store.runState === "idle" || store.runState === "loading"}
        <RunForm {store} {messages} />
      {:else}
        <p class="hint">{messages.interactiveNote}</p>
      {/if}
    </div>
  </section>

  {#if showProgress}
  <section class="card">
    <div class="card-header">
      <h2>{messages.progress}</h2>
      {#if hasActiveRun}<button class="danger-btn" type="button" onclick={() => void store.withError(() => store.cancelRun())}>{messages.cancel}</button>{/if}
    </div>
    <div class="card-body">
      <RunProgress {store} {messages} />
      <EventLog events={store.events} {messages} />
      <QuestionPanel {store} {messages} />
      <ConfirmPanel {store} {messages} />
    </div>
  </section>
  {/if}

  {#if store.artifacts.length > 0}
  <section class="card">
    <div class="card-header"><h2>{messages.artifacts}</h2></div>
    <div class="card-body"><ArtifactList {store} {messages} /></div>
  </section>
  {/if}
</main>
