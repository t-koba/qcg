<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import ArtifactList from "./components/ArtifactList.svelte";
  import ConfirmPanel from "./components/ConfirmPanel.svelte";
  import EventLog from "./components/EventLog.svelte";
  import McpConnections from "./components/McpConnections.svelte";
  import QuestionPanel from "./components/QuestionPanel.svelte";
  import RunForm from "./components/RunForm.svelte";
  import RunHistory from "./components/RunHistory.svelte";
  import RunProgress from "./components/RunProgress.svelte";
  import RunTabs from "./components/RunTabs.svelte";
  import { setStoredApiToken, storedApiToken } from "./api/client";
  import { currentMessages } from "./messages";
  import { RunStore } from "./run-store.svelte";

  const store = new RunStore();
  const language = navigator.language.toLowerCase().startsWith("ja") ? "ja" : "en";
  const messages = currentMessages(language);
  let refreshTimer: ReturnType<typeof setInterval> | null = null;
  let apiTokenInput = $state(storedApiToken());

  function saveApiToken(): void {
    setStoredApiToken(apiTokenInput);
    // Reload so every fetch and stream picks up the credential; the view is
    // restored from local storage by the store.
    window.location.reload();
  }

  function clearApiToken(): void {
    setStoredApiToken("");
    apiTokenInput = "";
    window.location.reload();
  }

  /** Downloads the bundle through an authenticated blob URL. */
  async function downloadZip(): Promise<void> {
    if (!store.currentRun) return;
    try {
      const blob = await store.api.zipBlob(store.currentRun);
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `${store.currentRun}.zip`;
      anchor.click();
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } catch (error) {
      store.errorText = error instanceof Error ? error.message : String(error);
    }
  }

  type ThemeChoice = "light" | "dark" | "system";
  const themeStorageKey = "qcg-theme";
  let theme = $state<ThemeChoice>("system");

  function applyTheme(choice: ThemeChoice): void {
    // Explicit choice sets dataset.theme; system follows the OS media query.
    if (choice === "system") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.dataset.theme = choice;
    }
  }

  function cycleTheme(): void {
    theme = theme === "system" ? "light" : theme === "light" ? "dark" : "system";
    try {
      localStorage.setItem(themeStorageKey, theme);
    } catch {
      // Storage may be unavailable; theme still applies for this session.
    }
    applyTheme(theme);
  }

  let themeLabel = $derived(
    theme === "light" ? messages.themeLight : theme === "dark" ? messages.themeDark : messages.themeSystem,
  );

  onMount(() => {
    document.documentElement.lang = language;
    let initial: ThemeChoice = "system";
    try {
      const stored = localStorage.getItem(themeStorageKey);
      if (stored === "light" || stored === "dark" || stored === "system") initial = stored;
    } catch {
      initial = "system";
    }
    theme = initial;
    applyTheme(theme);
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const onMediaChange = (): void => {
      if (theme === "system") applyTheme("system");
    };
    media.addEventListener("change", onMediaChange);
    void store.withError(() => store.initialize());
    refreshTimer = setInterval(() => {
      if (document.visibilityState === "visible") {
        void store.withError(() => store.refreshRuns());
      }
    }, 5000);
    return () => media.removeEventListener("change", onMediaChange);
  });
  onDestroy(() => {
    if (refreshTimer) clearInterval(refreshTimer);
    store.destroy();
  });

  let showRun = $derived(store.currentRun !== "");
  let terminal = $derived(["succeeded", "failed", "canceled", "interrupted"].includes(store.runState));
  let retryablePlaceholder = $derived(store.failedPlaceholderId());
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
          onclick={() => void store.withError(() => store.selectGenerator(generator.id))}
        >
          <span>{generator.name || generator.id}</span>
          {#if generator.description}<small>{generator.description}</small>{/if}
        </button>
      {/each}
    </nav>
    <RunHistory {store} {messages} />
    <div class="theme-switch">
      <button type="button" onclick={cycleTheme} aria-label={messages.theme} title={messages.theme}>
        {messages.theme}: {themeLabel}
      </button>
    </div>
    <div class="token-switch">
      <label for="api-token">{messages.apiToken}</label>
      <input
        id="api-token"
        type="password"
        bind:value={apiTokenInput}
        placeholder={messages.apiTokenPlaceholder}
        title={messages.apiTokenHint}
        autocomplete="off"
      />
      <div class="token-actions">
        <button type="button" onclick={saveApiToken}>{messages.apiTokenSave}</button>
        <button type="button" onclick={clearApiToken}>{messages.apiTokenClear}</button>
      </div>
    </div>
    <McpConnections {messages} />
  </aside>

  <main class="workspace">
    <div class="workspace-inner">
      {#if store.errorText}
        <div class="error-banner" role="alert">
          <span>{store.errorText}</span>
          {#if retryablePlaceholder}
            <button class="secondary-btn" type="button" disabled={store.pendingAction !== null} onclick={() => void store.withError(() => store.retryFailedStart(retryablePlaceholder))}>{messages.retryStart}</button>
          {/if}
          <button type="button" aria-label={messages.dismissError} onclick={() => store.dismissError()}>×</button>
        </div>
      {/if}

      <RunTabs {store} {messages} />

      <header class="workspace-header">
        <div>
          <p class="eyebrow">{store.detail?.generator?.id || "qcg"}</p>
          <h1>{generatorName}</h1>
          {#if store.detail?.generator?.description}
            <p class="generator-description">{store.detail.generator.description}</p>
          {/if}
          {#if store.runState === "queued" && store.queuePosition !== null}
            <p class="queue-note">{messages.queuedPosition.replace("{position}", String(store.queuePosition))}</p>
          {/if}
        </div>
      </header>

      {#if store.runState === "loading"}
        <section class="surface loading-surface" aria-label={messages.statusLoading} aria-busy="true">
          <div class="skeleton wide"></div><div class="skeleton"></div><div class="skeleton short"></div>
        </section>
      {:else if !store.detail}
        <section class="surface empty-surface"><p>{messages.noGenerators}</p></section>
      {:else if !showRun}
        <section class="surface input-surface"><RunForm {store} {messages} {language} /></section>
      {:else}
        <section class="surface run-surface" aria-live="polite">
          <RunProgress {store} {messages} />
          <QuestionPanel {store} {messages} {language} />
          <ConfirmPanel {store} {messages} />
          {#if terminal}
            <div class="completion-actions">
              <button class="secondary-btn" type="button" disabled={!store.currentRun || store.pendingAction !== null} onclick={() => void store.withError(() => store.forkCurrentRun())}>{messages.forkRun}</button>
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
            {#if store.currentRun}<button id="zip-link" class="secondary-btn" type="button" onclick={() => void downloadZip()}>{messages.downloadZip}</button>{/if}
          </div>
          <ArtifactList {store} {messages} />
        </section>
      {/if}
    </div>
  </main>
</div>
