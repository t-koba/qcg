<script lang="ts">
  import type { OutputArtifact } from "../api/client";
  import { formatBytes } from "../format";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  import ArtifactPreview from "./ArtifactPreview.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let selected = $state<OutputArtifact | null>(null);

  /** Opens an artifact through an authenticated blob URL: a plain anchor
   * cannot attach the Authorization header an authenticated instance
   * requires, and the token must never travel in a query string. */
  async function openArtifact(artifact: OutputArtifact): Promise<void> {
    if (!store.currentRun) return;
    try {
      const blob = await store.api.artifactBlob(store.currentRun, artifact.path);
      const url = URL.createObjectURL(blob);
      window.open(url, "_blank", "noopener");
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } catch (error) {
      store.errorText = error instanceof Error ? error.message : String(error);
    }
  }
</script>

<div id="artifact-list" class="artifact-list">
  {#each store.artifacts as artifact}
    <article class="artifact" class:selected={selected?.path === artifact.path}>
      <div class="file-icon" aria-hidden="true">
        <svg viewBox="0 0 24 24"><path d="M6 3h8l4 4v14H6z"/><path d="M14 3v5h5"/></svg>
      </div>
      <div class="artifact-meta">
        <strong>{artifact.label || artifact.path}</strong>
        <span>{artifact.path} · {formatBytes(artifact.bytes)}</span>
        {#if artifact.description}<p>{artifact.description}</p>{/if}
      </div>
      <div class="artifact-actions">
        {#if artifact.preview !== "none"}<button type="button" onclick={() => selected = artifact}>{messages.preview}</button>{/if}
        <button type="button" onclick={() => void openArtifact(artifact)}>{messages.open}</button>
      </div>
    </article>
  {/each}
</div>
<ArtifactPreview {store} artifact={selected} {messages} onClose={() => selected = null} />

<style>
  /* Artifact list scoped to the generated files section. */
  .artifact-list {
    display: grid;
    gap: 10px;
  }

  .artifact {
    align-items: center;
    border: 1px solid var(--line);
    border-radius: var(--radius-lg);
    display: grid;
    gap: var(--space-md);
    grid-template-columns: auto minmax(0, 1fr) auto;
    padding: 12px 14px;
    transition: border-color .15s ease, background-color .15s ease;
  }

  .artifact.selected {
    background: var(--accent-soft);
    border-color: var(--accent);
  }

  .file-icon {
    align-items: center;
    background: var(--surface-muted);
    border-radius: 9px;
    color: var(--gray-500);
    display: flex;
    height: 38px;
    justify-content: center;
    width: 38px;
  }

  .file-icon svg {
    fill: none;
    height: 20px;
    stroke: currentColor;
    stroke-linecap: round;
    stroke-linejoin: round;
    stroke-width: 1.8;
    width: 20px;
  }

  .artifact-meta {
    display: grid;
    min-width: 0;
  }

  .artifact-meta strong {
    font-size: 13px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .artifact-meta span {
    color: var(--muted);
    font-size: 11px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .artifact-meta p {
    color: var(--muted);
    font-size: 11px;
    line-height: 1.4;
    margin: 3px 0 0;
  }

  .artifact-actions {
    display: flex;
    gap: var(--space-xs);
  }

  .artifact-actions button {
    align-items: center;
    background: transparent;
    border: 0;
    border-radius: var(--radius-md);
    color: var(--accent-dark);
    cursor: pointer;
    display: inline-flex;
    font-size: 12px;
    font-weight: 700;
    justify-content: center;
    min-height: 34px;
    padding: 6px 9px;
    text-decoration: none;
  }

  .artifact-actions button:hover {
    background: var(--accent-soft);
  }

  @media (max-width: 600px) {
    .artifact {
      grid-template-columns: auto minmax(0, 1fr);
    }

    .artifact-actions {
      grid-column: 1 / -1;
      justify-content: flex-end;
    }
  }
</style>
