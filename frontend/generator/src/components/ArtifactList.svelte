<script lang="ts">
  import type { OutputArtifact } from "../api/client";
  import { formatBytes } from "../format";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  import ArtifactPreview from "./ArtifactPreview.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let selected = $state<OutputArtifact | null>(null);
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
        <a
          href={store.currentRun ? store.api.artifactUrl(store.currentRun, artifact.path) : "#"}
          target="_blank"
          rel="noopener"
        >{messages.open}</a>
      </div>
    </article>
  {/each}
</div>
<ArtifactPreview {store} artifact={selected} {messages} onClose={() => selected = null} />
