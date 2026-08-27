<script lang="ts">
  import type { OutputArtifact } from "../api/client";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";
  import ArtifactPreview from "./ArtifactPreview.svelte";
  let { store, messages }: { store: RunStore; messages: Messages } = $props();
  let selected = $state<OutputArtifact | null>(null);
</script>

<div class="panel-heading"><div><h2>{messages.artifacts}</h2><p>{store.artifacts.length ? `${store.artifacts.length} artifact(s)` : ""}</p></div></div>
<div id="artifact-list" class="artifact-list">
  {#each store.artifacts as artifact}
    <div class="artifact">
      <div><strong>{artifact.label || artifact.path}</strong><div class="hint">{artifact.path} · {artifact.bytes} bytes</div></div>
      <div class="artifact-actions"><button type="button" onclick={() => selected = artifact}>{messages.preview}</button><a href={store.currentRun ? store.api.artifactUrl(store.currentRun, artifact.path) : "#"}>{messages.open}</a></div>
    </div>
  {/each}
</div>
<ArtifactPreview {store} artifact={selected} />
