<script lang="ts">
  import { untrack } from "svelte";
  import { errorMessage, type OutputArtifact } from "../api/client";
  import type { RunStore } from "../run-store.svelte";
  let { store, artifact }: { store: RunStore; artifact: OutputArtifact | null } = $props();
  let url = $state("");
  let text = $state("");
  let kind = $state<"image" | "text" | "frame" | "">("");
  let loadId = 0;

  $effect(() => {
    const selected = artifact;
    const runId = store.currentRun;
    const currentLoadId = ++loadId;
    untrack(reset);
    if (!selected || !runId) return;
    void load(store.api.artifactUrl(runId, selected.path), currentLoadId);
    return () => {
      if (currentLoadId === loadId) untrack(reset);
    };
  });

  async function load(artifactUrl: string, currentLoadId: number): Promise<void> {
    const response = await fetch(artifactUrl);
    if (!response.ok) throw new Error(await errorMessage(response));
    const blob = await response.blob();
    if (currentLoadId !== loadId) return;
    const contentType = response.headers.get("content-type")?.split(";", 1)[0] || blob.type;
    kind = contentType.startsWith("image/")
      ? "image"
      : contentType === "text/html" || contentType === "application/xhtml+xml"
        ? "frame"
        : contentType.startsWith("text/") || contentType === "application/json"
          ? "text"
          : "frame";
    if (kind === "text") text = await blob.text();
    else url = URL.createObjectURL(blob);
  }

  function reset(): void {
    if (url) URL.revokeObjectURL(url);
    url = "";
    text = "";
    kind = "";
  }
</script>

{#if artifact && kind}
  <div id="artifact-preview" class="artifact-preview">
    <h3>{artifact.label || artifact.path}</h3>
    {#if kind === "image"}<img alt={artifact.label || artifact.path} src={url} />
    {:else if kind === "text"}<pre>{text}</pre>
    {:else}<iframe sandbox="" referrerpolicy="no-referrer" src={url} title={artifact.label || artifact.path}></iframe>{/if}
  </div>
{/if}
