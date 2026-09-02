<script lang="ts">
  import { untrack } from "svelte";
  import { errorMessage, type OutputArtifact } from "../api/client";
  import { MAX_PREVIEW_BYTES, normalizeMime, normalizePreviewMode, previewMatchesMime, PreviewTooLargeError, readBoundedBlob, resolvePreviewKind, type PreviewKind } from "../preview";
  import type { Messages } from "../messages";
  import type { RunStore } from "../run-store.svelte";

  let {
    store,
    artifact,
    messages,
    onClose,
  }: {
    store: RunStore;
    artifact: OutputArtifact | null;
    messages: Messages;
    onClose: () => void;
  } = $props();

  let url = $state("");
  let text = $state("");
  let mime = $state("");
  let kind = $state<PreviewKind>("");
  let loading = $state(false);
  let previewError = $state("");
  let loadId = 0;
  let controller: AbortController | null = null;
  const maxPreviewBytes = MAX_PREVIEW_BYTES;

  $effect(() => {
    const selected = artifact;
    const runId = store.currentRun;
    const currentLoadId = ++loadId;
    controller?.abort();
    controller = null;
    untrack(reset);
    if (!selected || !runId || normalizePreviewMode(selected.preview) === "none") return;
    void load(store.api.artifactUrl(runId, selected.path), selected, currentLoadId);
    return () => {
      if (currentLoadId === loadId) {
        controller?.abort();
        controller = null;
        untrack(reset);
      }
    };
  });

  async function load(artifactUrl: string, selected: OutputArtifact, currentLoadId: number): Promise<void> {
    loading = true;
    const requestController = new AbortController();
    controller = requestController;
    try {
      const requested = normalizePreviewMode(selected.preview);
      if (requested === "invalid") throw new Error(messages.previewInvalidMetadata);
      if (selected.bytes > maxPreviewBytes) throw new Error(messages.previewTooLarge);
      const response = await fetch(artifactUrl, { signal: requestController.signal });
      if (!response.ok) throw new Error(await errorMessage(response));
      const responseContentType = normalizeMime(response.headers.get("content-type"));
      const declaredLength = Number(response.headers.get("content-length"));
      if (Number.isFinite(declaredLength) && declaredLength > maxPreviewBytes) {
        requestController.abort();
        try {
          await response.body?.cancel();
        } catch {
          // The deterministic size error is more useful than a transport cancellation error.
        }
        throw new Error(messages.previewTooLarge);
      }
      let blob: Blob;
      try {
        blob = await readBoundedBlob(response, maxPreviewBytes);
      } catch (error) {
        if (error instanceof PreviewTooLargeError) throw new Error(messages.previewTooLarge);
        throw error;
      }
      if (currentLoadId !== loadId) return;
      if (blob.size > maxPreviewBytes) throw new Error(messages.previewTooLarge);
      const contentType = responseContentType || normalizeMime(selected.mime) || normalizeMime(blob.type);
      const resolved = resolvePreviewKind(requested, contentType, selected.path);
      if (!resolved) throw new Error(messages.previewUnavailable);
      if (requested !== "auto" && !previewMatchesMime(resolved, contentType)) throw new Error(messages.previewMimeMismatch);
      mime = contentType;
      kind = resolved;
      if (resolved === "text" || resolved === "json" || resolved === "markdown") {
        const body = await blob.text();
        if (currentLoadId !== loadId) return;
        text = body;
      } else {
        url = URL.createObjectURL(blob);
      }
    } catch (error) {
      if (requestController.signal.aborted || currentLoadId !== loadId) return;
      previewError = error instanceof Error ? error.message : String(error);
    } finally {
      if (currentLoadId === loadId) loading = false;
    }
  }

  function reset(): void {
    if (url) URL.revokeObjectURL(url);
    url = "";
    text = "";
    mime = "";
    kind = "";
    loading = false;
    previewError = "";
  }

</script>

{#if artifact}
  <section id="artifact-preview" class="artifact-preview" aria-label={artifact.label || artifact.path} aria-busy={loading}>
    <header>
      <div><strong>{artifact.label || artifact.path}</strong>{#if artifact.description}<p>{artifact.description}</p>{/if}<small>{mime || artifact.mime || ""}</small></div>
      <button type="button" aria-label={messages.close} onclick={onClose}>×</button>
    </header>
    {#if loading}<div class="preview-loading" aria-busy="true"><span class="spinner"></span></div>
    {:else if previewError}<p class="preview-error" role="alert">{previewError}</p>
    {:else if kind === "image"}<img alt={artifact.label || artifact.path} src={url} />
    {:else if kind === "text"}<pre aria-label={artifact.label || artifact.path}>{text}</pre>
    {:else if kind === "json"}<pre class="json-preview" aria-label={artifact.label || artifact.path}>{text}</pre>
    {:else if kind === "markdown"}<pre class="markdown-preview" aria-label={artifact.label || artifact.path}>{text}</pre>
    {:else if kind === "html"}<iframe sandbox="" referrerpolicy="no-referrer" src={url} title={artifact.label || artifact.path}></iframe>
    {:else if kind === "pdf"}<iframe sandbox="" referrerpolicy="no-referrer" src={url} title={artifact.label || artifact.path}></iframe>
    {:else if kind === "audio"}<audio controls preload="metadata" src={url} aria-label={artifact.label || artifact.path}></audio>
    {:else if kind === "video"}
      <!-- svelte-ignore a11y_media_has_caption: artifact outputs do not expose a separate caption track in the contract. -->
      <video controls preload="metadata" src={url} aria-label={artifact.label || artifact.path}></video>
    {/if}
  </section>
{/if}
