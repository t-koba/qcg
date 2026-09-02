export type PreviewKind = "image" | "text" | "json" | "markdown" | "html" | "pdf" | "audio" | "video" | "";
export type PreviewMode = PreviewKind | "auto" | "none" | "invalid";

export const MAX_PREVIEW_BYTES = 16 * 1024 * 1024;

export class PreviewTooLargeError extends Error {
  constructor() {
    super("preview body exceeds the configured byte limit");
    this.name = "PreviewTooLargeError";
  }
}

const TEXT_MIME_TYPES = new Set([
  "application/javascript",
  "application/ld+json",
  "application/json",
  "application/xml",
  "application/toml",
  "application/yaml",
  "text/css",
  "text/csv",
  "text/html",
  "text/javascript",
  "text/markdown",
  "text/plain",
  "text/xml",
]);

export function normalizeMime(mime: string | null | undefined): string {
  return (mime || "").split(";", 1)[0].trim().toLowerCase();
}

export function normalizePreviewMode(mode: unknown): PreviewMode {
  if (mode === undefined || mode === null) return "auto";
  if (typeof mode === "string" && ["auto", "none", "image", "text", "json", "markdown", "html", "pdf", "audio", "video"].includes(mode)) {
    return mode as PreviewMode;
  }
  return "invalid";
}

export function resolvePreviewKind(mode: unknown, mime: string | null | undefined, path: string): PreviewKind {
  const requested = normalizePreviewMode(mode);
  if (requested === "none" || requested === "invalid") return "";
  const normalizedMime = normalizeMime(mime);
  if (requested !== "auto") return requested;
  if (normalizedMime.startsWith("image/")) return "image";
  if (normalizedMime.startsWith("audio/")) return "audio";
  if (normalizedMime.startsWith("video/")) return "video";
  if (normalizedMime === "application/pdf" || extension(path) === "pdf") return "pdf";
  if (normalizedMime === "text/html" || normalizedMime === "application/xhtml+xml" || extension(path) === "html" || extension(path) === "htm") return "html";
  if (normalizedMime === "application/json" || normalizedMime.endsWith("+json") || extension(path) === "json" || extension(path) === "jsonl" || extension(path) === "ndjson") return "json";
  if (normalizedMime === "text/markdown" || normalizedMime === "text/x-markdown" || ["md", "markdown", "mdown", "mkdn"].includes(extension(path))) return "markdown";
  if (normalizedMime.startsWith("text/") || TEXT_MIME_TYPES.has(normalizedMime) || normalizedMime.endsWith("+xml")) return "text";
  return "";
}

export async function readBoundedBlob(response: Response, maxBytes = MAX_PREVIEW_BYTES): Promise<Blob> {
  const body = response.body;
  if (!body) {
    // A null body represents an empty response. Do not call response.blob here:
    // it would make an unbounded allocation for a body-less test double with a
    // misleading Content-Length header.
    return new Blob([], { type: normalizeMime(response.headers.get("content-type")) || undefined });
  }
  const reader = body.getReader();
  const chunks: ArrayBuffer[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      total += value.byteLength;
      if (total > maxBytes) {
        try {
          await reader.cancel();
        } catch {
          // Preserve the deterministic size error even when the transport has
          // already closed the stream.
        }
        throw new PreviewTooLargeError();
      }
      const copy = new ArrayBuffer(value.byteLength);
      new Uint8Array(copy).set(value);
      chunks.push(copy);
    }
  } finally {
    reader.releaseLock();
  }
  return new Blob(chunks, { type: normalizeMime(response.headers.get("content-type")) || undefined });
}

export function previewMatchesMime(kind: PreviewKind, mime: string | null | undefined): boolean {
  const normalizedMime = normalizeMime(mime);
  if (!normalizedMime || !kind) return true;
  if (kind === "image") return normalizedMime.startsWith("image/");
  if (kind === "audio") return normalizedMime.startsWith("audio/");
  if (kind === "video") return normalizedMime.startsWith("video/");
  if (kind === "pdf") return normalizedMime === "application/pdf";
  if (kind === "html") return normalizedMime === "text/html" || normalizedMime === "application/xhtml+xml";
  if (kind === "json") return normalizedMime === "application/json" || normalizedMime.endsWith("+json") || normalizedMime === "text/json";
  if (kind === "markdown") return normalizedMime === "text/markdown" || normalizedMime === "text/x-markdown" || normalizedMime.startsWith("text/");
  if (kind === "text") return normalizedMime.startsWith("text/") || TEXT_MIME_TYPES.has(normalizedMime) || normalizedMime.endsWith("+xml") || normalizedMime.endsWith("+json");
  return false;
}

function extension(path: string): string {
  const basename = path.split(/[\\/]/).pop() || "";
  const dot = basename.lastIndexOf(".");
  return dot >= 0 ? basename.slice(dot + 1).toLowerCase() : "";
}
