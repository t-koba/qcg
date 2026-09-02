import { describe, expect, it } from "vitest";
import { normalizeMime, normalizePreviewMode, previewMatchesMime, PreviewTooLargeError, readBoundedBlob, resolvePreviewKind } from "./preview";

describe("artifact preview selection", () => {
  it("normalizes MIME parameters and unknown preview values", () => {
    expect(normalizeMime("Application/JSON; charset=utf-8")).toBe("application/json");
    expect(normalizePreviewMode("unsafe-script")).toBe("invalid");
    expect(normalizePreviewMode(undefined)).toBe("auto");
    expect(normalizePreviewMode(null)).toBe("auto");
    expect(normalizePreviewMode("pdf")).toBe("pdf");
  });

  it("selects safe renderers from MIME and extension", () => {
    expect(resolvePreviewKind("auto", "application/json; charset=utf-8", "result.bin")).toBe("json");
    expect(resolvePreviewKind("auto", "text/plain", "result.md")).toBe("markdown");
    expect(resolvePreviewKind("auto", "application/pdf", "result.pdf")).toBe("pdf");
    expect(resolvePreviewKind("auto", "audio/ogg", "result.ogg")).toBe("audio");
    expect(resolvePreviewKind("auto", "video/mp4", "result.mp4")).toBe("video");
    expect(resolvePreviewKind("auto", "text/html", "result.html")).toBe("html");
    expect(resolvePreviewKind("none", "text/plain", "result.txt")).toBe("");
  });

  it("requires compatible MIME for explicit media and document modes", () => {
    expect(previewMatchesMime("image", "image/png")).toBe(true);
    expect(previewMatchesMime("image", "text/plain")).toBe(false);
    expect(previewMatchesMime("pdf", "application/pdf")).toBe(true);
    expect(previewMatchesMime("pdf", "application/json")).toBe(false);
    expect(previewMatchesMime("markdown", "text/markdown; charset=utf-8")).toBe(true);
  });

  it("stops reading streamed previews at the byte limit", async () => {
    const response = new Response(new Blob(["0123456789"]), { headers: { "content-type": "text/plain" } });
    await expect(readBoundedBlob(response, 5)).rejects.toBeInstanceOf(PreviewTooLargeError);
    const bounded = await readBoundedBlob(new Response("ok"), 2);
    expect(await bounded.text()).toBe("ok");
    expect((await readBoundedBlob(new Response(null), 2)).size).toBe(0);
  });
});
