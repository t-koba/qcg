//! Server-sent event frame parsing for fetch-based streams.
//!
//! The bundled UI reads SSE from a fetch body so it can send an
//! `Authorization` header, which EventSource cannot. Only `data` and `id`
//! fields are used by qcg; comments and other fields are ignored.

export type SseFrame = { data: string; id?: string };

/**
 * Splits complete SSE frames out of a decoded buffer and returns the
 * trailing partial frame. A frame without a `data` field is not an event.
 */
export function parseSseFrames(buffer: string): { frames: SseFrame[]; rest: string } {
  const frames: SseFrame[] = [];
  let rest = buffer;
  while (true) {
    const boundary = rest.indexOf("\n\n");
    if (boundary < 0) break;
    const raw = rest.slice(0, boundary);
    rest = rest.slice(boundary + 2);
    let data = "";
    let id: string | undefined;
    for (const line of raw.split("\n")) {
      if (line.startsWith("data:")) data += line.slice(5).trimStart();
      else if (line.startsWith("id:")) id = line.slice(3).trim();
    }
    if (data) frames.push({ data, id });
  }
  return { frames, rest };
}
