/** Shared SSE decoder. The SDK generator embeds this source verbatim. */
export type SseFrame = { data: string; id?: string; event?: string };

export class SseParser {
  #line = "";
  #data: string[] = [];
  #id: string | undefined;
  #event: string | undefined;
  #skipLf = false;
  #started = false;
  readonly maxFrameChars = 16 * 1024 * 1024;
  #size = 0;

  push(piece: string): SseFrame[] {
    const frames: SseFrame[] = [];
    for (const char of piece) {
      if (!this.#started) {
        this.#started = true;
        if (char === "\uFEFF") continue;
      }
      if (this.#skipLf) {
        this.#skipLf = false;
        if (char === "\n") continue;
      }
      if (char === "\r" || char === "\n") {
        this.#skipLf = char === "\r";
        const line = this.#line;
        this.#line = "";
        if (line === "") {
          if (this.#data.length) frames.push({ data: this.#data.join("\n"), id: this.#id, event: this.#event });
          this.#data = [];
          this.#event = undefined;
          this.#size = 0;
        } else if (!line.startsWith(":")) {
          const colon = line.indexOf(":");
          const field = colon < 0 ? line : line.slice(0, colon);
          let value = colon < 0 ? "" : line.slice(colon + 1);
          if (value.startsWith(" ")) value = value.slice(1);
          if (field === "data") this.#data.push(value);
          else if (field === "id" && !value.includes("\0")) this.#id = value;
          else if (field === "event") this.#event = value;
        }
      } else {
        this.#line += char;
        if (++this.#size > this.maxFrameChars) throw new Error("SSE frame exceeds the size limit");
      }
    }
    return frames;
  }
}

/** Convenience parser for complete buffers; streaming callers retain SseParser. */
export function parseSseFrames(buffer: string): { frames: SseFrame[]; rest: string } {
  const parser = new SseParser();
  const frames = parser.push(buffer);
  let end = 0;
  let previousBreak = false;
  for (let index = 0; index < buffer.length; index++) {
    if (buffer[index] === "\r" || buffer[index] === "\n") {
      if (buffer[index] === "\r" && buffer[index + 1] === "\n") index++;
      if (previousBreak) end = index + 1;
      previousBreak = true;
    } else previousBreak = false;
  }
  return { frames, rest: buffer.slice(end) };
}
