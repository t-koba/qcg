import { describe, expect, it } from "vitest";
import { parseSseFrames } from "./sse";

describe("parseSseFrames", () => {
  it("splits complete frames and keeps the partial tail", () => {
    const { frames, rest } = parseSseFrames('data: {"seq":1}\n\ndata: {"seq":2}\n\ndata: {"seq"');
    expect(frames.map((frame) => frame.data)).toEqual(['{"seq":1}', '{"seq":2}']);
    expect(rest).toBe('data: {"seq"');
  });

  it("reads multi-line data fields and ids", () => {
    const { frames } = parseSseFrames("id: 7\ndata: first\ndata: second\n\n");
    expect(frames).toEqual([{ id: "7", data: "firstsecond" }]);
  });

  it("ignores comments and frames without data", () => {
    const { frames } = parseSseFrames(": keep-alive\n\nid: 3\n\n");
    expect(frames).toEqual([]);
  });
});
