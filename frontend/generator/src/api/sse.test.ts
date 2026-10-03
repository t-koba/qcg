import { describe, expect, it } from "vitest";
import { SseParser, parseSseFrames } from "./sse";

describe("parseSseFrames", () => {
  it("splits complete frames and keeps the partial tail", () => {
    const { frames, rest } = parseSseFrames('data: {"seq":1}\n\ndata: {"seq":2}\n\ndata: {"seq"');
    expect(frames.map((frame) => frame.data)).toEqual(['{"seq":1}', '{"seq":2}']);
    expect(rest).toBe('data: {"seq"');
  });

  it("reads multi-line data fields and ids", () => {
    const { frames } = parseSseFrames("id: 7\ndata: first\ndata: second\n\n");
    expect(frames).toEqual([{ id: "7", data: "first\nsecond", event: undefined }]);
  });

  it("ignores comments and frames without data", () => {
    const { frames } = parseSseFrames(": keep-alive\n\nid: 3\n\n");
    expect(frames).toEqual([]);
  });
});

it("preserves whitespace, empty data, BOM and split CRLF at every boundary", () => {
  const input = "\uFEFFid: 8\r\ndata:  first\r\ndata: second\r\n\r\ndata:\r\r";
  for (let split = 0; split <= input.length; split++) {
    const parser = new SseParser();
    expect([...parser.push(input.slice(0, split)), ...parser.push(input.slice(split))])
      .toEqual([{id: "8", data: " first\nsecond", event: undefined}, {id: "8", data: "", event: undefined}]);
  }
});

import fixtures from '../../../../scripts/fixtures/sse.json';
it('shared SPA and SDK conformance fixtures preserve every byte split', () => {
  for (const fixture of fixtures) {
    const bytes = fixture.wire_hex ? Uint8Array.from(fixture.wire_hex.match(/../g)!, byte => parseInt(byte, 16)) : new TextEncoder().encode(fixture.wire);
    for (let split = 1; split < bytes.length; split++) {
      const parser = new SseParser();
      const decoder = new TextDecoder("utf-8", { ignoreBOM: true });
      const frames = [...parser.push(decoder.decode(bytes.slice(0, split), {stream:true})), ...parser.push(decoder.decode(bytes.slice(split), {stream:true}))];
      expect(frames.filter(frame => frame.data).map(frame => JSON.parse(frame.data))).toEqual(fixture.payloads);
    }
  }
});
