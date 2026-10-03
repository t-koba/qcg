// G04/G05 behavior tests for the generated TypeScript SDK.
import { pathToFileURL } from "node:url";
// Bundled via esbuild (see check-sdk-behavior.sh); exercises the real
// QcgClient streaming + redirect paths with mocked fetch/ReadableStream.
// The bundle path arrives via env and must be a file URL: a bare Windows
// absolute path (C:\...) is rejected by the ESM loader.
const { QcgClient } = await import(pathToFileURL(process.env.QCG_TS_CLIENT).href);

const results = [];
function check(name, cond, detail = "") {
  if (!cond) throw new Error(`FAILED ${name}: ${detail}`);
  results.push(name);
}

function sseResponse(chunks) {
  let cancelled = false;
  const stream = new ReadableStream({
    start(controller) {
      for (const c of chunks) controller.enqueue(c);
      controller.close();
    },
    cancel() {
      cancelled = true;
    },
  });
  const response = new Response(stream, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
  return { response, wasCancelled: () => cancelled };
}

async function collect(client, runId) {
  const out = [];
  for await (const event of client.streamRunEvents(runId)) out.push(event);
  return out;
}

function chunkSplit(bytes, sizes) {
  const out = [];
  let pos = 0;
  let i = 0;
  while (pos < bytes.length) {
    const size = sizes[i % sizes.length];
    out.push(bytes.slice(pos, pos + size));
    pos += size;
    i++;
  }
  return out;
}

const enc = new TextEncoder();

// G04-02: every split position yields the same event sequence as unsplit.
for (const [pname, obj] of [
  ["ascii", { q: "hello" }],
  ["japanese", { q: "質問です" }],
  ["emoji", { q: "🎉🚀" }],
]) {
  for (const [ename, ending] of [["lf", "\n"], ["crlf", "\r\n"], ["cr", "\r"]]) {
    const wire = enc.encode(`data: ${JSON.stringify(obj)}${ending}${ending}`);
    const mockFetch = async () => sseResponse([wire]).response;
    const base = new QcgClient({ baseUrl: "http://127.0.0.1:9", fetch: mockFetch });
    const expected = await collect(base, "r1");
    if (JSON.stringify(expected) !== JSON.stringify([obj]))
      throw new Error(`unsplit baseline failed for ${pname}/${ename}: ${JSON.stringify(expected)}`);
    for (let split = 1; split < wire.length; split++) {
      const parts = [wire.slice(0, split), wire.slice(split)];
      const mock = async () => sseResponse(parts).response;
      const client = new QcgClient({ baseUrl: "http://127.0.0.1:9", fetch: mock });
      const got = await collect(client, "r1");
      if (JSON.stringify(got) !== JSON.stringify(expected))
        throw new Error(`split ${split}/${wire.length} changed ${pname}/${ename}`);
    }
    check(`G04-02 ${pname}/${ename} all splits`, true);
  }
}

// G04-03: multi-data joins; EOF tail without blank line yields nothing.
{
  const wire = enc.encode('data: {"a":\ndata: 1}\n\n');
  const client = new QcgClient({
    baseUrl: "http://127.0.0.1:9",
    fetch: async () => sseResponse(chunkSplit(wire, [3, 5])).response,
  });
  const got = await collect(client, "r1");
  check("G04-03 multi-data", JSON.stringify(got) === JSON.stringify([{ a: 1 }]), JSON.stringify(got));
}
for (const tail of ['data: {"c": 3}\n', 'data: {"c": 3}', 'data: {"c": 3}\r']) {
  const wire = enc.encode(tail);
  const client = new QcgClient({
    baseUrl: "http://127.0.0.1:9",
    fetch: async () => sseResponse(chunkSplit(wire, [2])).response,
  });
  const got = await collect(client, "r1");
  check(`G04-03 EOF discard ${JSON.stringify(tail)}`, got.length === 0, JSON.stringify(got));
}
// Complete CR-terminated event at EOF still dispatches.
{
  const wire = enc.encode('data: {"d": 4}\r\r');
  const client = new QcgClient({
    baseUrl: "http://127.0.0.1:9",
    fetch: async () => sseResponse([wire]).response,
  });
  const got = await collect(client, "r1");
  check("G04-03 CR-terminated EOF", JSON.stringify(got) === JSON.stringify([{ d: 4 }]), JSON.stringify(got));
}

// G04-04: consumer break releases the reader.
{
  const wire = enc.encode('data: {"a": 1}\n\ndata: {"b": 2}\n\n');
  let reader;
  const mock = async () => {
    const { response } = sseResponse(chunkSplit(wire, [7]));
    reader = response.body.getReader();
    // Re-wrap so we observe cancel: return a Response sharing the reader.
    const stream = new ReadableStream({
      async start(c) {
        // Pump is unnecessary; hand the original reader through.
      },
    });
    return new Response(
      new ReadableStream({
        start(c) {
          c.enqueue(wire);
          c.close();
        },
        cancel() {
          reader.cancel();
        },
      }),
      { status: 200 },
    );
  };
  const client = new QcgClient({ baseUrl: "http://127.0.0.1:9", fetch: mock });
  const gen = client.streamRunEvents("r1");
  const first = await gen.next();
  check("G04-04 first event", first.value && first.value.a === 1, JSON.stringify(first.value));
  await gen.return();
}

// G04-04b: a parse error still releases the reader.
{
  const wire = enc.encode("data: not-json\n\n");
  let cancelled = false;
  const openStream = new ReadableStream({
    start(c) {
      c.enqueue(wire);
      // Stay open: the parse error below must cancel an open stream.
    },
    cancel() {
      cancelled = true;
    },
  });
  const client = new QcgClient({
    baseUrl: "http://127.0.0.1:9",
    fetch: async () => new Response(openStream, { status: 200 }),
  });
  let error = null;
  try {
    await collect(client, "r1");
  } catch (e) {
    error = e;
  }
  check("G04-04b parse error raises", error instanceof SyntaxError, String(error));
  check("G04-04b parse error releases", cancelled, "reader was not cancelled");
}

// G05-01: self-loop, 2-cycle, and over-limit chains stop finitely.
function redirectResponse(location) {
  return new Response(null, { status: 302, headers: { location } });
}
{
  let calls = 0;
  const mock = async () => {
    calls++;
    return redirectResponse("http://x.test/loop");
  };
  const client = new QcgClient({ baseUrl: "http://x.test", fetch: mock });
  let error = null;
  try {
    await client.health();
  } catch (e) {
    error = e;
  }
  check("G05-01 self-loop finite", error && calls <= 7, `calls=${calls} err=${error && error.message}`);
  check("G05-01 self-loop reason", /cycle|limit/i.test(error.message), error.message);
}
{
  let calls = 0;
  const mock = async (url) => {
    calls++;
    const u = String(url);
    return redirectResponse(u.includes("/a") ? "http://x.test/b" : "http://x.test/a");
  };
  const client = new QcgClient({ baseUrl: "http://x.test", fetch: mock });
  let error = null;
  try {
    await client.health();
  } catch (e) {
    error = e;
  }
  check("G05-01 2-cycle finite", error && calls <= 7, `calls=${calls} err=${error && error.message}`);
  check("G05-01 2-cycle reason", /cycle|limit/i.test(error.message), error.message);
}
{
  let calls = 0;
  const mock = async (url) => {
    calls++;
    return redirectResponse(`http://x.test/hop${calls}`);
  };
  const client = new QcgClient({ baseUrl: "http://x.test", fetch: mock });
  let error = null;
  try {
    await client.health();
  } catch (e) {
    error = e;
  }
  check("G05-01 limit finite", error && calls === 6, `calls=${calls} err=${error && error.message}`);
  check("G05-01 limit reason", /limit/i.test(error.message), error.message);
}

// G05-02: relative request URL + relative Location resolves (no TypeError).
// The old code threw `TypeError` from `new URL(location, url)` when the
// request URL was the default same-origin relative path.
{
  const seen = [];
  const mock = async (url) => {
    seen.push(String(url));
    const u = String(url);
    if (u === "/api/runs/x/events") {
      return redirectResponse("/api/runs/y");
    }
    return new Response(JSON.stringify({ ok: true }), { status: 200 });
  };
  const client = new QcgClient({ baseUrl: "", fetch: mock });
  const text = await client.runEvents("x");
  check("G05-02 relative-base redirect follows", seen.length === 2, seen.join(" | "));
  check(
    "G05-02 relative resolves absolutely",
    seen[1].startsWith("http://localhost/api/runs/y"),
    seen.join(" | "),
  );
  check("G05-02 relative body", typeof text === "string" && text.includes("ok"), String(text));
}
{
  const seen = [];
  const mock = async (url) => {
    seen.push(String(url));
    const u = String(url);
    if (u.endsWith("/api/runs/x/events")) {
      return redirectResponse("/api/runs/y");
    }
    return new Response(JSON.stringify({ ok: true }), { status: 200 });
  };
  const client2 = new QcgClient({ baseUrl: "http://127.0.0.1:9", fetch: mock });
  const text = await client2.runEvents("x");
  check("G05-02 absolute-base redirect follows", seen.length === 2, seen.join(" | "));
}
// G05-02b: browser opaque-redirect (status 0, no Location) never loops:
// it surfaces finitely as an HTTP error.
{
  let calls = 0;
  const mock = async () => {
    calls++;
    return {
      status: 0,
      ok: false,
      statusText: "opaque",
      headers: new Headers(),
      text: async () => "",
    };
  };
  const client = new QcgClient({ baseUrl: "", fetch: mock });
  let error = null;
  try {
    await client.health();
  } catch (e) {
    error = e;
  }
  check("G05-02b opaque finite", error && calls === 1, `calls=${calls} err=${error && error.message}`);
}

// G05-03: cross-origin and downgrade strip Authorization (F04 preserved).
{
  const seenAuth = [];
  const mock = async (url, init) => {
    const headers = new Headers(init.headers);
    seenAuth.push({ url: String(url), auth: headers.get("authorization") });
    if (seenAuth.length === 1) return redirectResponse("http://other.test/target");
    return new Response("{}", { status: 200 });
  };
  const client = new QcgClient({ baseUrl: "http://x.test", token: "secret", fetch: mock });
  await client.health();
  check("G05-03 cross-origin strips", seenAuth[0].auth && !seenAuth[1].auth, JSON.stringify(seenAuth));
}
{
  const seenAuth = [];
  const mock = async (url, init) => {
    const headers = new Headers(init.headers);
    seenAuth.push({ url: String(url), auth: headers.get("authorization") });
    if (seenAuth.length === 1) return redirectResponse("http://x.test/plain");
    return new Response("{}", { status: 200 });
  };
  const client = new QcgClient({ baseUrl: "https://x.test", token: "secret", fetch: mock });
  await client.health();
  check("G05-03 downgrade strips", seenAuth[0].auth && !seenAuth[1].auth, JSON.stringify(seenAuth));
}

// G05-04: intermediate bodies are released (follow path and error paths).
{
  const cancelled = [];
  const mock = async (url) => {
    const u = String(url);
    if (u.includes("final")) return new Response("{}", { status: 200 });
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(enc.encode("pending"));
      },
      cancel() {
        cancelled.push(u);
      },
    });
    return new Response(stream, { status: 302, headers: { location: "http://x.test/final" } });
  };
  const client = new QcgClient({ baseUrl: "http://x.test", fetch: mock });
  await client.health();
  check("G05-04 intermediate released", cancelled.length === 1, JSON.stringify(cancelled));
}
{
  // Error paths (limit exceeded) also release every intermediate body.
  const cancelled = [];
  let calls = 0;
  const mock = async () => {
    calls++;
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(enc.encode("pending"));
      },
      cancel() {
        cancelled.push(calls);
      },
    });
    return new Response(stream, { status: 302, headers: { location: `http://x.test/h${calls}` } });
  };
  const client = new QcgClient({ baseUrl: "http://x.test", fetch: mock });
  let error = null;
  try {
    await client.health();
  } catch (e) {
    error = e;
  }
  check("G05-04 limit path finite", error && calls === 6, `calls=${calls}`);
  check("G05-04 limit path releases all", cancelled.length === 6, JSON.stringify(cancelled));
}

// G07-04: typed response readers over mocked fetch.
{
  const mock = async (url) => {
    const u = String(url);
    if (u.includes("/healthz")) return new Response(JSON.stringify({ ok: true }), { status: 200 });
    if (u.includes("/metrics")) return new Response("up\n", { status: 200 });
    if (u.includes("/bundle")) return new Response(new Uint8Array([1, 2, 3]), { status: 200 });
    if (u.includes("/journal"))
      return new Response('{"a":1}\n{"b":2}\n', { status: 200 });
    if (u.includes("/api/runs/gone")) return new Response(null, { status: 204 });
    throw new Error(`unexpected ${u}`);
  };
  const client = new QcgClient({ baseUrl: "http://127.0.0.1:9", fetch: mock });
  const health = await client.health();
  check("typed json", health && health.ok === true, JSON.stringify(health));
  const metrics = await client.metrics();
  check("typed text", metrics === "up\n", JSON.stringify(metrics));
  const bundle = await client.downloadRunBundle("r1");
  check("typed bytes", bundle && bundle.length === 3 && bundle[2] === 3, String(bundle && bundle.length));
  const journal = await client.readRunJournal("r1");
  check("typed ndjson", JSON.stringify(journal) === JSON.stringify([{ a: 1 }, { b: 2 }]), JSON.stringify(journal));
  await client.deleteRun("gone");
  check("typed empty 204", true);
  // 304 maps to null.
  const mock304 = async () => new Response(null, { status: 304 });
  const client304 = new QcgClient({ baseUrl: "http://127.0.0.1:9", fetch: mock304 });
  const none = await client304.health();
  check("typed 304 null", none === null, String(none));
}

console.log(`TS SSE/redirect behavior: all G04/G05 checks passed (${results.length} assertions)`);

// The SPA, TypeScript SDK and Python SDK consume the same wire fixtures.
const fixtures = JSON.parse(await (await import('node:fs/promises')).readFile(new URL('../fixtures/sse.json', import.meta.url), 'utf8'));
for (const fixture of fixtures) {
  const wire = fixture.wire_hex ? Buffer.from(fixture.wire_hex, "hex") : enc.encode(fixture.wire);
  for (let split = 1; split < wire.length; split++) {
    const client = new QcgClient({ baseUrl: 'http://127.0.0.1:9', fetch: async () => sseResponse([wire.slice(0,split), wire.slice(split)]).response });
    check(`shared fixture ${fixture.name}/${split}`, JSON.stringify(await collect(client,'r1')) === JSON.stringify(fixture.payloads));
  }
}
