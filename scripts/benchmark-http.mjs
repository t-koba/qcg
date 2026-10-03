// Deterministic Linux HTTP/SSE load matrix. No external model is involved.
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { once } from "node:events";

if (process.platform !== "linux")
  throw new Error("process counters require Linux /proc");
const root = await mkdtemp(join(tmpdir(), "qcg-http-bench-"));
const report = resolve(
  process.env.QCG_HTTP_BENCHMARK_REPORT ?? "/tmp/qcg-http-benchmark.json",
);
const rows = [];
const listOnly = process.env.QCG_BENCHMARK_MODE === "list";
let server;
const percentile = (values, p) =>
  [...values].sort((a, b) => a - b)[
    Math.min(values.length - 1, Math.floor(values.length * p))
  ];
const common = (id, seq, t) => ({
  run_id: id,
  seq,
  t,
  ts: "2026-10-02T00:00:00Z",
  trace_id: "a".repeat(32),
  span_id: seq.toString(16).padStart(16, "0"),
});
const identity = (id) => ({
  ...common(id, 1, "run_started"),
  generator: "bench",
  generator_path: "bench",
  contract_sha256: "a".repeat(64),
  inputs: {},
  resource_hashes: [],
  schema_version: 1,
});
const line = (value) => JSON.stringify(value) + "\n";
async function journal(path, id, mib) {
  let seq = 2;
  const chunks = [line(identity(id))];
  let bytes = Buffer.byteLength(chunks[0]);
  const finish = () =>
    line({
      ...common(id, seq, "run_finished"),
      status: "success",
      metrics: {},
    });
  if (mib) {
    const size = mib * 1024 * 1024;
    while (bytes + Buffer.byteLength(finish()) < size) {
      const empty = {
        ...common(id, seq, "budget_charged"),
        amount: 0,
        node: "bench",
        padding: "",
      };
      // All seq values stay three digits in this fixture, so the final
      // terminal envelope has stable length while computing the padding.
      const remaining =
        size -
        bytes -
        Buffer.byteLength(
          line({
            ...common(id, seq + 1, "run_finished"),
            status: "success",
            metrics: {},
          }),
        ) -
        Buffer.byteLength(line(empty));
      if (remaining < 0)
        throw new Error("fixture cannot fit terminal envelope");
      const record = line({
        ...empty,
        padding: "x".repeat(Math.min(remaining, 512 * 1024)),
      });
      chunks.push(record);
      bytes += Buffer.byteLength(record);
      seq++;
    }
  }
  chunks.push(finish());
  const contents = chunks.join("");
  if (mib && Buffer.byteLength(contents) !== mib * 1024 * 1024)
    throw new Error("fixture size mismatch");
  await writeFile(path, contents);
}
async function counters() {
  const [io, status, stat] = await Promise.all(
    ["io", "status", "stat"].map((name) =>
      readFile(`/proc/${server.pid}/${name}`, "utf8"),
    ),
  );
  const number = (text, name) =>
    Number(text.match(new RegExp(`^${name}:\\s+(\\d+)`, "m"))?.[1] ?? 0);
  const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
  return {
    rchar: number(io, "rchar"),
    disk_read_bytes: number(io, "read_bytes"),
    cpu_ticks: Number(fields[11]) + Number(fields[12]),
    rss_peak_kib: number(status, "VmHWM"),
  };
}
async function checked(url, options) {
  const response = await fetch(url, {
    ...options,
    signal: AbortSignal.timeout(180000),
  });
  if (!response.ok)
    throw new Error(`${url}: ${response.status} ${await response.text()}`);
  return response;
}
async function snapshot(base) {
  const start = performance.now();
  const value = await (await checked(`${base}/api/runs/bench-00000`)).json();
  if (value.state !== "succeeded")
    throw new Error(`unexpected snapshot ${JSON.stringify(value)}`);
  return performance.now() - start;
}
async function subscription(base) {
  const start = performance.now();
  const response = await checked(`${base}/api/runs/bench-00000/events`);
  let buffer = "",
    count = 0,
    bytes = 0,
    terminal = false;
  const decoder = new TextDecoder();
  for await (const chunk of response.body) {
    bytes += chunk.length;
    buffer += decoder.decode(chunk, { stream: true });
    let boundary;
    while ((boundary = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const data = frame
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).replace(/^ /, ""))
        .join("\n");
      if (!data) continue;
      const event = JSON.parse(data);
      if (event.kind === "stream_error" || event.kind === "lagged")
        throw new Error(data);
      count++;
      terminal ||= event.kind === "run_finished";
    }
  }
  if (!terminal || buffer) throw new Error("stream did not complete cleanly");
  return { ms: performance.now() - start, bytes, events: count };
}
try {
  for (const runs of [100, 1000, 10000]) {
    const directory = join(root, String(runs));
    await mkdir(directory);
    let output = "";
    server = spawn(
      resolve(process.env.QCG_HTTP_BINARY ?? "target/debug/qcg"),
      [
        "serve",
        "--port",
        "0",
        "--runs-dir",
        directory,
        "--max-tracked-runs",
        "10000",
      ],
      {
        stdio: ["ignore", "pipe", "pipe"],
        env: {
          ...process.env,
          READ_CACHE_MAX_BYTES: String((listOnly ? 512 : 256) * 1024 * 1024),
          GC_KEEP: "10001",
          GC_KEEP_FAILED: "10001",
          GC_INTERVAL_SECS: "3600",
        },
      },
    );
    server.stdout.on("data", (data) => (output += data));
    server.stderr.on("data", (data) => (output += data));
    const deadline = performance.now() + 30000;
    let base;
    while (!(base = output.match(/listening on (http:\/\/\S+)/)?.[1])) {
      if (server.exitCode !== null || performance.now() > deadline)
        throw new Error(`server startup failed: ${output}`);
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    // Populate after boot: this measures disk observations, without
    // recovery/admission becoming an uncontrolled second workload.
    for (let index = 0; index < runs; index++) {
      const id = `bench-${String(index).padStart(5, "0")}`;
      const meta = join(directory, id, "meta");
      await mkdir(meta, { recursive: true });
      await journal(join(meta, "journal.jsonl"), id, 0);
    }
    for (const mib of [1, 16, 64]) {
      for (const subscribers of listOnly ? [1] : [1, 8, 64]) {
        await journal(
          join(directory, "bench-00000", "meta", "journal.jsonl"),
          "bench-00000",
          mib,
        );
        if (listOnly) {
          const before = await counters();
          const times = [];
          let warmStart;
          for (let repetition = 0; repetition < 21; repetition++) {
            const start = performance.now();
            const response = await checked(`${base}/api/runs?limit=100`);
            const value = await response.json();
            if (!Array.isArray(value.items) || !value.items.length)
              throw new Error(
                `invalid list response: ${JSON.stringify(value)}`,
              );
            times.push(performance.now() - start);
            if (repetition === 0) warmStart = await counters();
          }
          const after = await counters();
          const warmReadBytes = after.rchar - warmStart.rchar;
          // Requests still contribute to rchar; a cached census must not
          // re-read even the smallest 1 MiB target journal on every refresh.
          if (warmReadBytes >= 1024 * 1024)
            throw new Error(`warm list reread history: ${warmReadBytes} bytes`);
          rows.push({
            runs,
            cache_limit_bytes: 512 * 1024 * 1024,
            journal_mib: mib,
            cold_list_ms: times[0],
            warm_rchar_bytes: warmReadBytes,
            warm_list_p50_ms: percentile(times.slice(1), 0.5),
            warm_list_p95_ms: percentile(times.slice(1), 0.95),
            rchar_bytes: after.rchar - before.rchar,
            disk_read_bytes: after.disk_read_bytes - before.disk_read_bytes,
            cpu_ticks: after.cpu_ticks - before.cpu_ticks,
            process_max_rss_kib: after.rss_peak_kib,
          });
          await writeFile(report, JSON.stringify(rows, null, 2) + "\n");
          console.log(`HTTP list: ${runs} runs, ${mib} MiB complete`);
          continue;
        }
        const before = await counters();
        const cold = await Promise.all(
          Array.from({ length: subscribers }, () => snapshot(base)),
        );
        const warm = [];
        for (let repetition = 0; repetition < 20; repetition++)
          warm.push(
            ...(await Promise.all(
              Array.from({ length: subscribers }, () => snapshot(base)),
            )),
          );
        const afterSnapshots = await counters();
        const replay = [];
        for (let repetition = 0; repetition < 2; repetition++)
          replay.push(
            await Promise.all(
              Array.from({ length: subscribers }, () => subscription(base)),
            ),
          );
        const after = await counters();
        rows.push({
          runs,
          subscribers,
          journal_mib: mib,
          cold_snapshot_p50_ms: percentile(cold, 0.5),
          cold_snapshot_p95_ms: percentile(cold, 0.95),
          warm_snapshot_p50_ms: percentile(warm, 0.5),
          warm_snapshot_p95_ms: percentile(warm, 0.95),
          replay_cohorts: replay.map((values) => ({
            p50_ms: percentile(
              values.map((v) => v.ms),
              0.5,
            ),
            p95_ms: percentile(
              values.map((v) => v.ms),
              0.95,
            ),
            wire_bytes: values.reduce((a, v) => a + v.bytes, 0),
            events_per_subscriber: values[0].events,
          })),
          snapshot_rchar_bytes: afterSnapshots.rchar - before.rchar,
          replay_rchar_bytes: after.rchar - afterSnapshots.rchar,
          disk_read_bytes: after.disk_read_bytes - before.disk_read_bytes,
          cpu_ticks: after.cpu_ticks - before.cpu_ticks,
          process_max_rss_kib: after.rss_peak_kib,
        });
        await writeFile(report, JSON.stringify(rows, null, 2) + "\n");
        console.log(
          `HTTP/SSE: ${runs} runs, ${subscribers} subscribers, ${mib} MiB complete`,
        );
      }
    }
    server.kill("SIGTERM");
    await once(server, "exit");
    server = null;
    await rm(directory, { recursive: true, force: true });
  }
} finally {
  if (server) {
    server.kill("SIGKILL");
    await once(server, "exit");
  }
  await rm(root, { recursive: true, force: true });
}
