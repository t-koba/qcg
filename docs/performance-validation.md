# Bounded read-view measurement

Run the reproducible microbenchmark:

```sh
QCG_BENCHMARK_REPORT=/tmp/qcg-read-benchmark.json \
  cargo test -p service read_store::benchmarks::read_view_performance_matrix \
  -- --ignored --exact
```

The matrix covers 100/1,000/10,000 run directories, 1/8/64 simultaneous
readers of one target run, and exactly 1/16/64 MiB target journals. Other
runs have small identity journals; this does not model 10,000 journals each
of 64 MiB. A directory census primes those small run views. Each case has
one cold cohort and twenty warm cohorts. Cold refers to the application
read cache; the operating system page cache is not cleared. Cold p50/p95 are percentiles among
that cohort, not independent cold-start replications.

The JSON records latency p50/p95 separately for first and subsequent reads,
actual journal bytes read (including complete cached-prefix digest checks on
append), applied/folded events, retained cache charge, census duration,
cumulative process CPU, and process maximum RSS. RSS is KiB on Linux and bytes on macOS; Windows resource fields
are unavailable and represented by zero. Filesystem metadata I/O is not
included in the byte counter. CPU/RSS are whole-process observations,
including fixture generation, rather than per-request estimates.

Assertions check that each target event is folded once despite concurrent
readers, warm reads do not re-read target history, and retained views fit the
configured 256 MiB charge and 10,000-entry bounds. Collection/string capacities contribute
to the conservative charge. Blocking work has eight service permits and
same-run shard locking. Capacity accounting is incremental and LRU selection
uses an index. The microbenchmark also repeats the complete small-run census
and asserts zero additional journal bytes and folds when all views fit.
Cache eviction can require a new cold read;
execution, approval and artifact integrity paths still verify originals.

Recorded read-view runs are a reproducible sample,
not a cross-product claim. Raw read-view reports are produced by the command
above and kept with the release process rather than shipped in the install
bundle. Cache hits are revision-bound. Before reusing a full view across
same-inode growth on Unix, the cache streams the entire old prefix through
SHA-256 and resumes only when that digest matches; otherwise it rebuilds from
byte zero.
This detects prefix rewrites on changed revisions without relying on a matching
tail window. The constant-memory integrity check reads the old prefix but avoids
reparsing and refolding it when unchanged. Other platforms rebuild on change.
Authoritative execution and integrity decisions still read originals.

The actual HTTP/SSE matrix runs independently:

```sh
cargo build -p cli --locked
QCG_HTTP_BENCHMARK_REPORT=/tmp/qcg-http-sse.json node scripts/benchmark-http.mjs
QCG_BENCHMARK_MODE=list QCG_HTTP_BENCHMARK_REPORT=/tmp/qcg-http-list.json \
  node scripts/benchmark-http.mjs
```

HTTP/SSE runs cover all 27 combinations, using
actual fetch clients, 21 snapshot cohorts and two full history subscription
cohorts. List runs cover nine combinations,
with one initial listing and twenty subsequent listings. Every stream must
deliver a terminal event and close, and every snapshot must report success.
The fixture contains completed runs, with only one large journal. This measures
replay and snapshot/list observation, not active LLM execution or live tail
broadcast throughput. A full replay necessarily reads and transfers history
for each subscriber; it is not expected to have the same cache behavior as
snapshot refresh. The server retains at most 10,000 full views and explicitly
uses 256 MiB for snapshot/SSE. Historical list measurements used 512 MiB to
retain full views; conservative accounting could exceed 256 MiB for 10,000
fixtures. The current list-only benchmark uses the documented 64 MiB default
and retains compact revision-bound projections instead of full folded states.

An isolated handler diagnostic at source digest
`7f4f814abdb18076c0659c67d1659a69d49874bb50b673a3f05a16b14a2d3634` (record
`a7f6a19fdec9501e04548a4ea48dc09f814707712c072c256d0ded97894cb291`) used
10,001 completed journals and `limit=1`. The cold request took 12,283.372 ms,
missed 10,001 entries, applied 60,006 events and retained 7,820,890 charged
bytes. The next requests took 1,164.851 and 1,106.930 ms; each hit all 10,001
entries, with no eviction, journal-parser bytes read or events applied. The
instrumented parser read 28,883,104 journal bytes on the cold request. This is
synthetic host-specific evidence, not production latency; an earlier synthetic
study measured 15,425.261 ms median at 10,001 runs. Every request still
enumerates the directory, opens/fingerprints every eligible journal, reads
invalidation sidecars and filters/sorts all items. This is not O(page), a
persistent catalog or a bound on total RSS.

The recorded environment and bounds identify the debug
binary, host, versions, retention configuration and counter definitions.
Server `rchar` includes request socket reads; it is not a pure journal-byte
counter. CPU ticks can be converted using the recorded clock tick rate.
Maximum RSS includes parser/transmission buffers as well as retained views;
the cache budget does not limit total server RSS. Clients and server share
this development host, and other host work can affect timing.
Large simultaneous journals across every run remain a separate workload.
Release and slow live-consumer measurements are now recorded below.
OTLP regression exercises collector failures, partial success, a corrupt run
beside a healthy run and eight repeated GC/cursor-pruning cycles.

The list-only HTTP matrix now uses the 64 MiB product default and records
`warm_rchar_bytes` after the initial request, requiring it to stay below 1 MiB
across twenty refreshes. This allows request traffic while rejecting repeated
reads of the 1 MiB target journal. Compact list projections share the cache
budget with full views; the snapshot/SSE matrix keeps its explicit 256 MiB
full-view budget. Cache eviction still requires a cold reconstruction, and
warm listing retains O(number of runs) directory, revision and sidecar checks.

## Run-bundle streaming

The actual `GET /api/runs/{id}/bundle` endpoint was profiled on the exact
source digest `0e44d572d6a462f88925c60075c93b78a3f4fe5e8416f3e791bc1403d9788005`
using its debug CLI binary (check job `48313d3c7209cb64e403e20d27aa43fbd4ef82f40ffce42512ae3433a0ef4815`,
command record `d9927d73a2154065b2e870f05d97170f`). Synthetic completed-run
journals were 1/8/12 MiB with 0/2 MiB random artifacts; journal records were
about 256 KiB so each stayed below the 8 MiB per-event parser limit. All six
responses passed Python's `ZipFile.testzip()`, and every fixture fit the route's
effective 16 MiB / 1,000-entry limits. Each size case used one fresh server
process and two sequential requests; these are two observations, not percentiles
or release-build/production latency.

| Journal / artifact MiB | Headers ms (req 1 / 2) | Body ms (req 1 / 2) | ZIP bytes | CPU ticks H/B (req 1 / req 2) | Cumulative RSS HWM KiB after req 1 / 2 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 / 0 | 513.58 / 20.03 | 727.67 / 750.56 | 782,124 | 51/72 / 2/73 | 60,216 / 61,840 |
| 8 / 2 | 493.10 / 213.99 | 7,480.78 / 6,359.44 | 8,349,141 | 68/749 / 22/643 | 86,376 / 86,376 |
| 12 / 2 | 335.10 / 235.94 | 7,865.36 / 8,170.84 | 11,474,287 | 33/798 / 24/820 | 77,344 / 82,260 |

Headers-to-body timing is an approximation: preparation finishes before the
response is built, while the ZIP writer starts concurrently and can enqueue
initial chunks before the client sees headers. In a separate largest-fixture
12 MiB + 2 MiB run (check job
`9e543ae3a9afdcb4451029c42d3a2bf71a7ccc704608004686c9a57d77fde9fb`, command
record `d72b799e029b4e8b8c4d2e899f58ed3b`), the first request measured
469 ms to headers and 7,129 ms for the body, with 75 header-phase and 735
body-phase process CPU ticks. `/proc` high-water RSS was 39,584 KiB before the
request, 90,296 KiB at headers, and 94,680 KiB after the body. The second
request did not raise the high-water mark. Thus most observed additional peak
memory preceded the body, while streaming added about 4.3 MiB; these are
whole-process high-water samples, not per-allocation measurements or an RSS
quota. First-request `rchar` was anomalously higher than the repeat, so it is
not used to attribute journal/artifact reads. The body CPU and wall times were
close on this host, but combine deflate work, bounded-channel backpressure and
HTTP delivery; this experiment does not isolate transport from compression.

A separate disposable instrumented source copy (check job
`ac6dcb751559b32e94038d42e157cd2e8b0127db0735b4867f12a5a80eaca1aa`, command
record `28c7f955d3fb492b8159d3e191526bc9`) recorded internal phase durations on
the 12 MiB + 2 MiB fixture. Its input tree digest was
`7b27a741f9154a2e6eca6489e90c5451774225af5556c8601fc069485ba0f756`; the
workspace difference from the matrix source was this documentation draft, and
Rust instrumentation was applied only in the disposable copy.

| Request | Journal/manifest read, parse/fold and artifact metadata ms | Snapshot assembly ms | Artifact preflight SHA-256 ms | Total preparation ms | ZIP writer ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 294.457 | 0.207 | 87.942 | 383.165 | 9,014.710 |
| 2 | 160.720 | 0.241 | 77.133 | 238.741 | 6,115.459 |

The same run measured 736/248 ms to response headers and 9,024/6,146 ms for
the body, with 852/637 process CPU ticks during the two bodies. The ZIP writer
phase accounts for most of the measured wall time in this artifact-heavy
fixture, but it includes journal and artifact reads, deflate, streamed artifact
hashing, and bounded-channel backpressure; it is not an isolated compressor or
transport benchmark. The first-request whole-process RSS high-water mark was
104,360 KiB by headers and did not rise while the body drained. Instrumentation
and this debug build make these observations unsuitable as release latency or
memory estimates.

### Deflate-level tradeoff

A controlled scratch comparison (check job
`cbb4ab6ec9bdb809e5a8036e58f4a4e0f401755b92f49cfca584d54e3decbb65`, command
record `92d5e410276e48feb3e36dc5e3bbef2c`, based on source digest
`ed266c2d163ee0655f240ce4930acb397129d7f5918a0f48ce4105ce3c169e3c`) compared
the package default with `compression_level(Some(1))` on every non-directory
entry. Four fresh servers ran two requests each in default/level-1/level-1/
default order. Each used identical 12 MiB journal bytes and a 2 MiB random
artifact; the instrumented debug binaries and option change existed only in
disposable copies.

| Variant (4 requests) | Headers ms (range) | Body ms (range; median) | ZIP bytes | Body CPU ticks (range; median) | Process RSS HWM after body, KiB (two servers) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Package default | 258–1,239 | 7,335–9,492; 8,255 | 11,474,597 | 747–954; 834 | 92,424–100,700 |
| Deflate level 1 | 208–1,294 | 4,242–5,759; 5,535 | 14,765,325 | 473–591; 564 | 95,176–100,300 |

All eight archives passed `ZipFile.testzip()` and exact journal/artifact byte
checks, with identical entry lists. Level 1 reduced median body wall time by
about 33% and body CPU ticks by about 32%, but increased output by 3,290,728
bytes (28.7%) to within 2,011,891 bytes of the 16 MiB output limit. Header
ranges and whole-process RSS overlap substantially; this does not establish a
memory benefit. This tradeoff is not safe to adopt globally: at the same source
size, less compression consumes much more of the bounded output budget and can
reject bundles that the current setting accepts. Preserve the package default
and current output limit; no production compression change is justified by this
single fixture.

Source review explains the phases: `run_bundle_parts` reads and folds the
journal, constructs typed events and snapshot metadata, checks artifact paths
and lengths, and hashes artifact contents before responding. The blocking ZIP
writer then streams the journal and artifacts again, deflates entries, and
rechecks artifact hashes while copying. The source-byte/entry limits bound
accepted fixtures, not whole-process RSS. Removing either artifact hash pass
would weaken pre-response verification or stream-time mutation detection. No
buffer-size variant was measured. The profile did not test disconnect, shutdown,
or slow clients, and no cancellation behavior was changed.

## Release and slow live consumers

Release HTTP/SSE runs repeat all 27
snapshot/replay conditions using `target/release/qcg`.
Release list runs repeat all nine list
conditions; each warm cohort records zero `rchar` bytes. These are Linux
observations on this shared host, not measurements for other operating systems.

Slow live SSE runs cover 54 conditions: the full
100/1,000/10,000 run, 1/8/64 subscriber, 1/16/64 MiB matrix in both exclusive
and shared-filesystem modes. `scripts/benchmark-live-sse.mjs` holds an external
execution lease, connects readers before journal growth, appends bounded
records, and intentionally delays each reader by 10 ms. A 64-event live channel
and 50 ms polling interval exercise backpressure and `lagged` recovery.
Every consumer must receive contiguous real seq values through terminal and
EOF. Reconnection uses the last delivered durable seq, ignoring control seqs.
The final snapshot must report success. The observed process high-water RSS
remained below the 512 MiB measurement guard; the cache budget was 64 MiB.
This RSS guard is a benchmark assertion, not an OS memory quota.

The fixture is a real durable journal and HTTP server but uses synthetic events
and an external lease holder, not an LLM producer. This isolates observation
behavior without adding model/provider variance. Replay and tail bytes, CPU
ticks, p50/p95, maximum RSS, consumer delays, channel size and reconnect counts
are preserved in the raw report. Cache bytes/folds retain the separate exact
instrumentation from the read-view benchmark. Run the matrix with:

```sh
cargo build -p cli --release --locked
QCG_HTTP_BINARY=target/release/qcg \
  QCG_HTTP_BENCHMARK_REPORT=/tmp/qcg-release-http.json node scripts/benchmark-http.mjs
node scripts/benchmark-live-sse.mjs
```
