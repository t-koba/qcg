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
