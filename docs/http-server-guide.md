# HTTP Server and Sister-Product Composition

Start qcg on loopback:

```bash
qcg serve --bind 127.0.0.1 --port 8080 \
  --generators-dir generators --runs-dir /var/lib/qcg/runs \
  --max-active-runs 8
```

qcg listens on the selected address without forcing authentication. When
`--api-token` or `API_TOKEN` is set, clients send `Authorization: Bearer
<token>`. This authenticates an instance boundary, not individual run ownership.

The bundled generator UI supports the embedded bearer token: enter it in the
sidebar token field, where it is kept in `sessionStorage` for the tab and sent
as an `Authorization` header on every request, event stream, and artifact
download. EventSource cannot set headers, so the UI reads SSE frames from a
fetch body and resumes with `Last-Event-ID`; artifact and ZIP downloads use
authenticated fetches and blob URLs. The token never appears in a URL query
string, a cookie, or `localStorage`. Static generator assets
(`GET /api/generators/{id}/assets/{path}`) stay readable without a token so
the UI shell can load before the token is entered; declared assets are not
secrets. Every JSON API route, including runs, journals, and artifacts,
requires the credential when one is configured. When browser access must be
authenticated at a different boundary, place the UI and API behind qpx and
let qpx inject the `Authorization` header after validating the caller.

## Responsibilities

| qid | qpx | qcg |
| --- | --- | --- |
| Issues identity tokens and owns authorization-server behavior. | Terminates TLS, validates JWTs or introspects tokens, enforces access policy, and proxies accepted requests. | Loads generator contracts, executes bounded runs, records journals, and serves declared assets and artifacts. |

The deployment chain is `qid -> qpx -> qcg`: clients obtain a token from qid,
qpx validates it, and qpx forwards accepted HTTP traffic to qcg on
`127.0.0.1`. Integration is only at the binary and HTTP boundary; qcg has no
build-time or runtime dependency on either sister product.

Runs accept optional `labels` (bounded free-form metadata, for example
`{"owner": "team-a"}`) and an optional `audit_level` raise on `POST
/api/runs`; both are stored with the run and visible in snapshots. Labels
are metadata only: qcg does not read caller identity, authenticate from
them, or filter run resources by label. qcg does not attach an owner or
tenant to a run or filter run resources by user. Authentication at qpx therefore does not by
itself provide per-user isolation. Use qcg for trusted shared use, or enforce
separation outside qcg with a separate service and runs directory per trust
domain.

## Concurrency and run storage

The server accepts at most eight concurrently executing API runs by default.
Set `--max-active-runs` or `MAX_ACTIVE_RUNS` to change the limit. When no
execution slot is available, accepted starts and resumed interactions remain
in `queued` state until capacity becomes available. A run in `waiting` or
`confirming` HITL state has stopped its engine task and does not consume a
slot; answering or approving it re-enters the durable queue.

The limit is process-local. The service creates one LLM and MCP runtime at
startup. LLM HTTP clients and MCP OAuth credential/token managers are shared by
profile across runs, while every run creates an independent MCP protocol
session. Provider-side rate limits and credentials remain shared resources.

Every API run gets a UUID-based ID and an isolated directory under
`--runs-dir`:

```text
<runs-dir>/<generator-id>-<uuid>/meta/journal.jsonl
<runs-dir>/<generator-id>-<uuid>/workspace/
```

The default `--run-store exclusive` takes one filesystem lock on `--runs-dir`.
`--run-store shared-filesystem` allows multiple services on storage with
reliable advisory locking. A run-level lease admits exactly one executor;
services rescan every 5 seconds to recover work after an owner exits, and SSE
polls the durable journal when the owner is another process. Process-local
`--max-active-runs` limits still need external capacity planning.

## Routes

The server exposes health, Prometheus metrics, OpenAPI, generator, MCP, run,
event, artifact, and journal routes. Principal paths include:

- `GET /healthz`
- `GET /metrics` (Bearer-protected when a token is configured)
- `GET /api/openapi.json`
- `GET /api/generators`
- `GET /api/generators/{id}`
- `GET /api/generators/{id}/assets/{path...}`
- `GET /api/llm/catalog` (`?refresh=true` re-fetches external catalog sources)
- `GET /api/mcp/servers`
- `POST, DELETE /api/mcp/servers/{id}/authorization`
- `DELETE /api/mcp/servers/{id}/authorization/pending`
- `GET /api/mcp/oauth/callback`
- `GET, POST /api/runs`
- `GET /api/runs/{id}`
- `DELETE /api/runs/{id}` (terminal runs only; active runs need cancel first)
- `POST /api/runs/{id}/invalidation` (durably succeeded runs only)
- `PUT /api/runs/{id}/questions/{qid}`
- `PUT /api/runs/{id}/confirmations/{cid}`
- `POST /api/runs/{id}:cancel`
- `GET /api/runs/{id}/events`
- `GET /api/runs/{id}/artifacts`, `GET /api/runs/{id}/artifacts/{path...}`, and `GET /api/runs/{id}/artifacts.zip`
- `GET /api/runs/{id}/bundle` (self-contained zip: snapshot, inputs, journal, outputs, verified artifacts)
- `GET /api/runs/{id}/journal`
- `GET /api/runs/{id}/metrics` returns cost totals (`tokens_*`,
  `llm_calls`, `cost_microusd`, `cost_usd`, `duration_ms`) with a `priced`
  flag that is false when billed calls lacked contract pricing

### Invalidating a successful result

`POST /api/runs/{id}/invalidation` requires JSON:

```json
{"reason":"Result used incorrect source data", "actor_label":"quality-review"}
```

It returns `200` with `{run_id, reason, actor_label, invalidated_at}` (RFC 3339
UTC timestamp). Reason and actor label must be nonblank and contain no control
characters; limits are 4096 and 128 UTF-8 bytes respectively. The actor label is
caller-provided descriptive audit metadata, **not authenticated identity**.
The endpoint uses exactly the same bearer-auth middleware/authority as DELETE;
when API authentication is disabled both operations remain available without a
token. No extra role or permission is introduced.

Only durably successful runs qualify: missing runs return `404`; active, failed,
canceled, and interrupted runs return `409`. Exact reason+actor retries return
the original record; changed values return `409`. Invalid strings return `400`.
Store maintenance or an execution lease in progress can return `409`; retry
after settlement. There is no un-invalidate or record replacement.

Snapshots and list items expose nullable structured `invalidation`; execution
`state` and journal `seq` remain unchanged. Bundles include it in `snapshot.json`.
`latest_success` skips invalidated sources and falls back, pinned `run_id`
refuses with the recorded reason, and `latest_terminal` refuses if its newest
terminal selection is invalidated (no fallback). Persisted run-reference bytes
remain usable on resume. Forks are allowed without inheriting invalidation.
See [operations](operations.md#run-invalidation) for storage and retention.

### Starting a run

`POST /api/runs` accepts `generator_id`, an `inputs` object, and optional
`answers` (pre-provisioned answers keyed by question id) and `confirmations`
(pre-provisioned decisions keyed by confirmation id) for unattended runs.
`priority` (integer, default 0, higher runs first) orders the queue and may
preempt lower-priority running runs, which resume from their journals. Every
start must include `X-QCG-Package-SHA256` with the `package_sha256` from the
loaded `GET /api/generators/{id}` detail. The server rejects a missing,
malformed, or stale identity before run admission; the identity covers the
complete package tree and participates in start idempotency. A committed
start retried with the same key, body, and identity replays the original run
even if the installed package has since changed or disappeared. An
`Idempotency-Key` header enables request replay while its mapping is retained.
The default retention is 24 hours, configurable with `IDEMPOTENCY_TTL_SECS`;
`IDEMPOTENCY_MAX_ENTRIES` also bounds retained entries. Records are persisted
under `<runs-dir>/idempotency/` and shared across process restarts and
shared-store peers. A retained key with identical request content replays the
original result. Identical pending requests in the same process wait for the
owner and replay its result; a pending request owned by another process may
return `503 Service Unavailable`. Reusing a still-bound key with changed
content returns `409 Conflict`.

The same header is accepted by `POST /api/runs/{id}/fork`,
`PUT /api/runs/{id}/questions/{qid}`,
`PUT /api/runs/{id}/confirmations/{cid}`, and `POST /api/runs/{id}:cancel`.
Once a mapping expires or is evicted, that key no longer guarantees
deduplication: an otherwise eligible reuse can be admitted as a new request,
starting another run or repeating side effects. Expiry removes only the
idempotency mapping; it does not delete, cancel, or undo the original run.
Clients should retain the original `run_id` and reconcile its state rather
than rely on reuse of an expired key. In-memory Ready caches keep the persisted
record's original creation time, so reading or replaying a mapping does not
restart its configured TTL.
Snapshots of a `Queued` run additionally expose `queued_at` (RFC 3339) and
`queue_position` (1-based admission order) for queue observability.
The cancel response is returned after the active engine task has stopped. If a
run committed completion before the cancellation request won the race, the
response preserves that completed state instead of writing a second terminal
event.
The `{path...}` notation above denotes the nested wildcard captured by the
server; the OpenAPI document names that wildcard parameter `path`.

## MCP authorization

`GET /api/mcp/servers` lists the configured MCP server IDs, transport,
authentication mode, and whether an OAuth profile is authorized. For an OAuth
profile, `POST /api/mcp/servers/{id}/authorization` starts the authorization
code + PKCE flow and returns an authorization URL. Open that URL in the
browser, then the provider redirects to `GET /api/mcp/oauth/callback`. qcg
validates the single-use state, exchanges the code, and stores the resulting
credentials in the OS keyring by default. `DELETE` on the authorization path
clears the profile's stored OAuth credentials and any pending authorization.
`DELETE` on its `/pending` subpath cancels only an in-progress browser flow, so
it cannot erase credentials from a callback that won the completion race.

The authorization endpoints are available only when qcg listens on a loopback
address and reject a different request `Origin`. The callback is intentionally
small HTML so the browser can close it after the SPA observes the authorized
status. A callback state expires after ten minutes. The profile's
`allowed_hosts` and the contract's `permissions.network` continue to restrict
OAuth and MCP traffic; an OAuth authorization URL is never accepted merely
because it came from a configured server.

## FileValue

File fields are inline and have exactly one content representation:

```json
{"name":"settings.json","text":"{\"enabled\":true}"}
```

or:

```json
{"name":"logo.png","content_base64":"iVBORw0KGgo="}
```

`name` must be one safe filename component. `text` and `content_base64` are
mutually exclusive. `content_base64` is canonical RFC 4648 base64 with the
standard alphabet and required padding; unpadded or non-canonical values are
refused rather than normalized, so one byte string has exactly one accepted
spelling on every surface. Decoded content is limited only when the generator
contract sets an explicit `[runtime] file_input_limit_bytes`; otherwise there
is no mechanistic limit. Values are supplied inline with the run request.
`qcg serve` additionally accepts an explicit `--max-request-bytes`
(`MAX_REQUEST_BYTES`) bound for the whole request body. The bound is
fail-closed: when omitted, the server enforces the documented 2 MiB default
(Axum's secure default), so FileValue and contract limits govern within that
envelope. The effective bound is the configured value when set, otherwise
2 MiB. The startup log reports the effective policy, and `GET /healthz`
exposes it as `max_request_bytes` (always a number, never `null`). Health also
includes `shared_store_recovery` (`disabled`, `starting`, `healthy`, `degraded`,
or `restarting`), with no error or run details. `ok` remains process liveness,
not readiness: recovery degradation does not make healthy operations
unavailable. See [operations observability](operations.md#observability) for
recovery isolation, retry, and startup boundaries.

## Assets and frontend development

The bundled SPA is available from the normal assets API after it is built:

```text
http://127.0.0.1:8080/api/generators/generator/assets/ui/index.html
```

For source development:

```bash
API_TARGET=http://127.0.0.1:8080 \
  npm --prefix frontend/generator run dev -- --host 127.0.0.1 --port 5173
```

The frontend uses root-absolute `/api/...` requests in both modes. Vite proxies
them during development; production assets share qcg's origin.

The bundled generator's Connections panel uses the MCP endpoints above. It
does not receive, store, or display access tokens. For headless runs, complete
OAuth authorization once through the loopback server before starting a
generator that binds an OAuth MCP tool.

## External delivery pattern

qcg does not add a product-specific artifact upload API. A generator that must
send an artifact declares the exact command shape in `permissions.commands`
and runs a reviewed script with `side_effects = "confirm"`. The confirmation
keeps network delivery explicit and journaled.

## CORS and events

On a loopback listener every request must carry a loopback `Host` for the
bound port (`127.0.0.1`, `localhost`, `::1`, or the bound address); any other
`Host` is rejected with `403` before authentication, which blocks
DNS-rebinding pages from driving the local API. Non-loopback binds leave
`Host` validation to the reverse proxy.

CORS is disabled unless one or more exact `--cors-origin` values are supplied.
Allowed request headers are `Authorization`, `Content-Type`, `idempotency-key`,
`x-qcg-package-sha256`, `Last-Event-ID`, `If-None-Match`, `If-Range` and
`Range`. Browsers can read
`ETag`, `Location`, `Retry-After`, `Content-Disposition`, `Content-Range` and
`Accept-Ranges`; credentialed CORS is not enabled. Builds without the
`server-cors` cargo feature refuse configured origins with an explicit error
instead of serving without CORS. Run events use SSE and support `Last-Event-ID`
replay. A completed run returns only unread history and closes, including when
the terminal cursor has no unread events. Future cursors replay all history.
The `lagged`, `stream_error` and `shutdown` control frames do not advance the
durable cursor; reconnect using the last real event's seq.

Single-artifact downloads support a single `bytes` range, returning `206`,
`Content-Range` and `Accept-Ranges`. A matching strong `If-Range` validator
permits the range; a mismatch returns the complete `200` representation.
Unsatisfiable ranges return `416` with `Content-Range: bytes */size`.
Unsupported units and multipart ranges fall back to the complete representation.
The complete file is verified before delivering a successful range response.

### Artifact and run-snapshot response bounds (provisional)

`GET /api/runs/{id}/artifacts`, `GET /api/runs/{id}/artifacts/{path...}`,
`GET /api/runs/{id}/artifacts.zip`, `GET /api/runs/{id}/bundle`,
`GET /api/runs/{id}`, and idempotent `POST /api/runs` / `POST /api/runs/{id}/fork`
snapshot responses share a server-only provisional profile of 16 MiB and 1,000 entries.
These are provisional product-policy defaults, not customer-calibrated values, a p95
target, or a 64-MiB peak-RSS or concurrency guarantee. No aggregate or
concurrent memory envelope is claimed: each buffered body has its own
per-request ceiling, and concurrent requests each enforce their own ceiling.
Direct CLI export (`qcg runs export`) uses the unbounded default and never applies
the provisional profile; only HTTP applies `artifact_limits.effective()`.

Explicit `--max-artifact-bytes` (`MAX_ARTIFACT_BYTES`) and
`--max-artifact-entries` (`MAX_ARTIFACT_ENTRIES`) remain authoritative,
including values larger than the provisional default. Omitted means the
provisional default for these snapshot and dedicated artifact routes only,
never unlimited. No new flags are introduced.

Enforcement order: raw `outputs.json` bytes are capped before deserialization
using the opened file's metadata for the exact stable size (a concurrent
growth fails closed instead of reporting `cap+1` as the exact size; the raw
and entry bounds constrain aggregate metadata/path work, without a separate
per-path or aggregate-memory guarantee); directory-inclusive entry counts
(`directories + files`, so nested `a/b/c` counts 3) are enforced
incrementally as unique prefixes are inserted and return before
hashing/streaming; the single-file size admission runs before
`If-None-Match`/`304` and `Range` handling while preserving the exact ETag,
conditional, `Range`, and file-verification semantics; the ZIP path caps raw
manifest, total source bytes, and directory-inclusive entry count before
hashing (hashes bounded by admitted `bytes+1`) and streaming (copies bounded
by `bytes+1` with length recheck so growth cannot complete successfully), and
caps final compressed bytes. The normalized list body size is tracked exactly
(total attempted bytes, not `limit+1`). Nothing silently truncates: list and
single-file byte admissions fail before headers with `413 Payload Too Large`
(`payload_too_large`) and exact stable sizes; manifest entry-count failures on
those routes use `422` (`validation_failed`, field `artifacts`), never byte
fields. ZIP is streamed after `200` headers, so raw-manifest, source-byte,
entry-count, verification, or final-byte failures surface as body stream
errors (logged by the server), not `413`/`422` responses or successful
truncation. Treat any body error as failure; for cap errors, use a larger
explicit override or a smaller artifact set, while verification/I/O errors
may require repairing the artifact source.

Bundle preparation/delivery shares the same provisional profile but enforces
it before headers: raw `outputs.json` is capped before deserialization, parsed
once from exactly the length stat’d on its open handle (short reads fail),
checked for duplicate artifact paths, and moved/reused with no clone. Journal
scanning retains the engine’s default event/count/total bounds and torn-final-line
tolerance, with a stricter route total cap applied before deserialization; its
consumed byte count pins the archived prefix. Exact serialized metadata lengths
are counted through a nonallocating sink before buffering, then summed with
source bytes, including raw `outputs.json`, overflow-safely. Aggregate bytes, artifact count and
incremental directory-inclusive entry checks all complete before artifact
hashing. All blocking FS/read/hash work runs in `blocking_work`.

A private prepared state pins journal/audit/artifact lengths and streams each
with `take(length)` (including empty-but-present audit), excluding appended
bytes; a short read fails the body stream with an I/O error. The writer
rechecks the prepared aggregate and entry count against its supplied limits
before emitting any ZIP bytes; no mutable public preflight bypass exists.
Explicit `u64` to `usize` conversions fail closed. Hashes fail before headers,
with no duplicate hash or metadata serialization after headers. The final ZIP
cap includes the footer and never reports a truncated archive as success.
Only final archive overruns and actual writer/stream errors may occur
post-header (as body stream errors, not `413`/`422`). Raw-manifest, journal and
aggregate byte admissions return `413`; entry admissions return `422`
(`validation_failed`, field `artifacts`). Verification and preparation I/O
failures also precede headers and retain their normal error classifications.

Out of scope here: `GET /api/runs` summaries (`RunListItem` carries no `OutputManifest`)
are not covered by this profile. Snapshots bound raw
`outputs.json` before deserialization and the complete `RunSnapshot` JSON before
buffering, preserving exact-body `ETag`/`304`, `201` status/`Location`, and
idempotent key-to-run replay without truncation (`413` before headers/body when
over; manifest entry overflows use `422` on field `artifacts`). If a start
or fork has created a run but its snapshot fails, the error response (including
`413` or `422`) includes `Location: /api/runs/{run_id}`. A keyed retry
retains that same run identity and does not create another run.

## Shutdown behavior

On `SIGINT`/`SIGTERM` the server stops accepting mutating requests with
`503` (JSON problem shape), closes active event streams with an explicit
`shutdown` marker (distinguishing shutdown from truncation), stops resident
tasks, and settles tracked runs as `Interrupted` under the configured
`SHUTDOWN_SETTLE_SECS` outer deadline (default 150 s, range 1..=3600); a
timeout is returned as an error to the host. The HTTP drain is bounded by
`SHUTDOWN_DRAIN_SECS` (default 30 s, range 1..=3600), and settle must be at
least drain. The total bound from signal to exit is the sum of the configured
phases: 180 s by default, at most 7200 s under their validated ranges. The
outer deadline starts after the drain, so a wedged drain delays settlement
by design but is cut at its configured bound. Startup order is policy resolve,
service build, router build, then recovery before resident tasks; shutdown order
is signal, mutating-work gate, HTTP drain, then resident-task and active-run
settlement concurrently under the outer deadline. Settled runs are terminal and cover every tracked
non-terminal run including `Queued`; only work that was waiting on human
input, explicitly requeued, or never tracked by the stopping peer resumes
after a restart. Single-peer enforcement: the lease-holding peer settles
its own runs; leaseless peers never touch others' runs. In shared mode stopping one peer settles only that peer's
runs. A cancel racing the shutdown settles as `Interrupted` once shutdown
started, else `Canceled`. The HTTP drain is bounded by the configured
`SHUTDOWN_DRAIN_SECS` value; a wedged connection is cut so shutdown proceeds
(warned, never silent). See `docs/operations.md` for the normative shutdown
contract.

## Conditional snapshots

`GET /api/runs/{id}` (and the artifact inventory) return an exact-body
`ETag`. Repeating the request with `If-None-Match` set to that value yields
`304` while the body is unchanged; `If-None-Match: *` matches any existing
snapshot, and comma-separated lists use weak comparison per RFC 7232.
Malformed validators fail closed with `400`. Mutation responses (`POST`
start/fork, idempotent replays) carry the same
validator so the next read can condition on it, and replays return the
identical validator. Queue movement, metrics,
prompts, and approvals all change the body and therefore the validator.
The run list, cost metrics, and single-artifact bodies carry validators
(exact-body digest, or manifest sha256 for artifacts); the bounded journal
snapshot carries an exact-body validator and the unbounded live tail carries
a weak revision validator over (size + mtime), so both journal shapes are
conditional. The generator catalog and the artifact zip carry no validator and always return
`200` headers (cap failures surface as body stream errors): hashing the full
archive upfront would defeat streaming. The run bundle carries no validator;
successful preparation returns `200` with a streaming body, while bundle bounds
(raw manifest, journal, aggregate, entries, hashes) fail before headers with
`413`/`422`; only the final archive overrun and writer/stream errors occur
post-header as body errors. For streams the snapshot or
artifact-list validator is the queue-revision alternative: conditional reads
apply to snapshots, the run list, cost metrics, single artifacts, the
artifact inventory, bounded and unbounded journal reads, and mutation responses.
