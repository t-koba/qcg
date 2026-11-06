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
services rescan every `SHARED_RESCAN_SECS` (5 seconds by default) to recover
work after an owner exits, and SSE
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
- `DELETE /api/runs/{id}` (terminal, unpinned runs only; active runs need cancel first)
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
  flag that is false when billed calls lacked contract pricing, the run
  recorded an unmetered hosted-search charge, or its pricing contract cannot
  be resolved

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
The default replay TTL is 24 hours from initial Ready publication, configurable
with `IDEMPOTENCY_TTL_SECS`. Records persist under `<runs-dir>/idempotency/`
through process-local cache eviction, restarts, and shared-store peers.
`IDEMPOTENCY_MAX_ENTRIES` (default 1024) governs only the process-local
cache/admission map; Pending entries are protected from capacity eviction and
can temporarily exceed this budget. There is no absolute durable record count
cap. A retained key with identical request content replays the
original result. Identical pending requests in the same process wait for the
owner and replay its result if the owner completes successfully; owner
cancellation or error wakes them to retry through durable admission instead.
A pending request owned by another process may return `503 Service Unavailable`.
Reusing a still-bound key with changed content normally returns `409 Conflict`.
Busy-gate contention can instead time out with `503`, even when a different
digest would otherwise conflict: expired claims and shard collisions need
not expose a live conflicting claim before the gate is acquired.

Keyed durable ownership and service acceptance share a cross-process execution
gate, held from claim validation through execution and Ready publication.
The SHA-256 hash of the key selects a stable gate by its first byte: at most
256 `.execution-XX.lock` files in the idempotency directory, outside deletable
run directories. This byte-addressed namespace is a shared protocol, not a
per-host tuning setting; colliding keys serialize. Gate files are permanent
coordination state: never unlink, replace, or rotate them, including when idle.
A waiter or blocked writer may already hold an open handle to the same inode.
Claim publication tries the store `.maintenance.lock` after the gate and before
`.idempotency.lock`; busy maintenance returns retryable `503` without waiting
under the key shard. Existing mutation targets are rechecked under maintenance
before Pending publication: the run directory, metadata directory and journal
must be real directories/a regular file, not symlinks. No process waits for a gate or run admission while
holding the metadata lock. Request cancellation stops gate
polling; a service blocking writer already in flight retains shared ownership
until it exits. Process death releases the OS lock, not the Pending evidence.
Contention uses the existing peer wait policy (30 seconds, polling every
100 milliseconds); Pending's existing 60-second lease is unchanged. These
fixed protocol defaults are not new environment settings. Lease expiry alone
cannot supersede an owner that still holds its execution gate.

A service execution that returns an error preserves its Pending claim: the
error alone does not prove the operation had no effects. During ordinary
operation, cancel mailbox publication serializes with new answer and
confirmation acceptance under the run's journal lock. If a valid mailbox is
published first, new answers and approvals or denials are refused before their
journal append; execution admission also rechecks the mailbox before running a
resumed step. An accepted answer or approval remains cancelable by a later
request. Conversely, a denial already accepted before cancel publication is
reconciled by cancel without publishing a mailbox; a failed reconciliation
preserves the accepted decision and retryable Pending evidence. A mailbox from
an accepted cancel remains on an error; shutdown may withdraw only a newly
published mailbox when refusing that request, as noted below. An error while
reconciling an earlier denial is not an accepted cancel.
Releasing the claim on error could let a retry execute as a fresh owner over an
already-accepted operation. Only the process-local waiter entry is withdrawn
and woken; the
durable Pending may remain unresolved after its existing 60-second lease. The
timestamp controls when a same-digest retry may adopt, measured from claim
publication rather than from a new quarantine after the error; it does not
authorize replacing Pending with a different digest. A different-digest request
continues to conflict while that evidence remains, regardless of age. While
the lease is live, a same-key retry rejoins as a peer instead of executing
fresh. Retention is not proof that replay is safe, and may hold a key even when
no effect occurred. Route-specific acceptance reconciliation is still
required.

New durable Pending claims include an opaque operation ID before execution.
A same-key, same-digest adoption retains that ID as well as the reserved run ID.
For **cancel only**, HTTP passes the ID to the service's cancellation acceptance:
a process crash after mailbox publication but before Ready retries the same
logical cancel, rather than generating another random mailbox operation.
An existing mailbox with the same operation/run identity is reused without
replacing its original attribution or timestamp; corrupt or mismatched evidence
fails closed. An already-journaled operation ID is the same acceptance, and the
journal-locked drain deduplicates concurrent deliveries. Journaled and terminal
cancel retries consume a still-published matching mailbox without adding
another event or publishing a phantom control. A valid mailbox that conflicts
with an already-accepted denial is retained and fails closed; malformed or
unrelated evidence is retained. Shutdown refusal can withdraw only a mailbox
newly published by that call, not evidence accepted by an earlier request.
Direct service and unkeyed HTTP cancels still generate fresh IDs.

This is a **partial process-crash recovery step**, not a power-loss durability
guarantee or general crash-after-acceptance reconciliation. Start/fork and
answer/confirmation crash windows remain unresolved. Ambiguous Pending evidence
is not reaped or retired by age; existing recovery and indeterminate-operation
refusals still apply. An expired lease does not authorize takeover while its
gate is held, and a released OS lock is not proof that an operation had no effects.

Legacy Pending JSON without an operation ID remains readable for live-peer
waiting, but **expired adoption fails closed and retains the evidence** rather
than inventing an identity. Stop and join **all** old peers and their in-flight
writers before upgrading a shared store, and upgrade every writer before
resuming. Reconcile legacy Pending operations and their associated runs under
quiescence before manually resolving their records; do not simply delete them
or manufacture IDs to unblock retries. Mixed-version writers are unsupported:
old binaries neither preserve these IDs nor honor execution gates. Ready format
and TTL are unchanged. Keep runs-directory ancestry trusted and unchanged.

Ready expires when the wall clock reaches its persisted creation second plus
`IDEMPOTENCY_TTL_SECS` (the cutoff is inclusive). Replay never renews it.
Peers sharing a store must use matching TTL settings and reasonably
synchronized wall clocks; a shorter TTL or fast clock can end another
peer's intended replay window early.
Physical deletion is asynchronous: a service-owned resident task runs at startup
and after each `IDEMPOTENCY_SWEEP_INTERVAL_SECS` delay (default 60), even with no
requests. Each pass retires at most `IDEMPOTENCY_SWEEP_MAX_ENTRIES` due expiry obligations (default 256), under
the shared idempotency lock. A backlog adds successive pass delays; errors or a
stopped service can extend cleanup lag. Expired mappings do not replay during
that lag. A physically present canonical Ready record still pins its run until
record removal; this can delay run GC beyond logical TTL but never renews replay.
Unresolved Pending pins its associated run regardless of age. Pending is resolved
only by a canonical Ready with the exact key, digest, run ID and generation;
then Ready alone pins. Request-triggered expiry and the resident sweep retire an
exactly matching Pending and attempt to sync its removal before unlinking Ready.
A Pending unlink or directory-sync error keeps Ready for retry. Ambiguous Pending
is retained and never resolved by age. Multiple keys pin independently. GC skips
pinned runs, and explicit DELETE returns `409` while any retained association
pins the run.

Before deletion, maintenance takes a complete canonical-record snapshot under
`.idempotency.lock`, releasing that metadata lock before run admission/handoff/
execution locks or removal. The scan counts all top-level idempotency-directory
entries against `MAX_DIRECTORY_SCAN_ENTRIES`, caps each record at the existing
64 KiB durable-read bound, validates filename/key identity and uses no-follow
reads. It does not traverse `.expiry` or `.temps` and writes no reverse index or
pin markers. An absent idempotency directory means no associations; malformed,
unknown or unreadable associations, or an incomplete scan, refuse destructive
GC and DELETE rather than guessing which runs are safe. Resolve the underlying
evidence under quiescence; never remove ambiguous Pending solely by age.

Replay takes run admission before its final canonical Ready identity/TTL check
and holds it through bounded snapshot materialization. Changed/expired mappings
return to normal claiming, not stale replay. If an observed Ready is still
within TTL but its canonical file is missing, replay fails with a storage error;
absence is not permission to execute again. Only actual expiry or a different
current canonical record permits leaving that observed replay behind. Initial keyed responses hold the
result run's admission from before Ready publication through snapshot reads, so
physical record expiry cannot strand a response between commit and reading.
These guards do not change ordinary public read semantics or add mixed-version
fencing or a power-loss guarantee. Shutdown requests cancellation and joins the resident task. Checks
between lock polls, discovery entries, migration attempts, radix steps and temp
entries are cooperative; filesystem calls already in progress are not
interruptible. If shutdown grace expires, aborting the async resident can detach
its in-flight blocking pass; joining that wrapper does not prove the pass ended.

Each Ready publication first persists an expiry-index entry under that lock.
The index identifies the exact publication, including a fresh publication UUID,
so obsolete work cannot delete a replacement even if its generation restarts.
While the completion marker is absent, each owner streams legacy directory
entries under the lock, inspecting at most `IDEMPOTENCY_SWEEP_MAX_ENTRIES`
entries per pass (including ignored names), independently of expiry and temp
cleanup. The directory cursor resumes across passes with constant-size retry
state, rather than collecting paths. A failed entry ends that pass; later passes
resume at the next entry so corrupt records cannot starve healthy ones. At EOF
following a failure, discovery restarts to retry unresolved obligations. A restart
before completion also begins a bounded traversal from the start.
The marker is published only after a full successful traversal; observing a valid
peer marker or publishing it releases the cursor. Errors are warned for
retry; corrupt or foreign records require operator repair, never speculative
deletion. With the marker present, maintenance traverses only bounded due index
work, not all Ready files, including after restart. Retirement attempts are
bounded at twice the entry budget, and radix traversal/pruning at 256 steps per
budget slot (the u64 index has sixteen hex levels). Cursors resume across passes.
Empty index paths, including those left by a failed publication, are pruned by
that cursor once due; failed removal/sync retains its position for retry without
a separate tree scan. A successful Ready publication (or exact same-Ready
convergence) best-effort retires only the Pending claim matching its key, digest,
run ID, owner and generation; retirement failure does not undo the committed
Ready. Before removing an exact scheduled Ready that is due, maintenance also
removes and directory-syncs a Pending matching its key, digest, run ID and
generation. This resolves leftover claims, not ambiguous effects: mismatched,
corrupt or unreadable Pending evidence remains, regardless of age. Pending is
never deleted solely because it expired; unexpired Ready remains untouched.
Do not mix pre-index writers with index-aware servers in one store.

Exact generated staging temps live separately and have an independent budget
of at most `IDEMPOTENCY_SWEEP_MAX_ENTRIES` yielded temp entries per pass; Ready
keys cannot consume it. Legacy regular staging temps move there during migration;
exact generated legacy symlinks are instead unlinked in place without opening or
following their targets (including Windows file/directory symlinks). This is safe
within the shared-lock, trusted runs-directory boundary: staging leaves are
expendable, their exact generated names are checked, and their ancestry must not
be mutated by uncoordinated actors. Ordinary record opens still reject symlinks
and reparse points. Regular temps require verified age of at least 60 seconds;
exact generated staging symlinks are unlinked without following them. Unknown
names remain untouched.
Maintenance failures are warned and retryable evidence is retained; maintenance
and publishers serialize on the same lock. The settings above,
`IDEMPOTENCY_TTL_SECS`, and `IDEMPOTENCY_MAX_ENTRIES` freeze at startup: unset
selects the default; zero, negative, non-integer, overflowing, or non-Unicode
values refuse startup. Cadence is independent of pending-claim TTL/temp age.

The same header is accepted by `POST /api/runs/{id}/fork`,
`PUT /api/runs/{id}/questions/{qid}`,
`PUT /api/runs/{id}/confirmations/{cid}`, and `POST /api/runs/{id}:cancel`.
Once a mapping expires (not when its cache entry is evicted), that key no
longer guarantees deduplication: an otherwise eligible reuse can be admitted
as a new request, starting another run or repeating side effects. Expiry
removes only the idempotency mapping; it does not delete, cancel, or undo the original run.
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
`GET /api/runs/{id}`, `POST /api/runs`, `POST /api/runs/{id}/fork`,
`PUT /api/runs/{id}/questions/{qid}`,
`PUT /api/runs/{id}/confirmations/{cid}`, and
`POST /api/runs/{id}:cancel` responses share a server-only provisional profile
of 16 MiB and 1,000 entries.
These are provisional product-policy defaults, not customer-calibrated values, a p95
target, or a peak-RSS guarantee. Each route enforces its per-request ceiling.
The JSON run-view and mutation snapshot responses and single-artifact downloads
additionally share the response-byte budget below; other routes have no shared
response-memory budget.
Direct CLI export (`qcg runs export`) uses the unbounded default and never applies
the provisional profile; only HTTP applies `artifact_limits.effective()`.

Explicit `--max-artifact-bytes` (`MAX_ARTIFACT_BYTES`) and
`--max-artifact-entries` (`MAX_ARTIFACT_ENTRIES`) remain authoritative,
including values larger than the provisional default. Omitted means the
provisional default for these snapshot and dedicated artifact routes only,
never unlimited.

`--max-artifact-response-bytes` (`MAX_ARTIFACT_RESPONSE_BYTES`, default
`67108864`, 64 MiB) bounds aggregate response-owned bytes for concurrent
`GET /api/runs/{id}`, `GET /api/runs/{id}/artifacts`,
`GET /api/runs/{id}/artifacts/{path...}`, `POST /api/runs`,
`POST /api/runs/{id}/fork`, `PUT /api/runs/{id}/questions/{qid}`,
`PUT /api/runs/{id}/confirmations/{cid}`, and
`POST /api/runs/{id}:cancel` responses, including unkeyed responses and
in-memory and durable idempotency replays after restart. The default admits four
current default-sized 16 MiB bodies. The positive byte limit is frozen at server
boot; zero refuses startup.

The JSON run-view and mutation snapshot responses count their serialized bytes
without retaining a body, enforce the per-response limit, and reserve the exact
size before buffering JSON. Small responses use only their actual serialized
size, not the per-response maximum. The exact-body ETag and `If-None-Match`
behavior are unchanged; `304` releases the temporary serialization reservation immediately.
Single-artifact responses reserve the full manifest artifact size before
snapshot allocation or body reads, including for a range. Conditional
single-artifact `304` responses still verify the file but retain no snapshot
and do not use this budget.

Reservations stay with the response bytes/body until dropped; serialization,
verification, allocation and conditional-response errors release them. One
body larger than the budget returns `413`; temporary aggregate exhaustion
returns `503`, before response buffering (or single-artifact body reads).
These limits do not change the existing per-response byte and entry limits.
A mutation may have committed before response admission fails; start/fork
errors retain `Location`. Once a keyed mutation commits, its Ready record
survives response-admission failure. After a temporary `503`, retrying with the
same idempotency key replays the run snapshot once capacity is available, without
repeating the mutation.
For `413`, raise the applicable configured byte limit if that response must be
admitted.

This bounds only the covered response-owned bytes, not total RSS, allocator
or streaming overhead, metadata, or other endpoints (including zip, bundles,
run-list, and mutation responses other than the snapshots listed above). Raising
per-artifact limits does not raise this budget; configure it independently to
admit larger responses.

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
by design but is cut at its configured bound. Each managed resident task and
OTLP exporter gets `SHUTDOWN_TASK_GRACE_SECS` to join concurrently during
settlement (default 5 s, range 0..=3600; `0` gives no waiting grace). This
policy is validated and frozen at boot. An expired per-task grace aborts and
joins the async task and reports an error; blocking work may outlive its
wrapper. The outer settle deadline still wins if shorter, so a longer task
grace does not extend the shutdown bound. Startup order is policy resolve,
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
