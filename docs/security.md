# Security Model

qcg is a generation runtime with optional bearer authentication, not an identity provider,
authorization server, or TLS endpoint. It has no per-user ownership model for
runs. qcg listens on the explicitly selected network without imposing an
authentication policy. Set `--api-token` or `QCG_API_TOKEN` when instance-level
bearer protection is wanted. One deployment configures a single token per
instance; there is no per-user or per-run token table. Deploy one instance and runs directory per tenant
or trust domain.

## Trust boundary

When a token is configured, anyone holding the instance bearer token can list generators, start and control
runs, read journals, and download artifacts. The token is an instance boundary,
not row-level ownership. Terminate TLS before qcg on untrusted networks.

For production, use this binary boundary:

```text
client -> qpx (TLS + token enforcement) -> 127.0.0.1 qcg
             ^
             |
          qid issues tokens
```

qcg does not import, link to, or invoke qid or qpx. The products remain
independently buildable and composable over HTTP.

Authentication at qpx or by qcg does not add ownership checks inside qcg. Anyone accepted
by the trusted boundary can access the API resources exposed by that qcg
instance, including other runs in its runs directory. Use qcg only for trusted
shared use, or enforce per-user or per-tenant separation outside qcg with separate
service instances and runs directories or an owner-aware gateway.

The default run-store mode exclusively owns a runs directory. The explicit
`shared-filesystem` mode permits multiple services only when the storage
provides reliable advisory locks. A run-level lease prevents duplicate engine
execution, and periodic recovery reclaims abandoned runs. This is availability,
not tenant isolation. API runs receive UUID-based IDs and separate
`meta/journal.jsonl` and `workspace/` paths. Direct runs reject concurrent use
of the same output directory.

The process-local `--max-active-runs` or `QCG_MAX_ACTIVE_RUNS` limit defaults
to 8. It is a capacity control, not an access-control mechanism. Runs beyond
the limit wait in the durable execution queue, while runs paused for human
input or confirmation release their execution slot. All runs in one process share the configured LLM and
search provider runtimes and provider HTTP clients.

## Contract sandbox

Generator capabilities are denied unless declared. Contracts bound filesystem
paths, commands and argument shapes, network hosts, containers, secrets, side
effects, runtime, and budget. Side effects require confirmation when declared
with `side_effects = "confirm"`.

Agent Skills (`skill` and `skill_library` resources) are data, not
capabilities. Loading a skill never grants command, network, or write
permissions, and `allowed-tools` frontmatter is informational. A skill can only
influence a model through declared LLM context or the read-only `skill` agent
tool, which returns skill instructions and bundled file contents from the
package. Every effect a skill describes still requires its own explicit
permission and, for side effects, the normal approval flow.

`permissions.side_effects_scope` selects how far one approval reaches
(mirror of the normative Q1 definition in `docs/contract-reference.md`;
salt/run-scope details live there; this section states the enforcement):
`invocation` (the default) authorizes a single call, so a later call
re-confirms even for identical content; `content` reuses the approval for
identical content until the run ends. Identical content means the same target
plus the same canonical operation details (method, full-URL digest with
sorted query pairs, header digest, body/stdin
digest, and `sensitive_query` digest); this matches the
`docs/contract-reference.md` definition. Confirmation ids are never the
2-element `<node>:<kind>` form: `content` scope uses
`<node>:<kind>:<operation_digest>` (3 parts) and `invocation` scope uses
`<node>:<kind>:<operation_digest>:<invocation_hash>` (4 parts), with
`invocation_hash` = hex SHA-256 over `execution:<node>:<count>` for
single-shot steps or over the model call id for agent tool calls.
`ConfirmSpec.operation_digest` and `ConfirmSpec.scope` are both required with no default at the confirmation API boundary. The manifest `permissions.side_effects_scope` defaults to `invocation` when omitted; every minted `ConfirmSpec` carries an explicit `scope` and `operation_digest`, and a confirmation without either is corrupt and fails closed (never an invocation-scoped default) (Q1). The operation identity always binds the
method, a digest of the sorted request headers, and the body or stdin digest;
secret header values are digested, never written to the journal. Targets
(URLs, command lines, tool names) are journaled in plaintext so approvals are
auditable: credentials belong in declared secrets or headers, which are
digested, never in query strings. URL query VALUES are redacted by default in
journals (keys stay visible). `http` steps can declare
`sensitive_query = ["name", ...]`: those values are removed from the
journaled target and step output, digested into the approval, and passed to
the gateway so canary redaction covers the request and its final URL.
Journaled HTTP bodies and `fs.write` contents are hash placeholders
(`[REDACTED:sha256:<hex>]`), never plaintext: execution uses the raw bytes
held in memory, while a redacted resume returns cached success or fails
closed instead of sending the placeholder (E09). Response headers pass
through to step outputs unscrubbed by design: flows legitimately extract
tokens from login responses, so scrubbing them would break the feature
the redaction protects on the request side; request-side credential
hygiene (declared secrets, canary checks) is where leaks are stopped. The content digests are
domain-separated `SHA256` bindings salted with the per-operation id, so identical secrets in
different operations yield different journaled digests and reuse cannot be correlated across
runs. The salt is journaled in plaintext alongside, so this is unlinkability, not secrecy
against the journal reader: a targeted dictionary attack by a journal reader remains possible.
High-value credentials belong in headers sourced from declared secrets (canary-checked, never
journaled), and query strings should carry only values whose equality binding — not
confidentiality against journal readers — is what matters. Tool and
MCP invocation arguments are journaled redacted for audit, so credentials must be
declared in `[secrets]` (rendered through templates and canary-checked)
instead of being embedded literally in arguments. Pre-approved content scope reuses one approval for identical content until the run ends: its blast radius is the run. Prefer invocation scope for high-value side effects (Q1).
Risk boundary (E09): command-line redaction covers only credential-shaped
assignments (`key=value` / `key: value`) and URL-embedded secrets. A bare
secret with no key shape (for example a raw token as a positional argv
element) passes through to the journaled plaintext target. Secrets must
therefore never be embedded literally in argv: pass them via `input` /
`stdin`, environment injection (`inject_secrets`), or declared secret
headers, which are digested and never journaled in plaintext.
Response boundary (E09): step outputs carry response bodies and command
stdout/stderr up to their configured byte limits with only authorization-like
response headers redacted. A remote that echoes a secret therefore leaves it
in the journaled output in plaintext. The resend cache is written only after
the tool-output guardrails and the declared-secret scan pass; provider
credentials reflected verbatim fail closed, and other echoes are stopped at
the declared-secret scan before the next LLM turn, not by output redaction.
Flows that handle high-value secrets in responses must declare them in
`[secrets]` so the scan covers them, or avoid persisting such outputs.

Workspace filesystem
writes are governed by `permissions.fs_write` and are not side-effect
confirmations; only command, HTTP, and MCP calls enter the approval flow.
Safe HTTP methods (GET,
HEAD) perform no side effect and are not approval-bound; a flow that needs an approval for a read-like request must use a non-safe method. Safe methods use no resend cache and always re-execute, so a header change simply changes the read without aliasing another approval (E09). They still run
under the declared host allowlist, honor `sensitive_query` redaction, and are
journaled in full. Two-form identity by design (E09): the used-call registry
binds the single REDACTED form (all query values redacted so a checkpointed
copy recomputes identically), while the approval details bind the FULL form
(method plus full canonical URL digest plus headers digest). A safe-read query
change therefore aliases in the registry and simply executes the new query
(side-effect-free, never stale reuse), with separation enforced in details.
Journals written before the invocation scope split fold
unknown operation states as indeterminate, so they are refused rather than
silently retried.

Workspace filesystem isolation is handle-relative on Unix for atomic
replacements and for the reads that feed command input, HTTP body, prompt,
media, transform sources, and `check.format`/`check.schema`: they walk
directory handles with `O_NOFOLLOW` at every component from the workspace
root, so a parent directory swapped after validation cannot redirect the open
or the rename outside the workspace. This covers step output writers (render/copy/write/transform base64/zip
and HTTP file outputs). Directory-tree consumers that must parse the tree
(`check.contract`, `check.tool`, zip sources) read a private handle-relative snapshot under
the run metadata; the external tool process reads the snapshot, never the
live workspace, so a parent swapped after validation cannot redirect it
(E13). The synchronous `qcg-fs` helper writes trusted
internal paths (run metadata, artifacts) with the same handle-relative
staging and replace on Unix. Windows keeps canonicalize-based checks;
non-cooperative concurrent modification of the workspace by another process
with the same OS user remains outside the guaranteed boundary there. The same
boundary applies to a container workload writing the mounted workspace while
host steps write the same paths: flows must not parallelize container and host
writes to overlapping paths (E13). Acceptance scope: container/host
parallelism is prohibited by policy rather than proven by a passing-workload
test — no acceptance test runs a live container against concurrent host
writes; the host-parallel proof (`concurrent_overlapping_writes_stay_atomic`)
and the parent-swap refusal proof
(`swapped_parent_cannot_redirect_read_or_write_outside`) cover the
single-host boundary only.
Staging and commit are split around the await on every platform: the
staging leaf name is generated before spawning, so the awaiting future
owns the staging path and its `Drop` reclaims the file even when the
outer future is aborted and the blocking task is detached. A detached
stage can never commit (the commit only runs after the stage succeeds
without cancellation), and the commit re-validates the leaf
handle-relative before renaming. Only a killed process can leave a
uniquely named staging file, which is never reused and never read as an
artifact. Run startup reaps such orphans (`.qcg-part-*` in the workspace,
`.tmp-*` in `checkpoint-blobs`) older than one hour without following
symlinks.

Platform guarantees:

- Unix: handle-relative traversal with `O_NOFOLLOW` for atomic replacements,
  reads, removals, and tree snapshots; directory fsync after replace and
  remove.
- Non-Unix (including Windows): canonicalize-based parent and leaf checks
  before each operation plus `MoveFileExW` replace and an ownership drop
  guard for staging; an ancestor swapped between validation and use by
  another process running as the same user is outside the guaranteed
  boundary. The cancel control mailbox has the same boundary: Unix pins
  the control directory with `O_DIRECTORY | O_NOFOLLOW` so concurrent
  writers cannot redirect create or publish, while non-Unix keeps only
  create-then-recheck and deployments that allow untrusted concurrent
  writers to the control directory must use Unix (E01).
- Non-Unix targets: there are no POSIX mode bits. Archived modes map only
  onto the read-only flag (an archived mode without any write bit lands
  read-only); executable bits do not round-trip. Implicit parents use
  platform defaults. Deterministic 0644/0755 modes are a Unix guarantee
  only.

Packages store only the masked 0777 permission bits (never file-type,
setuid, setgid, or sticky bits); executable bits are preserved deliberately
and dangerous bits are dropped, and restored scripts may be executed
directly: executability is a preserved contract property, not a side
channel. Unpacking sanitizes every archived mode
(`sanitize_restored_mode` in `crates/qcg-service/src/package.rs`): setuid,
setgid, and sticky bits are stripped by the `0o777` mask and the
world-writable bit is cleared, so an archived `0777` restores `0775`, never
world-writable. Least privilege for shipped files remains the generator
author's duty at pack time, and reviewers can see exactly what ships because
unpack applies precisely the archived modes after sanitizing. On Unix,
unpacking applies archived modes when present (every entry must carry an
explicit Unix mode; mode-less entries are refused, never defaulted from the
umask), defaults files to 0644 and directories to 0755, and creates implicit
parent directories with least-privilege 0700 regardless of the process umask
(`create_parent_dirs`); only implicit parents use 0700, explicit entries keep
their own sanitized modes. On non-Unix targets the same archive maps only
onto the read-only flag (an archived mode without any write bit lands
read-only; executable bits do not round-trip), and implicit parents use
platform defaults. Deterministic 0644/0755 modes are a Unix guarantee only.
Packing reads the author's live input tree and refuses any symlink inside it; a
non-cooperative same-user writer racing the pack is outside the boundary,
exactly like the Windows workspace boundary above.

Resuming a run verifies every revision the journal pinned: historical
revisions are checked against immutable checkpoint blobs under the run
metadata, and the workspace must match the latest pin per path. A missing or
mismatched blob is corruption and refuses resume. Forks copy every referenced
revision, not only the latest. File inputs are compared to the admitted bytes
on resume unless a successful step pinned the path, so tampering outside the
journal is refused.

`budget.max_elapsed_seconds` is a hard deadline anchored to the durable
acceptance timestamp (wall-clock `run_queued` instant): a node running past
it stops with a distinct elapsed error that is never retried, separate from a
node timeout or a cancellation. Enforcement after startup uses a monotonic
clock so an NTP step cannot stretch or shrink the budget; only the
resume-time remainder derivation reads the wall clock once (E11). Foreach child nodes honor their own retry
policy, per-attempt timeout, and this deadline exactly like top-level nodes.
The deadline runs from the durable acceptance timestamp, so it includes queue
wait and time spent waiting for a user question or confirmation; it is not
paused while a run is suspended or the service is restarted. An answer or
confirmation accepted before the deadline lets the run continue; the deadline
is re-evaluated at the next execution boundary, so a response accepted after
the deadline surfaces as an elapsed failure instead of extending the budget.
A stop additionally allows a bounded 5 second cooperative settle grace for the
executor to release resources; the outcome is still reported as elapsed and
never as success.

The durability model targets process termination: journal records, idempotency
mappings, and admission records survive SIGKILL and restart. Non-terminal
journal appends are not individually fsynced and survive a process crash via
the page cache (a host power loss may lose them — outside the guaranteed
boundary); terminal records are fsynced, and a crash-truncated tail is
repaired under the journal lock on next open. Host power loss and
storage-media failure are outside the guaranteed boundary; those would require
synchronizing every external side effect behind directory-entry durability.
Directory-entry durability means file bytes plus the naming directory entry
are both durable (data + parent directory fsync so a crash cannot lose the
rename). qcg applies it to `state.json`, workspace replacements/removals,
and idempotency records; journal appends use operation-driven fsync plus
repair instead. Terminal journal records and operation mappings
(`operation_started` / `operation_finished`) are fsynced individually with
parent directory sync (`JournalWriter::event` fast path and
`append_events_if` batch path in
`crates/qcg-engine/src/journal/writer.rs`); non-terminal appends
rely on the next terminal sync or clean shutdown, and a crash-truncated tail
without a trailing newline is repaired on the next open under the journal
lock. Repair marker: a terminal event writes `.clean_shutdown` next to the
journal; any later non-terminal append clears it. A truncated tail WITH the
marker refuses repair as possible tampering; WITHOUT it the tail repairs as
crash residue (complete JSON gains its newline, torn fragments truncate).
The marker must be a non-symlink regular file; unreadable markers fail
closed. See `docs/operations.md` durability section for the operator mapping
table, admission-records vs `.admission-*.lock` distinction, large-result
sidecar spill (64 KiB bound, `result_ref`), shared per-run journal pollers,
and second-precision snapshot durations for stable ETags. The model call-id
registry fails closed: the same call id with different args is refused, never
re-executed silently.

Every command permission must choose `container` or `trusted_host` isolation.
`trusted_host` grants execution as the qcg OS user. Stdio MCP servers follow
the same rule. Container backends share one lifecycle contract but enforce
isolation with family-specific mechanisms:

| family | runtimes | network | mounts | capabilities | rootfs | env |
|---|---|---|---|---|---|---|
| Docker-compatible | `docker`, `podman`, `docker_runsc` | none | workspace only at `/work` | all dropped, no-new-privileges, PID limit | read-only, bounded `/tmp` | cleared (`PATH`, run `TMPDIR`) |
| Incus-like | `incus`, `lxd` | no NIC device | workspace-only disk device | unprivileged, no nesting | image default | cleared; explicit flags only |

Digest pinning follows the family: Docker-compatible images use
`name@sha256:<hex>` enforced by the daemon; Incus-like images use
`<remote>:<path>@sha256:<fingerprint>` and launch by fingerprint so
exactly the pinned bits run (pre-pull fingerprints with `image copy`).
The declared
runtime is recorded in the command plan, and cleanup (stop/delete with a
Drop-path safety net) runs on success, error, cancel, and timeout for
every family. Managed MCP server processes additionally pass secret-backed
values through explicit spawn flags, which are briefly visible in the
host-local process table; prefer Docker-family runtimes for secret-heavy
MCP servers when that visibility matters.

Provider credentials are read from named environment variables or private
files selected by `api_key_file_env`; file-backed credentials are re-read per
request for rotation and must be absolute, non-symlink, UTF-8, at most 64 KiB,
and inaccessible to group/other users on Unix. Generator secrets support the
same bounded `file_env` model at run boundaries. Credentials are sent only to
the configured provider authentication location. Generator commands run
with a cleared environment containing only `PATH` and a run-local `TMPDIR`;
stdio MCP servers additionally receive only configured non-sensitive `env` and
explicitly mapped `env_from` values. Neither inherits provider credentials or
other parent-process state. Process-control and language-runtime injection
variables are rejected. Their stderr is discarded so an MCP child cannot copy
an `env_from` credential into qcg logs. Credential variables named by loaded
LLM, search, or MCP registry rows are reserved and cannot be redeclared as
generator secrets. Credentialed remote providers require
HTTPS, do not follow redirects, and fail closed when an upstream response
contains the exact active credential in its raw or JSON-decoded content.
Provider URLs and query parameters cannot interpolate the configured
credential environment variable, regardless of its name. LLM text and decoded
tool-call argument keys and values are checked recursively against declared
generator secrets before they enter run state.

The model catalog is metadata only. Declared models, optional discovery
(`models_discovery`), and the external `[catalog]` source never grant network,
command, or side-effect permission and never widen `permissions.network`; a
run still uses exactly the registered provider row the contract or operator
selection resolves to. Discovery requests reuse the provider's configured
credential, do not follow redirects, are time-bounded, and enforce a response
size limit. Credentials and environment-variable values are never returned by
`GET /api/llm/catalog` or `qcg models`. Fetched external catalogs are cached
under `$QCG_HOME` (or the configured path) as metadata; `sha256` pins fetched
bytes, and corrupt or stale caches are reported instead of silently trusted.

The `web.search` agent tool is an explicit opt-in. Its contract declaration
contains only the selected `provider`, `max_results`, and `max_calls` in
addition to the ordinary tool identity fields. The selected `[[search_provider]]`
row in the unified `providers.toml` registry fixes the endpoint, request
mapping, response mapping, and authentication. There is no implicit search
profile or fallback: the tool must name a profile or the registry must declare
`[default].search`; no default ships enabled. The bundled `tinyfish-api` row is the API-key REST
profile; it is separate from the OAuth MCP profile named `tinyfish`.

The selected profile reads its API key from the configured `api_key_env` and
injects it into the configured authentication location at runtime. The profile
host must still be declared in `permissions.network`; registry configuration
does not grant a contract capability. Credentialed remote profiles require
HTTPS and never forward credentials through redirects. Missing profiles or
credentials fail explicitly. Search responses are size-bounded by the HTTP
runtime limit, normalized before being returned to the model, and labeled as
untrusted data. Search snippets can contain prompt-injection text and must
never be treated as instructions. Result URLs are citations, not an implicit
permission to fetch those pages.

The generic `mcp` agent tool is also explicit opt-in. Its contract fixes the
profile `server`, remote `tool`, per-tool call budget, and whether the operation
is expected to have side effects. Streamable HTTP profiles allow only their
declared `allowed_hosts`, require every one of those hosts in
`permissions.network`, and use HTTPS for non-loopback endpoints. Stdio profiles
allow only their exact command vector through `permissions.commands`; the child
does not receive the qcg process environment.

MCP `tools/list` metadata is untrusted. qcg strips descriptive schema
annotations before putting a server schema into an LLM tool definition, then
validates each model argument against the original schema. When a server
advertises `outputSchema`, its `structuredContent` is validated before the
result enters the next LLM turn. External schema references are rejected, so
schema validation cannot initiate undeclared network or filesystem access.
Tool results remain untrusted data (the agent
guardrail tells the model not to treat them as instructions) and are scanned
for declared secret values before they enter the next LLM turn. The resend
cache for external operations is written only after the tool-output
guardrails and that scan pass: a rejected result is journaled as a
successful operation without a reusable result, so a resume refuses
automatic replay instead of re-executing the tool.

Streaming text is published as `llm_delta` events only after a holdback
window covering the longest registered secret clears: each arrival is scanned
jointly with the tail of already-published text, and the final suffix is
withheld until the completed response passes its scan. A rejected stream
therefore leaves no recoverable secret in the journal or event stream, while
clean streams keep incremental delivery.

MCP interactive input (`InputRequired`) passes the same credential-reflection
and size checks as completed tool results before its questions reach the UI
or journal. A suspended MCP call records its server continuation
(`mcp_input_pending`) so resuming answers the original remote request instead
of starting a duplicate one.

MCP OAuth uses authorization-server discovery, PKCE, state validation, and
single-use callback state. The default `oauth_store = "keyring"` keeps access
and refresh credentials in the OS credential store; `memory` is an explicit
ephemeral alternative. Tokens are never serialized into registry files,
generator packages, prompts, journals, artifacts, or logs. OAuth discovery,
authorization, token, and registration requests are restricted to the profile's
`allowed_hosts`, with bounded response bodies and no cross-host redirects.
The bundled TinyFish MCP profile therefore does not use `TINYFISH_API_KEY`, but
it requires interactive OAuth authorization from the browser Connections panel
on a loopback qcg server.

The process-level MCP runtime shares the authorized credential/token manager by
profile, but every generator run creates an independent MCP protocol session.
Each session inherits the run cancellation token, applies the profile timeout
(120 seconds by default), caps response bodies at 4 MiB by default, and closes
when the run ends. MCP discovery is limited to 100 pages and individual input
and output schemas to 256 KiB, with additional nesting, node-count, width, and
string-size bounds before schema compilation. A contract's
`side_effects = true` binding is denied by
`permissions.side_effects = "none"`, or pauses for the normal HITL
confirmation under `confirm` / `dry_run_first`; only a reviewed `allowed`
policy executes it without confirmation. Confirmation journals summarize only
argument names and encoded size. Each MCP profile selects `initialize` or
`discover`. Known servers are pinned to their verified lifecycle, and qcg never
silently retries a different protocol lifecycle after a negotiation failure.
qcg advertises MCP Tasks, polls task completion
within the bounded profile timeout, and sends task cancellation when the run is
canceled. Multi-round-trip `input_required` responses are converted into the
same durable, journaled HITL boundary as contract questions, then resumed with
the server-provided request state. Deprecated client sampling is not exposed.

Header authentication is preferred. When an upstream API requires query
authentication, qcg appends the secret only after checking the public endpoint
against `permissions.network`, disables redirects, removes the sensitive
parameter from returned URLs and HTTP errors, and rejects credential reflection
in the decoded response. Streaming responses apply the same decoded and
concatenated checks: raw SSE chunk matching plus per-chunk decoded JSON
inspection plus a running decoded window so split or escaped reflections
cannot bypass the single-chunk test.

`dry_run_first` records a plan confirmation, not a true simulation of
arbitrary external commands or HTTP calls. Adapters with genuine dry-run
support remain distinct; the policy name documents the plan-review boundary.

These controls reduce generator capability; they do not authenticate HTTP
callers or provide run ownership. Review a contract before approving it, keep
generator packages from trusted sources, and do not use `--yes` for unreviewed
contracts.

Secret declarations are capabilities: a contract can deliberately materialize
a declared secret with `inject_secrets`. Permission summaries therefore show
both the logical secret name and its environment-variable name. Treat any
contract requesting a sensitive variable as code requesting that credential;
deny it unless the package and intended output are trusted.

Safe relative paths reject empty paths, absolute paths, NUL bytes, backslashes,
`.` / `..`, and empty components. Directory-backed assets are canonicalized
and must remain below the canonical generator root, including through symlinks.

## Shutdown and restart semantics

Mirrors `docs/operations.md` shutdown section (normative operator text lives
there). On `SIGINT`/`SIGTERM` the server stops accepting new mutating
requests (they receive `503`); this covers start, fork, answer, confirm,
and cancel alike, including idempotent replays that would otherwise report
`Ok` without new work. Read requests (snapshots, events, artifacts)
stay admissible but their streams close as the drain proceeds, so only
mutating work is ever refused. Read-allowed-vs-stream-close means exactly
this: a `GET` during drain is not rejected with `503`, but its long-lived
body (SSE, journal stream, artifacts zip) ends at shutdown instead of holding
the drain open, and the client reconnects from its last sequence number. The
server closes SSE streams, stops maintenance tasks (shared-store refresh,
queued resumer, retention GC), then settles active runs under a 150 second
outer deadline (`SHUTDOWN_DEADLINE` in
`crates/qcg-server/src/server/serve.rs`, returned as `Err` to the embedding
host by `serve_with_listener_and_deadline`; see `docs/operations.md` for the
normative shutdown contract). Startup order is policy resolve, service
build, router build, then recovery before resident tasks; shutdown order is
signal, mutating-work gate, HTTP drain bounded by 30 s (`DRAIN_TIMEOUT`),
then resident-task and active-run settlement under the outer deadline. The
deadline starts after the HTTP drain
completes; HTTP drain is bounded by 30 s, so the total bound from signal to
exit is 180 s (30 s drain + 150 s outer; a wedged drain connection is cut
after 30 s with a warning and delays settlement by design).

Shutdown settles a cancel race as `Interrupted` when shutdown is already in
effect, else `Canceled` (checked at settle time); settled runs journal
`run_interrupted` and are terminal and not auto-resumed by the next startup.
Resume only applies to work waiting on human input or explicitly requeued.
Explicitly-requeued work (answered HITL suspensions re-entering the durable
queue via `requeue_answered_suspension`) is distinct from preemption (a
higher-priority arrival returning the lowest-priority running run to `Queued`
keeping its journal; equal priorities never preempt; finished steps replay on
resume). Host error reporting: deadline overruns and resident-task failures
are returned to the embedding host instead of being logged and ignored. Single-peer
enforcement: the lease-holding peer settles its own runs; leaseless peers
never touch others' runs. In
shared mode, stopping one peer settles that peer's tracked runs only: a
peer's runs are exactly the non-terminal runs in its own memory map (every
record there was admitted or adopted by that peer under its `owner_id`
claim), and a cancel from a non-owner during the drain is observed through
the shared mailbox within the resumer window. The library never calls `process::exit`;
only the `qcg` CLI binary exits on fatal errors, so embedding hosts always
observe shutdown as a returned outcome, never as process death.

## Browser surface

Generator assets are first-party UI served from the qcg origin. Asset responses
receive a restrictive policy allowing only same-origin script, style, images,
API/SSE connections, and the WASM evaluation mode needed by qcg's expression
runtime. JSON responses receive `default-src 'none'`. All responses use
`X-Content-Type-Options: nosniff` and `X-Frame-Options: DENY`.

CORS is off by default. When one or more exact `--cors-origin` values are
supplied, allowed request headers are `authorization`, `content-type`, and
`idempotency-key` (`apply_cors_layer` in
`crates/qcg-server/src/server/serve.rs`); cookies and credentialed CORS are
not supported. Availability is feature-gated: builds without the
`server-cors` cargo feature refuse configured origins at boot/router build
with an explicit error instead of silently serving without CORS
(`parse_cors_origins`). The bundled generator UI keeps a bearer token in
`sessionStorage` for the tab and sends it as an `Authorization` header on
API calls, event streams, and artifact downloads; the token never enters a
URL, a cookie, or `localStorage` (see `docs/http-server-guide.md`). Static
generator assets stay readable without a token so the UI shell can load;
every JSON API route requires the credential when one is configured.
Browser access that must be authenticated at a different boundary goes
through qpx, which injects the header after validating the caller.

## File inputs and outputs

File inputs use the inline `FileValue` JSON shape and are bounded only by an
explicit `[runtime] file_input_limit_bytes` in the generator contract.
The canonical input is recorded in the journal before it is
materialized below the run workspace. Journals therefore contain file contents
and must be protected like other run data.

Artifacts are constrained by the output manifest. To send an artifact outside
qcg, declare a command permission and execute a reviewed script as a confirmed
side effect. qcg has no implicit upload or exfiltration API.
