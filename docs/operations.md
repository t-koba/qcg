# Operations Guide

## Distribution layout

Keep `bin/qcg` next to `share/qcg/`. The bundle contains the `generator` demo,
unified provider registry, documentation, third-party notices, and SBOM. The
registry contains LLM `[[provider]]` rows, REST search `[[search_provider]]`
rows, and generic `[[mcp_server]]` rows. Test fixtures and frontend source are
not distributed.

Validate a bundle and start the loopback server:

```bash
./bin/qcg validate ./share/qcg/generators/generator
./bin/qcg serve --bind 127.0.0.1 --port 8080 \
  --runs-dir /var/lib/qcg/runs \
  --max-active-runs 8 \
  --run-store shared-filesystem
```

Open `/api/generators/generator/assets/ui/index.html` for the bundled SPA.

## Production topology

qcg uses the selected listener as-is. Set `QCG_API_TOKEN` when optional
instance-level bearer authentication is wanted. In production, qid may issue
user tokens while qpx terminates TLS, enforces
identity, and proxies accepted requests to the loopback qcg listener. The
three products remain independently deployed binaries.

qcg's bearer token authenticates the instance, not a user or run owner. One
deployment configures a single token per instance (`--api-token` /
`QCG_API_TOKEN`); there is no per-user or per-run token table inside qcg.
Use it behind a trusted shared boundary, or deploy separate qcg instances and runs directories
for trust domains that require isolation. See `docs/security.md` (trust
boundary) and `docs/http-server-guide.md` (bearer + CORS) for the full
boundary. Platform filesystem boundaries differ by OS: Unix uses
handle-relative `O_NOFOLLOW` traversal with directory fsync, Windows uses
canonicalize-based checks, and non-Unix targets map modes onto the read-only
flag only — see `docs/security.md` "Platform guarantees" for the normative
definition. The default `exclusive` run store
takes one directory lock. `shared-filesystem` enables active-active services
when the underlying storage provides reliable advisory locks: run-level leases
prevent duplicate execution, abandoned work is rescanned every 5 seconds, and
non-owner services follow the durable journal for SSE delivery (roughly
250 ms poll granularity rather than the 5 second rescan). Each process
carries an owner id; answers, approvals, and cancel requests are journaled
durably (`user_answered`, `user_confirmed`, `user_cancel_requested`) so the
owner merges peer progress on refresh and resumes answered prompts. Cancel
from a non-owner is observed within the 5 second refresh window.

The server default is eight concurrently executing API runs. Set
`--max-active-runs` or `QCG_MAX_ACTIVE_RUNS` to change the process-local limit.
Accepted work above that limit remains queued durably. Runs waiting for user
input or side-effect confirmation release their slot until resumed. All runs in one process share the configured LLM and
search provider runtimes and provider HTTP clients, so provider quotas and
credentials are shared. MCP OAuth token managers are shared by profile, but
each run owns an independent MCP protocol session.

API runs use a UUID-based run ID and separate `meta/journal.jsonl` and
`workspace/` directories below `--runs-dir`. Direct runs also reject a second
concurrent invocation targeting the same output directory. Direct (`qcg run`)
executions share the process execution permits with API runs, and warn when
their runs directory is owned by a live server, whose `max_active_runs`
otherwise does not apply across processes. A periodic resumer retries queued
runs that lost their engine task; queue admission time and position are
visible on run snapshots while queued.

## Search operation

Web search is contract-level opt-in. A contract must declare a `web.search`
agent tool and may select only its `provider`, `max_results`, and `max_calls`;
the selected `[[search_provider]]` profile supplies endpoint, request/response
mapping, and authentication. The bundled REST profile is `tinyfish-api`; there
is no implicit search profile or fallback. A tool may omit `provider` only when
the registry explicitly configures `[default].search`; no default ships enabled.

The profile host must be listed in `permissions.network`. The bundled TinyFish
REST profile requires `TINYFISH_API_KEY` in the qcg process environment.
Missing profiles or credentials fail explicitly.

## MCP operation

MCP is a generic contract-level capability. A generator declares a `mcp` agent
tool with a model-visible alias plus fixed `server`, `tool`, `max_calls`, and
`side_effects` fields. The runtime resolves the server profile from
`[[mcp_server]]`, connects over Streamable HTTP or stdio, discovers the remote
input schema, and validates every call. The bundled `tinyfish` profile uses
Streamable HTTP and OAuth and does not require a TinyFish API key; it is
separate from `tinyfish-api` and must first be authorized from the loopback
Connections panel.

Streamable HTTP profiles require each `allowed_hosts` entry in the contract's
`permissions.network`. Stdio profiles require the exact `command` vector in
`permissions.commands`; their child environment is cleared and receives only
`PATH`, configured non-sensitive `env`, and explicitly mapped `env_from` values.
OAuth credentials use the OS keyring by default. The SPA Connections panel
starts authorization on a loopback server, while the process-level token
manager is shared by profile across runs. Each run has its own bounded MCP
connection, timeout, cancellation token, and close operation. The client
prefers the 2026-07-28 `server/discover` lifecycle and explicitly supports the
2025-11-25 lifecycle for older servers.

Operational bounds are 120 seconds per MCP operation and 4 MiB per profile
response by default, with at most 100 discovery pages and 256 KiB per
discovered schema plus structural complexity limits. MCP side effects use the same `none`, `confirm`,
`dry_run_first`, and `allowed` policy as other agent tools; confirmation pauses
the run and releases its execution slot. qcg advertises the current MCP Tasks
extension, polls accepted tasks within the profile timeout, and cancels the
remote task when the run is canceled. Multi-round-trip `input_required`
responses use the durable generator HITL boundary and resume the original tool
call with its request state. Deprecated MCP sampling is not exposed.

## Run retention

`qcg serve` periodically retains the newest 50 terminal run directories plus
10 additional failed runs and deletes any run past its contract retention
window. `QCG_GC_KEEP`, `QCG_GC_KEEP_FAILED`, and `QCG_GC_INTERVAL_SECS`
(default `86400`, minimum `60`) retune the sweep; invalid or zero values
refuse boot. Set `QCG_AUTO_GC=0` to disable automatic retention and use:

```bash
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50 --keep-failed 10 --delete
```

GC never removes a non-terminal run. The first command is a dry run.
Failed runs keep an additional `--keep-failed` budget (default 10) for
post-mortems even when they fall outside `--keep`.

Retention is count-based only; there is no byte or disk-usage ceiling in
mechanism. Size the `runs-dir` volume externally and export a run bundle
(`qcg runs export`) before GC when audit evidence must survive deletion:
deletion removes journals, FileValue content, and artifacts together.
Queue depth beyond 10,000 entries refuses snapshots instead of degrading
silently; treat sustained growth as capacity policy owned by the orchestrator.
No aging, quota, or starvation avoidance exists: low-priority runs wait in
priority order with FIFO ties by design.

Journals include inline FileValue content and should be protected as sensitive
run data. `qcg runs show` summarizes file values by name, decoded bytes, and
SHA-256 instead of printing base64.

## Required limits for production

Every byte/count limit is explicit-only: `None` (omitted) means no mechanistic
limit. Defaults favor local development; production operators must set bounds
or a single oversized input, package, artifact, or LLM context can exhaust
memory or disk. Minimum set:

- `[runtime]` in `qcg.toml`: `file_input_limit_bytes`, `file_count_limit`,
  `input_total_limit_bytes`, `output_file_limit_bytes`,
  `output_total_limit_bytes`, `output_artifact_limit`,
  `command_input_limit_bytes`, `command_output_limit_bytes`,
  `http_body_limit_bytes`, `template_source_limit_bytes`,
  `template_context_limit_bytes`, `template_output_limit_bytes`,
  `state_limit_bytes`, plus `journal_event_limit_bytes`,
  `journal_total_limit_bytes`, and `journal_event_count_limit`.
- LLM nodes: `max_context_bytes` / `max_context_tokens`, and
  `max_media_bytes` when `params.media` is declared.
- Resources: explicit `params` bounds (`max_bytes` for `file`, `url`,
  `openapi`, and `exec`; `max_files`, `max_bytes`, `max_depth`,
  `max_entries`, `max_selected_bytes` for `dir`, `skill`, and
  `skill_library`).
- `qcg package`: `--max-entries`, `--max-bytes`, `--max-metadata-bytes`.
  Unbounded packaging trusts the source tree; never package untrusted trees
  without bounds.

`[budget]` (`max_steps`, `max_tokens`, `max_cost_usd`,
`max_elapsed_seconds`) is the run-wide backstop and should always be set for
server-hosted generators.

## Skill resources and diagnostics

`skill` and `skill_library` resources follow the agentskills.io format.
Names, descriptions, and unparseable frontmatter are hard errors at contract
load; soft violations (name length, character set, or a name that differs from
the directory name) are recorded as diagnostics on the run's resource snapshot
and logged with `tracing` instead of failing the run. Validate a skill or a
library before packaging with:

```bash
qcg skill validate path/to/skill
qcg skill validate --library path/to/skills
qcg skill validate --json path/to/skill
```

The CLI exits non-zero on hard errors and prints soft diagnostics. Skills are
always data: `allowed-tools` is informational, and loading a skill never grants
command, network, or write permissions.

## Verification

From the source tree, run:

```bash
bash scripts/check-ci-local.sh
bash scripts/check-demo-local.sh
```

The demo check generates OpenAPI and WASM bindings, validates the frontend,
runs browser tests against both the Vite proxy and assets served by qcg, and
checks the distribution bundle.

When a qpx binary is available, verify the documented deployment boundary with:

```bash
cargo build -p qcg
QPXD_BIN=/path/to/qpxd bash scripts/e2e-qpx-smoke.sh
```

## Artifact delivery

External delivery is modeled as a generator command, not a server feature.
Declare the exact script invocation in `permissions.commands`, declare the
required network host, and mark the step `side_effects = "confirm"`. Operators
then review and approve the delivery at the same HITL boundary as any other
side effect.

## Unattended operation

qcg ships no scheduler, timer, or webhook receiver. Mechanism (durable runs,
pre-provisioned answers, idempotent mutations) lives in qcg; policy (when to
run, how often to retry, whom to notify) lives outside in cron, systemd
timers, or a workflow orchestrator. A run whose questions and confirmations
are all pre-provisioned completes without interaction.

Prerequisites for any unattended run:

- The contract is reviewed. Pre-provisioning answers and approvals bypasses
  human review, so treat the provisioned values as part of the deployment.
- Every `ask_user` question id and every confirmation id is known in advance.
  Confirmation ids are never the 2-element `<node>:<kind>` form (mirror of
  the normative Q1 definition in `docs/contract-reference.md`). The exact
  forms are:

  | scope | confirmation id form | authorizes |
  |---|---|---|
  | `content` | `<node>:<kind>:<operation_digest>` (3 parts) | identical content until the run ends |
  | `invocation` (default) | `<node>:<kind>:<operation_digest>:<invocation_hash>` (4 parts) | one call only |

  `operation_digest` is hex SHA-256 over the target plus a `0` byte plus the
  canonical details JSON (`operation_digest(target, details)` in
  `crates/qcg-engine/src/engine/run_context.rs`). `invocation_hash` is hex
  SHA-256 over the invocation id. Single-shot steps use
  `execution:<node>:<count>` (finished-execution count, so a repair or
  regenerate is a new invocation); agent tool calls use the stable model call
  id. The operation id (`run:node:sha256(invocation)` in
  `crates/qcg-engine/src/state.rs`) is a different value: it is the remote
  idempotency key, never a confirmation id. The manifest
  `permissions.side_effects_scope` defaults to `invocation` when omitted;
  every minted `ConfirmSpec` carries an explicit `scope` and
  `operation_digest`, and a confirmation without either is corrupt and fails
  closed (never an invocation-scoped default).
- `side_effects = "confirm"` steps are pre-approved per confirmation id, or
  the generator uses `side_effects = "allowed"` / `"none"`.

Predict the id from a prior interactive run instead of guessing it: read the
`confirm_request` event or the run snapshot `confirm.id` (both carry the
full id including scope and invocation hash). The journal `side_effect` /
`dry_run` events carry only the content `operation_digest`, which predicts
3-part content-scope ids alone but never a 4-part invocation-scoped id. The
`--confirm` value must be the full 3-part or 4-part id for the run's
`permissions.side_effects_scope`, e.g. `publish:http:<64hex>` for content
scope or `publish:http:<64hex>:<64hex>` for invocation scope (the default). MCP approvals live in two namespaces that
never mix: single-shot `mcp.call` steps bind the node execution
(`execution:<node>:<count>`), while agent `mcp` tool calls bind the model
call id plus canonical redacted args
(`<node>:agentmcp:<alias>:<invocation_hash>#__mcp_pending` continuation key in
`crates/qcg-llm-steps/src/tool_events.rs` (the `#__mcp_pending` suffix marks stored continuations)), so one can never authorize the
other. Doc-to-key conformance: the `<node>:agentmcp:<alias>:<64hex>#__mcp_pending`
format is pinned by `frontend/generator/src/confirm-scope.test.ts` ("mcp
continuation key format") against a captured real key shape, since the key
constructor lives outside the docs scope.

### cron

```cron
# Daily 02:30 unattended generation. --yes disables prompting; unanswered
# questions and unapproved confirmations pause or fail the run instead.
# The confirmation id is the full 3-part (content scope) or 4-part
# (invocation scope) form, e.g. publish:http:<64hex> or
# publish:http:<64hex>:<64hex>; copy it from a prior
# confirm_request event, never the 2-element <node>:<kind> form.
# The examples below assume side_effects_scope = "content" (3-part ids);
# with the default invocation scope, use the 4-part id from the
# confirm_request event instead.
30 2 * * * /usr/local/bin/qcg run /srv/qcg/generators/report \
  --input date=$(date +\%F) \
  --answer scope=brief \
  --confirm publish:http:<64hex>=true \
  --output /srv/qcg/out --yes >>/var/log/qcg/report.log 2>&1
```

`--confirm` takes `ID=approve|deny` pairs; `--confirmations-file` accepts the
same mapping as a JSON id-to-boolean object. `ID` is the full confirmation
id from the table above. Decisions apply only to server-minted pending ids
by exact match: an unknown id (including an unknown scope) matches nothing
and is refused, so deny stays safe to offer unconditionally while approve
is disabled for unknown scopes in the UI.

### systemd timer

```ini
# /etc/systemd/system/qcg-report.service
[Unit]
Description=Unattended qcg report generation
After=network-online.target

[Service]
Type=oneshot
User=qcg
Environment=QCG_API_TOKEN_FILE=/etc/qcg/api-token
ExecStart=/usr/local/bin/qcg run /srv/qcg/generators/report \
  --input date=%Y-%m-%d \
  --answer scope=brief \
  --confirm publish:http:<64hex>=true \
  --output /srv/qcg/out --yes
```

```ini
# /etc/systemd/system/qcg-report.timer
[Unit]
Description=Daily unattended qcg report generation

[Timer]
OnCalendar=*-*-* 02:30:00
Persistent=true

[Install]
WantedBy=timers.list
```

### Event-driven API loop

For event triggers (queue messages, file arrival, webhooks received
elsewhere), drive the server API from an external loop. Pre-provisioning
makes single-shot runs finish without follow-up calls; poll or subscribe for
runs that may still pause:

```bash
BASE=http://127.0.0.1:8080
RUN=$(curl -fsS -X POST "$BASE/api/runs" \
  -H "Authorization: Bearer $QCG_API_TOKEN" \
  -H "Idempotency-Key: report-$(date +%F)" \
  -H "Content-Type: application/json" \
  -d '{"generator_id":"report","inputs":{"date":"2026-09-06"},
       "answers":{"scope":"brief"},"confirmations":{"publish:http:<64hex>":true}}' \
  | jq -r .run_id)
curl -fsSN "$BASE/api/runs/$RUN/events" -H "Last-Event-ID: 0" \
  -H "Authorization: Bearer $QCG_API_TOKEN"
```

Event-stream client contract: history replays first, then the live tail.
If the client falls behind the live buffer it receives a `lagged` marker
carrying the last actually-delivered sequence number — never a fabricated
cursor — and the stream ends. The client must reconnect with that number
as `Last-Event-ID`; the journal replay then yields the next real event,
so ignoring the marker (instead of reconnecting) is the only way to lose
events, and that is the client's responsibility. A stream for an
already-settled run returns history and ends immediately.

## Durability guarantees

The durability model targets process termination (including SIGKILL) and
restart. Host power loss and storage-media failure are outside the guaranteed
boundary; those would require synchronizing every external side effect behind
directory-entry durability (see below).

Directory-entry durability means both the file bytes and the directory entry
that names them are durable: the data reaches the disk and the parent
directory is fsynced so a crash cannot lose the rename that installed the
file. qcg applies this to run metadata (`state.json` atomic replace plus
parent directory sync in `persist_serialized_atomic`), workspace atomic
replacements and removals, idempotency records, fork journals and blobs, and
large operation-result sidecars (file plus parent directory sync);
journal line appends rely on terminal-only fsync plus repair (next table),
not on a per-append directory fsync. Host power loss and storage-media
failure stay outside the guaranteed boundary as stated above, so no test
fault-injects them: the table below pins the process-crash contract that
native tests do cover.

Terminal-only fsync mapping (`crates/qcg-engine/src/journal/writer.rs`):

| path | what is fsynced | when |
|---|---|---|
| `JournalWriter::event` fast path | `file.sync_data()` on the journal file plus parent directory sync | when the event is terminal or operation-driven (`operation_started` / `operation_finished` set `needs_sync`; see `JournalWriter::event` in `writer.rs`) |
| `append_events_if` batch path | `file.sync_data()` on the journal file plus parent directory sync | when the batch contains a terminal or operation event (`needs_sync`) |
| torn-tail repair `truncate_to` / newline commit | `sync_data()` after truncate or newline commit | every repair |
| `state.json` persist | atomic write + replace via `persist_serialized_atomic` | every append (state always follows the journal) |
| `.clean_shutdown` marker | plain `write` / `remove_file`, no fsync | best-effort I/O, failures propagate (a failed terminal marker fails the operation; see repair marker below) |

Cancel mailbox (`request_remote_cancel` in `crates/qcg-service/src/run_dirs.rs`):
post-publish directory-sync failure is warn-only by necessity — reporting an
error would make the caller retry under a fresh operation id and journal a
duplicate cancel for one published request. Power loss may drop the directory
entry; the next boot observes the request as absent and a retry publishes
under a fresh operation id, with deduplication by cancel-drain idempotence
(E01). Idempotency Ready commit (`store_durable_ready` in
`crates/qcg-server/src/server/idempotency/durable.rs`): directory-sync
failure is likewise warn-only (the staged file is `sync_all` durable); a
power-loss entry loss resurrects the key as unclaimed and a retry re-commits
the same mapping instead of minting a duplicate. Duplicate execution under
power loss is accepted there (E02/Q2).
Non-terminal appends are not individually fsynced; they rely on the next
terminal sync or clean shutdown for media durability. A crash-truncated tail
without a trailing newline is repaired on the next open under the journal
lock: complete JSON without its newline gains the newline; torn JSON or a
partial fragment is truncated so prior newline-terminated events survive and
the next append does not fuse lines.

Repair marker (tamper vs crash): a terminal event writes the
`.clean_shutdown` marker (exact magic content `qcg-clean-shutdown-v1`)
next to the journal; any later non-terminal append
clears it (`writer.rs`: `write_clean_shutdown_marker` /
`clear_clean_shutdown_marker`). A truncated tail found WITH the magic
marker present refuses repair as possible tampering (durable history was
damaged after a clean shutdown); WITHOUT the marker the truncation is a
crash remnant and repairs as above. The marker must read as a non-symlink
regular file with the exact content; an unreadable marker fails closed and
never repairs. A terminal marker write or clear failure fails the
operation before success is reported (Q2): warn-only would leave a sealed
run without its shutdown claim, so both propagate and a continued run never
carries a stale shutdown claim. The `failed_terminal_marker_fails_the_event_operation`
native test pins this fail-closed behavior.

Admission records vs `.admission-*.lock`: they are different mechanisms.

- Admission records are persistent: idempotency mappings under
  `<runs-dir>/idempotency/` (24-hour TTL, survive restarts and shared-store
  peers) plus the durable `run_queued` journal event. A reused
  `Idempotency-Key` with identical content replays the original result; the
  same key with different content is rejected with `409 Conflict`. The
  pre-execution ownership recheck cannot fully close the window before
  execution starts: a successor that acts in between may let the stale owner
  briefly start its engine. Such surplus execution never commits and is
  canceled by the orphan settlement at commit time, so every stale execution
  converges or cancels and never leaks (E02).
- Per-run admission locks are small, fixed-size files next to the run
  directories (`.admission-*.lock`). They are intentionally never unlinked:
  removing a locked file would let a later admission lock a fresh inode while
  the current holder still owns the old one. They hold no run state, are
  stateless coordination only, and can be ignored by operators.

Large operation results spill to a sidecar blob under the run meta dir when
they exceed 64 KiB (`OPERATION_RESULT_MAX_BYTES` in
`crates/qcg-engine/src/state.rs`); the journal carries `result_ref` (the
content-hash blob name) instead of the inline bytes, and the dir-backed guard
reloads the blob on resend. Shared per-run journal pollers serve all SSE
subscribers of one run from a single poll task
(`journal_pollers` in `crates/qcg-service/src/types.rs`). Snapshot live
`duration_ms` is quantized down to whole seconds so exact-digest ETags stay
stable within a second and conditional requests can return 304; crossing a
second boundary advances the body (and the validator) even when nothing
else changed, so an unrelated `200` there is correct, not stale
(`live_metrics` in `crates/qcg-service/src/summaries/metrics.rs`).

A reused `Idempotency-Key` with identical content replays the original result
instead of starting a duplicate run; the same key with different content is
rejected with `409 Conflict`. Records persist for 24 hours under
`<runs-dir>/idempotency/` and survive restarts and shared-store peers. If a run still reaches `Waiting` or
`Confirming`, answer mechanically with
`PUT /api/runs/{id}/questions/{qid}` or
`PUT /api/runs/{id}/confirmations/{cid}`. `GET /healthz` and `GET /metrics`
cover liveness and Prometheus monitoring.

### Installed package repair

Installing an already-installed parent re-verifies its file inventory and
re-walks its dependency closure instead of returning success on the parent
alone, so an interrupted install is repaired by re-running the same command;
there is no rollback of the already-committed parent. Dependencies that
cannot be resolved or fetched fail the repair closed: a partial closure is
never recorded as complete. Direct path installs (a local directory or file
instead of a registry package) also walk the dependency closure against the
configured registries after committing the parent; a missing set fails the
install with the partial state and the repair (rerun the same command)
instead of lingering silently. Crash recovery is not an automatic rollback:
a commit interrupted after the backup rename leaves the target missing with
a `.qcg-install-backup-*` sibling intact; rerun the install to converge (or
manually rename the newest backup back to the target). Backups are retained
on failure paths, never auto-deleted, and only reaped by the stale sweep
after aging out.

## Shutdown and restart semantics

On `SIGINT`/`SIGTERM` the server stops accepting new mutating requests (they
receive `503`); this covers start, fork, answer, confirm, and cancel alike,
including idempotent replays that would otherwise report `Ok` without new
work. Read requests (snapshots, events, artifacts) stay admissible but their
streams close as the drain proceeds, so only mutating work is ever refused:
a `GET` during drain is not rejected with `503`, but its long-lived body
(SSE, journal stream, artifacts zip) ends at shutdown instead of holding the
drain open, and the client reconnects from its last sequence number. SSE
shutdown closes with an explicit `shutdown` marker event so clients
distinguish shutdown from truncation (a terminal close carries its terminal
event, a truncation carries neither). It
closes SSE streams, stops maintenance tasks (shared-store refresh, queued
resumer, retention GC), and then settles active runs under a 150 second
outer deadline (`SHUTDOWN_DEADLINE` in
`crates/qcg-server/src/server/serve.rs`, enforced by
`serve_with_listener_and_deadline`); exceeding it is reported to the
embedding host instead of
being logged and ignored. Total bound from shutdown signal to process exit
is 180 s (`TOTAL_SHUTDOWN_BOUND` = `DRAIN_TIMEOUT` 30 s + `SHUTDOWN_DEADLINE`
150 s): the outer deadline starts after the HTTP drain completes, so a
wedged drain delays settlement by design (cut after 30 s with a warning)
and the total never exceeds 180 s. Startup order is resolve deployment policy once,
build the service, build the router, then start recovery
(`resume_recovered_runs`) before resident tasks; shutdown order is signal,
stop accepting mutating work (shared token), HTTP drain bounded by 30 s
(`DRAIN_TIMEOUT`), mark the service shutting down, then resident-task
shutdown and active-run settlement concurrently under the 150 s outer
deadline. Resident-join and run-convergence run concurrently by design
(E05): serializing them would let one consume the other's settlement
budget, so the outer deadline covers both together. Responsibility split
(E05/Q3): the HTTP drain (30 s) ends accepting work, resident-task shutdown
stops the execution sources (shared token, GC, resumer, shared-store
refresh), active-run settlement converges the runs, and the resume exception
(tracked runs terminate; only a durable `Queued` journal never tracked by the
stopping peer resumes next boot) decides what restarts. Q3 owns the
terminate-vs-resume policy; E05 owns the drain/join/settle mechanism. The outer deadline starts after the HTTP drain completes:
in-flight requests that drain promptly do not consume the
active-run settlement budget, while a wedged drain connection is cut after
30 s so shutdown proceeds (the drain timeout is warned, never silent).

Shutdown signal platform matrix: Unix waits for Ctrl-C or SIGTERM;
Windows waits for Ctrl-C plus the console/service controls tokio exposes
(`ctrl_close`, `ctrl_break`) as the SIGTERM equivalents; other targets wait
for Ctrl-C only. A wait failure still proceeds to shutdown (fail-safe).
Settled runs are terminal:
all tracked non-terminal runs, including `Queued`, are journaled as
`run_interrupted` and are not auto-resumed by the next startup. A durable
`Queued` journal never tracked by the stopping peer (a pre-existing adopted
orphan admitted nowhere) keeps its queue and resumes on the next boot. This
tracked vs durable-queue resume exception is the single source of truth; the
`contract-reference` mirror must match it. A restart
therefore never conflates "server process restarted" with "resume every
run"; resume only applies to work that was waiting on human input or was
explicitly requeued before the shutdown began. A cancel racing the shutdown
settles as `Interrupted` once shutdown started, else `Canceled`. Single-peer
enforcement: the lease-holding peer settles its own runs; leaseless peers
never touch others' runs. In shared mode, stopping one peer settles that
peer's runs only (exactly the non-terminal runs in its own memory map);
other peers keep their own runs, and an Exclusive owner cannot start while
any shared peer holds the store (and vice versa). Fork follows the same shutdown gate as
start: it is refused with `503` during drain, never half-copied.

Unattended limits:

- MCP OAuth authorization needs a browser on loopback and cannot run
  unattended. Authorize once from the loopback Connections panel, then persist
  the OS keyring entry per deployment policy (backup and rotation are operator
  policy; qcg never exports tokens). Headless or container renewal without a
  prior authorized keyring fails closed.
- A denied confirmation ends the run as `Failed`; retries, backoff, and
  notifications are the orchestrator's job, not qcg's.
- `qcg serve` retains the newest 50 terminal run directories plus 10 failed
  runs and sweeps every 24 hours; size the `runs-dir` volume and retention
  for the schedule above.
- `Waiting` and `Confirming` runs release their execution slot but have no
  TTL in mechanism; an unanswered run stays paused until answered, confirmed,
  or canceled. Expire it from the orchestrator with `cancel` when policy
  requires. Note `budget.max_elapsed_seconds` covers queue plus HITL dwell.

## Observability

- `GET /healthz` is liveness only (`ok` plus `max_request_bytes`); readiness,
  queue depth, lease, and GC state are not reported there.
- `GET /metrics` exposes gauges only: durable runs by state, active, queued,
  waiting, confirming, in-flight preempted (`qcg_runs_preempted`), distinct
  generators, and per-generator top-20 counts.
  Latency histograms, error rates, queue dwell, preemption totals, and GC
  deletion or failure counts are not exported; alert thresholds live outside.
- OTLP export exists in implementation (`QCG_OTLP_ENDPOINT`,
  `QCG_OTLP_INTERVAL_MS`, best-effort) and `qcg runs trace` builds
  hierarchical spans, but no SLO is defined here. Treat traces as mechanism
  facts for external analysis.

## Priority scheduling and preemption

`POST /api/runs` and `POST /api/runs/{id}/fork` accept an optional integer
`priority` (default 0, higher runs first). Queued runs start in priority
order with FIFO ties; `queue_position` in snapshots follows the same order.
When all execution slots are busy and a higher-priority run arrives, the
lowest-priority running run is preempted: it returns to `Queued` keeping its
journal, and already finished steps replay on resume instead of re-running.
Equal priorities never preempt each other, and cancellation during backoff or
a concurrent `cancel` wins over the resume. Priorities persist in the run
journal, so rehydrated queues keep their order across restarts.

## Run families: fork links and await

`POST /api/runs/{id}/fork` records the source as `parent_run_id`, visible in
snapshots and durable across restarts. A flow can join sibling runs with an
`await` node (`runs` plus optional `timeout_secs`), which succeeds with an
id-to-state map once every listed run is terminal. Unknown runs and timeouts
fail explicitly. Typical fan-out/fan-in: start or fork children, then await
them from a collector run before proceeding.

Queued snapshots carry RFC 3339 queued_at stamped once at admission and
shared by journal and memory; queue_position follows priority then queued_at
then id (E16). Memory state is only a fallback for journals written before
the stamp existed; the journaled instant always wins, so every process
observes identical FIFO order. Position merges live and on-disk queued runs
under one FIFO; stores beyond 10,000 entries refuse the snapshot with an
internal error instead of letting latency grow with run count. The on-disk
half is served from a 1 s process-wide cache shared across subscribers (at
most 128 runs-directory keys with whole-map clear on overflow): only
disk-only peer runs can lag a queue move by at most the TTL, which delays
position display without misordering execution (the scheduler never consults
this cache). A lagging position still serves the exact-body validator for
the lagging body, so the ETag stays exact for what is served — display
staleness by design, never a mismatched validator (E12/E16).
