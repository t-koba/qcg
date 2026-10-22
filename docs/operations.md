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

qcg uses the selected listener as-is. Set `API_TOKEN` when optional
instance-level bearer authentication is wanted. In production, qid may issue
user tokens while qpx terminates TLS, enforces
identity, and proxies accepted requests to the loopback qcg listener. The
three products remain independently deployed binaries.

qcg's bearer token authenticates the instance, not a user or run owner. One
deployment configures a single token per instance (`--api-token` /
`API_TOKEN`); there is no per-user or per-run token table inside qcg.
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
`--max-active-runs` or `MAX_ACTIVE_RUNS` to change the process-local limit.
Accepted work above that limit remains queued durably. Runs waiting for user
input or side-effect confirmation release their slot until resumed. All runs in one process share the configured LLM and
search provider runtimes and provider HTTP clients, so provider quotas and
credentials are shared. MCP OAuth token managers are shared by profile, but
each run owns an independent MCP protocol session.

API runs use a UUID-based run ID with `meta/journal.jsonl`, `workspace/`, and
a pinned `generator-package/` tree inside each run directory below `--runs-dir`. Direct runs also reject a second
concurrent invocation targeting the same output directory. Direct (`qcg run`)
executions share the process execution permits with API runs, and warn when
their runs directory is owned by a live server, whose `max_active_runs`
otherwise does not apply across processes. A periodic resumer retries queued
runs that lost their engine task; queue admission time and position are
visible on run snapshots while queued.

## Container daemon operation policy

Container daemon budgets are deployment policy, shared by engine command
execution and container-isolated MCP sessions. Configure them only through
these startup environment variables (seconds):

| Variable | Default | Applies to |
| --- | ---: | --- |
| `CONTAINER_CREATE_TIMEOUT_SECS` | 600 | Managed instance create/init |
| `CONTAINER_START_TIMEOUT_SECS` | 120 | Managed start and readiness (each) |
| `CONTAINER_ADMIN_TIMEOUT_SECS` | 60 | Daemon discovery and configuration (each command) |
| `CONTAINER_STOP_TIMEOUT_SECS` | 30 | Cleanup stop/kill/delete/rm (each command) |

Every value must be an integer in **1..=3600**; unset selects the documented
operation default. Malformed (including empty or non-Unicode), zero,
overflowing, and out-of-range values fail closed before server bind and before
CLI execution starts. The CLI resolves the bundle for `run` except `--plan`,
`eval`, `runs replay/fork`, and `serve`/`dev`. Non-executing operations such
as `qcg models`, `qcg runs invalidate`, `qcg runs delete`, and
`qcg runs export` do not validate or consume daemon policy; service construction
uses defaults. Unrelated commands such as `qcg new` are unaffected.
Startup resolves one frozen bundle and passes it through execution service,
engine command gateways, and MCP runtimes. Operations never reread these
variables; change them and restart to apply a new deployment policy.

Every provision/session captures the bundle before daemon side effects,
including partial provisions. Normal awaited teardown, synchronous teardown,
detached Drop cleanup, and inline cleanup after thread-spawn failure use that
same captured stop budget. A failed stop/kill still attempts delete/rm;
a failed cleanup retains the existing reporting/retry semantics.

If a timed-out daemon client must be killed, cleanup may spend up to an
additional **100 ms per client** on local process reaping after the daemon
operation deadline. Any direct client still unreaped is retained and reported
as outstanding by the shutdown cleanup join; the process reaping allowance
does not change the configured server shutdown wait budget. Descendants of a
daemon client are outside local reaping ownership.

These values do not belong in generator contracts, MCP profiles, per-run
requests, or workload timeouts. Docker-compatible backends keep one-shot
`run` with their existing workload timeout, not the managed create/start
budgets; the stop budget applies only to cleanup. Readiness probes retain
their 500 ms retry cadence, and the start budget also bounds a stuck probe.
Server drain/settlement budgets are independent and unchanged: raising daemon
budgets does not lengthen shutdown deadlines or guarantee cleanup finishes
before those deadlines. Inspect reported cleanup failures/outstanding work.

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
Each profile also carries an operator-owned `allowed_destinations` ceiling for
server-side fetch (hosts extracted from whole-value `http(s)` URL arguments,
empty by default which denies every extracted host). To allow a fetch
destination, add its host to the server's row in `providers.toml` and restart
qcg; tightening the list applies retroactively to resumed calls. A row may
additionally configure a fallback-only `destination_decider` command plus
bounded `destination_decider_timeout_seconds` for a static miss the per-tool
narrowing admits; the decider takes hosts-only input, must answer strict
`{v,allow}`, denies fail-closed, and its verdict is journaled as
`mcp_destination_decision` with fresh re-invocation on resume. Denials are
`Refused` `tool_call` failures naming the offending host, and the extracted
hosts ride the `operation_started` details alongside the argument summary for
audit.
OAuth credentials use the OS keyring by default. Keyring entries live in the session's unlocked login collection (the Linux Secret Service default collection), not in a backable file, and do not survive container restart by themselves; WSL has no default collection. The SPA Connections panel
starts authorization on a loopback server, while the process-level token
manager is shared by profile across runs. A headless host without an unlocked provider fails closed with an actionable OS-keyring-unavailable error; either run an unlocked provider or accept `oauth_store = "memory"` plus loopback re-authorization after every restart. Each run has its own bounded MCP
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

## Run invalidation

A run that completed successfully can later be found wrong. Use
`qcg runs invalidate <id> --reason "..." --actor-label "quality-review"`, or
`POST /api/runs/{id}/invalidation` with those JSON fields. Only durable success
qualifies; invalidation never changes `Succeeded`, appends an execution event,
or changes the journal sequence/outcome. It writes one immutable
`meta/invalidation.json` containing run id, reason, descriptive actor label,
and `invalidated_at` timestamp. Actor labels are not authenticated identities.
The HTTP authority is identical to run deletion, including disabled-auth mode.
Reason (4096 UTF-8 bytes maximum) and actor label (128 bytes maximum) must be
nonblank and control-free. Identical retries return the original record;
different values conflict. No un-invalidate is provided.

`latest_success` skips invalidated results, falling back to an older success.
A pinned `run_id` refuses with the recorded reason before other eligibility
checks. `latest_terminal` keeps selecting the newest terminal run and refuses
an invalidated selection rather than falling back. Already-persisted run-ref
material is unaffected on resume. Forking remains possible and creates an
independent run with no inherited invalidation.

Status, lists, CLI summaries, and bundle snapshots expose the structured record.
Readers bypass the execution-journal cache for this metadata. Corrupt,
unreadable, linked, or mismatched records fail closed for status and source
selection; do not remove or edit the sidecar to restore eligibility. Journals,
artifacts, and audit evidence are preserved. Invalidation does **not** change
retention budgets or expiry: these runs still count as successful terminal
runs, age out normally, and can be explicitly deleted. Export a bundle before
deletion when evidence must survive.

Invalidation, service deletion, automatic GC, and CLI deleting GC coordinate
through the stable store-level `.maintenance.lock`; service deletion and GC
acquire it before coordination leases. Preemption acquires a fixed-sharded
`.handoff-shards/` lease before canceling a victim and holds it through durable
requeue. Service deletion and GC probe admission, handoff, and execution
leases, then recheck terminal state immediately before removal; the admission
shard prevents same-ID creation from racing deletion.
A contended deletion reports a retryable conflict; service GC skips contended
candidates rather than waiting. Maintenance, admission, and handoff
coordination files live outside run directories; shard counts are fixed.
Per-run execution locks live
in run metadata. Never unlink a coordination file while any process uses the
store.
Publication syncs a same-directory temporary file, atomically renames it to
the authoritative name without replacement under the store lease, and syncs
the directory on Unix. A crash before publication leaves only an ignored
staging file; after publication, readers see the complete record. Exact retries
can safely inspect that record and return the original metadata.

## Run retention

One retention mechanism serves every sweep path: `qcg serve`'s periodic task,
the sweep that precedes `qcg run`, and the manual `qcg runs gc`. Its budget is
resolved once from the deployment environment. `keep` (default 50) retains the
newest nonfailed terminal runs (success, canceled, or interrupted); failed
runs do not consume that budget and are governed only by the separate
`keep_failed` budget (default 10). An explicit `--keep`/`--keep-failed`
overrides the resolved count for that sweep. With defaults, a sweep may retain
up to 50 nonfailed terminal runs plus 10 failed runs.

Expiry comes from the run's journal: when the contract declared a retention
window, the run recorded it when it started, and a sweep expires that run on
the recorded window — a run past its window is collected even when the count
budget would keep it, while a run inside its window is still governed by the
count budgets. Without a declared window only the count budgets apply. A
sweep never consults today's contract, because it cannot reconstruct the
policy the run originally ran under.

The store scan is bounded by `MAX_DIRECTORY_SCAN_ENTRIES` (default `100000`);
a store with more entries refuses the sweep instead of scanning without
bound. A refused sweep is a failure, not a skip: it ends `qcg serve`'s
retention task for the rest of the process and is reported as a failure when
the server shuts down, and it fails the run that `qcg run` precedes. The
automatic sweeps — `qcg serve`'s periodic task and the sweep that precedes
`qcg run` — skip a store another holder is already maintaining, because that
holder is doing the same work; the manual destructive sweep refuses it with
`run store maintenance is busy; retry`.

`GC_KEEP`, `GC_KEEP_FAILED`, and `GC_INTERVAL_SECS` (default `86400`, minimum
`60`) retune the sweep; invalid or zero values refuse boot. The environment
refuses zero because a deployment that keeps nothing is more likely a
misconfiguration than an intent, while an explicit `--keep 0` is a direct
instruction and keeps nothing of that budget. The retention task of
`qcg serve` sweeps once when the task starts, then once per
`GC_INTERVAL_SECS`. Set `AUTO_GC=0` to disable automatic retention — the
periodic task and the sweep that precedes `qcg run` — and use:

```bash
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50 --keep-failed 10 --delete
```

GC never removes a non-terminal run. The first command is a dry run.

Retention is bounded by the count budgets and, when the contract declared
one, the recorded retention window; there is no byte or disk-usage ceiling in
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

Runtime, LLM, and resource byte/count limits are explicit-only: `None`
(omitted) means no mechanistic limit. The HTTP body limit, the package
entry count, and the package expanded bytes are the exceptions: they
default to 16 MiB per body, 1000 entries, and 10 MiB.
Defaults favor local development; production operators must set bounds
or a single oversized input, artifact, or LLM context can exhaust
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
- `qcg package`: `--max-entries` (default 1000), `--max-bytes`
  (default 10 MiB), `--max-metadata-bytes` (explicit-only).
  The defaults already bound untrusted trees; raise them only for
  reviewed large generators.
- `qcg install`: `--max-entries` (default 1000), `--max-bytes`
  (default 10 MiB), `--max-archive-bytes` plus `--max-metadata-bytes`
  (both explicit-only). The entry/expanded defaults already reject
  entry-flood and zip-bomb shapes; bound the archive dimension explicitly
  for untrusted installs, e.g.
  `qcg install pkg.pkg --max-archive-bytes 5242880` (5 MiB
  archive: wide headroom over the in-repo fixtures at ~30 KiB and 13
  entries).

`[budget]` (`max_steps`, `max_tokens`, `max_cost_usd`,
`max_elapsed_seconds`) is the run-wide backstop and should always be set for
server-hosted generators.

## Skill resources and diagnostics

`skill` and `skill_library` resources follow the agentskills.io format.
Names, descriptions, and unparseable frontmatter are hard errors at contract
load; unknown top-level keys (ecosystem extensions) are ignored with a warning
diagnostic, while a misspelled required field still fails through the missing
name/description check. Soft violations (name length, character set, or a name
that differs from the directory name) are recorded as diagnostics on the run's
resource snapshot and logged with `tracing` instead of failing the run. Validate a skill or a
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
builds it, and checks the distribution bundle. Browser tests against both
the Vite proxy and assets served by qcg run only in CI (`Demo end-to-end` job).

When a qpx binary is available, verify the documented deployment boundary with:

```bash
cargo build -p cli
QPXD_BIN=/path/to/qpxd bash scripts/e2e-qpx-smoke.sh
```

## Artifact delivery

External delivery is modeled as a generator command, not a server feature.
Declare the exact script invocation in `permissions.commands`, declare the
required network host, and mark the step `side_effects = "confirm"`. Operators
then review and approve the delivery at the same HITL boundary as any other
side effect.

## Receiver idempotency for external effects

qcg's `operation_id` is a run-scoped key for its external-operation journal
and same-invocation result cache. Approvals use separate run-scoped
confirmation IDs; neither local mechanism coordinates claims or approvals
across runs. For `HttpStep` side-effect methods (anything other than
`GET`/`HEAD`), one declared `params.headers` entry named
`Idempotency-Key` (case-insensitive) is sent unchanged as the receiver's
transport key and suppresses qcg's run-scoped fallback header. If absent,
`HttpStep` keeps the existing behavior and sends its local `operation_id` as
the default remote key. `GET`/`HEAD` behavior is unchanged: declared headers
are sent, but qcg adds no fallback. The explicit header remains part of the
HTTP operation/approval digest; invocation-scoped approvals are still fresh
for each call, and approvals never cross runs.

Commands have no `operation_id` environment-variable ABI. A command receiver
that needs a stable key across runs must receive it through the existing
structured `params.input` JSON on stdin and define/validate that input in its
own protocol; qcg does not invent a command parameter or inject a key.

The receiver owns deduplication, same-key/different-payload conflict handling,
and retention duration. Define these semantics at the receiver and retain
records long enough for the intended retry and fresh-run horizon. Keys are
identifiers, not authorization credentials; do not put secrets or personal
data in them. qcg does not guarantee exactly-once effects: if an external
operation has started but no completion was journaled, qcg refuses automatic
replay because the outcome is unknown. A receiver-side key can help reconcile
or deduplicate a deliberate retry, but qcg maintains no global cross-run
key store and makes no receiver claim on its behalf.

## Unattended operation

qcg ships no scheduler, timer, or webhook receiver. Mechanism (durable runs,
pre-provisioned answers, run-scoped effect guards, and receiver-key transport)
lives in qcg; scheduling policy (when to run, how often to retry, whom to
notify) lives outside in cron, systemd timers, or a workflow orchestrator. A
run whose questions and confirmations are all pre-provisioned completes
without interaction.

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
  `crates/engine/src/engine/run_context.rs`). `invocation_hash` is hex
  SHA-256 over the invocation id. Single-shot steps use
  `execution:<node>:<count>` (finished-execution count, so a repair or
  regenerate is a new invocation); agent tool calls use the stable model call
  id. The operation id (`run:node:sha256(invocation)` in
  `crates/engine/src/state.rs`) is qcg's local journal/result-cache identity,
  never a confirmation id. It is the default remote HTTP key only when a
  caller-declared `HttpStep`
  `Idempotency-Key` header is absent; command receivers must use their own
  structured stdin protocol. The manifest
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
`crates/llm-steps/src/tool_events.rs` (the `#__mcp_pending` suffix marks stored continuations)), so one can never authorize the
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

Compute the calendar date outside the unit file: `%Y`, `%m`, and `%d`
are systemd unit specifiers (fragment directory, machine id, credentials
directory), not strftime, so `--input date=%Y-%m-%d` would pass host
metadata instead of a date. Keep the date policy in a small wrapper and
call it from the unit, mirroring the cron example above:

```sh
# /usr/local/bin/qcg-report
#!/bin/sh
exec /usr/local/bin/qcg run /srv/qcg/generators/report \
  --input "date=$(date +%F)" \
  --answer scope=brief \
  --confirm publish:http:<64hex>=true \
  --output /srv/qcg/out --yes
```

```ini
# /etc/systemd/system/report.service
[Unit]
Description=Unattended qcg report generation
After=network-online.target

[Service]
Type=oneshot
User=qcg
Environment=API_TOKEN_FILE=/etc/qcg/api-token
ExecStart=/usr/local/bin/qcg-report
```

```ini
# /etc/systemd/system/report.timer
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
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Idempotency-Key: report-$(date +%F)" \
  -H "Content-Type: application/json" \
  -d '{"generator_id":"report","inputs":{"date":"2026-09-06"},
       "answers":{"scope":"brief"},"confirmations":{"publish:http:<64hex>":true}}' \
  | jq -r .run_id)
curl -fsSN "$BASE/api/runs/$RUN/events" -H "Last-Event-ID: 0" \
  -H "Authorization: Bearer $API_TOKEN"
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

Terminal-only fsync mapping (`crates/engine/src/journal/writer.rs`):

| path | what is fsynced | when |
|---|---|---|
| `JournalWriter::event` fast path | `file.sync_data()` on the journal file plus parent directory sync | when the event is terminal or operation-driven (`operation_started` / `operation_finished` set `needs_sync`; see `JournalWriter::event` in `writer.rs`) |
| `append_events_if` batch path | `file.sync_data()` on the journal file plus parent directory sync | when the batch contains a terminal or operation event (`needs_sync`) |
| torn-tail repair `truncate_to` / newline commit | `sync_data()` after truncate or newline commit | every repair |
| `state.json` persist | atomic write + replace via `persist_serialized_atomic` | every append (state always follows the journal) |
| `.clean_shutdown` marker | plain `write` / `remove_file`, no fsync | best-effort I/O, failures propagate (a failed terminal marker fails the operation; see repair marker below) |

Cancel mailbox (`request_remote_cancel` in `crates/service/src/run_dirs.rs`):
post-publish directory-sync failure is warn-only by necessity — reporting an
error would make the caller retry under a fresh operation id and journal a
duplicate cancel for one published request. Power loss may drop the directory
entry; the next boot observes the request as absent and a retry publishes
under a fresh operation id, with deduplication by cancel-drain idempotence
(E01). Idempotency Ready commit (`store_durable_ready` in
`crates/server/src/server/idempotency/durable.rs`): directory-sync
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
`.clean_shutdown` marker next to the journal, containing exactly
`b"clean-shutdown-v1\n"` (ASCII `clean-shutdown-v1` followed by one LF
byte, `0x0a`); any later non-terminal append
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

Admission records and run coordination locks are different mechanisms.

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
- Admission and preemption-handoff locks use separate fixed-size shard sets
  in `.admission-shards/` and `.handoff-shards/` under the runs directory.
  They are intentionally never unlinked: removing a locked file would let a
  later operation lock a fresh inode while the current holder still owns the
  old one. They hold no run state and are stateless coordination only.

Large operation results spill to a sidecar blob under the run meta dir when
they exceed 64 KiB (`OPERATION_RESULT_MAX_BYTES` in
`crates/engine/src/state.rs`); the journal carries `result_ref` (the
content-hash blob name) instead of the inline bytes, and the dir-backed guard
reloads the blob on resend. Shared per-run journal pollers serve all SSE
subscribers of one run from a single poll task
(`journal_pollers` in `crates/service/src/types.rs`). Snapshot live
`duration_ms` is quantized down to whole seconds so exact-digest ETags stay
stable within a second and conditional requests can return 304; crossing a
second boundary advances the body (and the validator) even when nothing
else changed, so an unrelated `200` there is correct, not stale
(`live_metrics` in `crates/service/src/summaries/metrics.rs`).

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
repair converges forward, it never rolls back the already-committed parent.
Dependencies that cannot be resolved or fetched fail the repair closed: a
partial closure is never recorded as complete. The full closure is staged
before anything commits, for registry and direct path installs alike (a
local directory or file stages its closure against the configured registries
first); siblings sharing a dependency select a single version for the whole
closure, and a missing or conflicting set refuses pre-commit with nothing
committed, so there is no partial state to repair. Destination copies,
provider validation and confirmations also finish before publication locks.
Publication holds shared process-global and file-global guards, then locks the
union of prepared and selected IDs (including all reused transitive nodes).
Distinct process shards are locked once each in ascending shard-index order;
per-ID file locks follow in deterministic ID order. All guards remain held
through closure revalidation, leaf-first commits (parent last), fsyncs and
backup cleanup. Disjoint closures on different shards can publish concurrently;
overlapping closures serialize, and shard collisions may also serialize
unrelated IDs. Uninstall remains global-exclusive and global-first across its
dependents recheck and removal. Maintenance uses the same global-process →
global-file → shard → per-ID order, with shared global locks and try-lock/skip
at every contended lock; it never acquires global locks after per-ID.

Reused packages are inventory-verified again and must retain the selected
identity, version and dependency set. If a concurrent uninstall or replacement
made the plan stale, installation refuses before publication and asks for a
retry; it never fetches or prompts under the guards. A later commit failure
can leave earlier standalone dependencies installed, but never commits the
parent after a failed dependency.

Before moving an existing target, replacement creates and flushes an external
`.install-backup-<nonce>.json` preparation record beside the planned
`.install-backup-<nonce>` directory. No recovery metadata is written inside a
package (and inventory verification has no recovery-file exemptions).
Only after the backup rename and parent sync succeed does the live commit
atomically persist ownership in that sidecar. An interruption before that
update leaves an ambiguous copy retained for inspection, not automatic recovery.
Installs attempt recovery before sweeping: a missing target is restored
only from one unambiguous, inventory-verified backup with a matching manifest
ID. Multiple candidates refuse recovery with an inspection error; never choose
the newest by timestamp. See [Install backup maintenance](#install-backup-maintenance)
for cleanup, failure handling and platform limits.

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
resumer, retention GC), and settles active runs under the configured outer
`SHUTDOWN_SETTLE_SECS` deadline (default 150 s, range 1..=3600); exceeding
it is reported to the embedding host instead of being logged and ignored.
`SHUTDOWN_DRAIN_SECS` (default 30 s, range 1..=3600) bounds the preceding
HTTP drain. The total bound from shutdown signal to process exit is the sum
of these configured phases: 180 s by default, at most 7200 s under the
validated ranges. `SHUTDOWN_SETTLE_SECS` must be at least
`SHUTDOWN_DRAIN_SECS`. The outer deadline starts after the HTTP drain
completes, so a wedged drain delays settlement by design but is cut at its
configured bound with a warning. Startup order is resolve deployment policy
once, build the service, build the router, then start recovery
(`resume_recovered_runs`) before resident tasks; shutdown order is signal,
stop accepting mutating work (shared token), HTTP drain, mark the service
shutting down, then resident-task shutdown and active-run settlement
concurrently under the outer deadline. Resident-join and run-convergence run concurrently by design
(E05): serializing them would let one consume the other's settlement
budget, so the outer deadline covers both together. Responsibility split
(E05/Q3): the configured HTTP drain ends accepting work, resident-task shutdown
stops the execution sources (shared token, GC, resumer, shared-store
refresh), active-run settlement converges the runs, and the resume exception
(tracked runs terminate; only a durable `Queued` journal never tracked by the
stopping peer resumes next boot) decides what restarts. Q3 owns the
terminate-vs-resume policy; E05 owns the drain/join/settle mechanism. The outer deadline starts after the HTTP drain completes:
in-flight requests that drain promptly do not consume the
active-run settlement budget, while a wedged drain connection is cut at the configured drain
bound so shutdown proceeds (the drain timeout is warned, never silent).

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
  unattended. Authorize once from the loopback Connections panel on a host
  with an unlocked OS keyring. Entries live in the unlocked login collection
  and are not file-backable, so they do not survive container restart or
  redeploy by themselves (rotation and re-authorization are operator policy;
  qcg never exports tokens). Headless or container renewal without an
  unlocked provider fails closed; either run an unlocked Secret Service
  provider or set `oauth_store = "memory"` and re-authorize from loopback
  after every restart.
- A denied confirmation ends the run as `Failed`; retries, backoff, and
  notifications are the orchestrator's job, not qcg's.
- `keep` (default 50) retains the newest nonfailed terminal runs (success,
  canceled, or interrupted); failed runs do not consume that budget and are
  governed only by the separate `keep_failed` budget (default 10). A
  retention window recorded in a run's journal expires that run independently
  of the counts: a run past its window is collected even when the count
  budget would keep it, while a run inside its window is still governed by
  the count budgets. With defaults, a sweep may retain up to 50 nonfailed
  terminal runs plus 10
  failed runs. The retention task sweeps once when it starts and then once
  per `GC_INTERVAL_SECS` (default 24 hours); size the `runs-dir` volume and
  retention for that schedule.
- `Waiting` and `Confirming` runs release their execution slot but have no
  TTL in mechanism; an unanswered run stays paused until answered, confirmed,
  or canceled. Expire it from the orchestrator with `cancel` when policy
  requires. By default `budget.max_elapsed_seconds` covers queue plus HITL dwell,
  and the wall-clock it measures starts when the run is enqueued: a run that
  waits behind a backlog can exhaust its budget before it executes a step, so
  size the budget for the queue the deployment actually produces.
  Such a run reports that the elapsed limit expired before the next step
  could start, not that a step ran out of time while running; the
  journal's `step_started` records show the steps that did run.
  `ELAPSED_BUDGET_MODE=active_execution` excludes ordinary waits (see below).

## Observability

- `GET /healthz` reports process liveness (`ok: true`), the effective
  `max_request_bytes`, and a status-only `shared_store_recovery` field:
  `disabled` (exclusive store), `starting` (no refresh completed yet),
  `healthy` (last refresh completed without skipped runs), `degraded` (a
  store-wide failure or one or more skipped runs), or `restarting` (an
  unexpectedly exited/panicked recovery worker is awaiting restart).
  No error strings, paths, or run data are included. This is not readiness:
  a bad run must not make healthy operations unavailable, so recovery
  degradation does not change `ok` or the HTTP success status. Queue depth,
  lease, and GC state are not reported there.
- Startup rehydration stays strict: unreadable run data refuses startup.
  Periodic shared-store refresh instead logs and skips individual unreadable
  entries/runs, retaining a tracked run's last in-memory state and continuing
  healthy-run adoption, journal convergence, and the normal resume pass.
  Per-run journal reads/writes remain strict; skipped data is never guessed
  or repaired by refresh. Store-wide traversal errors and configured scan/run
  caps remain global refresh failures. A clean later refresh returns health
  to `healthy`.
- Global refresh failures retry indefinitely with exponential delays of
  2, 4, 8, 16, 32, then at most 64 times `SHARED_STORE_RESCAN_SECS` (default
  rescan cadence 5 s; maximum retry delay 320 s). Success resets the failure
  backoff; isolated run failures keep the normal cadence. Unexpected worker
  exits/panics are logged and supervised with the same bounded restart
  backoff. Shutdown interrupts retry/restart waits; the server's existing
  shutdown grace/deadline still bounds in-progress work.
- `GET /metrics` exposes gauges only: durable runs by state, active, queued,
  waiting, confirming, in-flight preempted (`qcg_runs_preempted`), distinct
  generators, and per-generator counts under a bounded selection
  (`METRICS_GENERATOR_LIMIT`, default 20, 1-256; `METRICS_PINNED_GENERATORS`,
  optional comma-separated generator ids that survive truncation when present).
  Pinned ids lead id-ascending, the remainder fills top by run count, and the
  scrape never exceeds the budget.
  Latency histograms, error rates, queue dwell, preemption totals, and GC
  deletion or failure counts are not exported; alert thresholds live outside.
- OTLP export exists in implementation (`OTLP_ENDPOINT`,
  `OTLP_INTERVAL_MS`, best-effort) and `qcg runs trace` builds
  hierarchical spans, but no SLO is defined here. Treat traces as mechanism
  facts for external analysis.

## Priority scheduling and preemption

`POST /api/runs` and `POST /api/runs/{id}/fork` accept an optional integer
`priority` (default 0, higher runs first). Queued runs start in priority
order with FIFO ties; `queue_position` in snapshots follows the same order.
When all execution slots are busy and a higher-priority run arrives, the
lowest-priority eligible running run is requested to yield. Equal priorities
never preempt each other. The victim keeps its journal: completed work replays
on resume instead of re-executing.

The server and CLI host resolve these independent **deployment** settings at
boot. `PREEMPTION_MODE`, `PREEMPTION_QUEUE_POSITION`, and `ELAPSED_BUDGET_MODE` accept only the listed
spellings: invalid, empty, or
non-Unicode values refuse startup, even when preemption is disabled. Embedded
services pass the typed fields on
`ServiceDeploymentPolicy` (or call `with_scheduling_env` before construction).
Run requests and generator contracts cannot override them.

| Environment | Accepted spellings | Default |
| --- | --- | --- |
| `PREEMPTION` | `1`/`0`, `true`/`false`, `yes`/`no`, `on`/`off` (case-insensitive) | `true` |
| `PREEMPTION_MODE` | `step_boundary`, `interrupt` | `step_boundary` |
| `PREEMPTION_QUEUE_POSITION` | `preserve`, `tail` | `preserve` |
| `ELAPSED_BUDGET_MODE` | `wall_clock`, `active_execution` | `wall_clock` |

`step_boundary` requests a cooperative yield distinct from ordinary cancellation.
The current flow node (including nested execution), or a complete already-started
parallel wave, finishes and checkpoints before yielding; no next node/wave starts.
Startup/lifecycle work belongs to the current boundary. A yield is nonterminal:
it does not run failure hooks or write a canceled/failed settlement. Completion,
HITL suspension, and actual execution failures retain their normal handling.
The service acquires the handoff lease before requesting yield; after the old
task and writer exit, it acquires the execution lease and holds both through
requeue. Retention GC skips any candidate whose handoff or execution lease is
owned, so temporary interrupt terminals cannot be collected mid-requeue. There
is **no five-second timeout-to-interrupt fallback** in this mode. Urgent work waits for
the current boundary; user cancellation, shutdown, command timeouts, and elapsed
budgets remain separate and effective, and may still interrupt work.

`interrupt` retains immediate execution cancellation, with the existing
five-second bounded-stop/abort fallback. This can leave an `operation_started`
without a determinate result. Resume still **refuses to replay indeterminate
external effects**; choosing interrupt does not weaken that fail-closed guard.

`preserve` carries a scheduler-preempted victim's prior durable `queued_at`
forward as its FIFO ordering key, including through repeated preemptions and
restarts. `tail` instead rejoins the tail among equal-priority queued runs,
using the requeue event's actual timestamp as the new key. Neither choice
backdates journal event `ts`. This choice applies only to scheduler preemption:
fresh admissions, forks, and HITL answer/confirmation queue episodes establish
new current admission times. It is independent of `PREEMPTION` and
`PREEMPTION_MODE`.

A concurrent `cancel` wins over resume. Priorities and queue order persist in
the journal across restarts.

### Elapsed-budget waiting policy

`ElapsedBudgetMode::WallClock` (the default, `wall_clock`) preserves total
wall-clock age from the first durable `run_queued` or direct `run_started`:
initial queue, preempted queue, questions, confirmations, and downtime all count.
Its existing resume-time clock-skew behavior is unchanged.

`ElapsedBudgetMode::ActiveExecution` (`active_execution`) counts the elapsed
wall duration of each **engine execution episode**, not CPU time. It includes
gateway/resource/input setup and startup cleanup inside the episode, lifecycle
hooks, retries and backoff, provider/network waits, and all already-started
parallel work **once for the whole episode**, not once per node. Normal initial
queue, scheduler-preempted queue, and HITL-paused intervals are excluded. Contract
validation, input resolution and directory/journal opening are preflight, before
an execution episode. An exhausted active allowance starts no new engine work.
This setting is independent of all three preemption settings and cannot be
selected by a run request or contract.

Fresh admissions pin the resolved mode in `run_queued`; direct execution pins it
in `run_started`. Requeues repeat that mode, adopted resumes read journal truth,
and forks inherit the checkpoint's mode, accumulated consumption, and any open
interval rather than applying today's deployment mode to the source's history.
Changing deployment configuration cannot reinterpret existing runs. Journal
schema 2 requires the pinned mode; schema-1 absence explicitly means historical
`wall_clock`, never zero active consumption. Folded state is schema 2; old cached
state is rebuilt from the journal. Unknown versions are refused.

Active accounting uses one durable `elapsed_accounted` ledger: accumulated
`active_nanoseconds` plus nullable `active_started_at`. The first active-mode
identity (`run_queued`, or `run_started` for fresh direct execution) atomically
establishes a zero total with a closed anchor, so a crash immediately after that
append is recoverable without a separate initialization record. Later identities,
including fork/requeue admissions, preserve the ledger and refuse a missing one;
they never reset consumption. Opening and closing records
sync the journal in both writer paths. An open record precedes episode work;
ordinary exits close using monotonic elapsed time before terminal/yield settlement
or publishing a service waiting/confirmation state. Pending prompts emitted by a
still-open episode are provisional, not yet eligible for service answers or
decisions. Nanosecond totals preserve subsecond consumption across suspensions;
in-process hard deadlines use a precise `Duration` remainder and monotonic
`Instant`, including while hooks, retries, and parallel waves run. Scheduler
interrupt truncates only its canceled suffix, never accounting records. Operation
replay still refuses indeterminate external effects.

A process crash or task abort can leave the interval open. Recovery under the
execution lease conservatively charges from its wall anchor through current wall
time, **including downtime**, syncs the closed total exactly once, then opens the
next episode if budget remains. A prompt with an open crash anchor is recovered,
not indefinitely parked as a free HITL wait. A fork of an open checkpoint uses
the same conservative rule. A closed record never charges the intervening wait
again. This assumes a **non-regressing wall clock across recovery**: a future
open anchor, invalid/missing active ledger, illegal transition, or arithmetic
overflow refuses execution instead of clamping or resetting consumption. Clock
jumps forward can overcharge after a crash; that is the deliberate fail-closed
tradeoff, not a claim to measure CPU use or reconstruct lost monotonic time.

Direct/ephemeral scheduler queueing has no durable admission and cannot charge
its pre-engine queue delay. Direct `wall_clock` behavior is unchanged: its clock
begins at `run_started`. Direct active episodes pin/account their mode when the
engine starts, but likewise do not account that earlier queue. Snapshot and
metrics `duration_ms` continue to mean wall lifetime, not active consumption.


### Measured latency/safety tradeoff

The supplied interrupt baseline for the one-slot long-command regression was
0.48 s test time: urgent work dispatched before the 60 s command ended, but the
victim later failed with indeterminate replay refusal. That regression now
explicitly selects `interrupt` to keep exercising the safety guard.

A bounded real one-slot comparison is reproducible with
`cargo test -p service --locked --offline bounded_scheduler_comparison -- --nocapture`.
It requests preemption during a command that sleeps 0.3 s before appending an
effect, followed by a dependent node. Boundary mode checkpoints the command once,
starts urgent work before the dependent node, then resumes without duplicate
effects and succeeds. In a focused local debug run, urgent work was admitted in
397 ms with default StepBoundary versus 77 ms with Interrupt (command delay: 300 ms).
Interrupt dispatched sooner but the victim failed with indeterminate replay
refusal. The original long-command regression passed in 0.48 s test time.
Additional regressions cover cancel racing with a draining yield, complete
parallel-wave checkpoint/replay, HITL winning over yield, and a gated command
held past five seconds without an implicit abort.

Local timings are observations, not guarantees:
the default trades current-boundary latency for avoiding scheduler-created
indeterminate side effects.

## Per-run generator package snapshots

API admission copies the accepted generator tree into the fixed
`generator-package/` directory inside the run directory. The bounded, no-follow
copy must hash to the accepted `package_sha256` before the run is queued. Runtime
contracts and package-relative files are loaded from this copy, not the installed
generator. Replacing or uninstalling the installed generator therefore cannot
change queued work, a resumed run, or a fork. The snapshot is run-owned content:
do not edit it; retention/GC removes it with the rest of the run directory.

`run_queued` and `run_started` carry the same full-package SHA-256 identity,
including across requeues and forks. `generator_path` retains its original
source-path meaning; no snapshot absolute path is journaled. `contract_sha256`
still identifies the contract, not the whole package, and package timestamps
remain excluded from package identity. This is an additive schema-v2 field;
legacy/direct events may omit `package_sha256` for read-only observation.

Execution re-verifies the snapshot after queueing and before entering the
engine. Restart recovery refuses non-terminal runs without a package identity,
with a missing snapshot, or with mismatched content; it never reloads the current
installed package to repair them. Such a refusal can prevent service startup or
shared-store refresh until the store is repaired or the affected run is removed
under the normal store ownership rules. Forks likewise verify and copy the source
run's snapshot and refuse missing/legacy/mismatched content. A previously admitted
fork retry verifies its own snapshot instead. Copy/checkpoint failures clean only
the fresh target, never the source or a pre-existing target. Package pinning does
not relax recovery refusal for indeterminate external effects.

**Direct execution is not package-pinned by this slice.** Direct/ephemeral runs
continue to use their supplied generator root, including reads after scheduler
queue delay; they have no durable package admission snapshot. If interrupted
before terminal settlement, they cannot be resumed and the recovery rule above
may refuse service startup until the run is repaired or removed under normal
store ownership. Their existing pre-engine queue-delay accounting limitation
also remains unchanged.

## Run families: fork links and await

`POST /api/runs/{id}/fork` records the source as `parent_run_id`, visible in
snapshots and durable across restarts. A flow can join sibling runs with an
`await` node (`runs` plus optional `timeout_secs`), which succeeds with an
id-to-state map once every listed run is terminal. Unknown runs and timeouts
fail explicitly. Typical fan-out/fan-in: start or fork children, then await
them from a collector run before proceeding.

Queued snapshots carry RFC 3339 `queued_at`, the durable FIFO ordering key
shared by journal and memory, not necessarily the latest transition time:
under `preserve`, scheduler requeues retain the previous key. Fresh admissions,
forks, and HITL acceptances stamp a new key; `tail` scheduler requeues use their
actual event time. `queue_position` follows descending priority, then ascending
`queued_at`, then id (E16). Memory state is only a fallback for journals written before
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

### Queue position quality and preemption

`queue_position_quality` is `exact` for a complete local observation,
`estimated` for shared-store/peer observations or an incomplete directory scan,
and `unavailable` when no queued position can be derived. The SPA prefixes
estimated positions with `≈`. Display caches never drive scheduling.
Priority descends first, then equal-priority runs use their durable queued time
(FIFO). Higher priorities may preempt lower ones. Finished steps replay from
the journal; interrupted external operations with indeterminate outcomes
refuse automatic replay. Preemption is therefore not a promise that every
external command or side effect can safely restart.

The observation cache retains at most `MAX_TRACKED_RUNS` entries and
`READ_CACHE_MAX_BYTES` bytes of conservative memory charge (64 MiB by default).
For deployments retaining 10,000 runs, the measured synthetic views need about
240 MiB of charge; the benchmark explicitly sets a 256 MiB budget. A smaller
budget evicts views and correctly requires rereading them. Capacity accounting
is incremental and LRU selection uses an index. This budget excludes transient
request/parser buffers and is not a total process RSS limit.

## Install backup maintenance

Recovery records are external sibling files, not package contents. The bounded
(4096-byte), typed JSON record contains `schema: 2`, a safe single-component
`id`, the exact `backup` basename, and `phase` (`prepared`, `owned`, `restoring`,
or `cleaning`). It is created exclusively without following an existing symlink;
all bytes and the file are synced, then its parent is synced where supported,
**before** moving the old target.
After the no-replace move, the parent is synced before the live commit
atomically replaces and syncs the sidecar with `phase: "owned"` (including a
parent sync). A crash before the move can leave an orphan prepared sidecar
with no backup directory.

A prepared sidecar proves intent only. If its backup basename exists, retry,
recovery, sweep and uninstall retain the directory and record: they cannot
know whether the no-replace move succeeded or an unrelated directory arrived
first. Even a valid package with the expected ID does not resolve ownership.
This includes crashes immediately after successful rename, backup-parent-sync
failures, and interruptions before the ownership update. A missing target is
not automatically restored in this ambiguous state; inspect and validate the
copies, then explicitly move the intended package/records to resolve it. Never
promote a prepared record to owned merely because a directory exists.

Recovery, sweep, commit and uninstall share ownership parsing and ordered
cleanup. Only regular `owned` sidecars with supported schema, safe ID and
matching basename own a real backup directory. Malformed, oversized, symlinked,
unsafe or mismatched records; unmarked backups; schema-1 intent-only sidecars;
legacy in-package `.backup.json` markers; and symlinked backup paths outside a
restoring/cleaning record are unknown and retained for operator inspection,
never adopted or age-deleted. Legacy markers are not migrated.

For a missing target, recovery requires exactly one owned backup directory,
validates its SBOM/inventory-aware contract and manifest ID, then atomically
persists and syncs `phase: "restoring"` **before** renaming it back without
replacement. It syncs the parent before retiring only the restoring sidecar;
this successful live path never accesses the now-vacant backup basename.
A definite no-move target-conflict error from the rename permits the live
process to persist `owned` again. Other rename errors, sync failures and
interruptions (including just before the restore rename) retain ambiguity.

A restoring record never authorizes deletion or restoration of anything at
its backup basename. If that path exists (even as a valid matching package,
symlink or non-directory), retry, sweep and uninstall retain it and the record.
If the target is missing, retain the record for inspection even if the backup
is also absent. Only an absent backup path plus an inventory-verified current
target and successful parent sync permit retry/sweep/uninstall to retire the
sidecar alone. This prevents a directory planted after the restore rename
from being mistaken for the old recovery package. Resolve an interrupted
pre-rename restore explicitly after inspecting the original package; never
promote a restoring record merely because its backup still exists.

Owned cleanup probes pre-mutation failures first, then atomically persists and
syncs `phase: "cleaning"` **before** removing the backup. It syncs the parent,
removes the sidecar and syncs the parent again. A crash/error after removal can
therefore never leave an owned record authorizing deletion of a directory
planted at the vacant basename. A cleaning record, like a restoring record,
never authorizes deletion or restoration of an occupied basename, including
non-directories and symlinks: retry, sweep and uninstall retain both path and
record. Only an absent backup path, inventory-verified current target and
successful parent sync permit retiring the sidecar alone. If the target is
missing, retain the record even when the backup is also absent. An interruption
before backup removal (or a partial/failed removal) leaves ambiguity requiring
operator inspection; never promote a cleaning record merely because its backup
still exists. This addresses persisted-phase interruption gaps, not general
path replacement by a same-user process bypassing CLI locks.

Invalid packages or multiple candidates produce an actionable error and remain
untouched. Resolve ambiguity by inspecting and explicitly moving conflicting
records/copies out of the generator parent; do not automatically select the
newest. Unknown copies require operator validation rather than automatic repair.

An ordinary publication rename failure attempts the same no-replace, verified
restore. Restore failure retains recovery state. A successful publish rename
followed by a parent-sync failure is **not** rolled back over the possibly
published target: its backup and sidecar remain. Before a further replacement,
old owned records must be cleaned only after the current target verifies and
its parent sync succeeds; otherwise replacement refuses with cleanup pending.
This also handles orphan sidecars with no backup directory. A prepared,
restoring or cleaning record can authorize removing only that sidecar, never a
directory at its basename.
An orphan with no verified target needs operator inspection; it cannot
authorize deletion or restoration by itself.

The stale sweep has a one-hour sidecar age gate, but age alone never authorizes
removal: a verified current target and successful parent sync are required.
Cleanup durably relinquishes ownership as cleaning before removing the backup,
then syncs the parent, removes the sidecar and syncs the parent again. Failures
are reported; retry retires a cleaning sidecar only under the absence and
verification rules above. No recovery backup is owned by a delete-on-drop guard. Successful uninstall performs this cleanup
under its exclusive-global/per-ID locks **before** removing the target, so
later maintenance cannot resurrect it from owned records. Unknown data remains.
Maintenance takes global-process → global-file → shard → per-ID locks in that
order, tries rather than waits, and skips contention with active publication or
uninstall. Closure publication retains its existing full-closure locking.

Durability is platform/filesystem dependent. Unix parent-directory open/sync
failures are surfaced. On non-Unix platforms the directory-sync helper is
currently a no-op: the sidecar file is synced, but no directory-entry or
power-loss durability is promised. Linux uses `RENAME_NOREPLACE`; elsewhere
no-replace uses existence probes plus CLI locks and a plain directory rename,
so lock-bypassing filesystem changes retain a check/rename race. Parent syncs
order directory entries; they are not a claim that all package payload files
are recursively flushed or that hardware/filesystems honor flushes under power
loss. Fault-probe tests exercise protocol ordering and error retention, not
physical power-loss behavior.
