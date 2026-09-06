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

qcg's bearer token authenticates the instance, not a user or run owner. Use it
behind a trusted shared boundary, or deploy separate qcg instances and runs directories
for trust domains that require isolation. The default `exclusive` run store
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

`qcg serve` periodically retains the newest 50 terminal run directories. Set
`QCG_AUTO_GC=0` to disable automatic retention and use:

```bash
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50 --keep-failed 10 --delete
```

GC never removes a non-terminal run. The first command is a dry run.
Failed runs keep an additional `--keep-failed` budget (default 10) for
post-mortems even when they fall outside `--keep`.

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
  `max_entries`, `max_selected_bytes` for `dir` and `skill`).
- `qcg package`: `--max-entries`, `--max-bytes`, `--max-metadata-bytes`.
  Unbounded packaging trusts the source tree; never package untrusted trees
  without bounds.

`[budget]` (`max_steps`, `max_tokens`, `max_cost_usd`,
`max_elapsed_seconds`) is the run-wide backstop and should always be set for
server-hosted generators.

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
- Every `ask_user` question id and every confirmation id (`<node>:<kind>`
  for side effects) is known in advance.
- `side_effects = "confirm"` steps are pre-approved per confirmation id, or
  the generator uses `side_effects = "allowed"` / `"none"`.

### cron

```cron
# Daily 02:30 unattended generation. --yes disables prompting; unanswered
# questions and unapproved confirmations pause or fail the run instead.
30 2 * * * /usr/local/bin/qcg run /srv/qcg/generators/report \
  --input date=$(date +\%F) \
  --answer scope=brief \
  --confirm publish:http=true \
  --output /srv/qcg/out --yes >>/var/log/qcg/report.log 2>&1
```

`--confirm` takes `ID=approve|deny` pairs; `--confirmations-file` accepts the
same mapping as a JSON id-to-boolean object.

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
  --confirm publish:http=true \
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
       "answers":{"scope":"brief"},"confirmations":{"publish:http":true}}' \
  | jq -r .run_id)
curl -fsSN "$BASE/api/runs/$RUN/events" -H "Last-Event-ID: 0" \
  -H "Authorization: Bearer $QCG_API_TOKEN"
```

A reused `Idempotency-Key` with identical content replays the original result
instead of starting a duplicate run; the same key with different content is
rejected with `409 Conflict`. Records persist for 24 hours under
`<runs-dir>/idempotency/` and survive restarts and shared-store peers. If a run still reaches `Waiting` or
`Confirming`, answer mechanically with
`PUT /api/runs/{id}/questions/{qid}` or
`PUT /api/runs/{id}/confirmations/{cid}`. `GET /healthz` and `GET /metrics`
cover liveness and Prometheus monitoring.

Unattended limits:

- MCP OAuth authorization needs a browser on loopback and cannot run
  unattended.
- A denied confirmation ends the run as `Failed`; retries, backoff, and
  notifications are the orchestrator's job, not qcg's.
- `qcg serve` retains the newest 50 terminal run directories; size the
  `runs-dir` volume and retention for the schedule above.

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
