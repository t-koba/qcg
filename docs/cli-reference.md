# CLI Reference

Global options may precede any subcommand:

- `--verbose`: use `qcg=debug` as the default log filter.
- `--log-format text|json`: select human-readable or JSON logs.
- `--providers <PATH>`: use this authoritative unified provider registry path
  (LLM providers, REST search profiles, and MCP servers) for validation, direct
  runs, replay, and the server.
- `--help` and `--version`: print command help or version.

`RUST_LOG` overrides the default filter. Logs go to stderr; command results and
JSON event output go to stdout.

## Generator commands

- `qcg new <path> [--id ID] [--force]` creates a minimal generator skeleton.
  Mechanism only: a valid `qcg.toml` and immediate `validate`. Prompt text, schemas,
  budgets, and permissions stay author policy as follow-up edits. Fails closed
  when the target is non-empty without `--force` or when the skeleton does not
  validate.
- `qcg validate <path> [--json]` loads the contract, checks the graph, validates every
  registered step's closed `params`, and prints the contract digest.
  All rule violations are reported together instead of stopping at the first
  one; `--json` prints a machine-readable verdict.
- `qcg run <generator> [--input k=v]... [--input-file k=path]...
  [--inputs-file file.json]
  [--answer id=value]... [--confirm id=approve|deny]...
  [--confirmations-file file.json] [--output dir] [--yes] [--json] [--plan] [--diff]
  [--max-inputs-file-bytes N]` runs a package.
  `--yes` runs non-interactively: unanswered questions and unapproved
  confirmations pause or fail the run instead of prompting, so pre-provision
  them with `--answer` and `--confirm` (or `--confirmations-file`, a JSON
  id-to-boolean map); use only for a reviewed contract.
  `--plan` prints the execution plan (steps with `needs`, `on_deps`, full `when`,
  `output`, `parallel` membership, permissions, declared outputs,
  pre-provision coverage, plus read-only `ids` with `questions` and
  `confirmation_shapes`) without running; add `--json` for machine-readable
  output. `--plan --diff` additionally prints a read-only forecast per node
  (declared writes, command allowlist result, literal HTTP host check, side-effect
  flag) plus whole-plan budget estimates. No filesystem writes, processes, or
  network requests are performed. Digests are runtime facts and are never
  predicted by plan output.
  A previous package (`.pkg` ZIP, `blueprint-package.json`, or any file) is
  re-entered as an ordinary `type = "file"` input via
  `--input-file field=path`, optionally combined with extra `--input` values
  for re-editing. See `fixtures/generators/package-reinput` for the
  file-plus-command expansion pattern.
  A retention sweep runs before the run starts and may delete run directories
  from `.data/runs` that fall outside the retention budget, so it removes
  data without asking. It prints nothing on stdout (stdout carries the run's
  output) and a one-line summary on stderr when it removes something.
  `AUTO_GC=0` skips it. A store another process is already maintaining is
  skipped as well, but a sweep that fails fails the run: the store it could
  not inspect is the store the run would use. See
  [run retention](operations.md#run-retention).
- `qcg list [--generators-dir dir]` lists available packages from the installed
  directory and the bundled generator directory. Installed IDs take precedence.
- `qcg models [--json] [--refresh]` lists the selectable LLM providers, models,
  reasoning efforts, prices, and limits derived from `providers.toml`, the
  optional external catalog, and per-provider discovery. `--refresh` re-fetches
  external catalog sources first; stale or failed sources are reported on
  stderr. Credentials are never printed.
- `qcg eval <generator> --suite suite.json [--output .data/evals]
  [--runs-dir .data/runs] [--baseline REPORT_JSON] [--json]` executes isolated
  cases and optional repeated seed variations. Assertions cover artifacts,
  regex, JSON Schema, manifest pointers, event counts and ordered trajectories,
  plus recorded metric ceilings. Reports include per-case pass counts across
  repetitions with flaky detection, per-case terminal metrics
  (`tokens_total`, `cost_microusd`, `duration_ms`, `llm_calls`), and baseline
  p50 cost/duration deltas. A baseline comparison fails on pass-rate or
  case regressions. Suite schema: `{ name, min_pass_rate = 1.0,
  repetitions = 1, cases: [{ name, inputs = {}, answers = {},
  confirmations = {}, seed?, assertions: [...] }] }`. Assertion `type` values:
  `artifact_exists`, `artifact_sha256`, `artifact_contains`,
  `artifact_matches`, `artifact_json_schema`, `manifest_pointer`,
  `event_count`, `event_sequence`, `metric_max`. See
  `fixtures/generators/hello-template/suite.json` for a minimal example.
  Semantic judges are out of mechanism: use an external `command` step or
  guardrail when meaning must be judged, with explicit timeout and byte limits.
  Assertions stay declared-only evaluator policy: only asserted artifact
  paths, manifest pointers, and capped event counts are checked, so an extra
  file, manifest artifact, or uncapped event passes unless the suite caps it
  with an explicit `event_count` `max` (or equivalent assertion).
  `event_sequence` is a subsequence match: kinds listed in order pass with
  interleaved noise between them, so cap the noise kind with an explicit
  `event_count` `max` when extras must fail.
  `metric_max` reads only `run_finished` metrics, so a failed run fails the
  assertion as not recorded.
- `qcg package <dir> [-o package.pkg] [--signing-key key.pk8]
  [--max-entries N] [--max-bytes N] [--max-metadata-bytes N]` creates a `.pkg`
  ZIP from a directory, prints its SHA-256, and optionally writes detached
  Ed25519 `.sig` and `.pub` files. Archives are deterministic and include an
  SPDX 2.3 file inventory plus in-toto/SLSA provenance. Symlinks and output
  paths inside the source tree are rejected. Entry count defaults to 1000
  and expanded bytes to 10 MiB (`PACKAGE_MAX_ENTRIES`, `PACKAGE_MAX_BYTES`
  override); `--max-metadata-bytes` (`PACKAGE_MAX_METADATA_BYTES`)
  enforces an explicit max only when set.
- `qcg install <path-or-url> [--sha256 HEX] [--signature HEX --public-key HEX]
  [--generators-dir dir] [--yes] [--force]
  [--max-entries N] [--max-bytes N] [--max-archive-bytes N]
  [--max-metadata-bytes N]` verifies, stages, validates,
  summarizes permissions, and installs a package. Remote packages must be
  pinned by SHA-256 or verified by an Ed25519 signature. A bare
  `<id>[@version-requirement]` instead installs from configured registries
  (see below), including `[dependencies]` recursively. Entry count defaults
  to 1000 and expanded bytes to 10 MiB (`PACKAGE_MAX_ENTRIES`,
  `PACKAGE_MAX_BYTES` override); archive and metadata bounds stay
  explicit-only (`PACKAGE_MAX_ARCHIVE_BYTES`,
  `PACKAGE_MAX_METADATA_BYTES`).
  For untrusted packages the entry/expanded defaults already apply; bound
  the archive dimension explicitly as well, e.g.
  `qcg install pkg.pkg --max-archive-bytes 5242880` (5 MiB
  archive: wide headroom over the in-repo fixtures at ~30 KiB and 13
  entries, while the defaults reject entry-flood and zip-bomb shapes).
- `qcg registry add <name> <url>`, `qcg registry remove <name>`,
  `qcg registry list`: manage registry index URLs (`file://` or `https://`)
  under `$DATA_HOME` (default `~/.data`). Everything is user-owned files; no
  privilege is required. `qcg search <query>` matches id, description, and
  registry. Index rows may carry an Ed25519 `signature` with `key_id`;
  signed rows verify against `$DATA_HOME/trusted-keys.toml` (entries with an
  `expires` timestamp stop verifying after it), while unsigned rows rely on
  index transport plus the entry `sha256` checked at install.
- `qcg uninstall <id> [--generators-dir dir] [--yes]` removes one installed
  generator ID after checking that the ID is a safe relative path.
- `qcg skill validate <path> [--library] [--json]` validates an Agent Skills
  directory (agentskills.io `SKILL.md` frontmatter) or, with `--library`, a
  directory of `<name>/SKILL.md` skills. Hard errors exit non-zero; soft
  violations are printed as warnings and included in `--json` output.

## Run commands

- `qcg runs list [--runs-dir dir] [--state <s>] [--generator <id>]`
- `qcg runs show <id> [--runs-dir dir] [--json] [--diagnose]` prints status, inputs,
  artifacts, and cost metrics (tokens in/out/cached, LLM calls, microUSD with
  a USD estimate, duration). `--diagnose` bundles journal failure signals
  (`step_failed`, `step_skipped`, `tool_failed`, `llm_route_failed`,
  `llm_validation_failed`, `step_retry`, `context_compacted`, guardrail and
  run errors) into one ordered screen plus read-only per-node LLM cost facts
  (`node_costs` folded from `llm_call` events: calls, tokens, microUSD);
  `--json --diagnose` returns
  `{summary, diagnosis}` with no threshold judgement.
- `qcg runs costs [--runs-dir dir] [--state <s>] [--generator <id>]
  [--by-model] [--json]` prints per-run cost totals plus a grand total.
  `--by-model` adds a provider/model breakdown with tokens, calls, contract
  unit prices (USD per 1M tokens, `n/a` when unpriced), and cost. Runs with
  unpriced billed calls are flagged, since their totals may understate spend.
- `qcg runs replay <id> [generator] [--runs-dir dir] [--output dir]
  [--reuse-seed] [--answer id=value]... [--confirm id=approve|deny]...
  [--confirmations-file file.json] [--json]` re-executes the recorded inputs
  with re-specified answers and confirmations for unattended completion.
- `qcg runs fork <id> --at-seq N [--state-patch patch.json]
  [--answer id=value]... [--confirm id=approve|deny]...
  [--confirmations-file file.json] [--runs-dir dir] [--json]` restores
  content-addressed files and folded state
  at an exact journal sequence, applies explicit input/step patches, and resumes
  the remaining DAG as a new run.
- `qcg runs invalidate <id> --reason "..." --actor-label "..."
  [--runs-dir dir] [--json]` irreversibly invalidates a durably successful
  run's result without changing its execution status or journal. Both strings
  are required, nonblank, and control-free; limits are 4096 UTF-8 bytes for
  the reason and 128 for the descriptive actor label (not authenticated identity).
  Exact retries return the original record; changed values conflict. `show`,
  `list`, and JSON summaries expose invalidation; `export` includes it in the
  bundle snapshot. There is no un-invalidate. See [operations](operations.md#run-invalidation).
- `qcg runs delete <id> [--runs-dir dir] [--json]` deletes a terminal run
  directory. Active runs are rejected; cancel first. A repeated delete
  reports not-found.
- `qcg runs export <id> [--runs-dir dir] [--output bundle.zip]` writes a
  self-contained run bundle (snapshot, inputs, journal, outputs, verified
  artifacts).
- `qcg runs trace <id> [--runs-dir dir] [--output trace.json]
  [--otlp-endpoint URL]` builds hierarchical run/step/event spans and writes or
  exports an OTLP JSON trace.
- `qcg runs gc [--runs-dir dir] [--keep N] [--keep-failed N] [--delete]`

GC is a dry run without `--delete` and never removes a non-terminal run.
`--keep` retains the newest nonfailed terminal runs (success, canceled, or
interrupted). Failed runs do not consume that budget and are governed only by
the separate `--keep-failed` budget. Both default to the deployment policy
(`GC_KEEP`, default `50`, and `GC_KEEP_FAILED`, default `10`); an explicit
count overrides the policy for that sweep, and a count of zero keeps nothing
of that budget, so `--keep 0` collects every nonfailed terminal run. A
retention window recorded in a run's journal expires that run independently of
the counts: a run past its window is collected even when the count budget
would keep it, while a run inside its window is still governed by the count
budgets. A sweep with `--keep 50 --keep-failed 10` may retain up to 50
nonfailed terminal runs plus 10 failed runs. A destructive sweep refuses a
store another holder is maintaining (`run store maintenance is busy; retry`);
the automatic sweeps skip such a store instead.

## Server and generated documentation

- `qcg serve [--bind 127.0.0.1] [--port 0]
  [--generators-dir dir] [--runs-dir dir] [--max-active-runs 8]
  [--run-store exclusive|shared-filesystem]
  [--cors-origin origin]... [--api-token token]
  [--max-request-bytes N] [--max-artifact-bytes N]
  [--max-artifact-entries N] [--max-asset-bytes N]`
- `qcg dev [--bind 127.0.0.1] [--port 0] [--generators-dir dir]
  [--runs-dir dir] [--max-active-runs 8] [--providers PATH]
  [--watch-interval-ms 500] [--eval generator-id]`
- `qcg docs step-schemas`
- `qcg docs run-events`
- `qcg docs openapi`

`qcg dev` runs the same server as `qcg serve` plus a polling watcher over
`--generators-dir`: it prints one line per changed path and, with `--eval`,
re-runs the generator's `suite.json` eval after each change. Eval failures are
reported but never stop the dev loop, and `--watch-interval-ms 0` is refused.
Contracts are re-read per request, so no restart is needed.

Port `0` selects an available port. `qcg serve` exposes REST, SSE, and assets
declared by generator contracts; it does not open a browser. The selected bind
is used directly. `--api-token` optionally protects the instance but does not
create per-user run ownership. Repeat `--cors-origin`
for multiple exact frontend origins; a comma-separated `CORS_ORIGIN`
environment variable is also accepted.

`--max-active-runs` limits concurrently executing API runs to 8 by default.
`MAX_ACTIVE_RUNS` sets the same value. Additional accepted runs wait in the
durable queue. Runs paused at a human-input or confirmation boundary release
their execution slot until resumed. The limit and provider runtimes are process-local; LLM and
search provider HTTP clients and MCP OAuth token managers are shared by runs in
one service process. Each run still owns an independent MCP protocol session.

`--max-total-steps` (or `MAX_TOTAL_STEPS`) caps every run below its
contract budget; omitted means the engine default. The enforced value and its
origin are recorded per run so admission, snapshots, and the journal agree.

For OAuth MCP profiles, use the loopback server's SPA Connections panel. The
server exposes the MCP authorization endpoints documented in the
[HTTP server guide](http-server-guide.md); there is no separate CLI command for
copying OAuth tokens, and tokens are kept in the OS keyring by default.

The default `exclusive` store locks `--runs-dir`. `shared-filesystem` permits
multiple services over a filesystem with reliable advisory locks, uses a
run-level execution lease to prevent duplicate execution, polls for abandoned
runs every 5 seconds, and follows journals written by another process. Each API
run has a UUID-based ID and separate `meta/journal.jsonl`
and `workspace/` paths. Direct `qcg run --output` invocations reject concurrent
use of the same output directory.

## Environment variables

- `RUST_LOG`: tracing filter, for example `qcg=debug,engine=trace`.
- `AUTO_GC=0|false|off`: disable every automatic retention sweep — `qcg
  serve`'s periodic task and the sweep that precedes `qcg run`. The manual
  `qcg runs gc` sweep is not automatic and still runs.
- `MAX_ACTIVE_RUNS`: maximum concurrently executing API runs for `qcg serve`
  (default `8`).
- `RUN_STORE`: `exclusive` or `shared-filesystem`.
- `API_TOKEN`: optional bearer token applied to the server API when set.
- `GENERATORS_DIR`: default directory for generator list/install/uninstall
  and serve; the per-command `--generators-dir` option overrides it.
- `PROVIDERS`: default path to the unified LLM, REST search, and MCP
  provider registry; the global `--providers <PATH>` option overrides it. See
  `docs/llm-provider-guide.md`. Search and MCP are opt-in: a declared
  `web.search` tool selects an explicit `[[search_provider]]` row, while a
  declared `mcp` tool selects an explicit `[[mcp_server]]` row. The selected
  hosts or exact stdio command must still be listed in the contract's
  permissions; there is no implicit fallback.
- `RATE_LIMIT_RPS` and `RATE_LIMIT_BURST`: optional per-credential
  token bucket for the HTTP server. Unset `RATE_LIMIT_RPS` disables rate
  limiting; `RATE_LIMIT_BURST` defaults to the rps value. Buckets are keyed
  by the presented bearer credential (one anonymous bucket otherwise), so
  credentials never share a budget. Requests over the limit receive `429` with
  `Retry-After`; `GET /healthz` is exempt. Invalid values, zero, or a burst
  without an rps refuse boot.
- `RATE_LIMIT_MAX_IDENTITIES` (default `4096`, range `1..=65536`) and
  `RATE_LIMIT_OVERFLOW` (`fold` or `reject`, default `fold`): deployment
  identity budget and overflow choice. The bucket map never exceeds the
  budget; at capacity `fold` shares the anonymous bucket while `reject`
  denies newcomers with `429` and keeps established budgets independent.
  Neither mode refills an exhausted bucket via rotation. Invalid values
  refuse boot.
- `LIVE_EVENT_CHANNEL_CAPACITY` (default `512`, range `16..=65536`),
  `JOURNAL_POLL_INTERVAL_MS` (default `250`, range `50..=5000`),
  `MAX_DIRECTORY_SCAN_ENTRIES` (default `100000`, range `1000..=10000000`),
  `SHARED_RESCAN_SECS` and `QUEUED_RESUMER_SECS` (default `5`, range
  `1..=3600`), `SHUTDOWN_DRAIN_SECS` (default `30`, range `1..=3600`) and
  `SHUTDOWN_SETTLE_SECS` (default `150`, range `1..=3600`): deployment
  tuning for the live stream, shared-store cadence, directory scans, queued-run
  recovery and journal-observation retry cadence, and the graceful shutdown
  phases. Settle must be at least drain; out-of-range values
  refuse boot. Total shutdown time is the sum of the configured drain and
  settle phases (180 s by default; at most 7200 s).
- `CONTAINER_CREATE_TIMEOUT_SECS` (default `600`),
  `CONTAINER_START_TIMEOUT_SECS` (default `120`),
  `CONTAINER_ADMIN_TIMEOUT_SECS` (default `60`), and
  `CONTAINER_STOP_TIMEOUT_SECS` (default `30`): host container-daemon
  operation budgets in seconds, each in `1..=3600`. Unset selects the default;
  malformed, zero, overflowing, or out-of-range values refuse server startup
  before bind and CLI execution. The CLI resolves the bundle for `run` except
  `--plan`, `eval`, `runs replay/fork`, and `serve`/`dev`. `models` and the
  non-executing `runs invalidate`, `runs delete`, and `runs export` commands do
  not consume daemon policy; service construction uses defaults. Unrelated
  commands such as `new` are unaffected. All execution paths receive one
  frozen deployment bundle for engine/MCP execution and cleanup. Create covers
  managed create/init, start covers start/readiness, admin
  covers discovery/configuration, and stop covers cleanup stop/kill/delete/rm.
  Docker keeps one-shot `run` and its independent workload timeout; stop only
  bounds cleanup. These are not generator, MCP profile, or per-run settings.
  Probe cadence and server shutdown budgets are unchanged. See
  [operations](operations.md#container-daemon-operation-policy).
- `MAX_PARALLEL_STEPS`: deployment cap on parallel wave scheduling.
  Unset uses the CPU count; `0` and invalid values refuse boot.
- `READ_CACHE_MAX_BYTES` (default `67108864`): maximum retained run read-view
  memory charge. Positive integers only; invalid values refuse boot. Entry
  count is also bounded by `MAX_TRACKED_RUNS`. This is a rebuildable observation
  cache; execution and artifact integrity verification still read originals.
- `API_TOKEN_FILE`: path to a file containing the instance bearer token.
  `--api-token` / `API_TOKEN` wins when set; an unreadable or empty file
  refuses boot instead of starting unauthenticated.
- `GC_KEEP` (default `50`), `GC_KEEP_FAILED` (default `10`), and
  `GC_INTERVAL_SECS` (default `86400`, minimum `60`): retention counts and
  the cadence of `qcg serve`'s retention task, which sweeps once when the
  task starts and then once per interval. The counts apply to every
  sweep — the periodic task, the sweep that precedes `qcg run`, and the
  manual `qcg runs gc` (whose `--keep`/`--keep-failed` override them).
  `GC_KEEP` counts newest nonfailed terminal runs (success, canceled,
  interrupted); failed runs do not consume it and are governed only by the
  separate `GC_KEEP_FAILED` count. A retention window recorded in a run's
  journal expires that run independently of the counts: a run past its window
  is collected even when the count budget would keep it, while a run inside
  its window is still governed by the count budgets. Invalid or zero values
  refuse boot.
- `AUDIT_FLOOR=minimal|standard`: deployment audit floor. `standard`
  restores full observation-record persistence for every run regardless of the
  contract's `[audit]` policy; a floor can only raise persistence.
- `CORS_ORIGIN`: comma-separated exact origins allowed for cross-origin
  API requests. CORS is disabled when it is unset. Allowed request headers are
  `authorization`, `content-type`, `idempotency-key`, and
  `x-qcg-package-sha256`; builds without the
  `server-cors` cargo feature refuse configured origins with an explicit error.
- `PACKAGE_MAX_ENTRIES` (default `1000`), `PACKAGE_MAX_BYTES`
  (default `10485760`): entry and expanded-byte bounds for `qcg package`
  and `qcg install`. `PACKAGE_MAX_ARCHIVE_BYTES`,
  `PACKAGE_MAX_METADATA_BYTES`: explicit max for archive and metadata
  bytes. Unset means no mechanistic limit for those two only.
- `MAX_INPUTS_FILE_BYTES`: explicit max for `qcg run --inputs-file`.
  Unset means no mechanistic limit.
- `MAX_REQUEST_BYTES` (default `2097152`, 2 MiB): whole-request-body limit
  for `qcg serve`. Omitted means the default, never unlimited.
- `MAX_ARTIFACT_BYTES`, `MAX_ARTIFACT_ENTRIES`: explicit maxima for the
  snapshot and dedicated artifact responses (`GET /api/runs/{id}` and idempotent
  start/fork snapshots plus `artifacts`, single `artifacts/{path}`, and
  `artifacts.zip`). Unset means the provisional shared default (16 MiB,
  1,000 entries) for those routes only, never unlimited. Larger explicit
  values are honored. Other routes (`bundle`, run-list) are not
  covered; producer `output_*` limits and direct CLI behavior are unchanged.
- `MAX_ASSET_BYTES` (default `16777216`, 16 MiB): maximum bytes for one served
  generator package asset. Omitted means the hard default, never unlimited.
