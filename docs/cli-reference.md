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
  `--plan` prints the execution plan (steps, permissions, declared outputs,
  pre-provision coverage) without running; add `--json` for machine-readable
  output. `--plan --diff` additionally prints a read-only forecast per node
  (declared writes, command allowlist result, literal HTTP host check, side-effect
  flag) plus whole-plan budget estimates. No filesystem writes, processes, or
  network requests are performed.
  A previous package (`.qcg` ZIP, `blueprint-package.json`, or any file) is
  re-entered as an ordinary `type = "file"` input via
  `--input-file field=path`, optionally combined with extra `--input` values
  for re-editing. See `fixtures/generators/package-reinput` for the
  file-plus-command expansion pattern.
- `qcg list [--generators-dir dir]` lists available packages from the installed
  directory and the bundled generator directory. Installed IDs take precedence.
- `qcg eval <generator> --suite suite.json [--output .qcg/evals]
  [--runs-dir .qcg/runs] [--baseline REPORT_JSON] [--json]` executes isolated
  cases and optional repeated seed variations. Assertions cover artifacts,
  regex, JSON Schema, manifest pointers, event counts and ordered trajectories,
  plus recorded metric ceilings. Reports include per-case pass counts across
  repetitions with flaky detection, per-case terminal metrics
  (`tokens_total`, `cost_microusd`, `duration_ms`, `llm_calls`), and baseline
  p50 cost/duration deltas. A baseline comparison fails on pass-rate or
  case regressions.
- `qcg package <dir> [-o package.qcg] [--signing-key key.pk8]
  [--max-entries N] [--max-bytes N] [--max-metadata-bytes N]` creates a `.qcg`
  ZIP from a directory, prints its SHA-256, and optionally writes detached
  Ed25519 `.sig` and `.pub` files. Archives are deterministic and include an
  SPDX 2.3 file inventory plus in-toto/SLSA provenance. Symlinks and output
  paths inside the source tree are rejected. There is no mechanistic size
  limit: `--max-entries`, `--max-bytes`, and `--max-metadata-bytes`
  (`QCG_PACKAGE_MAX_ENTRIES`, `QCG_PACKAGE_MAX_BYTES`,
  `QCG_PACKAGE_MAX_METADATA_BYTES`) enforce an explicit max only when set.
- `qcg install <path-or-url> [--sha256 HEX] [--signature HEX --public-key HEX]
  [--generators-dir dir] [--yes] [--force]
  [--max-entries N] [--max-bytes N] [--max-archive-bytes N]
  [--max-metadata-bytes N]` verifies, stages, validates,
  summarizes permissions, and installs a package. Remote packages must be
  pinned by SHA-256 or verified by an Ed25519 signature. A bare
  `<id>[@version-requirement]` instead installs from configured registries
  (see below), including `[dependencies]` recursively. Archive and inventory
  bounds are explicit-only
   (`QCG_PACKAGE_MAX_ENTRIES`, `QCG_PACKAGE_MAX_BYTES`,
   `QCG_PACKAGE_MAX_ARCHIVE_BYTES`, `QCG_PACKAGE_MAX_METADATA_BYTES`).
- `qcg registry add <name> <url>`, `qcg registry remove <name>`,
  `qcg registry list`: manage registry index URLs (`file://` or `https://`)
  under `$QCG_HOME` (default `~/.qcg`). Everything is user-owned files; no
  privilege is required. `qcg search <query>` matches id, description, and
  registry. Index rows may carry an Ed25519 `signature` with `key_id`;
  signed rows verify against `$QCG_HOME/trusted-keys.toml` (entries with an
  `expires` timestamp stop verifying after it), while unsigned rows rely on
  index transport plus the entry `sha256` checked at install.
- `qcg uninstall <id> [--generators-dir dir] [--yes]` removes one installed
  generator ID after checking that the ID is a safe relative path.

## Run commands

- `qcg runs list [--runs-dir dir] [--state <s>] [--generator <id>]`
- `qcg runs show <id> [--runs-dir dir] [--json] [--diagnose]` prints status, inputs,
  artifacts, and cost metrics (tokens in/out/cached, LLM calls, microUSD with
  a USD estimate, duration). `--diagnose` bundles journal failure signals
  (`step_failed`, `step_skipped`, `tool_failed`, `llm_route_failed`,
  `llm_validation_failed`, `step_retry`, `context_compacted`, guardrail and
  run errors) into one ordered screen; `--json --diagnose` returns
  `{summary, diagnosis}`.
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
- `qcg runs delete <id> [--runs-dir dir] [--json]` deletes a terminal run
  directory. Active runs are rejected; cancel first. A repeated delete
  reports not-found.
- `qcg runs export <id> [--runs-dir dir] [--output bundle.zip]` writes a
  self-contained run bundle (snapshot, inputs, journal, outputs, verified
  artifacts).
- `qcg runs trace <id> [--runs-dir dir] [--output trace.json]
  [--otlp-endpoint URL]` builds hierarchical run/step/event spans and writes or
  exports an OTLP JSON trace.
- `qcg runs gc [--runs-dir dir] [--keep 50] [--keep-failed 10] [--delete]`

GC is a dry run without `--delete` and never removes a non-terminal run.
Failed runs get an additional `--keep-failed` retention budget for post-mortems.

## Server and generated documentation

- `qcg serve [--bind 127.0.0.1] [--port 0]
  [--generators-dir dir] [--runs-dir dir] [--max-active-runs 8]
  [--run-store exclusive|shared-filesystem]
  [--cors-origin origin]... [--api-token token]
  [--max-request-bytes N] [--max-artifact-bytes N]
  [--max-artifact-entries N] [--max-asset-bytes N]`
- `qcg docs step-schemas`
- `qcg docs run-events`
- `qcg docs openapi`

Port `0` selects an available port. `qcg serve` exposes REST, SSE, and assets
declared by generator contracts; it does not open a browser. The selected bind
is used directly. `--api-token` optionally protects the instance but does not
create per-user run ownership. Repeat `--cors-origin`
for multiple exact frontend origins; a comma-separated `QCG_CORS_ORIGIN`
environment variable is also accepted.

`--max-active-runs` limits concurrently executing API runs to 8 by default.
`QCG_MAX_ACTIVE_RUNS` sets the same value. Additional accepted runs wait in the
durable queue. Runs paused at a human-input or confirmation boundary release
their execution slot until resumed. The limit and provider runtimes are process-local; LLM and
search provider HTTP clients and MCP OAuth token managers are shared by runs in
one service process. Each run still owns an independent MCP protocol session.

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

- `RUST_LOG`: tracing filter, for example `qcg=debug,qcg_engine=trace`.
- `QCG_AUTO_GC=0|false|off`: disable serve's periodic retention task.
- `QCG_MAX_ACTIVE_RUNS`: maximum concurrently executing API runs for `qcg serve`
  (default `8`).
- `QCG_RUN_STORE`: `exclusive` or `shared-filesystem`.
- `QCG_API_TOKEN`: optional bearer token applied to the server API when set.
- `QCG_GENERATORS_DIR`: default directory for generator list/install/uninstall
  and serve; the per-command `--generators-dir` option overrides it.
- `QCG_PROVIDERS`: default path to the unified LLM, REST search, and MCP
  provider registry; the global `--providers <PATH>` option overrides it. See
  `docs/llm-provider-guide.md`. Search and MCP are opt-in: a declared
  `web.search` tool selects an explicit `[[search_provider]]` row, while a
  declared `mcp` tool selects an explicit `[[mcp_server]]` row. The selected
  hosts or exact stdio command must still be listed in the contract's
  permissions; there is no implicit fallback.
- `QCG_CORS_ORIGIN`: comma-separated exact origins allowed for cross-origin
  API requests. CORS is disabled when it is unset.
- `QCG_PACKAGE_MAX_ENTRIES`, `QCG_PACKAGE_MAX_BYTES`,
  `QCG_PACKAGE_MAX_ARCHIVE_BYTES`, `QCG_PACKAGE_MAX_METADATA_BYTES`: explicit
  max for `qcg package` and `qcg install`. Unset means no mechanistic limit.
- `QCG_MAX_INPUTS_FILE_BYTES`: explicit max for `qcg run --inputs-file`.
  Unset means no mechanistic limit.
- `QCG_MAX_REQUEST_BYTES`, `QCG_MAX_ARTIFACT_BYTES`,
  `QCG_MAX_ARTIFACT_ENTRIES`, `QCG_MAX_ASSET_BYTES`: explicit max for
  `qcg serve` request bodies, artifact ZIP output, and generator assets.
  Unset means no mechanistic limit.
