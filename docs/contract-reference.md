# Contract Reference

Every generator is rooted at a `qcg.toml` manifest. The parser rejects unknown
fields, so this file is the authoritative surface developers can rely on.

## Package Layout

```text
my-generator/
  qcg.toml
  prompts/
  templates/
  schemas/
  resources/
  assets/
```

Only `qcg.toml` is required. Other directories are conventional paths used by
steps and resources.

## `[generator]`

Required:

- `id`: safe generator id.
- `name`: display name.
- `version`: generator version.

Optional:

- `description`
- `authors`
- `qcg_version`

## `[dependencies]`

Optional map of generator id to semver requirement, resolved at install time
from configured registries:

```toml
[dependencies]
report-charts = "^1.2"
```

Dependencies install as independent generators alongside the dependent; no
code or state is shared at runtime. Self-dependencies and invalid
requirements are rejected at validation.

## `[llm]`

The whole section is optional. If omitted, LLM steps are invalid and non-LLM
generators continue to work. It defines generator-wide defaults and resource
ceilings. Invocation policy belongs in each LLM node's `params.request`; an
agent-as-tool may add a final `request` layer. The deterministic order is
`[llm]` then node `request` then specialist `request`.

Fields:

- `model`: a single `{ provider, model }` entry declaring the default route.
  An LLM node can override it with `params.model` and declare an ordered
  `params.fallback_models` list. A retryable route failure advances through
  that list and records `llm_route_failed`. When omitted, the `[default]`
  model declared in `providers.toml` is used; a run fails when neither is
  present. Provider IDs come from the `providers.toml` registry documented in
  `docs/llm-provider-guide.md`.
- `input_cost_per_million_usd` / `output_cost_per_million_usd` on `model`:
  required when `budget.max_cost_usd` is set.
- `models`: additional priced model entries available to node-level routing.
- `temperature`: optional sampling temperature from `0` through `2`. Omit it
  when `reasoning_effort` is set.
- `top_p`: optional nucleus sampling value from `0` through `1`. It is mutually
  exclusive with `temperature` and `reasoning_effort`.
- `max_tokens`: required positive output limit. For reasoning models this
  includes both hidden reasoning tokens and visible output tokens; qcg maps it
  to the field required by the selected API.
- `journal_scan_window_bytes`: in-memory tail/repair scan window for the
  run journal, between 4096 and 67108864 bytes. Omitted uses 1 MiB. The
  window bounds repair memory; the total journal size stays governed by the
  `journal_*` limits.
- `max_context_bytes`
- `max_context_tokens`
- `max_media_bytes`: required aggregate byte limit when an LLM node declares
  image, audio, file, or video `params.media` input. Media paths remain confined to
  the run workspace and are encoded only after this bound is checked. Video
  inputs require a `video/` MIME type, request the `file_input` capability,
  and travel as file parts.
- `cache`: prompt-cache policy. Unset and `off` send no cache instructions;
  `auto` attaches the selected provider's declared cache mechanism
  (`prompt_cache_field`). A provider that does not advertise
  `capabilities.prompt_cache` refuses an `auto` request explicitly instead of
  dropping the policy. Cache reads are reported as `cached_input` tokens.
- `context_overflow`: `error` by default, or explicitly `truncate_head` /
  `truncate_tail`. Truncation is UTF-8 safe, visibly marked, deterministic,
  and recorded as `context_compacted`; no silent compaction occurs.
- `system`: generator-specific system text appended after qcg's
  mechanism-owned guardrail. Node and specialist `request.system` values are
  appended in layer order; all are rendered against the node context.
- `retry_prompt`: minijinja text used after schema validation failures.
  Available variables are `error` and `attempt`.
- `seed`: optional best-effort deterministic seed passed only to providers
  that advertise it. It cannot be combined with `reasoning_effort`.
- `reasoning_effort`: optional `none`, `minimal`, `low`, `medium`, `high`,
  `xhigh`, or `max`. Supported values are model-specific and must also appear
  in the selected registry row. Omission means that qcg sends no effort value;
  it is distinct from the explicit value `none`. Node-level and generator-wide
  values may be minijinja templates resolved from durable run variables
  immediately before each request; the resolved value is validated against the
  selected model's advertised list and follows the same incompatibility rules.
  A template that renders empty (for example when an optional `ask_user`
  effort selection was skipped) leaves that layer unset instead of sending an
  invalid value.
- `structured_output`: `auto` by default. `auto` uses strict native JSON Schema
  only when the Schema is compatible, otherwise uses the provider's compatible
  native mode, and falls back to prompt transport when the provider has no
  native Schema capability. `native_strict`, `native_compatible`, and `prompt`
  are explicit overrides. Every mode is followed by qcg's own Schema
  validation, so provider transport never becomes the correctness boundary.
  An explicit `native_strict` contract is rejected locally when any object
  Schema is open or has optional properties; it is never sent as a knowingly
  incompatible strict request.
- `stop_sequences`: at most eight non-empty strings of at most 1024 bytes.
- `tool_choice`: `none`, `auto`, `required`, or `{ tool = "declared_name" }`.
  It is valid only for tool-enabled invocations.
- `parallel_tool_calls`: optional explicit permission for the provider to emit
  parallel tool calls. qcg still serializes side-effecting and interactive
  tools at execution time.
- `verbosity`: optional `low`, `medium`, or `high` response-detail policy for a
  provider that advertises the mechanism.
- `requires`: provider capabilities required by all LLM nodes. Known
  capabilities are `tool_use`, `json_schema`,
  `structured_output_with_tools`, `seed`, `reasoning_effort`, `image_input`,
  `audio_input`, `file_input`, `streaming`, `temperature`, `top_p`,
  `stop_sequences`, `tool_choice`, `parallel_tool_calls`, and `verbosity`.
  Configured controls automatically require their corresponding capability.

Every LLM node accepts `params.request` with `clear`, `system`, `temperature`, `top_p`,
`max_tokens`, `stop_sequences`, `seed`, `reasoning_effort`,
`structured_output`, `tool_choice`, `parallel_tool_calls`, `verbosity`,
`stream`, `requires`, `max_context_bytes`, `max_context_tokens`,
`max_media_bytes`, `context_overflow`, and `retry_prompt`. Node and specialist
limits may tighten, but never raise, generator-wide ceilings. An explicit
`stop_sequences = []` clears the inherited list. Sampling and reasoning
controls clear incompatible inherited controls rather than sending an invalid
combination. `clear` is a typed list containing any of `temperature`, `top_p`,
`stop_sequences`, `seed`, `reasoning_effort`, `tool_choice`,
`parallel_tool_calls`, or `verbosity`; it explicitly omits inherited optional
controls before applying the current layer. Safety ceilings, system policy,
and required capabilities cannot be cleared. Agent tools additionally own `model`, `fallback_models`, and the
same `request` object; an explicitly empty specialist `fallback_models` list
disables fallback instead of inheriting the parent node's routes.

Provider credentials and endpoint overrides are declared per row in
`providers.toml`; see `docs/llm-provider-guide.md`.

## `[[inputs.stages]]`

Stage fields:

- `id`
- `when`: optional boolean expression.
- `fields`: nested field array.

Input field fields:

- `id`
- `label`: optional default display label. Clients fall back to a humanized
  `id` when it is absent.
- `label_i18n`: optional locale-to-label map. Clients match the display locale
  exactly, then by its primary language, and finally use `label`.
- `description` / `description_i18n`: explanatory text displayed next to the
  control.
- `placeholder` / `placeholder_i18n`: localized input hint.
- `type`: `string`, `text`, `number`, `boolean`, `select`, `multiselect`,
  `list`, `file`, `json`, or `natural_language`. A `json` field holds any
  JSON value (object, array, scalar); the Web UI submits it as a
  pretty-printed JSON textarea and the engine validates that the value is
  well-formed JSON. Namespaced lowercase custom kinds are also preserved;
  their value shape is defined by `schema` instead of an implicit string
  fallback.
- `required`
- `default`
- `pattern`
- `options`
- `option_labels_i18n`: optional locale-to-option-label map. Its innermost keys
  are values from `options`; localized labels never change submitted values.
- `min_items`
- `item_type`
- `schema`: arbitrary JSON Schema applied after the canonical field-type
  conversion. This is the authoritative extension surface for constraints not
  represented by the convenience fields above.
- `ui`: renderer-neutral metadata. The bundled Web UI understands `widget`,
  `input_type`, and `rows`; other clients may define additional keys without
  changing the execution contract.

Only active stages are resolved. A stage is active when `when` is absent or
evaluates true.

## `[resources.<name>]`

Fields:

- `type`: `file`, `dir`, `url`, `skill`, `skill_library`, `openapi`, or `exec`
- `path`: local package-relative path
- `url`: remote URL, fetched through the network allowlist
- `trust`: `trusted` or `untrusted`
- `llm_visible`: required before an LLM context can include the resource
- `pin_sha256`: optional hash pin for snapshotted URL/OpenAPI resources
- `cache_ttl_seconds`: optional remote snapshot TTL
- `params`: optional explicit bounds for the selected built-in resource type.
  `file`, `url`, and `openapi` resources accept a positive
  `max_bytes` limit; built-in `dir`, `skill`, and `skill_library` resources
  accept positive `max_files`, `max_bytes`, `max_depth`, `max_entries`, and
  `max_selected_bytes`. Unset means no mechanistic limit. Reads stop
  at an explicit bound; limit violations and directory walk failures are
  explicit errors.

A `skill` resource is an Agent Skills directory containing `SKILL.md`. The
frontmatter follows the agentskills.io specification: required `name` and
`description`, optional `license`, `compatibility`, `metadata` (a string map),
and `allowed-tools`. `allowed-tools` is informational and never grants
permissions. Conformant YAML is parsed for plain, quoted, folded (`>`), and
literal (`|`) scalars; missing `name` or `description`, unparseable
frontmatter, and a missing `SKILL.md` fail explicitly. Soft violations such as
a name that differs from the directory name are recorded as diagnostics in the
resource snapshot and logged instead of failing the run.

A `skill_library` resource scans its `path` for direct subdirectories
containing `SKILL.md` and exposes each one as a skill. It is the opt-in way to
vendor a skill collection into a package: the declaration names the library
root, and the skills inside are discovered automatically. Non-directory
entries, `.git`, `node_modules`, and directories without `SKILL.md` are
ignored.

An `exec` resource is the stock declarative extension boundary for an external
data source. It forbids `path` and `url`, requires
`params.command = ["program", "arg", ...]`, and accepts an optional positive
`params.max_bytes` (unbounded when omitted; otherwise checked against an
explicit `runtime.command_output_limit_bytes` when one is set). The complete command shape must also be
present in `permissions.commands`. qcg runs it through the ordinary isolated command
gateway before the flow starts, bounds stdout, snapshots it under run metadata,
checks `pin_sha256` when declared, and then exposes the immutable UTF-8 snapshot
through ordinary resource context. Non-zero exit, non-UTF-8 output, permission
denial, and limit violations fail explicitly.

LLM context selectors:

- `resources.name`
- `resources.openapi#paths`
- `resources.openapi#operations`
- `resources.openapi#operations(tag=tag-name)`
- `resources.skill#meta`
- `resources.skill#instructions`
- `resources.skill#tree` / `resources.skill#files`: sorted file
  metadata and content hashes for the skill directory.
- `resources.skill#files/path`: one UTF-8 file from a `skill` resource,
  for example `references/guide.md`.
- `resources.library#catalog`: the `{name, description}` catalog for a
  `skill_library`.
- `resources.library#meta/skill` / `#instructions/skill` / `#tree/skill`:
  per-skill metadata, instructions, and file listing. Structured references
  use `{ resource = "library", select = "instructions", path = "skill" }`.
- `resources.library#files/skill/references/path`: one UTF-8 file from a
  library skill.
- `resources.directory#tree` / `resources.directory#files`: sorted file
  metadata and content hashes for a `dir` resource.
- `resources.directory#files/path`: one UTF-8 file from a `dir` resource.
  Parent traversal, absolute paths, and symlink escapes are rejected.

## `[permissions]`

Workspace reads and writes, network access, commands, containers, and side
effects are denied unless explicitly declared.

- `fs_read`: include `workspace` to allow steps to read generated workspace
  files. Paths are normalized and symlink escapes are rejected.
- `fs_write`: include `workspace` to allow workspace writes.
- `network`: allowed host names.
- `commands`: allowlisted `{ bin, args, purpose, isolation, image? }` shapes.
  `isolation` is mandatory. `container` requires an `image` pinned with
  `@sha256:` and present in `permissions.containers.images`; `trusted_host`
  explicitly grants execution under the qcg OS identity and cannot name an
  image.
- `containers`: `{ enabled, runtime, images, on_missing }`. Enabled containers
  must select `docker`, `podman`, `docker_runsc`, `incus`, or `lxd`;
  runtime auto-detection is deliberately forbidden. Image pin forms follow
  the runtime: `docker`, `podman`, and `docker_runsc` require
  `name@sha256:<hex>`; `incus` and `lxd` require
  `<remote>:<path>@sha256:<fingerprint>` and launch by fingerprint so
  exactly the pinned bits run (pre-pull the fingerprint with
  `image copy`).
- `side_effects`: `none`, `confirm`, `dry_run_first`, or `allowed`.
- `side_effects_scope`: `invocation` (default) or `content`. The default
  applies at manifest-parse time only (`SideEffectScope::Invocation` when
  the key is omitted); every minted `ConfirmSpec` still carries an explicit
  `scope`, and a confirmation without one is corrupt (never silently
  invocation-scoped). `invocation`
  requires a fresh approval for every call; `content` reuses one approval for
  identical content until the run ends. Identical content means the same
  target plus the same canonical operation details: for HTTP, the method plus
  the full-URL digest (canonical URL with sorted query pairs, so undeclared
  query values also bind), the header digest, body digest, and
  `sensitive_query` digest
  (`http_operation_details` in
  `crates/qcg-engine/src/engine/run_context.rs`); for commands, the argv plus
  the stdin digest; secret values bind the digest but never enter the journal
  (see `docs/security.md` contract sandbox). URL query VALUES are redacted by
  default in journals (keys stay visible); `sensitive_query = ["name", ...]`
  additionally removes those values from the journaled target and step
  output, digests them into the approval, and passes them to the gateway so
  canary redaction covers the request and its final URL. The scope is
  recorded on the confirmation (`ConfirmSpec.scope`, required) so UI clients
  can show how far an approval reaches, and the digest is recorded on
  `ConfirmSpec.operation_digest` (required): a confirmation without either is
  corrupt and fails closed, never an invocation-scoped default. Approvals
  never cross runs: the operation id binds the run id, so an identical call
  in another run always re-confirms. MCP approvals live in two namespaces
  that never mix: single-shot `mcp.call` steps bind the node execution
  (`execution:<node>:<count>` invocation id), while agent `mcp` tool calls
  bind the model call id plus canonical redacted args (continuation key
  `<node>:agentmcp:<alias>:<invocation_hash>#__mcp_pending` in
  `crates/qcg-llm-steps/src/tool_events.rs`; the suffix marks stored
  continuations), so one can never authorize the
  other.

  Confirmation id forms (never the 2-element `<node>:<kind>` form):

  | scope | confirmation id form | authorizes |
  |---|---|---|
  | `content` | `<node>:<kind>:<operation_digest>` (3 parts) | identical content until the run ends |
  | `invocation` (default) | `<node>:<kind>:<operation_digest>:<invocation_hash>` (4 parts) | one call only |

  `operation_digest` is hex SHA-256 over `target + 0x00 + canonical details
  JSON`. `invocation_hash` is hex SHA-256 over the invocation id:
  `execution:<node>:<count>` for single-shot steps (finished-execution count;
  retries and crash resumes keep the current identity, while a repair or
  regenerate is a new invocation that re-confirms), or the stable model call
  id for agent tool calls. The operation id (`run:node:sha256(invocation)` in
  `crates/qcg-engine/src/state.rs`) is the remote idempotency key, never a
  confirmation id. Predict confirmation ids from a prior `confirm_request`
  event or run snapshot `confirm.id` (both carry the full id including scope and invocation hash); `side_effect` events carry only the content digest and never predict invocation-scoped ids alone; a second
  approval for different content and native convergence across resends are
  covered by tests in `run_context.rs`.

Container commands run without a shell with no network and only the run
workspace mounted at `/work`. Docker-compatible runtimes add a read-only
root, all capabilities dropped, no-new-privileges, a PID limit, and a
bounded `/tmp`. Incus-like runtimes (`incus`, `lxd`) launch by image
fingerprint with no NIC, a workspace-only disk device, and unprivileged
confinement.
The declared runtime is recorded in the command plan, and every backend
family guarantees cleanup on cancel and timeout, including when the
awaiting future is dropped. Trusted-host commands run without a shell with
a cleared environment, timeout, process-tree cancellation, and output
limits. Stdio MCP processes use the same declared isolation mode.

## `[tools.<name>]`

Logical tools describe what a flow needs without forcing the flow to know how
the runtime will execute it. `check.tool` currently supports validator tools.

Common fields:

- `kind`: currently `validator`
- `input`: default input path for the tool
- `command`: logical command vector. `{input}` is replaced with the node input.
- `network`: `none` or `permissioned`
- `workspace`: `read_only`, `writable`, or `none`
- `timeout_seconds`
- `output_limit_bytes`

Resolution:

```toml
[tools.qpx_validate.resolution]
allowed_backends = ["bundled", "container", "host"]
preferred_backends = ["bundled", "container", "host"]
fallback = "explicit"
```

`fallback = "explicit"` is the default. If an earlier backend is unavailable,
the runtime requires user confirmation before falling back. Non-interactive runs
therefore fail instead of silently using a different backend.

Backends:

- `backends.host`: host binary. The resulting command must be allowed by
  `[permissions].commands`.
- `backends.bundled`: generator-relative binary path plus `sha256`.
- `backends.container`: pinned image and mount path. The image must be allowed
  by `[permissions].containers`.

Only `host`, `bundled`, and `container` are accepted backend fields.

## `[secrets.<name>]`

Each secret declares exactly one source. `env` reads a value directly:

```toml
[secrets.api_token]
env = "API_TOKEN"
```

`file_env` instead names an environment variable containing an absolute path
to a private, non-symlink UTF-8 file no larger than 64 KiB. This supports
Vault Agent and mounted-secret rotation at run boundaries without embedding a
secret in process configuration:

```toml
[secrets.api_token]
file_env = "API_TOKEN_FILE"
```

Secret values are loaded at runtime. They are not written into manifests,
journals, or LLM prompts; secret placeholders may be injected by the
`inject_secrets` transform.

## `[[flow]]`

Common node fields:

- `id`
- `type`
- `needs` (omission means the previous flow entry; roots omit it)
- `when`
- `on_deps`: `all_succeeded`, `any_succeeded`, or `none_failed` (skipped
  dependencies satisfy the policy; only failures block)
- `context`
- `output`
- `artifact`
- `on_fail`
- `failure`
- `retry` (optional execution retry policy, see below)
- `params` (closed, step-specific table)

Root-level `parallel = ["lint", "test"]` explicitly identifies a contiguous
parallel group; normal flow order is sequential. `when` expressions may inspect
both `steps.X.output` and `steps.X.status`.

Blocks may contain nested `foreach` nodes. Every level consumes the global step
budget, applies its own required iteration bound, uses a fully qualified node
path, and restores the parent `item` after the nested block finishes.

Resource context supports a closed table form in addition to short strings:

```toml
context = [
  { resource = "todo_api", select = "operations", tag = "todos" },
  { resource = "guide", select = "file", path = "README.md" },
]
```

### `retry`

Declares per-node execution retries. Counts and waits are policy declared by
the generator; the engine only executes them:

```toml
[[flow]]
id = "fetch_report"
type = "http"
needs = []
retry = { max_attempts = 3, backoff_ms = 1000, timeout_secs = 60 }
```

- `max_attempts`: total attempts including the first, 1 to 16 (default 1,
  meaning no retry).
- `backoff_ms`: fixed wait between attempts, up to 60000 (default 0).
- `timeout_secs`: per-attempt execution timeout, at least 1 when set
  (omitted means no timeout).

Only execution failures are retried. Contract, budget, and cancellation
errors fail fast, and cancellation during backoff aborts the wait. Each
failed attempt emits a `step_retry` journal event with `attempt`,
`max_attempts`, and `error`. Unified budget rule: every charged attempt
consumes the run-wide step budget, including ordinary retries; no path
gets a free retry while another pays. Repair/regenerate per-attempt
consumes follow the same rule through the shared retry wrapper
(`execute_node_with_retry`), including the private
`execute_node_after_budget` entry which is reachable only through that
wrapper so retry, timeout, and elapsed policies cannot be bypassed.
`retry` applies to scheduler-dispatched nodes
and to `foreach` children: every child runs through the same retry wrapper
with its own `max_attempts`, per-attempt `timeout_secs`, and the run-wide
elapsed deadline, for `parallel=1`, parallel, and nested iterations alike
(E10). Foreach budget follows the single-charge rule: the outer foreach
node is charged once total and children share that budget without
per-child consume. A fired node timeout or elapsed deadline cancels the node scope and
allows a 5 s cooperative grace: a parent cancel during the grace keeps the
cancel classification, a cooperative cancel normalizes to the fired deadline
(`TimedOut` retryable, `ElapsedExceeded` never retried), and any other
settlement inside the grace is adopted for node timeout but never for the
hard elapsed deadline, which always reports `ElapsedExceeded` once fired
(E11). Repair/regenerate cycles keep their own admission semantics.

Step-specific fields such as `prompt`, `output_file`, `command`, or `expect`
must appear under `[flow.params]`. Unknown fields and invalid types are rejected
during contract loading with a source line.

### Expressions

`when` accepts a boolean expression over `inputs.*`, `steps.<id>.output`,
`steps.<id>.status`, and (inside `foreach`) `item`. Paths resolve dotted
fields with numeric array indices (`inputs.sites.0`). Missing paths read as
`null`. Operators are `!`, `-` (unary), `||`, `&&`, `==`, `!=`, `>`, `<`,
`>=`, `<=`, `+`, `-`, `*`, `/`, `%`, with parentheses and array literals
(`[1, 2]`). Available functions:

- predicates: `len`, `contains`, `empty`, `starts_with`, `ends_with`
- defaults: `default(value, fallback)` (fallback only for `null`)
- strings: `upper`, `lower`, `trim`, `split`, `join`, `replace`
- collections: `first`, `last`, `keys`, `values`, `reverse`, `flatten`,
  `unique`, `sort` (numbers, strings, or booleans only)
- numbers: `sum`, `min`, `max` (empty arrays yield `null` for `min`/`max`)

`foreach.items` accepts a plain dotted path or any value-producing
expression such as `sort(inputs.tags)`. Unknown functions and type mismatches
are explicit errors.

## Step Types

`render`
: Render a package template to `output_file`.

`write`
: Render inline `content` to `output_file`.

`copy`
: Copy workspace/package content from `source` to `target`.

`transform`
: Requires `transform`, `source`, and `target`. Supported transforms are
  `inject_secrets`, `json_pretty`, `json_compact`, `toml_to_json`,
  `json_to_toml`, `json_merge`, and `zip`. `json_merge` also requires `with`;
  values from `source` win on key conflicts.

`command`
: Execute an allowlisted command vector.

`http`
: Execute an allowlisted HTTP request. Non-GET/HEAD methods are side effects.

`ask_user`
: Ask for a scalar answer, a static multi-field form through `fields`, or a
  dynamic form loaded from `fields_from`. `options` restrict scalar answers.
  `default` may select one declared scalar option for form-capable clients and
  for an empty interactive CLI answer.
  `content_i18n` localizes the rendered question, while
  `option_labels_i18n` localizes scalar option labels without changing their
  values. Static and dynamic fields use the input-field localization members
  described above.

  A scalar `options_from` or a field-level `options_from` names a dotted run
  variable path (`inputs.*`, `steps.<id>.output.*`, or `item.*`) whose value
  supplies the options at run time. Entries are strings or
  `{ value, label?, label_i18n? }` objects; `label_i18n` is keyed by language
  then value. `options_from` is mutually exclusive with static `options`, never
  valid on pre-run `[inputs]` fields, and an empty result fails the step
  explicitly instead of becoming an unconstrained text field. When combined
  with the `llm.catalog` step this exposes the live provider/model/effort
  catalog as form choices without any catalog logic in the form engine.

`llm.catalog`
: Read the selectable provider/model/effort catalog as a step output. `select`
  is `providers`, `models`, or `efforts`; `models` requires `provider`,
  `efforts` requires `provider` and `model` (both may be templates).
  `enabled_only` (default `true`) hides disabled entries, and `require` filters
  models by capability. The output contains `options` (values only), `entries`
  (value, label, metadata), `count`, and a `catalog` slice. Use
  `options_from = "steps.<id>.output.options"` or `.entries` to bind it to an
  `ask_user` form. The step never calls an LLM and never grants permission.

`check.schema`
: Validate a workspace JSON file against a package JSON schema.

`check.format`
: Validate JSON or TOML syntax.

`check.command`
: Run a command and check `expect.exit_code` and/or
  `expect.stdout_contains`.

`check.tool`
: Resolve and run a logical validator tool. The flow declares only `tool` and
  `input`; backend choice is handled by runtime resolution.

`check.container`
: Run a container check when a container runtime exists and the manifest allows
  the image. `on_missing` controls missing-runtime behavior.

`check.contract`
: Load and validate a generated qcg package.

`await`
: Wait for other runs to reach a terminal state. Params: `runs` (run id
  array, at least one) and optional `timeout_secs` (at least 1 when set).
  Succeeds with an object mapping each run id to its terminal state; unknown
  runs and timeouts fail explicitly. Needs server execution with sibling run
  visibility.

`mcp.call`
: Call a tool on a configured MCP server. Transport failures (connect and
  call) fail the run unless `optional = true`, which degrades to a null
  output and records a `degraded` tool call. Validation, elicitation, and
  confirmation paths still fail.

`llm.generate`
: Produce text, optionally writing it to `output_file`.

`llm.fill`
: Produce JSON and validate it against `schema`. Invalid JSON/schema responses
  retry up to `max_iterations` attempts.

`llm.choose`
: Choose from the closed `options` list. Out-of-set responses retry up to
  `max_iterations` attempts.

`llm.repair`
: Ask the LLM to rewrite `source` into `target`.

`llm.agent`
: Run a bounded tool loop. Requires `max_iterations`,
  `max_tokens_total`, and declared `tools`.

`foreach`
: Iterate over an array or object at `items` and execute the named `subflow`
  block. Array entries are `item`; object entries expose `item.key` and
  `item.value`. Requires `max_iterations`. Parallel iterations share the
  workspace, the journal, and the run-wide budget atomics
  (shared-budget/shared-journal): there is no per-iteration isolation for
  side effects, only for the variable scope cloned per iteration. Budget
  follows the single-charge rule (outer charged once, children share
  without per-child consume).

## Generated Step Parameter Schemas

The following block is generated from the registered `StepExecutor::params_schema()`
metadata. Update it with `qcg docs step-schemas`.

<!-- qcg-step-schemas:start -->
### `ask_user`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "type": "string"
    },
    "content_i18n": {
      "additionalProperties": {
        "type": "string"
      },
      "type": "object"
    },
    "default": {
      "type": "string"
    },
    "fields": {
      "items": {
        "type": "object"
      },
      "type": "array"
    },
    "fields_from": {
      "type": "string"
    },
    "option_labels_i18n": {
      "additionalProperties": {
        "additionalProperties": {
          "type": "string"
        },
        "type": "object"
      },
      "type": "object"
    },
    "options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "options_from": {
      "type": "string"
    }
  },
  "required": [
    "content"
  ],
  "type": "object"
}
```

### `await`

```json
{
  "additionalProperties": false,
  "properties": {
    "runs": {
      "items": {
        "type": "string"
      },
      "minItems": 1,
      "type": "array"
    },
    "timeout_secs": {
      "minimum": 1,
      "type": "integer"
    }
  },
  "required": [
    "runs"
  ],
  "type": "object"
}
```

### `check.command`

```json
{
  "additionalProperties": false,
  "properties": {
    "command": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "expect": {
      "properties": {
        "exit_code": {
          "type": "integer"
        },
        "exit_code_in": {
          "items": {
            "type": "integer"
          },
          "type": "array"
        },
        "stderr_contains": {
          "type": "string"
        },
        "stdout_contains": {
          "type": "string"
        },
        "stdout_matches": {
          "type": "string"
        }
      },
      "type": "object"
    }
  },
  "required": [
    "command"
  ],
  "type": "object"
}
```

### `check.container`

```json
{
  "additionalProperties": false,
  "properties": {
    "command": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "content": {
      "type": "string"
    },
    "expect": {
      "properties": {
        "exit_code": {
          "type": "integer"
        },
        "exit_code_in": {
          "items": {
            "type": "integer"
          },
          "type": "array"
        },
        "stderr_contains": {
          "type": "string"
        },
        "stdout_contains": {
          "type": "string"
        },
        "stdout_matches": {
          "type": "string"
        }
      },
      "type": "object"
    },
    "image": {
      "type": "string"
    },
    "mounts": {
      "items": {
        "properties": {
          "from": {
            "type": "string"
          },
          "mode": {
            "enum": [
              "ro",
              "rw"
            ],
            "type": "string"
          },
          "to": {
            "type": "string"
          }
        },
        "required": [
          "from",
          "to"
        ],
        "type": "object"
      },
      "type": "array"
    }
  },
  "required": [
    "command"
  ],
  "type": "object"
}
```

### `check.contract`

```json
{
  "additionalProperties": false,
  "properties": {
    "source": {
      "type": "string"
    }
  },
  "required": [
    "source"
  ],
  "type": "object"
}
```

### `check.format`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "enum": [
        "json",
        "toml"
      ],
      "type": "string"
    },
    "source": {
      "type": "string"
    }
  },
  "required": [
    "source",
    "content"
  ],
  "type": "object"
}
```

### `check.schema`

```json
{
  "additionalProperties": false,
  "properties": {
    "schema": {
      "type": "string"
    },
    "source": {
      "type": "string"
    }
  },
  "required": [
    "source",
    "schema"
  ],
  "type": "object"
}
```

### `check.tool`

```json
{
  "additionalProperties": false,
  "properties": {
    "input": {
      "type": "string"
    },
    "tool": {
      "type": "string"
    }
  },
  "required": [
    "tool"
  ],
  "type": "object"
}
```

### `command`

```json
{
  "additionalProperties": false,
  "properties": {
    "command": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "input": {},
    "input_file": {
      "type": "string"
    },
    "input_file_scope": {
      "enum": [
        "workspace",
        "package"
      ],
      "type": "string"
    },
    "output_schema": {},
    "result": {
      "enum": [
        "process",
        "structured"
      ],
      "type": "string"
    },
    "tool": {
      "type": "string"
    }
  },
  "required": [],
  "type": "object"
}
```

### `copy`

```json
{
  "additionalProperties": false,
  "properties": {
    "source": {
      "type": "string"
    },
    "target": {
      "type": "string"
    }
  },
  "required": [
    "source",
    "target"
  ],
  "type": "object"
}
```

### `fail`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "type": "string"
    }
  },
  "required": [],
  "type": "object"
}
```

### `foreach`

```json
{
  "additionalProperties": false,
  "properties": {
    "items": {
      "type": "string"
    },
    "max_iterations": {
      "maximum": 10000,
      "minimum": 1,
      "type": "integer"
    },
    "parallel": {
      "maximum": 256,
      "minimum": 1,
      "type": "integer"
    },
    "subflow": {
      "type": "string"
    }
  },
  "required": [
    "items",
    "subflow",
    "max_iterations"
  ],
  "type": "object"
}
```

### `http`

```json
{
  "additionalProperties": false,
  "properties": {
    "body_base64": {
      "type": "string"
    },
    "body_file": {
      "type": "string"
    },
    "body_file_scope": {
      "enum": [
        "workspace",
        "package"
      ],
      "type": "string"
    },
    "body_json": {},
    "body_text": {
      "type": "string"
    },
    "content_type": {
      "type": "string"
    },
    "headers": {
      "additionalProperties": {
        "type": "string"
      },
      "type": "object"
    },
    "method": {
      "type": "string"
    },
    "output": {
      "enum": [
        "text",
        "json",
        "base64",
        "file"
      ],
      "type": "string"
    },
    "output_file": {
      "type": "string"
    },
    "sensitive_query": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "url": {
      "type": "string"
    }
  },
  "required": [
    "url"
  ],
  "type": "object"
}
```

### `llm.agent`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "guardrails": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "kind": {
            "enum": [
              "regex_deny",
              "json_schema",
              "command"
            ]
          },
          "name": {
            "type": "string"
          },
          "on_error": {
            "enum": [
              "fail",
              "block"
            ]
          },
          "params": {},
          "stage": {
            "enum": [
              "input",
              "output",
              "tool_input",
              "tool_output"
            ]
          },
          "tool": {
            "type": "string"
          },
          "tripwire": {
            "type": "boolean"
          }
        },
        "required": [
          "name",
          "stage",
          "kind"
        ],
        "type": "object"
      },
      "type": "array"
    },
    "max_iterations": {
      "minimum": 1,
      "type": "integer"
    },
    "max_tokens_total": {
      "minimum": 1,
      "type": "integer"
    },
    "max_tool_calls_total": {
      "minimum": 1,
      "type": "integer"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file",
              "video"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
    },
    "tools": {
      "items": {
        "oneOf": [
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "input_schema": {
                "type": "object"
              },
              "kind": {
                "const": "fs.write"
              },
              "name": {
                "type": "string"
              },
              "path_prefix": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind",
              "path_prefix"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "command": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "description": {
                "type": "string"
              },
              "input_schema": {
                "type": "object"
              },
              "kind": {
                "const": "command"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind",
              "command"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "hosts": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "input_schema": {
                "type": "object"
              },
              "kind": {
                "const": "http"
              },
              "methods": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind",
              "methods",
              "hosts"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "input_schema": {
                "type": "object"
              },
              "kind": {
                "const": "ask_user"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "kind": {
                "const": "web.search"
              },
              "max_calls": {
                "maximum": 10,
                "minimum": 1,
                "type": "integer"
              },
              "max_results": {
                "maximum": 20,
                "minimum": 1,
                "type": "integer"
              },
              "name": {
                "type": "string"
              },
              "provider": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "kind": {
                "const": "mcp"
              },
              "max_calls": {
                "maximum": 10,
                "minimum": 1,
                "type": "integer"
              },
              "name": {
                "type": "string"
              },
              "server": {
                "type": "string"
              },
              "side_effects": {
                "type": "boolean"
              },
              "tool": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind",
              "server",
              "tool"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "fallback_models": {
                "items": {
                  "additionalProperties": false,
                  "properties": {
                    "clear": {
                      "items": {
                        "enum": [
                          "temperature",
                          "top_p",
                          "stop_sequences",
                          "seed",
                          "reasoning_effort",
                          "tool_choice",
                          "parallel_tool_calls",
                          "verbosity"
                        ]
                      },
                      "type": "array",
                      "uniqueItems": true
                    },
                    "input_cost_per_million_usd": {
                      "minimum": 0,
                      "type": "number"
                    },
                    "model": {
                      "minLength": 1,
                      "type": "string"
                    },
                    "output_cost_per_million_usd": {
                      "minimum": 0,
                      "type": "number"
                    },
                    "provider": {
                      "minLength": 1,
                      "type": "string"
                    }
                  },
                  "required": [
                    "provider",
                    "model"
                  ],
                  "type": "object"
                },
                "maxItems": 8,
                "type": "array"
              },
              "handoff": {
                "type": "boolean"
              },
              "input_schema": {
                "type": "object"
              },
              "instructions": {
                "type": "string"
              },
              "kind": {
                "const": "agent"
              },
              "max_calls": {
                "maximum": 10,
                "minimum": 1,
                "type": "integer"
              },
              "max_iterations": {
                "maximum": 32,
                "minimum": 1,
                "type": "integer"
              },
              "max_tokens_total": {
                "minimum": 1,
                "type": "integer"
              },
              "max_tool_calls_total": {
                "minimum": 1,
                "type": "integer"
              },
              "model": {
                "additionalProperties": false,
                "properties": {
                  "clear": {
                    "items": {
                      "enum": [
                        "temperature",
                        "top_p",
                        "stop_sequences",
                        "seed",
                        "reasoning_effort",
                        "tool_choice",
                        "parallel_tool_calls",
                        "verbosity"
                      ]
                    },
                    "type": "array",
                    "uniqueItems": true
                  },
                  "input_cost_per_million_usd": {
                    "minimum": 0,
                    "type": "number"
                  },
                  "model": {
                    "minLength": 1,
                    "type": "string"
                  },
                  "output_cost_per_million_usd": {
                    "minimum": 0,
                    "type": "number"
                  },
                  "provider": {
                    "minLength": 1,
                    "type": "string"
                  }
                },
                "required": [
                  "provider",
                  "model"
                ],
                "type": "object"
              },
              "name": {
                "type": "string"
              },
              "on_failure": {
                "additionalProperties": false,
                "properties": {
                  "by_code": {
                    "additionalProperties": false,
                    "properties": {
                      "guardrail_rejected": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "iteration_budget_exceeded": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "provider_failed": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "token_budget_exceeded": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "tool_call_budget_exceeded": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "tool_failed": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "validation_failed": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      }
                    },
                    "type": "object"
                  },
                  "default": {
                    "enum": [
                      "fail",
                      "return_error"
                    ]
                  }
                },
                "type": "object"
              },
              "output_schema": {
                "type": "string"
              },
              "request": {
                "additionalProperties": false,
                "properties": {
                  "context_overflow": {
                    "enum": [
                      "error",
                      "truncate_head",
                      "truncate_tail"
                    ]
                  },
                  "max_context_bytes": {
                    "minimum": 1,
                    "type": "integer"
                  },
                  "max_context_tokens": {
                    "minimum": 1,
                    "type": "integer"
                  },
                  "max_media_bytes": {
                    "minimum": 1,
                    "type": "integer"
                  },
                  "max_tokens": {
                    "minimum": 1,
                    "type": "integer"
                  },
                  "parallel_tool_calls": {
                    "type": "boolean"
                  },
                  "reasoning_effort": {
                    "enum": [
                      "none",
                      "minimal",
                      "low",
                      "medium",
                      "high",
                      "xhigh",
                      "max"
                    ]
                  },
                  "requires": {
                    "items": {
                      "enum": [
                        "tool_use",
                        "json_schema",
                        "structured_output_with_tools",
                        "seed",
                        "reasoning_effort",
                        "image_input",
                        "audio_input",
                        "file_input",
                        "streaming",
                        "temperature",
                        "top_p",
                        "stop_sequences",
                        "tool_choice",
                        "parallel_tool_calls",
                        "verbosity"
                      ]
                    },
                    "type": "array",
                    "uniqueItems": true
                  },
                  "retry_prompt": {
                    "type": "string"
                  },
                  "seed": {
                    "minimum": 0,
                    "type": "integer"
                  },
                  "stop_sequences": {
                    "items": {
                      "maxLength": 1024,
                      "minLength": 1,
                      "type": "string"
                    },
                    "maxItems": 8,
                    "type": "array"
                  },
                  "stream": {
                    "type": "boolean"
                  },
                  "structured_output": {
                    "enum": [
                      "auto",
                      "native_strict",
                      "native_compatible",
                      "prompt"
                    ]
                  },
                  "system": {
                    "type": "string"
                  },
                  "temperature": {
                    "maximum": 2,
                    "minimum": 0,
                    "type": "number"
                  },
                  "tool_choice": {
                    "oneOf": [
                      {
                        "enum": [
                          "none",
                          "auto",
                          "required"
                        ]
                      },
                      {
                        "additionalProperties": false,
                        "properties": {
                          "tool": {
                            "minLength": 1,
                            "type": "string"
                          }
                        },
                        "required": [
                          "tool"
                        ],
                        "type": "object"
                      }
                    ]
                  },
                  "top_p": {
                    "maximum": 1,
                    "minimum": 0,
                    "type": "number"
                  },
                  "verbosity": {
                    "enum": [
                      "low",
                      "medium",
                      "high"
                    ]
                  }
                },
                "type": "object"
              },
              "tools": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              }
            },
            "required": [
              "name",
              "kind",
              "instructions",
              "max_tool_calls_total"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    }
  },
  "required": [
    "prompt",
    "max_iterations",
    "max_tokens_total"
  ],
  "type": "object"
}
```

### `llm.catalog`

```json
{
  "additionalProperties": false,
  "properties": {
    "enabled_only": {
      "type": "boolean"
    },
    "model": {
      "type": "string"
    },
    "provider": {
      "type": "string"
    },
    "require": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "select": {
      "enum": [
        "providers",
        "models",
        "efforts"
      ]
    }
  },
  "required": [
    "select"
  ],
  "type": "object"
}
```

### `llm.choose`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "max_iterations": {
      "minimum": 1,
      "type": "integer"
    },
    "max_tokens_total": {
      "minimum": 1,
      "type": "integer"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file",
              "video"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
    }
  },
  "required": [
    "prompt",
    "options",
    "max_iterations",
    "max_tokens_total"
  ],
  "type": "object"
}
```

### `llm.decide`

```json
{
  "$defs": {
    "content": {
      "allOf": [
        {
          "$ref": "#/$defs/literal"
        }
      ],
      "type": [
        "string",
        "object",
        "array"
      ]
    },
    "literal": {
      "anyOf": [
        {
          "$ref": "#/$defs/text"
        },
        {
          "type": [
            "null",
            "boolean",
            "number"
          ]
        },
        {
          "items": {
            "$ref": "#/$defs/literal"
          },
          "type": "array"
        },
        {
          "additionalProperties": {
            "$ref": "#/$defs/literal"
          },
          "propertyNames": {
            "$ref": "#/$defs/text"
          },
          "type": "object"
        }
      ]
    },
    "text": {
      "not": {
        "pattern": "\\{\\{|\\{%|\\{#"
      },
      "type": "string"
    }
  },
  "additionalProperties": false,
  "oneOf": [
    {
      "required": [
        "state"
      ]
    },
    {
      "required": [
        "state_from"
      ]
    }
  ],
  "properties": {
    "max_tokens": {
      "description": "Local total input plus output usage ceiling, not sent upstream. Independent of [llm].max_tokens, which limits chat output. No [llm] chat controls are inherited; run-wide budgets remain enforced.",
      "maximum": 4294967295,
      "minimum": 1,
      "type": "integer"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "$ref": "#/$defs/text",
          "pattern": "\\S"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "$ref": "#/$defs/text",
          "pattern": "\\S"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "questions": {
      "additionalProperties": {
        "oneOf": [
          {
            "additionalProperties": false,
            "properties": {
              "criteria": {
                "additionalProperties": false,
                "properties": {
                  "false": {
                    "$ref": "#/$defs/text"
                  },
                  "true": {
                    "$ref": "#/$defs/text"
                  }
                },
                "type": [
                  "object",
                  "null"
                ]
              },
              "instructions": {
                "$ref": "#/$defs/content"
              },
              "type": {
                "const": "noul"
              }
            },
            "required": [
              "type",
              "instructions"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "criteria": {
                "additionalProperties": {
                  "anyOf": [
                    {
                      "$ref": "#/$defs/text"
                    },
                    {
                      "type": "null"
                    }
                  ]
                },
                "maxProperties": 255,
                "minProperties": 2,
                "propertyNames": {
                  "$ref": "#/$defs/text"
                },
                "type": "object"
              },
              "instructions": {
                "$ref": "#/$defs/content"
              },
              "type": {
                "const": "choice"
              }
            },
            "required": [
              "type",
              "instructions",
              "criteria"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "criteria": {
                "items": {
                  "$ref": "#/$defs/text"
                },
                "maxItems": 255,
                "minItems": 2,
                "type": "array"
              },
              "instructions": {
                "$ref": "#/$defs/content"
              },
              "type": {
                "const": "score"
              }
            },
            "required": [
              "type",
              "instructions",
              "criteria"
            ],
            "type": "object"
          }
        ]
      },
      "minProperties": 1,
      "propertyNames": {
        "$ref": "#/$defs/text"
      },
      "type": "object"
    },
    "state": {
      "$ref": "#/$defs/content"
    },
    "state_from": {
      "$ref": "#/$defs/text",
      "description": "ValueBag path resolved at execution without template rendering.",
      "pattern": "^(inputs\\.[^.\\s]+(\\.[^.\\s]+)*|steps\\.[^.\\s]+\\.(output|status)(\\.[^.\\s]+)*|item(\\.[^.\\s]+)*)$"
    }
  },
  "required": [
    "model",
    "questions",
    "max_tokens"
  ],
  "type": "object"
}
```

### `llm.fill`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "max_iterations": {
      "minimum": 1,
      "type": "integer"
    },
    "max_tokens_total": {
      "minimum": 1,
      "type": "integer"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file",
              "video"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
    }
  },
  "required": [
    "prompt",
    "max_iterations",
    "max_tokens_total"
  ],
  "type": "object"
}
```

### `llm.generate`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file",
              "video"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
    }
  },
  "required": [
    "prompt"
  ],
  "type": "object"
}
```

### `llm.repair`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file",
              "video"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
    },
    "source": {
      "type": "string"
    },
    "target": {
      "type": "string"
    }
  },
  "required": [
    "prompt"
  ],
  "type": "object"
}
```

### `mcp.call`

```json
{
  "additionalProperties": false,
  "properties": {
    "arguments": {
      "type": "object"
    },
    "input_schema": {},
    "optional": {
      "type": "boolean"
    },
    "output_schema": {},
    "server": {
      "type": "string"
    },
    "side_effects": {
      "type": "boolean"
    },
    "timeout_seconds": {
      "minimum": 1,
      "type": "integer"
    },
    "tool": {
      "type": "string"
    }
  },
  "required": [
    "server",
    "tool"
  ],
  "type": "object"
}
```

### `render`

```json
{
  "additionalProperties": false,
  "properties": {
    "output_file": {
      "type": "string"
    },
    "template": {
      "type": "string"
    }
  },
  "required": [
    "template",
    "output_file"
  ],
  "type": "object"
}
```

### `transform`

```json
{
  "additionalProperties": false,
  "properties": {
    "remove_source": {
      "type": "boolean"
    },
    "secrets": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "source": {
      "type": "string"
    },
    "target": {
      "type": "string"
    },
    "transform": {
      "enum": [
        "inject_secrets",
        "json_pretty",
        "json_compact",
        "toml_to_json",
        "json_to_toml",
        "json_merge",
        "base64_decode",
        "base64_encode",
        "zip"
      ],
      "type": "string"
    },
    "unix_mode": {
      "pattern": "^0[6-7][0-7]{2}$",
      "type": "string"
    },
    "with": {
      "type": "string"
    }
  },
  "required": [
    "transform",
    "source",
    "target"
  ],
  "type": "object"
}
```

### `write`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "type": "string"
    },
    "output_file": {
      "type": "string"
    },
    "unix_mode": {
      "pattern": "^0[6-7][0-7]{2}$",
      "type": "string"
    }
  },
  "required": [
    "output_file",
    "content"
  ],
  "type": "object"
}
```
<!-- qcg-step-schemas:end -->

`fail`
: Fail intentionally with `content`.

## `on_fail`

Supported strategies:

- `{ action = "fail" }`
- `{ action = "route", to = "node_id" }`
- `{ action = "ask_user" }`
- `{ action = "regenerate", max_attempts = 2, on_exhausted = { action = "fail" } }`
- `{ action = "repair", repair = "node_id", recheck = "node_id",
  max_attempts = 2, on_exhausted = { action = "fail" } }`

Repair and regenerate exhaustion support `fail`, `route`, and a typed
`ask_user` action. `ask_user` accepts an optional `title` and the same `fields`
array as normal input stages; when fields are omitted qcg supplies one required
text field. Answers are durable and resume through the normal run boundary.

## Agent Tools

Every declaration has a unique non-empty `name`, a `kind`, and an optional
`description`. `llm.agent` accepts these closed tool variants:

- `fs.write`: `path_prefix`, plus an optional `input_schema`.
- `command`: a fixed `command`, plus an optional `input_schema`.
- `http`: fixed `methods` and `hosts`, plus an optional `input_schema`.
- `ask_user`: an optional `input_schema` for a runtime-generated form.
- `web.search`: an opt-in search profile selected from the unified
  `providers.toml` registry. Its model-visible input is always the closed
  `{query, limit?}` schema and cannot be replaced.
- `mcp`: a fixed tool binding into one generic `[[mcp_server]]` profile. Its
  model-visible input schema is discovered from the MCP server at run time.
- `agent`: a bounded specialist with `instructions`, an allowlist of sibling
  `tools`, invocation bound `max_calls`, `max_iterations`, `max_tokens_total`, required
  `max_tool_calls_total`, optional `input_schema` and package-relative
  `output_schema`, an optional `model`, typed `on_failure`, and an optional `handoff`. Specialist
  agents cannot delegate other agent tools. The output schema is applied to
  provider-native structured output when supported and is always validated
  locally. A normal result is returned to the parent as agent-as-tool data;
  `handoff = true` makes the specialist result the node's final output.
- `skill`: activates declared `skill` or `skill_library` resources by name.
  The tool description carries the `{name, description}` catalog, and the
  model-visible input is `{skill, file?}` where `skill` is an enum of the
  discovered names. Activation returns the skill instructions plus its
  bundled resource listing (paths only); `file` reads one skill-relative
  reference on demand. The tool is read-only, never grants permissions, and
  `max_calls` defaults to `4`.

`on_failure.default` and `on_failure.by_code` select `return_error` or `fail`.
The default is `return_error`, so a recoverable specialist failure becomes a
closed `{isError, agent, error: {code, message, retryable, call_number, limits}}`
tool result and the parent can
continue, retry, or choose another declared tool. The error also carries a
`retryable` flag and the effective specialist limits. Invalid specialist arguments,
specialist input guardrail rejection, and specialist execution failures use the same
policy-controlled result path. `max_calls` bounds
parent-driven retries independently; each invocation receives fresh iteration,
token, and tool-call budgets. `retryable` is true only while the failure is recoverable
and every relevant parent and specialist bound permits another invocation. Token, tool-call, iteration,
validation, provider, guardrail, and tool failures have distinct codes.
Run-wide budget exhaustion and cancellation always propagate; contracts cannot
override these execution boundaries.

`max_tool_calls_total` bounds all tool calls made by one `llm.agent` node and
defaults to `32` when omitted.

An `llm.agent` node may also declare `schema`, using the same package-relative
JSON Schema path as `llm.fill`. Invalid final responses consume another bounded
agent turn with explicit validation feedback; token-limit and refusal stop
reasons are failures rather than partial successes.

`guardrails` is an ordered array of named checks for `input`, `output`,
`tool_input`, or `tool_output`. A tool-stage check may select one declared
tool. qcg ships `regex_deny` (`params.pattern`), `json_schema`
(`params.schema`), and `command` (`params.command`). The `command` kind is the
external extension boundary: it sends the inspected value as JSON on stdin and
accepts a typed pass, violation, or error JSON object on stdout. Every
evaluation is journaled without the inspected value. A
violating guardrail with `tripwire = true` terminates the node immediately.
Executors return typed configuration/evaluation errors and typed violations.
`on_error = "fail"` propagates an executor error; `on_error = "block"` converts
it into a policy violation. Both paths emit structured events with stable code,
kind, message, policy, and violation details so clients can handle them without
parsing strings.

Node-level `model.provider` and `model.model` may be templates. They are
resolved from the durable run variables immediately before each request, then
the selected provider and its required capabilities are validated. This lets a
contract ask the operator to select an explicitly configured model without an
implicit fallback.

### Web search

`web.search` is implemented by the qcg agent harness and is independent of the
selected LLM provider. The model decides whether to call it, but cannot select
the provider's HTTP method, endpoint, headers, response mapping, or request
body. Search is enabled only when the contract declares this tool. The
selected `[[search_provider]]` row is explicit; there is no implicit search
profile or fallback. Selection is either the tool's `provider` or an explicit
`[default].search` entry in the registry; when neither exists, validation fails.

```toml
[[flow.params.tools]]
name = "search_web"
kind = "web.search"
description = "Search public documentation when current information is needed"
provider = "tinyfish-api"
max_results = 5
max_calls = 3

[permissions]
network = ["api.search.tinyfish.ai"]
```

The search-specific contract fields are `provider`, `max_results`, and
`max_calls`. `provider` names a `[[search_provider]]` row; it is required when
no explicit default is configured. `max_results` defaults to `5` and is
limited to `20`, and `max_calls` defaults to `3` and is limited to `10`. The
selected registry row owns the endpoint, query parameters, fixed headers, RFC
6901 result mapping, and authentication. Those transport fields are not valid
inline contract fields. The bundled API-key REST profile is named
`tinyfish-api`; it is separate from the OAuth MCP profile named `tinyfish`.
The built-in `exa-public` and `parallel-public` MCP profiles are anonymous
public research endpoints and remain registered even when no `providers.toml`
exists. A contract still binds exact MCP tools and grants the corresponding
hosts explicitly. Their ids are reserved and cannot be overridden by registry
rows.

The API key is read from the selected profile's `api_key_env` and injected into
its configured authentication header at run time. It is not a generator
secret and must not appear in `qcg.toml`, prompts, resources, URLs, query
parameters, or generated artifacts. A missing profile or credential fails
explicitly; qcg does not silently select another profile. Credentialed remote
profiles require HTTPS and do not follow redirects.

Results are reduced to `title`, `url`, and `snippet`, bounded by the declared
result limit, ranked in provider order, and marked with
`content_trust = "untrusted"`. Titles are limited to 512 characters and
snippets to 4096; oversized or incorrectly typed fields fail explicitly.
Search result URLs must be absolute HTTP(S) URLs without embedded credentials.
Search result URLs are citations, not an implicit permission to fetch those
pages; fetching page content requires a separately declared `http` tool and
network permission.

Each tool declaration must be a subset of `[permissions]`, and each tool call is
checked against the declared tool and the same runtime permission gateways as
deterministic steps.
Agent `http` calls do not follow redirects. Methods other than `GET` and `HEAD`
also pass through the declared side-effect confirmation policy.

### MCP tools

An MCP declaration binds one model-visible alias to one configured server and
one remote tool:

```toml
[[flow.params.tools]]
name = "lookup"
kind = "mcp"
description = "Look up a record when the request needs current data"
server = "my-tools"
tool = "lookup"
max_calls = 3
side_effects = false

[permissions]
network = ["mcp.example.com"]
side_effects = "none"
```

The required fields are `name`, `kind`, `server`, and `tool`. `server` must
match either a built-in public profile or an `[[mcp_server]]` row in the
selected `providers.toml`; `tool` is the remote MCP name and is never chosen by
the model. `max_calls` defaults to 3 and is limited to 10. `side_effects`
defaults to `true`; set it to `false` only for a known read-only operation. A
true value routes the call through the
contract's `[permissions].side_effects` policy: `none` denies it, `confirm`
and `dry_run_first` create the normal HITL boundary, and `allowed` permits it.
Each confirmation id binds the exact operation digest plus scope:
`content` scope uses `<node>:<kind>:<operation_digest>` (3 parts),
`invocation` scope (default) uses
`<node>:<kind>:<operation_digest>:<invocation_hash>` (4 parts, with
`invocation_hash` = hex SHA-256 over the invocation id), so approving target
A never authorizes a regenerated target B. Node-wide
bulk approvals do not exist: every approval authorizes exactly one
operation digest (plus one invocation under `invocation` scope).

For Streamable HTTP, every host in the profile's `allowed_hosts` must also be
listed in `permissions.network`. For stdio, the complete profile `command`
vector must be listed in `permissions.commands`. qcg opens a separate MCP
protocol session for each run, even though OAuth credentials and token refresh
state are shared by the process-level profile runtime.

Before the first model request, qcg connects to each declared MCP server and
discovers its `tools/list` schema. That input schema is authoritative for
argument validation; untrusted descriptions, titles, defaults, examples, and
comments are removed from the model-facing schema. If the server advertises an
`outputSchema`, qcg validates the returned `structuredContent` against it.
Internal JSON Schema references and composition keywords are supported;
external references are rejected instead of being resolved over the network
or from the filesystem.
MCP result text remains untrusted data and is scanned for declared secret
values before it is returned to the model. Discovery is limited to 100 pages,
each input and output schema is limited to 256 KiB, and the profile's
`max_response_bytes` limits protocol and metadata bodies.

MCP calls use the profile's timeout (120 seconds by default), honor the run
cancellation token, and close the protocol session when the run ends. A run
cancelled through the API therefore cancels in-flight MCP discovery or calls as
well as the LLM loop.

Each MCP profile explicitly selects the `initialize` lifecycle or the 2026-07-28
`server/discover` lifecycle. Custom profiles default to `discover`, while known
public profiles are pinned to their verified lifecycle. The client advertises the
Tasks extension and supports multi-round-trip
`input_required` tool results. MCP elicitation requests become runtime-generated
forms on the ordinary durable DAG pause/resume boundary; answers and the opaque
request state are returned to the original tool call. Unsupported input request
methods fail explicitly. Client sampling and roots are not exposed.
Side-effect journals contain only argument names and encoded size, never raw MCP
argument values.

## `[outputs]`

Declare primary artifacts on the producing flow node so the path has one source
of truth:

```toml
[[flow]]
id = "write_report"
type = "write"
artifact = { label = "Report", description = "Generated analysis report", preview = "text", required = true, mime = "text/markdown" }

[flow.params]
output_file = "report.md"
content = "..."
```

`artifact` metadata is valid only when the step has a statically identifiable
`output_file`, `target`, or `destination`. Use `[[outputs.extras]]` only for
additional files selected by a workspace-relative `glob`:

```toml
[[outputs.extras]]
glob = "reports/**/*.json"
label = "Report data"
required = false
```

Artifacts are collected from the run workspace and hashed in `outputs.json`.
`description` is displayed by clients. `preview` is `auto`, `text`, `image`,
`html`, `json`, `markdown`, `pdf`, `audio`, `video`, or `none`; it controls
browser presentation and must be compatible with the artifact MIME type.
Symlinks and paths that resolve outside the workspace are rejected.

## `[runtime]`, `[budget]`, `[failure]`, `[journal]`, `[assets]`

These sections are parsed as policy/display surfaces for generators. Unknown
fields are rejected when their structs define a closed schema.

`[runtime]`
:: Single source for command, HTTP, file, and template limits:
   `command_timeout_seconds`, `command_input_limit_bytes`,
   `command_output_limit_bytes`, `http_timeout_seconds`,
   `http_body_limit_bytes`, `http_redirect_limit`, `file_input_limit_bytes`,
   `file_count_limit`, `input_total_limit_bytes`, `output_file_limit_bytes`,
   `output_total_limit_bytes`,
   `output_artifact_limit`, `template_source_limit_bytes`,
   `template_context_limit_bytes`, `journal_event_limit_bytes`,
   `journal_total_limit_bytes`, `journal_event_count_limit`,
   `state_limit_bytes`, `template_output_limit_bytes`, and `template_fuel`.
   Template output is streamed through a bounded writer and each render gets a
   fresh fuel budget; exceeding either limit fails the render explicitly.
   Template source and serialized context are bounded before compilation and
   evaluation. `file_input_limit_bytes` bounds every individual input value
   (including `file`, `json`, and schema-backed custom fields), while
   `input_total_limit_bytes` bounds the sum of encoded input values. These
   limits apply to initial inputs and interactive form answers.
   There is no mechanistic size limit: every byte/count limit is optional and
   unbounded when omitted (`None`). Set an explicit max in `qcg.toml`
   `[runtime]` only when a bound is wanted; `http_redirect_limit = 0` follows
   no redirects. Timeouts, `template_fuel`, and budget counters keep
   greater-than-zero sanity checks but no hard ceiling.
   Confirmation plans display the command limits used by execution.

`[budget]`
:: Run-wide limits that survive suspend/resume rounds: `max_steps`,
   `max_tokens`, `max_cost_usd`, and `max_elapsed_seconds`. A cost limit
   requires input/output pricing on the declared `[llm].model` entry.
   Guarantee strengths differ: `max_elapsed_seconds` is a hard deadline
   enforced monotonically (a running node is stopped and the finalization
   re-checks it before settling success), while `max_tokens` and
   `max_cost_usd` are enforced at attempt entry (`StepContext::step_checkpoint`)
   plus per-tool-call checkpoints inside agent execution (never mid-token-stream,
   but checked on every tool call boundary as well as every attempt start) (E11).
   Plan estimates (`--plan --diff` forecast/`estimates` in `crates/qcg/src/cli/plan.rs`,
   FOREIGN) report these declared budgets read-only and enforce nothing themselves:
   they must be presented with the enforcement strengths above (elapsed is a hard
   deadline, tokens/cost are checkpoint-only), never as bounds the plan itself
   guarantees.

`[failure]`
:: Hierarchical policy with `default` plus `[failure.by_kind]` entries.
   `out_of_contract` is the only defined kind; every other engine failure is
   fatal by mechanism, so no policy entry exists for it. Supported actions are
   `reject`, `clarify`, `clamp`, and `fail`. A flow node may declare its own
   `failure` table to override the generator policy. `out_of_contract = true`
   LLM responses are journaled before the selected policy is applied.

`[retention].days`
:: Retention window in days used by `qcg runs gc` in addition to `--keep`.

`[audit]`
:: Observation-stream policy (ADR 0001). Durable records are never filtered;
   this section only controls the observation records that live in
   `audit.jsonl`. `level = "standard"` (default) persists every observation
   record; `level = "minimal"` persists none unless a class override raises
   it. `[audit.classes]` maps an observation kind to `full`, `digest`, or
   `off`; `digest` replaces every string in the payload with its content
   digest so the record parses while the content is not retained. Durable
   kinds are rejected as class keys. `max_bytes` and `max_events` bound the
   observation stream; a breach or write failure degrades audit persistence
   for the run and records a durable `audit_degraded` event instead of
   failing the run. A deployment audit floor can only raise the effective
   policy.

`[[hooks.run_started]]`, `[[hooks.run_succeeded]]`, `[[hooks.run_failed]]`, `[[hooks.step_failed]]`
:: Contract-declared lifecycle nodes (ADR 0001). Each entry is an inline
   step (`id`, `type`, optional `[hooks.<event>.retry]`, and the required
   `on_error = "fail" | "warn"`) executed in declaration order through the
   normal bounded step path: hooks charge the run budget, pass the same
   permission gates, journal `step_started`/`step_finished`, and replay
   exactly once across resumes. `run_started` hooks run after input
   materialization and before the scheduler; `run_succeeded` hooks run after
   the final checkpoint and before outputs are collected, so a hook may
   write a declared artifact; `step_failed` hooks run once during failed
   settlement before `run_failed` hooks, with the failure list published as
   the reserved `hook_failures` step variable; `run_failed` hooks run after
   them and their `fail` policy adds to the recorded failure list.
   Hooks must not suspend: `ask_user`, `await`, and `foreach` are rejected at
   contract load, and a runtime suspension is a hook failure under
   `on_error`. A hook that cannot run because the run budget is exhausted is
   recorded as a durable `hook_skipped` event with `reason = "budget"` and is
   not a failure: a hook never fails a successful run for lack of budget. A failure records a durable `hook_failed` event; `warn`
   continues the run, `fail` ends it with the hook error. Hook ids share the
   flow-node id namespace and must be unique.

`[resources.<name>]` type `run_ref`
:: Immutable, hash-pinned snapshot of another run's declared artifact. The
   selector is policy and must name exactly one strategy: `run_id`,
   `latest_success = "<generator-id>"`, or `latest_terminal =
   "<generator-id>"`. `artifact` is the declared output path in the source
   run's output manifest, `max_bytes` is the required explicit byte bound,
   and optional `require_sha256` pins the exact revision. Resolution happens
   once before execution: the bytes are verified against the source
   manifest, copied to the workspace at `run-refs/<name>/<file>`, and the
   resolution is persisted under the run metadata, so resume and replay
   never depend on the source run surviving retention. An unresolvable
   selector or a revision mismatch fails the run explicitly.

`[assets]`
:: Optional client assets declared by safe relative package paths. `files`
   lists regular files that must exist when the contract loads. `dirs` exposes
   declared subtrees whose files are resolved at request time, allowing an
   unbuilt derived UI directory to return 404 without invalidating the package.
   Files are served by `GET /api/generators/{id}/assets/{path}`; overlapping,
   nested, duplicate, and unsafe declarations are rejected, and canonical path
   containment prevents symlink escapes. `meta` is a free-form JSON object
   forwarded to clients without backend interpretation. File extensions and
   UI entry-point conventions are client responsibilities.

## Durability guarantees

The durability model targets process termination (including SIGKILL) and
restart. Host power loss and storage-media failure are outside the guaranteed
boundary; those would require synchronizing every external side effect behind
directory-entry durability.

Directory-entry durability means both the file bytes and the directory entry
that names them are durable: data reaches the disk and the parent directory
is fsynced so a crash cannot lose the rename that installed the file. qcg
applies this to run metadata (`state.json` atomic replace plus parent
directory sync in `persist_serialized_atomic`), workspace atomic
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
| `JournalWriter::event` fast path | `file.sync_data()` on the journal file plus parent directory sync | only `operation_started` / `operation_finished` and terminal kinds (operation-driven `needs_sync`) |
| `append_events_if` batch path | `file.sync_data()` on the journal file plus parent directory sync | only when the batch contains a terminal or operation event (`needs_sync`) |
| torn-tail repair | `sync_data()` after truncate or newline commit | every repair |
| `state.json` persist | atomic write + replace plus parent directory sync | every append |
| `.clean_shutdown` marker | plain write / remove, no fsync | best-effort only, failures propagate (a failed terminal marker fails the operation) |

Non-terminal appends are not individually fsynced; they rely on the next
terminal sync or clean shutdown. A crash-truncated tail without a trailing
newline is repaired on the next open under the journal lock. Repair marker
semantics (tamper vs crash): a terminal event writes `.clean_shutdown` next
to the journal; any later non-terminal append clears it. A truncated tail
WITH the marker refuses repair as possible tampering; WITHOUT the marker it
repairs as crash residue. See `docs/operations.md` for operator guidance.

Admission records vs `.admission-*.lock`: admission records are persistent
(idempotency mappings under `<runs-dir>/idempotency/` with 24-hour TTL plus
the durable `run_queued` journal event), while `.admission-*.lock` files are
stateless coordination only — small fixed-size files that are never unlinked
and hold no run state. Large operation results spill to a sidecar blob under
the run meta dir past 64 KiB (`OPERATION_RESULT_MAX_BYTES`); the journal
carries `result_ref` and the guard reloads the blob on resend. One shared
per-run journal poller serves all SSE subscribers (`journal_pollers`); live
snapshot `duration_ms` is quantized to whole seconds so exact-digest ETags
stay stable for conditional requests.

## Shutdown and restart semantics

Mirrors `docs/operations.md` shutdown section (normative operator text
lives there). On `SIGINT`/`SIGTERM` the server stops accepting new mutating
requests (they receive `503`); read requests (snapshots, events, artifacts)
stay admissible but their streams close as the drain proceeds, so only
mutating work is ever refused. SSE shutdown closes with an explicit
`shutdown` marker event so clients distinguish shutdown from truncation.
Startup order is resolve deployment policy once, build the service, build
the router, then start recovery before resident tasks; shutdown order is
signal, stop accepting mutating work, HTTP drain bounded by 30 s
(`DRAIN_TIMEOUT`), mark the service shutting down, then resident-task
shutdown and active-run settlement concurrently under a 150 second outer
deadline (`SHUTDOWN_DEADLINE` in `crates/qcg-server/src/server/serve.rs`,
returned as `Err` to the embedding host by `serve_with_listener_and_deadline`;
see `docs/operations.md` for the normative shutdown contract). In shared mode,
stopping one peer settles that peer's tracked runs only; other peers keep
their own runs. The outer deadline starts
after the HTTP drain completes: in-flight requests that drain promptly do not
consume the settlement budget, while a wedged drain connection is cut after
30 s so shutdown proceeds (the drain timeout is warned, never silent).

Shutdown settles a cancel race as `Interrupted` when shutdown is already in
effect, else `Canceled` (checked at settle time in `lifecycle.rs` and
`runs_api.rs`); settled runs journal `run_interrupted` and are terminal and
not auto-resumed by the next startup. Resume only applies to work that was
waiting on human input, was explicitly requeued, or was never tracked by the
stopping peer (a pre-existing adopted orphan with a durable `Queued` journal
admitted nowhere keeps its queue and resumes on the next boot). This tracked
vs durable-queue resume exception is stated normatively in
`docs/operations.md`; this mirror must match it. Explicitly-requeued work
(answered HITL suspensions re-entering the durable queue) is distinct from
preemption (a higher-priority arrival returning the lowest-priority running
run to `Queued` keeping its journal; equal priorities never preempt; already
finished steps replay on resume). Host error reporting: deadline overruns
and resident-task failures are returned to the embedding host instead of
being logged and ignored.
