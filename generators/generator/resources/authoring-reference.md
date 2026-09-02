# qcg authoring reference

Build a constrained generation harness, not a configuration-only generator.
The artifact may be source code, prose, media metadata, datasets, project
trees, archives, reports, or any other bounded output supported by declared
steps.

The package proposal is a complete manifest body, including the core-required
`generator` metadata (`id`, `version`, and `qcg_version`) and any optional
`name`, `description`, and `authors` metadata, plus a typed source map. Only
`permissions` and `secrets` are withheld for operator authority. Each
source value is an object with `encoding = "utf8"` or `encoding = "base64"` and
string `content`; UTF-8 entries contain text and base64 entries contain
canonical padded base64 for the exact binary bytes. An optional `unix_mode`
uses canonical octal text from `0600` through `0777` and never includes special
bits. The authoring flow materializes both encodings and removes its temporary
base64 staging file. Never use a plain source string, include `qcg.toml` in
the source map, or use an unsafe path.

The operator authority is separate from the proposal. The proposal must not
include `permissions` or `secrets`; the operator supplies the complete
`permissions` object and `secrets` map in the authority form. Its
least-privilege default grants no filesystem, network, command, or side-effect
capability; add only exact capabilities required by the generated contract.
Container images must be digest-pinned, container
runtime must be present only when containers are enabled, and each secret must
name exactly one `env` or `file_env` source.

Input fields may use all built-in kinds (`string`, `text`, `number`, `boolean`,
`select`, `multiselect`, `list`, `json`, `natural_language`, and `file`) or a
lowercase namespaced custom kind such as `acme.geo.point`. Custom kinds must
carry a JSON Schema and renderer-neutral `ui` metadata; clients without a
specialized widget still validate the schema and preserve the value.

Use current harness capabilities when they materially improve the requested
workflow:

- staged and conditional inputs, including file, JSON, natural-language, and
  localized fields;
- explicit DAG dependencies, parallel groups, bounded nested foreach blocks,
  adaptive ask_user forms, confirmation, suspend/resume, and durable agent
  checkpoints plus checkpoint fork/time travel with explicit state patches;
- schema-constrained LLM output, streaming, node-level and specialist request
  policy, model routing and explicit fallbacks, sampling, reasoning effort,
  response verbosity, stop policy, tool selection, multimodal media, trusted resource context,
  deterministic context limits, bounded specialist agents, agent-as-tool,
  typed specialist `on_failure` recovery, handoff, and typed input/output/tool guardrail tripwires, including permitted
  stdin/stdout JSON commands for specialized policies;
- anonymous public Exa and Parallel MCP research, authenticated MCP, REST
  search, HTTP, commands, logical validator backends, and container checks;
- exact network, filesystem, command, container, secret, and side-effect
  permissions with no implicit fallback;
- run-wide step, token, cost, and elapsed-time budgets; runtime byte and time
  limits; explicit failure policies; journals; artifacts with checksums; and
  repeated quality and trajectory evaluation suites, baseline regression
  comparison, hierarchical trace/span events, and OTLP trace export.

The built-in flow vocabulary is `write`, `render`, `copy`, `http`, `command`,
`transform`, `ask_user`, `check.schema`, `check.format`, `check.command`,
`check.tool`, `check.container`, `check.contract`, `fail`, `foreach`,
`llm.generate`, `llm.fill`, `llm.choose`, `llm.repair`, `llm.agent`, and the
direct `mcp.call` step. Their parameter schemas are the runtime contract:
unknown parameters must be rejected, and all paths, models, providers,
commands, MCP servers/tools, and resource kinds must be declared explicitly.
Resource kinds are closed to `file`, `dir`, `skill`, `url`, `openapi`, and
`exec`; use `exec` for an external process-backed resource.
Use `[llm]` only for generator-wide defaults and ceilings. Every LLM flow node
may declare a typed `request` object, and every agent-as-tool may refine it with
its own `model`, `fallback_models`, and `request`. The layer order is `[llm]`,
node, specialist. An empty specialist fallback list disables fallback. Never
place provider capability facts or endpoint details in invocation policy.
Specialists should normally return a typed error to their parent with
`on_failure = { default = "return_error" }`; use `by_code` to make a specific
recoverable failure fatal. Token, tool-call, and iteration exhaustion are
distinct codes. Run-wide budget exhaustion and cancellation always propagate
and cannot be converted into tool results. Invalid specialist arguments and
specialist input guardrail rejection use the same policy-controlled path. Set the
specialist tool's `max_calls` independently from its per-invocation `max_iterations`,
`max_tokens_total`, and `max_tool_calls_total` so the parent has an explicit,
bounded retry budget.
Use the typed `request.clear` list only when an inner layer must omit inherited
sampling, reasoning, stop, tool-selection, or verbosity controls; it cannot
remove system policy, safety ceilings, or required capabilities.
For `http`, use one mutually exclusive body mode (`body_text`, `body_json`,
`body_base64`, or `body_file` with `body_file_scope`) and choose an explicit
`output` mode (`text`, `json`, `base64`, or `file`; file output requires
`output_file`). For `command`, use `input` or `input_file` (not both), and
choose `result = "process"` or `"structured"`; structured output is the
closed `{status, output, files, findings}` object and may be checked with
`output_schema`.

Prefer the smallest sufficient permission set and closed schemas. Put provider
endpoints and credentials in the external provider registry, never in a
generated package. Add requested validation to the DAG rather than describing
it only in prose. Reject unsupported or unsafe behavior explicitly.
