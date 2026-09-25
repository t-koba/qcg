# Provider Guide

qcg resolves LLM, web-search, and MCP connection profiles from one external
registry file, `providers.toml`. The built-in profiles are the `fake` LLM
provider and anonymous public `exa-public` / `parallel-public` MCP servers.
Other provider names and transport profiles live in the registry and
documentation rather than qcg's code. Contracts reference LLM rows by `id`,
REST search rows by `provider`, and MCP servers by `server`.

Web search is a harness capability and is opt-in per contract. Declaring a
`kind = "web.search"` tool enables one registry-defined REST mapping; declaring
`kind = "mcp"` binds an arbitrary MCP server tool. Merely having a profile
never performs a request. MCP tool schemas are discovered from the selected
server at run time.

## Registry resolution

The registry file is resolved once per process, in this order:

1. `--providers <PATH>` (CLI flag; authoritative, no fallback)
2. `QCG_PROVIDERS` environment variable
3. `./providers.toml` relative to the working directory
4. `<install prefix>/share/qcg/providers.toml` next to the binary

If no file is found, the built-in `fake` LLM provider and anonymous public
`exa-public` / `parallel-public` MCP servers remain available; referencing any
other provider fails with registry setup guidance. Release
archives bundle the default registry at
`share/qcg/providers.toml`; the source tree keeps it at the workspace root.

## Registry schema

```toml
# Defaults are used only after the corresponding capability is explicitly
# declared by a contract.
[default]
model = { provider = "openai", model = "gpt-4.1-mini" }
search = "tinyfish-api"

[[provider]]
id = "openai"
api = "chat_completions"
base_url = "https://api.openai.com/v1"
base_url_env = "QCG_OPENAI_BASE_URL"
api_key_env = "QCG_OPENAI_API_KEY"
chat_token_limit_field = "max_completion_tokens"
max_concurrency = 16
requests_per_minute = 500
circuit_breaker_failures = 5
circuit_breaker_cooldown_seconds = 30
capabilities = { tool_use = true, json_schema = true, seed = true, image_input = true, file_input = true, streaming = true, tool_choice = true, parallel_tool_calls = true, reasoning_effort = ["none", "minimal", "low", "medium", "high", "xhigh", "max"] }

[[search_provider]]
id = "tinyfish-api"
endpoint = "https://api.search.tinyfish.ai"
query_param = "query"
results_pointer = "/results"
title_pointer = "/title"
url_pointer = "/url"
snippet_pointer = "/snippet"
api_key_env = "TINYFISH_API_KEY"
auth_header = "X-API-Key"

[[mcp_server]]
id = "tinyfish"
transport = "streamable_http"
lifecycle = "initialize"
url = "https://agent.tinyfish.ai/mcp"
auth = "oauth"
oauth_store = "keyring"
allowed_hosts = ["agent.tinyfish.ai", "clerk.tinyfish.ai"]
```

Fields:

- `id`: unique identifier referenced by contracts.
- `api`: request protocol: `chat_completions`, `responses`,
  `anthropic_messages`, or `system_one`. The value selects the payload format
  and endpoint path (`chat/completions`, `responses`, `messages`, or
  `systemone`). System One uses typed decisions through `llm.decide`, not chat;
  its provider rows must not advertise chat capabilities.
- `base_url`: literal endpoint root. `{ENV_VAR}` placeholders are resolved
  from the environment before use. The configured `api_key_env` placeholder,
  other credential-like placeholders, URL userinfo, queries, and fragments are
  rejected.
- `base_url_env`: environment variable that overrides `base_url`.
- `api_key_env`: environment variable whose value is sent as the credential.
  Omit it for endpoints that need no authentication (for example Ollama).
- `api_key_file_env`: alternative credential source for Vault Agent or mounted
  secret rotation. Its environment variable must contain an absolute path to a
  private, non-symlink UTF-8 file no larger than 64 KiB. Exactly one of this
  field and `api_key_env` may be set; the file is read for every request.
- `auth_header`: header name carrying the credential. Omit to use standard
  Bearer authorization.
- `path_template`: optional request path where `{model}` expands to the model
  name from the contract.
- `query`: optional query parameters appended to the URL; values may contain
  non-credential `{ENV_VAR}` placeholders. The configured `api_key_env`,
  credential-like query names, and credential placeholders are rejected.
- `timeout_seconds`: timeout for each completion attempt. The default is 120
  seconds; retryable failures may start another independently timed attempt.
- `retry_attempts`: attempts per completion call including the first, 1 to 10.
  The default is 3. Only retryable failures (timeouts, 5xx, 429, empty
  responses) retry; other errors fail fast.
- `retry_base_backoff_ms`: base wait between retries in milliseconds, up to
  60000. The default is 200; waits grow exponentially from there.
- `retry_rate_limit_floor_ms`: minimum wait per retry attempt for rate-limited
  (429) and empty-body responses, multiplied by the attempt number; 0 to 60000.
  The default is 5000; `0` disables the floor. Other retryable failures only
  use the exponential backoff.
- `retry_backoff_exponent_cap`: largest exponent applied to the exponential
  backoff, 1 to 16. The default is 8; a higher cap grows waits further per
  attempt before they level off.
- `response_body_limit_bytes`: maximum response body size. The default is
  16 MiB; larger responses fail before JSON parsing instead of growing memory
  without a bound. An explicit value has no mechanistic ceiling.
- `max_concurrency` and `requests_per_minute`: optional provider-local admission
  limits. Requests wait for capacity instead of creating an unbounded burst.
- `circuit_breaker_failures` and `circuit_breaker_cooldown_seconds`: consecutive
  retryable-failure threshold and open duration. An open primary route may use
  only a fallback explicitly declared by the generator.
- `chat_token_limit_field`: Chat Completions output-limit field. It defaults
  to `max_tokens`; use `max_completion_tokens` for current OpenAI and Azure
  reasoning endpoints. It is rejected for other API flavors.
- `stream_retry_attempts`: retry attempts for a stream that fails before
  delivering any event (default `0`, range `0..=3`). A retry is refused once
  any delta was delivered, so two model responses can never interleave;
  `0` surfaces the provider error immediately.
- `prompt_cache_field`: prompt-cache mechanism for this row. `prompt_cache_key`
  is valid for `chat_completions` and `responses`; `cache_control` is valid for
  `anthropic_messages`. It is required exactly when `capabilities.prompt_cache`
  is enabled and rejected otherwise, so a contract `[llm].cache = "auto"`
  never silently degrades. Rows without prompt caching stay unchanged.
- `capabilities`: advertised support for `tool_use`, `json_schema`,
  `structured_output_with_tools`, `seed`, `image_input`, `audio_input`,
  `file_input`, `streaming`, `temperature`, `top_p`, `stop_sequences`,
  `tool_choice`, `parallel_tool_calls`, `prompt_cache`, and `verbosity`, plus the exact
  `reasoning_effort` values
  accepted by the model or deployment used through this row. Set
  `structured_output_with_tools` only when the endpoint can combine native
  schema output with external tool calls in one request. Missing capabilities
  default to disabled. Streaming consumes
  provider SSE incrementally and journals bounded `llm_delta` events. Once a
  delta is emitted, route fallback is refused to prevent mixed-model output.
  `verbosity` is currently valid only for Responses rows. Capability flags
  describe transport mechanisms; generator and node policy never belongs in
  this registry.

REST search profiles use the same registry and loading rules as LLM rows:

- `id`: unique profile identifier referenced by a `web.search` tool.
- `endpoint`: fixed HTTP(S) endpoint. It cannot contain credentials, query
  parameters, fragments, or unresolved credential placeholders. Use
  `endpoint_env` when the endpoint itself must come from an environment
  variable.
- `endpoint_env`: optional environment variable overriding `endpoint`; a
  missing variable is an explicit configuration error when no literal endpoint
  is present.
- `method`: `get` (the default) or `post`. GET profiles put the model query
  and optional limit in the URL; POST profiles put them in a JSON body.
- `headers`: optional fixed request headers. Authentication headers are
  configured separately and cannot be overridden by this map.
- `query`: optional fixed query parameters appended to the endpoint URL. They
  are profile data, never a place for a credential.
- `body`: optional fixed JSON fields for POST profiles. `query_is_array` makes
  the model query a one-element JSON array for POST APIs that require it.
- `query_param`: fixed name receiving the model's bounded query. It defaults to
  `q`.
- `limit_param`: optional fixed name receiving the requested result limit. If
  omitted, qcg truncates the normalized result list locally.
- `results_pointer`, `title_pointer`, `url_pointer`, and optional
  `snippet_pointer`: fixed RFC 6901 JSON Pointers used to normalize responses.
  `results_pointer` is required; title and URL pointers default to `/title` and
  `/url`.
- `api_key_env`: optional environment variable whose value is injected only in
  the configured authentication location.
- `auth_header` or `auth_query_param`: exactly one fixed authentication
  location when `api_key_env` is present. `auth_prefix` is allowed for header auth only.
  Query authentication is kept as a sensitive gateway parameter and is not
  visible in the contract or model tool schema.

Search rows are validated before use. Their hosts do not grant network access
to a generator: every contract that opts into REST search must still list the
profile host in `permissions.network`.

Unknown fields are rejected. A row without a usable URL, an unset credential
variable, or an unresolved `{ENV_VAR}` placeholder fails validation before the
corresponding capability runs with the message `set <VAR> before running the
generator`.

## Model catalog

Provider rows may declare models so operators and clients can select a
provider, model, and reasoning effort without hardcoding them in every
contract. Declarations are metadata: they never grant network, command, or
side-effect permission.

```toml
[[provider]]
id = "openai"
api = "chat_completions"
base_url = "https://api.openai.com/v1"
api_key_env = "QCG_OPENAI_API_KEY"
chat_token_limit_field = "max_completion_tokens"
catalog_id = "openai"            # optional external-catalog provider key
capabilities = { tool_use = true, json_schema = true }

[[provider.models]]
id = "gpt-5.2"
label = "GPT-5.2"
reasoning_effort = ["none", "low", "medium", "high", "xhigh"]
input_cost_per_million_usd = 1.75
output_cost_per_million_usd = 14.0
context_tokens = 400000
max_output_tokens = 128000

[[provider.models]]
id = "legacy-model"
enabled = false                  # hidden from selection, still usable when pinned
```

Fields:

- `id` / `label`: catalog identity and display name.
- `capabilities`: optional full per-model override of the provider row. It is
  validated against the same API-flavor rules as a provider row.
- `reasoning_effort`: shorthand that overrides only the provider's advertised
  effort list, without restating every capability.
- `input_cost_per_million_usd` / `output_cost_per_million_usd`: unit prices
  used by `budget.max_cost_usd` when the contract itself declares no price.
- `context_tokens` / `max_output_tokens`: selection metadata.
- `enabled`: `false` hides the model from `qcg models` and the catalog API.

A provider that declares no `models` list keeps the generic behavior: any
model string is accepted and the provider-level capabilities apply. Effective
capabilities resolve as provider row, then model `capabilities` (full
replace), then model `reasoning_effort` (list override).

### Discovery

`models_discovery = "openai"` enables live `GET {base_url}/models` discovery
with the provider's configured credentials. Discovered ids appear in the
catalog for selection and inherit provider capabilities; they never change
run validation and never grant permission. Discovery failures are reported
per provider in the catalog view instead of silently emptying it.

### External catalog

An optional `[catalog]` section fetches model metadata from an external
source. The `models_dev` kind accepts a models.dev `api.json` URL or a
vendored file and maps `reasoning_options[type=effort]`, tool/structured
output/attachment capabilities, context and output limits, and unit prices.
Explicit `[[provider.models]]` entries win field by field.

```toml
[catalog]
cache = "~/.qcg/cache/llm-catalog.json"   # default under $QCG_HOME
refresh_seconds = 86400

[[catalog.source]]
kind = "models_dev"
url = "https://models.dev/api.json"
sha256 = "..."                            # optional pin
```

The cache is loaded at process start so validation stays deterministic
offline. `qcg serve` refreshes stale sources in the background (failures are
logged and retried, never fatal), and `qcg models --refresh` or
`GET /api/llm/catalog?refresh=true` re-fetch on demand. The view marks stale
metadata and reports fetch errors.

`GET /api/llm/catalog` and `qcg models` list providers, models, efforts,
prices, and limits. Credentials and environment-variable values are never
returned.

## MCP server profiles

An `[[mcp_server]]` row describes one generic MCP endpoint. The registry owns
transport, authentication, and process configuration; a generator contract
only binds a model-visible alias to a fixed `server` and remote `tool` name.
The same runtime supports unrelated MCP servers and tools without adding
provider-specific code to qcg.

### Streamable HTTP

```toml
[[mcp_server]]
id = "tinyfish"
transport = "streamable_http"
lifecycle = "initialize"
url = "https://agent.tinyfish.ai/mcp"
auth = "oauth"
oauth_store = "keyring"
allowed_hosts = ["agent.tinyfish.ai", "clerk.tinyfish.ai"]
timeout_seconds = 120
max_response_bytes = 4194304
tools_list_page_limit = 100
oauth_state_ttl_seconds = 600
task_poll_interval_ms = 250
```

`transport` defaults to `streamable_http`. The URL must be an HTTP(S) URL
without userinfo, query, or fragment; a non-loopback remote URL must use
HTTPS. `allowed_hosts` must include the endpoint host and every host needed by
OAuth discovery, authorization, token, or registration requests. It is also
the redirect allowlist for the OAuth HTTP client. The contract must list every
one of these hosts in `[permissions].network`; the registry never grants that
permission by itself.

`lifecycle` selects MCP session negotiation explicitly: `initialize` uses the
widely deployed initialize handshake, while `discover` requires the modern
`server/discover` lifecycle and is the default for custom profiles. Use
`initialize` for a server that documents only the initialize handshake. qcg
never silently retries a different lifecycle. The built-in Exa and
Parallel profiles are pinned to `initialize` and are exercised by live
contract tests.

`headers` may contain fixed non-sensitive headers. Credential headers are
rejected there and must use the authentication fields below. Remote requests
do not follow redirects to unlisted hosts.

### Stdio

```toml
[[mcp_server]]
id = "local-tools"
transport = "stdio"
command = ["my-mcp-server", "--stdio"]
env = { MCP_MODE = "readonly" }
env_from = { MCP_TOKEN = "QCG_MCP_TOKEN" }
auth = "none"
```

`stdio` requires a non-empty command and cannot declare `url`, `headers`, or
`allowed_hosts`. The child process receives a cleared environment containing
only `PATH`, the non-sensitive `env` entries, and `env_from` values copied from
the qcg process. Sensitive environment names must use `env_from`; provider
credentials are never inherited implicitly. The complete command vector must
also be present in the contract's `[permissions].commands`. Child stderr is
discarded so mapped credential values cannot be copied into qcg logs. Process
control variables such as `LD_PRELOAD`, `DYLD_INSERT_LIBRARIES`, `NODE_OPTIONS`,
and language runtime search paths cannot be supplied by either environment
field. On Unix the server starts in a dedicated process group, so cancellation
and bounded shutdown also terminate descendants.

### Authentication and credentials

`auth` is one of `none`, `bearer`, `header`, or `oauth`:

- `none` sends no configured credential.
- `bearer` reads `credential_env` and sends a Bearer `Authorization` header.
- `header` reads `credential_env` and sends it in `auth_header`, optionally
  prefixed by `auth_prefix`.
- `oauth` performs MCP OAuth discovery and authorization. Optional
  `oauth_scopes` requests explicit scopes; `oauth_client_id_env` and
  `oauth_client_secret_env` select a pre-registered client when the server
  requires one. Omitting them lets the MCP authorization flow use discovery and
  dynamic registration where supported.

`oauth_store` defaults to `keyring`. `keyring` stores OAuth credentials in the
operating-system credential store under a profile-specific account. `memory`
is an explicit process-local option for isolated ephemeral use and is lost on
restart. Neither mode writes access tokens or refresh tokens to the registry,
generator packages, prompts, journals, or artifacts. Values for `credential_env`
and `env_from` are read only when a declared MCP server is used.

For OAuth profiles, authorize from the bundled SPA's Connections panel while
qcg is bound to loopback. qcg exposes `GET /api/mcp/servers`,
`POST /api/mcp/servers/{id}/authorization`,
`DELETE /api/mcp/servers/{id}/authorization/pending`,
`DELETE /api/mcp/servers/{id}/authorization`, and the callback
`GET /api/mcp/oauth/callback`. The authorization URL is returned only after
its host has passed the profile allowlist. Callback state is single-use and
expires after `oauth_state_ttl_seconds`. TinyFish's bundled `tinyfish` profile
uses this OAuth flow, does not use `TINYFISH_API_KEY`, and requires one-time
authorization from the loopback Connections panel.

`timeout_seconds` defaults to `120` and applies to connection and MCP
operations. `max_response_bytes` defaults to `4194304` and bounds MCP response
and OAuth metadata bodies. `tools_list_page_limit` defaults to `100` and caps
the `tools/list` pages fetched per connection (1 to 1000).
`oauth_state_ttl_seconds` defaults to `600` and bounds the lifetime of a
pending OAuth authorization state (60 to 3600). `task_poll_interval_ms`
defaults to `250` and is the poll cadence for asynchronous MCP tasks when the
server does not suggest one (50 to 5000); server suggestions are clamped into
the same hard range. These ranges are mechanism: an out-of-range configuration
fails registry validation instead of being silently adjusted. The runtime also
rejects an individual discovered input or output schema larger than 256 KiB.
Schema depth, node count, object width, and string length are bounded before
compilation. Reserved MCP transport headers and credential-like static header
names are rejected during registry validation.

Each profile chooses the `initialize` lifecycle or the 2026-07-28
`server/discover` lifecycle. Known public profiles are pinned to the lifecycle
verified against the real service. The client advertises
the Tasks extension and handles multi-round-trip `input_required`
results. Form elicitation requests are projected onto qcg's durable HITL form,
and the accepted values plus opaque request state resume the original tool call.
Unsupported input request methods fail explicitly. Client sampling and roots are
not exposed.

## Declaring the model

Contracts declare one model explicitly:

```toml
[llm]
model = { provider = "openai", model = "gpt-4.1-mini" }
system = "You generate concise configuration patches."
retry_prompt = "The previous JSON failed validation: {{ error }}. Try again."
max_tokens = 4096
seed = 42
requires = ["json_schema"]
```

When `[llm].model` is omitted, the `[default].model` row from the registry is
used. A run fails when neither is declared; there is no hidden fallback.

`[llm]` supplies generator-wide defaults and ceilings. Put invocation choices
on the node instead of cloning provider rows or baking workflow policy into the
transport registry:

```toml
[[flow]]
id = "plan"
type = "llm.generate"

[flow.params]
prompt = "prompts/plan.j2"

[flow.params.request]
reasoning_effort = "high"
max_tokens = 2048
verbosity = "low"
stream = true
```

The same typed `request` object is accepted by chat LLM nodes and by an
agent-as-tool declaration. `llm.decide` uses its separate typed decision params
and does not accept `request`. Settings layer as `[llm]`, node `request`, then
specialist `request`. Node and specialist resource limits may only tighten the
global ceilings. An empty specialist `fallback_models` list explicitly means
no fallback; it never borrows the parent node's routes. `request.clear` can
explicitly omit inherited optional sampling, reasoning, stop, tool-selection,
or verbosity controls. It cannot remove system policy, required capabilities,
or resource ceilings.

`requires` is checked against the selected provider's capabilities before a
run starts. If the provider does not advertise a required capability such as
`tool_use`, `json_schema`, `structured_output_with_tools`, `seed`, or
`reasoning_effort`, qcg fails contract
validation instead of trying a hidden fallback. A declared seed or reasoning
effort automatically requires the matching capability; unsupported values are
never silently discarded.

When a schema-bound agent also has tools, `structured_output = "auto"` uses
native schema mode only if the provider advertises both `json_schema` and
`structured_output_with_tools`. Otherwise it uses schema instructions plus the
same mandatory local validation and bounded correction loop. Selecting an
explicit native mode for an unsupported combination fails before transport.

### Budget and fallback policy guidance

Mechanism enforces, policy chooses. qcg enforces run-wide `[budget]`
(`max_steps`, `max_tokens`, `max_cost_usd`, `max_elapsed_seconds`), static
ordered `fallback_models` (at most 8, retryable route failures only), and the
streaming rule that route fallback is refused once the first delta is emitted
(partial billing is then unavoidable). Unpriced routes account as zero and are
flagged, since totals may understate spend.

Policy guidance for authors:

- Set `[budget]` on every server-hosted generator; treat provisioned answers
  and approvals as deployment policy, not code defaults.
- Keep `max_tokens` as the output ceiling (hidden reasoning tokens included);
  there is no separate reasoning budget. Tighten per node via
  `params.request.max_tokens` (tighten-only, never raise).
- Order `fallback_models` by cost and capability explicitly; an empty list
  disables fallback instead of inheriting. `llm.decide` and `llm.catalog`
  never fall back.
- Prefer small `max_iterations` and `max_tool_calls_total` on specialists;
  parent and specialist budgets are independent and additive.

### Reasoning models

Declare reasoning explicitly when the selected model supports it:

```toml
[llm]
model = { provider = "azure-openai", model = "my-gpt-deployment" }
max_tokens = 16384
reasoning_effort = "high"
```

The allowed values are `none`, `minimal`, `low`, `medium`, `high`, `xhigh`,
and `max`. They are not universally supported: keep each registry row's
`capabilities.reasoning_effort` list aligned with the actual model or Azure
deployment behind that row. qcg rejects a value outside that list before the
request. `temperature` and `seed` must be omitted whenever reasoning effort is
set.

For Chat Completions qcg sends top-level `reasoning_effort`; for Responses it
sends `reasoning.effort`. Chat rows that advertise reasoning must select
`chat_token_limit_field = "max_completion_tokens"`. Responses uses
`max_output_tokens`. During tool calls, Responses output items, including
encrypted reasoning state, are carried into the next stateless request with
their call IDs; Chat and Anthropic continuations likewise retain their native
tool-call IDs.

Reasoning and tool support can vary by model even when the endpoint accepts
both features. Use separate registry rows when models or deployments expose
different capability sets, and prefer the Responses API for current OpenAI
reasoning models with tools. The upstream service remains authoritative for
model-specific combinations.

## System One decisions

`llm.decide` sends `model`, `state`, and a map of typed `questions` to the
System One endpoint, `POST /v1/systemone`. It requires an explicit
`params.model` selecting a `system_one` provider. It is independent of `[llm]`:
no `[llm]` or registry default model, chat controls, or chat output ceiling is
inherited, and there is no model or provider fallback. Run-wide budgets still
apply. Chat parameters (including `prompt`, `request`, tools, and streaming),
templates, and node `context` are rejected rather than ignored. A System One
profile cannot serve chat requests.

This flow fragment uses the active `typesafe` profile:

```toml
[[flow]]
id = "classify"
type = "llm.decide"

[flow.params]
model = { provider = "typesafe", model = "jev-1.13.0", input_cost_per_million_usd = 0.042, output_cost_per_million_usd = 0.0 }
state = "Help! My payouts have been failing for three days."
max_tokens = 4096

[flow.params.questions.department]
type = "choice"
instructions = "Which team should review this request?"
criteria = { billing = "Payments, invoicing, refunds", technical = "Bugs, outages, integrations", sales = "Pricing, upgrades, new accounts" }

[flow.params.questions.urgent]
type = "noul"
instructions = "Does this request convey urgency?"

[flow.params.questions.frustration]
type = "score"
instructions = "How frustrated is the customer?"
criteria = ["Calm", "Frustrated", "Very angry"]
```

Specify exactly one of literal `state` or `state_from`. The example uses a
string; state also accepts an object or array. For runtime input, replace the
`state` line with `state_from = "inputs.name"` or a dotted ValueBag path such as
`state_from = "steps.node.output.message"` (and declare that node in `needs`).
The path resolves at execution without template rendering; missing paths or
values other than strings, objects, or arrays fail explicitly.

`questions` is a map keyed by question ID, represented by a Rust `BTreeMap`,
not a list or chat schema. Its typed variants are:

- Noul (`type = "noul"`): yes/no probability, with optional `criteria` containing
  string descriptions under `true` and/or `false`.
- Choice (`type = "choice"`): a `criteria` map of 2–255 option names to string
  descriptions or null (the Rust representation is a `BTreeMap`). TOML has no
  null literal, so the example uses descriptions.
- Score (`type = "score"`): an ordered `criteria` array of 2–255 level
  descriptions.

All question types require `instructions`, which accepts a string, object, or
array. The step returns the full typed response under `steps.classify.output`:
`model`, `answers` keyed by the original question IDs, and
`usage.input_tokens` / `usage.output_tokens`. Choice answers preserve `choice`,
`probabilities`, and `confidence`; Score preserves `score`, `legend`,
`probabilities`, and `confidence`. Noul returns `noul` and has no `confidence`
field. Each answer also preserves its `type`.

For a downstream review node with `needs = ["classify"]`, a branch expression
can inspect both the department choice and confidence:

```toml
when = "steps.classify.output.answers.department.confidence >= 0.8 && steps.classify.output.answers.department.choice == 'billing'"
```

This only selects a workflow branch. Confidence must not automatically grant
authorization or trigger side effects; existing human approval and permission
policy remains in force, regardless of the score.

`max_tokens = 4096` is a required positive local ceiling on total input plus
output usage, not a chat output limit, and is not sent upstream. The upstream
System One API has no token-cap parameter. Local reservation and post-hoc
accounting cannot guarantee that the provider never exceeds this ceiling;
retry attempts can also incur usage that is not reported in the final response.
Do not treat the local ceiling as an upstream spending guarantee. Explicit
input and output prices are required when `budget.max_cost_usd` is set.

The example declares USD 0.042 per million input tokens and USD 0 per million
output tokens, as listed in the official [Models](https://docs.typesafe.ai/models)
reference. Pin a version when thresholds depend on its behavior; `jev-latest`
is a moving alias. See the official [API reference](https://docs.typesafe.ai/api)
for request and response details. No live API validation was performed for
this integration; documentation review is not an end-to-end provider test.

## Bundled providers

The shipped registry follows an opt-in execution model. Local LLM endpoints
and the remote `typesafe` System One profile are active configuration rows;
other remote LLM providers ship as commented templates. Unlike those templates,
`typesafe` needs no uncommenting and is remote, not a local endpoint. Like the
local rows, it only contacts its endpoint when execution reaches a request
using that profile; loading the registry does not contact it.
Ollama and LM Studio are credential-free; `openai_client` requires its declared
environment variable. `typesafe` requires `TYPESAFE_API_KEY` and permits the
explicit `QCG_TYPESAFE_BASE_URL` override. MCP servers and REST search profiles
are opt-in: they are contacted only after a contract declares the corresponding
tool and grants the required permission.

Active by default: `fake` (built in), `ollama`, `lmstudio`, `openai_client`,
`typesafe` (remote System One; no chat capabilities).

Bundled MCP profile: `tinyfish` (Streamable HTTP + OAuth; no
`TINYFISH_API_KEY`; one-time loopback Connections-panel authorization; OAuth
credentials use the OS keyring).

Commented templates: `anthropic`, `openai`, `openai_responses`, `openrouter`,
`gemini`, `sakura`, `cloudflare`, `opencode-go`, `opencode-zen`,
`opencode-go-responses`, `opencode-zen-responses`, `groq`, `deepseek`,
`mistral`, `xai`, `together`, `fireworks`, `azure-openai`.

| Provider id | API | API key environment variable | Other required environment variables |
|---|---|---|---|
| `anthropic` | `anthropic_messages` | `QCG_ANTHROPIC_API_KEY` | none |
| `openai` | `chat_completions` | `QCG_OPENAI_API_KEY` | none |
| `openai_responses` | `responses` | `QCG_OPENAI_API_KEY` | none |
| `openai_client` (active) | `chat_completions` | `QCG_OPENAI_CLIENT_API_KEY` | none |
| `ollama` (active) | `chat_completions` | none | none |
| `lmstudio` (active) | `chat_completions` | none | none |
| `typesafe` (active, remote) | `system_one` | `TYPESAFE_API_KEY` | none |
| `openrouter` | `chat_completions` | `QCG_OPENROUTER_API_KEY` | none |
| `gemini` | `chat_completions` | `QCG_GEMINI_API_KEY` | none |
| `sakura` | `chat_completions` | `QCG_SAKURA_API_KEY` | none |
| `cloudflare` | `chat_completions` | `QCG_CLOUDFLARE_API_KEY` | `QCG_CLOUDFLARE_ACCOUNT_ID` |
| `opencode-go` | `chat_completions` | `QCG_OPENCODE_API_KEY` | none |
| `opencode-zen` | `chat_completions` | `QCG_OPENCODE_API_KEY` | none |
| `opencode-go-responses` | `responses` | `QCG_OPENCODE_API_KEY` | none |
| `opencode-zen-responses` | `responses` | `QCG_OPENCODE_API_KEY` | none |
| `groq` | `chat_completions` | `QCG_GROQ_API_KEY` | none |
| `deepseek` | `chat_completions` | `QCG_DEEPSEEK_API_KEY` | none |
| `mistral` | `chat_completions` | `QCG_MISTRAL_API_KEY` | none |
| `xai` | `chat_completions` | `QCG_XAI_API_KEY` | none |
| `together` | `chat_completions` | `QCG_TOGETHER_API_KEY` | none |
| `fireworks` | `chat_completions` | `QCG_FIREWORKS_API_KEY` | none |
| `azure-openai` | `responses` | `QCG_AZURE_OPENAI_API_KEY` | `QCG_AZURE_OPENAI_BASE_URL` |

| REST search profile | Method and endpoint | API key environment variable |
|---|---|---|
| `tinyfish-api` | GET `https://api.search.tinyfish.ai` | `TINYFISH_API_KEY` |
| `parallel-fast` | POST `https://api.parallel.ai/v1/search`, fast mode | `PARALLEL_API_KEY` |
| `parallel-advanced` | POST `https://api.parallel.ai/v1/search`, advanced mode | `PARALLEL_API_KEY` |
| `exa` | POST `https://api.exa.ai/search` | `EXA_API_KEY` |
| `firecrawl` | POST `https://api.firecrawl.dev/v2/search` | `FIRECRAWL_API_KEY` |
| `tavily` | POST `https://api.tavily.com/search`, basic depth | `TAVILY_API_KEY` |
| `brave` | GET `https://api.search.brave.com/res/v1/web/search` | `BRAVE_SEARCH_API_KEY` |
| `serper` | POST `https://google.serper.dev/search` | `SERPER_API_KEY` |
| `serpapi` | GET `https://serpapi.com/search.json` | `SERPAPI_API_KEY` |

| MCP profile | Endpoint | Authentication |
|---|---|---|
| `exa-public` | `https://mcp.exa.ai/mcp` | none |
| `parallel-public` | `https://search.parallel.ai/mcp` | none |
| `tinyfish` | `https://agent.tinyfish.ai/mcp` | OAuth |

Notes:

- The built-in `fake` provider and the anonymous `exa-public` and
  `parallel-public` MCP profiles work even when no providers registry file
  exists. The public MCP profiles still require exact contract tool bindings
  and network permissions. Their ids are reserved and registry rows cannot
  override their pinned endpoints or security policy.
- Referencing an unregistered LLM provider, REST search profile, or MCP server
  fails contract validation with "is not registered" plus registry setup
  guidance. qcg never silently selects another provider, search profile, or
  MCP server.
- A remote row accepts a base-URL override only when it declares
  `base_url_env`; qcg uses that exact environment-variable name and has no
  automatic `QCG_<ID>_BASE_URL` naming rule. See `providers.toml` for each
  profile's declaration.
- Azure OpenAI uses deployment names as the model value. The recommended
  `azure-openai` row uses the current Responses API and expects
  `QCG_AZURE_OPENAI_BASE_URL` to include `/openai/v1`. It does not use an
  `api-version` query.
  Cloudflare resolves `{QCG_CLOUDFLARE_ACCOUNT_ID}` inside its base URL. Rows
  fail validation while required placeholders remain unresolved.
- The `tinyfish-api` REST search profile requires `TINYFISH_API_KEY` in the qcg
  process environment when a contract actually uses it. The separate `tinyfish`
  MCP profile uses OAuth and does not require that API key; its OAuth
  credentials are obtained from the loopback Connections panel and managed
  through the OS keyring.
- `exa-public` and `parallel-public` are built-in anonymous Streamable HTTP MCP
  profiles. They require neither a registry file, an API key, nor an OAuth
  connection. Parallel's separate Task MCP endpoint requires authentication
  and is not bundled as an anonymous profile.

## Adding a provider

Add one `[[provider]]` row to your registry file, or uncomment a bundled
template. No rebuild is required:

```toml
[[provider]]
id = "my-provider"
api = "chat_completions"
base_url = "https://llm.example.com/v1"
api_key_env = "MY_PROVIDER_API_KEY"
capabilities = { tool_use = true, json_schema = true, seed = false }
```

Then reference it from a contract:

```toml
[llm]
model = { provider = "my-provider", model = "my-model" }
max_tokens = 2048
```

Because the registry is plain data, generators can emit customized
`providers.toml` files and contracts that reference them.

To add a search profile, add one `[[search_provider]]` row. The profile owns
the fixed transport and response mapping; the contract only selects the
profile and its per-agent budgets:

```toml
[[search_provider]]
id = "my-search"
endpoint = "https://search.example.com/api"
query_param = "query"
results_pointer = "/results"
title_pointer = "/title"
url_pointer = "/url"
snippet_pointer = "/snippet"
api_key_env = "MY_SEARCH_API_KEY"
auth_header = "X-API-Key"
```

```toml
[[flow.params.tools]]
name = "search_web"
kind = "web.search"
provider = "my-search"
max_results = 5
max_calls = 3

[permissions]
network = ["search.example.com"]
```

The `web.search` declaration is the explicit opt-in. Its provider profile is
not a generator secret, and the profile host is not implicitly granted by the
registry.

To expose an arbitrary MCP tool, add an `[[mcp_server]]` row and bind it from
the generator contract. For a remote server, all `allowed_hosts` entries must
also be granted by the contract:

```toml
[[mcp_server]]
id = "my-tools"
transport = "streamable_http"
url = "https://mcp.example.com/mcp"
auth = "bearer"
credential_env = "QCG_MY_MCP_TOKEN"
allowed_hosts = ["mcp.example.com"]

[[flow]]
id = "agent"
type = "llm.agent"
[flow.params]
prompt = "Use the declared tool when it is needed."
max_iterations = 4
max_tokens_total = 4096
tools = [
  { name = "lookup", kind = "mcp", server = "my-tools", tool = "lookup", max_calls = 3, side_effects = false },
]

[permissions]
network = ["mcp.example.com"]
side_effects = "none"
```

The shipped registry includes two anonymous public research profiles. They can
be used without an API key or Connections-panel authorization:

```toml
[[flow]]
id = "research"
type = "llm.agent"

[flow.params]
prompt = "Research current primary sources for the requested artifact."
max_iterations = 6
max_tokens_total = 16000
max_tool_calls_total = 4
request = { max_tokens = 4096, stream = true, tool_choice = "auto", parallel_tool_calls = true }
tools = [
  { name = "exa_search", kind = "mcp", server = "exa-public", tool = "web_search_exa", max_calls = 2, side_effects = false },
  { name = "parallel_search", kind = "mcp", server = "parallel-public", tool = "web_search", max_calls = 2, side_effects = false },
  { name = "researcher", kind = "agent", instructions = "Research with the delegated tools.", tools = ["exa_search", "parallel_search"], max_calls = 2, max_iterations = 4, max_tokens_total = 10000, max_tool_calls_total = 4, on_failure = { default = "return_error", by_code = { provider_failed = "fail" } } },
]

[permissions]
network = ["mcp.exa.ai", "search.parallel.ai"]
side_effects = "none"
```

Agent-as-tool failures are policy-controlled. `on_failure.default` and
`on_failure.by_code` accept `return_error` or `fail`. The default is
`return_error`, which gives the parent agent a closed error result containing
`isError`, `agent`, and `{code, message, retryable, call_number, limits}` instead of failing
the whole node. Invalid child arguments and child input guardrail rejection follow the
same policy-controlled path. `max_calls` independently bounds parent-driven retries; every
invocation receives a fresh per-invocation iteration, token, and tool-call budget.
`retryable` is true only while all relevant parent and specialist bounds permit another
invocation.
Codes distinguish `token_budget_exceeded`, `tool_call_budget_exceeded`,
`iteration_budget_exceeded`, `validation_failed`, `provider_failed`,
`guardrail_rejected`, and `tool_failed`. Run-wide budget exhaustion and
cancellation are execution boundaries, not specialist policy: they always
propagate, and contract validation rejects attempts to override them.

Exa also exposes `web_fetch_exa`; Parallel exposes `web_fetch`. Bind fetch only
when a workflow genuinely needs full-page retrieval. Search and fetched
content remain untrusted model inputs.

The MCP server's `tools/list` response supplies the input schema at run start.
qcg validates every model-generated argument against that schema and validates
`structuredContent` against the server's `outputSchema` when one is returned.
Internal JSON Schema references are supported; external references are
rejected and never fetched.
Remote descriptions and schema annotations are treated as untrusted metadata;
the contract's description is the trusted model-facing description. The
contract's `max_calls` is limited to 10 and defaults to 3; one agent's total
tool calls default to 32 and are bounded by `max_tool_calls_total`.
An MCP `isError` tool response is returned to the agent as a generic failed
tool result so it can use another declared tool or continue without that
result. Upstream error content is not exposed. Transport failures, timeouts,
permission failures, and schema violations remain fatal.

Set `side_effects = true` for a tool that can change external state. Such a
call uses the contract's `[permissions].side_effects` policy: `none` denies it,
`confirm` pauses at a HITL confirmation, `dry_run_first` records a dry-run
boundary before approval, and `allowed` permits it. Use `false` only when the
bound MCP operation is known to be read-only.

## Secrets

Provider credentials are read from the environment variables named by each
registry row. Their values are attached only to the configured authentication
location; they are not serialized into the registry, run journal, generated
artifacts, or debug representations. Commands launched by
generators receive a cleared environment containing only `PATH` and a
run-local `TMPDIR`, so provider credentials are not inherited by generator
processes. Do not place API key values directly in `providers.toml`,
`qcg.toml`, prompts, resources, URLs, query parameters, or generated artifacts.
A profile such as SerpAPI may declare the query parameter *name* through
`auth_query_param`; qcg injects its value only into the outbound request after
permission checks and removes it from returned URLs and transport errors.

Credentialed remote endpoints require HTTPS. Redirect following is disabled,
upstream error bodies and endpoint URLs are removed from errors, and a response
that contains the exact active credential in its raw or JSON-decoded content is
rejected. Every credential environment variable named by a loaded provider
registry row is reserved and cannot also be declared as a generator secret.
LLM text and decoded tool-call argument keys and values are also checked
recursively against declared generator secrets before they enter run state.

Set API credentials in the environment of the qcg process or through the
service manager that launches it. qcg does not load `.env` files. Repository
`.env` files are ignored defensively, but an external secret manager or OS
service credential facility is preferred for production. OAuth MCP profiles
use the OS keyring by default; do not replace that with a plaintext token file.
Rotate an API key or revoke an OAuth grant after any suspected disclosure.

Protocol references: [OpenAI Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create),
[OpenAI Responses](https://developers.openai.com/api/reference/resources/responses/methods/create),
[Azure OpenAI reasoning models](https://learn.microsoft.com/azure/foundry/openai/how-to/reasoning),
the [TinyFish Search API](https://docs.tinyfish.ai/search-api/reference),
[Parallel Search](https://docs.parallel.ai/api-reference/search/search),
[Parallel MCP programmatic use](https://docs.parallel.ai/integrations/mcp/programmatic-use),
[Exa Search](https://exa.ai/docs/reference/search),
[Exa MCP](https://exa.ai/docs/reference/exa-mcp),
[Firecrawl Search](https://docs.firecrawl.dev/api-reference/endpoint/search),
[Tavily Search](https://docs.tavily.com/documentation/api-reference/endpoint/search),
[Brave Search](https://api-dashboard.search.brave.com/api-reference/web/search/get),
[Serper](https://serper.dev/), and
[SerpAPI](https://serpapi.com/search-api).

## Retry Text

`[llm].retry_prompt` is rendered with minijinja and has access to `{{ error }}`
and `{{ attempt }}`. Invalid retry templates fail the step; qcg does not fall
back to a hidden prompt.
