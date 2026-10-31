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
2. `PROVIDERS` environment variable
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
base_url_env = "OPENAI_BASE_URL"
api_key_env = "OPENAI_API_KEY"
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
  `anthropic_messages`, or `system_one`. The value selects the payload family
  and default endpoint path (`chat/completions`, `responses`, `messages`, or
  `systemone`); `path_template` and the `decision_*` fields specialize the
  System One transport without code changes. System One uses typed decisions
  through `llm.decide`, not chat; its provider rows must not advertise chat
  capabilities.
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
  name from the contract. `{model}` expands as path segments so namespaced
  models such as `typesafe/jev` address `.../ai/run/typesafe/jev`; `?`, `#`,
  and `%` stay encoded, while empty, `.`, and `..` pieces fail closed.
- `query`: optional query parameters appended to the URL; values may contain
  non-credential `{ENV_VAR}` placeholders. The configured `api_key_env`,
  credential-like query names, and credential placeholders are rejected.
- `decision_input_wrapper`: System One request envelope mechanism
  (`none` or `input`, default `none`). `none` sends the direct TypeSafe
  shape `{"model","state","questions"}`; `input` sends the Cloudflare
  `POST .../ai/run` shape `{"model","input":{"state","questions"}}`.
  Rejected for other APIs.
- `decision_model_in_body`: System One model-placement mechanism (default
  `true`). `true` keeps `model` in the JSON body; `false` omits it so the
  model travels only via `path_template` `{model}` (Cloudflare
  `POST .../ai/run/{model}` style) and requires that placeholder.
  Rejected for other APIs.
- `decision_response_wrapper`: System One response-envelope mechanism
  (`auto`, `direct`, or `result`, default `auto`). `direct` accepts only the
  top-level TypeSafe shape; `result` requires the Cloudflare
  `{"result":{...},"success":...}` envelope (a `"success": false` envelope
  fails without echoing the body); `auto` accepts both. Rejected for other
  APIs. Together the three `decision_*` fields are the transport mechanism;
  the contract's `llm.decide` params stay pure policy
  (`provider`, `model`, `state`, `questions`, budgets).
- `timeout_seconds`: timeout for each completion attempt. The default is 120
  seconds; retryable failures may start another independently timed attempt.
- `retry_attempts`: attempts per completion call including the first, 1 to 10.
  The default is 3. The provider `retry_policy` selects which failures retry
  (by default timeouts, 5xx, 429, and empty responses); it cannot raise this bound.
- `retry_policy.max_retry_after_secs`: maximum header-derived delay candidate
  in seconds (default 60); `0` removes the header's delay contribution without
  changing ordinary backoff.
- `retry_policy.rules`: optional ordered transport-decision rules; see
  [Transport retry and route policy](#transport-retry-and-route-policy).
  Omission or an empty list uses the documented default.
- `retry_base_backoff_ms`: base wait between retries in milliseconds, up to
  60000. The default is 200; waits grow exponentially from there.
- `retry_rate_limit_floor_ms`: minimum wait per retry attempt for rate-limited
  (429) and empty-body responses, multiplied by the attempt number; 0 to 60000.
  The default is 5000; `0` disables the floor. Other retryable failures only
  use the exponential backoff.
- `retry_backoff_exponent_cap`: largest exponent applied to the exponential
  backoff, 1 to 16. The default is 8; a higher cap grows waits further per
  attempt before they level off.
- `response_body_limit_bytes`: maximum response-body size. The default is
  16 MiB; oversized non-streaming responses fail before JSON parsing. For
  streaming responses, the limit counts raw response-body bytes delivered by
  reqwest, including SSE framing and comments. Each received chunk is checked
  before SSE parsing;
  a chunk crossing the limit is rejected in full, so the parser never accepts
  bytes beyond the limit. Reqwest has already allocated the received chunk:
  this is an application/parser bound, not a guarantee on all transport
  allocation. An explicit value has no mechanistic ceiling.
- `max_concurrency` and `requests_per_minute`: optional provider-local admission
  limits. Requests wait for capacity instead of creating an unbounded burst.
- `circuit_breaker_failures` and `circuit_breaker_cooldown_seconds`: consecutive
  retryable-failure threshold and open duration. An open primary route may use
  only a fallback explicitly declared by the generator.
- `chat_token_limit_field`: Chat Completions output-limit field. It defaults
  to `max_tokens`; use `max_completion_tokens` for current OpenAI and Azure
  reasoning endpoints. It is rejected for other API flavors.
- `web_search_billing`: Responses-only hosted-search billable-unit mapping:
  `unknown` (the default), `openai_search_actions`, or `azure_num_requests`.
  Select it explicitly for the provider's billing semantics; qcg never guesses
  from the provider id or endpoint URL. See [provider-native web search](#provider-native-web-search).
- `stream_retry_attempts`: retry attempts for a stream that fails before
  delivering any event (default `0`, range `0..=3`). A retry is refused once
  any event was delivered, so two model responses can never interleave.
  `0` disallows same-route retries, but the policy can still select a declared
  fallback before emission. Only policy-selected failures retry; unlike the
  former blanket pre-emission retry, application/validation errors fail fast.
- `prompt_cache_field`: prompt-cache mechanism for this row. `prompt_cache_key`
  is valid for `chat_completions` and `responses`; `cache_control` and
  `cache_control_auto` are valid for `anthropic_messages`. It is required
  exactly when `capabilities.prompt_cache` is enabled and rejected otherwise,
  so a contract `[llm].cache = "auto"` never silently degrades. Rows without
  prompt caching stay unchanged. `cache_control` marks only the stable system
  prefix and is the safe default; `cache_control_auto` adds the top-level
  automatic tail marker (explicit system plus automatic tail, 5-minute default,
  2 of 4 slots) for multi-turn agent loops. Opt in only on routes whose docs
  support the top-level field. Azure breakpoint support is deployment-specific:
  Standard pay-as-you-go supports breakpoints; PTU-M does not. Before GPT-5.6,
  Azure does not support `prompt_cache_options`/breakpoints. Keep cache mechanism
  rows scoped to the exact API/model/deployment; split rows when deployment
  tiers differ, and never infer support from an arbitrary deployment name.
  Prefix hygiene (author guidance, no new mechanism): caching is a prefix
  match, so keep the contract tool declaration order stable and `[llm].system`
  templates free of per-run values (timestamps, UUIDs, run IDs). qcg emits
  tools in declaration order and derives `prompt_cache_key` from the system
  text with a single system breakpoint, so churn there silently breaks the prefix.
  Keep dynamic queries and tool outputs at the message tail and prefer system-only
  routing over full-context caching: compaction rewrites only message bodies and
  preserves the system/tools prefix, while any full-transcript cache necessarily
  diverges after compaction; per-turn tool-discovery reorder likewise breaks the
  tools prefix. Minimum cacheable prefixes and TTL pricing are model-dependent;
  verify with `cache_read_input_tokens` after any prompt-assembly change.
- `prompt_cache_retention`: extended-retention mechanism for the `extended`
  tier (`retention_24h`, `ttl_12h`, `ttl_1h`). Optional; absent means the route
  declares no `extended` tier and an `extended` request fails closed before
  dispatch. Model-aware: a `[[provider.models]]` entry may set
  `prompt_cache_retention` for that model only (overriding the row); a
  row-level value is an operator claim that every model on the row honors the
  mechanism (never inferred from the model-name string). API compatibility is
  enforced: `retention_24h` is valid for `chat_completions` and `responses`
  only (older OpenAI/Azure models, emitting `prompt_cache_retention: "24h"`);
  OpenAI older-model availability/defaults can depend on model and organization
  (including Zero Data Retention); qcg does not discover this, so declare only
  the exact route/org mechanism supported by its official documentation.
  `ttl_12h` is valid for `responses` only (OpenAI GPT-5.6+, emitting
  `prompt_cache_options: {"ttl": "12h"}`); `ttl_1h` is valid for
  `anthropic_messages` only (adding `cache_control: {"ttl": "1h"}` to every
  cache marker the row emits, system and top-level automatic markers). Setting
  `prompt_cache_retention` requires `capabilities.prompt_cache` on the
  effective row/model. Azure GPT-5.6+ supports only
  `prompt_cache_options.ttl: "30m"`, not an `extended` tier; older Azure models
  use the separate `prompt_cache_retention` control. These are cache-lifetime
  controls, not data-retention policies or deletion guarantees. Official
  documentation checked 2026-10-24 UTC: OpenAI's GPT-5.6+ Responses and
  earlier-model prompt-caching material; Azure's GPT-5.6+ and deployment-tier
  material; Anthropic's automatic/explicit caching, TTL and usage material:
  OpenAI
  https://developers.openai.com/api/docs/guides/prompt-caching, Azure
  https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/prompt-caching,
  Anthropic https://platform.claude.com/docs/en/build-with-claude/prompt-caching.
- `prompt_cache_explicit_mode`: when `true`, the row/model claims that the exact
  API/model/deployment disables reads and writes with
  `prompt_cache_options: {"mode": "explicit"}` and no qcg breakpoints. OpenAI
  documents this for GPT-5.6+ Responses; do not infer it for Chat Completions.
  Azure documents no-breakpoint explicit mode for GPT-5.6+; use it only on the
  exact API/model/deployment covered. Its separate breakpoint support is
  Standard pay-as-you-go only, not PTU-M; do not infer either declaration
  across mixed deployment tiers.
  Anthropic Messages rejects `forbid`: caching may occur during thinking/tool
  continuations or on server-tool results even without markers. Unsupported
  explicit `forbid` fails before dispatch on every route/fallback;
  `system_one` and `llm.decide` also reject non-default cache policies.
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

### ChatGPT subscription authentication

The standalone `qcg auth login`, `qcg auth status`, and `qcg auth logout`
commands manage Sign in with ChatGPT credentials in the OS keyring; each requires
`--account <label>`. See the [CLI credential lifecycle](cli-reference.md#chatgpt-subscription-credentials-standalone).
This is not an inference provider or an eligibility check. The provider schema
does not accept these credentials. An OpenAI API key is a separate, metered API billing path, not a ChatGPT
subscription credential. qcg does not send subscription-authenticated
Responses requests: the SIWC preview route requires `store = false` and
`stream = true` but does not support `max_output_tokens`, which qcg requires
to enforce its per-request generation bound. Keep subscription inference
unavailable until the documented route supports a compatible bound; never
remove the cap or fall back to a metered key. See OpenAI's [SIWC preview
limitations](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations)
and [API billing](https://openai.com/api/pricing/).

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
api_key_env = "OPENAI_API_KEY"
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
  used by `budget.max_cost_usd` when the contract leaves that price field unset.
- `cache_read_input_cost_per_million_usd` /
  `cache_creation_input_cost_per_million_usd`: optional cache input unit prices
  (USD/1M). Each defaults to the ordinary input rate when omitted; explicit
  zero is preserved. These rates flow through the catalog and per-call pricing.
  No vendor prices or cache TTL tiers are built in. `require_pricing` requires
  ordinary input/output rates and checks resolved cache-read and cache-creation
  pricing in every reachable base/tier row before dispatch, even without a cost
  cap. A configured Extended TTL may derive creation pricing from ordinary input
  only if every row has an input price. Costs remain estimates, not invoices.
- `cost_tiers`: optional context tiers, the same shape the contract's
  `model.cost_tiers` uses. A tier names `threshold_tokens` and the rates that
  price a request whose whole prompt input reaches that threshold. A rate the
  tier does not name inherits the base row's rate for that category, so a
  cache rate the tier does not name inherits the base row's cache rate. A
  registry tier list replaces the external catalog's tier list for the same
  model instead of merging with it, so an operator can state the exact
  schedule.
- A rate the registry row pins prices every tier it inherits, exactly as a
  contract pin does: the pinned rate is carried into each inherited external
  tier, so it prices that tier, and the tier contributes only the rates the
  pin leaves open. A registry row that declares its own tier list still
  replaces the external list outright.
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
Explicit `[[provider.models]]` metadata overlays external catalog entries
field by field. For a request, explicit contract rates likewise override the
effective catalog model field by field. The cost meter then selects one pricing
row among matching invocation routes and contract model entries: the first
complete input/output row wins; if none is complete, the first matching partial
row applies. It never mixes rates between those candidate rows. An absent
cache rate in the selected base row falls back to that base row's ordinary
input rate when present; explicit zero is preserved.

The commented `openrouter` template is a generic Chat Completions route, not an
upstream endpoint pin. qcg neither selects nor records the OpenRouter endpoint
or service tier that served the request. Because a model can have differently
priced upstream endpoints, a configured provider/model rate and `require_pricing`
only establish qcg's estimate inputs; they do not establish the rate OpenRouter
billed. `budget.max_cost_usd` uses that estimate and is not an upstream spend
guarantee.

Configured prices must be finite and non-negative (the manifest rejects
invalid prices, and the runtime refuses to bill them). `ModelRef::invalid_price`
validates base-row and web-search rates; `policy::validate_cost_tiers` validates
tier schedules. Tier rates are resolved by `policy::CostRates::resolve`:
absent tier cache rates inherit the base row's resolved cache rate, never the
tier's own input rate.

A context tier schedule the source publishes but qcg cannot read leaves the
model's flat rates in place: qcg prices a request at a rate it read, never at
one it guessed, and a third-party payload never removes a usable model.
The dropped schedule is logged as a warning naming the model and is exposed in
that model's optional `warning` field in the API catalog, `qcg models`, and
`llm.catalog select=models` output. It does not populate the provider's `error`;
that field remains for provider-level failures such as model-discovery errors.
The model's flat rates remain.

The `models_dev` source publishes a context band as `tier: {type, size}`:
the kind of band and the size the band starts above. qcg reads the
`context` kind, and the published size is the size the band starts above, so
qcg's threshold is one token past it: a request of exactly the published size
stays in the shorter band, and the next token reaches the band. The size is
the model's own, and each band keeps the cache and output rates the source
published for it. A row that names another kind of band prices no request,
and a model the source gives no context band for keeps its flat rates and is
reported with the model in the catalog view.

```toml
[catalog]
cache = "~/.data/cache/llm-catalog.json"   # default under $DATA_HOME
refresh_seconds = 86400

[[catalog.source]]
kind = "models_dev"
url = "https://models.dev/api.json"
sha256 = "..."                            # optional pin
```

The cache is loaded at process start so validation stays deterministic
offline. `qcg serve` refreshes stale sources in the background (failures are
logged and retried, never fatal), and `qcg models --refresh` or
Local catalog source files and the catalog cache have the same 64 MiB byte
limit as remote catalog sources. Oversized or non-regular files report an
error; an unreadable cache is ignored with a visible error. Cache writes that
exceed this limit are refused before publication.

`GET /api/llm/catalog?refresh=true` re-fetch on demand. The view marks stale
metadata and reports fetch errors.

`GET /api/llm/catalog` and `qcg models` list providers, models, efforts,
prices, and limits. Credentials and environment-variable values are never
returned.

Each model exposes `cost_tiers` in threshold order (`[]` for no schedule).
Each row has `threshold_tokens` and nullable `input_cost_per_million_usd`,
`cache_read_input_cost_per_million_usd`,
`cache_creation_input_cost_per_million_usd`, and `output_cost_per_million_usd`
(USD/1M tokens). These are effective catalog rates after the registry overlay:
missing tier categories inherit their base rate, missing base cache rates fall
back to base input (never tier input), explicit zero stays zero, and unresolved
rates are `null`. The existing flat fields remain base rates, not the rates of
any selected tier. The human CLI uses a compact
`>=threshold:input/cache-read/cache-creation/output` column, with `;` between
rows and `-` for unresolved rates or no schedule.

The greatest threshold T for which T <= whole prompt input selects the row
that prices the **whole request**, not just tokens above the threshold. Prompt
input includes both disjoint cached subsets (cache-read and cache-creation),
not just uncached input; output tokens do not choose the tier. The catalog
schedule is not a contract-specific final quote: contract rate pins carry into
inherited tiers, a contract's own schedule replaces the catalog schedule, and
configured cache TTL adjustments still apply. Costs remain estimates, not
invoices.

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
allowed_destinations = ["example.com"]
timeout_seconds = 120
max_response_bytes = 4194304
tools_list_page_limit = 100
oauth_state_ttl_seconds = 600
task_poll_interval_ms = 250
```

`transport` defaults to `streamable_http`. The URL must be an HTTP(S) URL
without userinfo, query, or fragment; a non-loopback remote URL must use
HTTPS. `allowed_hosts` must include the endpoint host and every host needed by
OAuth discovery, authorization, token, or registration requests (hosts only; IPv6 literals use the bracketed form `"[2001:db8::1]"`). It is also
the redirect allowlist for the OAuth HTTP client. The contract must list every
one of these hosts in `[permissions].network`; the registry never grants that
permission by itself.

`allowed_destinations` is the separate operator ceiling for server-side fetch:
every whole-value `http(s)` URL string in the tool arguments contributes its
host, and each host must be listed here (exact match or `"*"`, hosts only; IPv6 literals use the bracketed form `"[2001:db8::1]"`). A contract may narrow this ceiling per `mcp` tool with `destinations` (absent inherits; explicit list allows only those hosts, empty means query-only); see the contract reference.
It defaults to empty — host-free calls such as pure-query search pass, while
any fetch URL is refused until its host is listed. The endpoint host above
never implies destination rights. To migrate a fetch tool (for example an
Exa/Parallel fetch behind a custom `[[mcp_server]]` row), add each host the
model may ask the server to fetch; the built-in `exa-public` and
`parallel-public` profiles ship with the empty default, so their fetch tools
stay refused until the operator fronts the endpoint with an explicit row.
The ceiling covers only the initial whole-value host: qcg cannot re-check
server-side redirect targets or pin server-side DNS. The `HttpGateway`
per-hop re-check and `network_deny_private_ips` pinning apply only to
qcg-originated fetches. Prefer `web.search`/pure-query tools over server-side
fetch and list explicit per-host destinations (avoid `"*"` on
cloud/loopback-adjacent deployments). When enumeration cannot cover the reach,
a row may configure a fallback-only `destination_decider = ["bin", "arg"]`
plus `destination_decider_timeout_seconds` (default 5, bounded 1..=10): on a
static miss the per-tool narrowing would still admit, the execution gate hands
hosts-only `{v,server,tool,hosts}` on stdin to operator policy and requires
strict `{v,allow}` on stdout, denying on unset, failure, timeout, non-zero
exit, oversize, or malformed output. The binary must itself be allowed through
the contract's `[permissions].commands`; verdicts are journaled as
`mcp_destination_decision` (hosts only) and resumes re-invoke fresh. Unset
keeps the static ceiling exactly.

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
env_from = { MCP_TOKEN = "MCP_TOKEN" }
auth = "none"
```

`stdio` requires a non-empty command and cannot declare `url`, `headers`, or
`allowed_hosts`. It can declare `allowed_destinations`, with the same
whole-value extraction and empty-default-deny semantics as above, plus the
same optional fallback-only `destination_decider` and bounded
`destination_decider_timeout_seconds`. The child process receives a cleared environment containing
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
operating-system credential store under a profile-specific account, in the session's unlocked login collection (not a backable file; headless Linux without an unlocked Secret Service provider fails closed, and WSL has no default collection). `memory`
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
results. Direct form elicitation requests are projected onto qcg's durable HITL form,
and the accepted values plus opaque request state resume the original tool call.
A task that requests input while polling fails explicitly; only direct
elicitation suspends for input.
Unsupported input request methods fail explicitly. Only form-mode elicitation is
supported; URL (out-of-band) mode fails explicitly. Client sampling and roots are
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

### Transport retry and route policy

Transport facts and decisions belong to each provider row in `providers.toml`,
not to the generator's contract. The contract still exclusively declares the
ordered `fallback_models`; policy cannot invent a route, grant authorization,
change operation-completion/replay rules, or implement waiting/partial success.

Each failed attempt has a structured `kind` (`network`, `timed_out`,
`empty_response`, `invalid_response`, `circuit_open`, or `{ http_status = 503 }`,
for example), `phase` (`connect`, `response`, `unknown`), and request `dispatch`
(`not_sent`, `sent`, `unknown`). The adapter records `connect/not_sent` only
when reqwest reports a connection-establishment failure. Redirects are disabled,
so a failed redirected connection cannot hide an earlier dispatch. This includes
connection refusal and connect timeout; qcg does not guess DNS/reset causes
from error messages. Response reads/decoding, empty responses, and HTTP statuses
are `response/sent`. A generic send error or the router's whole-attempt timeout
is `unknown/unknown`: it does **not** prove that the request was never sent.
`sent` is dispatch evidence, not proof of completion or billed usage.

`retry_policy.rules` uses first-match order. Every omitted match field is a
wildcard: `kind`, `phase`, `dispatch`, `stream` (boolean), `attempt_from`,
`attempt_through`, and `provider_code`. Attempt bounds are inclusive, positive,
and one-based within the route, independent of the fallback index. A rule
chooses `action`:

- `fail_fast`: return the failure, without internal retry or route fallback.
- `retry`: retry the same unchanged request on the same route, with the existing
  timeout and bounded exponential backoff. At the attempt limit,
  `on_exhausted` chooses `fail_fast` (the rule default) or `next_route`;
  `on_exhausted = "retry"` is invalid.
- `next_route`: skip remaining internal attempts and try only the next declared
  route. With no route left the call fails. `llm.decide` has one declared route,
  so this decision ends in failure, never an invented decision fallback.

Unmatched failures use the built-in default: timeout, empty response, HTTP 429
and 5xx retry within the bound, then select `next_route`. An open circuit selects
`next_route` without another same-route attempt. All `network` failures fail fast,
**including known pre-dispatch failures**: this preserves existing behavior and
requires an explicit operator choice to add recovery. Ambiguous post-dispatch
network failures are therefore never retried/fallen back by default. The legacy
timeout default is retained even when dispatch is uncertain; operators who need
stricter duplicate-request avoidance can override it with a fail-fast rule.
Circuit failure counting remains based on its existing retryable-kind set;
transport policy does not redefine circuit accounting.

Non-success HTTP responses are inspected within a bounded prefix (at most
8 KiB and never more than the configured `response_body_limit_bytes`). Only
the supported `{"error":{"code":"..."}}` string shape is recognized, and only
a sanitized code (`1..=64` ASCII `A-Z a-z 0-9 . _ -`) is retained. Raw bodies,
endpoints, and credentials are never stored or surfaced; a body containing the
configured credential fails closed without a code, and malformed, oversized,
or unrecognized bodies yield no code. Capture alone is inert: it changes no
default unless a provider rule explicitly names the same `provider_code`. No
OpenAI-specific disposition is hardcoded for compatible endpoints; an
exhaustion/quota code still follows the configured rule, including a
`next_route` to a separately authorized route.

`Retry-After` response headers retain raw representable `u64` seconds
(delta-seconds or the accepted RFC 2822 / IMF-fixdate date forms). Missing,
invalid, past, or unrepresentable values yield no observation. The failure
retains the raw observation; the provider's `retry_policy.max_retry_after_secs`
clips it once into a delay candidate (default **60** when omitted). This is a
nonnegative integer in the `u64` range; **0** removes the header's delay
contribution, not ordinary backoff. For example, set
`[provider.retry_policy]` with `max_retry_after_secs = 120` before its rule tables
to permit header-derived candidates above the default.

The router uses the candidate only when the matching rule sets
`use_retry_after = true` and the effective action remains a same-route `retry`;
otherwise current backoff and route behavior are unchanged. An applied candidate
never shortens computed backoff: the router waits the maximum of the two.
Route fallback and terminal guards never wait for the header.

Example, appended inside one provider row (before the next `[[provider]]`):

```toml
# Opt in to declared fallback only for a proven pre-dispatch network failure.
[[provider.retry_policy.rules]]
kind = "network"
phase = "connect"
dispatch = "not_sent"
action = "next_route"

# Refuse uncertain timeout replay instead of the default bounded timeout retry.
[[provider.retry_policy.rules]]
kind = "timed_out"
dispatch = "unknown"
action = "fail_fast"

# Alternatively select bounded retry with an explicit exhaustion disposition:
[[provider.retry_policy.rules]]
kind = { http_status = 503 }
attempt_from = 1
attempt_through = 10
action = "retry"
on_exhausted = "next_route"

# Distinguish identical 429s by bounded provider code: transient throttling
# retries and honors Retry-After, while a configured quota code falls back.
# This does not hardcode OpenAI semantics for other endpoints.
[[provider.retry_policy.rules]]
kind = { http_status = 429 }
provider_code = "slow_down"
use_retry_after = true
action = "retry"
on_exhausted = "next_route"

[[provider.retry_policy.rules]]
kind = { http_status = 429 }
provider_code = "insufficient_quota"
action = "next_route"
```

`retry_attempts` still bounds completion/decision attempts including the first
(default 3, maximum 10); `stream_retry_attempts` bounds *additional* stream
attempts (default 0, maximum 3). Streaming retries now use the configured backoff
and rate-limit/empty-response floor too. Cancellation drops attempts/backoff;
a canceled call never retries or falls back. Circuit-open errors cannot be
same-route retried. Application/configuration errors (`other`) and partial-stream
failures cannot be made recoverable by a rule. No policy can retry or fall back
after a stream event is forwarded, even when secret filtering has withheld the
delta from the journal. A declared fallback receives the unchanged request:
qcg rechecks the actual model's capabilities (including seed, effort, tools,
structured output, streaming and media) and the provider's API-specific checks;
it never silently removes an unsupported parameter.

`llm_call.retry_decisions` retains failed attempts preceding a successful call,
including decisions on the decision API. `llm_route_failed.retry_decisions`
retains them for a failed route, and `fallback_taken` states whether this
invocation advanced to another declared route. Each decision carries its kind,
facts, attempt, stream flag, requested/effective actions, source (`default` or
`rule:<zero-based index>`, or `gateway` for a gateway terminal guard), the observed
sanitized `provider_code`, raw `retry_after_observed_secs`, and policy-bounded
`retry_after_secs` candidate when present. Comparing observed and candidate
seconds reveals clipping even without opt-in. `retry_after_applied` is true only
when a matching rule opts in, an observation exists, and the effective action
remains `retry`; it means the candidate is used as a backoff floor, not that it
adds wait (a zero candidate or larger ordinary backoff may add none). Decisions
also carry any mechanism guard (`attempt_limit`, `circuit_open`, `terminal_error`,
`stream_emitted`). A gateway cancellation uses
a null attempt because cancellation during backoff versus an active attempt
cannot be distinguished there; it never invents an additional dispatched attempt.
These observation events
follow the configured audit policy. They contain no request body, endpoint,
credentials, or upstream error text. Successful calls still account usage once;
failed attempts can incur unreported upstream usage, so retries/fallback are not
an upstream spending guarantee.

### Budget and fallback policy guidance

Mechanism enforces, policy chooses. qcg enforces run-wide `[budget]`
(`max_steps`, `max_tokens`, `max_cost_usd`, `max_elapsed_seconds`), static
ordered `fallback_models` (at most 8, only when provider policy selects
`next_route`), and the
streaming rule that route fallback is refused once the first delta is emitted
(partial billing is then unavoidable). Unpriced routes account as zero and are
flagged, since totals may understate spend. Chat token and cost ceilings use
estimated preflight admission (prompt bytes/4 plus the requested max output,
priced on the initial route) plus post-hoc accounting from reported usage:
local admission and accounting cannot guarantee that the provider never
exceeds the ceiling, since token-dense input, a pricier fallback route, and
retry attempts with unreported usage can all exceed the estimate. Do not
treat the local ceiling as an upstream spending guarantee.

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

`llm.decide` sends the logical decision (`model`, `state`, typed `questions`)
through the provider row's declared System One transport. The bundled
`typesafe` row targets the direct endpoint, `POST /v1/systemone`. It requires
an explicit `params.model` selecting a `system_one` provider. It is independent of `[llm]`:
no `[llm]` or registry default model, chat controls, or chat output ceiling is
inherited, and there is no model or provider fallback. Run-wide budgets still
apply. Chat parameters (including `prompt`, `request`, tools, and streaming),
templates, and node `context` are rejected rather than ignored. A System One
profile cannot serve chat requests.

Mechanism lives in `providers.toml`; policy lives in the contract. The
contract never names paths or envelopes: it selects `provider`/`model` and
declares `state`/`questions`/budgets, while the row declares `base_url`,
`path_template`, and the three `decision_*` envelope fields. Adding a new
System One gateway is a registry edit with no rebuild.

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

### Cloudflare Workers AI Jev

Cloudflare hosts the same Jev model as `typesafe/jev` with a different wire
envelope ([model page](https://developers.cloudflare.com/ai/models/typesafe/jev/)).
The contract fragment above stays unchanged except for the route; only the
registry row changes. Two registry styles are supported, both pure mechanism:

```toml
# Generic `POST .../ai/run`: model in the body, inputs under `input`.
[[provider]]
id = "cloudflare-jev"
api = "system_one"
base_url = "https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai"
api_key_env = "CLOUDFLARE_API_KEY"
path_template = "run"
decision_input_wrapper = "input"
decision_response_wrapper = "auto"
```

```toml
# Classic `POST .../ai/run/{model}`: model in the path only, bare inputs.
[[provider]]
id = "cloudflare-jev-path"
api = "system_one"
base_url = "https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai"
api_key_env = "CLOUDFLARE_API_KEY"
path_template = "run/{model}"
decision_model_in_body = false
decision_response_wrapper = "auto"
```

```toml
[[flow]]
id = "classify"
type = "llm.decide"
[flow.params]
model = { provider = "cloudflare-jev", model = "typesafe/jev" }
state = "Help! My payouts have been failing for three days."
max_tokens = 4096
[flow.params.questions.urgent]
type = "noul"
instructions = "Does this convey urgency?"
```

`{model}` expands as path segments, so `typesafe/jev` addresses
`.../ai/run/typesafe/jev`; `decision_response_wrapper = "auto"` (the default)
accepts both the direct `{model,answers,usage}` shape and the Cloudflare
`{"result":{...},"success":...}` envelope. A `"success": false` envelope fails
without echoing the upstream body. Set `CLOUDFLARE_API_KEY` (Workers AI Read +
Write) and `CLOUDFLARE_ACCOUNT_ID` before running; the shipped
`providers.toml` carries both rows as commented templates.

## Bundled providers

The shipped registry follows an opt-in execution model. Local LLM endpoints
and the remote `typesafe` System One profile are active configuration rows;
other remote LLM providers ship as commented templates. Unlike those templates,
`typesafe` needs no uncommenting and is remote, not a local endpoint. Like the
local rows, it only contacts its endpoint when execution reaches a request
using that profile; loading the registry does not contact it.
Ollama and LM Studio are credential-free; `openai_client` requires its declared
environment variable. `typesafe` requires `TYPESAFE_API_KEY` and permits the
explicit `TYPESAFE_BASE_URL` override. MCP servers and REST search profiles
are opt-in: they are contacted only after a contract declares the corresponding
tool and grants the required permission.

Active by default: `fake` (built in), `ollama`, `lmstudio`, `openai_client`,
`typesafe` (remote System One; no chat capabilities).

Bundled MCP profile: `tinyfish` (Streamable HTTP + OAuth; no
`TINYFISH_API_KEY`; one-time loopback Connections-panel authorization; OAuth
credentials use the OS keyring).

Commented templates: `anthropic`, `openai`, `openai_responses`, `openrouter`,
`gemini`, `sakura`, `cloudflare`, `cloudflare-jev`, `cloudflare-jev-path`,
`opencode-go`, `opencode-zen`, `opencode-zen-free`,
`opencode-go-responses`, `opencode-zen-responses`, `groq`, `deepseek`,
`mistral`, `xai`, `together`, `fireworks`, `azure-openai`.

| Provider id | API | API key environment variable | Other required environment variables |
|---|---|---|---|
| `anthropic` | `anthropic_messages` | `ANTHROPIC_API_KEY` | none |
| `openai` | `chat_completions` | `OPENAI_API_KEY` | none |
| `openai_responses` | `responses` | `OPENAI_API_KEY` | none |
| `openai_client` (active) | `chat_completions` | `OPENAI_CLIENT_API_KEY` | none |
| `ollama` (active) | `chat_completions` | none | none |
| `lmstudio` (active) | `chat_completions` | none | none |
| `typesafe` (active, remote) | `system_one` | `TYPESAFE_API_KEY` | none |
| `openrouter` | `chat_completions` | `OPENROUTER_API_KEY` | none |
| `gemini` | `chat_completions` | `GEMINI_API_KEY` | none |
| `sakura` | `chat_completions` | `SAKURA_API_KEY` | none |
| `cloudflare` | `chat_completions` | `CLOUDFLARE_API_KEY` | `CLOUDFLARE_ACCOUNT_ID` |
| `cloudflare-jev` | `system_one` | `CLOUDFLARE_API_KEY` | `CLOUDFLARE_ACCOUNT_ID` |
| `cloudflare-jev-path` | `system_one` | `CLOUDFLARE_API_KEY` | `CLOUDFLARE_ACCOUNT_ID` |
| `opencode-go` | `chat_completions` | `OPENCODE_API_KEY` | none |
| `opencode-zen` | `chat_completions` | `OPENCODE_API_KEY` | none |
| `opencode-zen-free` (anonymous example) | `chat_completions` | none | none |
| `opencode-go-responses` | `responses` | `OPENCODE_API_KEY` | none |
| `opencode-zen-responses` | `responses` | `OPENCODE_API_KEY` | none |
| `groq` | `chat_completions` | `GROQ_API_KEY` | none |
| `deepseek` | `chat_completions` | `DEEPSEEK_API_KEY` | none |
| `mistral` | `chat_completions` | `MISTRAL_API_KEY` | none |
| `xai` | `chat_completions` | `XAI_API_KEY` | none |
| `together` | `chat_completions` | `TOGETHER_API_KEY` | none |
| `fireworks` | `chat_completions` | `FIREWORKS_API_KEY` | none |
| `azure-openai` | `responses` | `AZURE_OPENAI_API_KEY` | `AZURE_OPENAI_BASE_URL` |

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
  automatic `<ID>_BASE_URL` naming rule. See `providers.toml` for each
  profile's declaration.
- Azure OpenAI uses deployment names as the model value. The recommended
  `azure-openai` row uses the current Responses API and expects
  `AZURE_OPENAI_BASE_URL` to include `/openai/v1`. It does not use an
  `api-version` query. For hosted search, advertise `web_search = true` only
  on supported models and select `web_search_billing = "azure_num_requests"`
  (already selected in the commented profile).
  Cloudflare resolves `{CLOUDFLARE_ACCOUNT_ID}` inside its base URL. Rows
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
credential_env = "MY_MCP_TOKEN"
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

## Anonymous free-model endpoints

Any compatible endpoint can use the ordinary provider configuration without
`api_key_env` or `api_key_file_env`. qcg then sends no authorization header and
reads no implicit provider credential. A zero token price is accounting metadata,
not evidence that the endpoint permits anonymous inference.

The bundled `opencode-zen-free` example uses this mechanism. Uncomment its
`[[provider]]` block and nested `[[provider.models]]` block together in
`providers.toml`, then select it in the contract:

```toml
[llm]
model = { provider = "opencode-zen-free", model = "big-pickle" }

[permissions]
network = ["opencode.ai"]
```

Merge the network host into existing permissions rather than replacing them.
The profile does not become the default model. Even if `OPENCODE_API_KEY` is
set, this anonymous profile does not read it; the separate `opencode-zen`
profile continues to require that credential.

**Availability checked on 2026-10-03:** a direct anonymous `big-pickle`
inference request returned HTTP 403 with `FreeTierError`, stating that the
free tier can only be used within OpenCode. This example documents the wire
configuration; it is not a verified working inference service. qcg surfaces
HTTP failures without switching to a paid provider. Its existing error handling
does not expose arbitrary upstream response bodies.

The [OpenCode server source](https://github.com/anomalyco/opencode/blob/dev/packages/console/app/src/routes/zen/util/handler.ts)
treats the public token `public` as absent authentication and checks
`allowAnonymous` per model. Omitting authentication uses that anonymous path;
no dummy credential or OpenCode client impersonation is needed in qcg's config.
The provider can still reject a request under its current access policy.

To add another model, add an explicit `[[provider.models]]` entry under the
chosen provider with its exact model ID and verified input/output prices.
For a different service, declare its own provider ID, compatible API format,
and base URL, and grant its host in the contract. Advertise only confirmed
capabilities; the example leaves tools, structured output, sampling controls,
and streaming disabled until verified for the selected model.

The example intentionally omits `models_discovery` and `catalog_id`. The public
`/models` response also includes paid models and does not certify anonymous
inference access. Explicit model entries curate the picker, not an access
allowlist: contracts may still name other models. Do not infer free access
from a name suffix, zero price, or successful model discovery. Availability
and prices must be rechecked when changing the configuration.


### Provider usage normalization

[OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching)
reports total Chat `prompt_tokens` or Responses `input_tokens`, with
`cached_tokens` and (when supplied) `cache_write_tokens` as disjoint subsets.
qcg preserves the inclusive total and records reads as `cached_input` and
writes as `cache_creation_input`. [Anthropic prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)
reports disjoint `input_tokens`, `cache_read_input_tokens`, and
`cache_creation_input_tokens`. qcg sums them as total `input` and retains both
cache buckets. Anthropic can also report TTL-specific 5m/1h write subcounters;
qcg currently consumes the aggregate only. Streaming uses the same normalization;
cumulative delta counters replace earlier counters.
Without explicit extended retention, Anthropic cache markers use the default
five-minute lifetime. Default Anthropic writes cost 1.25x ordinary input;
extended `ttl_1h` writes cost 2x. OpenAI GPT-5.6+ Responses default 30-minute writes cost
1.25x ordinary input; Responses extended `ttl_12h` writes cost 2.5x. Default
write rates remain supplied by the selected pricing row; qcg does not infer
prices from model names or default markers. Older OpenAI/Azure
`retention_24h` policies do not change the configured write rate.

When `cache_retention = "extended"` requests `ttl_1h` or `ttl_12h`, the cost
meter replaces only the `cache_creation_input` rate with the corresponding
multiplier of the selected context tier's ordinary input rate. Read, ordinary
input and output prices, usage normalization, and context-tier selection are
unchanged. qcg prices aggregate cache writes at the requested TTL multiplier,
not provider-specific TTL subcounters; streaming usage may omit the breakdown.
Anthropic may create default 5m writes during tool/thinking continuations even
when qcg requested 1h markers, so extended write-cost estimates can differ from
provider charges. Pre-call max-cost admission includes the multiplier at the
dearest possible tier's input rate; if an ordinary input rate needed for a
possible tier is unavailable, dispatch fails closed. This is still an
estimated reservation,
not a bound on provider spend or a redesign of fallback-route reservations.
Missing optional counters are zero; present invalid counts, subset overflow,
and cache sums exceeding total input fail closed instead of being clamped.

## Provider-native web search

Some providers host their own web search: the model asks the provider to
search, the provider fetches the pages, and the provider bills the calls.
That is a different mechanism from qcg's own `web.search` tool, from MCP
tools and from the external search adapters, and qcg never presents one as
another.

The capability is off unless a contract asks for it. Selecting an OpenAI
model does not enable it, and a provider offers it only when the registry
row advertises `web_search = true` in its capabilities.

| Capability | Responses endpoint | Chat Completions endpoint | Anthropic Messages endpoint |
|---|---|---|---|
| Provider-hosted web search | supported on models whose row advertises `web_search` | not supported | not supported |

A contract opts in per node:

```toml
[[flow]]
id = "research"
type = "llm.generate"

[flow.params]
prompt = "research.j2"
provider_web_search = { search_context_size = "medium", allowed_domains = ["example.com"], max_calls = 3 }
```

- `allowed_domains`, `blocked_domains`: at most 100 entries each, and a
  contract states one list or the other, never both.
- `search_context_size`: `low`, `medium` or `high`. The provider's own
  default applies when the field is absent.
- `user_location`: an approximate `country`, `region`, `city` and
  `timezone`.
- `max_calls`: the bound on hosted search calls per request, 3 by default;
  local function-tool calls do not consume this budget. It maps to Responses
  `max_tool_calls`, which OpenAI defines over built-in tools only. qcg adds
  `web_search` as the only built-in tool in this request; custom function tools
  retain their separate local call limits. See the
  [Responses API reference](https://platform.openai.com/docs/api-reference/responses/create).

Permissions and networking: the search runs at the provider, so its egress
is the provider's, not the generator's, and the contract's network
permission allowlist does not govern it. qcg sends no request to the
searched hosts.

Usage and cost: `llm_call.web_search.calls` counts provider-billable units,
not every `web_search_call` output item. The provider row's Responses-only
`web_search_billing` setting selects the mapping:

| Setting | Billable count |
|---|---|
| `unknown` (default) | Unknown, even if output items or usage are present |
| `openai_search_actions` | Number of `search` actions in the output array; `open_page` and `find_in_page` are not billed calls |
| `azure_num_requests` | Provider-reported nonnegative integer `tool_usage.web_search.num_requests` (Bing requests); absent or invalid values remain unknown |

The shipped `openai_responses` profile selects `openai_search_actions`;
`azure-openai` selects `azure_num_requests`. Other compatible endpoints must
select their supported mapping explicitly; the id and URL do not choose it.
For OpenAI mode, an output array with no search actions reports zero; no
output array leaves the count unknown. In streaming, only OpenAI mode may
fall back to observed finished search actions when the terminal response
omits output. Azure and unknown modes never infer usage from streamed items.
All modes retain the full action events and URL citations independently of
the billable count, including streamed items when terminal output is absent.
A null count is unknown, never zero.

OpenAI's public Responses pricing lists `web_search` at $10 per 1,000 calls
($0.01 per billable search action), with search-content tokens billed at model
input rates. For that rate, set `web_search_call_cost_usd = 0.01` on the selected
model's contract pricing row; configure token rates separately. For Azure,
price the reported Bing request unit at the applicable Azure rate, not an
OpenAI action rate. qcg does not infer hosted-search prices: unknown usage or
a missing price for positive billable usage remains unmetered, never free.
A run whose cost budget requires pricing refuses requested-but-unknown search
usage and known positive counts without a price; no request and known zero are
distinct. Totals are estimates, not invoices. Verify effective
rates and usage semantics against
[OpenAI web search](https://developers.openai.com/api/docs/guides/tools-web-search),
[OpenAI API pricing](https://developers.openai.com/api/docs/pricing), and
[Azure OpenAI web search](https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/web-search).

### Anthropic context-window terminal

Anthropic Messages (including SSE) can terminate with
`model_context_window_exceeded` (documented for Sonnet 4.5 and newer supported
models). qcg retains this as `StopReason::ModelContextWindowExceeded`, not an
unknown response or a completed answer. Partial text, empty output and tool
blocks retain their reported usage; unfinished SSE tool JSON is retained as a
string argument fact, never an executable call. Unknown terminal reasons remain
invalid responses.

The gateway writes one `llm_call` with the original normalized `stop_reason`,
reported token usage (including cache usage), and the configured usage-based
cost, then returns nonretryable `provider_context_exhausted`. Canonical failure
details omit `exhaustion`: the provider does not report context capacity or an
exact used/allowed pair. Partial output cannot complete a generation or dispatch
tools, including through the structured-output shim. This terminal does not
trigger route fallback, step/validation retry, delegated-agent error recovery,
automatic compaction, continuation, or a limit increase. Existing `max_tokens`
and `refusal` behavior is unchanged. Stream deltas already published remain
partial observations, not successful step outputs; no partial response text is
added to the failure report or `llm_call`.
