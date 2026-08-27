# LLM Provider Guide

qcg resolves LLM providers from an external registry file, `providers.toml`.
Apart from the built-in `fake` provider, provider-specific names live in the
registry and documentation rather than qcg's code. Contracts reference
registry rows by `id` and declare explicitly which provider and model each run
uses.

## Registry resolution

The registry file is resolved once per process, in this order:

1. `--providers <PATH>` (CLI flag; authoritative, no fallback)
2. `QCG_PROVIDERS` environment variable
3. `./providers.toml` relative to the working directory
4. `<install prefix>/share/qcg/providers.toml` next to the binary

If no file is found, only the built-in `fake` provider remains available;
referencing any other provider fails with registry setup guidance. Release
archives bundle the default registry at
`share/qcg/providers.toml`; the source tree keeps it at the workspace root.

## Registry schema

```toml
# Optional default used when a contract omits `[llm].model`.
[default]
model = { provider = "openai", model = "gpt-4.1-mini" }

[[provider]]
id = "openai"
api = "chat_completions"
base_url = "https://api.openai.com/v1"
base_url_env = "QCG_OPENAI_BASE_URL"
api_key_env = "QCG_OPENAI_API_KEY"
auth_header = "api-key"            # optional; omit for Bearer auth
path_template = "openai/deployments/{model}/chat/completions"
query = { "api-version" = "{QCG_AZURE_OPENAI_API_VERSION}" }
capabilities = { tool_use = true, json_schema = true, streaming = false, seed = true }
```

Fields:

- `id`: unique identifier referenced by contracts.
- `api`: request protocol: `chat_completions`, `responses`, or
  `anthropic_messages`. The value selects the payload format and endpoint path
  (`chat/completions`, `responses`, or `messages`).
- `base_url`: literal endpoint root. `{ENV_VAR}` placeholders are resolved
  from the environment before use. The configured `api_key_env` placeholder,
  other credential-like placeholders, URL userinfo, queries, and fragments are
  rejected.
- `base_url_env`: environment variable that overrides `base_url`.
- `api_key_env`: environment variable whose value is sent as the credential.
  Omit it for endpoints that need no authentication (for example Ollama).
- `auth_header`: header name carrying the credential. Omit to use standard
  Bearer authorization.
- `path_template`: optional request path where `{model}` expands to the model
  name from the contract.
- `query`: optional query parameters appended to the URL; values may contain
  non-credential `{ENV_VAR}` placeholders. The configured `api_key_env`,
  credential-like query names, and credential placeholders are rejected.
- `timeout_seconds`: timeout for each completion attempt. The default is 120
  seconds; retryable failures may start another independently timed attempt.
- `capabilities`: advertised support for `tool_use`, `json_schema`,
  `streaming`, and `seed`.

Unknown fields are rejected. A row without a usable base URL, an unset
credential variable, or an unresolved `{ENV_VAR}` placeholder fails contract
validation before any run starts with the message
`set `<VAR>` before running the generator`.

## Declaring the model

Contracts declare one model explicitly:

```toml
[llm]
model = { provider = "openai", model = "gpt-4.1-mini" }
system = "You generate concise configuration patches."
retry_prompt = "The previous JSON failed validation: {{ error }}. Try again."
seed = 42
requires = ["json_schema"]
```

When `[llm].model` is omitted, the `[default].model` row from the registry is
used. A run fails when neither is declared; there is no hidden fallback.

`requires` is checked against the selected provider's capabilities before a
run starts. If the provider does not advertise a required capability such as
`tool_use`, `json_schema`, `streaming`, or `seed`, qcg fails the contract
validation instead of trying a hidden fallback. The `seed` value itself is
only sent to providers whose capabilities include `seed`.

## Bundled providers

The shipped registry follows an opt-in format: local endpoints are active by
default, and every remote provider ships as a commented template. Ollama and
LM Studio are credential-free; `openai_compat` requires its declared
environment variable. Enable a remote provider by deleting the leading `# `
from its block; the URLs are fixed constants, so that is the only registry edit
needed before setting the credential variable.

Active by default: `fake` (built in), `ollama`, `lmstudio`, `openai_compat`.

Commented templates: `anthropic`, `openai`, `openai_responses`, `openrouter`,
`gemini`, `sakura`, `cloudflare`, `opencode-go`, `opencode-zen`,
`opencode-go-responses`, `opencode-zen-responses`, `groq`, `deepseek`,
`mistral`, `xai`, `together`, `fireworks`, `azure-openai`.

| Provider id | API | API key environment variable | Other required environment variables |
|---|---|---|---|
| `anthropic` | `anthropic_messages` | `QCG_ANTHROPIC_API_KEY` | none |
| `openai` | `chat_completions` | `QCG_OPENAI_API_KEY` | none |
| `openai_responses` | `responses` | `QCG_OPENAI_API_KEY` | none |
| `openai_compat` (active) | `chat_completions` | `QCG_OPENAI_COMPAT_API_KEY` | none |
| `ollama` (active) | `chat_completions` | none | none |
| `lmstudio` (active) | `chat_completions` | none | none |
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
| `azure-openai` | `chat_completions` | `QCG_AZURE_OPENAI_API_KEY` | `QCG_AZURE_OPENAI_ENDPOINT`, `QCG_AZURE_OPENAI_API_VERSION` |

Notes:

- The built-in `fake` provider works even when no providers registry file
  exists at all, so demo generators that declare `fake` run offline.
- Referencing a provider whose row is still commented fails contract
  validation with "is not registered" plus guidance: enable the row when a
  registry is present, or set one up via `--providers` / `QCG_PROVIDERS`
  when no registry was found.
- Every remote row also accepts a base-URL override environment variable
  (`QCG_<ID>_BASE_URL`, for example `QCG_OPENAI_BASE_URL`). See
  `providers.toml` for exact names.
- Azure OpenAI uses deployment names as the model value and requires its
  endpoint and API-version variables. Cloudflare resolves
  `{QCG_CLOUDFLARE_ACCOUNT_ID}` inside its base URL. Both rows fail validation
  while required placeholders remain unresolved.

## Adding a provider

Add one `[[provider]]` row to your registry file, or uncomment a bundled
template. No rebuild is required:

```toml
[[provider]]
id = "my-provider"
api = "chat_completions"
base_url = "https://llm.example.com/v1"
api_key_env = "MY_PROVIDER_API_KEY"
capabilities = { tool_use = true, json_schema = true, streaming = false, seed = false }
```

Then reference it from a contract:

```toml
[llm]
model = { provider = "my-provider", model = "my-model" }
```

Because the registry is plain data, generators can emit customized
`providers.toml` files and contracts that reference them.

## Secrets

Provider credentials are read from the environment variables named by each
registry row. Their values are attached only to the provider request header;
they are not serialized into the registry, run journal, generated artifacts,
or debug representations. Commands launched by generators receive a cleared
environment containing only `PATH` and a run-local `TMPDIR`, so provider
credentials are not inherited by generator processes. Do not place API keys
directly in `providers.toml`, `qcg.toml`, prompts, resources, URLs, query
parameters, or generated artifacts.

Credentialed remote endpoints require HTTPS. Redirect following is disabled,
upstream error bodies and endpoint URLs are removed from errors, and a response
that contains the exact active credential in its raw or JSON-decoded content is
rejected. Every credential environment variable named by a loaded provider
registry row is reserved and cannot also be declared as a generator secret.
LLM text and decoded tool-call argument keys and values are also checked
recursively against declared generator secrets before they enter run state.

Set credentials in the environment of the qcg process or through the service
manager that launches it. qcg does not load `.env` files. Repository `.env`
files are ignored defensively, but an external secret manager or OS service
credential facility is preferred for production. Rotate a key after any
suspected disclosure.

## Retry Text

`[llm].retry_prompt` is rendered with minijinja and has access to `{{ error }}`
and `{{ attempt }}`. Invalid retry templates fail the step; qcg does not fall
back to a hidden prompt.
