# Security Model

qcg is a generation runtime with optional bearer authentication, not an identity provider,
authorization server, or TLS endpoint. It has no per-user ownership model for
runs. qcg listens on the explicitly selected network without imposing an
authentication policy. Set `--api-token` or `QCG_API_TOKEN` when instance-level
bearer protection is wanted. Deploy one instance and runs directory per tenant
or trust domain.

## Trust boundary

When a token is configured, anyone holding the instance bearer token can list generators, start and control
runs, read journals, and download artifacts. The token is an instance boundary,
not row-level ownership. Terminate TLS before qcg on untrusted networks.

For production, use this binary boundary:

```text
client -> qpx (TLS + token enforcement) -> 127.0.0.1 qcg
             ^
             |
          qid issues tokens
```

qcg does not import, link to, or invoke qid or qpx. The products remain
independently buildable and composable over HTTP.

Authentication at qpx or by qcg does not add ownership checks inside qcg. Anyone accepted
by the trusted boundary can access the API resources exposed by that qcg
instance, including other runs in its runs directory. Use qcg only for trusted
shared use, or enforce per-user or per-tenant separation outside qcg with separate
service instances and runs directories or an owner-aware gateway.

The default run-store mode exclusively owns a runs directory. The explicit
`shared-filesystem` mode permits multiple services only when the storage
provides reliable advisory locks. A run-level lease prevents duplicate engine
execution, and periodic recovery reclaims abandoned runs. This is availability,
not tenant isolation. API runs receive UUID-based IDs and separate
`meta/journal.jsonl` and `workspace/` paths. Direct runs reject concurrent use
of the same output directory.

The process-local `--max-active-runs` or `QCG_MAX_ACTIVE_RUNS` limit defaults
to 8. It is a capacity control, not an access-control mechanism. Runs beyond
the limit wait in the durable execution queue, while runs paused for human
input or confirmation release their execution slot. All runs in one process share the configured LLM and
search provider runtimes and provider HTTP clients.

## Contract sandbox

Generator capabilities are denied unless declared. Contracts bound filesystem
paths, commands and argument shapes, network hosts, containers, secrets, side
effects, runtime, and budget. Side effects require confirmation when declared
with `side_effects = "confirm"`.

Every command permission must choose `container` or `trusted_host` isolation.
Container execution requires a digest-pinned allowlisted image and uses no
network, a read-only root, dropped capabilities, no-new-privileges, a PID limit,
and a single workspace mount. `trusted_host` grants execution as the qcg OS
user. Stdio MCP servers follow the same rule.

Provider credentials are read from named environment variables or private
files selected by `api_key_file_env`; file-backed credentials are re-read per
request for rotation and must be absolute, non-symlink, UTF-8, at most 64 KiB,
and inaccessible to group/other users on Unix. Generator secrets support the
same bounded `file_env` model at run boundaries. Credentials are sent only to
the configured provider authentication location. Generator commands run
with a cleared environment containing only `PATH` and a run-local `TMPDIR`;
stdio MCP servers additionally receive only configured non-sensitive `env` and
explicitly mapped `env_from` values. Neither inherits provider credentials or
other parent-process state. Process-control and language-runtime injection
variables are rejected. Their stderr is discarded so an MCP child cannot copy
an `env_from` credential into qcg logs. Credential variables named by loaded
LLM, search, or MCP registry rows are reserved and cannot be redeclared as
generator secrets. Credentialed remote providers require
HTTPS, do not follow redirects, and fail closed when an upstream response
contains the exact active credential in its raw or JSON-decoded content.
Provider URLs and query parameters cannot interpolate the configured
credential environment variable, regardless of its name. LLM text and decoded
tool-call argument keys and values are checked recursively against declared
generator secrets before they enter run state.

The `web.search` agent tool is an explicit opt-in. Its contract declaration
contains only the selected `provider`, `max_results`, and `max_calls` in
addition to the ordinary tool identity fields. The selected `[[search_provider]]`
row in the unified `providers.toml` registry fixes the endpoint, request
mapping, response mapping, and authentication. There is no implicit search
profile or fallback: the tool must name a profile or the registry must declare
`[default].search`; no default ships enabled. The bundled `tinyfish-api` row is the API-key REST
profile; it is separate from the OAuth MCP profile named `tinyfish`.

The selected profile reads its API key from the configured `api_key_env` and
injects it into the configured authentication location at runtime. The profile
host must still be declared in `permissions.network`; registry configuration
does not grant a contract capability. Credentialed remote profiles require
HTTPS and never forward credentials through redirects. Missing profiles or
credentials fail explicitly. Search responses are size-bounded by the HTTP
runtime limit, normalized before being returned to the model, and labeled as
untrusted data. Search snippets can contain prompt-injection text and must
never be treated as instructions. Result URLs are citations, not an implicit
permission to fetch those pages.

The generic `mcp` agent tool is also explicit opt-in. Its contract fixes the
profile `server`, remote `tool`, per-tool call budget, and whether the operation
is expected to have side effects. Streamable HTTP profiles allow only their
declared `allowed_hosts`, require every one of those hosts in
`permissions.network`, and use HTTPS for non-loopback endpoints. Stdio profiles
allow only their exact command vector through `permissions.commands`; the child
does not receive the qcg process environment.

MCP `tools/list` metadata is untrusted. qcg strips descriptive schema
annotations before putting a server schema into an LLM tool definition, then
validates each model argument against the original schema. When a server
advertises `outputSchema`, its `structuredContent` is validated before the
result enters the next LLM turn. External schema references are rejected, so
schema validation cannot initiate undeclared network or filesystem access.
Tool results remain untrusted data (the agent
guardrail tells the model not to treat them as instructions) and are scanned
for declared secret values before they enter the next LLM turn.

MCP OAuth uses authorization-server discovery, PKCE, state validation, and
single-use callback state. The default `oauth_store = "keyring"` keeps access
and refresh credentials in the OS credential store; `memory` is an explicit
ephemeral alternative. Tokens are never serialized into registry files,
generator packages, prompts, journals, artifacts, or logs. OAuth discovery,
authorization, token, and registration requests are restricted to the profile's
`allowed_hosts`, with bounded response bodies and no cross-host redirects.
The bundled TinyFish MCP profile therefore does not use `TINYFISH_API_KEY`, but
it requires interactive OAuth authorization from the browser Connections panel
on a loopback qcg server.

The process-level MCP runtime shares the authorized credential/token manager by
profile, but every generator run creates an independent MCP protocol session.
Each session inherits the run cancellation token, applies the profile timeout
(120 seconds by default), caps response bodies at 4 MiB by default, and closes
when the run ends. MCP discovery is limited to 100 pages and individual input
and output schemas to 256 KiB, with additional nesting, node-count, width, and
string-size bounds before schema compilation. A contract's
`side_effects = true` binding is denied by
`permissions.side_effects = "none"`, or pauses for the normal HITL
confirmation under `confirm` / `dry_run_first`; only a reviewed `allowed`
policy executes it without confirmation. Confirmation journals summarize only
argument names and encoded size. Each MCP profile selects `initialize` or
`discover`. Known servers are pinned to their verified lifecycle, and qcg never
silently retries a different protocol lifecycle after a negotiation failure.
qcg advertises MCP Tasks, polls task completion
within the bounded profile timeout, and sends task cancellation when the run is
canceled. Multi-round-trip `input_required` responses are converted into the
same durable, journaled HITL boundary as contract questions, then resumed with
the server-provided request state. Deprecated client sampling is not exposed.

Header authentication is preferred. When an upstream API requires query
authentication, qcg appends the secret only after checking the public endpoint
against `permissions.network`, disables redirects, removes the sensitive
parameter from returned URLs and HTTP errors, and rejects credential reflection
in the decoded response.

These controls reduce generator capability; they do not authenticate HTTP
callers or provide run ownership. Review a contract before approving it, keep
generator packages from trusted sources, and do not use `--yes` for unreviewed
contracts.

Secret declarations are capabilities: a contract can deliberately materialize
a declared secret with `inject_secrets`. Permission summaries therefore show
both the logical secret name and its environment-variable name. Treat any
contract requesting a sensitive variable as code requesting that credential;
deny it unless the package and intended output are trusted.

Safe relative paths reject empty paths, absolute paths, NUL bytes, backslashes,
`.` / `..`, and empty components. Directory-backed assets are canonicalized
and must remain below the canonical generator root, including through symlinks.

## Browser surface

Generator assets are first-party UI served from the qcg origin. Asset responses
receive a restrictive policy allowing only same-origin script, style, images,
API/SSE connections, and the WASM evaluation mode needed by qcg's expression
runtime. JSON responses receive `default-src 'none'`. All responses use
`X-Content-Type-Options: nosniff` and `X-Frame-Options: DENY`.

CORS is off by default. Explicit origins may send `content-type` and
`idempotency-key`; cookies and credentialed CORS are not supported.

## File inputs and outputs

File inputs use the inline `FileValue` JSON shape and are limited to 16 MiB
after decoding. The canonical input is recorded in the journal before it is
materialized below the run workspace. Journals therefore contain file contents
and must be protected like other run data.

Artifacts are constrained by the output manifest. To send an artifact outside
qcg, declare a command permission and execute a reviewed script as a confirmed
side effect. qcg has no implicit upload or exfiltration API.
