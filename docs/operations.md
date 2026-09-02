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
non-owner services follow the durable journal for SSE delivery.

The server default is eight concurrently executing API runs. Set
`--max-active-runs` or `QCG_MAX_ACTIVE_RUNS` to change the process-local limit.
Accepted work above that limit remains queued durably. Runs waiting for user
input or side-effect confirmation release their slot until resumed. All runs in one process share the configured LLM and
search provider runtimes and provider HTTP clients, so provider quotas and
credentials are shared. MCP OAuth token managers are shared by profile, but
each run owns an independent MCP protocol session.

API runs use a UUID-based run ID and separate `meta/journal.jsonl` and
`workspace/` directories below `--runs-dir`. Direct runs also reject a second
concurrent invocation targeting the same output directory.

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
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50 --delete
```

GC never removes a non-terminal run. The first command is a dry run.

Journals include inline FileValue content and should be protected as sensitive
run data. `qcg runs show` summarizes file values by name, decoded bytes, and
SHA-256 instead of printing base64.

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
