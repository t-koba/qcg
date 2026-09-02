# qcg

qcg is a contract-driven harness for bounded, purpose-specialized generation. A
generator is a directory containing a declarative `qcg.toml` plus optional
prompts, templates, resources, and browser assets. Each generator can use only
the inputs, resources, steps, permissions, and outputs declared by its
contract. Generated artifacts are not limited to configuration: a contract may
produce source code, documents, media, archives, deployment inputs, or any
other declared file while retaining the same capability, budget, provenance,
and human-approval boundaries.

LLM, web-search, and generic MCP connection profiles are configured in the
unified `providers.toml` registry. MCP tools are declared by a generator and
resolved at run time from the selected profile. The built-in `exa-public` and
`parallel-public` Streamable HTTP profiles are available even without a
registry file and provide anonymous public search and fetch without setup; the
demo offers both together as its recommended research choice. The `tinyfish`
profile remains an OAuth alternative, and
`tinyfish-api` is a separate API-key-based REST profile. See the
[provider guide](docs/llm-provider-guide.md) and
[contract reference](docs/contract-reference.md).

Web search is never enabled implicitly. A contract must explicitly declare
either `web.search` for a registry-defined REST search mapping or `mcp` for a
provider's MCP search tool, and must grant the corresponding hosts in
`permissions.network` (or the exact MCP command in `permissions.commands`).

## Quick start

When running from the source tree, build the generated SPA assets first:

```bash
npm --prefix frontend/generator ci
npm --prefix frontend/generator run generate:api
npm --prefix frontend/generator run generate:wasm
npm --prefix frontend/generator run build
```

Then start the unauthenticated loopback server (the default execution limit is
eight active API runs):

```bash
cargo run -p qcg -- serve --bind 127.0.0.1 --port 8080 \
  --runs-dir /tmp/qcg-runs \
  --max-active-runs 8
```

The `generator` demo is served through its normal `[assets]` contract.
Distribution archives contain the built SPA; source checkouts require the
build above. Open this URL after the server starts:

```text
http://127.0.0.1:8080/api/generators/generator/assets/ui/index.html
```

For frontend development, run the source SPA through Vite's API proxy:

```bash
npm --prefix frontend/generator ci
npm --prefix frontend/generator run generate:api
npm --prefix frontend/generator run generate:wasm
QCG_API_TARGET=http://127.0.0.1:8080 \
  npm --prefix frontend/generator run dev -- --host 127.0.0.1 --port 5173
```

Every selected bind address is used as requested. Authentication is optional;
set `--api-token` or `QCG_API_TOKEN` when instance-level bearer protection is
wanted. The token protects the whole instance and does not create per-user run
ownership. In production,
place qpx in front for TLS and identity enforcement,
with tokens issued by qid; qpx or a separate service boundary must enforce any
per-user or per-tenant separation. qcg has no build-time or runtime dependency on
either sister product.

OAuth-backed MCP profiles are authorized from the bundled SPA's Connections
panel while qcg is listening on loopback. The callback stores OAuth
credentials in the operating-system keyring; qcg does not put access or refresh
tokens in `providers.toml`, generator packages, prompts, journals, or
artifacts. Each run opens its own bounded MCP connection while the process-level
credential manager may share and refresh the profile token safely. Each MCP
profile selects `initialize` or `discover`; known public profiles are pinned to
their verified lifecycle. The client maps MCP task
and input-required flows onto cancellation-safe, durable run execution.

Each API run receives a UUID-based ID and its own workspace and journal below
`--runs-dir`. The default run store exclusively owns that directory;
`--run-store shared-filesystem` enables multiple services using run-level
execution leases and periodic abandoned-run recovery on storage with reliable
advisory locks. `--max-active-runs` (also `QCG_MAX_ACTIVE_RUNS`)
defaults to 8. Additional accepted runs enter the durable FIFO execution queue
instead of failing under temporary saturation. A run waiting for human input
or confirmation releases its execution slot until it resumes. Queued and active
runs resume from their verified journal after a service restart.

`fixtures/generators/` contains test-only fixtures and is never bundled.
Installed generator packages under `--generators-dir` appear alongside the
bundled demo, with installed IDs taking precedence.

## Documentation

- [Contract reference](docs/contract-reference.md)
- [Built-in generator](docs/built-in-generators.md)
- [DAG flow guide](docs/dag-flow-guide.md)
- [Generator assets and custom UI guide](docs/dynamic-ui-guide.md)
- [HTTP server and proxy guide](docs/http-server-guide.md)
- [Security model](docs/security.md)
- [External process guide](docs/external-process-guide.md)
- [Tool backend guide](docs/tool-backend-guide.md)
- [LLM provider guide](docs/llm-provider-guide.md)
- [Agent and MCP verification matrix](docs/agent-mcp-verification.md)
- [Operations guide](docs/operations.md)
- [CLI reference](docs/cli-reference.md)
