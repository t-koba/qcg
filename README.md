# qcg

qcg is a contract-driven runtime for bounded configuration generators. A
generator is a directory containing a declarative `qcg.toml` plus optional
prompts, templates, resources, and browser assets. Each generator can use only
the inputs, resources, steps, permissions, and outputs declared by its
contract.

LLM providers are configured in `providers.toml`. The registry includes local
OpenAI-compatible endpoints and commented templates for hosted providers,
including OpenRouter. See the [LLM provider guide](docs/llm-provider-guide.md).

## Quick start

When running from the source tree, build the generated SPA assets first:

```bash
npm --prefix frontend/generator ci
npm --prefix frontend/generator run generate:api
npm --prefix frontend/generator run generate:wasm
npm --prefix frontend/generator run build
```

Then start the unauthenticated loopback server:

```bash
cargo run -p qcg -- serve --bind 127.0.0.1 --port 8080 \
  --runs-dir /tmp/qcg-runs
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

`qcg` intentionally provides no authentication. Keep it on loopback for local
use. In production, place qpx in front for TLS and identity enforcement, with
tokens issued by qid. qcg has no build-time or runtime dependency on either
sister product.

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
- [Custom extension guide](docs/custom-extension-guide.md)
- [Tool backend guide](docs/tool-backend-guide.md)
- [LLM provider guide](docs/llm-provider-guide.md)
- [Operations guide](docs/operations.md)
- [CLI reference](docs/cli-reference.md)
