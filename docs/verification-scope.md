# Verification scope

A passing suite establishes its checked cases, not absence of every defect.
Release readiness requires the gates below. Claims in the Capability Matrix
are matched against Cargo's registered tests and executed in CI.

| Surface | Gate / evidence | Scope |
|---|---|---|
| API and service | `cargo test --workspace --locked` | State transitions, leases, cancel/completion races, approvals, cursor replay, concurrent readers, queue ordering, artifact integrity |
| CLI | Workspace tests, `check-fixtures.sh`, `e2e-server-smoke.sh` | Contract validation, run output, installation, HTTP smoke |
| TypeScript/Python SDK | `check-sdk.sh`, `check-sdk-behavior.sh` | Generated freshness, common SSE wire fixtures, every byte split, redirects, response readers, cleanup |
| SPA | API/WASM generation, `npm run check`, `npm test`, `e2e-ui-playwright.mjs` | Type safety, coalesced refresh, terminal snapshot recovery, reader release, interactive run flows, served assets and Vite proxy |
| Browser CORS | UI Playwright cross-origin case | Actual preflight through Last-Event-ID/If-None-Match, EOF at terminal cursor, visible ETag/Content-Disposition/Content-Range/Accept-Ranges, 304, Range/If-Range 206 and unsatisfiable Range 416 |
| Release metadata | `node --test scripts/product.test.mjs` | Correct/incorrect tags, absent/ambiguous owning binary, product rename |
| Distribution | `check-dist-bundle.sh`, `verify-bundle.mjs`, `validate-spdx.py` | Extracted required files, Markdown links, SPA/WASM asset presence, full SPDX 2.3 validation, packaged CLI/server startup |
| Minimal features | `cargo test -p server --no-default-features`, MCP equivalent | Unsupported configuration refuses explicitly |
| OS | CI Rust and release matrix | Linux, macOS, Windows; Unix file-mode guarantees do not apply to Windows |
| Bounded read performance | [measurement protocol](performance-validation.md) | Read-view counters, HTTP snapshot/list and full-history SSE matrices; release measurements and 54 slow live-tail conditions in both store modes |
| Product comparison | [comparison protocol](comparison-protocol.md) | Deterministic real qcg/LangGraph/Temporal runs are recorded; Dify runs through a separate native-DSL adapter with the same input/output validator; real LLM comparison requires a common model credential |

Local validation in this change uses Linux. A workflow definition is not evidence
that macOS/Windows jobs have executed; those remain pending CI on those hosts.
Environment mutation tests execute in isolated subprocesses with 60-second
whole-test deadlines. OTLP configuration validation injects values directly.
Socket-dependent suites require permission to bind local loopback listeners.

Distribution checks use the [official SPDX tools](https://github.com/spdx/tools-python)
with the complete Python dependency versions pinned in
`scripts/requirements-spdx.txt`. Install them in a virtual environment before
running `check-demo-local.sh` or `check-dist-bundle.sh`; `check-ci-local.sh`
creates a temporary environment automatically. License declarations preserve
metadata, converting Cargo's deprecated slash alternatives to `OR`; concluded
licenses remain `NOASSERTION` because this inventory does not audit source licenses.

SSE decoding follows the [WHATWG parsing rules](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream).
OTLP malformed nonempty success responses are protocol failures; partial
success advances accepted records without resending the complete batch, as
required by the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/#partial-success).

npm verification includes development dependencies. Vite/Vitest/esbuild/
Playwright/openapi-typescript execute on developer or CI machines. Svelte
runtime and compiled application/WASM ship in the SPA; the build tools and
Node modules do not ship as server endpoints. `npm audit --audit-level=moderate`
is a required gate, with the lockfile reviewed instead of force-upgrading
outside parent compatibility constraints.
