# Verification scope

A passing suite establishes its checked cases, not absence of every defect.
Release readiness requires the gates below. Claims in the Capability Matrix
are matched against Cargo's registered tests and executed in CI.

| Surface | Gate / evidence | Scope |
|---|---|---|
| API and service | `cargo test --workspace --locked` | State transitions, leases, cancel/completion races, approvals, cursor replay, concurrent readers, queue ordering, artifact integrity |
| CLI | Workspace tests, `check-fixtures.sh`, `e2e-server-smoke.sh` | Contract validation, run output, installation, HTTP smoke |
| TypeScript/Python SDK | `check-sdk.sh`, `check-sdk-behavior.sh` | Generated freshness, common SSE wire fixtures, every byte split, redirects, response readers, cleanup |
| SPA | API/WASM generation, `npm run check`, `npm test` (local); `e2e-ui-playwright.mjs` (CI `Demo end-to-end` only) | Type safety, coalesced refresh, terminal snapshot recovery, reader release, interactive run flows, served assets and Vite proxy |
| Browser CORS | UI Playwright cross-origin case (CI `Demo end-to-end` only) | Actual preflight through Last-Event-ID/If-None-Match, EOF at terminal cursor, visible ETag/Content-Disposition/Content-Range/Accept-Ranges, 304, Range/If-Range 206 and unsatisfiable Range 416 |
| Release metadata | `node --test scripts/product.test.mjs` | Correct/incorrect tags, absent/ambiguous owning binary, product rename |
| Distribution | `check-dist-bundle.sh`, `verify-bundle.mjs`, `validate-spdx.py` | Extracted required files, Markdown links, SPA/WASM asset presence, full SPDX 2.3 validation, packaged CLI/server startup |
| Minimal features | `cargo test -p server --no-default-features`, MCP equivalent | Unsupported configuration refuses explicitly |
| OS | CI Rust and release matrix | Linux, macOS, Windows; Unix file-mode guarantees do not apply to Windows |
| Bounded read performance | [measurement protocol](performance-validation.md) | Read-view counters, HTTP snapshot/list and full-history SSE matrices; release measurements and 54 slow live-tail conditions in both store modes |
| Product comparison | [comparison protocol](comparison-protocol.md) | Deterministic real qcg/LangGraph/Temporal runs are recorded; Dify runs through a separate native-DSL adapter with the same input/output validator; real LLM comparison requires a common model credential |

Local validation uses Linux. A workflow definition is not evidence
that macOS/Windows jobs have executed; that coverage requires CI on those hosts.
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

SSE decoding follows the [WHATWG parsing rules](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream): raw frames dispatch an empty `data:` or bare `data` line as `""` (one leading space after the colon is stripped, multiple data lines join with LF, and a dispatch with no data field dispatches nothing). The public SDK projections (`streamRunEvents` / `stream_run_events`) parse only non-empty data strings as JSON, so empty raw frames are dispatched at the parser level but never surface as JSON events.
OTLP malformed nonempty success responses are protocol failures; partial
success advances accepted records without resending the complete batch, as
required by the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/#partial-success).

npm verification includes development dependencies. Vite/Vitest/esbuild/
Playwright/Hey API execute on developer or CI machines. Svelte
runtime and compiled application/WASM ship in the SPA; the build tools and
Node modules do not ship as server endpoints. `npm audit --audit-level=moderate`
is a required gate, with the lockfile reviewed instead of force-upgrading
outside parent compatibility constraints.

## OpenAPI declaration generation

Both API generation entry points use `frontend/generator/scripts/openapi-types.mjs`:
unmodified `@hey-api/openapi-ts` 0.97.1 translates schemas, then qcg adapts its
TypeScript AST into the legacy `paths`, `webhooks`, `components`, `$defs` and
`operations` exports. TypeScript is pinned to 5.9.3; the 7.x peer is incompatible
with this generator's `NewLineKind.LineFeed` usage. Generated declarations carry
no runtime dependency. SDK method names and runtime readers remain separately
pinned by `scripts/generate-sdk.mjs`.

The adapter accepts qcg's OpenAPI 3.1 subset: local component schema/response
references, the configured HTTP bearer security scheme, inline query/header/path/cookie
parameters, request bodies, response headers/media, operation IDs, primitive/object/array schemas, nullable type
unions, enums/constants and `oneOf`/`anyOf`. Defaulted properties remain required;
boolean-true schemas and unconstrained media remain `unknown`; binary strings
remain `string`. Validation-only bounds, patterns, formats and negated `required`
constraints are not runtime validators (nor were they in the legacy types).
Security/server/description metadata does not contribute declaration fields.
Unknown keywords, external/unresolved references, boolean-false schemas,
`allOf`/conditionals/discriminators, path-level parameters, nonempty webhooks,
root `$defs`, non-bearer security schemes, and component parameters/requestBodies/
headers/pathItems fail explicitly before generation. They are not silently erased
or replaced with `unknown`; expanding this subset requires a compatibility review.

`npm run test:api` checks bidirectional structural parity for all five exports
against the checked-in baseline and against an independently described synthetic
mutation (operation ID, required enum query, referenced response, defaulted field
and boolean-true field). It also compiles the SDK and frontend API client under
strict TypeScript and tests unsupported-input rejection. `check-sdk.sh` checks
SDK freshness and every SDK TypeScript file; `npm run check:api` independently
regenerates the server OpenAPI and shared types. The required online audit and
full frontend checks/build remain CI gates; local offline generation is not
security-advisory evidence.

## Test maintenance policy

Every continuing test should name the behavior it protects and exercise the
production code responsible for it. Prefer structured events, observable state
transitions, effects, and read counts over incidental error text or source-code
spelling. Keep separate tests where OS, feature selection, transport, or failure
mode changes the guarantee. Generated freshness checks remain separate from
runtime behavior checks.

Confirmation UI tests render the actual panel and check scope messaging,
binding display, and enabled actions. MCP continuation identity and rejection
are checked against the real Rust continuation store, rather than a fixed key
and a separate regular expression. Fork execution probes count actual snapshot
uses and disk fallbacks; the pure snapshot derivation test also removes the
journal. Broadcast overflow uses a small real channel and a question as a
synchronization point, requiring lag and cursor-based replay rather than
accepting either lag or a direct terminal event. Secret sentinel tests and real agent execution checks protect emitted
values; lexical bans on Debug formatting are not treated as security evidence.

The local full pipeline installs frontend dependencies once and reuses the
frontend just built (`npm run build`) for distribution verification.
`check-demo-local.sh --dependencies-installed` requires an existing dependency
installation. `check-dist-bundle.sh --frontend-built` (forwarded to packaging)
requires freshly generated API/WASM and a freshly built frontend. Standalone
packaging builds these inputs by default. Rust OS CI forwards
`--dependencies-installed` to distribution verification after its dependency
installation; API/WASM generation and frontend building still run there.
End-to-end CI builds the frontend
inside its browser runner once, after generating API/WASM inputs.

## Measured timing bounds

The container tests' wall-clock bounds are sized from measurements. On
2026-10-21 the daemon-policy test's stop-to-removal gap was measured from
the fake daemon's own arrival stamps, over the four teardown paths of both
backends: the worst observed gap was 2 s and the usual one 1 s, over 122
observations taken on an unpinned runner, on one pinned CPU, on one CPU
with eight competing processes, and with the whole container and MCP suites
running in parallel on one CPU. `STOP_TIMEOUT_GAP_BOUND` is 4 s, twice the
worst observed gap and below the fake daemon's eight-second hang, which it
separates from the teardown that honors the frozen stop timeout.

Joining the container's outstanding cleanups was measured the same way:
1.0 s to 2.1 s in the same runs and 4.06 s at worst, over 72 joins taken
with the whole container suite running in parallel on one CPU.
`CLEANUP_JOIN_BOUND` is 10 s, more than twice that worst case and above the
fake daemon's eight-second hang.

A bound that no longer fits its measurement is a defect in either
direction: raising one to keep a test green hides the regression it exists
to catch, and lowering one below the measured window turns a slow runner
into a red build. Re-measure after a change to the container's cleanup path,
and record the observation beside the bound it sizes.
