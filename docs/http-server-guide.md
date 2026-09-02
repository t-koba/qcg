# HTTP Server and Sister-Product Composition

Start qcg on loopback:

```bash
qcg serve --bind 127.0.0.1 --port 8080 \
  --generators-dir generators --runs-dir /var/lib/qcg/runs \
  --max-active-runs 8
```

qcg listens on the selected address without forcing authentication. When
`--api-token` or `QCG_API_TOKEN` is set, clients send `Authorization: Bearer
<token>`. This authenticates an instance boundary, not individual run ownership.

## Responsibilities

| qid | qpx | qcg |
| --- | --- | --- |
| Issues identity tokens and owns authorization-server behavior. | Terminates TLS, validates JWTs or introspects tokens, enforces access policy, and proxies accepted requests. | Loads generator contracts, executes bounded runs, records journals, and serves declared assets and artifacts. |

The deployment chain is `qid -> qpx -> qcg`: clients obtain a token from qid,
qpx validates it, and qpx forwards accepted HTTP traffic to qcg on
`127.0.0.1`. Integration is only at the binary and HTTP boundary; qcg has no
build-time or runtime dependency on either sister product.

qcg does not read caller identity, attach an owner or tenant to a run, or
filter run resources by user. Authentication at qpx therefore does not by
itself provide per-user isolation. Use qcg for trusted shared use, or enforce
separation outside qcg with a separate service and runs directory per trust
domain.

## Concurrency and run storage

The server accepts at most eight concurrently executing API runs by default.
Set `--max-active-runs` or `QCG_MAX_ACTIVE_RUNS` to change the limit. When no
execution slot is available, accepted starts and resumed interactions remain
in `queued` state until capacity becomes available. A run in `waiting` or
`confirming` HITL state has stopped its engine task and does not consume a
slot; answering or approving it re-enters the durable queue.

The limit is process-local. The service creates one LLM and MCP runtime at
startup. LLM HTTP clients and MCP OAuth credential/token managers are shared by
profile across runs, while every run creates an independent MCP protocol
session. Provider-side rate limits and credentials remain shared resources.

Every API run gets a UUID-based ID and an isolated directory under
`--runs-dir`:

```text
<runs-dir>/<generator-id>-<uuid>/meta/journal.jsonl
<runs-dir>/<generator-id>-<uuid>/workspace/
```

The default `--run-store exclusive` takes one filesystem lock on `--runs-dir`.
`--run-store shared-filesystem` allows multiple services on storage with
reliable advisory locking. A run-level lease admits exactly one executor;
services rescan every 5 seconds to recover work after an owner exits, and SSE
polls the durable journal when the owner is another process. Process-local
`--max-active-runs` limits still need external capacity planning.

## Routes

The server exposes health, Prometheus metrics, OpenAPI, generator, MCP, run,
event, artifact, and journal routes. Principal paths include:

- `GET /healthz`
- `GET /metrics` (Bearer-protected when a token is configured)
- `GET /api/openapi.json`
- `GET /api/generators`
- `GET /api/generators/{id}`
- `GET /api/generators/{id}/assets/{path...}`
- `GET /api/mcp/servers`
- `POST, DELETE /api/mcp/servers/{id}/authorization`
- `DELETE /api/mcp/servers/{id}/authorization/pending`
- `GET /api/mcp/oauth/callback`
- `GET, POST /api/runs`
- `GET /api/runs/{id}`
- `PUT /api/runs/{id}/questions/{qid}`
- `PUT /api/runs/{id}/confirmations/{cid}`
- `POST /api/runs/{id}:cancel`
- `GET /api/runs/{id}/events`
- `GET /api/runs/{id}/artifacts`, `GET /api/runs/{id}/artifacts/{path...}`, and `GET /api/runs/{id}/artifacts.zip`
- `GET /api/runs/{id}/journal`

`POST /api/runs` accepts `generator_id` and an `inputs` object. An
`Idempotency-Key` header makes retries deterministic for 24 hours within the
service process.
The cancel response is returned after the active engine task has stopped. If a
run committed completion before the cancellation request won the race, the
response preserves that completed state instead of writing a second terminal
event.
The `{path...}` notation above denotes the nested wildcard captured by the
server; the OpenAPI document names that wildcard parameter `path`.

## MCP authorization

`GET /api/mcp/servers` lists the configured MCP server IDs, transport,
authentication mode, and whether an OAuth profile is authorized. For an OAuth
profile, `POST /api/mcp/servers/{id}/authorization` starts the authorization
code + PKCE flow and returns an authorization URL. Open that URL in the
browser, then the provider redirects to `GET /api/mcp/oauth/callback`. qcg
validates the single-use state, exchanges the code, and stores the resulting
credentials in the OS keyring by default. `DELETE` on the authorization path
clears the profile's stored OAuth credentials and any pending authorization.
`DELETE` on its `/pending` subpath cancels only an in-progress browser flow, so
it cannot erase credentials from a callback that won the completion race.

The authorization endpoints are available only when qcg listens on a loopback
address and reject a different request `Origin`. The callback is intentionally
small HTML so the browser can close it after the SPA observes the authorized
status. A callback state expires after ten minutes. The profile's
`allowed_hosts` and the contract's `permissions.network` continue to restrict
OAuth and MCP traffic; an OAuth authorization URL is never accepted merely
because it came from a configured server.

## FileValue

File fields are inline and have exactly one content representation:

```json
{"name":"settings.json","text":"{\"enabled\":true}"}
```

or:

```json
{"name":"logo.png","content_base64":"iVBORw0KGgo="}
```

`name` must be one safe filename component. `text` and `content_base64` are
mutually exclusive. Decoded content is limited to 16 MiB and is supplied inline
with the run request.

## Assets and frontend development

The bundled SPA is available from the normal assets API after it is built:

```text
http://127.0.0.1:8080/api/generators/generator/assets/ui/index.html
```

For source development:

```bash
QCG_API_TARGET=http://127.0.0.1:8080 \
  npm --prefix frontend/generator run dev -- --host 127.0.0.1 --port 5173
```

The frontend uses root-absolute `/api/...` requests in both modes. Vite proxies
them during development; production assets share qcg's origin.

The bundled generator's Connections panel uses the MCP endpoints above. It
does not receive, store, or display access tokens. For headless runs, complete
OAuth authorization once through the loopback server before starting a
generator that binds an OAuth MCP tool.

## External delivery pattern

qcg does not add a product-specific artifact upload API. A generator that must
send an artifact declares the exact command shape in `permissions.commands`
and runs a reviewed script with `side_effects = "confirm"`. The confirmation
keeps network delivery explicit and journaled.

## CORS and events

CORS is disabled unless one or more exact `--cors-origin` values are supplied.
Allowed request headers are `content-type` and `idempotency-key`; credentialed
CORS is not enabled. Run events use SSE and support `Last-Event-ID` replay.
