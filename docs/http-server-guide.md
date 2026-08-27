# HTTP Server and Sister-Product Composition

Start qcg on loopback:

```bash
qcg serve --bind 127.0.0.1 --port 8080 \
  --generators-dir generators --runs-dir /var/lib/qcg/runs
```

The API is intentionally unauthenticated. A non-loopback bind emits a warning.

## Responsibilities

| qid | qpx | qcg |
| --- | --- | --- |
| Issues identity tokens and owns authorization-server behavior. | Terminates TLS, validates JWTs or introspects tokens, enforces access policy, and proxies accepted requests. | Loads generator contracts, executes bounded runs, records journals, and serves declared assets and artifacts. |

The deployment chain is `qid -> qpx -> qcg`: clients obtain a token from qid,
qpx validates it, and qpx forwards accepted HTTP traffic to qcg on
`127.0.0.1`. Integration is only at the binary and HTTP boundary; qcg has no
build-time or runtime dependency on either sister product.

## Routes

The server exposes 13 logical route groups, represented by 15 paths and 16 HTTP
operations:

- `GET /healthz`
- `GET /api/openapi.json`
- `GET /api/generators`
- `GET /api/generators/{id}`
- `GET /api/generators/{id}/assets/{path...}`
- `GET, POST /api/runs`
- `GET /api/runs/{id}`
- `PUT /api/runs/{id}/questions/{qid}`
- `PUT /api/runs/{id}/confirmations/{cid}`
- `POST /api/runs/{id}:cancel`
- `GET /api/runs/{id}/events`
- `GET /api/runs/{id}/artifacts`, `GET /api/runs/{id}/artifacts/{path...}`, and `GET /api/runs/{id}/artifacts.zip`
- `GET /api/runs/{id}/journal`

`POST /api/runs` accepts `generator_id` and an `inputs` object. An
`Idempotency-Key` header makes retries deterministic for 24 hours.
The `{path...}` notation above denotes the nested wildcard captured by the
server; the OpenAPI document names that wildcard parameter `path`.

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

## External delivery pattern

qcg does not add a product-specific artifact upload API. A generator that must
send an artifact declares the exact command shape in `permissions.commands`
and runs a reviewed script with `side_effects = "confirm"`. The confirmation
keeps network delivery explicit and journaled.

## CORS and events

CORS is disabled unless one or more exact `--cors-origin` values are supplied.
Allowed request headers are `content-type` and `idempotency-key`; credentialed
CORS is not enabled. Run events use SSE and support `Last-Event-ID` replay.
