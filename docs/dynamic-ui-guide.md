# Generator Assets and Custom UI Guide

A generator may ship browser files through the ordinary `[assets]` contract.
The backend treats `meta` as opaque JSON. Clients may adopt the convention
`meta.entry = "ui/index.html"`, but qcg assigns no semantic meaning to it.

```toml
[assets]
files = ["legal/NOTICE"]
dirs = ["ui"]
meta = { entry = "ui/index.html", theme = "light" }
```

`files` is a complete list. Every entry must exist when the contract loads.
`dirs` publishes safe relative subtrees and is checked when a request arrives,
so an unbuilt frontend directory does not invalidate a generator package and
returns 404 until built. File and directory declarations must not overlap or
nest.

Requested paths must be safe relative paths. qcg canonicalizes both the
generator root and the requested target and rejects symlink escapes.

## Same-origin application model

A custom UI is a first-party application, not an iframe protocol. It calls
root-absolute `/api/...` routes and subscribes to run events with a plain
`EventSource`. The generator detail supplies input stages, fields, and opaque
asset metadata. The frontend build generates the ignored expression WASM
package, which evaluates stage `when` conditions with the same semantics as
Rust. Until that package is generated, the development loader fails closed and
does not reveal conditional stages.

The bundled `generator` demonstrates generator discovery, staged forms,
FileValue inputs, SSE progress, questions, confirmations, artifact previews,
and ZIP download.

## Content Security Policy

Asset responses use:

```text
default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src 'self' data: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'
```

Keep scripts, styles, WASM, images, and API calls on the same origin. The
policy does not grant a `font-src`, so custom Web fonts are blocked. Inline
scripts and styles are not allowed. `frame-ancestors 'none'` and
`X-Frame-Options: DENY` prohibit embedding.

## Development and packaging

The source application lives in `frontend/generator`. Vite builds directly to
`generators/generator/ui` with `base: "./"`. That output is ignored in source
control and is built before distribution packaging:

```bash
npm --prefix frontend/generator ci
npm --prefix frontend/generator run generate:api
npm --prefix frontend/generator run generate:wasm
npm --prefix frontend/generator run check
npm --prefix frontend/generator test
npm --prefix frontend/generator run build
```
