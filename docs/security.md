# Security Model

qcg is an unauthenticated generator runtime intended to listen on loopback. It
is not an identity provider, authorization server, TLS endpoint, or public
policy-enforcement point. The default bind address is `127.0.0.1`; qcg logs a
warning when configured on a non-loopback address.

## Trust boundary

Anyone who can reach the HTTP listener can list generators, start and control
runs, read journals, and download artifacts. Do not expose qcg directly to an
untrusted network.

For production, use this binary boundary:

```text
client -> qpx (TLS + token enforcement) -> 127.0.0.1 qcg
             ^
             |
          qid issues tokens
```

qcg does not import, link to, or invoke qid or qpx. The products remain
independently buildable and composable over HTTP.

## Contract sandbox

Generator capabilities are denied unless declared. Contracts bound filesystem
paths, commands and argument shapes, network hosts, containers, secrets, side
effects, runtime, and budget. Side effects require confirmation when declared
with `side_effects = "confirm"`.

Provider credentials are read from named environment variables and sent only
in provider request headers. Generator commands run with a cleared environment
containing only `PATH` and a run-local `TMPDIR`; they do not inherit provider
credentials or other parent-process state. Credential variables named by
loaded provider registry rows are reserved and cannot be redeclared as
generator secrets. Credentialed remote providers require HTTPS, do not follow
redirects, and fail closed when an upstream response contains the exact active
credential in its raw or JSON-decoded content.
Provider URLs and query parameters cannot interpolate the configured
credential environment variable, regardless of its name. LLM text and decoded
tool-call argument keys and values are checked recursively against declared
generator secrets before they enter run state.

These controls reduce generator capability; they do not authenticate HTTP
callers. Review a contract before approving it, keep generator packages from
trusted sources, and do not use `--yes` for unreviewed contracts.

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
