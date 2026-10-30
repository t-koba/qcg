# CLI HTTP operations

Registry index fetches, remote package downloads, and `qcg runs trace`
exports use the CLI-owned client in `src/http.rs`:

- Each request has a **120-second total timeout**, including connection,
  redirects and response-body consumption (when consumed).
- At most **three redirects** are followed. Every target must keep the initial
  URL's host and userinfo (username and password); ports may change. Same-host
  HTTP-to-HTTP, HTTP-to-HTTPS, and HTTPS-to-HTTPS redirects are allowed, including
  across ports. Once any hop uses HTTPS, no later hop may use HTTP. Cross-host
  redirects, HTTPS downgrades, and userinfo changes fail the command.
- Remote registry indexes have a **16 MiB** cap, reusing
  `policy::DEFAULT_HTTP_BODY_LIMIT_BYTES`. Both advertised Content-Length and
  streamed bytes are checked before buffering beyond the cap. Local `file://`
  indexes retain their existing filesystem behavior.
- Remote package bytes retain the explicit `PackageLimits.max_archive_bytes`
  bound (`--max-archive-bytes` / `PACKAGE_MAX_ARCHIVE_BYTES`), checked from
  Content-Length and during download. Omission still means no archive-byte
  cap: this is distinct from the default expanded-size and entry-count gates,
  which defend against decompression bombs. Local package handling is unchanged.
- Trace export checks the response status and does not buffer its body.

Initial endpoint support is unchanged: registry indexes use `file://` or
`https://`, direct remote packages allow `http://` and `https://`, and trace
exports use the operator-supplied endpoint. These transport defaults do not
change the service's package limits or the run's HTTP-step policies.

## Standalone subscription authentication

`src/cli/auth.rs` and its private `auth/oauth.rs` adapter own the ChatGPT
credential lifecycle. They are deliberately separate from the general CLI HTTP
client and from API-key provider routing. OAuth HTTP uses reqwest 0.13 through
openidconnect 4.0.1's HTTP interface (no library-default HTTP client), with a
30-second request timeout, no redirects, same-issuer-origin endpoints and a
256 KiB response-body bound enforced on advertised and streamed bytes.
Callback requests have an 8 KiB bound and a five-second read timeout.

See [CLI authentication](../../docs/cli-reference.md#chatgpt-subscription-credentials-standalone)
for explicit account selection, keyring persistence, callback, refresh and logout
behavior. `cargo test -p cli --bin qcg cli::auth --locked --offline` uses only
loopback protocol mocks and a memory credential store; the ignored `lock_child`
test is invoked by the cross-process-lock test as a subprocess helper. No tests
contact OpenAI, use real credentials or establish subscription eligibility.
