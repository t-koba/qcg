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
