# Tool Backend Guide

Logical tools describe intent. Backends describe execution environments. A
flow node should depend on the logical tool name, not on `host`, `bundled`, or
`container` directly.

```toml
[tools.qpx_validate]
kind = "validator"
input = "qpx.yaml"
command = ["qpxd", "check", "--config", "{input}"]
network = "none"
workspace = "read_only"
timeout_seconds = 30

[tools.qpx_validate.resolution]
allowed_backends = ["bundled", "container", "host"]
preferred_backends = ["bundled", "container", "host"]
fallback = "explicit"

[tools.qpx_validate.backends.bundled]
bin = "resources/bin/{{ os }}/{{ arch }}/qpxd"
sha256 = "..."

[tools.qpx_validate.backends.container]
image = "ghcr.io/t-koba/qpxd@sha256:..."
mount = "/work"

[tools.qpx_validate.backends.host]
bin = "qpxd"
version_command = ["qpxd", "--version"]
```

`fallback = "explicit"` means qcg must not silently switch from a safer
backend to host execution. Interactive runs can ask for confirmation. CI and
non-interactive runs fail unless a permitted fallback has already been made
explicit.

## Backend Contract

All backends share the same safety contract:

- declared filesystem scope
- declared network policy
- a cleared command environment
- timeout and output-size limits
- exact host/bundled binary or container-image identity
- side-effect confirmation and journal records

The supported backend families are `host`, `bundled`, and `container`.
