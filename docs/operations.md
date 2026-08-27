# Operations Guide

## Distribution layout

Keep `bin/qcg` next to `share/qcg/`. The bundle contains the `generator` demo,
provider registry, documentation, third-party notices, and SBOM. Test fixtures
and frontend source are not distributed.

Validate a bundle and start the loopback server:

```bash
./bin/qcg validate ./share/qcg/generators/generator
./bin/qcg serve --bind 127.0.0.1 --port 8080 \
  --runs-dir /var/lib/qcg/runs
```

Open `/api/generators/generator/assets/ui/index.html` for the bundled SPA.

## Production topology

Do not expose qcg directly. qid issues tokens, qpx terminates TLS and enforces
identity, and qpx proxies accepted requests to the loopback qcg listener. The
three products remain independently deployed binaries.

## Run retention

`qcg serve` periodically retains the newest 50 terminal run directories. Set
`QCG_AUTO_GC=0` to disable automatic retention and use:

```bash
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50
qcg runs gc --runs-dir /var/lib/qcg/runs --keep 50 --delete
```

GC never removes a non-terminal run. The first command is a dry run.

Journals include inline FileValue content and should be protected as sensitive
run data. `qcg runs show` summarizes file values by name, decoded bytes, and
SHA-256 instead of printing base64.

## Verification

From the source tree, run:

```bash
bash scripts/check-ci-local.sh
bash scripts/check-demo-local.sh
```

The demo check generates OpenAPI and WASM bindings, validates the frontend,
runs browser tests against both the Vite proxy and assets served by qcg, and
checks the distribution bundle.

When a qpx binary is available, verify the documented deployment boundary with:

```bash
cargo build -p qcg
QPXD_BIN=/path/to/qpxd bash scripts/e2e-qpx-smoke.sh
```

## Artifact delivery

External delivery is modeled as a generator command, not a server feature.
Declare the exact script invocation in `permissions.commands`, declare the
required network host, and mark the step `side_effects = "confirm"`. Operators
then review and approve the delivery at the same HITL boundary as any other
side effect.
