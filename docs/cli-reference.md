# CLI Reference

Global options may precede any subcommand:

- `--verbose`: use `qcg=debug` as the default log filter.
- `--log-format text|json`: select human-readable or JSON logs.
- `--providers <PATH>`: use this authoritative LLM provider registry path for
  validation, direct runs, replay, and the server.
- `--help` and `--version`: print command help or version.

`RUST_LOG` overrides the default filter. Logs go to stderr; command results and
JSON event output go to stdout.

## Generator commands

- `qcg validate <path>` loads the contract, checks the graph, validates every
  registered step's closed `params`, and prints the contract digest.
- `qcg run <generator> [--input k=v]... [--input-file k=path]...
  [--inputs-file file.json]
  [--answer id=value]... [--output dir] [--yes] [--json]` runs a package.
  `--yes` approves confirmations; use only for a reviewed contract.
- `qcg list [--generators-dir dir]` lists available packages from the installed
  directory and the bundled generator directory. Installed IDs take precedence.
- `qcg package <dir> [-o package.qcg]` creates a `.qcg` ZIP from a directory.
  Run `qcg validate <dir>` separately before packaging.
- `qcg install <path-or-url> [--generators-dir dir] [--yes] [--force]` stages,
  validates, summarizes permissions, and installs a package. URL installs do
  not verify a checksum or signature; verify provenance out of band.
- `qcg uninstall <id> [--generators-dir dir] [--yes]` removes one installed
  generator ID after checking that the ID is a safe relative path.

## Run commands

- `qcg runs list [--runs-dir dir]`
- `qcg runs show <id> [--runs-dir dir] [--json]`
- `qcg runs replay <id> [generator] [--runs-dir dir] [--output dir]
  [--reuse-seed] [--json]`
- `qcg runs gc [--runs-dir dir] [--keep 50] [--delete]`

GC is a dry run without `--delete` and never removes a non-terminal run.

## Server and generated documentation

- `qcg serve [--bind 127.0.0.1] [--port 0]
  [--generators-dir dir] [--runs-dir dir]
  [--cors-origin origin]...`
- `qcg docs step-schemas`
- `qcg docs run-events`
- `qcg docs openapi`

Port `0` selects an available port. `qcg serve` exposes REST, SSE, and assets
declared by generator contracts; it does not open a browser. Repeat
`--cors-origin` for multiple exact frontend origins; a comma-separated
`QCG_CORS_ORIGIN` environment variable is also accepted.

## Environment variables

- `RUST_LOG`: tracing filter, for example `qcg=debug,qcg_engine=trace`.
- `QCG_AUTO_GC=0|false|off`: disable serve's periodic retention task.
- `QCG_GENERATORS_DIR`: default directory for generator list/install/uninstall
  and serve; the per-command `--generators-dir` option overrides it.
- `QCG_PROVIDERS`: default path to the LLM providers registry; the global
  `--providers <PATH>` option overrides it. See
  `docs/llm-provider-guide.md`.
- `QCG_CORS_ORIGIN`: comma-separated exact origins allowed for cross-origin
  API requests. CORS is disabled when it is unset.
