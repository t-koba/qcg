# Product comparison protocol and measured scope

The deterministic comparison executes actual qcg, LangGraph, Temporal and Dify
engines with the same canonical JSON input and exact output-byte validator.
One recorded run (2026-10-03 Japan time) used two warmups and thirty measured
trials per product/case; the tables below summarize it. Raw per-trial
outcomes, including failures, are produced by `scripts/comparison/run.py`
(see `scripts/comparison/README.md`) and kept with the release process
rather than shipped in the install bundle. These measurements establish the
tested behavior, not overall product superiority.

## Common cases and interpretation

| Case | Checked observation |
|---|---|
| Generation | The result parses as JSON and exactly matches the canonical expected bytes and SHA-256 digest |
| Approval and resume | Execution pauses at the human gate before the final effect; approval resumes to the validated result |
| Restart at pending approval | Persistent pending state survives the specified interruption; completed preparation is not repeated and approval finishes the result |

qcg and Temporal worker processes receive SIGKILL at the pending gate. Temporal's
persistent development service remains available. LangGraph creates a fresh
client process and SQLite checkpoint connection after interrupt. Dify's API
container receives SIGKILL and is explicitly started again; PostgreSQL, Redis
and the worker remain running. These are distinct fault scopes. They do not
measure whole-cluster outages, uncertain external side effects or identical
recovery architectures.

qcg, LangGraph and Temporal adapters create a preparation file exclusively,
then a final JSON file; repeated preparation fails. Dify imports its native
DSL 0.7.0, checks that preparation finishes once, uses the native human-input
form and validates the final output variable. Dify's output variable is not a
filesystem artifact registry, so digest agreement does not establish equivalent
artifact APIs. Approval time excludes deliberate human think time.

qcg additionally refuses a filesystem write without the declared permission
before the effect. LangGraph and Temporal permissions depend on the application
and worker host; no matching built-in declarative allowlist was configured.
Dify's broader tool/permission controls were not measured. These restrictions
are reported explicitly, rather than assigning an unsupported capability a
numeric zero or a simulated pass.

## Recorded latency

All four products below passed 30/30 measured trials in each case. The scopes
above and resource exclusions below apply to every number. Dify also has a separate raw report because output delivery and container recovery differ.

| Product | Generation p50 / p95 (ms) | Approval p50 / p95 (ms) | Restart p50 / p95 (ms) |
|---|---|---|---|
| qcg | 16.5 / 28.9 | 48.5 / 62.7 | 2191.4 / 2237.1 |
| langgraph | 17.2 / 22.8 | 26.1 / 33.2 | 7876.4 / 10262.0 |
| temporal | 236.7 / 416.2 | 315.3 / 406.1 | 12307.2 / 13393.8 |
| dify | 1898.3 / 2008.4 | 5890.4 / 6001.4 | 32232.8 / 35397.6 |

## Versions, setup and resources

Repository file `scripts/comparison/runtime-lock.json` pins the Temporal
CLI/server, Dify source revision, container digests, Docker and Compose archives.
Repository file `scripts/comparison/requirements.txt` pins all installed
packages. qcg is the locally modified release binary, identified by SHA-256 and
its base Git revision in the report. The checked-in
repository adapter instructions in `scripts/comparison/README.md` provide reproducible
setup and execution commands.

The setup operations are: qcg release build plus server start; LangGraph Python
environment/package installation plus graph/checkpoint setup; Temporal the
same Python environment plus fixed CLI acquisition, persistent server start and
worker start; Dify fixed source acquisition, digest-pinned image pulls, isolated
Compose deployment, local test-account setup and native workflow import/publish.
Shared prerequisite installation and download time are not counted as workflow
latency. These recorded operations are a fixture procedure, not a universal
installation-step ranking.

All services bind loopback and use disposable local persistence. Dify runs in
rootless Docker with internal application networks; API and worker telemetry
is disabled. An ingress-only reverse proxy publishes the loopback API. No
production account or external model provider is used. The Dify fixture's
source commit, image contents and resource configuration are preserved in the
runtime lock and setup script.

Wall p50/p95 are calculated from individual measured trials. Driver CPU and
maximum RSS, qcg server and Temporal worker process observations are recorded
with their scopes. Dify samples cumulative per-container CPU and current memory
after trials; those samples are not peak RSS. Temporal server and Docker daemon
resources are outside these process scopes. Consequently these numbers cannot
be combined into a uniform infrastructure efficiency rank. Separate qcg
[performance measurements](performance-validation.md) cover run count, journal
size, subscribers, read bytes, fold counts, cache bounds and live backpressure.

## Unmeasured axes and real models

This deterministic fixture does not measure equivalent cross-product priority
queues, arbitrary workflow concurrency, complete installation effort or every
security boundary. No real model credential, common model configuration or
billing budget was supplied. External model use, token use and cost are zero in
the deterministic fixture; this is not a real-LLM quality or cost result.

Real-model trials require the same model identifier/revision, parameters,
prompt, input and validator. Record execution date, token usage, billing rates,
cost, retries, tool calls, approval latency and validity; preserve failures and
keep these results separate. Do not aggregate unmeasured axes into an overall
quality ranking.

The adapters use the products' documented persistence and human-input surfaces:
[LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
and [interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts),
[Dify self-hosting](https://docs.dify.ai/en/self-host/deploy/quick-start/docker-compose),
and [Temporal self-hosting](https://docs.temporal.io/self-hosted-guide).
