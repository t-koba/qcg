# Deterministic product comparison

These adapters execute real qcg, LangGraph and Temporal workflows. The Dify
adapter imports native DSL 0.7.0 into a local Dify 1.17.1 instance and uses its
public workflow/human-input APIs. No adapter replaces another product's engine.
All products receive the same canonical JSON input and validate the same output
bytes. Dify delivers an output variable; the other adapters write files. This
is a documented surface difference, not evidence of equivalent artifact APIs.

Use an isolated Python environment, with exact versions from `requirements.txt`.
`runtime-lock.json` identifies the downloaded CLI/container/runtime contents.
Build qcg in release mode and start the fixed Temporal development server with
a persistent SQLite file before running `run.py`. For example:

```sh
python3 -m venv /tmp/qcg-comparison
/tmp/qcg-comparison/bin/python3 -m pip install -r scripts/comparison/requirements.txt
cargo build -p cli --release --locked
# temporal 1.9.1, archive SHA-256 in runtime-lock.json
# The server is local, headless and disposable; never use a production server.
temporal server start-dev --headless --ip 127.0.0.1 --port 17233 \
  --db-filename /tmp/qcg-temporal-comparison.sqlite
# In another terminal:
/tmp/qcg-comparison/bin/python3 scripts/comparison/run.py
```

The graph/checkpoint/application definitions are checked-in source. Each case
runs two warmup trials and thirty measured trials. Raw failures remain in the
report; any failed trial makes the command fail. qcg and Temporal worker
processes are SIGKILLed at a durable pending human gate, then restarted.
LangGraph recreates the client process and SQLite connection after its interrupt.
The fixture preparation uses exclusive file creation, and qcg checks its
preparation file timestamp, to detect accidental repeat effects after resume.

Run Dify separately with `dify_driver.py`. It requires an **owned disposable
local deployment**, not a user's existing account. The adapter initializes its
local test account once and stores its generated password outside the repo in
`/tmp/qcg-dify-test-account.json` with mode 0600. No credentials or form tokens
are included in the checked-in results. Set `QCG_DIFY_URL` for its loopback URL,
`QCG_COMPARISON_DOCKER` for the Docker CLI, and `DOCKER_HOST` for the rootless
socket. `setup-dify.py` prepares the fixed source, digest-pinned images and an
isolated comparison Compose project. API and worker set
`DISABLE_TELEMETRY=true`, and their networks are internal. Only a loopback
reverse proxy has an ingress network; it forwards exclusively to the Dify API.
The API container is explicitly killed and restarted at the pending human gate.

`QCG_COMPARISON_TRIALS` can reduce the count for adapter smoke checks; publish
only results with the full thirty-trial count. Report wall time, driver and
server/worker resources with their exact scopes. An in-process library, an HTTP
service, and a gRPC durable service do not have interchangeable baselines.
Do not aggregate their measurements into a claim of overall superiority.

Real-LLM comparisons require a separately configured common model and credential,
pricing/token metadata and an execution budget. No model credentials were
available in this execution, so the reports identify model use as absent and
retain that restriction rather than substituting simulated LLM scores.
