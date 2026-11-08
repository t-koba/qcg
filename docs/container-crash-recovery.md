# Container cleanup after a qcg crash

## Supported boundary

Container cleanup currently covers normal exit, catchable cancellation,
timeout, and dropped futures **while qcg is alive**. It does not provide a
crash-recovery cleanup guarantee for Docker, Podman, Incus, or LXD. `docker_runsc`
uses the same Docker client lifecycle and has the same limitation. MCP servers
launched in containers are affected too.

After SIGKILL or a host/process crash, a daemon can retain a workload or finish
creating one after qcg has exited. A missing container in one lookup is not a
creation-cancellation acknowledgement. The qcg run execution lease excludes
other qcg owners; it does not fence a request already submitted to a daemon.

Run recovery's refusal to repeat an indeterminate operation is not proof that
its external resources were stopped. Likewise, deleting a run or applying
retention GC is not a container cleanup operation. Do not use run deletion as a
way to stop orphaned containers. Retain the run journal and daemon-side evidence
until the original backend and its in-flight operations have been reconciled.
A durable external operation still marked `Started` is retained while its
outcome is unknown. Same-run ownership re-entry refuses a `Started` operation
whose durable `on_indeterminate` policy is fail-closed; a `repeat` operation
reaches the existing guard, which re-executes it with the acknowledged
double-apply risk. `qcg runs replay` applies the source policy gate too, but
starts a distinct new run; it does not reconcile or clear the original journal.
Explicit run deletion and retention GC retain every `Started` operation
regardless of policy. GC skips such a run without charging retention slots or
blocking collection of unrelated runs. The effective policy is journaled with
each `operation_started` event (`fail`/`repeat`) and folded into the operation
record; pre-policy journals and unknown values default to `fail` closed. The
diagnostic identifies the operation without repeating its command, arguments,
or payload. This gate retains evidence; it neither stops the effect nor proves
it stopped. A terminal run event is not an operation finish: cancellation can
settle normally while leaving a `Started` record, and that record still blocks
deletion and fail-closed re-entry. A completed `FailedIndeterminate` operation
keeps its existing replay policy, including an explicitly configured repeat;
it is not blocked by this gate.

There is no automatic reconciliation command yet. Keep the journal intact while
establishing the original backend's outcome and resource ownership; do not edit
operation status merely to bypass the gate. A future reconciliation path must
record an evidenced outcome before permitting recovery or deletion.

## Why the current resource handles are insufficient

| Path | Current identity owner | Missing crash proof |
|---|---|---|
| Commands and container tools | `CmdGateway::run_container_workload` in `crates/engine/src/gateway/command.rs` allocates a temporary CID-file path | No run-persisted intent before `run`; the daemon writes an ID only after creation |
| MCP Docker-family servers | `McpRuntime::connect_stdio` in `crates/mcp/src/mcp/runtime.rs` allocates a separate temporary CID-file path | Same window, with a separate launch and cleanup path |
| Managed instances | `provision_incus` in `crates/container/src/session.rs` generates a name before `init` | The name and provisioning guard exist only in process memory; a friendly name is not a reuse guard |
| Recovery and maintenance | Run journal plus run/store leases | `operation_started` does not bind a daemon resource or acknowledge cancellation of creation |

`RunContext::guard_external_operation` does durably append a generic
`operation_started` before the command gateway is called, and returns its
stable operation ID to the step. The ordinary and agent command paths do not
pass that ID into `CmdGateway`; the container adapter therefore cannot derive
a run-bound name/label from it. The generic record also carries no effect type,
backend identity, or container ID. It is a pre-call breadcrumb, not a container
recovery handle: a safe protocol still needs effect-specific identity and
creation/reconciliation evidence tied to the daemon request.

`docker_run_argv` in `crates/container/src/plans.rs` currently uses `run --rm
--cidfile ...`, without a precommitted ownership name or label. Automatic removal
also means a completed workload can disappear before recovery can inspect it.
`teardown` and `teardown_sync` in `session.rs` treat a missing CID file as success;
this is not a safe crash-recovery rule.

For Docker-family teardown the existing adapter supplies `kill ID` and `rm -f
ID`. For Incus-like teardown it supplies `stop -f NAME` and `delete -f NAME`.
Neither adapter models an expected ownership token, a conditional deletion
precondition, or a daemon operation handle. `Backend` in
`crates/container/src/backend.rs` captures a client binary and optional runtime
flag, not a stable daemon/project identity or a creation fence.

## Docker/Podman reconciliation blocker

A preselected name and ownership label, applied together at creation, would
close the CID-file discovery gap. They would not by themselves make cleanup
safe. Consider this sequence under otherwise exclusive qcg run ownership:

1. Recovery inspects full ID A and verifies its operation label.
2. A is removed by another daemon actor (or automatic removal).
3. A different resource B acquires the **name** A.
4. Recovery submits `container rm --force A`.

If the removal interface falls back from an absent ID to a name, step 4 can
remove B despite the earlier ownership check. Re-inspecting immediately before
removal only narrows this race. Passing the result of a label-filtered list to
`rm` has the same problem. A full-length ID string is not an ID-only interface
contract. The current adapter supplies neither an expected-label precondition
nor a backend-wide exclusion of other actors. Binding the original daemon and
context is necessary too; the client binary alone does not identify that daemon.

The public API surfaces reviewed for Docker Engine v1.48 and Podman 4.9.x do
not document a conditional-delete precondition. Docker's
[`DELETE /containers/{id}`](https://docs.docker.com/reference/api/engine/version/v1.48/)
selector accepts an ID or name and exposes removal flags; Podman's
[`DELETE /libpod/containers/{name}`](https://docs.podman.io/en/v4.9.3/_static/api.html)
selector likewise accepts a name or ID and exposes force/removal options.
Neither endpoint requires the expected immutable ID, ownership label, or
generation as a condition on deletion. Calling these routes directly through
HTTP or an SDK therefore does not, by itself, remove the verified-ID-to-delete
race. The bounded diagnostic below now exercises these routes sequentially,
but it does not race a concurrent replacement or fence an in-flight create; an
alternative must still be checked against its exact supported server
implementation and version.

An ownership-conditional removal primitive would address only replacement at
delete time. It would not fence an asynchronous create submitted before qcg
crashed. Safe reconciliation needs both instance-safe destruction and a durable
creation token with authoritative terminal status or an acknowledged
cancel/drain barrier that prevents late resource creation. Neither is provided
by the current adapter or established by the CLI or sequential API diagnostic.

Review of the version-pinned create contracts found no durable create-request
handle or cancellation acknowledgement. Docker Engine v1.48 specifies a 201
`ContainerCreateResponse` for `POST /containers/create`; Moby v28.0.4's
[`postContainersCreate` handler](https://github.com/moby/moby/blob/v28.0.4/api/server/router/container/container_routes.go)
waits for `ContainerCreate(ctx, ...)` and writes that response after it returns.
Podman v4.9.3 registers `POST /libpod/containers/create` with a 201
`containerCreateResponse`; its
[Libpod handler](https://github.com/containers/podman/blob/v4.9.3/pkg/api/handlers/libpod/containers_create.go)
waits for `ExecuteCreate(r.Context(), ...)` and then returns the container ID
and warnings. The
[Docker v1.48 API schema](https://github.com/moby/moby/blob/v28.0.4/api/swagger.yaml)
and [Podman v4.9.3 route contract](https://github.com/containers/podman/blob/v4.9.3/pkg/api/server/register_containers.go)
expose a result on successful response, but no durable create-job ID or
request-specific terminal-status or cancellation acknowledgement.
Both implementations pass request context downstream, but these sources do not
establish that client disconnect aborts or rolls back all side effects, and no
direct disconnect test was run. A lost response therefore leaves no
caller-observable terminal result; it does not prove that creation was canceled
or rolled back. Keep that case unresolved, even if a later lookup is empty.

Docker and rootless Podman still require separate backend verification; do not
assume that a property on one holds for the other. Podman's filtering or
storage APIs need their own ownership/removal analysis, not a claim of
equivalence to Docker's CLI. Missing resources, daemon errors, and cleanup
failures must remain unresolved, not successful cleanup. An empty lookup must
never clear a durable Started operation.

## Incus v6.23.0 contract audit

The source review does not establish a safe Incus recovery primitive. The
version-pinned [`instances_post.go`](https://github.com/lxc/incus/blob/v6.23.0/cmd/incusd/instances_post.go)
creates a task operation with no `onCancel` callback for image/empty instance
creation, then runs instance creation asynchronously. Incus operations expose
`may_cancel` only when a cancel callback or cancelable HTTP request is present;
`DELETE /operations/{id}` is a best-effort operation cancellation API, not a
durable create fence or proof that no later side effect can complete. The
create request does not provide a client-selected durable operation key, and
the caller learns the generated operation UUID only in the asynchronous
response. A lost response or qcg SIGKILL therefore has no proven terminal
create outcome or acknowledged cancel-and-drain barrier. This is a source
contract review, not a direct daemon disconnect/cancellation test.

The deletion route in
[`instance_delete.go`](https://github.com/lxc/incus/blob/v6.23.0/cmd/incusd/instance_delete.go)
resolves `DELETE /instances/{name}` (scoped by project), loads the instance by
name, then schedules `inst.Delete(false)`. The route/client contract reviewed
has no expected immutable instance ID, ownership token, or generation
precondition on deletion. A prior GET/name/marker check consequently cannot
prove that a later name-addressed delete still targets the same instance.
Incus's internal database ID and per-instance operation locks do not provide
qcg a documented public compare-and-delete contract.

Do not implement Incus cleanup from this evidence: durable create fencing and
identity-conditional destruction are both unproven, and the isolated Incus
daemon needed for direct race/SIGKILL coverage was not available. Source
receipts for this bounded v6.23.0 audit are `202e3034190bc5b8be2738c9a8bdd3e58e8684347622b3c89f5d87a0d05bb5f8`
(creation route), `e71ed65300f794adf617e8da8fb92e7a16345b6b590f3a1c610854ef6ddae443`
(create implementation), `2e157865047bfc55ef3c7411c1964de58adb9695468ccb0087ed79fbee625801`
(deletion route), and `5b1d242142dcf6939d0af93fb551893e1a4a26a9991c01c177dc600151e59c54`
(operation cancellation lifecycle). These are partial source-review evidence,
not crash-cleanup acceptance. Keep Incus effects unresolved and split this
backend from any future backend whose complete contract and tests differ.

## Real-backend identity diagnostic

`scripts/probe-container-identity.py` prepares a bounded, destructive protocol
probe for a **disposable local daemon with no concurrent resource mutation**:

```sh
python3 scripts/probe-container-identity.py docker \
  --command-timeout 30 --api-response-limit 1048576
python3 scripts/probe-container-identity.py podman \
  --command-timeout 30 --api-response-limit 1048576
```

It builds a static C fixture with the local compiler (`--compiler`, default
`cc`), imports its filesystem, and uses `--pull=never` and no container network.
It needs no base-image download. It creates and removes an owned fixture, then
creates another owned fixture whose name equals the first fixture's full ID.
Only after inspecting that alias and checking its actual ID, name and test
label does it deliberately try the stale-ID removal. This is a counterexample
probe, **not** a proposed cleanup algorithm. It then creates a second fixture
for a direct API probe: Docker uses the same local Unix endpoint as its CLI;
rootless Podman starts a temporary service on a private Unix socket. It records
server/API versions, inspects the stale full-ID token through the backend's
versioned API route, and submits DELETE only if that route resolves to the
expected different-ID fixture and the fixture's ID, name and ownership label
are verified. A 404/no-alias observation skips DELETE. API response bodies are
bounded by the required `--api-response-limit` argument. Ordinary fixture
cleanup verifies IDs and labels too, under the disposable/quiescent-daemon
precondition that production recovery cannot assume.

The earlier CLI publication
[`a1ff46772f9da523cc58629d087eaf39ba49dc12`](https://github.com/t-koba/qcg/actions/runs/42098124987)
observed Docker CLI 28.0.4 and rootless Podman 4.9.3 both resolving the stale
full-ID string as a replacement's name and removing that different-ID fixture.
The updated direct API probe then ran on SHA
`f75b3e2eefac649079a23fd97f71d6ce0814fa66` (source commit `1c0d960`) in
[workflow run 42108752211](https://github.com/t-koba/qcg/actions/runs/42108752211).
Docker client/server 28.0.4 negotiated Engine API 1.48; API inspection of the
stale full-ID token returned 200 for the replacement fixture, and DELETE by
that token returned 204 and removed the different-ID fixture. Rootless Podman
client/server 4.9.3 reported compat API 1.41 and Libpod API 4.9.3; API
inspection likewise returned 200 for the replacement, and DELETE returned 200
and removed it. The exact artifacts are recorded as evidence
`5b653de423a54df99886f6cb1027dc32` (Docker) and
`895e39ae781e464ebd16d9bb60d78732` (Podman).

These real-backend results establish only the tested sequential stale-ID/name
resolution and deletion behavior. They do not test a concurrent replacement
race, qcg SIGKILL recovery, or create/cancel fencing, and neither result
certifies crash cleanup. Future diagnostic results must likewise be read before
selecting any production primitive.

The JSON result separates CLI and API observations and includes API version
metadata, inspect status, delete status and whether the known different-ID
fixture disappeared. `crash_cleanup_certified` remains **false** in every
result. Failure to create the alias, backend unavailability, or an unconfirmed
API state is inconclusive, not evidence of an ID-only primitive. Timeouts and
failed creates/imports can leave fixtures: discard the disposable daemon, or
retain its evidence for manual reconciliation. There are no absence-based
cleanup retries. Per-command bounds are supplied by the caller; the dedicated
Ubuntu 24.04 workflow configures 30 seconds and a five-minute job deadline.

`.github/workflows/container-identity.yml` runs Docker and rootless Podman
separately after a relevant `mizu` push or explicit dispatch. A green diagnostic
job is not crash-cleanup acceptance. Its real-backend results must be read
before selecting a production primitive; preparing the workflow does not mean
it has run. Local tests cover diagnostic refusal on ownership mismatch,
unknown API metadata, and unavailability, plus Unix transport bounds; they do
not establish daemon behavior.

CLI doubles cannot establish daemon resource replacement, ID-or-name
resolution, or conditional-deletion semantics. Normal lifecycle and
indeterminate-replay tests likewise are not acceptance evidence for crash
cleanup. Real qcg SIGKILL tests during creation and workload, gate integration,
backend identity binding, and failure/other-run isolation remain required once
a safe backend primitive is established. Incus/LXD identity and Unix/MCP
process containment remain separate; see [host-process crash-recovery
limits](host-process-crash-recovery.md). A persisted friendly name, PID or
PGID supplies no durable reuse guard.
