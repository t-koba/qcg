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

No production reconciliation protocol is established for either Docker or
Podman yet. They require separate backend verification: a property observed
on Docker must not be assumed for rootless Podman, or conversely. In particular,
Podman's filtering or storage APIs need their own ownership/removal analysis,
not a claim of equivalence to Docker's CLI. An ID-only or atomically
ownership-conditional removal primitive would address the replacement issue;
it would not independently fence an asynchronous create. Missing resources,
daemon errors, and cleanup failures must remain unresolved, not successful
cleanup. An empty lookup must never clear a durable Started operation.

## Real-backend identity diagnostic

`scripts/probe-container-identity.py` prepares a bounded, destructive protocol
probe for a **disposable local daemon with no concurrent resource mutation**:

```sh
python3 scripts/probe-container-identity.py docker --command-timeout 30
python3 scripts/probe-container-identity.py podman --command-timeout 30
```

It builds a static C fixture with the local compiler (`--compiler`, default
`cc`), imports its filesystem, and uses `--pull=never` and no container network.
It needs no base-image download. It creates and removes an owned fixture, then
creates another owned fixture whose name equals the first fixture's full ID.
Only after inspecting that alias and checking its actual ID, name and test
label does it deliberately try the stale-ID removal. This is a counterexample
probe, **not** a proposed cleanup algorithm. Ordinary fixture cleanup verifies
IDs and labels too, under the disposable/quiescent-daemon precondition that
production recovery cannot assume.

A successful JSON result reports whether the stale-ID removal deleted the
different-ID fixture; it explicitly reports that crash cleanup is **not
certified**. Failure to create the alias, backend unavailability, or failed
removal is inconclusive, not evidence of an ID-only primitive. Timeouts and
failed creates/imports can leave fixtures: discard the disposable daemon, or
retain its evidence for manual reconciliation. There are no absence-based
cleanup retries. Per-command bounds are supplied by the caller; the dedicated
Ubuntu 24.04 workflow configures 30 seconds and a five-minute job deadline.

`.github/workflows/container-identity.yml` runs Docker and rootless Podman
separately after a relevant `mizu` push or explicit dispatch. A green diagnostic
job is not crash-cleanup acceptance. Its real-backend results must be read
before selecting a production primitive; preparing the workflow does not mean
it has run. Local guard tests only cover the diagnostic's refusal on ownership
mismatch/unavailability and reporting of cleanup failure.

CLI doubles cannot establish daemon resource replacement, ID-or-name
resolution, or conditional-deletion semantics. Normal lifecycle and
indeterminate-replay tests likewise are not acceptance evidence for crash
cleanup. Real qcg SIGKILL tests during creation and workload, gate integration,
backend identity binding, and failure/other-run isolation remain required once
a safe backend primitive is established. Incus/LXD identity and Unix/MCP
process containment remain separate; see [host-process crash-recovery
limits](host-process-crash-recovery.md). A persisted friendly name, PID or
PGID supplies no durable reuse guard.
