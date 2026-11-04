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
The current implementation does not itself prevent deletion of this evidence.

## Why the current resource handles are insufficient

| Path | Current identity owner | Missing crash proof |
|---|---|---|
| Commands and container tools | `CmdGateway::run_container_workload` in `crates/engine/src/gateway/command.rs` allocates a temporary CID-file path | No run-persisted intent before `run`; the daemon writes an ID only after creation |
| MCP Docker-family servers | `McpRuntime::connect_stdio` in `crates/mcp/src/mcp/runtime.rs` allocates a separate temporary CID-file path | Same window, with a separate launch and cleanup path |
| Managed instances | `provision_incus` in `crates/container/src/session.rs` generates a name before `init` | The name and provisioning guard exist only in process memory; a friendly name is not a reuse guard |
| Recovery and maintenance | Run journal plus run/store leases | `operation_started` does not bind a daemon resource or acknowledge cancellation of creation |

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

## Verification limit

The existing adapters do not establish an ID-only or atomic ownership-guarded
removal contract, a durable creation acknowledgement, or an authenticated
backend identity. A matching label followed by an unqualified removal command
is therefore not an established identity-safe recovery protocol. No such
protocol is currently claimed for any family.

CLI doubles can test qcg's ordering and demonstrate late creation after client
death. They cannot establish a real daemon's resource replacement, ID-or-name
resolution, or conditional-deletion semantics. Normal lifecycle tests and
passing indeterminate-replay tests are not acceptance evidence for crash
cleanup. Unix command trees and host MCP stdio containment are separate
concerns; a persisted PID or PGID does not supply a durable identity either.
