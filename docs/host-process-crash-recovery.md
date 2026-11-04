# Host-process cleanup after a qcg crash

## Current boundary

Trusted-host commands and host-mode MCP stdio servers execute as external
processes. While qcg is alive, command cancellation/timeout and MCP transport
close/drop use an in-memory process-tree guard. On Unix the guard signals a
dedicated process group; it covers only processes that remain in that group.
A descendant can call `setsid` to leave it. Tokio's child `kill_on_drop` only
covers the direct child when the guard is dropped.

These guards do not run after qcg receives SIGKILL. The Unix PGID is held only
in memory, and a numeric PID/PGID after restart cannot prove that the same
process group still exists. The Windows command path uses a Job Object with
`KILL_ON_JOB_CLOSE`, but assigns the spawned process after `spawn`; the launch
path has not proved that untrusted code cannot create an unassigned descendant
first. The MCP stdio path does not attach its Windows child to that Job Object.
No whole-tree crash-cleanup guarantee is claimed for these paths.

## Recovery safety

A durable external operation still marked `Started` is retained, and the
existing fail-closed policy blocks its re-entry or deletion/GC where required.
That preserves evidence; it does not stop a process or establish its outcome.
Run deletion, a missing process lookup, and a changed PID/PGID are not cleanup
acknowledgements. There is no automatic process reconciliation command. Keep
the journal and reconcile the external effect before removing its evidence.
Container daemon effects have a separate boundary and procedure in
[container crash-recovery limits](container-crash-recovery.md).

## Platform work not yet proven

- **Linux:** cgroup v2 is a candidate for recursive process containment, but
  qcg does not create or own a protected per-operation cgroup, place a process
  there before untrusted code starts, or prove that the workload cannot migrate
  itself out. A safe implementation also needs durable object ownership,
  restart-safe identity checks, and confirmation that the subtree is empty.
- **macOS:** the current process-group mechanism is escapable. No supported,
  non-escapable process-tree boundary that survives qcg SIGKILL has been
  established for arbitrary host commands or MCP stdio.
- **Windows:** the engine command Job Object path needs launch-time membership
  proof; MCP stdio needs an equivalent whole-tree owner. Neither is covered by
  the current crash-recovery evidence gate.

Until platform-specific launch, ownership, descendant, and recovery tests prove
those boundaries, retain unresolved run evidence and do not claim that orphaned
host processes were cleaned up. Normal live-process cancellation remains
available; this limitation does not change container cleanup policy.
