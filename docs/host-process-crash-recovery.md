# Host-process cleanup after a qcg crash

## Current boundary and configuration

Trusted-host commands and host-mode MCP stdio servers execute as the qcg OS
user. `trusted_host` means the workload is trusted, not sandboxed against
hostile code; use container isolation for untrusted code. A process-tree
cleanup mechanism does not change that trust boundary. Container daemon
effects have a separate boundary and procedure in
[container crash-recovery limits](container-crash-recovery.md).

Crash recovery for host processes is Linux cgroup-v2 only and opt-in.
The deployment sets `QCG_HOST_CGROUP_PARENT` to an already-delegated cgroup
directory (for example `/sys/fs/cgroup/qcg.slice`). Unset or blank means
disabled: launches keep their existing live process-group cancellation and no
crash-cleanup claim is made. A present-but-invalid value fails before any
host command or MCP stdio launch, without fallback to a PID/PGID claim.

For a systemd service, set `Delegate=yes` in its `[Service]` section (see
[systemd resource control](https://www.freedesktop.org/software/systemd/man/latest/systemd.resource-control.html)):

```ini
[Service]
Delegate=yes
```

After the service is active, query its `ControlGroup` property:

```bash
systemctl show qcg.service --property=ControlGroup --value
```

The returned path is relative to `/sys/fs/cgroup`; set
`QCG_HOST_CGROUP_PARENT` to that directory in the service environment. For
example, `/system.slice/qcg.service` maps to
`/sys/fs/cgroup/system.slice/qcg.service`. Use the actual value for the unit;
user services and custom slices have different paths. qcg's startup checks
remain authoritative for the configured parent and required interfaces.

Installing qcg on a cgroup-v2 host, or placing its service in a delegated
systemd unit, does not enable cleanup by itself. The configured parent must
already be delegated to the qcg user: absolute, beneath `/sys/fs/cgroup`
(never the mount itself), a v2 domain cgroup with writable `cgroup.procs`,
readable `cgroup.events` with a valid `populated` field.
No controllers are enabled or required; `cgroup.subtree_control` need not be
writable. qcg records intent before creating a per-run child, confirms the
new root is empty, then writes `1` to its `cgroup.kill` interface and confirms
it remains empty before publishing Bound or launching a payload. The parent
itself need not grant kill permission. qcg never chowns or sudoes beneath a shared
root at runtime. The host must expose cgroup v2 (`0::` in `/proc/self/cgroup`
and `cgroup.controllers` at the mount).

While qcg is alive, command cancellation/timeout and MCP transport close/drop
use an in-memory process-tree guard alongside the cgroup. On Unix the guard
signals a dedicated process group; it covers only processes that remain in
that group. A descendant can call `setsid` to leave it. Tokio's child
`kill_on_drop` covers only the direct child when the guard is dropped. These
guards do not run after qcg receives SIGKILL. A numeric PID/PGID after
restart cannot prove that the same process group still exists.

## Recovery safety and its limits

A durable external operation still marked `Started` is retained. Under
exclusive run ownership, re-entry refuses default fail-closed operations; an
explicit `on_indeterminate=repeat` still reaches the existing operation guard.
Explicit deletion and retention GC refuse every `Started` operation,
including repeat-policy operations. These checks preserve evidence; they do
not stop a process or establish its outcome.

The operation gate is not a complete inventory of host processes. In
particular, MCP can start its stdio server while connecting and discovering
tools, before the tool call's `operation_started` record exists. Absence of a
`Started` record therefore does not establish that no MCP process survives.
Run deletion, a missing process lookup, and a changed PID/PGID are not cleanup
acknowledgements. There is no automatic host-process reconciliation command.
Keep the journal and reconcile the external effect before removing its
evidence. Even confirmed process termination would not prove that a tool's
external effect was unapplied or make an indeterminate operation safe to replay.

## Linux: delegated per-run subtree

When enabled, qcg binds one unique per-run-execution root beneath the
authorized delegated parent before any host launch, including MCP connection
setup. Schema v2 in `meta/host-cgroup.json` first durably records a Prepared
intent (`prepared: true`) with the run id, a fresh nonce, canonical parent
and intended root paths, kernel boot id, and retirement flags **before mkdir**.
Only that exact path is created and validated while still Prepared: empty
confirmation, `cgroup.kill` validation, then post-kill empty confirmation.
Only then qcg publishes Bound (`prepared: false`) durably before returning a
launch scope. A Prepared intent never yields a usable scope
(`binding.scope()` is `None` while `prepared`).
The root name is `qcg-<sanitized-run-id>-<nonce>`. Legacy v1 records without
`prepared` remain Bound (default false), never Prepared; v1 with Prepared
set is refused. Older binaries refuse v2 rather than misreading an intent as
cleanup authority.

Only permission-checked `trusted_host` commands and host-mode MCP stdio
payloads attach this scope. Docker/Podman/Incus clients, lifecycle helpers,
and container daemon processes do not; container cleanup policy is unchanged.

Children self-place via a `pre_exec` write of their own pid to the run root's
`cgroup.procs` before payload execution. The shared engine/MCP launch helper
uses raw syscalls and pre-built data. It arms `PR_SET_PDEATHSIG(SIGKILL)` and
checks that the original owner is still the parent before placement, covering
owner death before the hook runs. After successful placement it clears that
signal: the cgroup, not the spawning thread's lifetime, now owns cleanup.
A child delayed before placement cannot execute payload code after its owner
dies, even if recovery already removed the empty root. Post-spawn migration,
saved PID/PGID numbers, and shell wrappers are not ownership boundaries.
Trusted descendants may move among child cgroups inside the root; workloads
must not migrate outside it. This is not an adversarial sandbox.

Under the per-run execution lease, reconciliation precedes recovery, direct
re-entry, replay decisions, deletion, and retention GC. CLI replay delegates
reconciliation to the service under store-maintenance, admission, handoff,
and execution locks. An incomplete admission with any extant binding is not
wiped; ambiguous evidence must first be reconciled under exclusive ownership.
Checkpoint forks copy checkpoint data, never the source's cgroup binding.

The sidecar is a bounded (16 KiB identity-record format), no-follow regular-file
read. Every binding is checked for the caller's expected run id, schema,
32-hex-digit nonce, nonce-derived root identity, and retirement state. A
durably cleaned binding with empty confirmation is an idempotent no-op after
reboots or platform migration, even if its former parent no longer exists.
An uncleaned binding still requires its exact nonce-derived root, canonical
parent, matching configuration when set,
and same kernel boot before targeting anything. With the parent variable
unset it uses the persisted parent. It never infers ownership from PID/PGID
numbers.

A Prepared intent does **not** prove ownership of an existing filesystem
object. Reconciliation under exclusive run ownership first validates the run,
nonce, boot, canonical parent and configured-parent identity. If the exact
candidate is absent, it retires the never-launched attempt (`cleaned` and
`empty_confirmed` record absence for Prepared), allowing a fresh nonce on the
next attempt. If the candidate exists, even empty, or cannot be inspected,
reconciliation returns a visible identity error, preserves the intent and
candidate, and blocks same-run retries, deletion and retention GC. A persisted
Prepared intent never authorizes `cgroup.kill` or directory removal during
reconciliation and never yields a usable launch/cleanup scope; only the still-
running binder may write `cgroup.kill` to its just-created, empty root as a
validation probe. No name-prefix scan, parent sweep, or name-only deletion is
used.

This does not completely eliminate resource leaks: mkdir and durable Bound
publication are separate operations, with no durable filesystem ownership
identity across the gap. A crash or validation/publication error after mkdir
can leave an ambiguous Prepared candidate that qcg cannot safely remove.
Pre-change roots created before any sidecar was published remain untracked
and also cannot be automatically removed. Keep the evidence and have the
operator establish ownership outside qcg before any manual intervention;
a matching run/nonce/path or an empty cgroup alone is insufficient proof.

For Bound records, cleanup kills the whole root via `cgroup.kill`, confirms `cgroup.events`
reports `populated 0`, and removes nested cgroups bottom-up. Confirmation
uses deployment settings `QCG_HOST_CGROUP_CLEANUP_TIMEOUT_MS` (default 10000)
and `QCG_HOST_CGROUP_POLL_MS` (default 50), positive integer milliseconds;
invalid timing fails before launch. Reading only root `cgroup.procs` does not
prove emptiness. Normal return/handoff retires this run's tree the same way.

Binding updates use a unique create-new temporary file, file fsync, atomic
rename, and metadata-directory fsync. Empty confirmation is durably recorded
*before* directory removal, then `cleaned` is recorded after removal. Recovery
can finish an interrupted retirement from that durable confirmation. Missing
Bound roots without it remain ambiguous and refuse cleanup. Timeouts, I/O errors,
or identity ambiguity retain evidence and block recovery/deletion; normal
retirement errors are returned visibly. Stopping processes never resolves an
indeterminate operation or bypasses Started/Repeat policy.

## Other platforms

- **macOS:** qcg launches host commands and host MCP stdio servers in
  dedicated Unix process groups. Group-wide live cleanup reaches only members
  that remain in the group; a descendant can leave it with `setsid`. The
  Apple [`launchd.plist(5)`
  documentation](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5)
  describes cleanup of remaining processes with the launchd job's *same*
  process-group ID (unless `AbandonProcessGroup` disables that behavior), not
  ownership of an arbitrary descendant tree. Apple's [Technical Note
  TN2083](https://developer.apple.com/library/archive/technotes/tn2083/_index.html)
  likewise describes signaling the associated process group and identifies
  `setsid` as a way for a child to leave it. Even if qcg itself is a launchd
  job, its command's separately created process group is outside that
  same-PGID cleanup; an escaped descendant is outside its command group too.
  The reviewed macOS process/launchd mechanisms therefore provide no
  established, non-escapable descendant boundary tied to qcg's lifetime for
  arbitrary host commands or MCP stdio. A helper, process-tree walk, kqueue
  exit notification, or saved PID/PGID is not durable ownership after qcg
  dies; process enumeration and numeric identities cannot establish that an
  escaped process is still the original workload. Do not claim crash cleanup
  on macOS. If a deployment requires that guarantee, use Linux with explicit
  delegated cgroup-v2 recovery or supply a separately validated outer
  containment boundary. Otherwise the deployment must accept unresolved host
  effects after a qcg crash, or the product must deny host commands/MCP stdio
  on macOS when that residual risk is unacceptable; retain the journal and
  `Started` evidence for reconciliation.
- **Windows:** the engine command path calls Tokio `Command::spawn()` before
  creating, configuring, and assigning its per-command Job Object. Its explicit
  process flag is `CREATE_NEW_PROCESS_GROUP`, not `CREATE_SUSPENDED`, so payload
  code can run before `AssignProcessToJobObject`; descendants created in that
  window are not retroactively enrolled. The guard can terminate job members
  after attachment, but does not prove whole-tree cancellation or crash cleanup.
  Host MCP does not attach its Windows child to the command Job Object. Its
  trusted-host stdio path uses Tokio `Command` with `kill_on_drop(true)`;
  `BoundedChildTransport` sets a process group only under `cfg(unix)` and has
  no Windows Job Object attachment. That drop policy covers normal Rust-side
  cleanup, not qcg death: abrupt termination does not run `Drop`, and Windows
  [does not terminate child processes when their parent exits](https://learn.microsoft.com/en-us/windows/win32/procthread/terminating-a-process).
  No whole-tree crash cleanup is claimed for Windows MCP; retain the existing
  unresolved-operation and `Started`-evidence policy.

  The Windows API supports creation-time job assignment through
  `PROC_THREAD_ATTRIBUTE_JOB_LIST` (Windows 10 version 1703 and Windows Server
  2016 or later). Microsoft describes the behavior in its [job-list
  explanation](https://devblogs.microsoft.com/oldnewthing/20230209-00/?p=107812):
  the child is assigned before its initial thread is allowed to run. The
  locked Rust 1.99/Tokio 1.53.3 stack has no supported stable Tokio hook for
  those process attributes: Rust's
  `CommandExt::spawn_with_attributes` remains nightly-only, and Tokio's
  `Command::spawn_with` requires `tokio_unstable`. Stable Rust can call the
  documented Win32 APIs through bindings, but qcg has no custom `CreateProcessW`
  adapter that preserves its command-line, environment, working-directory,
  stdio, and asynchronous child semantics; that route is not implemented or
  tested. See Microsoft's [`UpdateProcThreadAttribute` reference](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-updateprocthreadattribute)
  and [job-object limits](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects).

  A stable `CREATE_SUSPENDED` creation flag could prevent the child from running
  before a later `AssignProcessToJobObject` and explicit resume, but it is not
  an atomic owner-death boundary: qcg can die after process creation and before
  assignment, leaving a suspended child outside the kill-on-close job. The
  locked stable Rust/Tokio child APIs do not expose the initial thread handle
  needed for a direct resume. An app-wide Job Object assigned to qcg affects
  all child processes by default and depends on the surrounding job
  hierarchy, so it is not an equivalent per-command fix. None of these
  alternatives has been implemented or tested for this process hierarchy,
  handle lifetime, or Windows runner environment. Until launch-time membership
  and qcg-death cleanup are validated on Windows, no Windows whole-tree
  crash-cleanup guarantee is claimed.

Normal cancellation still targets the tracked process and any attached job
members; it does not close the pre-assignment gap. These limitations do not
change container cleanup or the journal's indeterminate-operation policy.

## Provisioning and verification

The Linux CI job runs `scripts/test-host-cgroup.sh` as the runner UID inside
an actual transient systemd service with `Delegate=yes` and `User`/`Group`.
The script derives its parent from its own `/proc/self/cgroup` and probes a
new child for kill/occupancy support; it never changes shared-root controllers
or moves a runner shell manually. Unavailable delegation fails the job. There
is no root fallback, so a successful job establishes unprivileged delegation.

Tests without a configured parent skip live cgroup cases (including on a
read-only local sandbox); a configured but unusable parent fails, never skips.
Coverage includes launch membership, MCP transport launch, session escape,
leader exit, sibling isolation, binding tampering, replay lease exclusion,
incomplete-admission evidence retention, and interrupted retirement. Test-only
binding checkpoints also inject an error or SIGKILL the owner after root
creation and validation but before Bound publication. They check byte-identical
Prepared evidence, candidate identity and occupancy, Prepared scope absence,
non-destructive refusal, sibling survival, absent-candidate retirement and v1
Bound compatibility.
Service deletion and GC tests also exercise Prepared refusal before normal
Bound retirement. The existing delegated harness runs these tests; production
has no environment-driven checkpoint or failure hook. A separate
owner test executable uses qcg's binding/launch/reconciliation APIs: controlled
pre-placement and pre-exec barriers plus a running workload let the runner
SIGKILL the owner (not the command leader), then start a different owner to
reconcile. The CLI SIGKILL regression also kills a real `qcg run` during
workload and, when delegated, verifies membership and cleanup by a new qcg
owner before the unchanged Started gate refuses re-entry. These are test
cases, not a claim that a live delegated CI run has completed.
