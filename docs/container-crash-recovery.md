# Container cleanup after a qcg crash

qcg reclaims container effects by **unique identity**. It does not rely on a
create-time fence or on compare-and-delete from the backend.

## Mechanism

Applies to Docker, Podman, Docker with runsc, Incus and LXD, for command
containers and for container-isolated MCP servers.

1. Before any create call, the run journal records `effect_intent` (fsynced
   like `operation_started`): the backend (client binary and runtime flag) and
   a fresh name `qcg-<32 hex>`. The token is 128 bits from the OS RNG. If the
   intent cannot be journaled, nothing is created.
2. The instance is created with exactly that name (`docker run --name`,
   `incus init <name>`). It also carries labels `qcg.store` (hash of the
   canonical runs directory), `qcg.run` and `qcg.effect` (Docker `--label`,
   Incus `user.*` config). Labels are for diagnostics only; nothing is ever
   selected or deleted by label or prefix.
3. A normal run stops and removes the instance by name and journals
   `effect_closed` (`released`). The detached drop-guard retry does the same
   and may close after the run is terminal.

## Reconciliation

For each `effect_intent` without `effect_closed`, qcg lists the backend for
the exact journaled name. A match is stopped and deleted (`removed`). No match
is re-checked once after `CONTAINER_RECLAIM_SETTLE_SECS` (default 10), because
a create submitted before the crash may still be completing in the daemon;
still no match is `absent`. The result is journaled as `effect_closed`. The
operation is idempotent: closed intents are never revisited. Only names that
pass the exact `qcg-<32 hex>` shape and a known backend client are acted on.

It runs:

- at service startup, before recovery admits runs and before the listener
  accepts new ones, for every run directory the construction scan found with
  an open intent (terminal and direct runs included);
- when a recovered run takes its execution lease, before any replay (the same
  point as host-cgroup reconciliation).

A run whose execution lease a live process holds is never touched. While an
intent is open, run deletion and retention GC refuse the run.

## Guarantees

- An instance qcg created, in a run that was journaled, is removed at the next
  recovery that can reach its backend, even if the create finished after the
  crash, provided it finished within the settle interval of the check.
- Unrelated instances, including ones with similar names, prefixes or labels,
  are never stopped or deleted.
- Reconciliation failure is reported (log, run re-entry refused with the
  journal retained) and never read as absence.

## Not guaranteed

- Backend unreachable at recovery: the intent stays open, the run is not
  resumed and cannot be deleted; the next recovery retries.
- A create that completes later than the settle interval after the final check
  leaves an instance with a closed intent. Raise the interval if the backend
  is slow to finish detached creates.
- Host processes are outside this mechanism; see
  [host-process crash recovery](host-process-crash-recovery.md).
- Power loss: only the intent is fsynced, the same crash model as the rest of
  the journal (process kill).
- Instances created by a qcg older than this mechanism are not tracked.

## Trust assumption

An instance carrying a journaled `qcg-<token>` name is ours because nobody
else can guess a 128-bit token. A party that can read the run journal and
create instances on the same daemon can defeat this, but it can already
delete that daemon's instances directly. Journals are trusted store contents.
