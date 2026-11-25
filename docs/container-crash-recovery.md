# Container effects during run recovery

qcg records container effect intent in the run journal before provisioning.
The record currently contains the backend descriptor and a random
`qcg-<32 hex>` name. The client creates the instance with that name and
attaches diagnostic ownership labels. Live teardown of an owned session still
stops and deletes that session and journals `effect_closed`.

After the creator dies, qcg does **not** reclaim name-keyed effects: it does
not delete by name and does not certify absence. Recovery makes one bounded
presence probe, records what was observed in the refusal detail, and retains
the intent whichever way the probe went: qcg has not proved that the effect
stopped. The refusal is written as a durable failed run result, so run failure
detail surfaces it rather than leaving it only in server logs. Replay,
including a direct `qcg run`, is refused while any intent is open, as are run
deletion and retention GC; the external outcome must be reconciled manually.

Decision: operator end state's "stops, or reports" has two realizations. For
name-keyed container effects there is no safe stop under the current backend
contracts (see [Limits](#limits)): a journaled name cannot prove ownership, so
qcg refuses replay and deletion, reports the effect, and retains its intent
instead of stopping or deleting anything. Delegated cgroup host effects are
different: recovery actually stops them by writing `cgroup.kill` and confirming
`populated 0` (see [host process recovery](host-process-crash-recovery.md)).
When a run already has a terminal outcome, that event and its reason are
preserved unchanged; the unresolved container effect is surfaced as a
state-free `effect_unresolved` journal event whose `detail` carries the exact
marker `qcg has not proved that the effect stopped`.

Recovery runs at startup and again under the run execution lease before a
recovered run resumes. This prevents concurrent qcg execution from creating an
effect while recovery is acting, but it does not fence a daemon-side create
that is still completing after the process dies. The refusal is permanent, not
a transient reconciliation failure, so it is not retried; a later scan may
rediscover the retained intent and record the same refusal again.

## Limits

This is not a proven identity-safe crash-cleanup protocol. The journal stores
no immutable backend instance ID, and the implementation has no atomic
compare-and-delete operation. A matching name cannot prove ownership: if a name
is reused, name-targeted commands cannot prove that the instance is the one
qcg created, and a missing entry does not fence a create that finishes later.
Labels are diagnostic, not an atomic ownership check. Recovery therefore
neither removes nor settles effects by name.

A bounded real-daemon diagnostic on Docker and rootless Podman found that an
absent full container ID can resolve to a different container deliberately
created with that ID-shaped name, and removal by that stale reference can
remove the replacement. This is evidence that those interfaces do not provide
a strict ID-only reference in that case; the diagnostic did not exercise qcg
crash recovery and is not cleanup certification. Incus/LXD's current CLI
plans also address instances by name.

The manually dispatched `Container identity diagnostic` workflow also offers an
Incus-only probe on an ephemeral Ubuntu 24.04 runner. It installs the distro
Incus packages and initializes a local daemon with `incus admin init --minimal`.
`scripts/probe-incus-identity.py` creates a unique run-scoped project and a stopped
`create --empty --storage default` container, verifies its ownership and `volatile.uuid`, removes
it, and creates a verified replacement under the same name with a different
UUID. It then observes lookup and deletion through the stale name via the local
Unix API. No instance is started and no image is fetched. The
`container-identity-incus` artifact contains only sanitized observations, always
`crash_cleanup_certified:false` and `qcg_crash_recovery_test:false`.

A real-daemon observation has since been established on an ephemeral Ubuntu
24.04 runner with Incus 6.0.0: the stale name resolved to the replacement
instance and removal through that stale name deleted the replacement
(`outcome: observed_name_keyed_replacement_deletion`,
`stale_name_resolved_to_replacement: true`,
`stale_name_delete_removed_replacement: true`), while the artifact still
records `crash_cleanup_certified:false` and `qcg_crash_recovery_test:false`.
The observation is recorded in the `container-identity-incus/incus-identity.json`
artifact (digest
`6e10c6c8092a361348161585f517f570b6d13fe12651150124133033793d6a21`) from the
`Container identity diagnostic` workflow run `45300052460`
(https://github.com/t-koba/qcg/actions/runs/45300052460), check `Identity probe
(Incus, not cleanup certification)`, on published revision
`d76d469c579f51986ba6b60d01eae78d0e3991bb`. This is diagnostic evidence of the
same name-keyed hazard seen with Docker and Podman, not a cleanup guarantee,
and it provides no LXD evidence.

Setup assumes Ubuntu's Incus packages, local socket,
the initialized `default` storage pool, and API/`volatile.uuid` support. The
project shares the default project's profiles (`features.profiles=false`);
explicit storage selection supplies the root disk without relying on an
inherited root-device setting. An unavailable pool makes creation
inconclusive with no fallback storage or cleanup. Setup failure,
command or API timeout, schema mismatch, or uncertain ownership yields
`inconclusive`, never a cleanup-safety claim. The job is bounded to 10 minutes,
setup commands to 120/120/45 seconds, the whole probe to 120 seconds, and each
probe command/API request to 15 seconds (outer `timeout` commands force-kill
after an additional 5-second grace). CLI calls are forced local and pinned
to the same socket as the API. Only successfully created and verified resources
are deleted, under the disposable, quiescent-daemon precondition; the checks are
not atomic compare-and-delete. After ambiguity there is no destructive retry or
fallback cleanup; any remaining resources expire with the runner. On success
the probe verifies instance absence and retires its empty project. This does
not exercise qcg process death, late creates, continuation, or reset.

Recovery's single presence probe is finite: a create that completes after it
is simply observed later, and neither observation settles the intent. An
unreachable backend leaves the intent open and produces the same fail-closed
refusal. Recovery never journals `effect_closed` and never deletes by name, so
it cannot mistake a replacement for the original instance. Therefore no
Docker, Podman, runsc, Incus, or LXD crash-cleanup guarantee is claimed, and
`effect_closed` from live teardown alone is not evidence that an effect is safe
to replay.

## Requirements for a cleanup guarantee

A supported backend needs a durable, run-bound instance identity, a way to
reconcile creates that finish after client death, and a deletion operation
that atomically checks the exact identity and ownership. Real-daemon recovery
tests must cover process death during create, late completion, replacement of
the name, repeated recovery, wrong-owner resources, and cleanup failure. A
name, prefix, label, finite settle interval, or successful CLI exit is not by
itself that proof.
