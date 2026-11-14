# Container effects during run recovery

qcg records container effect intent in the run journal before provisioning.
The record currently contains the backend descriptor and a random
`qcg-<32 hex>` name. The client creates the instance with that name and
attaches diagnostic ownership labels. Normal teardown and recovery list by
name, then stop and delete by that same name. A successful command is recorded
as `effect_closed`; backend errors leave the intent open and block run re-entry
or deletion.

Recovery runs at startup and again under the run execution lease before a
recovered run resumes. This prevents concurrent qcg execution from creating an
effect while recovery is acting, but it does not fence a daemon-side create
that is still completing after the process dies.

## Limits

This is not a proven identity-safe crash-cleanup protocol. The journal stores
no immutable backend instance ID, and the implementation has no atomic
compare-and-delete operation. A matching name is treated as ownership. If a
name is reused between lookup and removal, current name-targeted commands
cannot prove that the instance being removed is the one qcg created. Labels
are diagnostic, not an atomic ownership check.

A bounded real-daemon diagnostic on Docker and rootless Podman found that an
absent full container ID can resolve to a different container deliberately
created with that ID-shaped name, and removal by that stale reference can
remove the replacement. This is evidence that those interfaces do not provide
a strict ID-only reference in that case; the diagnostic did not exercise qcg
crash recovery and is not cleanup certification. Incus/LXD have no equivalent
real-daemon identity probe here; their current CLI plans also address
instances by name.

The settle check is finite. A create that completes after the last absence
check can be left behind after the intent is closed. A successful name-based
removal likewise proves only that the backend accepted the request, not that
it removed the originally journaled instance. Backend errors leave the intent
open, but the current code cannot recognize a successful name-reuse race as
ambiguous and may record the intent closed after removing a replacement.
Therefore no Docker, Podman, runsc, Incus, or LXD crash-cleanup guarantee is
claimed, and `effect_closed` alone is not evidence that an effect is safe to
replay.

## Requirements for a cleanup guarantee

A supported backend needs a durable, run-bound instance identity, a way to
reconcile creates that finish after client death, and a deletion operation
that atomically checks the exact identity and ownership. Real-daemon recovery
tests must cover process death during create, late completion, replacement of
the name, repeated recovery, wrong-owner resources, and cleanup failure. A
name, prefix, label, finite settle interval, or successful CLI exit is not by
itself that proof.
