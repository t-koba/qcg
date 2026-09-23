//! Idempotency-Key admission for run mutations: same key plus same
//! request digest returns the same run, same key plus different content
//! conflicts, across restarts and processes. In-memory entries live in
//! [`config`], the durable claim protocol in [`durable`], owner cleanup in
//! [`guard`]; this module orchestrates admission, waiting, and commit.

pub(crate) mod config;
pub(crate) mod durable;
pub(crate) mod guard;

pub(crate) use config::{
    IdempotencyEntry, effective_idempotency_max_entries, effective_idempotency_ttl,
    prune_idempotency,
};
pub(crate) use durable::{
    ClaimOutcome, OwnerCheck, StoreReadyError, WaitOutcome, check_pending_owner,
    claim_durable_pending, find_orphan_run, load_durable_ready_result, release_durable_pending,
    store_durable_ready, wait_for_peer_ready, write_orphan_record,
};
pub(crate) use guard::PendingIdempotencyGuard;

use anyhow::Result;
use axum::http::HeaderMap;
use axum::response::Response;
use qcg_api::ApiError;
use qcg_policy::IDEMPOTENCY_HEADER;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Instant;

use crate::server::config::AppState;
use crate::server::error::ApiHttpError;
use crate::server::runs::respond_with_snapshot;

pub(crate) fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiHttpError> {
    headers
        .get(IDEMPOTENCY_HEADER)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("Idempotency-Key must be valid ASCII"))?
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string)
        .map(Ok)
        .transpose()
}

/// Inputs for [`with_idempotency`], bundled so the admission signature
/// stays reviewable as options grow.
pub(crate) struct IdempotentCall<'a, F, Fut>
where
    F: FnOnce(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<String, ApiError>>,
{
    pub(crate) state: &'a Arc<AppState>,
    pub(crate) headers: &'a HeaderMap,
    pub(crate) scope: &'a str,
    pub(crate) target: &'a str,
    pub(crate) body: &'a [u8],
    pub(crate) created: bool,
    pub(crate) reserved_run_id: Option<String>,
    pub(crate) execute: F,
}

/// Runs a run mutation under an idempotency key when one is supplied.
/// `scope`, `target`, and `body` feed the request digest, so a reused key
/// with different content conflicts instead of aliasing another request.
/// `reserved_run_id` carries the pre-execution run reservation: for keyed
/// start/fork requests the handler reserves the run id before the claim so
/// a crash between run creation and Ready commit retries into the SAME run
/// instead of orphaning one run and creating another. Answer, confirm, and
/// cancel pass their target id; the reservation is ignored by their
/// executors but recorded in the pending claim for uniformity.
pub(crate) async fn with_idempotency<F, Fut>(
    call: IdempotentCall<'_, F, Fut>,
) -> Result<Response, ApiHttpError>
where
    F: FnOnce(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<String, ApiError>>,
{
    let IdempotentCall {
        state,
        headers,
        scope,
        target,
        body,
        created,
        reserved_run_id,
        execute,
    } = call;
    let mut digest_input = Vec::with_capacity(scope.len() + target.len() + body.len() + 2);
    digest_input.extend_from_slice(scope.as_bytes());
    digest_input.push(0);
    digest_input.extend_from_slice(target.as_bytes());
    digest_input.push(0);
    digest_input.extend_from_slice(body);
    let request_digest = hex::encode(Sha256::digest(&digest_input));
    let Some(idempotency_key) = idempotency_key(headers)? else {
        // Unkeyed requests skip the claim/commit protocol, but a run-creating
        // handler has already reserved its run id; the executor must receive
        // it so the run is created under that reservation instead of failing
        // closed. Answer/confirm/cancel executors ignore the value.
        let run_id = execute(reserved_run_id.clone())
            .await
            .map_err(ApiHttpError::from_api)?;
        let snapshot = state
            .service
            .snapshot(run_id)
            .await
            .map_err(ApiHttpError::from_api)?;
        return respond_with_snapshot(snapshot, created);
    };
    // Frozen boot policy: every prune, capacity, TTL, and expiry decision
    // below uses the values resolved once at boot and stored in AppState.
    // The environment is never re-read here, so tuning applies at the next
    // boot and one request can never mix two standards or drift from boot
    // validation (E04).
    let policy_ttl = state.idempotency_ttl;
    let policy_max_entries = state.idempotency_max_entries;
    // An orphaned run id observed while waiting rides across claim-loop
    // iterations: the claim may be replaced or reaped before the loop
    // re-reads it, so carrying the id preserves the run association. The
    // durable claim owner rides along too, so every release below only
    // ever removes our own claim file. `claim_owner`/`claim_generation`
    // are overwritten on every Owner arm before any read, so no reset is
    // needed when looping on Superseded (E02).
    let mut carried_adopted: Option<String> = None;
    let mut claim_owner: Option<String> = None;
    let mut claim_generation: Option<u64> = None;
    let (owner_id, adopted_run_id) = 'admit: loop {
        let (owner_id, adopted_run_id) = loop {
            let (wait, owner_id, adopted_run_id) = {
                let mut idempotency = state.idempotency.lock().await;
                prune_idempotency(
                    &mut idempotency,
                    Instant::now(),
                    policy_ttl,
                    policy_max_entries,
                )
                .map_err(ApiHttpError::internal)?;
                match idempotency.get(&idempotency_key) {
                    Some(IdempotencyEntry::Ready { digest, run_id, .. }) => {
                        if digest != &request_digest {
                            return Err(idempotency_conflict());
                        }
                        let run_id = run_id.clone();
                        drop(idempotency);
                        let snapshot = state
                            .service
                            .snapshot(run_id)
                            .await
                            .map_err(ApiHttpError::from_api)?;
                        return respond_with_snapshot(snapshot, created);
                    }
                    Some(IdempotencyEntry::Pending {
                        digest, completed, ..
                    }) => {
                        if digest != &request_digest {
                            return Err(idempotency_conflict());
                        }
                        (Some(completed.subscribe()), None, None)
                    }
                    None => {
                        // Release the map lock before any await: one slow disk
                        // must not serialize admissions for every other key.
                        // The durable claim protocol (not the memory map)
                        // arbitrates cross-process ownership, and the map is
                        // re-examined under a fresh lock before registering
                        // (E02).
                        drop(idempotency);
                        // Cross-restart and cross-process guarantee: a durable
                        // Ready record wins over starting a duplicate run.
                        // Blocking filesystem work runs off the async runtime (E02).
                        let runs_dir = state.runs_dir.clone();
                        let key = idempotency_key.clone();
                        if let Some(durable) =
                            load_durable_ready_async(runs_dir, key, policy_ttl).await?
                        {
                            if durable.digest != request_digest {
                                return Err(idempotency_conflict());
                            }
                            let run_id = durable.run_id.clone();
                            // Refresh in-memory cache for subsequent hits.
                            state.idempotency.lock().await.insert(
                                idempotency_key.clone(),
                                IdempotencyEntry::Ready {
                                    digest: durable.digest,
                                    created_at: Instant::now(),
                                    run_id: run_id.clone(),
                                },
                            );
                            let snapshot = state
                                .service
                                .snapshot(run_id)
                                .await
                                .map_err(ApiHttpError::from_api)?;
                            return respond_with_snapshot(snapshot, created);
                        }
                        // Cross-process reservation before becoming in-memory
                        // owner: peers with the same key converge to one run.
                        // An adopted run id from an expired claim binds the retry
                        // to the orphaned run instead of creating a second one.
                        // A carried id from the waiter path fills in when the
                        // claim file is already gone; a live claim reaped here
                        // takes precedence. When no Ready record exists, a
                        // run-side orphan sidecar (written after execution)
                        // recovers the original run after Ready loss: the retry
                        // adopts its journal instead of starting a fresh run
                        // (E03). A sidecar with a different digest conflicts
                        // instead of aliasing another request.
                        // Blocking work runs off the async runtime (E02).
                        // Scoped to run-creating requests only: answer,
                        // confirm, and cancel target an existing run and
                        // never need a full store scan per admission (E02).
                        if carried_adopted.is_none() && matches!(scope, "start_run" | "fork_run") {
                            let runs_dir = state.runs_dir.clone();
                            let key = idempotency_key.clone();
                            if let Some((orphan_run_id, orphan_digest)) =
                                find_orphan_run_async(runs_dir, key, policy_ttl).await?
                            {
                                if orphan_digest != request_digest {
                                    return Err(idempotency_conflict());
                                }
                                carried_adopted = Some(orphan_run_id);
                            }
                        }
                        let runs_dir = state.runs_dir.clone();
                        let key = idempotency_key.clone();
                        let digest = request_digest.clone();
                        let reserved = carried_adopted.clone().or(reserved_run_id.clone());
                        let adopted = match claim_durable_pending_async(
                            runs_dir, key, digest, reserved, policy_ttl,
                        )
                        .await?
                        {
                            ClaimOutcome::Peer => {
                                match wait_for_peer_ready(
                                    &state.runs_dir,
                                    &idempotency_key,
                                    &request_digest,
                                    policy_ttl,
                                )
                                .await?
                                {
                                    WaitOutcome::Ready(run_id) => {
                                        state.idempotency.lock().await.insert(
                                            idempotency_key.clone(),
                                            IdempotencyEntry::Ready {
                                                digest: request_digest.clone(),
                                                created_at: Instant::now(),
                                                run_id: run_id.clone(),
                                            },
                                        );
                                        let snapshot = state
                                            .service
                                            .snapshot(run_id)
                                            .await
                                            .map_err(ApiHttpError::from_api)?;
                                        return respond_with_snapshot(snapshot, created);
                                    }
                                    // The owner died before committing: loop back
                                    // and claim (adopting its run id) instead of
                                    // wedging on 503.
                                    WaitOutcome::RetryClaim { adopted_run_id } => {
                                        carried_adopted = carried_adopted.or(adopted_run_id);
                                        continue;
                                    }
                                }
                            }
                            ClaimOutcome::Owner {
                                run_id,
                                owner,
                                generation,
                            } => {
                                claim_owner = Some(owner);
                                claim_generation = Some(generation);
                                run_id
                            }
                            ClaimOutcome::Ready { run_id } => {
                                // Committed between our last look and the claim:
                                // converge without executing.
                                state.idempotency.lock().await.insert(
                                    idempotency_key.clone(),
                                    IdempotencyEntry::Ready {
                                        digest: request_digest.clone(),
                                        created_at: Instant::now(),
                                        run_id: run_id.clone(),
                                    },
                                );
                                let snapshot = state
                                    .service
                                    .snapshot(run_id)
                                    .await
                                    .map_err(ApiHttpError::from_api)?;
                                return respond_with_snapshot(snapshot, created);
                            }
                            ClaimOutcome::Conflict => {
                                return Err(idempotency_conflict());
                            }
                        };
                        // Re-examine the map under a fresh lock: a peer may have
                        // registered while disk was read or the claim published.
                        // Our durable claim is released on every diverge path so
                        // it never wedges the key (E02).
                        let mut idempotency = state.idempotency.lock().await;
                        // Clone only the fields needed after the lock drops:
                        // the entry holds an unclonable watch sender.
                        enum Reexamined {
                            Ready {
                                digest: String,
                                run_id: String,
                            },
                            Pending {
                                digest: String,
                                waiter: tokio::sync::watch::Receiver<bool>,
                            },
                            Absent,
                        }
                        let reexamined = match idempotency.get(&idempotency_key) {
                            Some(IdempotencyEntry::Ready { digest, run_id, .. }) => {
                                Reexamined::Ready {
                                    digest: digest.clone(),
                                    run_id: run_id.clone(),
                                }
                            }
                            Some(IdempotencyEntry::Pending {
                                digest, completed, ..
                            }) => Reexamined::Pending {
                                digest: digest.clone(),
                                waiter: completed.subscribe(),
                            },
                            None => Reexamined::Absent,
                        };
                        match reexamined {
                            Reexamined::Ready { digest, run_id } => {
                                // A peer committed while we claimed: our durable
                                // claim never executed anything, so release it
                                // and converge without executing (E02). A
                                // release failure fails fast with 503 instead
                                // of warning and parking peers until TTL: a
                                // corrupt or unreadable claim must surface
                                // immediately (E02). This path always returns,
                                // so no identity reset is needed.
                                let release = claim_owner.clone().zip(claim_generation);
                                drop(idempotency);
                                if let Some((owner, generation)) = release {
                                    release_durable_pending_or_503(
                                        state.runs_dir.clone(),
                                        idempotency_key.clone(),
                                        owner,
                                        generation,
                                    )
                                    .await?;
                                }
                                if digest != request_digest {
                                    return Err(idempotency_conflict());
                                }
                                let snapshot = state
                                    .service
                                    .snapshot(run_id)
                                    .await
                                    .map_err(ApiHttpError::from_api)?;
                                return respond_with_snapshot(snapshot, created);
                            }
                            Reexamined::Pending { digest, waiter } => {
                                let release = claim_owner.clone().zip(claim_generation);
                                drop(idempotency);
                                if let Some((owner, generation)) = release {
                                    // Fail fast with 503 on release failure
                                    // (E02): a corrupt claim must not degrade
                                    // to warn plus wait-until-TTL.
                                    release_durable_pending_or_503(
                                        state.runs_dir.clone(),
                                        idempotency_key.clone(),
                                        owner,
                                        generation,
                                    )
                                    .await?;
                                }
                                if digest != request_digest {
                                    return Err(idempotency_conflict());
                                }
                                // A peer owns this key in memory: wait on its
                                // completion instead of executing twice. The
                                // next Owner arm overwrites the claim identity
                                // before any read, so no reset is needed here.
                                (Some(waiter), None, None)
                            }
                            Reexamined::Absent => {
                                if idempotency.len() >= policy_max_entries {
                                    // Capacity overflow keeps the durable claim
                                    // across the 503 so a retry rejoins the
                                    // same claim instead of minting a new one
                                    // (E02). Releasing here would turn the
                                    // retry into a fresh claim with a new run.
                                    drop(idempotency);
                                    return Err(ApiHttpError::service_unavailable(
                                        "too many idempotent requests are still in progress",
                                    ));
                                }
                                let (completed, _) = tokio::sync::watch::channel(false);
                                let owner_id = uuid::Uuid::now_v7();
                                idempotency.insert(
                                    idempotency_key.clone(),
                                    IdempotencyEntry::Pending {
                                        digest: request_digest.clone(),
                                        owner_id,
                                        created_at: Instant::now(),
                                        completed,
                                    },
                                );
                                (None, Some(owner_id), adopted)
                            }
                        }
                    }
                }
            };
            if let Some(owner_id) = owner_id {
                break (owner_id, adopted_run_id);
            }
            if let Some(mut completed) = wait {
                if !*completed.borrow() {
                    let _ = completed.changed().await;
                }
                continue;
            }
            // Unreachable by construction (every iteration either waits or
            // assigns an owner), but an admission race must error rather than
            // panic if the invariant ever breaks.
            return Err(ApiHttpError::internal(
                "idempotency admission must wait or assign an owner",
            ));
        };
        // The loop only breaks through the Owner arm, which always sets the
        // claim identity first. A missing identity means the invariant broke:
        // fail closed instead of releasing with empty defaults.
        let (owner, generation) = match (claim_owner.clone(), claim_generation) {
            (Some(owner), Some(generation)) => (owner, generation),
            _ => {
                return Err(ApiHttpError::internal(
                    "idempotency admission completed without a claim identity",
                ));
            }
        };
        // Pre-execution ownership recheck: a descheduled owner whose claim
        // expired and was adopted while it slept must not start executing its
        // stale reservation — that run could never commit and would linger as
        // an orphan (E02). A successor that acted between the recheck and the
        // execution below is a millisecond-scale residual; the commit-time
        // ownership check and the orphan settlement below still converge it.
        // That residual may let this stale owner briefly start its engine;
        // such surplus execution never commits and is canceled by the orphan
        // settlement below (E02).
        match check_pending_owner_async(
            state.runs_dir.clone(),
            idempotency_key.clone(),
            owner,
            generation,
            request_digest.clone(),
        )
        .await?
        {
            OwnerCheck::Current => break 'admit (owner_id, adopted_run_id),
            OwnerCheck::Superseded | OwnerCheck::Absent => {
                // Withdraw our in-memory Pending entry (waking any same-key
                // waiter via sender drop) and re-enter the claim loop with the
                // adopted id riding along. Never execute a superseded
                // reservation.
                {
                    let mut idempotency = state.idempotency.lock().await;
                    if matches!(
                        idempotency.get(&idempotency_key),
                        Some(IdempotencyEntry::Pending {
                            owner_id: current,
                            ..
                        }) if *current == owner_id
                    ) {
                        idempotency.remove(&idempotency_key);
                    }
                }
                carried_adopted = carried_adopted.or(adopted_run_id);
                // No identity reset needed: the next Owner arm overwrites
                // both before any read.
                continue 'admit;
            }
        }
    };
    // The admit loop only breaks with a rechecked-current claim, which
    // always carries an identity. A missing identity means the invariant
    // broke: fail closed instead of releasing with empty defaults.
    let (claim_owner, claim_generation) = match (claim_owner, claim_generation) {
        (Some(owner), Some(generation)) => (owner, generation),
        _ => {
            return Err(ApiHttpError::internal(
                "idempotency admission completed without a claim identity",
            ));
        }
    };
    let mut pending_guard =
        PendingIdempotencyGuard::new(Arc::clone(state), idempotency_key.clone(), owner_id);
    let run_id = match execute(adopted_run_id).await {
        Ok(run_id) => run_id,
        Err(error) => {
            let completed = {
                let mut idempotency = state.idempotency.lock().await;
                if matches!(
                    idempotency.get(&idempotency_key),
                    Some(IdempotencyEntry::Pending {
                        owner_id: current,
                        ..
                    }) if *current == owner_id
                ) {
                    match idempotency.remove(&idempotency_key) {
                        Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                        _ => None,
                    }
                } else {
                    None
                }
            };
            pending_guard.disarm();
            // Release the cross-process claim so a retry can become owner.
            // A release failure fails fast with 503 (E02), overriding the
            // execution error: a corrupt claim must surface immediately.
            // Blocking work runs off the async runtime (E02).
            release_durable_pending_or_503(
                state.runs_dir.clone(),
                idempotency_key.clone(),
                claim_owner.clone(),
                claim_generation,
            )
            .await?;
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
            return Err(ApiHttpError::from_api(error));
        }
    };
    // Reverse mapping for Ready-loss recovery (E03): after execution, record
    // the key association inside the run directory itself so a retry after
    // Ready loss adopts the original run from its journal instead of
    // starting a fresh one. Only run-creating scopes need it; answer,
    // confirm, and cancel target an existing run. Best-effort: a missing
    // sidecar only loses the recovery path while the Ready record remains
    // primary. Blocking work runs off the async runtime (E02).
    if matches!(scope, "start_run" | "fork_run")
        && let Err(error) = write_orphan_record_async(
            state.runs_dir.clone(),
            run_id.clone(),
            idempotency_key.clone(),
            request_digest.clone(),
        )
        .await
    {
        tracing::warn!(%error, run_id = %run_id, "idempotency orphan record write failed; Ready commit remains primary");
    }
    // Durable commit before memory publish: a crash after HTTP success
    // must still return the same run_id on retry, and memory must never
    // advertise a Ready record that durable storage rejected (A03). The
    // commit must still own the claim it was admitted with; the returned
    // id is the committed mapping and may differ from the freshly executed
    // run when a same-digest Ready won the race (E02).
    // Settles a run whose result can never be returned: when the commit
    // loses to another mapping, the executed run is an orphan by
    // definition. Canceling converges it instead of burning resources
    // silently; a finished run makes cancel a no-op. A cancel failure is
    // propagated as an error instead of warn-only: an unsettled orphan must
    // surface (E02).
    let settle_orphan = |run_id: String| async move {
        state
            .service
            .cancel(run_id.clone())
            .await
            .map_err(ApiHttpError::from_api)
    };
    let committed_run_id = match store_durable_ready_async(
        state.runs_dir.clone(),
        idempotency_key.clone(),
        request_digest.clone(),
        run_id.clone(),
        claim_owner.clone(),
        claim_generation,
        policy_ttl,
    )
    .await
    {
        Ok(committed) => {
            if committed != run_id {
                // Same-digest race won by a peer's mapping: our execution
                // is surplus; converge it and return the winner (E02). A
                // cancel failure propagates instead of warn-only.
                settle_orphan(run_id.clone()).await?;
            }
            committed
        }
        Err(error) => {
            if matches!(error, StoreReadyError::DigestConflict) {
                release_durable_pending_or_503(
                    state.runs_dir.clone(),
                    idempotency_key.clone(),
                    claim_owner.clone(),
                    claim_generation,
                )
                .await?;
                settle_orphan(run_id.clone()).await?;
                return Err(idempotency_conflict());
            }
            // Stale-owner commit refusal with a committed same-digest Ready
            // is a surplus orphan by definition (the Ready winner already
            // serves this key): cancel the executed run so it cannot burn
            // resources silently, and attempt the owner-checked release
            // (a successor's live claim survives as a noop; our own stale
            // or expired claim is freed). A cancel failure propagates
            // instead of warn-only so an unsettled orphan surfaces (E02).
            // Without a committed Ready the run is kept for TTL adoption
            // (see below), never canceled here.
            // Re-verification is under the durable lock at commit time; a
            // successor stealing between the pre-execution recheck and the
            // end of execution cannot be narrowed further (execution is
            // long), so this orphan settlement is the airtight backstop:
            // every stale execution converges or cancels, never leaks (E02).
            if let Ok(Some(committed)) = load_durable_ready_async(
                state.runs_dir.clone(),
                idempotency_key.clone(),
                policy_ttl,
            )
            .await
                && committed.digest == request_digest
                && committed.run_id != run_id
            {
                settle_orphan(run_id.clone()).await?;
                // Best-effort release of our own stale claim (E02): a foreign
                // successor survives the owner check as a noop, so this
                // never deletes another owner's file. Unlike the execution
                // paths above, a release failure here does NOT degrade to
                // 503: this request already failed with the storage refusal
                // below (500), and masking it would hide the root cause. A
                // still-wedged claim surfaces as 503 on the next retry via
                // the normal claim path, so nothing is silently wedged.
                let _ = release_durable_pending_async(
                    state.runs_dir.clone(),
                    idempotency_key.clone(),
                    claim_owner.clone(),
                    claim_generation,
                )
                .await;
            }
            // Storage failure fails closed rather than risking duplicate
            // runs. The executed run is kept (not canceled) so TTL expiry
            // can adopt its id and converge instead of starting a second
            // run; a persistently broken disk surfaces as repeated errors,
            // never as silent duplicates. The memory Pending entry is
            // reverted so memory and durable agree that no Ready exists;
            // the durable pending claim is kept, so peers report 503 until
            // it expires (E02).
            let completed = {
                let mut idempotency = state.idempotency.lock().await;
                if matches!(
                    idempotency.get(&idempotency_key),
                    Some(IdempotencyEntry::Pending {
                        owner_id: current,
                        ..
                    }) if *current == owner_id
                ) {
                    match idempotency.remove(&idempotency_key) {
                        Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                        _ => None,
                    }
                } else {
                    None
                }
            };
            pending_guard.disarm();
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
            return Err(ApiHttpError::internal(format!(
                "failed to persist idempotency record: {error}"
            )));
        }
    };
    let completed = {
        let mut idempotency = state.idempotency.lock().await;
        let completed = if matches!(
            idempotency.get(&idempotency_key),
            Some(IdempotencyEntry::Pending {
                owner_id: current,
                ..
            }) if *current == owner_id
        ) {
            match idempotency.remove(&idempotency_key) {
                Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                _ => None,
            }
        } else {
            None
        };
        idempotency.insert(
            idempotency_key.clone(),
            IdempotencyEntry::Ready {
                digest: request_digest.clone(),
                created_at: Instant::now(),
                run_id: committed_run_id.clone(),
            },
        );
        completed
    };
    release_durable_pending_or_503(
        state.runs_dir.clone(),
        idempotency_key.clone(),
        claim_owner.clone(),
        claim_generation,
    )
    .await?;
    pending_guard.disarm();
    if let Some(completed) = completed {
        let _ = completed.send(true);
    }
    let snapshot = state
        .service
        .snapshot(committed_run_id)
        .await
        .map_err(ApiHttpError::from_api)?;
    respond_with_snapshot(snapshot, created)
}

pub(crate) fn idempotency_conflict() -> ApiHttpError {
    ApiHttpError::from_api(ApiError::Conflict {
        detail: "Idempotency-Key was already used with a different request".into(),
    })
}

/// Releases our durable claim, failing fast with 503 on failure instead of
/// degrading to warn plus wait-until-TTL. A corrupt or unreadable claim is
/// real damage: parking the request until TTL would wedge retries silently,
/// so the failure surfaces immediately for operator action (E02).
async fn release_durable_pending_or_503(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    owner: String,
    generation: u64,
) -> Result<(), ApiHttpError> {
    if let Err(error) = release_durable_pending_async(runs_dir, key, owner, generation).await {
        tracing::warn!(%error, "idempotency claim release failed; failing fast");
        return Err(ApiHttpError::service_unavailable(
            "idempotent request is still in progress elsewhere; retry with the same key",
        ));
    }
    Ok(())
}

/// Blocking filesystem claim offloaded from the async runtime: the
/// cross-process file lock blocks the OS thread, so it must never run on a
/// Tokio worker (E02).
async fn claim_durable_pending_async(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    digest: String,
    reserved_run_id: Option<String>,
    ttl: std::time::Duration,
) -> Result<ClaimOutcome, ApiHttpError> {
    tokio::task::spawn_blocking(move || {
        claim_durable_pending(&runs_dir, &key, &digest, reserved_run_id, ttl)
    })
    .await
    .map_err(|error| ApiHttpError::internal(format!("idempotency claim task failed: {error}")))?
}

/// Blocking Ready load offloaded from the async runtime (E02).
async fn load_durable_ready_async(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    ttl: std::time::Duration,
) -> Result<Option<durable::DurableIdempotencyRecord>, ApiHttpError> {
    tokio::task::spawn_blocking(move || load_durable_ready_result(&runs_dir, &key, ttl))
        .await
        .map_err(|error| ApiHttpError::internal(format!("idempotency load task failed: {error}")))?
        .map_err(|error| {
            ApiHttpError::internal(format!("failed to load idempotency record: {error}"))
        })
}

/// Blocking Ready commit offloaded from the async runtime (E02).
async fn store_durable_ready_async(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    digest: String,
    run_id: String,
    owner: String,
    generation: u64,
    ttl: std::time::Duration,
) -> Result<String, durable::StoreReadyError> {
    tokio::task::spawn_blocking(move || {
        store_durable_ready(&runs_dir, &key, &digest, &run_id, &owner, generation, ttl)
    })
    .await
    .map_err(|error| {
        durable::StoreReadyError::Storage(format!("idempotency commit task failed: {error}"))
    })?
}

/// Blocking claim release offloaded from the async runtime (E02).
async fn release_durable_pending_async(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    owner: String,
    generation: u64,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        release_durable_pending(&runs_dir, &key, &owner, generation)
    })
    .await
    .map_err(|error| format!("idempotency release task failed: {error}"))?
}

/// Blocking pre-execution ownership recheck offloaded from the async
/// runtime (E02).
async fn check_pending_owner_async(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    owner: String,
    generation: u64,
    expected_digest: String,
) -> Result<OwnerCheck, ApiHttpError> {
    tokio::task::spawn_blocking(move || {
        check_pending_owner(&runs_dir, &key, &owner, generation, &expected_digest)
    })
    .await
    .map_err(|error| ApiHttpError::internal(format!("idempotency recheck task failed: {error}")))?
    .map_err(ApiHttpError::internal)
}

/// Blocking orphan sidecar write offloaded from the async runtime (E02).
async fn write_orphan_record_async(
    runs_dir: camino::Utf8PathBuf,
    run_id: String,
    key: String,
    digest: String,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || write_orphan_record(&runs_dir, &run_id, &key, &digest))
        .await
        .map_err(|error| format!("idempotency orphan write task failed: {error}"))?
}

/// Blocking orphan scan offloaded from the async runtime (E02). A damaged
/// sidecar never fails the retry: it is skipped, never mistaken for
/// absence of the caller's own key without evidence.
async fn find_orphan_run_async(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    ttl: std::time::Duration,
) -> Result<Option<(String, String)>, ApiHttpError> {
    tokio::task::spawn_blocking(move || find_orphan_run(&runs_dir, &key, ttl))
        .await
        .map_err(|error| {
            ApiHttpError::internal(format!("idempotency orphan scan task failed: {error}"))
        })?
        .map_err(ApiHttpError::internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// Removes the temp runs dir on drop so a failed assertion cannot leak
    /// test directories.
    struct ModTempGuard(camino::Utf8PathBuf);
    impl Drop for ModTempGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.as_std_path());
        }
    }

    fn mod_test_state(runs: &camino::Utf8PathBuf) -> Arc<AppState> {
        let workspace = camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        Arc::new(AppState {
            service: crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
                .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: qcg_policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: qcg_policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    #[tokio::test]
    async fn recall_recheck_decision_withdraws_and_reclaims_with_real_state() {
        // The `with_idempotency` admit loop cannot be unit-isolated without
        // mocks: admission intertwines the async memory map, the blocking
        // durable file lock, and the service snapshot. Isolating the loop
        // body would require stubbing AppState or the filesystem, which is
        // forbidden. This test therefore drives the loop decision function
        // (`check_pending_owner`) in isolation with real tempdir-backed
        // durable state plus the exact memory-withdrawal step the loop runs
        // on Superseded/Absent, then reclaims through the real claim path.
        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-mod-recall-{}", uuid::Uuid::now_v7()));
        let _temp_guard = ModTempGuard(runs.clone());
        let state = mod_test_state(&runs);
        let key = "recall-decision".to_string();
        let digest = "digest".to_string();
        let owner_id = uuid::Uuid::now_v7();
        {
            let (completed, _) = tokio::sync::watch::channel(false);
            state.idempotency.lock().await.insert(
                key.clone(),
                IdempotencyEntry::Pending {
                    digest: digest.clone(),
                    owner_id,
                    created_at: Instant::now(),
                    completed,
                },
            );
        }
        // A live foreign claim reads as Superseded for a stale identity.
        let (owner, generation) = match claim_durable_pending(
            &runs,
            &key,
            &digest,
            Some("run-A".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("claim should not error")
        {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("expected owner, got {other:?}"),
        };
        assert!(matches!(
            check_pending_owner(&runs, &key, "stale-owner", 0, &digest),
            Ok(OwnerCheck::Superseded)
        ));
        assert!(matches!(
            check_pending_owner(&runs, &key, &owner, generation, &digest),
            Ok(OwnerCheck::Current)
        ));
        // The loop withdraws only its own memory entry on Superseded/Absent:
        // replicate that exact step with real state.
        {
            let mut idempotency = state.idempotency.lock().await;
            if matches!(
                idempotency.get(&key),
                Some(IdempotencyEntry::Pending {
                    owner_id: current,
                    ..
                }) if *current == owner_id
            ) {
                idempotency.remove(&key);
            }
        }
        assert!(
            !state.idempotency.lock().await.contains_key(&key),
            "a superseded owner must withdraw its memory entry before reclaiming"
        );
        // A released claim reads as Absent and reclaims as a fresh Owner.
        release_durable_pending(&runs, &key, &owner, generation).expect("release should succeed");
        assert!(matches!(
            check_pending_owner(&runs, &key, &owner, generation, &digest),
            Ok(OwnerCheck::Absent)
        ));
        match claim_durable_pending(
            &runs,
            &key,
            &digest,
            Some("run-A".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("reclaim should not error")
        {
            ClaimOutcome::Owner { .. } => {}
            other => panic!("absent claim must reclaim as owner, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_idempotency_recall_loop_converges_on_one_run() {
        // Smallest real harness for the full `with_idempotency`
        // Superseded/Gone recall-and-reclaim loop at the mod layer: two
        // concurrent same-key admissions through `with_idempotency` itself
        // (not the HTTP handler) with a real service and real tempdir
        // storage. Exactly one executes and both converge on one run.
        use axum::http::{HeaderMap, HeaderValue};
        use qcg_api::StartRun;
        use serde_json::json;

        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-mod-recall-loop-{}", uuid::Uuid::now_v7()));
        let _temp_guard = ModTempGuard(runs.clone());
        let state = mod_test_state(&runs);
        let request = StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
            ..Default::default()
        };
        let body = serde_json::to_vec(&request).expect("request should serialize");
        let mut headers = HeaderMap::new();
        headers.insert(
            qcg_policy::IDEMPOTENCY_HEADER,
            HeaderValue::from_static("mod-recall-loop"),
        );
        let call_once = |state: Arc<AppState>,
                         headers: HeaderMap,
                         body: Vec<u8>,
                         request: StartRun| async move {
            let reserved = state
                .service
                .reserve_start_run_id(&request.generator_id)
                .expect("reservation should succeed");
            let service = state.service.clone();
            let req = request.clone();
            with_idempotency(IdempotentCall {
                state: &state,
                headers: &headers,
                scope: "start_run",
                target: "",
                body: &body,
                created: true,
                reserved_run_id: Some(reserved),
                execute: |adopted| async move {
                    let Some(adopted) = adopted else {
                        return Err(qcg_api::ApiError::Internal {
                            detail: "idempotency reservation is missing; commit refused".into(),
                        });
                    };
                    service.start_run_with_id(req, Some(adopted)).await
                },
            })
            .await
        };
        let (first, second) = tokio::join!(
            call_once(
                Arc::clone(&state),
                headers.clone(),
                body.clone(),
                request.clone()
            ),
            call_once(Arc::clone(&state), headers, body, request)
        );
        let first = first.expect("first recall request should succeed");
        let second = second.expect("second recall request should converge");
        assert_eq!(
            first.headers().get(axum::http::header::LOCATION),
            second.headers().get(axum::http::header::LOCATION),
            "recall-and-reclaim must converge on one run"
        );
        assert_eq!(
            state
                .service
                .list_run_items()
                .await
                .expect("runs should list")
                .len(),
            1,
            "exactly one run must execute"
        );
    }

    #[tokio::test]
    async fn stale_commit_with_ready_present_cancels_surplus_through_with_idempotency() {
        // E02: the recheck-then-execute window cannot be narrowed further
        // (execution is long), so the orphan backstop must be airtight: a
        // stale-owner commit refusal with a committed same-digest Ready
        // cancels the surplus execution through `with_idempotency` instead
        // of leaking it. Real service, real tempdir storage, no mocks.
        use axum::http::{HeaderMap, HeaderValue};
        use qcg_api::StartRun;
        use serde_json::json;
        use sha2::Digest as _;

        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-mod-orphan-{}", uuid::Uuid::now_v7()));
        let _temp_guard = ModTempGuard(runs.clone());
        let state = mod_test_state(&runs);
        // Winner run that the planted Ready will point to.
        let winner = state
            .service
            .start_run(StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), json!("winner"))]),
                ..Default::default()
            })
            .await
            .expect("winner run should start");
        let request = StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("orphan"))]),
            ..Default::default()
        };
        let body = serde_json::to_vec(&request).expect("request should serialize");
        let mut headers = HeaderMap::new();
        headers.insert(
            qcg_policy::IDEMPOTENCY_HEADER,
            HeaderValue::from_static("orphan-cleanup-key"),
        );
        // Digest as `with_idempotency` computes it for scope start_run.
        let mut digest_input = Vec::with_capacity("start_run".len() + body.len() + 2);
        digest_input.extend_from_slice(b"start_run");
        digest_input.push(0);
        digest_input.push(0);
        digest_input.extend_from_slice(&body);
        let digest = hex::encode(sha2::Sha256::digest(&digest_input));
        let runs_for_execute = runs.clone();
        let winner_for_execute = winner.clone();
        let digest_for_execute = digest.clone();
        let key_for_execute = "orphan-cleanup-key".to_string();
        let reserved = state
            .service
            .reserve_start_run_id(&request.generator_id)
            .expect("reservation should succeed");
        let service = state.service.clone();
        let req = request.clone();
        let error = with_idempotency(IdempotentCall {
            state: &state,
            headers: &headers,
            scope: "start_run",
            target: "",
            body: &body,
            created: true,
            reserved_run_id: Some(reserved),
            execute: |adopted| async move {
                let Some(adopted) = adopted else {
                    return Err(qcg_api::ApiError::Internal {
                        detail: "idempotency reservation is missing; commit refused".into(),
                    });
                };
                // Execute the surplus run first.
                let surplus = service.start_run_with_id(req, Some(adopted)).await?;
                // Race: a same-digest Ready wins before our commit, and our
                // pending claim is damaged (Unusable) so the commit refuses
                // as stale instead of converging.
                let idempotency_dir = runs_for_execute.join("idempotency");
                std::fs::create_dir_all(idempotency_dir.as_std_path()).map_err(|error| {
                    qcg_api::ApiError::Internal {
                        detail: format!("test setup failed: {error}"),
                    }
                })?;
                let ready_path = idempotency_dir.join(format!(
                    "{}.json",
                    hex::encode(sha2::Sha256::digest(key_for_execute.as_bytes())),
                ));
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_secs())
                    .unwrap_or(1);
                let ready = serde_json::json!({
                    "key": key_for_execute,
                    "digest": digest_for_execute,
                    "run_id": winner_for_execute,
                    "created_at_unix": now,
                    "generation": 1u64,
                });
                std::fs::write(
                    ready_path.as_std_path(),
                    serde_json::to_vec(&ready).map_err(|error| qcg_api::ApiError::Internal {
                        detail: format!("test setup failed: {error}"),
                    })?,
                )
                .map_err(|error| qcg_api::ApiError::Internal {
                    detail: format!("test setup failed: {error}"),
                })?;
                let pending_path = idempotency_dir.join(format!(
                    "{}.pending.json",
                    hex::encode(sha2::Sha256::digest(key_for_execute.as_bytes())),
                ));
                std::fs::write(pending_path.as_std_path(), b"{torn").map_err(|error| {
                    qcg_api::ApiError::Internal {
                        detail: format!("test setup failed: {error}"),
                    }
                })?;
                Ok(surplus)
            },
        })
        .await
        .expect_err("stale commit with Ready present must fail closed");
        assert_eq!(
            error.problem.status,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            "stale storage refusal must surface as 500, got: {error:?}"
        );
        // The surplus orphan must be settled (canceled to terminal), not
        // left running: the backstop cancels it even though the commit was
        // refused.
        let surplus_id = {
            let items = state
                .service
                .list_run_items()
                .await
                .expect("runs should list");
            items
                .iter()
                .map(|item| item.run_id.clone())
                .find(|id| *id != winner)
                .expect("surplus run should exist")
        };
        let snapshot = state
            .service
            .snapshot(surplus_id.clone())
            .await
            .expect("surplus snapshot should load");
        assert!(
            snapshot.state.is_terminal(),
            "stale surplus run must be settled to terminal, got: {:?}",
            snapshot.state
        );
    }
}
