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
    ClaimOutcome, ExecutionGate, OwnerCheck, StoreReadyError, WaitOutcome, find_orphan_run,
    load_durable_ready_result, wait_for_peer_ready, write_orphan_record,
};
#[cfg(test)]
pub(crate) use durable::{check_pending_owner, claim_durable_pending, release_durable_pending};
pub(crate) use guard::PendingIdempotencyGuard;

use anyhow::Result;
use api::ApiError;
use axum::http::HeaderMap;
use axum::response::Response;
use policy::IDEMPOTENCY_HEADER;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Instant;

use crate::server::config::AppState;
use crate::server::error::ApiHttpError;
use crate::server::runs::respond_with_snapshot;

pub(crate) fn request_digest(
    scope: &str,
    target: &str,
    body: &[u8],
    context: Option<&[u8]>,
) -> String {
    let mut digest_input = Vec::with_capacity(scope.len() + target.len() + body.len() + 2);
    digest_input.extend_from_slice(scope.as_bytes());
    digest_input.push(0);
    digest_input.extend_from_slice(target.as_bytes());
    digest_input.push(0);
    digest_input.extend_from_slice(body);
    if let Some(context) = context {
        digest_input.push(0);
        digest_input.extend_from_slice(context);
    }
    hex::encode(Sha256::digest(&digest_input))
}

pub(crate) fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiHttpError> {
    Ok(headers
        .get(IDEMPOTENCY_HEADER)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("Idempotency-Key must be valid ASCII"))?
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string))
}

/// Private HTTP executor inputs; service receives no HTTP claim policy.
pub(crate) struct ExecutorContext {
    pub(crate) reserved_run_id: Option<String>,
    pub(crate) operation_id: Option<String>,
}

pub(crate) struct IdempotentCall<'a, F, Fut>
where
    F: FnOnce(ExecutorContext) -> Fut,
    Fut: std::future::Future<Output = Result<String, ApiError>>,
{
    pub(crate) state: &'a Arc<AppState>,
    pub(crate) headers: &'a HeaderMap,
    pub(crate) scope: &'a str,
    pub(crate) target: &'a str,
    pub(crate) body: &'a [u8],
    /// Additional request identity, used only by start runs. Other mutation
    /// digests remain byte-for-byte unchanged when this is `None`.
    pub(crate) digest_context: Option<&'a [u8]>,
    pub(crate) created: bool,
    /// Start/fork reservation, or mutation target checked against any adopted id.
    pub(crate) reserved_run_id: Option<String>,
    pub(crate) execute: F,
}

/// Read-only status of a start request's current idempotency claim.
pub(crate) enum StartIdempotencyLookup {
    Ready(Response),
    Pending,
    Absent,
}

/// One bounded snapshot body and validator for initial responses and all replays.
async fn snapshot_response(
    state: &Arc<AppState>,
    run_id: String,
    created: bool,
) -> Result<Response, ApiHttpError> {
    let result = async {
        // Snapshot bodies share response-budget admission on success and replay.
        // Response failure must not undo the already committed Ready record.
        #[cfg(test)]
        execution_gate_tests::snapshot_barrier(&state.runs_dir).await;
        let snapshot = state
            .service
            .snapshot_with_limits(run_id.clone(), &state.artifact_limits)
            .await
            .map_err(ApiHttpError::from_api)?;
        let (body, reservation) = super::artifact_response::serialize_json(
            &snapshot,
            state.artifact_limits.effective().max_bytes.unwrap(),
            &state.artifact_response_budget,
        )?;
        respond_with_snapshot(body, Some(reservation), run_id.clone(), created)
    }
    .await;
    result.map_err(|mut error| {
        if created {
            error.location = format!("/api/runs/{run_id}").parse().ok();
        }
        error
    })
}

/// Replay/join before package preflight; absent/expired claims need current-package validation.
pub(crate) async fn lookup_start_idempotency<F, Fut>(
    call: &IdempotentCall<'_, F, Fut>,
) -> Result<StartIdempotencyLookup, ApiHttpError>
where
    F: FnOnce(ExecutorContext) -> Fut,
    Fut: std::future::Future<Output = Result<String, ApiError>>,
{
    let Some(key) = idempotency_key(call.headers)? else {
        return Ok(StartIdempotencyLookup::Absent);
    };
    let digest = request_digest(call.scope, call.target, call.body, call.digest_context);
    let memory_pending = {
        let entries = call.state.idempotency.lock().await;
        match entries.get(&key) {
            Some(IdempotencyEntry::Pending {
                digest: stored,
                created_at,
                ..
            }) if created_at.elapsed() < call.state.idempotency_ttl => {
                if stored != &digest {
                    return Err(idempotency_conflict());
                }
                true
            }
            _ => false,
        }
    };
    if let Some(record) = load_durable_ready_async(
        call.state.runs_dir.clone(),
        key.clone(),
        call.state.idempotency_ttl,
    )
    .await?
        && let Some(response) = replay_ready(call.state, record, call.created, &digest).await?
    {
        return Ok(StartIdempotencyLookup::Ready(response));
    }

    if memory_pending {
        return Ok(StartIdempotencyLookup::Pending);
    }
    let directory = call.state.runs_dir.clone();
    let pending_key = key.clone();
    if let Some(pending_digest) =
        storage(move || durable::load_durable_pending_digest_result(&directory, &pending_key))
            .await?
    {
        if pending_digest != digest {
            return Err(idempotency_conflict());
        }
        return Ok(StartIdempotencyLookup::Pending);
    }

    // A pending owner can publish Ready between the first Ready read and the
    // pending observation. Re-read before declaring this request new.
    if let Some(record) = load_durable_ready_async(
        call.state.runs_dir.clone(),
        key.clone(),
        call.state.idempotency_ttl,
    )
    .await?
        && let Some(response) = replay_ready(call.state, record, call.created, &digest).await?
    {
        return Ok(StartIdempotencyLookup::Ready(response));
    }
    Ok(StartIdempotencyLookup::Absent)
}

/// Admit keyed mutations by scope/target/body/context digest (None preserves legacy
/// bytes). Start/fork reserve before claiming and adopt after crashes; mutations
/// require the adopted reservation to match their original target.
pub(crate) async fn with_idempotency<F, Fut>(
    call: IdempotentCall<'_, F, Fut>,
) -> Result<Response, ApiHttpError>
where
    F: FnOnce(ExecutorContext) -> Fut,
    Fut: std::future::Future<Output = Result<String, ApiError>>,
{
    let IdempotentCall {
        state,
        headers,
        scope,
        target,
        body,
        digest_context,
        created,
        reserved_run_id,
        execute,
    } = call;
    let request_digest = request_digest(scope, target, body, digest_context);
    let Some(idempotency_key) = idempotency_key(headers)? else {
        // Unkeyed requests skip the claim/commit protocol, but a run-creating
        // handler has already reserved its run id; the executor must receive
        // it so the run is created under that reservation instead of failing
        // closed. Answer/confirm/cancel executors check it against their target.
        let run_id = execute(ExecutorContext {
            reserved_run_id: reserved_run_id.clone(),
            operation_id: None,
        })
        .await
        .map_err(ApiHttpError::from_api)?;
        return snapshot_response(state, run_id, created).await;
    };
    // Use only boot-frozen policy, never per-request environment reads.
    let policy_ttl = state.idempotency_ttl;
    let policy_max_entries = state.idempotency_max_entries;
    // Carry waiter run association across retries. Owner/generation and the
    // acceptance ID always come from the latest gated durable Owner outcome.
    let mut carried_adopted: Option<String> = None;
    let mut execution_gate = None;
    let mut claim_owner: Option<String> = None;
    let mut claim_generation: Option<u64> = None;
    let mut operation_id = None;
    let (owner_id, adopted_run_id, mut pending_guard) = 'admit: loop {
        let (owner_id, adopted_run_id, mut pending_guard) = loop {
            let (wait, owner_id, adopted_run_id) = {
                let mut idempotency = state.idempotency.lock().await;
                prune_idempotency(
                    &mut idempotency,
                    Instant::now(),
                    durable::now_unix().map_err(ApiHttpError::internal)?,
                    policy_ttl,
                    policy_max_entries,
                )
                .map_err(ApiHttpError::internal)?;
                match idempotency.get(&idempotency_key) {
                    Some(IdempotencyEntry::Ready { record, .. }) => {
                        let record = record.clone();
                        drop(idempotency);
                        if let Some(response) =
                            replay_ready(state, record.clone(), created, &request_digest).await?
                        {
                            return Ok(response);
                        }
                        let mut entries = state.idempotency.lock().await;
                        if matches!(entries.get(&idempotency_key), Some(IdempotencyEntry::Ready { record: current, .. }) if current == &record)
                        {
                            entries.remove(&idempotency_key);
                        }
                        continue;
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
                        // Durable claims arbitrate peers; recheck memory after off-thread I/O.
                        drop(idempotency);
                        // Canonical Ready outranks a new execution, including after restart.
                        let runs_dir = state.runs_dir.clone();
                        let key = idempotency_key.clone();
                        if let Some(durable) =
                            load_durable_ready_async(runs_dir, key, policy_ttl).await?
                            && let Some(response) =
                                replay_ready(state, durable, created, &request_digest).await?
                        {
                            return Ok(response);
                        }
                        // Only run creation needs orphan-sidecar discovery.
                        // Mutations bind to their target without a store scan.
                        if carried_adopted.is_none() && matches!(scope, "start_run" | "fork_run") {
                            let runs_dir = state.runs_dir.clone();
                            let key = idempotency_key.clone();
                            if let Some((orphan_run_id, orphan_digest)) =
                                storage(move || find_orphan_run(&runs_dir, &key, policy_ttl))
                                    .await?
                            {
                                if orphan_digest != request_digest {
                                    return Err(idempotency_conflict());
                                }
                                carried_adopted = Some(orphan_run_id);
                            }
                        }
                        let key = idempotency_key.clone();
                        let digest = request_digest.clone();
                        let reserved = carried_adopted.clone().or(reserved_run_id.clone());
                        let gate = acquire_execution_gate(
                            state.runs_dir.clone(),
                            idempotency_key.clone(),
                            request_digest.clone(),
                        )
                        .await?;
                        execution_gate = Some(Arc::clone(&gate));
                        let adopted = match claim_durable_pending_async(
                            gate,
                            key,
                            digest,
                            reserved,
                            policy_ttl,
                            state.idempotency_sweep.clone(),
                            !matches!(scope, "start_run" | "fork_run"),
                        )
                        .await?
                        {
                            ClaimOutcome::Peer => {
                                execution_gate.take();
                                match wait_for_peer_ready(
                                    &state.runs_dir,
                                    &idempotency_key,
                                    &request_digest,
                                    policy_ttl,
                                )
                                .await?
                                {
                                    WaitOutcome::Ready(record) => {
                                        if let Some(response) =
                                            replay_ready(state, record, created, &request_digest)
                                                .await?
                                        {
                                            return Ok(response);
                                        }
                                        continue;
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
                                operation_id: accepted_id,
                                run_id,
                                owner,
                                generation,
                            } => {
                                operation_id = Some(accepted_id);
                                claim_owner = Some(owner);
                                claim_generation = Some(generation);
                                run_id
                            }
                            ClaimOutcome::Ready(record) => {
                                // Committed between our last look and the claim.
                                execution_gate.take();
                                if let Some(response) =
                                    replay_ready(state, record, created, &request_digest).await?
                                {
                                    return Ok(response);
                                }
                                continue;
                            }
                            ClaimOutcome::Conflict => {
                                return Err(idempotency_conflict());
                            }
                        };
                        // Disk work raced local registration: recheck before
                        // inserting, releasing our durable claim on divergence.
                        let mut idempotency = state.idempotency.lock().await;
                        // Clone only the fields needed after the lock drops:
                        // the entry holds an unclonable watch sender.
                        enum Reexamined {
                            Ready,
                            Pending {
                                digest: String,
                                waiter: tokio::sync::watch::Receiver<bool>,
                            },
                            Absent,
                        }
                        let reexamined = match idempotency.get(&idempotency_key) {
                            Some(IdempotencyEntry::Ready { record, .. })
                                if !record.expired_at(
                                    durable::now_unix().map_err(ApiHttpError::internal)?,
                                    policy_ttl,
                                ) =>
                            {
                                Reexamined::Ready
                            }
                            Some(IdempotencyEntry::Pending {
                                digest, completed, ..
                            }) => Reexamined::Pending {
                                digest: digest.clone(),
                                waiter: completed.subscribe(),
                            },
                            None | Some(IdempotencyEntry::Ready { .. }) => Reexamined::Absent,
                        };
                        match reexamined {
                            Reexamined::Ready => {
                                // We have not executed: release our exact claim
                                // and replay. Release failures must surface as 503.
                                let release = claim_owner.clone().zip(claim_generation);
                                drop(idempotency);
                                if let Some((owner, generation)) = release {
                                    release_durable_pending_or_503(
                                        Arc::clone(execution_gate.as_ref().ok_or_else(|| {
                                            ApiHttpError::internal("missing execution gate")
                                        })?),
                                        idempotency_key.clone(),
                                        owner,
                                        generation,
                                    )
                                    .await?;
                                }
                                execution_gate.take();
                                continue;
                            }
                            Reexamined::Pending { digest, waiter } => {
                                let release = claim_owner.clone().zip(claim_generation);
                                drop(idempotency);
                                if let Some((owner, generation)) = release {
                                    // Fail fast with 503 on release failure
                                    // (E02): a corrupt claim must not degrade
                                    // to warn plus wait-until-TTL.
                                    release_durable_pending_or_503(
                                        Arc::clone(execution_gate.as_ref().ok_or_else(|| {
                                            ApiHttpError::internal("missing execution gate")
                                        })?),
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
                                execution_gate.take();
                                (Some(waiter), None, None)
                            }
                            Reexamined::Absent => {
                                if idempotency.len() >= policy_max_entries {
                                    // Retain durable Pending across 503 so retries adopt the same run.
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
                                // Arm memory cleanup under the registration lock, before
                                // any fallible await (including the owner recheck).
                                let guard = PendingIdempotencyGuard::new(
                                    Arc::clone(state),
                                    idempotency_key.clone(),
                                    owner_id,
                                );
                                (None, Some((owner_id, guard)), adopted)
                            }
                        }
                    }
                }
            };
            if let Some((owner_id, guard)) = owner_id {
                break (owner_id, adopted_run_id, guard);
            }
            if let Some(mut completed) = wait {
                if !*completed.borrow() {
                    let _ = completed.changed().await;
                }
                continue;
            }
            // Fail closed on a broken admission invariant.
            return Err(ApiHttpError::internal(
                "idempotency admission must wait or assign an owner",
            ));
        };
        // Fail closed if the owner arm omitted its claim identity.
        let (owner, generation) = claim_owner.clone().zip(claim_generation).ok_or_else(|| {
            ApiHttpError::internal("idempotency admission completed without a claim identity")
        })?;
        // Claim, validation and service acceptance share the same gate.
        // Expiry observations cannot authorize replacement while it is held.
        match check_pending_owner_async(
            Arc::clone(
                execution_gate
                    .as_ref()
                    .ok_or_else(|| ApiHttpError::internal("missing execution gate"))?,
            ),
            idempotency_key.clone(),
            owner,
            generation,
            request_digest.clone(),
        )
        .await?
        {
            OwnerCheck::Current => break 'admit (owner_id, adopted_run_id, pending_guard),
            OwnerCheck::Superseded | OwnerCheck::Absent => {
                // Withdraw memory/wake waiters, then reclaim carrying the adopted ID.
                PendingIdempotencyGuard::withdraw(
                    Arc::clone(state),
                    idempotency_key.clone(),
                    owner_id,
                )
                .await;
                pending_guard.disarm();
                carried_adopted = carried_adopted.or(adopted_run_id);
                execution_gate.take();
                continue 'admit;
            }
        }
    };
    // Admission must carry a complete claim identity; never release with defaults.
    let ((claim_owner, claim_generation), operation_id) = claim_owner
        .zip(claim_generation)
        .zip(operation_id)
        .ok_or_else(|| {
            ApiHttpError::internal("idempotency admission completed without a claim identity")
        })?;
    let gate = Arc::clone(
        execution_gate
            .as_ref()
            .ok_or_else(|| ApiHttpError::internal("missing execution gate"))?,
    );
    let run_id = match service::with_acceptance_guard(
        gate.clone(),
        execute(ExecutorContext {
            reserved_run_id: adopted_run_id,
            operation_id: Some(operation_id),
        }),
    )
    .await
    {
        Ok(run_id) => run_id,
        Err(error) => {
            // Errors do not prove absence of effects: cancel may already have
            // published a mailbox. Retain the durable claim and its acceptance
            // ID; withdraw only memory waiters. Other route reconciliation is
            // still incomplete, so this can hold even an effect-free failure.
            PendingIdempotencyGuard::withdraw(Arc::clone(state), idempotency_key.clone(), owner_id)
                .await;
            pending_guard.disarm();
            return Err(ApiHttpError::from_api(error));
        }
    };
    // Best-effort run-side reverse mapping recovers start/fork after Ready
    // loss. Mutation targets need no orphan scan or sidecar; Ready is primary.
    if matches!(scope, "start_run" | "fork_run")
        && let Err(error) = write_orphan_record_async(
            gate.clone(),
            state.runs_dir.clone(),
            run_id.clone(),
            idempotency_key.clone(),
            request_digest.clone(),
        )
        .await
    {
        tracing::warn!(%error, run_id = %run_id, "idempotency orphan record write failed; Ready commit remains primary");
    }
    // Commit before memory publication or HTTP success. Damaged/external state
    // may make a same-digest winner differ: cancel surplus execution, surfacing
    // settlement errors. The gate excludes such races among conforming peers.
    let settle_orphan = |run_id: String| {
        let gate = gate.clone();
        async move {
            service::with_acceptance_guard(gate, state.service.cancel(run_id))
                .await
                .map_err(ApiHttpError::from_api)
        }
    };
    let mut snapshot_guard = Some(
        state
            .service
            .lock_idempotency_snapshot(&run_id)
            .await
            .map_err(ApiHttpError::from_api)?,
    );
    let committed = match store_durable_ready_async(
        gate.clone(),
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
            if committed.run_id != run_id {
                snapshot_guard.take();
                // Settle surplus execution before returning the winner.
                settle_orphan(run_id.clone()).await?;
            }
            committed
        }
        Err(error) => {
            snapshot_guard.take();
            if matches!(error, StoreReadyError::DigestConflict) {
                release_durable_pending_or_503(
                    gate.clone(),
                    idempotency_key.clone(),
                    claim_owner.clone(),
                    claim_generation,
                )
                .await?;
                settle_orphan(run_id.clone()).await?;
                return Err(idempotency_conflict());
            }
            // Only a committed same-digest winner proves surplus execution.
            // Otherwise preserve the executed run for adoption, not cancellation.
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
                // Owner-checked release cannot remove a successor. Keep the original
                // storage error if this best-effort release also fails.
                let _ = release_durable_pending_async(
                    gate.clone(),
                    idempotency_key.clone(),
                    claim_owner.clone(),
                    claim_generation,
                )
                .await;
            }
            // Withdraw memory only; retain durable Pending and its executed run for
            // reconciliation. Age alone never retires ambiguous evidence.
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
    let committed_record = committed.clone();
    let committed_run_id = committed.run_id.clone();
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
        idempotency.insert(idempotency_key.clone(), IdempotencyEntry::ready(committed));
        completed
    };
    release_durable_pending_or_503(
        Arc::clone(
            execution_gate
                .as_ref()
                .ok_or_else(|| ApiHttpError::internal("missing execution gate"))?,
        ),
        idempotency_key.clone(),
        claim_owner.clone(),
        claim_generation,
    )
    .await?;
    pending_guard.disarm();
    if let Some(completed) = completed {
        let _ = completed.send(true);
    }
    drop(gate);
    drop(execution_gate);
    if snapshot_guard.is_some() {
        snapshot_response(state, committed_run_id, created).await
    } else {
        replay_ready(state, committed_record, created, &request_digest)
            .await?
            .ok_or_else(|| {
                ApiHttpError::service_unavailable(
                    "idempotency mapping changed; retry with the same key",
                )
            })
    }
}

/// Admission precedes the final exact canonical identity/TTL check. Expiry or
/// replacement falls through to normal claiming; cached records cannot replay
/// a journal that GC was allowed to collect. No execution-shard read lock.
async fn replay_ready(
    state: &Arc<AppState>,
    record: durable::DurableIdempotencyRecord,
    created: bool,
    expected_digest: &str,
) -> Result<Option<Response>, ApiHttpError> {
    let _admission = state
        .service
        .lock_idempotency_snapshot(&record.run_id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let dir = state.runs_dir.clone();
    let key = record.key.clone();
    let current = storage(move || durable::read_ready(&dir, &key)).await?;
    let expired = record.expired_at(
        durable::now_unix().map_err(ApiHttpError::internal)?,
        state.idempotency_ttl,
    );
    if current.as_ref() != Some(&record) || expired {
        if current.is_none() && !expired {
            return Err(ApiHttpError::internal(
                "unexpired idempotency Ready disappeared",
            ));
        }
        return Ok(None);
    }
    if record.digest != expected_digest {
        return Err(idempotency_conflict());
    }
    state
        .idempotency
        .lock()
        .await
        .insert(record.key.clone(), IdempotencyEntry::ready(record.clone()));
    snapshot_response(state, record.run_id, created)
        .await
        .map(Some)
}

pub(crate) fn idempotency_conflict() -> ApiHttpError {
    ApiHttpError::from_api(ApiError::Conflict {
        detail: "Idempotency-Key was already used with a different request".into(),
    })
}

/// Release failure surfaces as 503, never silent waiting over damaged evidence.
async fn release_durable_pending_or_503(
    gate: Arc<ExecutionGate>,
    key: String,
    owner: String,
    generation: u64,
) -> Result<(), ApiHttpError> {
    if let Err(error) = release_durable_pending_async(gate, key, owner, generation).await {
        tracing::warn!(%error, "idempotency claim release failed; failing fast");
        return Err(ApiHttpError::service_unavailable(
            "idempotent request is still in progress elsewhere; retry with the same key",
        ));
    }
    Ok(())
}

/// Dropping this future stops retries; no blocking lock wait survives it.
async fn acquire_execution_gate(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    digest: String,
) -> Result<Arc<ExecutionGate>, ApiHttpError> {
    let deadline = tokio::time::Instant::now() + durable::PEER_WAIT;
    loop {
        let dir = runs_dir.clone();
        let key = key.clone();
        let expected = digest.clone();
        if let Some(gate) = tokio::task::spawn_blocking(move || {
            let gate = ExecutionGate::try_acquire(&dir, &key).map_err(ApiHttpError::internal)?;
            // Observation can reject conflicts, never grant ownership.
            if gate.is_none()
                && durable::load_durable_pending_digest_result(&dir, &key)
                    .map_err(ApiHttpError::internal)?
                    .is_some_and(|stored| stored != expected)
            {
                return Err(idempotency_conflict());
            }
            Ok(gate)
        })
        .await
        .map_err(|e| ApiHttpError::internal(e.to_string()))??
        {
            return Ok(Arc::new(gate));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ApiHttpError::service_unavailable(
                "idempotent request is still in progress elsewhere; retry with the same key",
            ));
        }
        tokio::time::sleep(durable::PEER_POLL).await;
    }
}

/// Off-thread claim publication under the execution gate.
async fn claim_durable_pending_async(
    gate: Arc<ExecutionGate>,
    key: String,
    digest: String,
    reserved_run_id: Option<String>,
    ttl: std::time::Duration,
    sweep: Arc<durable::IdempotencySweep>,
    require_target: bool,
) -> Result<ClaimOutcome, ApiHttpError> {
    tokio::task::spawn_blocking(move || {
        durable::claim_durable_pending_for_target_gated(
            &gate,
            &key,
            &digest,
            reserved_run_id,
            ttl,
            &sweep,
            require_target,
        )
    })
    .await
    .map_err(|error| ApiHttpError::internal(format!("idempotency claim task failed: {error}")))?
}

async fn load_durable_ready_async(
    runs_dir: camino::Utf8PathBuf,
    key: String,
    ttl: std::time::Duration,
) -> Result<Option<durable::DurableIdempotencyRecord>, ApiHttpError> {
    storage(move || load_durable_ready_result(&runs_dir, &key, ttl)).await
}

/// Every filesystem storage closure runs off Tokio; join/I/O errors fail closed.
async fn storage<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, ApiHttpError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(ApiHttpError::internal)?
        .map_err(ApiHttpError::internal)
}

async fn store_durable_ready_async(
    gate: Arc<ExecutionGate>,
    key: String,
    digest: String,
    run_id: String,
    owner: String,
    generation: u64,
    ttl: std::time::Duration,
) -> Result<durable::DurableIdempotencyRecord, durable::StoreReadyError> {
    tokio::task::spawn_blocking(move || {
        durable::store_durable_ready_gated(&gate, &key, &digest, &run_id, &owner, generation, ttl)
    })
    .await
    .map_err(|error| {
        durable::StoreReadyError::Storage(format!("idempotency commit task failed: {error}"))
    })?
}

async fn release_durable_pending_async(
    gate: Arc<ExecutionGate>,
    key: String,
    owner: String,
    generation: u64,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        durable::release_durable_pending_gated(&gate, &key, &owner, generation)
    })
    .await
    .map_err(|error| format!("idempotency release task failed: {error}"))?
}

async fn check_pending_owner_async(
    gate: Arc<ExecutionGate>,
    key: String,
    owner: String,
    generation: u64,
    expected_digest: String,
) -> Result<OwnerCheck, ApiHttpError> {
    #[cfg(test)]
    tests::owner_recheck_boundary().await;
    storage(move || {
        durable::check_pending_owner_gated(&gate, &key, &owner, generation, &expected_digest)
    })
    .await
}

async fn write_orphan_record_async(
    gate: Arc<ExecutionGate>,
    runs_dir: camino::Utf8PathBuf,
    run_id: String,
    key: String,
    digest: String,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        let _gate = gate;
        write_orphan_record(&runs_dir, &run_id, &key, &digest)
    })
    .await
    .map_err(|error| format!("idempotency orphan write task failed: {error}"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    // Task-local one-shot boundary: no global hook or wall-clock scheduling race.
    tokio::task_local! {
        static OWNER_RECHECK: std::cell::RefCell<Option<(
            tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>,
        )>>;
    }

    pub(super) async fn owner_recheck_boundary() {
        let hook = OWNER_RECHECK
            .try_with(|hook| hook.borrow_mut().take())
            .ok()
            .flatten();
        if let Some((reached, resume)) = hook {
            let _ = reached.send(());
            let _ = resume.await;
        }
    }

    #[tokio::test]
    async fn owner_recheck_abort_and_error_wake_memory_waiters_and_allow_retry() {
        use std::time::Duration;
        for mode in ["abort", "error"] {
            let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .unwrap()
                .join(format!("mod-recheck-{mode}-{}", uuid::Uuid::now_v7()));
            let _temp_guard = ModTempGuard(runs.clone());
            let state = mod_test_state(&runs);
            let reserved = state
                .service
                .reserve_start_run_id("hello-template")
                .unwrap();
            let key = "recheck-boundary";
            let mut headers = HeaderMap::new();
            headers.insert(policy::IDEMPOTENCY_HEADER, key.parse().unwrap());
            let (reached, at_boundary) = tokio::sync::oneshot::channel();
            let (resume, wait) = tokio::sync::oneshot::channel();
            let owner_state = state.clone();
            let owner_headers = headers.clone();
            let owner_reserved = reserved.clone();
            let owner = tokio::spawn(OWNER_RECHECK.scope(
                std::cell::RefCell::new(Some((reached, wait))),
                async move {
                    with_idempotency(IdempotentCall {
                        state: &owner_state,
                        headers: &owner_headers,
                        scope: "start_run",
                        target: "",
                        body: b"recheck",
                        digest_context: None,
                        created: true,
                        reserved_run_id: Some(owner_reserved),
                        execute: |_| async { panic!("failed owner must not execute") },
                    })
                    .await
                },
            ));
            tokio::time::timeout(Duration::from_secs(5), at_boundary)
                .await
                .unwrap()
                .unwrap();
            let mut completed = match state.idempotency.lock().await.get(key).unwrap() {
                IdempotencyEntry::Pending { completed, .. } => completed.subscribe(),
                _ => panic!("owner must be Pending before recheck"),
            };
            let path = runs.join("idempotency").join(format!(
                "{}.pending.json",
                hex::encode(Sha256::digest(key.as_bytes()))
            ));
            let original = std::fs::read(&path).unwrap();
            if mode == "abort" {
                owner.abort();
                assert!(owner.await.unwrap_err().is_cancelled());
                assert_eq!(std::fs::read(&path).unwrap(), original);
            } else {
                // Real recheck error, not a mocked outcome. Restore the exact
                // evidence afterward so retry exercises the normal claim path.
                std::fs::write(&path, b"not-json").unwrap();
                resume.send(()).unwrap();
                assert_eq!(owner.await.unwrap().unwrap_err().problem.status, 500);
                std::fs::write(&path, &original).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), completed.changed())
                .await
                .unwrap()
                .unwrap();
            assert!(*completed.borrow());
            assert!(!state.idempotency.lock().await.contains_key(key));
            // Controlled lease-expiry seam: do not wait 60s, and do not
            // change product retention. Memory cleanup must not wait Ready TTL.
            let mut claim: serde_json::Value = serde_json::from_slice(&original).unwrap();
            claim["created_at_unix"] = 1.into();
            std::fs::write(&path, serde_json::to_vec(&claim).unwrap()).unwrap();
            let request = api::StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), serde_json::json!("recheck"))]),
                ..Default::default()
            };
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                with_idempotency(IdempotentCall {
                    state: &state,
                    headers: &headers,
                    scope: "start_run",
                    target: "",
                    body: b"recheck",
                    digest_context: None,
                    created: true,
                    reserved_run_id: None,
                    execute: |context| async {
                        let adopted = context.reserved_run_id;
                        assert_eq!(adopted.as_ref(), Some(&reserved));
                        state.service.start_run_with_id(request, adopted).await
                    },
                }),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(result.status(), axum::http::StatusCode::CREATED);
            assert_eq!(
                durable::load_durable_ready_result(&runs, key, policy::IDEMPOTENCY_TTL)
                    .unwrap()
                    .unwrap()
                    .run_id,
                reserved
            );
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn withdrawn_memory_owner_cannot_remove_successor() {
        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("mod-successor-{}", uuid::Uuid::now_v7()));
        let _temp_guard = ModTempGuard(runs.clone());
        let state = mod_test_state(&runs);
        let key = "successor".to_owned();
        let old_owner = uuid::Uuid::now_v7();
        let successor = uuid::Uuid::now_v7();
        let (completed, mut waiter) = tokio::sync::watch::channel(false);
        state.idempotency.lock().await.insert(
            key.clone(),
            IdempotencyEntry::Pending {
                digest: "digest".into(),
                owner_id: successor,
                created_at: Instant::now(),
                completed,
            },
        );
        // Await the exact cleanup used by Drop, with a superseded identity;
        // no scheduler timing is needed to prove the successor survives it.
        PendingIdempotencyGuard::withdraw(state.clone(), key.clone(), old_owner).await;
        assert!(matches!(state.idempotency.lock().await.get(&key),
            Some(IdempotencyEntry::Pending { owner_id, .. }) if *owner_id == successor));
        assert!(!waiter.has_changed().unwrap());
        PendingIdempotencyGuard::withdraw(state.clone(), key.clone(), successor).await;
        waiter.changed().await.unwrap();
        assert!(*waiter.borrow());
        assert!(!state.idempotency.lock().await.contains_key(&key));
    }

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
            idempotency_ttl: policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: policy::IDEMPOTENCY_MAX_ENTRIES,
            idempotency_sweep: Default::default(),
            api_token_digest: None,
            artifact_limits: service::ArtifactZipLimits::default(),
            artifact_response_budget:
                crate::server::artifact_response::ArtifactResponseBudget::new(None).unwrap(),
            asset_limit: None,
            max_request_bytes: None,
            metrics_policy: crate::server::MetricsPolicy::default(),
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
            .join(format!("mod-recall-{}", uuid::Uuid::now_v7()));
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
            policy::IDEMPOTENCY_TTL,
            &state.idempotency_sweep,
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
            policy::IDEMPOTENCY_TTL,
            &state.idempotency_sweep,
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
        use api::StartRun;
        use axum::http::{HeaderMap, HeaderValue};
        use serde_json::json;

        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("mod-recall-loop-{}", uuid::Uuid::now_v7()));
        let _temp_guard = ModTempGuard(runs.clone());
        let state = mod_test_state(&runs);
        let request = StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("test"))]),
            ..Default::default()
        };
        let body = serde_json::to_vec(&request).expect("request should serialize");
        let mut headers = HeaderMap::new();
        headers.insert(
            policy::IDEMPOTENCY_HEADER,
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
                digest_context: None,
                created: true,
                reserved_run_id: Some(reserved),
                execute: |context| async move {
                    let adopted = context.reserved_run_id;
                    let Some(adopted) = adopted else {
                        return Err(api::ApiError::Internal {
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
    async fn execute_error_preserves_durable_claim_for_ambiguous_acceptance() {
        // Observed ambiguous acceptance: `service.cancel` publishes its
        // durable mailbox before returning Conflict/error. The `with_idempotency`
        // execution error must therefore preserve the durable Pending claim;
        // only the process-local waiter entry is withdrawn and woken. A
        // same-key retry must rejoin as Peer, never immediately execute as a
        // fresh owner. Real service and tempdir storage, no mocks.
        use axum::http::{HeaderMap, HeaderValue};

        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("mod-ambiguous-{}", uuid::Uuid::now_v7()));
        let _temp_guard = ModTempGuard(runs.clone());
        let state = mod_test_state(&runs);
        let key = "ambiguous-acceptance";
        let scope = "cancel";
        let run_id = state
            .service
            .start_run(api::StartRun {
                generator_id: "ask-user".into(),
                ..Default::default()
            })
            .await
            .expect("mutation target should exist before claim publication");
        let target = run_id.as_str();
        let body = b"cancel-payload";
        let digest = request_digest(scope, target, body, None);
        let mut headers = HeaderMap::new();
        headers.insert(
            policy::IDEMPOTENCY_HEADER,
            HeaderValue::from_str(key).expect("key should be a valid header"),
        );
        let side_effect = runs.join("mailbox-side-effect");
        let side_effect_for_execute = side_effect.clone();
        let error = with_idempotency(IdempotentCall {
            state: &state,
            headers: &headers,
            scope,
            target,
            body,
            digest_context: None,
            created: false,
            reserved_run_id: Some(target.to_string()),
            execute: |context| async move {
                let reserved = context.reserved_run_id;
                assert_eq!(
                    reserved.as_deref(),
                    Some(target),
                    "reservation must ride into execution"
                );
                // Simulate the accepted cancel mailbox before the error.
                std::fs::write(&side_effect_for_execute, b"mailbox").map_err(|error| {
                    api::ApiError::Internal {
                        detail: format!("test setup failed: {error}"),
                    }
                })?;
                Err(api::ApiError::Conflict {
                    detail: "run `run-1` is executing elsewhere; cancel was signaled".into(),
                })
            },
        })
        .await
        .expect_err("ambiguous execution error must surface");
        assert_eq!(
            error.problem.status,
            axum::http::StatusCode::CONFLICT.as_u16(),
            "ambiguous cancel error must surface as 409, got: {error:?}"
        );
        assert!(
            side_effect.is_file(),
            "the accepted side effect must exist before the error is assessed"
        );
        // Durable evidence remains under the same identity.
        assert_eq!(
            durable::load_durable_pending_digest_result(&runs, key)
                .expect("pending digest should read"),
            Some(digest.clone()),
            "the durable claim digest must survive the execution error"
        );
        let pending_path = runs.join("idempotency").join(format!(
            "{}.pending.json",
            hex::encode(Sha256::digest(key.as_bytes()))
        ));
        let before = std::fs::read(pending_path.as_std_path())
            .expect("pending claim file must survive the execution error");
        assert!(
            !state.idempotency.lock().await.contains_key(key),
            "the process-local waiter entry must be withdrawn on execution error"
        );
        // Same-key same-digest work cannot immediately execute as a fresh
        // owner: it rejoins as Peer. A different digest still conflicts.
        assert!(
            matches!(
                claim_durable_pending(
                    &runs,
                    key,
                    &digest,
                    Some("run-fresh".into()),
                    policy::IDEMPOTENCY_TTL,
                    &state.idempotency_sweep,
                )
                .expect("reclaim should not fail"),
                ClaimOutcome::Peer
            ),
            "preserved claim must force a same-digest retry into Peer, never Owner"
        );
        assert!(
            matches!(
                claim_durable_pending(
                    &runs,
                    key,
                    "different-digest",
                    Some("run-fresh".into()),
                    policy::IDEMPOTENCY_TTL,
                    &state.idempotency_sweep,
                )
                .expect("conflict check should not fail"),
                ClaimOutcome::Conflict
            ),
            "preserved claim must still conflict a different digest"
        );
        assert_eq!(
            std::fs::read(pending_path.as_std_path()).expect("pending should still read"),
            before,
            "a failed fresh claim must not replace the preserved evidence"
        );
    }

    #[tokio::test]
    async fn stale_commit_with_ready_present_cancels_surplus_through_with_idempotency() {
        // Defensive behavior under externally damaged state: gate-aware peers
        // cannot supersede a live execution, so this test deliberately bypasses
        // the protocol to plant Ready plus a corrupt Pending. The refusal must
        // still settle a surplus run. This is not a supported peer takeover.
        // Real service and tempdir storage, no mocks.
        use api::StartRun;
        use axum::http::{HeaderMap, HeaderValue};
        use serde_json::json;
        use sha2::Digest as _;

        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("mod-orphan-{}", uuid::Uuid::now_v7()));
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
            policy::IDEMPOTENCY_HEADER,
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
            digest_context: None,
            created: true,
            reserved_run_id: Some(reserved),
            execute: |context| async move {
                let adopted = context.reserved_run_id;
                let Some(adopted) = adopted else {
                    return Err(api::ApiError::Internal {
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
                    api::ApiError::Internal {
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
                    serde_json::to_vec(&ready).map_err(|error| api::ApiError::Internal {
                        detail: format!("test setup failed: {error}"),
                    })?,
                )
                .map_err(|error| api::ApiError::Internal {
                    detail: format!("test setup failed: {error}"),
                })?;
                let pending_path = idempotency_dir.join(format!(
                    "{}.pending.json",
                    hex::encode(sha2::Sha256::digest(key_for_execute.as_bytes())),
                ));
                std::fs::write(pending_path.as_std_path(), b"{torn").map_err(|error| {
                    api::ApiError::Internal {
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

    #[test]
    fn start_digest_includes_package_identity_but_other_mutation_digests_stay_unchanged() {
        let body = br#"{"generator_id":"demo","inputs":{}}"#;
        let first = request_digest("start_run", "", body, Some(&[b'a'; 64]));
        let second = request_digest("start_run", "", body, Some(&[b'b'; 64]));
        assert_ne!(
            first, second,
            "a changed package identity changes start request identity"
        );

        let mut legacy = Vec::new();
        legacy.extend_from_slice(b"answer");
        legacy.push(0);
        legacy.extend_from_slice(b"run-1");
        legacy.push(0);
        legacy.extend_from_slice(body);
        assert_eq!(
            request_digest("answer", "run-1", body, None),
            hex::encode(Sha256::digest(&legacy)),
            "other mutation digests preserve the prior byte layout",
        );
    }
}

#[cfg(test)]
#[path = "tests/execution_gate.rs"]
mod execution_gate_tests;
