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
    ClaimOutcome, StoreReadyError, WaitOutcome, claim_durable_pending, load_durable_ready_result,
    release_durable_pending, store_durable_ready, wait_for_peer_ready,
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
        let run_id = execute(None).await.map_err(ApiHttpError::from_api)?;
        let snapshot = state
            .service
            .snapshot(run_id)
            .await
            .map_err(ApiHttpError::from_api)?;
        return respond_with_snapshot(snapshot, created);
    };
    // An orphaned run id observed while waiting rides across claim-loop
    // iterations: expiry observation already reaped the pending file, so a
    // later re-read cannot recover it. The durable claim owner rides along
    // too, so every release below only ever removes our own claim file.
    let mut carried_adopted: Option<String> = None;
    let mut claim_owner: Option<String> = None;
    let mut claim_generation: Option<u64> = None;
    let (owner_id, adopted_run_id) = loop {
        let (wait, owner_id, adopted_run_id) = {
            let mut idempotency = state.idempotency.lock().await;
            prune_idempotency(&mut idempotency, Instant::now()).map_err(ApiHttpError::internal)?;
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
                    // Cross-restart and cross-process guarantee: a durable
                    // Ready record wins over starting a duplicate run.
                    match load_durable_ready_result(&state.runs_dir, &idempotency_key) {
                        Ok(Some(durable)) => {
                            if durable.digest != request_digest {
                                return Err(idempotency_conflict());
                            }
                            let run_id = durable.run_id.clone();
                            // Refresh in-memory cache for subsequent hits.
                            idempotency.insert(
                                idempotency_key.clone(),
                                IdempotencyEntry::Ready {
                                    digest: durable.digest,
                                    created_at: Instant::now(),
                                    run_id: run_id.clone(),
                                },
                            );
                            drop(idempotency);
                            let snapshot = state
                                .service
                                .snapshot(run_id)
                                .await
                                .map_err(ApiHttpError::from_api)?;
                            return respond_with_snapshot(snapshot, created);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            // Corrupt or unreadable durable records fail
                            // closed instead of starting a duplicate run.
                            return Err(ApiHttpError::internal(format!(
                                "failed to load idempotency record: {error}"
                            )));
                        }
                    }
                    // Cross-process reservation before becoming in-memory
                    // owner: peers with the same key converge to one run.
                    // An adopted run id from an expired claim binds the retry
                    // to the orphaned run instead of creating a second one.
                    // A carried id from the waiter path fills in when the
                    // claim file is already gone; a live claim reaped here
                    // takes precedence.
                    let adopted = match claim_durable_pending(
                        &state.runs_dir,
                        &idempotency_key,
                        &request_digest,
                        carried_adopted.clone().or(reserved_run_id.clone()),
                    )? {
                        ClaimOutcome::Peer => {
                            drop(idempotency);
                            match wait_for_peer_ready(
                                &state.runs_dir,
                                &idempotency_key,
                                &request_digest,
                            )
                            .await?
                            {
                                WaitOutcome::Ready(run_id) => {
                                    let mut idempotency = state.idempotency.lock().await;
                                    idempotency.insert(
                                        idempotency_key.clone(),
                                        IdempotencyEntry::Ready {
                                            digest: request_digest.clone(),
                                            created_at: Instant::now(),
                                            run_id: run_id.clone(),
                                        },
                                    );
                                    drop(idempotency);
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
                            idempotency.insert(
                                idempotency_key.clone(),
                                IdempotencyEntry::Ready {
                                    digest: request_digest.clone(),
                                    created_at: Instant::now(),
                                    run_id: run_id.clone(),
                                },
                            );
                            drop(idempotency);
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
                    if idempotency.len()
                        >= effective_idempotency_max_entries().map_err(ApiHttpError::internal)?
                    {
                        if let (Some(owner), Some(generation)) =
                            (claim_owner.as_deref(), claim_generation)
                            && let Err(error) = release_durable_pending(
                                &state.runs_dir,
                                &idempotency_key,
                                owner,
                                generation,
                            )
                        {
                            tracing::warn!(%error, "idempotency claim release failed; TTL expiry will adopt it");
                        }
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
    // claim identity first. Empty fallbacks release nothing (no owner id
    // is ever empty, generation 0 never matches a live claim) and any
    // stranded file still expires via TTL.
    let claim_owner = claim_owner.unwrap_or_default();
    let claim_generation = claim_generation.unwrap_or(0);
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
            if let Err(error) = release_durable_pending(
                &state.runs_dir,
                &idempotency_key,
                &claim_owner,
                claim_generation,
            ) {
                tracing::warn!(%error, "idempotency claim release failed; TTL expiry will adopt it");
            }
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
            return Err(ApiHttpError::from_api(error));
        }
    };
    // Durable commit before memory publish: a crash after HTTP success
    // must still return the same run_id on retry, and memory must never
    // advertise a Ready record that durable storage rejected (A03).
    if let Err(error) = store_durable_ready(
        &state.runs_dir,
        &idempotency_key,
        &request_digest,
        &run_id,
        claim_generation,
    ) {
        if matches!(error, StoreReadyError::DigestConflict) {
            if let Err(error) = release_durable_pending(
                &state.runs_dir,
                &idempotency_key,
                &claim_owner,
                claim_generation,
            ) {
                tracing::warn!(%error, "idempotency claim release failed; TTL expiry will adopt it");
            }
            return Err(idempotency_conflict());
        }
        // Storage failure fails closed rather than risking duplicate runs.
        // The memory Pending entry is reverted so memory and durable agree
        // that no Ready exists; the durable pending claim is kept, so peers
        // report 503 until it expires. Expiry then re-enters the claim loop
        // and adopts the orphaned run id, converging onto the
        // already-created run instead of starting a second one. A
        // persistently broken disk surfaces as repeated errors, never as
        // silent duplicates.
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
                run_id: run_id.clone(),
            },
        );
        completed
    };
    if let Err(error) = release_durable_pending(
        &state.runs_dir,
        &idempotency_key,
        &claim_owner,
        claim_generation,
    ) {
        tracing::warn!(%error, "idempotency claim release failed; TTL expiry will adopt it");
    }
    pending_guard.disarm();
    if let Some(completed) = completed {
        let _ = completed.send(true);
    }
    let snapshot = state
        .service
        .snapshot(run_id.clone())
        .await
        .map_err(ApiHttpError::from_api)?;
    respond_with_snapshot(snapshot, created)
}

pub(crate) fn idempotency_conflict() -> ApiHttpError {
    ApiHttpError::from_api(ApiError::Conflict {
        detail: "Idempotency-Key was already used with a different request".into(),
    })
}
