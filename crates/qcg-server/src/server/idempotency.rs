use anyhow::Result;
use axum::http::HeaderMap;
use axum::response::Response;
use camino::Utf8PathBuf;
use qcg_api::ApiError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::config::AppState;
use super::error::ApiHttpError;
use super::runs::respond_with_snapshot;
use qcg_policy::{IDEMPOTENCY_HEADER, IDEMPOTENCY_MAX_ENTRIES, IDEMPOTENCY_TTL};

#[derive(Debug)]
pub(crate) enum IdempotencyEntry {
    Pending {
        digest: String,
        owner_id: uuid::Uuid,
        created_at: Instant,
        completed: tokio::sync::watch::Sender<bool>,
    },
    Ready {
        digest: String,
        created_at: Instant,
        run_id: String,
    },
}

pub(crate) struct PendingIdempotencyGuard {
    state: Arc<AppState>,
    key: String,
    owner_id: uuid::Uuid,
    armed: bool,
}

impl PendingIdempotencyGuard {
    pub(crate) fn new(state: Arc<AppState>, key: String, owner_id: uuid::Uuid) -> Self {
        Self {
            state,
            key,
            owner_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingIdempotencyGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let state = Arc::clone(&self.state);
        let key = self.key.clone();
        let owner_id = self.owner_id;
        tokio::spawn(async move {
            let completed = {
                let mut entries = state.idempotency.lock().await;
                if matches!(
                    entries.get(&key),
                    Some(IdempotencyEntry::Pending {
                        owner_id: current,
                        ..
                    }) if *current == owner_id
                ) {
                    match entries.remove(&key) {
                        Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                        _ => None,
                    }
                } else {
                    None
                }
            };
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
        });
    }
}

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

/// Runs a run mutation under an idempotency key when one is supplied.
/// `scope`, `target`, and `body` feed the request digest, so a reused key
/// with different content conflicts instead of aliasing another request.
/// Retried execution is safe: fork admits a single owner per key, and the
/// interaction endpoints already dedupe identical payloads.
pub(crate) async fn with_idempotency<F, Fut>(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    scope: &str,
    target: &str,
    body: &[u8],
    created: bool,
    execute: F,
) -> Result<Response, ApiHttpError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<String, ApiError>>,
{
    let mut digest_input = Vec::with_capacity(scope.len() + target.len() + body.len() + 2);
    digest_input.extend_from_slice(scope.as_bytes());
    digest_input.push(0);
    digest_input.extend_from_slice(target.as_bytes());
    digest_input.push(0);
    digest_input.extend_from_slice(body);
    let request_digest = format!("{:x}", Sha256::digest(&digest_input));
    let Some(idempotency_key) = idempotency_key(headers)? else {
        let run_id = execute().await.map_err(ApiHttpError::from_api)?;
        let snapshot = state
            .service
            .snapshot(run_id)
            .await
            .map_err(ApiHttpError::from_api)?;
        return respond_with_snapshot(snapshot, created);
    };
    let owner_id = loop {
        let (wait, owner_id) = {
            let mut idempotency = state.idempotency.lock().await;
            prune_idempotency(&mut idempotency, Instant::now());
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
                    (Some(completed.subscribe()), None)
                }
                None => {
                    // Cross-restart and cross-process guarantee: a durable
                    // Ready record wins over starting a duplicate run.
                    if let Some(durable) = load_durable_ready(&state.runs_dir, &idempotency_key) {
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
                    if idempotency.len() >= IDEMPOTENCY_MAX_ENTRIES {
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
                    (None, Some(owner_id))
                }
            }
        };
        if let Some(owner_id) = owner_id {
            break owner_id;
        }
        if let Some(mut completed) = wait {
            if !*completed.borrow() {
                let _ = completed.changed().await;
            }
            continue;
        }
        unreachable!("idempotency admission must wait or assign an owner");
    };
    let mut pending_guard =
        PendingIdempotencyGuard::new(Arc::clone(state), idempotency_key.clone(), owner_id);
    let run_id = match execute().await {
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
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
            return Err(ApiHttpError::from_api(error));
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
                run_id: run_id.clone(),
            },
        );
        completed
    };
    // Durable commit before responding: a crash after HTTP success must
    // still return the same run_id on retry with the same key.
    if let Err(error) =
        store_durable_ready(&state.runs_dir, &idempotency_key, &request_digest, &run_id)
    {
        if error.contains("different request") {
            return Err(idempotency_conflict());
        }
        // Storage failure fails closed rather than risking duplicate runs.
        return Err(ApiHttpError::internal(format!(
            "failed to persist idempotency record: {error}"
        )));
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableIdempotencyRecord {
    key: String,
    digest: String,
    run_id: String,
    created_at_unix: u64,
}

fn idempotency_dir(runs_dir: &Utf8PathBuf) -> Utf8PathBuf {
    runs_dir.join("idempotency")
}

fn idempotency_path(runs_dir: &Utf8PathBuf, key: &str) -> Utf8PathBuf {
    idempotency_dir(runs_dir).join(format!("{:x}.json", Sha256::digest(key.as_bytes())))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn ttl_secs() -> u64 {
    IDEMPOTENCY_TTL.as_secs().max(1)
}

/// Load a durable Ready record, returning None when absent or expired.
/// Expired files are removed best-effort.
pub(crate) fn load_durable_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
) -> Option<DurableIdempotencyRecord> {
    let path = idempotency_path(runs_dir, key);
    let bytes = std::fs::read(path.as_std_path()).ok()?;
    let record: DurableIdempotencyRecord = serde_json::from_slice(&bytes).ok()?;
    if record.key != key {
        return None;
    }
    if now_unix().saturating_sub(record.created_at_unix) >= ttl_secs() {
        let _ = std::fs::remove_file(path.as_std_path());
        return None;
    }
    Some(record)
}

/// Persist a Ready record atomically (create_new temp + rename) so restart
/// and peer processes observe the same operation id to run id mapping.
/// Same key + same digest is idempotent; same key + different digest is a
/// conflict preserved from the existing record.
pub(crate) fn store_durable_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    run_id: &str,
) -> Result<(), String> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| error.to_string())?;
    let path = idempotency_path(runs_dir, key);
    if let Some(existing) = load_durable_ready(runs_dir, key) {
        if existing.digest != digest {
            return Err("Idempotency-Key was already used with a different request".into());
        }
        return Ok(());
    }
    let record = DurableIdempotencyRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.to_string(),
        created_at_unix: now_unix(),
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
    // create_new avoids clobbering a concurrent peer's record; on
    // AlreadyExists re-read and compare digests for conflict accuracy.
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.as_std_path())
    {
        Ok(mut file) => {
            use std::io::Write as _;
            file.write_all(&bytes).map_err(|error| error.to_string())?;
            file.sync_data().map_err(|error| error.to_string())?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            match load_durable_ready(runs_dir, key) {
                Some(existing) if existing.digest == digest => Ok(()),
                Some(_) => Err("Idempotency-Key was already used with a different request".into()),
                // Expired between check and create; retry once via rename.
                None => {
                    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
                    std::fs::write(tmp.as_std_path(), &bytes).map_err(|error| error.to_string())?;
                    std::fs::rename(tmp.as_std_path(), path.as_std_path())
                        .map_err(|error| error.to_string())?;
                    Ok(())
                }
            }
        }
        Err(error) => Err(error.to_string()),
    }
}

pub(crate) fn prune_idempotency(entries: &mut BTreeMap<String, IdempotencyEntry>, now: Instant) {
    entries.retain(|_, entry| {
        let created_at = match entry {
            IdempotencyEntry::Pending { created_at, .. }
            | IdempotencyEntry::Ready { created_at, .. } => created_at,
        };
        now.duration_since(*created_at) < IDEMPOTENCY_TTL
    });
    while entries.len() >= IDEMPOTENCY_MAX_ENTRIES {
        let Some(oldest) = entries
            .iter()
            .filter_map(|(key, entry)| match entry {
                IdempotencyEntry::Ready { created_at, .. } => Some((key, *created_at)),
                IdempotencyEntry::Pending { .. } => None,
            })
            .min_by_key(|(_, created_at)| *created_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        entries.remove(&oldest);
    }
}
