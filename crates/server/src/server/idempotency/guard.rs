//! Owner cleanup guard: an owner that fails or is dropped without
//! disarming reverts its in-memory pending entry and wakes its waiters, so
//! a dead owner never wedges retries on its key. Only the memory entry is
//! reverted here: proven pre-execution divergence may explicitly release the
//! durable claim, but ambiguous execute errors retain its evidence for the
//! existing lease/adoption window. Guard drop never deletes a successor's
//! claim file (E02). Durable mutations require the execution gate;
//! memory cleanup never grants ownership. Blocking service writers retain a
//! shared gate independently of this request-local guard.

use std::sync::Arc;

use crate::server::config::AppState;

use super::config::IdempotencyEntry;

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

    pub(super) async fn withdraw(state: Arc<AppState>, key: String, owner_id: uuid::Uuid) {
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
    }

    pub(crate) fn disarm(&mut self) {
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
        tokio::spawn(Self::withdraw(state, key, owner_id));
    }
}
