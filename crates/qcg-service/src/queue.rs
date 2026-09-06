use crate::types::RunRecord;
use qcg_api::RunStatus;
use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};

/// 1-based positions of queued runs in schedule order: higher priority
/// first, then earlier admission, then run id.
/// Runs without a recorded admission time sort last, ties break by run id.
pub(crate) fn queue_positions(runs: &BTreeMap<String, RunRecord>) -> BTreeMap<String, usize> {
    let mut queued: Vec<(&String, &RunRecord)> = runs
        .iter()
        .filter(|(_, record)| record.state == RunStatus::Queued)
        .collect();
    queued.sort_by(|left, right| queue_order(left.1, left.0).cmp(&queue_order(right.1, right.0)));
    queued
        .into_iter()
        .enumerate()
        .map(|(index, (run_id, _))| (run_id.clone(), index.saturating_add(1)))
        .collect()
}

/// Queue order: higher priority first, then earlier admission, then run id.
/// Waiting behind a higher-priority run is normal scheduling, not starvation
/// of equals: equal priorities keep FIFO order.
fn queue_order<'a>(
    record: &'a RunRecord,
    run_id: &'a str,
) -> (
    std::cmp::Reverse<i32>,
    Option<chrono::DateTime<chrono::Utc>>,
    &'a str,
) {
    (std::cmp::Reverse(record.priority), record.queued_at, run_id)
}

/// Highest-priority queued run id, if any.
pub(crate) fn queue_head(runs: &BTreeMap<String, RunRecord>) -> Option<String> {
    runs.iter()
        .filter(|(_, record)| record.state == RunStatus::Queued)
        .min_by(|left, right| queue_order(left.1, left.0).cmp(&queue_order(right.1, right.0)))
        .map(|(run_id, _)| run_id.clone())
}

/// Execution slot pool with priority-ordered wakeups. A plain semaphore
/// would wake the longest-waiting task, letting a low-priority run take a
/// slot freed for a higher-priority one; here only the queue head proceeds
/// and every waiter rechecks on each notification.
#[derive(Debug)]
pub(crate) struct PriorityPermits {
    available: std::sync::Mutex<usize>,
}

/// RAII execution slot. Dropping returns the slot and wakes waiters.
#[derive(Debug)]
pub(crate) struct Permit {
    permits: Arc<PriorityPermits>,
    notify: Arc<tokio::sync::Notify>,
}

impl PriorityPermits {
    pub(crate) fn new(count: usize) -> Self {
        Self {
            available: std::sync::Mutex::new(count),
        }
    }

    /// Non-blocking take. The counter lock is never held across awaits.
    pub(crate) fn try_take(
        permits: &Arc<Self>,
        notify: Arc<tokio::sync::Notify>,
    ) -> Option<Permit> {
        let mut available = permits
            .available
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if *available == 0 {
            return None;
        }
        *available -= 1;
        drop(available);
        Some(Permit {
            permits: Arc::clone(permits),
            notify,
        })
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        *self
            .permits
            .available
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;
        self.notify.notify_waiters();
    }
}
