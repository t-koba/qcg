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
    bool,
    Option<chrono::DateTime<chrono::Utc>>,
    &'a str,
) {
    // Runs without a recorded admission time sort last; Option's default
    // ordering would put None first, contradicting the documented FIFO rule.
    (
        std::cmp::Reverse(record.priority),
        record.queued_at.is_none(),
        record.queued_at,
        run_id,
    )
}

/// Highest-priority queued run id, if any.
pub(crate) fn queue_head(runs: &BTreeMap<String, RunRecord>) -> Option<String> {
    runs.iter()
        .filter(|(_, record)| record.state == RunStatus::Queued)
        .min_by(|left, right| queue_order(left.1, left.0).cmp(&queue_order(right.1, right.0)))
        .map(|(run_id, _)| run_id.clone())
}

/// Victim for priority preemption: capacity is judged on total running runs,
/// then the lowest-priority run below the arrival priority is evicted.
/// Equal priorities never preempt each other.
pub(crate) fn select_preemption_victim(
    running: &[(&str, i32)],
    max_active_runs: usize,
    arrival_priority: i32,
) -> Option<String> {
    if running.len() < max_active_runs {
        return None;
    }
    running
        .iter()
        .filter(|(_, priority)| *priority < arrival_priority)
        .min_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(left.0)))
        .map(|(run_id, _)| run_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preemption_evicts_lowest_priority_only_when_full() {
        let running = [("high", 10), ("low", 0)];
        // Capacity 2, arrival 5: only the lower-priority run is evictable.
        assert_eq!(
            select_preemption_victim(&running, 2, 5).as_deref(),
            Some("low")
        );
        // Free slot: no preemption even with an evictable candidate.
        assert_eq!(select_preemption_victim(&running, 3, 5), None);
        // No candidate below arrival: no preemption when full.
        assert_eq!(select_preemption_victim(&running, 2, 0), None);
        assert_eq!(select_preemption_victim(&[("high", 10)], 1, 5), None);
        // Equal priorities never preempt each other.
        assert_eq!(select_preemption_victim(&[("same", 5)], 1, 5), None);
        // Ties prefer the larger run id, matching scheduler order.
        assert_eq!(
            select_preemption_victim(&[("a", 0), ("b", 0)], 2, 5).as_deref(),
            Some("b")
        );
    }
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
