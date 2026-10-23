//! One elapsed interval per engine episode, never per node or parallel task.
//! Deliberately no Drop settlement: aborts/crashes leave an open durable anchor.
use super::types::EngineError;
use crate::{JournalWriter, StepError};
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time::Instant;

/// Exact measured usage, separate from the unchanged enforcement deadline.
#[derive(Clone, Copy, Debug)]
pub struct ElapsedClock {
    pub deadline: Instant,
    pub measured_at: Instant,
    pub used: Duration,
}
impl ElapsedClock {
    pub fn used(&self) -> Duration {
        self.used + self.measured_at.elapsed()
    }
}

pub(super) struct Episode {
    started: Instant,
    total: u64,
    closed: bool,
}

fn invalid(message: &str) -> EngineError {
    EngineError::Failed(format!("active elapsed accounting refused: {message}"))
}

fn nanos(duration: Duration) -> Result<u64, EngineError> {
    u64::try_from(duration.as_nanos()).map_err(|_| invalid("nanoseconds overflow"))
}

fn add(total: u64, duration: Duration) -> Result<u64, EngineError> {
    total
        .checked_add(nanos(duration)?)
        .ok_or_else(|| invalid("active total overflow"))
}

fn record(journal: &JournalWriter, total: u64, anchor: Option<String>) -> Result<(), EngineError> {
    journal.event(
        "elapsed_accounted",
        api::ElapsedAccountedEventData {
            active_nanoseconds: total,
            active_started_at: anchor,
        },
    )?;
    Ok(())
}

/// Under the execution lock, recover downtime once for an open interval only.
pub(super) fn recover(journal: &JournalWriter, now: DateTime<Utc>) -> Result<u64, EngineError> {
    let ledger = journal
        .state()
        .budget
        .elapsed
        .clone()
        .ok_or_else(|| invalid("missing ledger"))?;
    if let Some(anchor) = ledger.active_started_at {
        let anchor =
            DateTime::parse_from_rfc3339(&anchor).map_err(|_| invalid("invalid anchor"))?;
        let elapsed = now
            .signed_duration_since(anchor)
            .to_std()
            .map_err(|_| invalid("wall clock moved backwards"))?;
        let total = add(ledger.active_nanoseconds, elapsed)?;
        record(journal, total, None)?;
        Ok(total)
    } else {
        Ok(ledger.active_nanoseconds)
    }
}

impl Episode {
    pub(super) fn open(journal: &JournalWriter, total: u64) -> Result<Self, EngineError> {
        let wall_started = Utc::now();
        let started = Instant::now();
        record(journal, total, Some(wall_started.to_rfc3339()))?;
        Ok(Self {
            started,
            total,
            closed: false,
        })
    }

    pub(super) fn deadline(&self, remaining: Duration) -> Result<ElapsedClock, EngineError> {
        let deadline = self
            .started
            .checked_add(remaining)
            .ok_or_else(|| invalid("deadline overflow"))?;
        Ok(ElapsedClock {
            deadline,
            measured_at: self.started,
            used: Duration::from_nanos(self.total),
        })
    }

    pub(super) fn close(&mut self, journal: &JournalWriter) -> Result<(), EngineError> {
        if self.closed {
            return Ok(());
        }
        // Live monotonic accounting cannot be vetoed by recovery wall time.
        record(journal, add(self.total, self.started.elapsed())?, None)?;
        self.closed = true;
        Ok(())
    }
}

pub(super) fn remaining(limit: u64, total: u64) -> Result<Duration, EngineError> {
    Duration::from_secs(limit)
        .checked_sub(Duration::from_nanos(total))
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| {
            // The allowance was exhausted before a step could start (E11).
            StepError::ElapsedExpiredBeforeStart {
                limit_secs: limit,
                used: Duration::from_nanos(total),
            }
            .into()
        })
}

pub(super) fn close(
    episode: &mut Option<Episode>,
    journal: &JournalWriter,
) -> Result<(), EngineError> {
    if let Some(episode) = episode {
        episode.close(journal)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn journal(label: &str) -> (camino::Utf8PathBuf, JournalWriter) {
        let dir = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("elapsed-{label}-{}", uuid::Uuid::now_v7()));
        let writer =
            JournalWriter::create(&dir.join("journal.jsonl"), "elapsed", false, None).unwrap();
        writer
            .event(
                "run_queued",
                json!({
                    "generator": "test@1", "generator_path": "test", "contract_sha256": "test",
                    "inputs": {}, "schema_version": 2, "elapsed_budget_mode": "active_execution"
                }),
            )
            .unwrap();
        (dir, writer)
    }

    fn identity() -> Value {
        json!({
            "generator": "test@1", "generator_path": "test", "contract_sha256": "test",
            "inputs": {}, "schema_version": 2, "elapsed_budget_mode": "active_execution"
        })
    }

    #[tokio::test(start_paused = true)]
    async fn active_elapsed_facts_use_exact_total_and_live_monotonic_interval() {
        let error = remaining(1, 1_000_000_007).unwrap_err();
        assert_eq!(
            crate::engine::types::failure_detail_for_error(&error)
                .exhaustion
                .unwrap(),
            model::BudgetExhaustion::elapsed(Duration::new(1, 7), 1)
        );
        let (dir, writer) = journal("exact-clock");
        record(&writer, 0, Some(Utc::now().to_rfc3339())).unwrap();
        record(&writer, 123, None).unwrap();
        let mut episode = Episode::open(&writer, 123).unwrap();
        let clock = episode.deadline(Duration::from_secs(1)).unwrap();
        tokio::time::advance(Duration::new(1, 7)).await;
        assert_eq!(clock.used(), Duration::new(1, 130));
        episode.close(&writer).unwrap();
        assert_eq!(
            writer.state().budget.elapsed.unwrap().active_nanoseconds,
            1_000_000_130
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn first_active_identity_is_atomic_initialization_on_replay() {
        for kind in ["run_queued", "run_started"] {
            let dir = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .unwrap()
                .join(format!("elapsed-first-{}", uuid::Uuid::now_v7()));
            let path = dir.join("journal.jsonl");
            let writer = JournalWriter::create(&path, "first", false, None).unwrap();
            writer.event(kind, identity()).unwrap();
            assert_eq!(recover(&writer, Utc::now()).unwrap(), 0);
            assert!(
                writer
                    .state()
                    .budget
                    .elapsed
                    .unwrap()
                    .active_started_at
                    .is_none()
            );
            drop(writer);
            // Crash at the first append boundary, before even the state cache
            // is durable. The identity alone must reconstruct the closed ledger.
            std::fs::remove_file(dir.join("state.json")).unwrap();
            let writer = JournalWriter::create(&path, "first", false, None).unwrap();
            assert_eq!(writer.state().last_seq, 1);
            assert_eq!(recover(&writer, Utc::now()).unwrap(), 0);
            let mut episode = Episode::open(&writer, 0).unwrap();
            episode.close(&writer).unwrap();
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn requeue_and_fork_preserve_closed_total_and_open_anchor() {
        let (dir, writer) = journal("identity-preserves");
        record(&writer, 0, Some("2026-01-01T00:00:00Z".into())).unwrap();
        record(&writer, 123, None).unwrap();
        writer.event("run_queued", identity()).unwrap();
        assert_eq!(recover(&writer, Utc::now()).unwrap(), 123);
        record(&writer, 123, Some("2026-01-01T00:00:01Z".into())).unwrap();
        let mut values =
            crate::read_journal_values(&dir.join("journal.jsonl"), crate::JournalLimits::default())
                .unwrap()
                .events;
        let mut fork = values[0].clone();
        fork["run_id"] = json!("fork");
        fork["parent_run_id"] = json!("elapsed");
        fork["seq"] = json!(writer.state().last_seq + 1);
        values.push(fork);
        let state = crate::RunState::fold_values(&values).unwrap();
        let ledger = state.budget.elapsed.unwrap();
        assert_eq!(ledger.active_nanoseconds, 123);
        assert_eq!(
            ledger.active_started_at.as_deref(),
            Some("2026-01-01T00:00:01Z")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn missing_ledger_after_history_is_never_reinitialized() {
        for kind in ["run_queued", "run_started"] {
            let (dir, writer) = journal("missing-history");
            writer.event(kind, identity()).unwrap();
            // Inject the missing-ledger state after valid requeue/execution
            // history; neither recovery nor a new identity may zero it out.
            writer.state.lock().unwrap().budget.elapsed = None;
            assert!(
                recover(&writer, Utc::now())
                    .unwrap_err()
                    .to_string()
                    .contains("missing ledger")
            );
            assert!(writer.event("run_queued", identity()).is_err());
            assert!(writer.event("run_started", identity()).is_err());
            let mut fork = identity();
            fork["t"] = json!("run_queued");
            fork["run_id"] = json!("fork");
            fork["parent_run_id"] = json!("elapsed");
            let mut state = writer.state();
            assert!(
                state
                    .apply(&fork)
                    .unwrap_err()
                    .to_string()
                    .contains("missing active ledger")
            );
            assert!(record(&writer, 0, None).is_err());
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn malformed_ledger_is_refused_without_changing_initial_total() {
        let (dir, writer) = journal("malformed-ledger");
        for data in [
            json!({"active_started_at": null}),
            json!({"active_nanoseconds": 0}),
            json!({"active_nanoseconds": -1, "active_started_at": null}),
            json!({"active_nanoseconds": "0", "active_started_at": null}),
            json!({"active_nanoseconds": 0, "active_started_at": "not-a-time"}),
        ] {
            assert!(writer.event("elapsed_accounted", data).is_err());
        }
        assert_eq!(recover(&writer, Utc::now()).unwrap(), 0);
        assert_eq!(writer.state().last_seq, 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn schema_one_absence_is_explicitly_wall_clock_and_new_schema_requires_mode() {
        let dir = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("elapsed-legacy-{}", uuid::Uuid::now_v7()));
        let writer =
            JournalWriter::create(&dir.join("journal.jsonl"), "legacy", false, None).unwrap();
        let mut identity = json!({"generator":"test@1", "generator_path":"test", "contract_sha256":"test", "inputs":{}, "schema_version":2});
        assert!(writer.event("run_queued", identity.clone()).is_err());
        identity["schema_version"] = json!(1);
        writer.event("run_queued", identity.clone()).unwrap();
        assert_eq!(
            writer.state().budget.elapsed_budget_mode,
            policy::ElapsedBudgetMode::WallClock
        );
        assert!(writer.state().budget.elapsed.is_none());
        identity["elapsed_budget_mode"] = json!("active_execution");
        assert!(writer.event("run_started", identity).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn known_handoff_closes_forced_abort_before_queue_wait() {
        let (dir, writer) = journal("handoff");
        let handoff = DateTime::parse_from_rfc3339("2026-01-01T00:00:10Z")
            .unwrap()
            .with_timezone(&Utc);
        record(&writer, 0, Some("2026-01-01T00:00:07Z".into())).unwrap();

        crate::settle_active_elapsed(&writer, handoff).unwrap();
        let settled = writer.state().budget.elapsed.as_ref().unwrap().clone();
        assert_eq!(settled.active_nanoseconds, 3_000_000_000);
        assert!(settled.active_started_at.is_none());

        // The durable requeue follows settlement. A much later resume/handoff
        // must observe the closed total, not charge the known queue interval.
        writer
            .event(
                "run_queued",
                json!({
                    "generator": "test@1", "generator_path": "test", "contract_sha256": "test",
                    "inputs": {}, "schema_version": 2, "elapsed_budget_mode": "active_execution"
                }),
            )
            .unwrap();
        crate::settle_active_elapsed(&writer, handoff + chrono::Duration::hours(1)).unwrap();
        let after_queue_state = writer.state();
        let after_queue = after_queue_state.budget.elapsed.as_ref().unwrap();
        assert_eq!(after_queue.active_nanoseconds, settled.active_nanoseconds);
        assert!(after_queue.active_started_at.is_none());

        let events =
            crate::read_journal_values(&dir.join("journal.jsonl"), crate::JournalLimits::default())
                .unwrap()
                .events;
        let closed = events
            .iter()
            .rposition(|event| {
                event.get("t").and_then(Value::as_str) == Some("elapsed_accounted")
                    && event.get("active_started_at") == Some(&Value::Null)
            })
            .unwrap();
        let requeued = events
            .iter()
            .rposition(|event| event.get("t").and_then(Value::as_str) == Some("run_queued"))
            .unwrap();
        assert!(
            closed < requeued,
            "ledger close must precede durable requeue"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn open_crash_charges_downtime_exactly_once_across_recovery() {
        let (dir, writer) = journal("recovery");
        record(&writer, 0, Some("2026-01-01T00:00:00Z".into())).unwrap();
        drop(writer);
        let writer =
            JournalWriter::create(&dir.join("journal.jsonl"), "elapsed", false, None).unwrap();
        let now = DateTime::parse_from_rfc3339("2026-01-01T00:00:10.000000123Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(recover(&writer, now).unwrap(), 10_000_000_123);
        let seq = writer.state().last_seq;
        assert_eq!(
            recover(&writer, now + chrono::Duration::hours(1)).unwrap(),
            10_000_000_123
        );
        assert_eq!(seq, writer.state().last_seq);
        drop(writer);
        let writer =
            JournalWriter::create(&dir.join("journal.jsonl"), "elapsed", false, None).unwrap();
        assert_eq!(recover(&writer, now).unwrap(), 10_000_000_123);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn live_close_uses_monotonic_time_despite_a_future_wall_anchor() {
        let (dir, writer) = journal("live-clock-rollback");
        // Model an episode opened before a wall-clock rollback without
        // mutating the process clock. Recovery must refuse this anchor, but
        // the still-live episode knows its interval from monotonic time.
        let future = Utc::now() + chrono::Duration::days(1);
        record(&writer, 0, Some(future.to_rfc3339())).unwrap();
        assert!(recover(&writer, Utc::now()).is_err());
        let mut episode = Episode {
            started: Instant::now() - Duration::from_secs(2),
            total: 0,
            closed: false,
        };
        let before = episode.started.elapsed();
        episode.close(&writer).unwrap();
        let after = episode.started.elapsed();
        let ledger = writer.state().budget.elapsed.unwrap();
        assert!(ledger.active_started_at.is_none());
        assert!(ledger.active_nanoseconds >= add(0, before).unwrap());
        assert!(ledger.active_nanoseconds <= add(0, after).unwrap());
        let seq = writer.state().last_seq;
        episode.close(&writer).unwrap();
        assert_eq!(writer.state().last_seq, seq, "live close is idempotent");
        drop(writer);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn backward_clock_and_overflow_refuse_without_closing() {
        let (dir, writer) = journal("invalid");
        assert_eq!(recover(&writer, Utc::now()).unwrap(), 0);
        record(&writer, 0, Some("2026-01-02T00:00:00Z".into())).unwrap();
        let before = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(
            recover(&writer, before)
                .unwrap_err()
                .to_string()
                .contains("backwards")
        );
        assert!(
            writer
                .state()
                .budget
                .elapsed
                .as_ref()
                .unwrap()
                .active_started_at
                .is_some()
        );
        record(&writer, u64::MAX, None).unwrap();
        record(&writer, u64::MAX, Some(before.to_rfc3339())).unwrap();
        assert!(recover(&writer, before + chrono::Duration::nanoseconds(1)).is_err());
        assert!(
            writer
                .state()
                .budget
                .elapsed
                .as_ref()
                .unwrap()
                .active_started_at
                .is_some()
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ledger_transitions_and_precise_repeated_short_episodes() {
        let (dir, writer) = journal("transitions");
        assert!(record(&writer, 0, None).is_err()); // cannot reset a closed ledger
        let mut total = 0;
        for _ in 0..100 {
            record(&writer, total, Some(Utc::now().to_rfc3339())).unwrap();
            assert!(record(&writer, total, Some(Utc::now().to_rfc3339())).is_err());
            total = add(total, Duration::from_nanos(7_000_001)).unwrap();
            record(&writer, total, None).unwrap();
        }
        assert_eq!(
            remaining(1, total).unwrap(),
            Duration::from_nanos(299_999_900)
        );
        assert!(remaining(1, 1_000_000_000).is_err());
        assert!(remaining(1, 1_000_000_001).is_err());
        assert!(writer.event("run_queued", json!({
            "generator":"test@1", "generator_path":"test", "contract_sha256":"test", "inputs":{},
            "schema_version":2, "elapsed_budget_mode":"wall_clock"
        })).is_err());
        assert!(
            writer
                .event("elapsed_accounted", json!({"active_nanoseconds": total}))
                .is_err()
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
