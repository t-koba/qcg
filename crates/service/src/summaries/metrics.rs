use super::super::types::ServiceError;
use api::{RunEvent, RunEventData};
use camino::{Utf8Path, Utf8PathBuf};
use engine::JournalLimits;
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use model::RunMetrics;
use tokio_stream::wrappers::ReceiverStream;

use super::reads::read_durable_run_events;
use super::state::fold_run_state;
#[cfg(test)]
use super::summary::run_meta_dir;
use policy::JOURNAL_POLL_CHANNEL_CAPACITY;

/// Cost metrics for one run: exact terminal totals when finished, otherwise a
/// live synthesis from the folded budget. Returns `None` only for an empty
/// journal with no recorded activity.
pub fn read_run_metrics(run_dir: &Utf8Path) -> Result<Option<RunMetrics>, ServiceError> {
    let events = read_durable_run_events(run_dir)?;
    let state = fold_run_state(run_dir)?;
    read_run_metrics_from_view(&events, &state)
}

/// Metrics from one journal read: typed events plus their fold. Callers
/// that already hold both never re-read the journal per metric.
pub fn read_run_metrics_from_view(
    events: &[RunEvent],
    state: &engine::RunState,
) -> Result<Option<RunMetrics>, ServiceError> {
    if let Some(metrics) = terminal_metrics(events) {
        return Ok(Some(metrics));
    }
    if state.last_seq == 0 {
        return Ok(None);
    }
    Ok(Some(live_metrics(&state.budget)))
}

/// Latest terminal metrics from already-parsed events, if any terminal event
/// carries them.
pub fn terminal_metrics(events: &[RunEvent]) -> Option<RunMetrics> {
    events.iter().rev().find_map(|event| match &event.data {
        RunEventData::RunFinished(data) => Some(data.metrics.clone()),
        RunEventData::RunError(data) => Some(data.metrics.clone()),
        RunEventData::RunCanceled(data) | RunEventData::RunInterrupted(data) => {
            Some(data.metrics.clone())
        }
        _ => None,
    })
}

/// Best-effort totals for a run that has not terminated yet. The duration
/// is quantized to whole seconds: snapshot bodies feed exact-digest ETags,
/// so a millisecond-synthesized duration would change every body and a
/// conditional request could never return 304. Second precision still
/// reflects live progress while letting the validator hold within a second
/// (E16).
fn live_metrics(budget: &engine::BudgetState) -> RunMetrics {
    // A corrupt start instant (never written by this service) must never
    // silently read as zero duration: the failure is logged and the
    // display-only snapshot reports zero, while settlement never consults
    // this field. Second quantization keeps exact-digest ETags stable.
    let duration_ms = match budget.started_at.as_deref() {
        None => 0,
        Some(started) => match chrono::DateTime::parse_from_rfc3339(started) {
            Ok(started) => {
                let raw = chrono::Utc::now()
                    .signed_duration_since(started)
                    .num_milliseconds()
                    .max(0) as u64;
                raw - (raw % 1000)
            }
            Err(error) => {
                tracing::warn!(started_at = %started, %error, "run start instant is unparseable; reporting zero live duration");
                0
            }
        },
    };
    RunMetrics {
        steps_total: budget.steps_executed as u64,
        steps_succeeded: budget.steps_succeeded,
        steps_failed: budget.steps_failed,
        steps_skipped: budget.steps_skipped,
        steps_executed: budget.steps_executed as u64,
        budget_charged: budget.budget_charged as u64,
        repair_attempts: budget.repair_attempts,
        regenerate_attempts: budget.regenerate_attempts,
        llm_calls: budget.llm_calls,
        tokens_input: budget.tokens_input,
        tokens_output: budget.tokens_output,
        tokens_cached_input: budget.tokens_cached_input,
        tokens_total: budget.tokens_input.saturating_add(budget.tokens_output),
        cost_microusd: budget.cost_microusd,
        duration_ms,
    }
}

pub(crate) fn poll_journal_events(
    run_dir: Utf8PathBuf,
    run_id: String,
    delivered_seq: u64,
    poll_interval_millis: u64,
    shutdown: tokio_util::sync::CancellationToken,
) -> BoxStream<'static, RunEvent> {
    poll_journal_events_with_limits(
        run_dir,
        run_id,
        delivered_seq,
        JournalLimits::default(),
        poll_interval_millis,
        shutdown,
    )
}

/// Failure-close marker for the shared journal poller. Every poll failure
/// ends the stream with this event first, so consumers distinguish a
/// failure-close from a terminal-close (terminal kinds) and from a
/// shutdown-close (no marker). The kind is intentionally not terminal and
/// never fakes a normal outcome: `RunEventData::parse` carries it as
/// `Unknown`, which the SSE layer forwards like the `lagged` control
/// signal (E05).
///
/// Store-lock participation (E12): this poller deliberately does NOT join
/// the runs-directory store lock. The store lock serializes store *writers*
/// (exclusive boot vs shared peer boots, held for the process lifetime at
/// construction); this poller is a read-only journal observer that must
/// keep serving while any owner writes. Mutual exclusion is already
/// guaranteed at construction (the process holds its store-mode lock for
/// life), and per-run authority stays with the execution lease plus the
/// journal lock. Taking the store lock per poll tick would serialize every
/// subscriber against boot and GC for no safety gain.
pub(crate) fn stream_error_event(run_id: &str, seq: u64, detail: String) -> RunEvent {
    RunEvent {
        seq,
        ts: chrono::Utc::now().to_rfc3339(),
        run_id: run_id.to_string(),
        trace_id: api::trace_id_for_run(run_id),
        span_id: api::span_id_for_seq(seq),
        parent_span_id: None,
        path: None,
        kind: "stream_error".to_string(),
        data: api::RunEventData::Unknown(serde_json::json!({"error": detail})),
    }
}

pub(crate) fn poll_journal_events_with_limits(
    run_dir: Utf8PathBuf,
    run_id: String,
    mut delivered_seq: u64,
    limits: JournalLimits,
    poll_interval_millis: u64,
    shutdown: tokio_util::sync::CancellationToken,
) -> BoxStream<'static, RunEvent> {
    let (sender, receiver) = tokio::sync::mpsc::channel(JOURNAL_POLL_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut cursor = crate::EventCursor::default();
        let mut terminal = false;
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let directory = run_dir.clone();
            let current = cursor.clone();
            static PERMITS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
                std::sync::OnceLock::new();
            let permits = PERMITS
                .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(8)))
                .clone();
            let permit = tokio::select! {_=shutdown.cancelled()=>return, permit=permits.acquire_owned()=>permit.expect("poll permits are never closed")};
            let result = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                current.read_with_limits(&directory, limits)
            })
            .await;
            let batch = match result {
                Ok(Ok(batch)) => batch,
                result => {
                    let detail = match result {
                        Ok(Err(e)) => e.to_string(),
                        Err(e) => e.to_string(),
                        _ => unreachable!(),
                    };
                    let event = stream_error_event(&run_id, delivered_seq, detail);
                    tokio::select! { _ = shutdown.cancelled() => {}, _ = sender.send(event) => {} }
                    return;
                }
            };
            cursor = batch.cursor;
            for value in batch.events {
                let event = match RunEvent::from_flat(&value) {
                    Ok(event) => event,
                    Err(detail) => {
                        let _ = sender
                            .send(stream_error_event(&run_id, delivered_seq, detail))
                            .await;
                        return;
                    }
                };
                if api::is_terminal_event_kind(&event.kind) {
                    terminal = true;
                } else if matches!(
                    event.kind.as_str(),
                    "run_queued" | "run_started" | "run_resumed"
                ) {
                    terminal = false;
                }
                if event.seq > delivered_seq {
                    delivered_seq = event.seq;
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        result = sender.send(event) => { if result.is_err() { return; } }
                    }
                }
            }
            if terminal && batch.exhausted {
                return;
            }
            if batch.exhausted {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(poll_interval_millis)) => {}
                }
            }
        }
    });
    ReceiverStream::new(receiver).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_shutdown_ends_journal_poll_before_the_first_tick() {
        // E05: a poll task started during shutdown must exit on the token
        // instead of waiting for events that will never arrive.
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("poll-cancel-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        std::fs::create_dir_all(run_meta_dir(&dir)).expect("meta dir should be created");
        std::fs::write(run_meta_dir(&dir).join("journal.jsonl"), "").expect("journal");
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let mut stream = poll_journal_events(
            dir.clone(),
            "run".into(),
            0,
            policy::JOURNAL_POLL_INTERVAL_MILLIS,
            shutdown,
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
                .await
                .expect("poll must end promptly")
                .is_none(),
            "a cancelled poll must close its stream"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resumed_history_does_not_end_a_live_poll() {
        use std::io::Write;
        let directory = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("poll-resumed-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        std::fs::create_dir_all(run_meta_dir(&directory)).unwrap();
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let record = |seq, kind| {
            let mut value = serde_json::json!({
                "seq": seq, "run_id": "resumed", "t": kind,
                "ts": "2026-01-01T00:00:00Z",
                "trace_id": api::trace_id_for_run("resumed"), "span_id": api::span_id_for_seq(seq)
            });
            if kind == "run_interrupted" {
                value["reason"] = serde_json::json!({"code":"interrupted", "message":"test"});
            } else if kind == "run_finished" {
                value["status"] = serde_json::json!("success");
                value["metrics"] = serde_json::to_value(model::RunMetrics::default()).unwrap();
            }
            format!("{}\n", value)
        };
        std::fs::write(
            &path,
            format!(
                "{}{}",
                record(1, "run_interrupted"),
                record(2, "run_resumed")
            ),
        )
        .unwrap();
        let token = tokio_util::sync::CancellationToken::new();
        let mut stream =
            poll_journal_events(directory.clone(), "resumed".into(), 2, 10, token.clone());
        // Let the initial batch be consumed, then append a new durable event.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), stream.next())
                .await
                .is_err()
        );
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        write!(file, "{}", record(3, "run_finished")).unwrap();
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.seq, 3);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .is_none()
        );
        token.cancel();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn invalid_journal_json_ends_with_a_failure_marker_not_a_terminal() {
        // E05: corrupt journal bytes end the poll with a `stream_error`
        // marker first, never a faked terminal event, so consumers tell
        // failure-close apart from terminal-close.
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("poll-corrupt-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        std::fs::create_dir_all(run_meta_dir(&dir)).expect("meta dir should be created");
        std::fs::write(
            run_meta_dir(&dir).join("journal.jsonl"),
            "{not valid json\n",
        )
        .expect("corrupt journal should be written");
        let mut stream = poll_journal_events(
            dir.clone(),
            "corrupt-run".into(),
            0,
            policy::JOURNAL_POLL_INTERVAL_MILLIS,
            tokio_util::sync::CancellationToken::new(),
        );
        let failure = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("corrupt journal should emit a failure marker")
            .expect("failure marker should be delivered");
        assert_eq!(failure.kind, "stream_error");
        assert!(
            !api::is_terminal_event_kind(failure.kind.as_str()),
            "the failure marker must never fake a terminal event"
        );
        let end = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("stream should close after the failure marker");
        assert!(end.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
