use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use qcg_api::{RunEvent, RunEventData};
use qcg_engine::JournalLimits;
use qcg_types::RunMetrics;
use serde_json::Value;
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tokio_stream::wrappers::ReceiverStream;

use super::reads::read_durable_run_events;
use super::state::fold_run_state;
use super::summary::run_meta_dir;
use qcg_policy::JOURNAL_POLL_CHANNEL_CAPACITY;

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
    state: &qcg_engine::RunState,
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
fn live_metrics(budget: &qcg_engine::BudgetState) -> RunMetrics {
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
        trace_id: qcg_api::trace_id_for_run(run_id),
        span_id: qcg_api::span_id_for_seq(seq),
        parent_span_id: None,
        path: None,
        kind: "stream_error".to_string(),
        data: qcg_api::RunEventData::Unknown(serde_json::json!({"error": detail})),
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
        let meta_dir = run_meta_dir(&run_dir);
        let durable_path = meta_dir.join("journal.jsonl");
        let audit_path = meta_dir.join("audit.jsonl");
        let mut durable_offset = 0_u64;
        let mut audit_offset = 0_u64;
        let mut event_count = 0_usize;
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(poll_interval_millis));
        /// Sends the failure marker best-effort before closing: a dropped
        /// receiver means no consumer remains, so the send failure is
        /// intentionally ignored (documented best-effort, not a silent drop).
        async fn fail_closed(
            sender: &tokio::sync::mpsc::Sender<RunEvent>,
            run_id: &str,
            seq: u64,
            detail: String,
        ) {
            let _ = sender.send(stream_error_event(run_id, seq, detail)).await;
        }
        /// Reads complete newline-terminated records appended after
        /// `offset`. A partial tail stays for the next tick, and a shrunken
        /// file resets the cursor (delivered-seq filtering prevents
        /// duplicates). Lines without a seq are corruption.
        async fn drain(
            path: &Utf8Path,
            offset: &mut u64,
            max_event_bytes: Option<usize>,
            label: &str,
        ) -> Result<Vec<(u64, Vec<u8>)>, String> {
            use tokio::io::AsyncBufReadExt as _;
            let metadata = match tokio::fs::metadata(path).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Vec::new());
                }
                Err(error) => return Err(format!("failed to inspect {label}: {error}")),
            };
            if metadata.len() < *offset {
                tracing::warn!(
                    offset,
                    len = metadata.len(),
                    "shared {label} shrank; resetting poll cursor"
                );
                *offset = 0;
            }
            let mut file = tokio::fs::File::open(path)
                .await
                .map_err(|error| format!("failed to open {label}: {error}"))?;
            file.seek(SeekFrom::Start(*offset))
                .await
                .map_err(|error| format!("failed to seek {label}: {error}"))?;
            let mut reader = tokio::io::BufReader::new(file);
            let mut lines = Vec::new();
            loop {
                let mut line = Vec::new();
                let line_limit = max_event_bytes.map(|limit| limit.saturating_add(2));
                let read = match line_limit {
                    Some(line_limit) => {
                        (&mut reader)
                            .take(line_limit as u64)
                            .read_until(b'\n', &mut line)
                            .await
                    }
                    None => reader.read_until(b'\n', &mut line).await,
                }
                .map_err(|error| format!("failed to read {label}: {error}"))?;
                if read == 0 {
                    break;
                }
                if line.last() != Some(&b'\n') {
                    if line_limit.is_some_and(|limit| read == limit) {
                        return Err(format!("{label} event exceeds byte limit"));
                    }
                    break;
                }
                *offset = offset
                    .checked_add(read as u64)
                    .ok_or_else(|| format!("{label} offset overflowed"))?;
                line.pop();
                if max_event_bytes.is_some_and(|limit| line.len() > limit) {
                    return Err(format!("{label} event exceeds byte limit"));
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let seq = serde_json::from_slice::<Value>(&line)
                    .ok()
                    .and_then(|value| value.get("seq").and_then(Value::as_u64))
                    .ok_or_else(|| format!("{label} contains an event without seq"))?;
                lines.push((seq, line));
            }
            Ok(lines)
        }
        loop {
            // Shutdown must end every poll task even when the run never
            // emits another event; the SSE wrapper cannot reach this task
            // (E05). A terminal event also ends the task; both paths close
            // the stream and the client reconnects from its last seq.
            // Only failure paths emit the error marker above: shutdown and
            // terminal closes carry no marker, so the three endings stay
            // distinguishable (E05).
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {}
            }
            // Durable byte bound: enforced against the file size before any
            // content is read, exactly as the single-stream poller did.
            if let Some(limit) = limits.max_total_bytes
                && let Ok(metadata) = tokio::fs::metadata(&durable_path).await
                && metadata.len() > limit as u64
            {
                tracing::error!(%run_id, actual = metadata.len(), "shared run journal exceeds byte limit");
                fail_closed(
                    &sender,
                    &run_id,
                    delivered_seq,
                    "shared run journal exceeds byte limit".to_string(),
                )
                .await;
                return;
            }
            let mut pending = match drain(
                &durable_path,
                &mut durable_offset,
                limits.max_event_bytes,
                "run journal",
            )
            .await
            {
                Ok(lines) => lines,
                Err(detail) => {
                    tracing::error!(%run_id, %detail, "shared run journal poll failed");
                    fail_closed(&sender, &run_id, delivered_seq, detail).await;
                    return;
                }
            };
            // Observation records share the seq space and are merged by seq
            // so consumers see the same single order as the durable path
            // (ADR 0001). An audit read failure never fails the durable
            // stream: it is logged and retried on the next tick.
            match drain(&audit_path, &mut audit_offset, None, "audit stream").await {
                Ok(mut observed) => pending.append(&mut observed),
                Err(detail) => {
                    tracing::warn!(%run_id, %detail, "shared audit stream poll failed; retrying");
                }
            }
            pending.sort_by_key(|(seq, _)| *seq);
            for (_, line) in pending {
                event_count = event_count.saturating_add(1);
                if limits
                    .max_event_count
                    .is_some_and(|limit| event_count > limit)
                {
                    tracing::error!(%run_id, limit = ?limits.max_event_count, "shared run journal event count exceeds limit");
                    fail_closed(
                        &sender,
                        &run_id,
                        delivered_seq,
                        "shared run journal event count exceeds limit".to_string(),
                    )
                    .await;
                    return;
                }
                let value = match serde_json::from_slice::<Value>(&line) {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "shared run journal contains invalid JSON");
                        fail_closed(
                            &sender,
                            &run_id,
                            delivered_seq,
                            format!("shared run journal contains invalid JSON: {error}"),
                        )
                        .await;
                        return;
                    }
                };
                let event = match RunEvent::from_flat(&value) {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "shared run journal contains an invalid event");
                        fail_closed(
                            &sender,
                            &run_id,
                            delivered_seq,
                            format!("shared run journal contains an invalid event: {error}"),
                        )
                        .await;
                        return;
                    }
                };
                let terminal = qcg_api::is_terminal_event_kind(event.kind.as_str());
                if event.seq > delivered_seq {
                    delivered_seq = event.seq;
                    if sender.send(event).await.is_err() {
                        return;
                    }
                }
                if terminal {
                    return;
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
            std::env::temp_dir().join(format!("qcg-poll-cancel-{}", uuid::Uuid::now_v7())),
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
            qcg_policy::JOURNAL_POLL_INTERVAL_MILLIS,
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
    async fn invalid_journal_json_ends_with_a_failure_marker_not_a_terminal() {
        // E05: corrupt journal bytes end the poll with a `stream_error`
        // marker first, never a faked terminal event, so consumers tell
        // failure-close apart from terminal-close.
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-poll-corrupt-{}", uuid::Uuid::now_v7())),
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
            qcg_policy::JOURNAL_POLL_INTERVAL_MILLIS,
            tokio_util::sync::CancellationToken::new(),
        );
        let failure = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("corrupt journal should emit a failure marker")
            .expect("failure marker should be delivered");
        assert_eq!(failure.kind, "stream_error");
        assert!(
            !qcg_api::is_terminal_event_kind(failure.kind.as_str()),
            "the failure marker must never fake a terminal event"
        );
        let end = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("stream should close after the failure marker");
        assert!(end.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
