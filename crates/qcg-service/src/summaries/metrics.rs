use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use qcg_api::{RunEvent, RunEventData};
use qcg_engine::JournalLimits;
use qcg_types::RunMetrics;
use serde_json::Value;
use std::io::SeekFrom;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncSeekExt as _};
use tokio_stream::wrappers::ReceiverStream;

use super::reads::read_run_events;
use super::state::fold_run_state;
use super::summary::run_meta_dir;
use qcg_policy::JOURNAL_POLL_CHANNEL_CAPACITY;

/// Cost metrics for one run: exact terminal totals when finished, otherwise a
/// live synthesis from the folded budget. Returns `None` only for an empty
/// journal with no recorded activity.
pub fn read_run_metrics(run_dir: &Utf8Path) -> Result<Option<RunMetrics>, ServiceError> {
    let events = read_run_events(run_dir)?;
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

/// Best-effort totals for a run that has not terminated yet.
fn live_metrics(budget: &qcg_engine::BudgetState) -> RunMetrics {
    let duration_ms = budget
        .started_at
        .as_deref()
        .and_then(|started| chrono::DateTime::parse_from_rfc3339(started).ok())
        .map(|started| {
            chrono::Utc::now()
                .signed_duration_since(started)
                .num_milliseconds()
                .max(0) as u64
        })
        .unwrap_or_default();
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
) -> BoxStream<'static, RunEvent> {
    poll_journal_events_with_limits(run_dir, run_id, delivered_seq, JournalLimits::default())
}

pub(crate) fn poll_journal_events_with_limits(
    run_dir: Utf8PathBuf,
    run_id: String,
    mut delivered_seq: u64,
    limits: JournalLimits,
) -> BoxStream<'static, RunEvent> {
    let (sender, receiver) = tokio::sync::mpsc::channel(JOURNAL_POLL_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let journal_path = run_meta_dir(&run_dir).join("journal.jsonl");
        let mut offset = 0_u64;
        let mut event_count = 0_usize;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            interval.tick().await;
            let metadata = match tokio::fs::metadata(&journal_path).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    tracing::error!(%error, %run_id, "failed to inspect shared run journal");
                    return;
                }
            };
            if limits
                .max_total_bytes
                .is_some_and(|limit| metadata.len() > limit as u64)
            {
                tracing::error!(%run_id, limit = ?limits.max_total_bytes, actual = metadata.len(), "shared run journal exceeds byte limit");
                return;
            }
            let mut file = match tokio::fs::File::open(&journal_path).await {
                Ok(file) => file,
                Err(error) => {
                    tracing::error!(%error, %run_id, "failed to open shared run journal");
                    return;
                }
            };
            if let Err(error) = file.seek(SeekFrom::Start(offset)).await {
                tracing::error!(%error, %run_id, "failed to seek shared run journal");
                return;
            }
            let mut reader = tokio::io::BufReader::new(file);
            loop {
                let mut line = Vec::new();
                let line_limit = limits.max_event_bytes.map(|limit| limit.saturating_add(2));
                let read = match line_limit {
                    Some(line_limit) => {
                        (&mut reader)
                            .take(line_limit as u64)
                            .read_until(b'\n', &mut line)
                            .await
                    }
                    None => reader.read_until(b'\n', &mut line).await,
                };
                let read = match read {
                    Ok(read) => read,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "failed to read shared run journal");
                        return;
                    }
                };
                if read == 0 {
                    break;
                }
                let has_newline = line.last() == Some(&b'\n');
                if !has_newline {
                    if line_limit.is_some_and(|limit| read == limit) {
                        tracing::error!(%run_id, limit = ?limits.max_event_bytes, "shared run journal event exceeds byte limit");
                        return;
                    }
                    break;
                }
                offset = match offset.checked_add(read as u64) {
                    Some(offset) => offset,
                    None => {
                        tracing::error!(%run_id, "shared run journal offset overflowed");
                        return;
                    }
                };
                line.pop();
                if limits
                    .max_event_bytes
                    .is_some_and(|limit| line.len() > limit)
                {
                    tracing::error!(%run_id, limit = ?limits.max_event_bytes, actual = line.len(), "shared run journal event exceeds byte limit");
                    return;
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                event_count = event_count.saturating_add(1);
                if limits
                    .max_event_count
                    .is_some_and(|limit| event_count > limit)
                {
                    tracing::error!(%run_id, limit = ?limits.max_event_count, "shared run journal event count exceeds limit");
                    return;
                }
                let value = match serde_json::from_slice::<Value>(&line) {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "shared run journal contains invalid JSON");
                        return;
                    }
                };
                let event = match RunEvent::from_flat(&value) {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "shared run journal contains an invalid event");
                        return;
                    }
                };
                let terminal = matches!(
                    event.kind.as_str(),
                    "run_finished" | "run_error" | "run_canceled"
                );
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
