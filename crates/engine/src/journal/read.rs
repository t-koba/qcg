use camino::Utf8Path;
use chrono::Utc;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, Read as _};

use super::types::{JournalError, JournalLimits, JournalMetrics, JournalScan, JournalStats};
use super::writer::validate_limits;

pub fn read_journal_values(
    path: &Utf8Path,
    limits: JournalLimits,
) -> Result<JournalScan, JournalError> {
    read_journal_values_through(path, None, limits)
}

pub fn read_journal_values_through(
    path: &Utf8Path,
    through_seq: Option<u64>,
    limits: JournalLimits,
) -> Result<JournalScan, JournalError> {
    validate_limits(limits)?;
    if !path.exists() {
        return Ok(JournalScan {
            events: Vec::new(),
            stats: JournalStats::default(),
        });
    }
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut line_number = 0_usize;
    let mut stats = JournalStats::default();
    let mut events = Vec::new();
    let line_read_limit = limits
        .max_event_bytes
        .map(|limit| {
            limit.checked_add(2).ok_or(JournalError::LimitExceeded {
                resource: "event",
                actual: usize::MAX,
                limit,
            })
        })
        .transpose()?;
    loop {
        line.clear();
        let read = match line_read_limit {
            Some(limit) => {
                // An unrepresentable over-read window fails closed instead
                // of clamping the detection bound (E13).
                let over_read = u64::try_from(limit).map_err(|_| {
                    let actual = limit.saturating_add(1);
                    JournalError::LimitExceeded {
                        resource: "event",
                        actual,
                        limit,
                    }
                })?;
                (&mut reader).take(over_read).read_until(b'\n', &mut line)?
            }
            None => reader.read_until(b'\n', &mut line)?,
        };
        if read == 0 {
            break;
        }
        // Read-path counters overflow fail closed instead of wrapping the
        // reported line number or stats (E13).
        line_number = line_number
            .checked_add(1)
            .ok_or(JournalError::InvalidEvent(
                "journal line number overflowed".into(),
            ))?;
        stats.bytes =
            stats
                .bytes
                .checked_add(read)
                .ok_or_else(|| match limits.max_total_bytes {
                    Some(limit) => JournalError::LimitExceeded {
                        resource: "total journal",
                        actual: limit.saturating_add(1),
                        limit,
                    },
                    None => JournalError::LimitExceeded {
                        resource: "total journal",
                        actual: usize::MAX,
                        limit: usize::MAX,
                    },
                })?;
        if let Some(limit) = limits.max_total_bytes
            && stats.bytes > limit
        {
            return Err(JournalError::LimitExceeded {
                resource: "total journal",
                actual: stats.bytes,
                limit,
            });
        }
        let has_newline = line.last() == Some(&b'\n');
        if !has_newline
            && let Some(read_limit) = line_read_limit
            && read == read_limit
        {
            // Report the configured event limit, not the two-byte read
            // cap used to detect the overrun (E06).
            let limit = read_limit.saturating_sub(2);
            return Err(JournalError::LimitExceeded {
                resource: "event",
                actual: read,
                limit,
            });
        }
        let body_len = line.len().saturating_sub(usize::from(has_newline));
        if let Some(limit) = limits.max_event_bytes
            && body_len > limit
        {
            return Err(JournalError::LimitExceeded {
                resource: "event",
                actual: body_len,
                limit,
            });
        }
        let body = &line[..body_len];
        if body.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if let Some(limit) = limits.max_event_count
            && stats.events >= limit
        {
            return Err(JournalError::EventCountExceeded {
                actual: stats
                    .events
                    .checked_add(1)
                    .ok_or(JournalError::InvalidEvent(
                        "journal event count overflowed".into(),
                    ))?,
                limit,
            });
        }
        let event = match serde_json::from_slice::<Value>(body) {
            Ok(event) => event,
            Err(source) if !has_newline && source.is_eof() && reader.fill_buf()?.is_empty() => {
                break;
            }
            Err(source) => {
                return Err(JournalError::InvalidLine {
                    line: line_number,
                    source,
                });
            }
        };
        if through_seq.is_some_and(|limit| {
            event
                .get("seq")
                .and_then(Value::as_u64)
                .is_some_and(|seq| seq > limit)
        }) {
            break;
        }
        stats.events = stats
            .events
            .checked_add(1)
            .ok_or(JournalError::InvalidEvent(
                "journal event count overflowed".into(),
            ))?;
        events.push(event);
    }
    Ok(JournalScan { events, stats })
}

pub(crate) fn journal_metrics(budget: &crate::BudgetState) -> Result<JournalMetrics, JournalError> {
    let steps_executed = u64::try_from(budget.steps_executed).map_err(|_| {
        JournalError::InvalidEvent("executed step count exceeds journal metric range".into())
    })?;
    let started_at = budget.started_at.as_deref().ok_or_else(|| {
        JournalError::InvalidEvent("run start time is required before finishing a run".into())
    })?;
    let started_at = chrono::DateTime::parse_from_rfc3339(started_at)
        .map_err(|error| JournalError::InvalidEvent(format!("invalid run start time: {error}")))?;
    let duration_ms = Utc::now()
        .signed_duration_since(started_at)
        .num_milliseconds();
    let duration_ms = u64::try_from(duration_ms).map_err(|_| {
        JournalError::InvalidEvent("run start time is later than its finish time".into())
    })?;
    Ok(JournalMetrics {
        steps_total: steps_executed,
        steps_succeeded: budget.steps_succeeded,
        steps_failed: budget.steps_failed,
        steps_skipped: budget.steps_skipped,
        repair_attempts: budget.repair_attempts,
        regenerate_attempts: budget.regenerate_attempts,
        llm_calls: budget.llm_calls,
        tokens_input: budget.tokens_input,
        tokens_output: budget.tokens_output,
        tokens_cached_input: budget.tokens_cached_input,
        steps_executed,
        budget_charged: u64::try_from(budget.budget_charged).map_err(|_| {
            JournalError::InvalidEvent("budget charge count exceeds journal metric range".into())
        })?,
        tokens_total: budget
            .tokens_input
            .checked_add(budget.tokens_output)
            .ok_or(JournalError::InvalidEvent("token total overflowed".into()))?,
        cost_microusd: budget.cost_microusd,
        duration_ms,
    })
}
