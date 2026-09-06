use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunEvent;
use qcg_engine::{JournalLimits, read_journal_values};
use serde_json::Value;
use std::collections::BTreeMap;

use super::summary::run_meta_dir;

/// Durably accepted HITL responses: answers by question id and confirmations
/// by confirmation id.
pub(crate) type PersistedHitlMaps = (BTreeMap<String, Value>, BTreeMap<String, bool>);

pub fn resolve_run_dir(runs_dir: &Utf8Path, id: &str) -> Result<Utf8PathBuf, ServiceError> {
    if id.contains('/') || id.contains('\\') || id == "." || id == ".." {
        return Err(ServiceError::Invalid(format!(
            "run id `{id}` is not allowed"
        )));
    }
    let run_dir = runs_dir.join(id);
    if !run_meta_dir(&run_dir).join("journal.jsonl").exists() {
        return Err(ServiceError::Invalid(format!(
            "run `{id}` was not found under `{runs_dir}`"
        )));
    }
    Ok(run_dir)
}

pub fn read_journal_events(run_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    read_events_from_meta(&run_meta_dir(run_dir))
}

/// Drop trailing `run_canceled` bookkeeping events written by an engine task
/// that was preempted before terminal settlement. Returns whether anything
/// was removed. Never touches a journal whose tail is anything else, so a
/// genuine user cancellation is preserved.
pub(crate) fn truncate_trailing_canceled_events(run_dir: &Utf8Path) -> Result<bool, ServiceError> {
    let path = run_meta_dir(run_dir).join("journal.jsonl");
    let content = std::fs::read_to_string(&path)?;
    let mut lines: Vec<&str> = content.lines().collect();
    let mut removed = false;
    while let Some(last) = lines.last() {
        let canceled = serde_json::from_str::<Value>(last)
            .map(|event| event.get("t").and_then(Value::as_str) == Some("run_canceled"))
            .unwrap_or(false);
        if !canceled {
            break;
        }
        lines.pop();
        removed = true;
    }
    if removed {
        let mut rewritten = lines.join("\n");
        if !rewritten.is_empty() {
            rewritten.push('\n');
        }
        std::fs::write(&path, rewritten)?;
    }
    Ok(removed)
}

/// Latest scheduling identity from the trailing `run_queued` event:
/// priority and fork parent. Journals written before these fields existed
/// read as zero priority with no parent.
pub(crate) fn read_queued_identity(
    run_dir: &Utf8Path,
) -> Result<(i32, Option<String>), ServiceError> {
    let mut priority = 0;
    let mut parent = None;
    for event in read_journal_events(run_dir)? {
        if event.get("t").and_then(Value::as_str) != Some("run_queued") {
            continue;
        }
        if let Some(value) = event.get("priority").and_then(Value::as_i64) {
            priority = value.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        }
        parent = event
            .get("parent_run_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(parent);
    }
    Ok((priority, parent))
}

/// Durably accepted HITL responses folded from the journal.
///
/// `run_queued` may carry pre-provided `answers` / `confirmations` for
/// unattended runs, while later `user_answered` / `user_confirmed` events
/// record interactive acceptance. Later events win on the same key so a
/// restart resumes with the same values the API already acknowledged.
pub(crate) fn read_persisted_hitl(run_dir: &Utf8Path) -> Result<PersistedHitlMaps, ServiceError> {
    let mut answers: BTreeMap<String, Value> = BTreeMap::new();
    let mut confirmations: BTreeMap<String, bool> = BTreeMap::new();
    for event in read_journal_events(run_dir)? {
        match event.get("t").and_then(Value::as_str) {
            Some("run_queued") => {
                if let Some(map) = event.get("answers").and_then(Value::as_object) {
                    for (key, value) in map {
                        answers.insert(key.clone(), value.clone());
                    }
                }
                if let Some(map) = event.get("confirmations").and_then(Value::as_object) {
                    for (key, value) in map {
                        if let Some(approved) = value.as_bool() {
                            confirmations.insert(key.clone(), approved);
                        }
                    }
                }
            }
            Some("user_answered") => {
                if let (Some(id), Some(values)) = (
                    event.get("question_id").and_then(Value::as_str),
                    event.get("values").cloned(),
                ) {
                    answers.insert(id.to_string(), values);
                }
            }
            Some("user_confirmed") => {
                if let (Some(id), Some(approved)) = (
                    event.get("confirmation_id").and_then(Value::as_str),
                    event.get("approved").and_then(Value::as_bool),
                ) {
                    confirmations.insert(id.to_string(), approved);
                }
            }
            _ => {}
        }
    }
    Ok((answers, confirmations))
}

/// Journaled MCP input continuations keyed by reserved answers key.
///
/// Steps record `mcp_input_pending` before suspending for user input so a
/// restart can continue the original remote request with its
/// `request_state` instead of starting a duplicate first request.
pub(crate) fn read_persisted_mcp_pending(
    run_dir: &Utf8Path,
) -> Result<BTreeMap<String, Value>, ServiceError> {
    let mut pending: BTreeMap<String, Value> = BTreeMap::new();
    for event in read_journal_events(run_dir)? {
        if event.get("t").and_then(Value::as_str) != Some("mcp_input_pending") {
            continue;
        }
        let (Some(key), Some(question_id)) = (
            event
                .get("pending_key")
                .and_then(Value::as_str)
                .map(str::to_string),
            event
                .get("question_id")
                .and_then(Value::as_str)
                .map(str::to_string),
        ) else {
            continue;
        };
        pending.insert(key, json_mcp_pending(event, &question_id));
    }
    Ok(pending)
}

fn json_mcp_pending(event: Value, _question_id: &str) -> Value {
    let mut pending = serde_json::Map::new();
    for key in [
        "node",
        "question_id",
        "server",
        "tool",
        "alias",
        "arguments",
        "request_state",
        "input_requests",
    ] {
        if let Some(value) = event.get(key).cloned() {
            pending.insert(key.to_string(), value);
        }
    }
    Value::Object(pending)
}

/// Whether the shared journal holds a peer cancel request for this run.
pub(crate) fn has_remote_cancel_request(run_dir: &Utf8Path) -> Result<bool, ServiceError> {
    for event in read_journal_events(run_dir)? {
        if event.get("t").and_then(Value::as_str) == Some("user_cancel_requested") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn read_events_from_meta(meta_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    read_journal_values(&meta_dir.join("journal.jsonl"), JournalLimits::default())
        .map(|scan| scan.events)
        .map_err(|error| ServiceError::Invalid(error.to_string()))
}

pub fn read_run_events(run_dir: &Utf8Path) -> Result<Vec<RunEvent>, ServiceError> {
    read_journal_events(run_dir)?
        .into_iter()
        .map(|event| RunEvent::from_flat(&event).map_err(ServiceError::Invalid))
        .collect()
}
