//! A deliberately narrow history proof, not permission to continue a run.
use super::{JournalLimits, JournalStats, OutcomeTraces, digest, serialize_bounded};
use api::{RunCompletionStatus, RunEvent, RunEventData, StepStatus};
use camino::Utf8Path;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read as _, Seek as _, SeekFrom};

pub(super) fn journal_is_cleanly_sealed(path: &Utf8Path) -> bool {
    if !super::super::writer::clean_shutdown_marker_is_valid(path).is_ok_and(|valid| valid) {
        return false;
    }
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let Ok(metadata) = file.metadata() else {
        return false;
    };
    if !metadata.is_file() || metadata.len() == 0 || file.seek(SeekFrom::End(-1)).is_err() {
        return false;
    }
    let mut last = [0_u8; 1];
    file.read_exact(&mut last).is_ok() && last[0] == b'\n'
}

pub(super) fn proves_failed_history(
    events: &[Value],
    traces: &OutcomeTraces,
    limits: JournalLimits,
) -> Option<()> {
    let run_id = events.first()?["run_id"].as_str()?;
    if run_id.is_empty() {
        return None;
    }
    let mut seq = None;
    let mut queued = false;
    let mut started = false;
    let mut terminal = false;
    let mut failed_step = false;
    let mut active = None;
    let mut settled = HashMap::new();
    let mut stats = JournalStats::default();
    for (index, event) in events.iter().enumerate() {
        // Ordinary callers can request unbounded reads. Evidence still uses
        // the existing journal policies, including their configured defaults.
        let bytes = serialize_bounded(event, limits.max_event_bytes, "history evidence").ok()?;
        super::super::append_serialized_json_line(&mut std::io::sink(), bytes, &mut stats, limits)
            .ok()?;
        let current_seq = event["seq"].as_u64()?;
        if terminal
            || current_seq == 0
            || seq.is_some_and(|previous| current_seq <= previous)
            || event["run_id"].as_str() != Some(run_id)
            || event["node"]
                .as_str()
                .is_some_and(|node| node.starts_with("hook."))
        {
            return None;
        }
        seq = Some(current_seq);
        let parsed = RunEvent::from_flat(event).ok()?;
        // budget_charged is internal and has no public typed payload.
        if event["t"] == "budget_charged" {
            let object = event.as_object()?;
            if object.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "t" | "seq"
                        | "ts"
                        | "run_id"
                        | "trace_id"
                        | "span_id"
                        | "parent_span_id"
                        | "node"
                        | "amount"
                )
            }) {
                return None;
            }
            let (owner, charged) = active.as_mut()?;
            if event["node"].as_str() != Some(*owner)
                || event["amount"].as_u64() != Some(1)
                || *charged
            {
                return None;
            }
            *charged = true;
            continue;
        }
        match parsed.data {
            RunEventData::RunQueued(identity) | RunEventData::RunStarted(identity) => {
                if identity.schema_version != api::JOURNAL_SCHEMA_VERSION
                    || identity.parent_run_id.is_some()
                    || identity.reuse.is_some()
                {
                    return None;
                }
                if event["t"] == "run_queued" {
                    if index != 0 || queued || started {
                        return None;
                    }
                    queued = true;
                } else {
                    if started || index != usize::from(queued) {
                        return None;
                    }
                    started = true;
                }
            }
            RunEventData::StepStarted(step) if started => {
                let owner = event["node"].as_str()?;
                if owner.is_empty()
                    // Iteration and correction owners are not root steps.
                    || owner.contains(['[', ']', '/', '@'])
                    || active.is_some()
                    || settled.contains_key(owner)
                    || step.attempt != 1
                    || step.parallel
                    || step.step_type.is_empty()
                    // Event history proves lifecycle only; the independent
                    // executor-effect gate decides whether a node is reusable.
                    || step.step_type == "foreach"
                {
                    return None;
                }
                active = Some((owner, false));
            }
            RunEventData::StepFinished(step) if started => {
                let (owner, charged) = active.take()?;
                if event["node"].as_str() != Some(owner)
                    || !charged
                    || step.parallel
                    || step.to.is_some()
                    || step.question.is_some()
                    || step.answer.is_some()
                {
                    return None;
                }
                match step.status {
                    StepStatus::Success => {}
                    StepStatus::Failed | StepStatus::CheckFailed => failed_step = true,
                    _ => return None,
                }
                settled.insert(owner, event);
            }
            RunEventData::GraphResolved(_) | RunEventData::Resource(_)
                if started && active.is_none() && settled.is_empty() => {}
            RunEventData::ElapsedAccounted(_) if started => {}
            RunEventData::RunFinished(api::RunFinishedEventData {
                status: RunCompletionStatus::Failed,
                ..
            })
            | RunEventData::RunError(_)
                if started && active.is_none() && failed_step =>
            {
                terminal = true;
            }
            // Positive allowlist: retries, hooks, interactions, effects,
            // replay, lifecycle restarts and all future kinds are unproven.
            _ => return None,
        }
    }
    if !terminal {
        return None;
    }
    for (owner, outcome) in &traces.owners {
        let event = settled.get(owner.as_str())?;
        if event["seq"].as_u64() != Some(outcome.seq)
            || digest(event) != Some(outcome.event_digest)
            || !outcome
                .trace
                .complete_for(&contract::ReadOwner::Node(owner.clone()))
        {
            return None;
        }
    }
    Some(())
}
