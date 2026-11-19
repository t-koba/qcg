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
                    // A service-owned ActiveExecution run opens its elapsed
                    // ledger during admission, so `elapsed_accounted` records
                    // may sit between run_queued and run_started. Only those
                    // budget records are admitted in that window (see the
                    // ElapsedAccounted arm); lifecycle/outcome order stays
                    // strict and there is still exactly one run_started.
                    if started || (index != 0 && !queued) {
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
            // Elapsed ledger records are budget evidence, never outcomes.
            // ActiveExecution service runs open the ledger after run_queued
            // and before run_started; the final ledger is validated exactly
            // by the planner's budget gate, so admitting the record here does
            // not relax any outcome, effect or lifecycle proof.
            RunEventData::ElapsedAccounted(_) if queued || started => {}
            RunEventData::RunFinished(api::RunFinishedEventData {
                status: RunCompletionStatus::Failed,
                ..
            })
            | RunEventData::RunError(_)
                if started && active.is_none() && failed_step =>
            {
                terminal = true;
            }
            // A settled model charge is accounting evidence, not a lifecycle
            // transition. The typed payload already requires a provider,
            // model, token usage and a settled cost; ownership of the charge
            // is proved separately against preserved outcomes by the planner.
            RunEventData::LlmCall(_) if started && active.is_some() => {}
            // A fresh memo write records the response of the active call; it
            // is not a cross-run reuse. Reused responses remain unproven here
            // because their parent journal is outside this source.
            RunEventData::LlmMemo(memo)
                if started
                    && memo.reused_from.is_none()
                    && active
                        .as_ref()
                        .is_some_and(|(owner, _)| event["node"].as_str() == Some(*owner)) => {}
            // A skipped dependent has no outcome or charge. It is reset by the
            // planner and must not be mistaken for a settled owner.
            RunEventData::StepSkipped(_)
                if started
                    && active.is_none()
                    && !settled.contains_key(event["node"].as_str().unwrap_or_default())
                    && !traces
                        .owners
                        .contains_key(event["node"].as_str().unwrap_or_default()) => {}
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
