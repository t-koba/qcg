//! Test-only temporary directory guard with process-unique names.
//! Production code must not depend on developer-machine tempdir behavior;
//! tests use this guard instead of the `tempfile` crate.

#[cfg(test)]
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
pub(crate) struct TestDir {
    path: PathBuf,
}

#[cfg(test)]
impl TestDir {
    pub(crate) fn create(name: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("engine-test-{name}-{}-{nonce}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("test directory should be creatable");
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Reset-child staging for downstream continuation tests. Plans the sealed
/// source once, then mirrors the service's private fork projection into a
/// fresh child journal: copied source history, `run_forked`, the planned
/// overlay and `run_queued`. No production caller can reach this helper; it
/// never mutates the source and accepts no replacement plan or budget.
#[cfg(feature = "test-support")]
pub fn stage_reset_child(
    source_run_dir: &camino::Utf8Path,
    source_run_id: &str,
    child_run_dir: &camino::Utf8Path,
    child_run_id: &str,
    registry: &crate::StepRegistry,
    limits: crate::JournalLimits,
) -> Result<crate::ResetPlanCandidate, String> {
    use crate::ForkTraceProjection;
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};

    let plan = crate::plan_continuation(source_run_dir, registry, limits)
        .map_err(|refusal| refusal.reason().to_owned())?;
    let source_journal = source_run_dir.join("meta").join("journal.jsonl");
    let events = crate::read_journal_values(&source_journal, limits)
        .map_err(|error| error.to_string())?
        .events;
    let selected = events
        .into_iter()
        .filter(|event| {
            event["seq"]
                .as_u64()
                .is_some_and(|seq| seq <= plan.source_last_seq())
                && !matches!(
                    event["t"].as_str(),
                    Some(
                        "run_error"
                            | "run_finished"
                            | "run_canceled"
                            | "run_interrupted"
                            | "run_waiting"
                    )
                )
        })
        .collect::<Vec<Value>>();
    let contract_sha256 = selected
        .iter()
        .find(|event| event["t"] == "run_started")
        .and_then(|event| event["contract_sha256"].as_str())
        .ok_or("source has no bound run_started contract")?
        .to_owned();
    let mut projection =
        ForkTraceProjection::new(&source_journal, &selected, plan.source_last_seq(), limits)
            .ok_or("source private traces cannot be projected")?;

    let candidate = serde_json::to_value(&plan).map_err(|error| error.to_string())?;
    let candidate_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&candidate).map_err(|error| error.to_string())?,
    ));
    let binding = json!({
        "binding_version": 1,
        "plan_digest": "0".repeat(64),
        "reset_candidate_sha256": candidate_sha256,
    });
    let overlay = plan
        .fork_overlay_evidence(binding.clone())
        .map_err(|error| error.to_string())?;
    let overlay_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&overlay).map_err(|error| error.to_string())?,
    ));

    let child_meta = child_run_dir.join("meta");
    std::fs::create_dir_all(&child_meta).map_err(|error| error.to_string())?;
    let child_journal = child_meta.join("journal.jsonl");
    let mut child_events = Vec::new();
    let mut seq = 0_u64;
    for mut event in selected {
        let source_seq = event["seq"].as_u64().ok_or("selected event has no seq")?;
        seq += 1;
        event["seq"] = json!(seq);
        event["run_id"] = json!(child_run_id);
        projection
            .bind(source_seq, &event)
            .ok_or("source trace cannot be bound to the child event")?;
        child_events.push(event);
    }
    seq += 1;
    child_events.push(json!({
        "t": "run_forked", "seq": seq, "ts": "2026-01-01T00:00:00Z",
        "run_id": child_run_id, "trace_id": "test", "span_id": "test",
        "source_run_id": source_run_id, "source_seq": plan.source_last_seq(),
        "source_contract_sha256": contract_sha256,
        "binding": binding, "reset_overlay_sha256": overlay_sha256,
    }));
    seq += 1;
    child_events.push(json!({
        "t": "child_reset_overlay", "seq": seq, "ts": "2026-01-01T00:00:00Z",
        "run_id": child_run_id, "trace_id": "test", "span_id": "test",
        "overlay": overlay,
    }));
    seq += 1;
    let mut queued = child_events
        .iter()
        .find(|event| event["t"] == "run_started")
        .ok_or("source history has no run_started event")?
        .clone();
    queued["t"] = json!("run_queued");
    queued["seq"] = json!(seq);
    queued["parent_run_id"] = json!(source_run_id);
    queued["reuse"] = json!("matching");
    queued
        .as_object_mut()
        .ok_or("run_started event is not an object")?
        .remove("step_budget_baseline");
    child_events.push(queued);

    std::fs::write(
        &child_journal,
        child_events
            .iter()
            .map(|event| format!("{event}\n"))
            .collect::<String>(),
    )
    .map_err(|error| error.to_string())?;
    projection.publish(&child_journal);
    Ok(plan)
}
