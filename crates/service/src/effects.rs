//! Fail-closed observation of container effects orphaned by a dead run.

use camino::{Utf8Path, Utf8PathBuf};
#[cfg(test)]
use engine::JournalWriter;
#[cfg(test)]
use serde_json::json;
use std::collections::BTreeMap;

use crate::summaries::{read_recovery_state, run_meta_dir};
use crate::types::{LocalService, ServiceError};

/// Ordered by strength: merges must never downgrade evidence already observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EffectProvenance {
    /// Admission was busy before any journal could be inspected.
    ScanHint,
    /// Unreadable/unsafe evidence must survive even if it later disappears.
    PreserveEvidence,
    /// A gated fold proved this run had unresolved effects.
    ConfirmedEffects,
}

pub(crate) type EffectCandidates = BTreeMap<Utf8PathBuf, EffectProvenance>;

/// Observes every open `effect_intent` of one run. A name-keyed observation
/// cannot prove ownership, and a missing entry does not fence a late create,
/// so no outcome settles an intent: every one is retained, nothing is deleted,
/// and no `effect_closed` is journaled. The caller must hold the run's
/// execution lease, so no live process can be creating under these names.
#[cfg(test)]
pub(crate) async fn reconcile_run_effects(
    run_dir: &Utf8Path,
    run_id: &str,
    timeouts: policy::ContainerTimeouts,
) -> Result<(), ServiceError> {
    let state = crate::summaries::fold_run_state(run_dir)?;
    reconcile_effects(run_id, timeouts, &state).await
}

/// Observes effects from the caller's already-validated journal fold and
/// returns that state unchanged when it has no open intents. An open intent
/// always produces a refusal; recovery uses this without folding the same
/// journal twice.
pub(crate) async fn reconcile_run_effects_from_state(
    run_id: &str,
    timeouts: policy::ContainerTimeouts,
    state: engine::RunState,
) -> Result<engine::RunState, ServiceError> {
    reconcile_effects(run_id, timeouts, &state).await?;
    Ok(state)
}

async fn reconcile_effects(
    run_id: &str,
    timeouts: policy::ContainerTimeouts,
    state: &engine::RunState,
) -> Result<(), ServiceError> {
    let mut unresolved = Vec::new();
    for (name, backend) in &state.effects {
        let observation = match serde_json::from_value::<container::Backend>(backend.clone()) {
            Ok(backend) => container::reclaim(&backend, name, timeouts)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(format!("unreadable backend: {error}")),
        };
        let detail = match observation {
            Ok(container::Reclaimed::NamePresent) => {
                "an instance carrying the journaled name was observed".to_string()
            }
            Ok(container::Reclaimed::NameNotObserved) => {
                "no instance with the journaled name was observed in a bounded probe".to_string()
            }
            Err(error) => format!("the backend could not be probed: {error}"),
        };
        unresolved.push(format!(
            "container effect `{name}`: {detail}; qcg has not proved that the effect stopped; reconcile the external outcome manually"
        ));
    }
    if unresolved.is_empty() {
        return Ok(());
    }
    Err(ServiceError::invalid(format!(
        "container effects of run `{run_id}` are unresolved and stay journaled: {}",
        unresolved.join("; ")
    )))
}

/// Returns true only when a candidate can leave the retry queue. Paths are
/// untrusted scan hints, never authority to settle the effects of a copied fork;
/// a fail-closed refusal is durable and needs no retry.
async fn reconcile_effect_candidate(
    run_dir: &Utf8Path,
    timeouts: policy::ContainerTimeouts,
    provenance: &mut EffectProvenance,
) -> Result<bool, ServiceError> {
    let Some(admission) = crate::run_dirs::try_lock_run_admission(run_dir)? else {
        return Ok(false);
    };
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::invalid("effect candidate has no run id"))?;
    // Gate under admission before taking execution: the gate may safely wipe a
    // preparation-only fork, and must never do so while holding an execution fd.
    let Some(_) = recovery_state_for_effect_candidate(run_dir, run_id, provenance)? else {
        return Ok(true);
    };
    let Some(_execution) = crate::run_dirs::try_lock_existing_run_execution(run_dir)? else {
        return Ok(false);
    };
    // The run could have changed while a live execution lease was held. Re-read
    // under both leases so cleanup uses its final journal state.
    let Some(state) = recovery_state_for_effect_candidate(run_dir, run_id, provenance)? else {
        return Ok(true);
    };
    // Execution ownership protects the validated run through the bounded
    // presence probe; unrelated IDs need not wait on this shard.
    drop(admission);
    if let Err(error) = reconcile_effects(run_id, timeouts, &state).await {
        // A name-keyed refusal is permanent, not a transient probe failure:
        // record it durably for every run and stop retrying the candidate.
        // The settlement helper writes a failed terminal result only when the
        // run has none yet; an already-terminal run keeps its outcome and gets
        // a state-free `effect_unresolved` detail instead.
        crate::run_dirs::settle_unresolved_effect(run_dir, &error.to_string())?;
        return Ok(true);
    }
    Ok(true)
}

fn recovery_state_for_effect_candidate(
    run_dir: &Utf8Path,
    run_id: &str,
    provenance: &mut EffectProvenance,
) -> Result<Option<engine::RunState>, ServiceError> {
    // Revalidate every component under admission (and again under execution).
    // A missing or empty journal clears only a hint, never known/uncertain evidence.
    let meta = run_meta_dir(run_dir);
    let journal = meta.join("journal.jsonl");
    for (path, directory) in [
        (run_dir, true),
        (meta.as_path(), true),
        (journal.as_path(), false),
    ] {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return missing_effect_evidence(run_dir, provenance);
            }
            Err(error) => return Err(error.into()),
            Ok(metadata)
                if if directory {
                    metadata.is_dir()
                } else {
                    metadata.is_file()
                } => {}
            Ok(_) => {
                return Err(ServiceError::invalid(format!(
                    "effect candidate `{path}` is not a plain {}",
                    if directory { "directory" } else { "file" }
                )));
            }
        }
    }
    if crate::run_dirs::journal_is_empty(&journal)? {
        return missing_effect_evidence(run_dir, provenance);
    }
    let Some((_, state)) = read_recovery_state(run_dir)? else {
        // The fork admission gate is the only supported operation here that
        // removes its journal while preserving the run/control directory.
        return match std::fs::symlink_metadata(run_meta_dir(run_dir).join("journal.jsonl")) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A gate-authorized wipe settles a copied preparation prefix,
                // but cannot erase effects previously confirmed for this run.
                if *provenance == EffectProvenance::ConfirmedEffects {
                    missing_effect_evidence(run_dir, provenance)
                } else {
                    Ok(None)
                }
            }
            Err(error) => Err(error.into()),
            Ok(_) => Err(ServiceError::invalid(format!(
                "effect candidate `{run_dir}` has no foldable recovery state"
            ))),
        };
    };
    if state.run_id.as_deref() != Some(run_id) {
        return Err(ServiceError::invalid(format!(
            "effect candidate `{run_dir}` has no matching run identity"
        )));
    }
    if !state.effects.is_empty() {
        *provenance = EffectProvenance::ConfirmedEffects;
    }
    Ok(Some(state))
}

fn missing_effect_evidence(
    run_dir: &Utf8Path,
    provenance: &EffectProvenance,
) -> Result<Option<engine::RunState>, ServiceError> {
    if *provenance == EffectProvenance::ScanHint {
        Ok(None)
    } else {
        Err(ServiceError::invalid(format!(
            "effect candidate `{run_dir}` is missing recovery evidence ({provenance:?})"
        )))
    }
}

impl LocalService {
    pub(crate) fn merge_effect_candidates(
        &self,
        candidates: impl IntoIterator<Item = (Utf8PathBuf, EffectProvenance)>,
    ) {
        let mut pending = self
            .inner
            .unreconciled_effects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (path, provenance) in candidates {
            pending
                .entry(path)
                .and_modify(|old| *old = (*old).max(provenance))
                .or_insert(provenance);
        }
    }

    /// Converge a nonterminal record when this process settled a durable
    /// refusal. The spawn path's terminal precheck skips memory convergence,
    /// so an unconverged record would be retried by the queued resumer forever
    /// in Exclusive mode, where no shared refresh repairs memory.
    async fn converge_settled_effect_run(&self, run_dir: &Utf8Path) {
        let Some(run_id) = run_dir.file_name() else {
            return;
        };
        let Ok(state) = crate::summaries::fold_run_state(run_dir) else {
            // Leave memory untouched: the queued resumer can re-read it.
            return;
        };
        let Some(terminal) = state.terminal.clone() else {
            return;
        };
        {
            let mut runs = self.inner.runs.write().await;
            if let Some(record) = runs.get_mut(run_id)
                && !record.state.is_terminal()
                && !crate::types::task_slot_is_live(&record.task)
            {
                // A live owner's own finalizer must not be disturbed.
                record.state = crate::lifecycle::terminal_status(&terminal);
                record.preempted = false;
                record.clear_prompt();
            }
        }
        self.inner.queue_notify.notify_waiters();
    }

    /// Retry discovery and reconciliation from bounded startup/shared scans.
    /// Busy leases and failed evidence/cleanup remain candidates, deduplicated
    /// with discoveries made while this pass was awaiting a backend.
    pub(crate) async fn reconcile_startup_effects(&self) {
        let dirs = std::mem::take(
            &mut *self
                .inner
                .unreconciled_effects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let mut retry = Vec::new();
        for (run_dir, mut provenance) in dirs {
            match reconcile_effect_candidate(
                &run_dir,
                self.inner.deployment_policy.container_timeouts,
                &mut provenance,
            )
            .await
            {
                Ok(true) => self.converge_settled_effect_run(&run_dir).await,
                Ok(false) => retry.push((run_dir, provenance)),
                Err(error) => {
                    tracing::error!(%run_dir, %error, "container effect discovery/reconciliation incomplete");
                    retry.push((run_dir, provenance.max(EffectProvenance::PreserveEvidence)));
                }
            }
        }
        self.merge_effect_candidates(retry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_dirs::{try_lock_run_admission, try_lock_run_execution};
    use crate::summaries::{refresh_runs_with_effects, rehydrate_runs_with_effects};
    use crate::tests::support::{TempGuard, temp_run_dir};

    const MARKER: &str = "qcg has not proved that the effect stopped";

    fn service(runs: &Utf8Path) -> LocalService {
        LocalService::with_generator_roots_policy_and_store_mode(
            vec![],
            runs.to_owned(),
            None,
            1,
            8,
            crate::RunStoreMode::SharedFilesystem,
            crate::ServiceDeploymentPolicy::default(),
        )
        .unwrap()
    }

    fn pending(service: &LocalService) -> Vec<Utf8PathBuf> {
        service
            .inner
            .unreconciled_effects
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    // An invalid backend deterministically reports a cleanup failure without
    // invoking any external binary. The journal and effect fold are real.
    fn terminal_effect(dir: &Utf8Path) {
        let meta = run_meta_dir(dir);
        std::fs::create_dir_all(&meta).unwrap();
        let journal = JournalWriter::create(
            &meta.join("journal.jsonl"),
            dir.file_name().unwrap(),
            false,
            None,
        )
        .unwrap();
        journal.event("run_queued", json!({
            "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
            "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
        })).unwrap();
        journal
            .event(
                "effect_intent",
                json!({"effect": "fixture-effect", "backend": {"unknown": true}}),
            )
            .unwrap();
        journal
            .event(
                "run_canceled",
                json!({"reason": {"code": "canceled", "message": "test"}}),
            )
            .unwrap();
    }

    /// Same unreadable effect intent, but without a terminal event: the
    /// refusal must be recorded durably instead of being silently retried.
    fn open_effect(dir: &Utf8Path) {
        let meta = run_meta_dir(dir);
        std::fs::create_dir_all(&meta).unwrap();
        let journal = JournalWriter::create(
            &meta.join("journal.jsonl"),
            dir.file_name().unwrap(),
            false,
            None,
        )
        .unwrap();
        journal.event("run_queued", json!({
            "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
            "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
        })).unwrap();
        journal
            .event(
                "effect_intent",
                json!({"effect": "fixture-effect", "backend": {"unknown": true}}),
            )
            .unwrap();
    }

    fn assert_no_effect_closure(dir: &Utf8Path) {
        let journal = run_meta_dir(dir).join("journal.jsonl");
        let events = crate::summaries::read_journal_events(dir).unwrap();
        assert!(
            events.iter().all(|event| event["t"] != "effect_closed"),
            "recovery never settles a name-keyed effect: {journal}"
        );
        assert!(
            crate::summaries::fold_run_state(dir)
                .unwrap()
                .effects
                .contains_key("fixture-effect"),
            "the unreadable intent must stay journaled"
        );
    }

    /// A terminal run keeps its original journal prefix byte-for-byte and
    /// receives exactly one state-free diagnostic carrying the same marker.
    fn assert_terminal_effect_diagnostic(dir: &Utf8Path, original: &[u8]) {
        let journal = run_meta_dir(dir).join("journal.jsonl");
        assert!(
            std::fs::read(&journal).unwrap().starts_with(original),
            "the original terminal journal must stay byte-for-byte unchanged"
        );
        let events = crate::summaries::read_journal_events(dir).unwrap();
        let original_count = original.iter().filter(|byte| **byte == b'\n').count();
        assert_eq!(events.len(), original_count + 1, "exactly one diagnostic");
        assert_eq!(events[original_count - 1]["t"], "run_canceled");
        let diagnostic = &events[original_count];
        assert_eq!(diagnostic["t"], "effect_unresolved");
        assert!(
            diagnostic["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains(MARKER)),
            "{diagnostic}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["t"] == "run_canceled")
                .count(),
            1
        );
        assert_no_effect_closure(dir);
    }

    #[tokio::test]
    async fn busy_preparation_hints_clear_after_locked_revalidation() {
        for evidence in ["no-meta", "no-journal", "empty-journal", "gone"] {
            let root = temp_run_dir(&format!("effect-preparation-{evidence}"));
            let _guard = TempGuard(root.clone());
            let runs = root.join("runs");
            let peer = service(&runs);
            let dir = runs.join("run-37");
            std::fs::create_dir_all(&dir).unwrap();
            if matches!(evidence, "no-journal" | "empty-journal") {
                std::fs::create_dir_all(run_meta_dir(&dir)).unwrap();
            }
            if evidence == "empty-journal" {
                std::fs::write(run_meta_dir(&dir).join("journal.jsonl"), b"").unwrap();
            }
            // A different run's colliding shard hides even the absence of a journal.
            let admission = try_lock_run_admission(&runs.join("run-0"))
                .unwrap()
                .unwrap();
            assert!(try_lock_run_admission(&dir).unwrap().is_none());
            let startup = service(&runs);
            peer.refresh_shared_runs().await.unwrap();
            for svc in [&startup, &peer] {
                svc.reconcile_startup_effects().await;
                assert_eq!(pending(svc), std::slice::from_ref(&dir));
                assert_eq!(
                    svc.inner.unreconciled_effects.lock().unwrap()[&dir],
                    EffectProvenance::ScanHint
                );
            }
            if evidence == "gone" {
                std::fs::remove_dir_all(&dir).unwrap();
            }
            drop(admission);
            for svc in [&startup, &peer] {
                svc.reconcile_startup_effects().await;
                assert!(pending(svc).is_empty(), "{evidence}");
            }
            assert!(!run_meta_dir(&dir).join("execution.lock").exists());
            if evidence == "gone" {
                assert!(
                    !dir.exists(),
                    "revalidation must not recreate a deleted run"
                );
            }
        }
    }

    #[tokio::test]
    async fn confirmed_effect_retains_provenance_when_recovery_evidence_disappears() {
        let root = temp_run_dir("effect-confirmed-evidence-loss");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let service = service(&runs);
        let dir = runs.join("run-37");
        terminal_effect(&dir);
        let journal = run_meta_dir(&dir).join("journal.jsonl");
        let original = std::fs::read(&journal).unwrap();
        let admission = try_lock_run_admission(&runs.join("run-0"))
            .unwrap()
            .unwrap();
        service.refresh_shared_runs().await.unwrap();
        assert_eq!(
            service.inner.unreconciled_effects.lock().unwrap()[&dir],
            EffectProvenance::ScanHint
        );
        drop(admission);
        // Folding before execution contention must upgrade the hint for its retry.
        let execution = try_lock_run_execution(&dir).unwrap().unwrap();
        service.reconcile_startup_effects().await;
        assert_eq!(
            service.inner.unreconciled_effects.lock().unwrap()[&dir],
            EffectProvenance::ConfirmedEffects
        );
        drop(execution);
        // Exercise both orders of the merge used when discoveries arrive during
        // an in-flight reconciliation pass. Neither can weaken confirmed evidence.
        let retry = std::mem::take(&mut *service.inner.unreconciled_effects.lock().unwrap());
        service.merge_effect_candidates([(dir.clone(), EffectProvenance::ScanHint)]);
        service.merge_effect_candidates(retry);
        service.merge_effect_candidates([(dir.clone(), EffectProvenance::PreserveEvidence)]);
        for missing in [journal.clone(), run_meta_dir(&dir), dir.clone()] {
            if missing == journal {
                std::fs::remove_file(&missing).unwrap();
            } else {
                std::fs::remove_dir_all(&missing).unwrap();
            }
            let mut provenance = EffectProvenance::ConfirmedEffects;
            let error = reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut provenance,
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains("missing recovery evidence"),
                "{error}"
            );
            service.reconcile_startup_effects().await;
            assert_eq!(
                service.inner.unreconciled_effects.lock().unwrap()[&dir],
                EffectProvenance::ConfirmedEffects
            );
            assert!(!missing.exists(), "missing evidence must not be recreated");
        }
        // An empty journal is not proof of settlement, and only a ScanHint
        // (never confirmed/preserved evidence) clears when evidence disappears.
        std::fs::create_dir_all(run_meta_dir(&dir)).unwrap();
        std::fs::write(&journal, b"").unwrap();
        service.reconcile_startup_effects().await;
        assert_eq!(
            pending(&service),
            std::slice::from_ref(&dir),
            "empty is not proof of settlement"
        );
        std::fs::write(&journal, &original).unwrap();
        service.merge_effect_candidates([(dir.clone(), EffectProvenance::ScanHint)]);
        service.reconcile_startup_effects().await;
        assert!(
            pending(&service).is_empty(),
            "a durable refusal needs no retry"
        );
        assert_terminal_effect_diagnostic(&dir, &original);
    }

    #[tokio::test]
    async fn colliding_admission_retains_unreadable_effect_until_the_lease_is_released() {
        let root = temp_run_dir("effect-discovery-collision");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let peer = service(&runs); // Exists before the peer's terminal journal.
        let dir = runs.join("run-37");
        terminal_effect(&dir);
        let journal = run_meta_dir(&dir).join("journal.jsonl");
        // These distinct IDs share the FNV admission shard. A need not even
        // have a journal: only its unrelated preparation lease blocks B.
        let admission = try_lock_run_admission(&runs.join("run-0"))
            .unwrap()
            .unwrap();
        assert!(try_lock_run_admission(&dir).unwrap().is_none());
        let (records, candidates) =
            rehydrate_runs_with_effects(&runs, 8, 8, policy::LIVE_EVENT_CHANNEL_CAPACITY).unwrap();
        assert!(records.is_empty());
        assert_eq!(
            candidates,
            BTreeMap::from([(dir.clone(), EffectProvenance::ScanHint)])
        );
        let startup = service(&runs);
        assert_eq!(pending(&startup), std::slice::from_ref(&dir));
        for _ in 0..2 {
            peer.refresh_shared_runs().await.unwrap();
            peer.reconcile_startup_effects().await;
            assert_eq!(
                pending(&peer),
                std::slice::from_ref(&dir),
                "busy hints stay deduplicated"
            );
        }
        let original = std::fs::read(&journal).unwrap();
        drop(admission);
        assert!(
            reconcile_effect_candidate(
                &dir,
                peer.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint,
            )
            .await
            .unwrap(),
            "a permanent refusal resolves the candidate without cleanup"
        );
        assert_terminal_effect_diagnostic(&dir, &original);
        startup.reconcile_startup_effects().await;
        assert!(pending(&startup).is_empty());
        // A later scan legitimately rediscovers the still-open intent, but
        // its pass resolves it without deleting anything or journaling closure.
        let (_, rescanned) =
            rehydrate_runs_with_effects(&runs, 8, 8, policy::LIVE_EVENT_CHANNEL_CAPACITY).unwrap();
        assert_eq!(
            rescanned,
            BTreeMap::from([(dir.clone(), EffectProvenance::ConfirmedEffects)])
        );
        peer.inner.unreconciled_effects.lock().unwrap().clear();
        peer.merge_effect_candidates(rescanned);
        peer.reconcile_startup_effects().await;
        assert!(pending(&peer).is_empty());
        assert_no_effect_closure(&dir);
    }

    #[tokio::test]
    async fn an_unreadable_effect_refusal_is_durable_for_a_nonterminal_run() {
        let root = temp_run_dir("effect-refusal-detail");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let service = service(&runs);
        let dir = runs.join("run-88");
        open_effect(&dir);
        assert!(
            reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint,
            )
            .await
            .unwrap()
        );
        let events = crate::summaries::read_journal_events(&dir).unwrap();
        let terminal = events
            .iter()
            .find(|event| event["t"] == "run_finished")
            .expect("the refusal must be durable");
        assert_eq!(terminal["status"], "failed");
        assert_eq!(terminal["metrics"], json!({}));
        assert_eq!(terminal["reason"]["code"], "execution_failed");
        assert!(
            terminal["reason"]["message"]
                .as_str()
                .unwrap()
                .contains("qcg has not proved that the effect stopped"),
            "{terminal}"
        );
        assert!(
            terminal["reason"]["message"]
                .as_str()
                .unwrap()
                .contains("unreadable backend")
        );
        assert_no_effect_closure(&dir);
        assert!(pending(&service).is_empty());
        let settled = crate::summaries::read_journal_events(&dir).unwrap();
        assert!(
            reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint,
            )
            .await
            .unwrap()
        );
        let after_retry = crate::summaries::read_journal_events(&dir).unwrap();
        assert_eq!(
            &after_retry[..settled.len()],
            settled,
            "the terminal result must never be rewritten"
        );
        assert_eq!(after_retry.len(), settled.len() + 1, "one diagnostic");
        assert_eq!(after_retry.last().unwrap()["t"], "effect_unresolved");
        assert!(
            after_retry.last().unwrap()["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains(MARKER)),
            "{after_retry:?}"
        );
        assert_eq!(
            after_retry
                .iter()
                .filter(|event| event["t"] == "run_finished")
                .count(),
            1,
            "no second terminal event"
        );
        // A third scan must not grow the journal: repeated discovery of the
        // still-open intent is the steady state under the 5s rescan worker.
        assert!(
            reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint,
            )
            .await
            .unwrap()
        );
        let after_third = crate::summaries::read_journal_events(&dir).unwrap();
        assert_eq!(after_third, after_retry, "no diagnostic growth on rescan");
    }

    #[tokio::test]
    async fn execution_contention_and_malformed_evidence_remain_retryable() {
        let root = temp_run_dir("effect-discovery-evidence");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let dir = runs.join("terminal");
        terminal_effect(&dir);
        let service = service(&runs);
        let lease = try_lock_run_execution(&dir).unwrap().unwrap();
        let journal = run_meta_dir(&dir).join("journal.jsonl");
        let original = std::fs::read(&journal).unwrap();
        assert!(
            !reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint
            )
            .await
            .unwrap(),
            "a held execution lease retains a valid candidate"
        );
        std::fs::write(&journal, b"not json\n").unwrap();
        assert!(
            reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint
            )
            .await
            .is_err(),
            "malformed evidence is refused"
        );
        service.reconcile_startup_effects().await;
        assert_eq!(pending(&service), std::slice::from_ref(&dir));
        drop(lease);
        assert!(
            reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint
            )
            .await
            .is_err()
        );
        std::fs::remove_file(&journal).unwrap();
        service.reconcile_startup_effects().await;
        assert_eq!(pending(&service), std::slice::from_ref(&dir));
        std::fs::write(&journal, b"not json\n").unwrap();
        assert!(
            reconcile_run_effects(
                &dir,
                "terminal",
                service.inner.deployment_policy.container_timeouts
            )
            .await
            .is_err()
        );
        let scan =
            refresh_runs_with_effects(&runs, 8, 8, policy::LIVE_EVENT_CHANNEL_CAPACITY).unwrap();
        assert_eq!(scan.failures, 1);
        assert_eq!(
            scan.effects,
            BTreeMap::from([(dir.clone(), EffectProvenance::PreserveEvidence)])
        );
        service.refresh_shared_runs().await.unwrap();
        service.reconcile_startup_effects().await;
        assert_eq!(pending(&service), std::slice::from_ref(&dir));
        assert_eq!(std::fs::read(&journal).unwrap(), b"not json\n");
        std::fs::write(&journal, &original).unwrap();
        let error = reconcile_run_effects(
            &dir,
            "terminal",
            service.inner.deployment_policy.container_timeouts,
        )
        .await
        .expect_err("an open intent must be refused");
        assert!(
            error
                .to_string()
                .contains("qcg has not proved that the effect stopped"),
            "{error}"
        );
        assert!(
            reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint,
            )
            .await
            .unwrap()
        );
        service.reconcile_startup_effects().await;
        assert!(pending(&service).is_empty());
        assert_no_effect_closure(&dir);
        // A fresh hint that encounters malformed evidence is no longer allowed
        // to clear merely because that uncertain evidence later disappears.
        std::fs::write(&journal, b"not json\n").unwrap();
        service.merge_effect_candidates([(dir.clone(), EffectProvenance::ScanHint)]);
        service.reconcile_startup_effects().await;
        assert_eq!(
            service.inner.unreconciled_effects.lock().unwrap()[&dir],
            EffectProvenance::PreserveEvidence
        );
        std::fs::remove_file(&journal).unwrap();
        service.reconcile_startup_effects().await;
        assert_eq!(pending(&service), std::slice::from_ref(&dir));
        std::fs::write(&journal, &original).unwrap();
        assert!(
            reconcile_effect_candidate(
                &dir,
                service.inner.deployment_policy.container_timeouts,
                &mut EffectProvenance::ScanHint,
            )
            .await
            .unwrap()
        );
        service.reconcile_startup_effects().await;
        assert!(pending(&service).is_empty());
        assert_no_effect_closure(&dir);
        service.merge_effect_candidates([(dir.clone(), EffectProvenance::ScanHint)]);
        std::fs::remove_dir_all(&dir).unwrap();
        service.reconcile_startup_effects().await;
        assert!(pending(&service).is_empty());
        assert!(!dir.exists(), "retry must not recreate deleted runs");
    }
    #[tokio::test]
    async fn unverified_fork_candidate_runs_the_admission_gate_before_effect_cleanup() {
        let root = temp_run_dir("effect-discovery-fork-gate");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let service = service(&runs);
        let dir = runs.join("child");
        terminal_effect(&dir);
        let journal = run_meta_dir(&dir).join("journal.jsonl");
        let mut events = crate::summaries::read_journal_events(&dir).unwrap();
        events.pop(); // The copied source prefix is not terminal at the fork.
        events.push(json!({
            "t": "run_forked", "ts": chrono::Utc::now().to_rfc3339(),
            "seq": 3, "run_id": "child", "source_run_id": "source", "source_seq": 2,
            "trace_id": api::trace_id_for_run("child"), "span_id": api::span_id_for_seq(3),
        }));
        // Unexpected child history must be preserved, not reclaimed or wiped.
        events.push(json!({
            "t": "effect_intent", "ts": chrono::Utc::now().to_rfc3339(),
            "seq": 4, "run_id": "child", "effect": "child-effect", "backend": {},
            "trace_id": api::trace_id_for_run("child"), "span_id": api::span_id_for_seq(4),
        }));
        let write = |events: &[serde_json::Value]| {
            let text = events
                .iter()
                .map(|event| format!("{event}\n"))
                .collect::<String>();
            std::fs::write(&journal, text).unwrap();
        };
        write(&events);
        let error = reconcile_effect_candidate(
            &dir,
            service.inner.deployment_policy.container_timeouts,
            &mut EffectProvenance::ScanHint,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("post-marker history"), "{error}");
        service.merge_effect_candidates([(dir.clone(), EffectProvenance::ScanHint)]);
        service.reconcile_startup_effects().await;
        assert_eq!(pending(&service), std::slice::from_ref(&dir));
        assert_eq!(crate::summaries::read_journal_events(&dir).unwrap(), events);
        // With only the expected preparation prefix, the gate wipes it. If
        // copied source effects reached reclamation, the invalid backend
        // would instead fail and leave the journal/candidate behind.
        events.pop();
        write(&events);
        service.reconcile_startup_effects().await;
        assert!(pending(&service).is_empty());
        assert!(!journal.exists());
    }
}
