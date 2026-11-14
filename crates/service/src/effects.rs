//! Reclaims container effects orphaned by a dead run, by journaled name.

use camino::{Utf8Path, Utf8PathBuf};
use engine::{JournalLimits, JournalWriter};
use serde_json::json;

use crate::summaries::{fold_run_state, read_recovery_state, run_meta_dir};
use crate::types::{LocalService, ServiceError};

/// Settles every open `effect_intent` of one run: the exact journaled name is
/// looked up in its backend; a present instance is removed, an absent one is
/// confirmed absent, and either way `effect_closed` is journaled. A backend
/// failure leaves that intent open for the next recovery and is reported
/// after the remaining intents were attempted. The caller must hold the run's
/// execution lease, so no live process can be creating under these names.
pub(crate) async fn reconcile_run_effects(
    run_dir: &Utf8Path,
    run_id: &str,
    timeouts: policy::ContainerTimeouts,
) -> Result<(), ServiceError> {
    let state = fold_run_state(run_dir)?;
    reconcile_effects(run_dir, run_id, timeouts, state).await
}

async fn reconcile_effects(
    run_dir: &Utf8Path,
    run_id: &str,
    timeouts: policy::ContainerTimeouts,
    state: engine::RunState,
) -> Result<(), ServiceError> {
    let journal = run_meta_dir(run_dir).join("journal.jsonl");
    let mut unresolved = Vec::new();
    for (name, backend) in state.effects {
        let reclaimed = match serde_json::from_value::<container::Backend>(backend) {
            Ok(backend) => container::reclaim(&backend, &name, timeouts)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(format!("unreadable backend: {error}")),
        };
        let outcome = match reclaimed {
            Ok(container::Reclaimed::Removed) => "removed",
            Ok(container::Reclaimed::Absent) => "absent",
            Err(error) => {
                unresolved.push(format!("{name}: {error}"));
                continue;
            }
        };
        if let Err(error) = JournalWriter::append_single_event(
            &journal,
            run_id,
            "effect_closed",
            json!({"effect": name, "outcome": outcome}),
            JournalLimits::default(),
            None,
        ) {
            unresolved.push(format!("{name}: closure not journaled: {error}"));
        }
    }
    if unresolved.is_empty() {
        return Ok(());
    }
    Err(ServiceError::invalid(format!(
        "container effects of run `{run_id}` are not yet reclaimed and stay journaled for the next recovery: {}",
        unresolved.join("; ")
    )))
}

/// Returns true only when a candidate can leave the retry queue. Paths are
/// untrusted scan hints, not authority to reclaim the effects of a copied fork.
async fn reconcile_effect_candidate(
    run_dir: &Utf8Path,
    timeouts: policy::ContainerTimeouts,
) -> Result<bool, ServiceError> {
    // Do not recreate a deleted run, even while its former shard is busy.
    match std::fs::symlink_metadata(run_dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error.into()),
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(ServiceError::invalid(format!(
                "effect candidate `{run_dir}` is not a plain directory"
            )));
        }
    }
    let Some(admission) = crate::run_dirs::try_lock_run_admission(run_dir)? else {
        return Ok(false);
    };
    let meta = run_meta_dir(run_dir);
    let journal = meta.join("journal.jsonl");
    for (path, directory) in [(meta.as_path(), true), (journal.as_path(), false)] {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ServiceError::invalid(format!(
                    "effect candidate `{run_dir}` is missing recovery evidence at `{path}`"
                )));
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
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::invalid("effect candidate has no run id"))?;
    // Gate under admission before taking execution: the gate may safely wipe a
    // preparation-only fork, and must never do so while holding an execution fd.
    let Some(_) = recovery_state_for_effect_candidate(run_dir, run_id)? else {
        return Ok(true);
    };
    let Some(_execution) = crate::run_dirs::try_lock_existing_run_execution(run_dir)? else {
        return Ok(false);
    };
    // The run could have changed while a live execution lease was held. Re-read
    // under both leases so cleanup uses its final journal state.
    let Some(state) = recovery_state_for_effect_candidate(run_dir, run_id)? else {
        return Ok(true);
    };
    // Execution ownership protects the validated run through external cleanup
    // and journal closure; unrelated IDs need not wait on this shard.
    drop(admission);
    reconcile_effects(run_dir, run_id, timeouts, state).await?;
    Ok(true)
}

fn recovery_state_for_effect_candidate(
    run_dir: &Utf8Path,
    run_id: &str,
) -> Result<Option<engine::RunState>, ServiceError> {
    let Some((_, state)) = read_recovery_state(run_dir)? else {
        // The fork admission gate is the only supported operation here that
        // removes its journal while preserving the run/control directory.
        return match std::fs::symlink_metadata(run_meta_dir(run_dir).join("journal.jsonl")) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
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
    Ok(Some(state))
}

impl LocalService {
    pub(crate) fn merge_effect_candidates(&self, dirs: impl IntoIterator<Item = Utf8PathBuf>) {
        self.inner
            .unreconciled_effects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(dirs);
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
        for run_dir in dirs {
            match reconcile_effect_candidate(
                &run_dir,
                self.inner.deployment_policy.container_timeouts,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => retry.push(run_dir),
                Err(error) => {
                    tracing::error!(%run_dir, %error, "container effect discovery/reconciliation incomplete");
                    retry.push(run_dir);
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
            .iter()
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

    fn close_effect(dir: &Utf8Path) {
        JournalWriter::append_single_event(
            &run_meta_dir(dir).join("journal.jsonl"),
            dir.file_name().unwrap(),
            "effect_closed",
            json!({"effect": "fixture-effect", "outcome": "absent"}),
            JournalLimits::default(),
            None,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn colliding_admission_retains_terminal_effect_for_startup_and_shared_retry() {
        let root = temp_run_dir("effect-discovery-collision");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let peer = service(&runs); // Exists before the peer's terminal journal.
        let dir = runs.join("run-37");
        terminal_effect(&dir);
        // These distinct IDs share the FNV admission shard. A need not even
        // have a journal: only its unrelated preparation lease blocks B.
        let admission = try_lock_run_admission(&runs.join("run-0"))
            .unwrap()
            .unwrap();
        assert!(try_lock_run_admission(&dir).unwrap().is_none());
        let (records, candidates) = rehydrate_runs_with_effects(&runs, 8, 8).unwrap();
        assert!(records.is_empty());
        assert_eq!(candidates, std::slice::from_ref(&dir));
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
        drop(admission);
        let error =
            reconcile_effect_candidate(&dir, peer.inner.deployment_policy.container_timeouts)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("unreadable backend"), "{error}");
        // A failed cleanup remains queued, not just its durable effect.
        startup.reconcile_startup_effects().await;
        assert_eq!(pending(&startup), std::slice::from_ref(&dir));
        close_effect(&dir);
        peer.resume_recovered_runs().await;
        startup.reconcile_startup_effects().await;
        assert!(pending(&peer).is_empty());
        assert!(pending(&startup).is_empty());
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
            !reconcile_effect_candidate(&dir, service.inner.deployment_policy.container_timeouts)
                .await
                .unwrap(),
            "a held execution lease retains a valid candidate"
        );
        std::fs::write(&journal, b"not json\n").unwrap();
        assert!(
            reconcile_effect_candidate(&dir, service.inner.deployment_policy.container_timeouts)
                .await
                .is_err(),
            "malformed evidence is refused"
        );
        service.reconcile_startup_effects().await;
        assert_eq!(pending(&service), std::slice::from_ref(&dir));
        drop(lease);
        assert!(
            reconcile_effect_candidate(&dir, service.inner.deployment_policy.container_timeouts)
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
        let scan = refresh_runs_with_effects(&runs, 8, 8).unwrap();
        assert_eq!(scan.failures, 1);
        assert_eq!(scan.effects, std::slice::from_ref(&dir));
        service.refresh_shared_runs().await.unwrap();
        service.reconcile_startup_effects().await;
        assert_eq!(pending(&service), std::slice::from_ref(&dir));
        assert_eq!(std::fs::read(&journal).unwrap(), b"not json\n");
        std::fs::write(&journal, original).unwrap();
        close_effect(&dir);
        service.reconcile_startup_effects().await;
        assert!(pending(&service).is_empty());
        service.merge_effect_candidates([dir.clone()]);
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
        let error =
            reconcile_effect_candidate(&dir, service.inner.deployment_policy.container_timeouts)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("post-marker history"), "{error}");
        service.merge_effect_candidates([dir.clone()]);
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
