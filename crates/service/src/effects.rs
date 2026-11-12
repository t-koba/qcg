//! Reclaims container effects orphaned by a dead run, by journaled name.

use camino::Utf8Path;
use engine::{JournalLimits, JournalWriter};
use serde_json::json;

use crate::summaries::{fold_run_state, run_meta_dir};
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
    let journal = run_meta_dir(run_dir).join("journal.jsonl");
    let mut unresolved = Vec::new();
    // An unreadable journal is refused by the caller's own journal check;
    // it has no effects this pass could trust.
    let Ok(state) = fold_run_state(run_dir) else {
        return Ok(());
    };
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

impl LocalService {
    /// Startup recovery, before new runs are admitted: attempts every run
    /// directory the construction scan found with an open effect. A run whose
    /// execution lease a live process holds is skipped; a failure is logged
    /// and leaves the intent for the next recovery or run re-entry.
    pub(crate) async fn reconcile_startup_effects(&self) {
        let dirs = std::mem::take(
            &mut *self
                .inner
                .unreconciled_effects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for run_dir in dirs {
            let Some(run_id) = run_dir.file_name() else {
                continue;
            };
            match crate::run_dirs::try_lock_run_execution(&run_dir) {
                Ok(Some(_lease)) => {
                    if let Err(error) = reconcile_run_effects(
                        &run_dir,
                        run_id,
                        self.inner.deployment_policy.container_timeouts,
                    )
                    .await
                    {
                        tracing::error!(%run_id, %error, "container effect reconciliation incomplete");
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(%run_id, %error, "effect reconciliation could not take the run lease")
                }
            }
        }
    }
}
