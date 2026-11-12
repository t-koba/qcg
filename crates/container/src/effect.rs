//! Ownership of container effects by unique identity. Before any create call
//! the intent (backend plus a fresh unguessable name) is journaled through an
//! [`EffectLedger`]; the effect is created with exactly that name. Because
//! the name is unique, an instance carrying it is ours, so a later pass can
//! find and remove it by name alone, whether or not the creator survived.

use std::sync::Arc;

use super::backend::Backend;
use super::error::ContainerError;
use super::plans;
use super::session::{Session, run_admin, teardown};

/// Durable record of effect intent and closure, backed by the run journal.
pub trait EffectLedger: std::fmt::Debug + Send + Sync {
    /// Owner labels attached to every instance created under this ledger.
    fn labels(&self, name: &str) -> Vec<(String, String)>;
    /// Records the intent durably. Must succeed before the create call.
    fn open(&self, backend: &Backend, name: &str) -> std::io::Result<()>;
    /// Records that the instance no longer exists (`outcome` says how).
    fn close(&self, name: &str, outcome: &str) -> std::io::Result<()>;
}

/// What reclaiming one journaled name found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reclaimed {
    /// The instance existed and was stopped and deleted.
    Removed,
    /// No instance carried the name, now and after the settle interval.
    Absent,
}

/// Journals the intent and returns the session that owns the new name.
pub fn begin(
    backend: &Backend,
    timeouts: policy::ContainerTimeouts,
    ledger: Option<Arc<dyn EffectLedger>>,
) -> Result<Session, ContainerError> {
    let name = plans::effect_name()?;
    if let Some(ledger) = &ledger {
        ledger.open(backend, &name)?;
    }
    Ok(Session {
        timeouts,
        backend: backend.clone(),
        name,
        ledger,
    })
}

/// Removes the instance journaled under `name`, if any. Only the exact name
/// is ever addressed, never a prefix or label. Absence is re-checked once
/// after the policy settle interval, because a create submitted before a
/// crash may still be completing in the daemon.
pub async fn reclaim(
    backend: &Backend,
    name: &str,
    timeouts: policy::ContainerTimeouts,
) -> Result<Reclaimed, ContainerError> {
    if !backend.is_declared() || !plans::is_effect_name(name) {
        return Err(ContainerError::StageFailed {
            stage: "reclaim",
            instance: name.to_string(),
            detail: "journaled effect is not a qcg-created instance; refusing".into(),
        });
    }
    let session = Session {
        timeouts,
        backend: backend.clone(),
        name: name.to_string(),
        ledger: None,
    };
    for attempt in 0..2 {
        if attempt > 0 {
            tokio::time::sleep(timeouts.settle()).await;
        }
        let listing = run_admin(
            &plans::presence_argv(backend, name),
            timeouts.admin(),
            &tokio_util::sync::CancellationToken::new(),
            name,
            "inspect",
        )
        .await?;
        if listing.stdout.lines().any(|line| line.trim() == name) {
            teardown(&session).await?;
            return Ok(Reclaimed::Removed);
        }
    }
    Ok(Reclaimed::Absent)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::session::daemon_policy_test_support::Daemon;
    use std::sync::Mutex;

    const TEST: &str = "effect::tests::";

    #[derive(Debug, Default)]
    struct Recorder(Mutex<Vec<String>>);

    impl EffectLedger for Recorder {
        fn labels(&self, name: &str) -> Vec<(String, String)> {
            vec![("qcg.effect".into(), name.into())]
        }
        fn open(&self, _: &Backend, name: &str) -> std::io::Result<()> {
            self.0.lock().unwrap().push(format!("open {name}"));
            Ok(())
        }
        fn close(&self, name: &str, outcome: &str) -> std::io::Result<()> {
            self.0
                .lock()
                .unwrap()
                .push(format!("close {name} {outcome}"));
            Ok(())
        }
    }

    fn backends() -> [Backend; 2] {
        [
            Backend::Docker {
                binary: "docker".into(),
                runtime_flag: None,
            },
            Backend::Incus {
                binary: "incus".into(),
            },
        ]
    }

    fn timeouts() -> policy::ContainerTimeouts {
        policy::ContainerTimeouts::from_seconds([1; 5]).unwrap()
    }

    #[tokio::test]
    async fn reclaim_removes_only_the_exact_journaled_name() {
        let Some(daemon) = Daemon::isolated(&format!(
            "{TEST}reclaim_removes_only_the_exact_journaled_name"
        )) else {
            return;
        };
        let ours = plans::effect_name().unwrap();
        let other = plans::effect_name().unwrap();
        let similar = format!("{ours}x");
        for backend in backends() {
            daemon.set_instances(&[&ours, &other, &similar, "prod-db"]);
            assert_eq!(
                reclaim(&backend, &ours, timeouts()).await.unwrap(),
                Reclaimed::Removed
            );
            assert_eq!(
                daemon.instances(),
                [other.as_str(), similar.as_str(), "prod-db"]
            );
            // Idempotent: a second pass finds nothing and touches nothing.
            assert_eq!(
                reclaim(&backend, &ours, timeouts()).await.unwrap(),
                Reclaimed::Absent
            );
            assert_eq!(daemon.instances().len(), 3);
        }
        let touched: Vec<String> = daemon
            .log()
            .lines()
            .filter(|line| {
                line.contains(" kill ")
                    || line.contains(" stop ")
                    || line.contains(" rm ")
                    || line.contains(" delete ")
            })
            .map(String::from)
            .collect();
        assert!(
            touched.iter().all(|line| line.ends_with(&ours)),
            "{touched:?}"
        );
    }

    #[tokio::test]
    async fn reclaim_rechecks_an_absent_name_once_for_a_late_create() {
        let Some(daemon) = Daemon::isolated(&format!(
            "{TEST}reclaim_rechecks_an_absent_name_once_for_a_late_create"
        )) else {
            return;
        };
        let name = plans::effect_name().unwrap();
        let backend = &backends()[1];
        assert_eq!(
            reclaim(backend, &name, timeouts()).await.unwrap(),
            Reclaimed::Absent
        );
        let probes = daemon
            .log()
            .lines()
            .filter(|line| line.contains(" list "))
            .count();
        assert_eq!(probes, 2, "absence is confirmed by exactly one re-check");
        daemon.mode("late");
        daemon.create_late(&name);
        assert_eq!(
            reclaim(backend, &name, timeouts()).await.unwrap(),
            Reclaimed::Removed
        );
        assert!(daemon.instances().is_empty());
    }

    #[tokio::test]
    async fn reclaim_reports_an_unreachable_backend_without_claiming_absence() {
        let Some(daemon) = Daemon::isolated(&format!(
            "{TEST}reclaim_reports_an_unreachable_backend_without_claiming_absence"
        )) else {
            return;
        };
        let name = plans::effect_name().unwrap();
        daemon.set_instances(&[&name]);
        daemon.mode("broken");
        for backend in backends() {
            assert!(reclaim(&backend, &name, timeouts()).await.is_err());
        }
        assert_eq!(daemon.instances(), [name.as_str()]);
    }

    #[tokio::test]
    async fn reclaim_refuses_names_and_backends_we_would_never_create() {
        for name in ["prod-db", "qcg-", "ctr0123456789abcdef0123456789abcdef"] {
            let error = reclaim(&backends()[0], name, timeouts()).await.unwrap_err();
            assert!(error.to_string().contains("refusing"), "{error}");
        }
        let foreign = Backend::Incus {
            binary: "/tmp/not-a-client".into(),
        };
        let name = plans::effect_name().unwrap();
        assert!(reclaim(&foreign, &name, timeouts()).await.is_err());
    }

    #[tokio::test]
    async fn begin_journals_before_creation_and_teardown_closes_the_intent() {
        let Some(daemon) = Daemon::isolated(&format!(
            "{TEST}begin_journals_before_creation_and_teardown_closes_the_intent"
        )) else {
            return;
        };
        let recorder = Arc::new(Recorder::default());
        let ledger: Arc<dyn EffectLedger> = recorder.clone();
        let session = begin(&backends()[0], timeouts(), Some(ledger)).unwrap();
        assert!(plans::is_effect_name(&session.name));
        assert_eq!(
            session.labels(),
            [("qcg.effect".to_string(), session.name.clone())]
        );
        daemon.set_instances(&[&session.name]);
        assert_eq!(
            *recorder.0.lock().unwrap(),
            [format!("open {}", session.name)]
        );
        crate::session::teardown(&session).await.unwrap();
        assert!(daemon.instances().is_empty());
        assert_eq!(
            *recorder.0.lock().unwrap(),
            [
                format!("open {0}", session.name),
                format!("close {0} released", session.name)
            ]
        );
    }

    #[test]
    fn a_ledger_that_cannot_journal_blocks_creation() {
        #[derive(Debug)]
        struct Failing;
        impl EffectLedger for Failing {
            fn labels(&self, _: &str) -> Vec<(String, String)> {
                Vec::new()
            }
            fn open(&self, _: &Backend, _: &str) -> std::io::Result<()> {
                Err(std::io::Error::other("disk full"))
            }
            fn close(&self, _: &str, _: &str) -> std::io::Result<()> {
                Ok(())
            }
        }
        let ledger: Arc<dyn EffectLedger> = Arc::new(Failing);
        assert!(begin(&backends()[0], timeouts(), Some(ledger)).is_err());
    }
}
