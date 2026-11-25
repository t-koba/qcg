//! Container effect intent is journaled through an [`EffectLedger`] before
//! any create call, using a fresh unguessable name. A name match cannot prove
//! ownership: the name may have been reused, and a missing entry does not
//! fence a create still completing after the creator died. Recovery therefore
//! never deletes or settles an effect by name; it can only observe whether an
//! instance currently carries the name. Live teardown of an owned session is
//! unchanged.

use std::sync::Arc;

use super::backend::Backend;
use super::error::ContainerError;
use super::plans;
use super::session::{Session, run_admin};

/// Durable record of effect intent and closure, backed by the run journal.
pub trait EffectLedger: std::fmt::Debug + Send + Sync {
    /// Owner labels attached to every instance created under this ledger.
    fn labels(&self, name: &str) -> Vec<(String, String)>;
    /// Records the intent durably. Must succeed before the create call.
    fn open(&self, backend: &Backend, name: &str) -> std::io::Result<()>;
    /// Records that the instance no longer exists (`outcome` says how).
    fn close(&self, name: &str, outcome: &str) -> std::io::Result<()>;
}

/// What one bounded presence probe observed for a journaled name. Neither
/// observation settles the intent: a name match cannot prove ownership, and a
/// missing entry does not fence a late create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reclaimed {
    /// An instance carrying the journaled name was observed.
    NamePresent,
    /// No instance with that name was observed in this probe.
    NameNotObserved,
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

/// Probes whether an instance currently carries the journaled name. Only
/// the exact name is ever addressed, never a prefix or label. This is an
/// observation, not cleanup: it never tears down by name and never certifies
/// that the external effect stopped, so neither result settles the intent.
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
    let listing = run_admin(
        &plans::presence_argv(backend, name),
        timeouts.admin(),
        &tokio_util::sync::CancellationToken::new(),
        name,
        "inspect",
    )
    .await?;
    Ok(if listing.stdout.lines().any(|line| line.trim() == name) {
        Reclaimed::NamePresent
    } else {
        Reclaimed::NameNotObserved
    })
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
    async fn reclaim_observes_the_exact_journaled_name_without_deleting_anything() {
        let Some(daemon) = Daemon::isolated(&format!(
            "{TEST}reclaim_observes_the_exact_journaled_name_without_deleting_anything"
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
                Reclaimed::NamePresent
            );
            assert_eq!(
                reclaim(&backend, &ours, timeouts()).await.unwrap(),
                Reclaimed::NamePresent
            );
            assert_eq!(
                daemon.instances(),
                [ours.as_str(), other.as_str(), similar.as_str(), "prod-db"]
            );
        }
        let log = daemon.log();
        let probes = log
            .lines()
            .filter(|line| line.contains(" list ") || line.contains(" ps "))
            .count();
        assert_eq!(probes, 4, "one probe per backend pass: {log}");
        assert!(
            !log.contains(" kill ")
                && !log.contains(" stop ")
                && !log.contains(" rm ")
                && !log.contains(" delete "),
            "a name match cannot prove ownership: {log}"
        );
    }

    #[tokio::test]
    async fn reclaim_never_certifies_absence_or_fences_a_late_create() {
        let Some(daemon) = Daemon::isolated(&format!(
            "{TEST}reclaim_never_certifies_absence_or_fences_a_late_create"
        )) else {
            return;
        };
        let backend = &backends()[1];
        let late = plans::effect_name().unwrap();
        // A create completing around the probe is merely observed; the
        // intent it belongs to is still not settled by this observation.
        daemon.create_late(&late);
        daemon.set_instances(&[&late]);
        assert_eq!(
            reclaim(backend, &late, timeouts()).await.unwrap(),
            Reclaimed::NamePresent
        );
        assert_eq!(daemon.instances(), [late.as_str()]);

        let absent = plans::effect_name().unwrap();
        daemon.mode("ok");
        assert_eq!(
            reclaim(backend, &absent, timeouts()).await.unwrap(),
            Reclaimed::NameNotObserved
        );
        let probes = daemon
            .log()
            .lines()
            .filter(|line| line.contains(" list ") || line.contains(" ps "))
            .count();
        assert_eq!(probes, 1, "one bounded probe; absence is never certified");
        assert_eq!(
            daemon.instances(),
            [late.as_str()],
            "a missing entry does not fence or remove a late create"
        );
        assert!(!daemon.log().contains(" delete "));
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
