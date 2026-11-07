//! In-memory idempotency configuration: runtime-resolved limits, the
//! entry table, and its pruning. Durable protocol lives in [`durable`],
//! owner cleanup in [`guard`], request orchestration in the parent module.

use super::durable::DurableIdempotencyRecord;
use policy::{IDEMPOTENCY_MAX_ENTRIES, IDEMPOTENCY_TTL};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Frozen positive-integer deployment settings; unset alone selects the
/// default. Invalid or non-Unicode values refuse startup, never fall back.
fn positive_env<T: std::str::FromStr + PartialOrd + Default>(
    name: &str,
    default: T,
) -> Result<T, String> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("invalid {name}: must be Unicode")),
        Ok(value) => value
            .parse::<T>()
            .ok()
            .filter(|v| *v > T::default())
            .ok_or_else(|| format!("invalid {name}: must be a positive integer")),
    }
}

pub(crate) fn effective_idempotency_max_entries() -> Result<usize, String> {
    positive_env("IDEMPOTENCY_MAX_ENTRIES", IDEMPOTENCY_MAX_ENTRIES)
}

pub(crate) fn effective_idempotency_ttl() -> Result<std::time::Duration, String> {
    positive_env("IDEMPOTENCY_TTL_SECS", IDEMPOTENCY_TTL.as_secs())
        .map(std::time::Duration::from_secs)
}

pub(crate) fn effective_idempotency_sweep_interval() -> Result<std::time::Duration, String> {
    positive_env(
        "IDEMPOTENCY_SWEEP_INTERVAL_SECS",
        policy::IDEMPOTENCY_SWEEP_INTERVAL.as_secs(),
    )
    .map(std::time::Duration::from_secs)
}

pub(crate) fn effective_idempotency_sweep_max_entries() -> Result<usize, String> {
    positive_env(
        "IDEMPOTENCY_SWEEP_MAX_ENTRIES",
        policy::IDEMPOTENCY_SWEEP_MAX_ENTRIES,
    )
}

/// Boot-frozen adoption and peer contention policy, independent of Ready TTL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdempotencyTiming {
    pub pending_lease_secs: u64,
    pub peer_wait: Duration,
    pub peer_poll: Duration,
}
impl IdempotencyTiming {
    pub(crate) const DEFAULT: Self = Self {
        pending_lease_secs: 60,
        peer_wait: Duration::from_secs(30),
        peer_poll: Duration::from_millis(100),
    };
    pub(crate) fn poll_delay(self, elapsed: Duration) -> Duration {
        self.peer_poll.min(self.peer_wait.saturating_sub(elapsed))
    }
    pub(crate) fn resolve() -> Result<Self, String> {
        Ok(Self {
            pending_lease_secs: positive_env(
                "IDEMPOTENCY_PENDING_LEASE_SECS",
                Self::DEFAULT.pending_lease_secs,
            )?,
            peer_wait: Duration::from_secs(positive_env(
                "IDEMPOTENCY_PEER_WAIT_SECS",
                Self::DEFAULT.peer_wait.as_secs(),
            )?),
            peer_poll: Duration::from_millis(positive_env(
                "IDEMPOTENCY_PEER_POLL_MS",
                Self::DEFAULT.peer_poll.as_millis() as u64,
            )?),
        })
    }
}

#[derive(Debug)]
pub(crate) enum IdempotencyEntry {
    Pending {
        digest: String,
        owner_id: uuid::Uuid,
        created_at: Instant,
        completed: tokio::sync::watch::Sender<bool>,
    },
    // Persisted time governs TTL; cached_at only orders capacity eviction.
    Ready {
        record: DurableIdempotencyRecord,
        cached_at: Instant,
    },
}

impl IdempotencyEntry {
    pub(crate) fn ready(record: DurableIdempotencyRecord) -> Self {
        Self::Ready {
            record,
            cached_at: Instant::now(),
        }
    }
}

/// Entries are judged by the frozen boot `ttl` and `max_entries` passed by
/// the caller, never by a live environment read, so requests cannot drift
/// from boot validation (E04). TTL expiry removes both Pending and Ready;
/// This is memory bookkeeping, never authority to replace a durable owner:
/// claim/release/commit still require that key's cross-process execution gate.
/// Capacity pressure evicts only Ready: evicting a Pending would orphan live
/// waiters, and Pending count is already bounded by concurrent requests with
/// a TTL backstop (E04).
pub(crate) fn prune_idempotency(
    entries: &mut BTreeMap<String, IdempotencyEntry>,
    now: Instant,
    now_unix: u64,
    ttl: std::time::Duration,
    max_entries: usize,
) {
    entries.retain(|_, entry| match entry {
        IdempotencyEntry::Pending { created_at, .. } => now.duration_since(*created_at) < ttl,
        IdempotencyEntry::Ready { record, .. } => !record.expired_at(now_unix, ttl),
    });
    while entries.len() >= max_entries {
        let Some(oldest) = entries
            .iter()
            .filter_map(|(key, entry)| match entry {
                IdempotencyEntry::Ready { cached_at, .. } => Some((key, *cached_at)),
                IdempotencyEntry::Pending { .. } => None,
            })
            .min_by_key(|(_, created_at)| *created_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        entries.remove(&oldest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn peer_poll_delay_respects_interval_and_remaining_wait() {
        let timing = IdempotencyTiming {
            peer_wait: Duration::from_secs(7),
            peer_poll: Duration::from_millis(13),
            ..IdempotencyTiming::DEFAULT
        };
        assert_eq!(timing.poll_delay(Duration::ZERO), Duration::from_millis(13));
        assert_eq!(
            timing.poll_delay(Duration::from_millis(6999)),
            Duration::from_millis(1)
        );
        assert_eq!(timing.poll_delay(Duration::from_secs(8)), Duration::ZERO);
        // Accepted u64 durations do not require an overflowing Instant addition.
        let huge = IdempotencyTiming {
            peer_wait: Duration::from_secs(u64::MAX),
            ..timing
        };
        assert_eq!(huge.poll_delay(Duration::ZERO), timing.peer_poll);
    }

    #[test]
    fn capacity_still_evicts_the_oldest_cache_insertion_not_pending() {
        let now = Instant::now();
        let ttl = policy::IDEMPOTENCY_TTL;
        let (completed, _) = tokio::sync::watch::channel(false);
        let record = |key: &str, created| DurableIdempotencyRecord {
            publication_id: None,
            key: key.into(),
            digest: "digest".into(),
            run_id: "run".into(),
            created_at_unix: created,
            generation: 1,
        };
        let mut entries = BTreeMap::from([
            (
                "first".into(),
                IdempotencyEntry::Ready {
                    record: record("first", 200),
                    cached_at: now - std::time::Duration::from_secs(2),
                },
            ),
            (
                "second".into(),
                IdempotencyEntry::Ready {
                    record: record("second", 100),
                    cached_at: now - std::time::Duration::from_secs(1),
                },
            ),
            (
                "pending".into(),
                IdempotencyEntry::Pending {
                    digest: "digest".into(),
                    owner_id: uuid::Uuid::now_v7(),
                    created_at: now,
                    completed,
                },
            ),
        ]);
        prune_idempotency(&mut entries, now, 300, ttl, 3);
        assert!(!entries.contains_key("first"));
        assert!(entries.contains_key("second"));
        assert!(entries.contains_key("pending"));
    }

    #[test]
    fn invalid_idempotency_policy_names_its_variable() {
        if crate::tests::isolate_environment_test(
            "server::idempotency::config::tests::invalid_idempotency_policy_names_its_variable",
        ) {
            return;
        }
        // E04: unknown, zero, or non-numeric deployment knobs refuse at
        // startup instead of degrading silently; each error names the
        // offending variable.
        let _guard = ENV_GUARD.lock().expect("env lock");
        type Setting = (&'static str, fn() -> Result<u64, String>);
        let settings: [Setting; 7] = [
            ("IDEMPOTENCY_TTL_SECS", || {
                effective_idempotency_ttl().map(|value| value.as_secs())
            }),
            ("IDEMPOTENCY_MAX_ENTRIES", || {
                effective_idempotency_max_entries().map(|value| value as u64)
            }),
            ("IDEMPOTENCY_SWEEP_INTERVAL_SECS", || {
                effective_idempotency_sweep_interval().map(|value| value.as_secs())
            }),
            ("IDEMPOTENCY_SWEEP_MAX_ENTRIES", || {
                effective_idempotency_sweep_max_entries().map(|value| value as u64)
            }),
            ("IDEMPOTENCY_PENDING_LEASE_SECS", || {
                IdempotencyTiming::resolve().map(|value| value.pending_lease_secs)
            }),
            ("IDEMPOTENCY_PEER_WAIT_SECS", || {
                IdempotencyTiming::resolve().map(|value| value.peer_wait.as_secs())
            }),
            ("IDEMPOTENCY_PEER_POLL_MS", || {
                IdempotencyTiming::resolve().map(|value| value.peer_poll.as_millis() as u64)
            }),
        ];
        for (variable, resolve) in settings {
            // SAFETY: this test is isolated in its own process, with the guard held.
            unsafe {
                std::env::remove_var(variable);
            }
            resolve().expect("unset selects default");
            for value in ["0", "-1", "", "many", "18446744073709551616"] {
                unsafe {
                    std::env::set_var(variable, value);
                }
                assert!(resolve().unwrap_err().contains(variable));
            }
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStringExt;
                unsafe {
                    std::env::set_var(variable, std::ffi::OsString::from_vec(vec![0xff]));
                }
                assert!(
                    resolve().unwrap_err().contains(variable),
                    "non-Unicode is not unset"
                );
            }
            unsafe {
                std::env::set_var(variable, "7");
            }
            assert_eq!(resolve().expect("positive override accepted"), 7);
            unsafe {
                std::env::remove_var(variable);
            }
        }
        assert_eq!(
            IdempotencyTiming::resolve().unwrap(),
            IdempotencyTiming::DEFAULT
        );
        assert_eq!(
            effective_idempotency_sweep_interval().unwrap(),
            policy::IDEMPOTENCY_SWEEP_INTERVAL
        );
        assert_eq!(
            effective_idempotency_sweep_max_entries().unwrap(),
            policy::IDEMPOTENCY_SWEEP_MAX_ENTRIES
        );
    }
}
