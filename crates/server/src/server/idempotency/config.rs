//! In-memory idempotency configuration: runtime-resolved limits, the
//! entry table, and its pruning. Durable protocol lives in [`durable`],
//! owner cleanup in [`guard`], request orchestration in the parent module.

use super::durable::DurableIdempotencyRecord;
use anyhow::Result;
use policy::{IDEMPOTENCY_MAX_ENTRIES, IDEMPOTENCY_TTL};
use std::collections::BTreeMap;
use std::time::Instant;

/// Effective in-memory entry cap resolved once at boot:
/// `IDEMPOTENCY_MAX_ENTRIES` overrides the compiled default (3.2).
/// There is exactly one standard: an invalid value refuses boot, never a
/// silent fallback. Frozen into AppState like the TTL above (E04).
pub(crate) fn effective_idempotency_max_entries() -> Result<usize, String> {
    match std::env::var("IDEMPOTENCY_MAX_ENTRIES") {
        Err(_) => Ok(IDEMPOTENCY_MAX_ENTRIES),
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|entries| *entries > 0)
            .ok_or_else(|| {
                format!("invalid IDEMPOTENCY_MAX_ENTRIES `{value}`: must be a positive integer")
            }),
    }
}

/// Effective idempotency TTL resolved once at boot: `IDEMPOTENCY_TTL_SECS`
/// overrides the compiled default so operators tune retention without a
/// rebuild (3.2). There is exactly one standard: an invalid value refuses
/// boot, never a silent fallback. The resolved value freezes into AppState
/// and every request and prune path uses the frozen copy, never re-reads
/// the environment, so boot validation and request handling cannot drift
/// (E04).
pub(crate) fn effective_idempotency_ttl() -> Result<std::time::Duration, String> {
    match std::env::var("IDEMPOTENCY_TTL_SECS") {
        Err(_) => Ok(IDEMPOTENCY_TTL),
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|secs| *secs > 0)
            .map(std::time::Duration::from_secs)
            .ok_or_else(|| {
                format!("invalid IDEMPOTENCY_TTL_SECS `{value}`: must be a positive integer")
            }),
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
/// capacity pressure evicts only Ready: evicting a Pending would orphan live
/// waiters, and Pending count is already bounded by concurrent requests with
/// a TTL backstop (E04).
pub(crate) fn prune_idempotency(
    entries: &mut BTreeMap<String, IdempotencyEntry>,
    now: Instant,
    now_unix: u64,
    ttl: std::time::Duration,
    max_entries: usize,
) -> Result<(), String> {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn capacity_still_evicts_the_oldest_cache_insertion_not_pending() {
        let now = Instant::now();
        let ttl = policy::IDEMPOTENCY_TTL;
        let (completed, _) = tokio::sync::watch::channel(false);
        let record = |key: &str, created| DurableIdempotencyRecord {
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
        prune_idempotency(&mut entries, now, 300, ttl, 3).expect("capacity prune");
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
        for (variable, value) in [
            ("IDEMPOTENCY_TTL_SECS", "0"),
            ("IDEMPOTENCY_TTL_SECS", "soon"),
            ("IDEMPOTENCY_MAX_ENTRIES", "0"),
            ("IDEMPOTENCY_MAX_ENTRIES", "many"),
        ] {
            // SAFETY: the guard serializes environment mutation across
            // tests in this module.
            unsafe {
                std::env::set_var(variable, value);
            }
            let error: String = if variable == "IDEMPOTENCY_TTL_SECS" {
                effective_idempotency_ttl()
                    .map(|_| ())
                    .expect_err("invalid ttl must fail")
            } else {
                effective_idempotency_max_entries()
                    .map(drop)
                    .expect_err("invalid max entries must fail")
            };
            assert!(
                error.contains(variable),
                "policy errors must name {variable}"
            );
            // SAFETY: still holding the guard.
            unsafe {
                std::env::remove_var(variable);
            }
        }
        assert!(effective_idempotency_ttl().is_ok());
        assert!(effective_idempotency_max_entries().is_ok());
    }
}
