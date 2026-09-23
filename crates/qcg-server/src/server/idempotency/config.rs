//! In-memory idempotency configuration: runtime-resolved limits, the
//! entry table, and its pruning. Durable protocol lives in [`durable`],
//! owner cleanup in [`guard`], request orchestration in the parent module.

use anyhow::Result;
use qcg_policy::{IDEMPOTENCY_MAX_ENTRIES, IDEMPOTENCY_TTL};
use std::collections::BTreeMap;
use std::time::Instant;

/// Effective in-memory entry cap resolved once at boot:
/// `QCG_IDEMPOTENCY_MAX_ENTRIES` overrides the compiled default (3.2).
/// There is exactly one standard: an invalid value refuses boot, never a
/// silent fallback. Frozen into AppState like the TTL above (E04).
pub(crate) fn effective_idempotency_max_entries() -> Result<usize, String> {
    match std::env::var("QCG_IDEMPOTENCY_MAX_ENTRIES") {
        Err(_) => Ok(IDEMPOTENCY_MAX_ENTRIES),
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|entries| *entries > 0)
            .ok_or_else(|| {
                format!("invalid QCG_IDEMPOTENCY_MAX_ENTRIES `{value}`: must be a positive integer")
            }),
    }
}

/// Effective idempotency TTL resolved once at boot: `QCG_IDEMPOTENCY_TTL_SECS`
/// overrides the compiled default so operators tune retention without a
/// rebuild (3.2). There is exactly one standard: an invalid value refuses
/// boot, never a silent fallback. The resolved value freezes into AppState
/// and every request and prune path uses the frozen copy, never re-reads
/// the environment, so boot validation and request handling cannot drift
/// (E04).
pub(crate) fn effective_idempotency_ttl() -> Result<std::time::Duration, String> {
    match std::env::var("QCG_IDEMPOTENCY_TTL_SECS") {
        Err(_) => Ok(IDEMPOTENCY_TTL),
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|secs| *secs > 0)
            .map(std::time::Duration::from_secs)
            .ok_or_else(|| {
                format!("invalid QCG_IDEMPOTENCY_TTL_SECS `{value}`: must be a positive integer")
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
    Ready {
        digest: String,
        created_at: Instant,
        run_id: String,
    },
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
    ttl: std::time::Duration,
    max_entries: usize,
) -> Result<(), String> {
    entries.retain(|_, entry| {
        let created_at = match entry {
            IdempotencyEntry::Pending { created_at, .. }
            | IdempotencyEntry::Ready { created_at, .. } => created_at,
        };
        now.duration_since(*created_at) < ttl
    });
    while entries.len() >= max_entries {
        let Some(oldest) = entries
            .iter()
            .filter_map(|(key, entry)| match entry {
                IdempotencyEntry::Ready { created_at, .. } => Some((key, *created_at)),
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
    fn invalid_idempotency_policy_names_its_variable() {
        // E04: unknown, zero, or non-numeric deployment knobs refuse at
        // startup instead of degrading silently; each error names the
        // offending variable.
        let _guard = ENV_GUARD.lock().expect("env lock");
        for (variable, value) in [
            ("QCG_IDEMPOTENCY_TTL_SECS", "0"),
            ("QCG_IDEMPOTENCY_TTL_SECS", "soon"),
            ("QCG_IDEMPOTENCY_MAX_ENTRIES", "0"),
            ("QCG_IDEMPOTENCY_MAX_ENTRIES", "many"),
        ] {
            // SAFETY: the guard serializes environment mutation across
            // tests in this module.
            unsafe {
                std::env::set_var(variable, value);
            }
            let error: String = if variable == "QCG_IDEMPOTENCY_TTL_SECS" {
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
