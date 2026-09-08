//! In-memory idempotency configuration: runtime-resolved limits, the
//! entry table, and its pruning. Durable protocol lives in [`durable`],
//! owner cleanup in [`guard`], request orchestration in the parent module.

use anyhow::Result;
use qcg_policy::{IDEMPOTENCY_MAX_ENTRIES, IDEMPOTENCY_TTL};
use std::collections::BTreeMap;
use std::time::Instant;

/// Effective in-memory entry cap: `QCG_IDEMPOTENCY_MAX_ENTRIES` overrides
/// the compiled default (3.2). There is exactly one standard: an invalid
/// value is an error at boot and at request time, never a silent fallback.
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

/// Effective idempotency TTL resolved at runtime: `QCG_IDEMPOTENCY_TTL_SECS`
/// overrides the compiled default so operators tune retention without a
/// rebuild (3.2). There is exactly one standard: an invalid value is an
/// error at boot and at request time, never a silent fallback.
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

pub(crate) fn prune_idempotency(
    entries: &mut BTreeMap<String, IdempotencyEntry>,
    now: Instant,
) -> Result<(), String> {
    let ttl = effective_idempotency_ttl()?;
    entries.retain(|_, entry| {
        let created_at = match entry {
            IdempotencyEntry::Pending { created_at, .. }
            | IdempotencyEntry::Ready { created_at, .. } => created_at,
        };
        now.duration_since(*created_at) < ttl
    });
    while entries.len() >= effective_idempotency_max_entries()? {
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
