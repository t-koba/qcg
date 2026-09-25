//! Process-wide exclusion for read-modify-write file patch paths.
//!
//! Parallel `foreach` iterations, parallel-wave siblings, and concurrent
//! runs in this process serialize on the same file, so a base check and
//! its write stay atomic with respect to each other: the loser observes a
//! changed base and fails explicitly instead of silently overwriting.
//! Sharded static mutexes keep the table bounded (no per-path growth in a
//! long-lived server); unrelated files may share a shard and wait briefly.
//! Cross-process writers are outside this boundary: concurrent processes
//! still rely on base mismatch to fail closed.

use std::collections::BTreeSet;
use std::sync::LazyLock;

/// Number of lock shards. Power of two so the shard index is a bitmask.
const SHARD_COUNT: usize = 64;

/// One mutex per shard, shared process-wide.
static SHARDS: LazyLock<[std::sync::Arc<tokio::sync::Mutex<()>>; SHARD_COUNT]> =
    LazyLock::new(|| std::array::from_fn(|_| std::sync::Arc::new(tokio::sync::Mutex::new(()))));

/// Hash a resolved path to its shard index (FNV-1a, 64-bit).
fn shard_index(path: &str) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in path.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) & (SHARD_COUNT - 1)
}

/// Held guards keep the acquired shards locked. Drop order is reverse
/// acquisition order; shards are always acquired in ascending index order
/// so nested acquisitions across call sites cannot deadlock.
pub struct PatchPathsGuard {
    _guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

/// Lock every distinct shard covering `paths`, in ascending shard order.
/// Paths must already be resolved (post `resolve_write`/`resolve_read`)
/// so distinct spellings of one file share one shard.
pub async fn lock_patch_paths(paths: &[&camino::Utf8Path]) -> PatchPathsGuard {
    let shards: BTreeSet<usize> = paths
        .iter()
        .map(|path| shard_index(path.as_str()))
        .collect();
    let mut guards = Vec::with_capacity(shards.len());
    for index in shards {
        guards.push(SHARDS[index].clone().lock_owned().await);
    }
    PatchPathsGuard { _guards: guards }
}
