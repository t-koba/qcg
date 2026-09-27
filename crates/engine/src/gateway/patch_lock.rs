//! Process-wide exclusion for every workspace file mutation (G02).
//!
//! Parallel `foreach` iterations, parallel-wave siblings, and concurrent
//! runs in this process serialize on the same file, so a patch base check
//! and its commit stay atomic with respect to each other AND with respect
//! to unconditional writers: the loser observes a changed base and fails
//! explicitly instead of silently overwriting.
//! Sharded static mutexes keep the table bounded (no per-path growth in a
//! long-lived server); unrelated files may share a shard and wait briefly.
//! Cross-process writers are outside this boundary: concurrent processes
//! still rely on base mismatch to fail closed.
//!
//! Every mutating gateway path participates: `apply_anchored_patch`,
//! `write_file_atomic_with_mode` (+ stream), and `remove_file_resolved`
//! all hold this lock for their target. Patch bodies that already hold the
//! guard must use the `_under_guard` write variants, which skip
//! re-acquisition: `tokio::sync::Mutex` is not reentrant, so re-locking
//! the same shard from the holder would self-deadlock (G02-04).

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
