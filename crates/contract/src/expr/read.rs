use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};

/// Canonical node path, including foreach/repair namespaces. Detached reads
/// remain explicitly unscoped and cannot establish a complete dependency set.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReadOwner {
    #[default]
    Unscoped,
    Node(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReadKind {
    Lookup,
    Collection,
}

/// SHA-256 of the canonical JSON encoding, not a value retained in the trace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReadIdentity {
    /// The path resolves to a collection used only to reach a child. Its
    /// contents are not dependencies until that collection is enumerated or
    /// otherwise consumed as a whole.
    Present,
    Digest([u8; 32]),
    Missing,
    Unsupported,
}

impl ReadIdentity {
    pub fn of(value: Option<&serde_json::Value>) -> Self {
        struct HashWriter(Sha256);
        impl Write for HashWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let Some(value) = value else {
            return Self::Missing;
        };
        let mut writer = HashWriter(Sha256::new());
        // serde_json maps in this workspace are sorted (no preserve_order).
        if serde_json::to_writer(&mut writer, value).is_err() {
            return Self::Unsupported;
        }
        Self::Digest(writer.0.finalize().into())
    }
}

/// Paths are segments, never dotted keys. Collection reads cover the subtree.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReadDependency {
    pub owner: ReadOwner,
    pub path: Vec<String>,
    pub kind: ReadKind,
    pub identity: ReadIdentity,
}

/// Ephemeral and deliberately not serializable. Global loss (overflow/poison)
/// invalidates every owner; unsupported operations invalidate their owner.
#[derive(Clone)]
pub struct ReadTrace {
    pub reads: BTreeSet<ReadDependency>,
    pub owners: BTreeSet<ReadOwner>,
    pub incomplete_owners: BTreeSet<ReadOwner>,
    pub incomplete: bool,
    pub overflow: bool,
    pub unsupported: bool,
    pub poisoned: bool,
    /// Charged owned metadata; allocator/B-tree overhead is additional O(limit).
    pub retained_bytes: usize,
    pub byte_limit: usize,
}

impl std::fmt::Debug for ReadDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadDependency")
            .field("owner", &self.owner)
            .field("path_depth", &self.path.len())
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ReadTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadTrace")
            .field("read_count", &self.reads.len())
            .field("owner_count", &self.owners.len())
            .field("incomplete_owner_count", &self.incomplete_owners.len())
            .field("incomplete", &self.incomplete)
            .field("overflow", &self.overflow)
            .field("unsupported", &self.unsupported)
            .field("poisoned", &self.poisoned)
            .field("retained_bytes", &self.retained_bytes)
            .field("byte_limit", &self.byte_limit)
            .finish()
    }
}

impl std::fmt::Debug for ReadRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadRecorder")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl ReadTrace {
    pub fn complete_for(&self, owner: &ReadOwner) -> bool {
        self.owners.contains(owner)
            && !matches!(owner, ReadOwner::Unscoped)
            && !self.overflow
            && !self.poisoned
            && !self
                .incomplete_owners
                .iter()
                .any(|incomplete| match (incomplete, owner) {
                    (ReadOwner::Unscoped, _) => true,
                    (ReadOwner::Node(parent), ReadOwner::Node(node)) => {
                        node == parent
                            || ["/", "[", "@repair."]
                                .iter()
                                .any(|separator| node.starts_with(&format!("{parent}{separator}")))
                    }
                    _ => false,
                })
    }
}

#[derive(Clone, Default)]
pub struct ReadRecorder {
    trace: Arc<Mutex<ReadTrace>>,
    owner: ReadOwner,
}

impl Default for ReadTrace {
    fn default() -> Self {
        Self {
            reads: BTreeSet::new(),
            owners: BTreeSet::new(),
            incomplete_owners: BTreeSet::new(),
            incomplete: false,
            overflow: false,
            unsupported: false,
            poisoned: false,
            retained_bytes: 0,
            byte_limit: policy::DEFAULT_MAX_JOURNAL_EVENT_BYTES,
        }
    }
}

impl ReadRecorder {
    pub fn new(byte_limit: usize) -> Self {
        Self {
            trace: Arc::new(Mutex::new(ReadTrace {
                byte_limit,
                ..ReadTrace::default()
            })),
            owner: ReadOwner::Unscoped,
        }
    }

    fn lock(&self) -> MutexGuard<'_, ReadTrace> {
        self.trace.lock().unwrap_or_else(|error| {
            let mut trace = error.into_inner();
            trace.incomplete = true;
            trace.poisoned = true;
            trace
        })
    }

    pub fn for_owner(&self, owner: ReadOwner) -> Self {
        {
            let mut trace = self.lock();
            if !trace.owners.contains(&owner) && Self::charge(&mut trace, Self::owner_bytes(&owner))
            {
                trace.owners.insert(owner.clone());
            }
        }
        Self {
            trace: Arc::clone(&self.trace),
            owner,
        }
    }

    pub fn snapshot(&self) -> ReadTrace {
        self.lock().clone()
    }

    /// Whether additional bounded read evidence can still be retained.
    pub fn is_recording(&self) -> bool {
        let trace = self.lock();
        !trace.overflow && !trace.poisoned
    }

    /// Snapshot only a complete exact owner's reads, under one recorder lock.
    /// No unrelated owner payload is cloned at terminal outcome boundaries.
    pub fn complete_reads_for(&self, owner: &ReadOwner) -> Option<Vec<ReadDependency>> {
        let trace = self.lock();
        if !trace.complete_for(owner) {
            return None;
        }
        let start = ReadDependency {
            owner: owner.clone(),
            path: Vec::new(),
            kind: ReadKind::Lookup,
            identity: ReadIdentity::Present,
        };
        Some(
            trace
                .reads
                .range(start..)
                .take_while(|read| &read.owner == owner)
                .cloned()
                .collect(),
        )
    }

    pub(crate) fn fork(&self) -> Self {
        // Clones do not inherit either history or attribution.
        Self::new(self.lock().byte_limit)
    }

    pub fn set_byte_limit(&self, byte_limit: usize) {
        let mut trace = self.lock();
        trace.byte_limit = byte_limit;
        if trace.retained_bytes > byte_limit {
            trace.reads.clear();
            trace.incomplete_owners.clear();
            trace.owners.clear();
            trace.retained_bytes = 0;
            trace.incomplete = true;
            trace.overflow = true;
        }
    }

    fn charge(trace: &mut ReadTrace, bytes: usize) -> bool {
        if trace.overflow {
            return false;
        }
        if bytes > trace.byte_limit.saturating_sub(trace.retained_bytes) {
            trace.incomplete = true;
            trace.overflow = true;
            return false;
        }
        trace.retained_bytes += bytes;
        true
    }

    fn owner_bytes(owner: &ReadOwner) -> usize {
        std::mem::size_of::<ReadOwner>()
            + match owner {
                ReadOwner::Node(id) => id.len(),
                ReadOwner::Unscoped => 0,
            }
    }

    fn mark_unsupported(trace: &mut ReadTrace, owner: &ReadOwner) {
        trace.incomplete = true;
        trace.unsupported = true;
        if !trace.incomplete_owners.contains(owner) && Self::charge(trace, Self::owner_bytes(owner))
        {
            trace.incomplete_owners.insert(owner.clone());
        }
    }

    pub fn unsupported(&self) {
        Self::mark_unsupported(&mut self.lock(), &self.owner);
    }

    pub fn record(&self, path: &[String], kind: ReadKind, identity: ReadIdentity) -> bool {
        self.insert(ReadDependency {
            owner: self.owner.clone(),
            path: path.to_vec(),
            kind,
            identity,
        })
    }

    fn insert(&self, read: ReadDependency) -> bool {
        let mut trace = self.lock();
        if matches!(read.owner, ReadOwner::Unscoped) || read.identity == ReadIdentity::Unsupported {
            Self::mark_unsupported(&mut trace, &read.owner);
        }
        if trace.reads.contains(&read) {
            return !trace.overflow;
        }
        let bytes = read.path.iter().fold(
            std::mem::size_of::<ReadDependency>() + Self::owner_bytes(&read.owner),
            |total, segment| {
                total
                    .saturating_add(std::mem::size_of::<String>())
                    .saturating_add(segment.len())
            },
        );
        if !Self::charge(&mut trace, bytes) {
            return false;
        }
        trace.reads.insert(read);
        true
    }

    /// Explicit join: never rewrites child owners or shares child histories.
    pub fn merge(&self, other: &Self) {
        if Arc::ptr_eq(&self.trace, &other.trace) {
            return;
        }
        let child = other.snapshot();
        for owner in child.owners {
            self.for_owner(owner);
        }
        for read in child.reads {
            self.insert(read);
        }
        let mut trace = self.lock();
        for owner in child.incomplete_owners {
            Self::mark_unsupported(&mut trace, &owner);
        }
        trace.incomplete |= child.incomplete;
        trace.overflow |= child.overflow;
        trace.unsupported |= child.unsupported;
        trace.poisoned |= child.poisoned;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_deduplicates_and_overflows_without_resetting() {
        let reads = ReadRecorder::new(1024).for_owner(ReadOwner::Node("node".into()));
        let path = vec!["inputs".into(), "name".into()];
        let identity = ReadIdentity::of(Some(&serde_json::json!("one")));
        assert!(reads.is_recording());
        assert!(reads.record(&path, ReadKind::Lookup, identity.clone()));
        let bytes = reads.snapshot().retained_bytes;
        reads.set_byte_limit(bytes);
        assert!(reads.record(&path, ReadKind::Lookup, identity.clone()));
        assert!(!reads.snapshot().incomplete);
        assert!(!reads.record(&path, ReadKind::Collection, identity));
        let trace = reads.snapshot();
        assert!(trace.incomplete && trace.overflow);
        assert!(!reads.is_recording());
        assert_eq!(trace.reads.len(), 1);
        assert_eq!(trace.retained_bytes, bytes);
        reads.set_byte_limit(0);
        assert!(reads.snapshot().reads.is_empty());
        assert!(reads.snapshot().overflow);
    }

    #[test]
    fn joins_preserve_owner_failure_and_enforce_the_parent_budget() {
        let owner = ReadOwner::Node("each[0]".into());
        let child = ReadRecorder::new(1024).for_owner(owner.clone());
        child.record(&["item".into()], ReadKind::Lookup, ReadIdentity::Missing);
        child.unsupported();
        let parent = ReadRecorder::new(1024);
        let sibling = ReadOwner::Node("each[1]/child".into());
        parent.for_owner(sibling.clone());
        parent.merge(&child);
        assert!(
            !parent
                .snapshot()
                .complete_for(&ReadOwner::Node("each[0]/child".into()))
        );
        assert!(parent.snapshot().complete_for(&sibling));
        assert_eq!(parent.snapshot().reads, child.snapshot().reads);
        let tiny = ReadRecorder::new(0);
        tiny.merge(&child);
        assert!(tiny.snapshot().overflow);
        assert!(tiny.snapshot().reads.is_empty());
        assert_eq!(tiny.snapshot().retained_bytes, 0);
        assert!(!child.snapshot().overflow);
    }

    #[test]
    fn debug_diagnostics_redact_dynamic_paths_and_value_identities() {
        let recorder = ReadRecorder::default().for_owner(ReadOwner::Node("node".into()));
        recorder.record(
            &["inputs".into(), "private-user-key".into()],
            ReadKind::Lookup,
            ReadIdentity::of(Some(&serde_json::json!("private-value"))),
        );
        let trace = recorder.snapshot();
        let dependency = trace.reads.iter().next().expect("one dependency");
        for output in [
            format!("{trace:?}"),
            format!("{recorder:?}"),
            format!("{dependency:?}"),
        ] {
            assert!(!output.contains("private-user-key"));
            assert!(!output.contains("private-value"));
        }
    }

    #[test]
    fn poison_is_global_loss_not_a_runtime_panic() {
        let reads = ReadRecorder::default();
        let _ = std::panic::catch_unwind(|| {
            let _lock = reads.trace.lock().unwrap();
            panic!("poison");
        });
        reads.unsupported();
        assert!(reads.snapshot().poisoned);
        assert!(!reads.snapshot().complete_for(&ReadOwner::Node("n".into())));
        reads.merge(&ReadRecorder::default());
        reads.set_byte_limit(0);
        assert!(reads.fork().snapshot().reads.is_empty());
    }
}
