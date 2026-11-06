pub mod package;
mod run_package;
pub use package::{
    ArtifactZipLimits, DEFAULT_PACKAGE_MAX_ARCHIVE_BYTES, DEFAULT_PACKAGE_MAX_BYTES,
    DEFAULT_PACKAGE_MAX_ENTRIES, PackageLimits,
};

mod artifacts;
mod event_reader;
pub use event_reader::{EVENT_BATCH_BYTES, EVENT_BATCH_COUNT, EventBatch, EventCursor};
mod catalog;
mod idempotency_refs;
mod invalidation;
pub use idempotency_refs::MAX_IDEMPOTENCY_FILE_BYTES;
pub use invalidation::require_run_directory;
mod lifecycle;
mod queue;
mod read_store;
pub use read_store::with_acceptance_guard;
mod run_dirs;
mod run_refs;
mod runs_api;
mod summaries;
mod types;

pub use artifacts::*;
pub use run_dirs::*;
pub use summaries::*;
pub use types::*;

#[cfg(test)]
pub(crate) mod denial_settlement_test_hooks {
    use std::sync::Arc;

    tokio::task_local! {
        pub(crate) static BEFORE_SETTLE: Option<Arc<dyn Fn() + Send + Sync>>;
    }
}

#[cfg(test)]
pub(crate) mod retention_gc_test_hooks {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(crate) static PASSES: AtomicUsize = AtomicUsize::new(0);
    pub(crate) static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    pub(crate) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    pub(crate) fn record_attempt() {
        ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn record_pass() {
        PASSES.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

pub use policy::{ElapsedBudgetMode, PreemptionMode, PreemptionQueuePosition};
