pub mod package;
mod run_package;
pub use package::{
    ArtifactZipLimits, DEFAULT_PACKAGE_MAX_BYTES, DEFAULT_PACKAGE_MAX_ENTRIES, PackageLimits,
};

mod artifacts;
mod event_reader;
pub use event_reader::{EVENT_BATCH_BYTES, EVENT_BATCH_COUNT, EventBatch, EventCursor};
mod catalog;
mod invalidation;
mod lifecycle;
mod queue;
mod read_store;
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
pub(crate) mod retention_gc_test_hooks {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(crate) static PASSES: AtomicUsize = AtomicUsize::new(0);
    pub(crate) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    pub(crate) fn record_pass() {
        PASSES.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

pub use policy::{ElapsedBudgetMode, PreemptionMode, PreemptionQueuePosition};
