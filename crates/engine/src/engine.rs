mod checkpoint;
mod elapsed;
mod execute;
mod foreach;
mod repair;
mod repair_support;
mod replay;
mod run;
mod run_context;
mod types;

#[cfg(not(unix))]
pub(crate) use checkpoint::is_symlink_no_follow;
#[cfg(test)]
pub(crate) use checkpoint::*;
#[cfg(test)]
pub(crate) use replay::*;
pub(crate) use run_context::checkpoint_scope;
pub use run_context::{
    GuardDecision, OperationOutcome, bind_command_stdin, http_details_with_url,
    http_operation_details, safe_http_details, salted_binding_digest,
};
pub use types::*;

#[cfg(test)]
mod tests;

/// Settle an active-execution ledger at a known handoff while the caller holds
/// the run execution lease. Open intervals are charged through `now` using the
/// same checked recovery path used when an engine resumes; closed intervals
/// are left unchanged. Callers must not use this at process startup to mask an
/// uncertain crash boundary.
pub fn settle_active_elapsed(
    journal: &crate::JournalWriter,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), EngineError> {
    let state = journal.state();
    if state.budget.elapsed_budget_mode != policy::ElapsedBudgetMode::ActiveExecution {
        return Ok(());
    }
    if state.budget.elapsed.is_none() && !state.execution_started {
        // A queued run canceled before its first execution has no active
        // interval to settle. Once execution starts, a missing ledger is
        // corruption and the normal checked recovery path refuses it.
        return Ok(());
    }
    elapsed::recover(journal, now)?;
    Ok(())
}
