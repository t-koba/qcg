//! Source-currentness diagnostics only. No reset operations or child binding.
//! Only a workspace paired with the journal through the persisted service-run layout is accepted.
use super::{bounded, digest, history};
use crate::{
    BudgetState, ContinuationReadCoverage, ContinuationSafety, FsGateway, GatewayError,
    JournalLimits, NodeOutcome, RunState, StepRegistry, TerminalState, read_journal_values,
    serialize_bounded,
};
use camino::Utf8Path;
use contract::{Contract, ReadDependency, ReadIdentity, ReadKind, ReadOwner};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::{ErrorKind, Read};

/// Fixed, redacted refusal categories; never embed paths, values or I/O errors.
#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    SourceBinding,
    Unsealed,
    History,
    IncompleteTrace,
    Executor,
    UnsupportedState,
    Budget,
    InputIdentity,
    InputIo,
    Limit,
}

struct SourceDiagnostic {
    source_run_id: String,
    source_contract_sha256: String,
    source_last_seq: u64,
    retry_roots: Vec<String>,
    stale_outcomes: Vec<String>,
    preserved_outcomes: Vec<String>,
    inherited_budget: BudgetState,
}

// Even debug output is bounded and path/value-redacted. Node IDs are retained
// internally for inspection, never workspace paths or captured values.
impl std::fmt::Debug for SourceDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceDiagnostic")
            .field("retry_roots", &self.retry_roots.len())
            .field("stale_outcomes", &self.stale_outcomes.len())
            .field("preserved_outcomes", &self.preserved_outcomes.len())
            .finish_non_exhaustive()
    }
}

/// Requires an exact sealed source snapshot, not a projection from another
/// run. This function only reads; it does not promise power-loss durability or
/// make the source run continuable. In particular it cannot authorize a fork.
#[allow(clippy::too_many_arguments)] // Explicit source inputs; no reusable authorization context.
fn analyze_source(
    path: &Utf8Path,
    state: &RunState,
    events: &[Value],
    contract: &Contract,
    registry: &StepRegistry,
    workspace: &FsGateway,
    limits: JournalLimits,
) -> Result<SourceDiagnostic, Refusal> {
    let limits = bounded(limits);
    if !source_layout_matches(path, state, workspace) {
        return Err(Refusal::SourceBinding);
    }
    if !history::journal_is_cleanly_sealed(path) {
        return Err(Refusal::Unsealed);
    }
    if state.terminal != Some(TerminalState::Failed)
        || state.contract_sha256.as_deref() != Some(contract.sha256.as_str())
        || state.run_id.as_deref().is_none_or(str::is_empty)
        || state.generator_id.as_deref() != Some(contract.manifest.generator.id.as_str())
        || events.last().and_then(|event| event["seq"].as_u64()) != Some(state.last_seq)
        || events
            .iter()
            .any(|event| event["run_id"].as_str() != state.run_id.as_deref())
        || events
            .iter()
            .filter(|event| matches!(event["t"].as_str(), Some("run_started" | "run_queued")))
            .any(|event| event["contract_sha256"].as_str() != Some(contract.sha256.as_str()))
    {
        return Err(Refusal::SourceBinding);
    }
    // Re-read within configured bounds: a matching in-memory tail is not
    // enough to prove that the supplied events are the sealed source journal.
    let persisted = read_journal_values(path, limits).map_err(|_| Refusal::History)?;
    if persisted.events != events {
        return Err(Refusal::SourceBinding);
    }
    history::proves_failed_history(events, &state.dependency_traces, limits)
        .ok_or(Refusal::History)?;
    supported_state(state, contract)?;
    exact_budget(&state.budget)?;
    let mut folded = RunState::fold_values(events).map_err(|_| Refusal::History)?;
    let encode = |state: &RunState| {
        serialize_bounded(state, limits.max_state_bytes, "source diagnostic")
            .map_err(|_| Refusal::Limit)
    };
    if encode(state)? != encode(&folded)? {
        return Err(Refusal::SourceBinding);
    }
    // The skipped cache field is not covered by serialized state equality.
    // Restore source-side evidence separately; never accept a substituted or
    // shortened in-memory trace merely because its terminal digest matches.
    super::TraceJournal::restore(&mut folded, events, path, limits);
    let mut result = SourceDiagnostic {
        source_run_id: state.run_id.clone().ok_or(Refusal::SourceBinding)?,
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: state.last_seq,
        retry_roots: Vec::new(),
        stale_outcomes: Vec::new(),
        preserved_outcomes: Vec::new(),
        inherited_budget: state.budget.clone(),
    };
    let mut outcomes = 0;
    for event in events {
        if !matches!(event["t"].as_str(), Some("step_started" | "step_finished")) {
            continue;
        }
        let owner = event["node"].as_str().ok_or(Refusal::History)?;
        // Deliberately never inspect graph needs: captured reads are the only
        // source of dependencies. Lifecycle proof pairs each start and finish.
        let node = contract.graph.nodes.get(owner).ok_or(Refusal::Executor)?;
        let attestation = registry
            .continuation_attestation(node)
            .ok_or(Refusal::Executor)?;
        if registry.continuation_safety(node) != ContinuationSafety::ReadOnly
            || registry.continuation_read_coverage(node) != ContinuationReadCoverage::Complete
            || (event["t"] == "step_started" && event["type"].as_str() != Some(node.kind.as_str()))
        {
            return Err(Refusal::Executor);
        }
        if event["t"] == "step_started" {
            continue;
        }
        outcomes += 1;
        let trace = state
            .dependency_traces
            .owners
            .get(owner)
            .ok_or(Refusal::IncompleteTrace)?;
        let source_trace = folded
            .dependency_traces
            .owners
            .get(owner)
            .ok_or(Refusal::IncompleteTrace)?;
        if trace.seq != source_trace.seq
            || trace.event_digest != source_trace.event_digest
            || trace.trace.reads != source_trace.trace.reads
            || trace.trace.executor_attestations != source_trace.trace.executor_attestations
        {
            return Err(Refusal::IncompleteTrace);
        }
        let read_owner = ReadOwner::Node(owner.to_owned());
        if trace.trace.executor_attestations.get(&read_owner) != Some(&Some(attestation)) {
            return Err(Refusal::Executor);
        }
        if trace.seq != event["seq"].as_u64().ok_or(Refusal::History)?
            || Some(trace.event_digest) != digest(event)
            || !trace.trace.complete_for(&read_owner)
            || trace
                .trace
                .reads
                .iter()
                .any(|read| read.owner != read_owner)
        {
            return Err(Refusal::IncompleteTrace);
        }
        // Validate every captured input, including failed roots. A changed
        // failed input still retries; an unknown input refuses the diagnostic.
        let mut unchanged = true;
        for read in &trace.trace.reads {
            unchanged &= match read.kind {
                ReadKind::Lookup | ReadKind::Collection => state
                    .vars
                    .matches_dependency(read)
                    .ok_or(Refusal::InputIdentity)?,
                ReadKind::WorkspaceFile => file_matches(
                    workspace,
                    read,
                    contract.manifest.runtime.file_input_limit_bytes,
                )?,
            };
        }
        match event["status"].as_str() {
            Some("failed" | "check_failed") => result.retry_roots.push(owner.to_owned()),
            Some("success") if unchanged => result.preserved_outcomes.push(owner.to_owned()),
            Some("success") => result.stale_outcomes.push(owner.to_owned()),
            _ => return Err(Refusal::History),
        }
    }
    if outcomes != state.nodes.len() || outcomes != state.dependency_traces.owners.len() {
        return Err(Refusal::IncompleteTrace);
    }
    result.retry_roots.sort();
    result.stale_outcomes.sort();
    result.preserved_outcomes.sort();
    serialize_bounded(
        &(
            &result.source_run_id,
            &result.source_contract_sha256,
            result.source_last_seq,
            &result.retry_roots,
            &result.stale_outcomes,
            &result.preserved_outcomes,
            &result.inherited_budget,
        ),
        limits.max_state_bytes,
        "source diagnostic",
    )
    .map_err(|_| Refusal::Limit)?;
    // Detect source journal changes during analysis. There is deliberately no
    // reusable authorization token: external workspace mutation remains outside
    // this point-in-time, source-only diagnostic.
    if !history::journal_is_cleanly_sealed(path)
        || read_journal_values(path, limits)
            .map_err(|_| Refusal::History)?
            .events
            != events
    {
        return Err(Refusal::SourceBinding);
    }
    Ok(result)
}

/// Bind the file-input gateway to the service-managed source run. Direct
/// executions and nonstandard layouts remain unsupported rather than relying
/// on a caller to pair an unrelated workspace with this journal.
fn source_layout_matches(path: &Utf8Path, state: &RunState, workspace: &FsGateway) -> bool {
    let Some(run_id) = state.run_id.as_deref() else {
        return false;
    };
    let Some(metadata_dir) = path.parent() else {
        return false;
    };
    let Some(run_dir) = metadata_dir.parent() else {
        return false;
    };
    let expected_workspace = run_dir.join("workspace");
    path.file_name() == Some("journal.jsonl")
        && metadata_dir.file_name() == Some("meta")
        && run_dir.file_name() == Some(run_id)
        && workspace.workspace() == expected_workspace.as_path()
}

fn supported_state(state: &RunState, contract: &Contract) -> Result<(), Refusal> {
    if !contract.manifest.hooks.is_empty()
        || contract
            .graph
            .nodes
            .values()
            .any(|node| node.kind.as_str() == "foreach")
        || state.schema_version != crate::RUN_STATE_SCHEMA_VERSION
        || state.parent_run_id.is_some()
        || !state.checkpoints.is_empty()
        || !state.unresolved_tasks.is_empty()
        || !state.foreach_reports.is_empty()
        || !state.removed_files.is_empty()
        || !state.resource_pins.is_empty()
        || !state.latest_file_pins.is_empty()
        || !state.historical_file_pins.is_empty()
        || state.pending.is_some()
        || state.pending_seq.is_some()
        || !state.pending_interactions.is_empty()
        || !state.interaction_seqs.is_empty()
        || !state.operation_records.is_empty()
        || !state.operation_attempts.is_empty()
        || !state.answers.is_empty()
        || !state.confirmations.is_empty()
        || state.cancel_requested
        || !state.cancel_operations.is_empty()
        || !state.mcp_pending.is_empty()
        || !state.mcp_resumed.is_empty()
        || !state.hooks_settled.is_empty()
        || !state.effects.is_empty()
        // Lifecycle proof accepts only root outcomes at attempt 1. Persisted
        // counters must describe exactly those single finished executions.
        || state.node_executions.len() != state.nodes.len()
        || state
            .nodes
            .keys()
            .any(|node| state.node_executions.get(node.as_str()) != Some(&1))
        || state.nodes.values().any(|outcome| match outcome {
            NodeOutcome::Success { files, .. } => !files.is_empty(),
            NodeOutcome::Failed { .. } => false,
            _ => true,
        })
    {
        return Err(Refusal::UnsupportedState);
    }
    Ok(())
}

fn exact_budget(budget: &BudgetState) -> Result<(), Refusal> {
    if budget.legacy_budget_resume_pending
        || budget.legacy_step_budget_unknown
        || budget.unresolved_search_cost
        || budget.unresolved_llm_cost
        || !budget.has_budget_charges
        || budget.step_budget_seed.is_none()
        || budget.step_budget_charged_at_seed.is_none()
        || budget.step_budget_consumed().is_err()
        || budget.elapsed_budget_mode != policy::ElapsedBudgetMode::ActiveExecution
        || budget
            .elapsed
            .as_ref()
            .is_none_or(|elapsed| elapsed.active_started_at.is_some())
        || budget.repair_attempts != 0
        || budget.regenerate_attempts != 0
        || budget.llm_calls != 0
        || budget.tokens_input != 0
        || budget.tokens_output != 0
        || budget.tokens_cached_input != 0
        || budget.tokens_cache_creation_input != 0
        || budget.cost_microusd != 0
    {
        return Err(Refusal::Budget);
    }
    Ok(())
}

fn file_matches(
    fs: &FsGateway,
    read: &ReadDependency,
    limit: Option<usize>,
) -> Result<bool, Refusal> {
    let ReadIdentity::Digest(expected) = &read.identity else {
        return Err(Refusal::InputIdentity);
    };
    if read.path.is_empty()
        || read
            .path
            .iter()
            .any(|part| part.contains('/') || !policy::is_safe_relative_path(part))
    {
        return Err(Refusal::InputIdentity);
    }
    let limit = limit.ok_or(Refusal::Limit)?;
    let mut file = match fs.open_diagnostic_read(&read.path.join("/")) {
        Ok(file) => file,
        Err(GatewayError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(Refusal::InputIo),
    };
    if !file.metadata().map_err(|_| Refusal::InputIo)?.is_file() {
        return Err(Refusal::InputIo);
    }
    let mut hash = Sha256::new();
    let mut bytes = [0_u8; 8192]; // Streaming buffer, not a policy limit.
    let mut total = 0_usize;
    loop {
        let count = file.read(&mut bytes).map_err(|_| Refusal::InputIo)?;
        if count == 0 {
            break;
        }
        total = total.checked_add(count).ok_or(Refusal::Limit)?;
        if total > limit {
            return Err(Refusal::Limit);
        }
        hash.update(&bytes[..count]);
    }
    Ok(<[u8; 32]>::from(hash.finalize()) == *expected)
}

#[cfg(test)]
mod tests;
