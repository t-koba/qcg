//! Source-currentness and reset planning; private child projection lives in overlay.
//! Only a workspace paired with the journal through the persisted service-run layout is accepted.
use super::{bounded, digest, history};
use crate::{
    BudgetState, ContinuationReadCoverage, ContinuationSafety, FsGateway, GatewayError,
    JournalLimits, NodeOutcome, RunState, StepRegistry, TerminalState, read_journal_values,
    serialize_bounded,
};
use camino::{Utf8Path, Utf8PathBuf};
use contract::{Contract, FieldType, ReadDependency, ReadIdentity, ReadKind, ReadOwner};
use serde::{Deserialize, Serialize};

pub(crate) mod overlay;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{ErrorKind, Read};

/// Fixed, redacted refusal categories; never embed paths, values or I/O errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationRefusal {
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
    ResetDependency,
    NotFailed,
}

// The private analysis keeps its original name; only the public entry may
// expose a refusal, and only through the fixed categories above.
type Refusal = ContinuationRefusal;

impl ContinuationRefusal {
    /// One fixed, redacted reason per category. No caller-supplied text,
    /// path, value or I/O detail can reach this string.
    pub fn reason(self) -> &'static str {
        match self {
            Self::SourceBinding => {
                "source run is not bound to this journal, contract and workspace"
            }
            Self::Unsealed => "source journal is not cleanly sealed",
            Self::History => "source history is incomplete or invalid",
            Self::IncompleteTrace => "source outcome dependency evidence is incomplete",
            Self::Executor => "executor is not continuation-safe or its declaration changed",
            Self::UnsupportedState => "source state has unsupported continuation liabilities",
            Self::Budget => "source budget is not exact and inheritable",
            Self::InputIdentity => "source input identity is unresolved",
            Self::InputIo => "source input evidence is unavailable",
            Self::Limit => "configured size limit exceeded",
            Self::ResetDependency => "reset dependency graph is not safely resolvable",
            Self::NotFailed => "source run is not terminally failed",
        }
    }
}

impl std::fmt::Display for ContinuationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

impl std::error::Error for ContinuationRefusal {}

/// Bounded entry point for a source-only continuation plan. It loads the
/// service-owned run layout itself and never accepts caller-supplied state,
/// events, contract or workspace bytes. The result is point-in-time evidence,
/// not authorization to fork or reset anything.
#[doc(hidden)]
pub fn plan_continuation(
    source_dir: &Utf8Path,
    registry: &StepRegistry,
    limits: JournalLimits,
) -> Result<ResetPlanCandidate, ContinuationRefusal> {
    let journal = source_dir.join("meta").join("journal.jsonl");
    let events = read_journal_values(&journal, limits)
        .map_err(|_| Refusal::History)?
        .events;
    let state =
        RunState::fold_journal_with_limits(&journal, limits).map_err(|_| Refusal::History)?;
    if state.terminal != Some(TerminalState::Failed) {
        return Err(Refusal::NotFailed);
    }
    let contract_root = source_dir.join("generator-package");
    let contract = Contract::load(&contract_root).map_err(|_| Refusal::SourceBinding)?;
    let workspace = FsGateway::new(source_dir.join("workspace"), &contract.manifest.permissions);
    analyze_source(
        &journal, &state, &events, &contract, registry, &workspace, limits,
    )
}

/// Opaque point-in-time evidence identity, not authorization or a lease. It
/// does not close analysis-to-copy TOCTOU, even when two identities match.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiagnosticIdentity([u8; 32]);

impl std::fmt::Debug for DiagnosticIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DiagnosticIdentity([redacted])")
    }
}

/// Opaque, point-in-time reset-plan evidence, not fork authorization or a lease.
/// Only successful private source analysis creates this value. Serialization is
/// an internal evidence payload; there is deliberately no public decoding or digest
/// constructor. It does not close analysis-to-copy TOCTOU.
#[doc(hidden)]
#[derive(Clone, Serialize)]
pub struct ResetPlanCandidate {
    identity: DiagnosticIdentity,
    // Only live analysis retains staging requirements. The evidence identity
    // already commits to these reads; child overlays must not publish paths.
    #[serde(skip)]
    required_files: Option<BTreeMap<Utf8PathBuf, Option<String>>>,
    source_run_id: String,
    source_contract_sha256: String,
    source_last_seq: u64,
    retry_roots: Vec<String>,
    stale_outcomes: Vec<String>,
    preserved_outcomes: Vec<String>,
    inherited_budget: BudgetState,
    reset_nodes: BTreeMap<String, ResetNode>,
    // Point-in-time evidence for every outcome owner, not authorization.
    expected_executor_attestations: BTreeMap<String, [u8; 32]>,
}

impl ResetPlanCandidate {
    /// Exact source-relative staging requirements from live source analysis.
    /// Some(SHA) requires those bytes; None requires the path to remain absent.
    /// Private overlay decoding cannot reconstruct these requirements and must
    /// not be used to prepare another fork. No candidate construction is exposed.
    /// The last durable sequence of the analyzed source, used to pin the
    /// child's checkpoint history to exactly this sealed source journal.
    #[doc(hidden)]
    pub fn source_last_seq(&self) -> u64 {
        self.source_last_seq
    }

    /// Exact inherited budget evidence, never a replacement budget input.
    #[doc(hidden)]
    pub fn inherited_budget(&self) -> &BudgetState {
        &self.inherited_budget
    }

    #[doc(hidden)]
    pub fn required_file_observations(
        &self,
    ) -> Option<impl Iterator<Item = (&Utf8Path, Option<&str>)>> {
        self.required_files.as_ref().map(|files| {
            files
                .iter()
                .map(|(path, sha256)| (path.as_path(), sha256.as_deref()))
        })
    }

    fn require_file(
        &mut self,
        read: &ReadDependency,
        digest: Option<[u8; 32]>,
    ) -> Result<(), Refusal> {
        let root = match read.kind {
            ReadKind::PackageFile => "generator-package",
            ReadKind::WorkspaceFile => "workspace",
            _ => return Err(Refusal::InputIdentity),
        };
        let (_, relative) = file_read_identity(read)?;
        let path = Utf8Path::new(root).join(relative);
        let files = self.required_files.as_mut().ok_or(Refusal::InputIdentity)?;
        let sha256 = digest.map(hex::encode);
        match files.entry(path) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(sha256);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &sha256 => {
                return Err(Refusal::InputIdentity);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResetNode {
    output_alias: String,
    predecessors: BTreeSet<String>,
}

// Even debug output is bounded and path/value-redacted. Node IDs are retained
// internally for inspection, never workspace paths or captured values.
impl std::fmt::Debug for ResetPlanCandidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResetPlanCandidate")
            .field("retry_roots", &self.retry_roots.len())
            .field("stale_outcomes", &self.stale_outcomes.len())
            .field("preserved_outcomes", &self.preserved_outcomes.len())
            .finish_non_exhaustive()
    }
}

/// V2 hashes length-framed, labelled JSON parts under a versioned domain.
/// JSON maps and captured read sets have deterministic sorted iteration. Only
/// one policy-bounded part is materialized at a time and immediately discarded;
/// no values, paths, trace payloads or workspace bytes enter the result.
fn identity_part<T: Serialize>(
    hash: &mut Sha256,
    label: &str,
    value: &T,
    limit: Option<usize>,
) -> Result<(), Refusal> {
    let bytes = serialize_bounded(value, limit, "source diagnostic identity")
        .map_err(|_| Refusal::Limit)?;
    let label_len = u64::try_from(label.len()).map_err(|_| Refusal::Limit)?;
    let len = u64::try_from(bytes.len()).map_err(|_| Refusal::Limit)?;
    hash.update(label_len.to_be_bytes());
    hash.update(label.as_bytes());
    hash.update(len.to_be_bytes());
    hash.update(&bytes);
    Ok(())
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
) -> Result<ResetPlanCandidate, Refusal> {
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
    // The history proof establishes exactly one run_started. Use its admitted
    // values, never the folded/materialized workspace-path projection.
    let original_inputs = &events
        .iter()
        .find(|event| event["t"] == "run_started")
        .ok_or(Refusal::History)?["inputs"];
    // The journal keeps the declared raw inputs, while steps execute with file
    // inputs materialized to workspace-relative paths. Read evidence was
    // captured against that view, so compare it against the same projection.
    // Raw bytes remain bound by workspace_observation and journal identity;
    // this is only the deterministic path projection the engine performs.
    let raw_inputs: BTreeMap<String, Value> =
        serde_json::from_value(original_inputs.clone()).map_err(|_| Refusal::InputIdentity)?;
    let mut read_vars = state.vars.clone();
    read_vars.set_inputs(materialized_input_view(contract, &raw_inputs)?);
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
    let mut identity = Sha256::new();
    identity.update(b"qcg-private-source-diagnostic-v2\0");
    // Contract SHA binds the manifest/package declarations; consumed package
    // bytes are additionally bound by trace and current-file observations.
    identity_part(
        &mut identity,
        "contract-package",
        &(
            &contract.sha256,
            &contract.manifest.generator.id,
            &contract.manifest.generator.version,
        ),
        limits.max_state_bytes,
    )?;
    let mut result = ResetPlanCandidate {
        identity: DiagnosticIdentity([0; 32]),
        required_files: Some(BTreeMap::new()),
        source_run_id: state.run_id.clone().ok_or(Refusal::SourceBinding)?,
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: state.last_seq,
        retry_roots: Vec::new(),
        stale_outcomes: Vec::new(),
        preserved_outcomes: Vec::new(),
        inherited_budget: state.budget.clone(),
        reset_nodes: BTreeMap::new(),
        expected_executor_attestations: BTreeMap::new(),
    };
    let mut outcomes = 0;
    // Index only current successes by the exact step path they consumed. The
    // read set tracks data dependencies separately from scheduler `needs` gates.
    let mut readers_by_step = BTreeMap::<String, BTreeSet<String>>::new();
    let mut aggregate_readers = BTreeSet::<String>::new();
    for event in events {
        // Includes the exact admitted inputs in run_started, not the folded
        // workspace-path projection. Bind every validated event, not just tails.
        identity_part(
            &mut identity,
            "journal-event",
            event,
            limits.max_event_bytes,
        )?;
        if !matches!(event["t"].as_str(), Some("step_started" | "step_finished")) {
            continue;
        }
        let owner = event["node"].as_str().ok_or(Refusal::History)?;
        // Captured reads alone establish data dependencies; scheduler gates are
        // handled separately below. Lifecycle proof pairs each start and finish.
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
        if result
            .expected_executor_attestations
            .insert(owner.to_owned(), attestation)
            .is_some_and(|previous| previous != attestation)
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
        identity_part(
            &mut identity,
            "trace-outcome",
            &(
                super::VERSION,
                owner,
                trace.seq,
                trace.event_digest,
                attestation,
            ),
            limits.max_event_bytes,
        )?;
        // Validate every captured input, including failed roots. A changed
        // failed input still retries; an unknown input refuses the diagnostic.
        let mut unchanged = true;
        for read in &trace.trace.reads {
            // Use the private trace wire vocabulary, including identity tags.
            // Do not serialize Debug or retain the serialized payload.
            let kind = match read.kind {
                ReadKind::Lookup => super::Kind::Lookup,
                ReadKind::Collection => super::Kind::Collection,
                ReadKind::PackageFile => super::Kind::PackageFile,
                ReadKind::WorkspaceFile => super::Kind::WorkspaceFile,
            };
            let read_identity = match read.identity {
                ReadIdentity::Present => super::Identity::Present,
                ReadIdentity::Missing => super::Identity::Missing,
                ReadIdentity::Unsupported => super::Identity::Unsupported,
                ReadIdentity::Digest(value) => super::Identity::Digest(value),
            };
            identity_part(
                &mut identity,
                "trace-read",
                &(&read.path, kind, read_identity),
                limits.max_event_bytes,
            )?;
            unchanged &= match read.kind {
                ReadKind::Lookup | ReadKind::Collection => read_vars
                    .matches_dependency(read)
                    .ok_or(Refusal::InputIdentity)?,
                ReadKind::PackageFile | ReadKind::WorkspaceFile => {
                    let observed = if read.kind == ReadKind::PackageFile {
                        package_observation(path, contract, read, policy::MAX_JSON_SCHEMA_BYTES)?
                    } else {
                        workspace_observation(workspace, read, contract, original_inputs)?
                    };
                    // None is missing, Some is the raw-byte digest read through
                    // the very same bounded no-follow handle used for currentness.
                    identity_part(
                        &mut identity,
                        "current-file",
                        &observed,
                        limits.max_event_bytes,
                    )?;
                    result.require_file(read, observed)?;
                    observed.is_some_and(|digest| read.identity == ReadIdentity::Digest(digest))
                }
            };
        }
        match event["status"].as_str() {
            Some("failed" | "check_failed") => result.retry_roots.push(owner.to_owned()),
            Some("success") if unchanged => {
                result.preserved_outcomes.push(owner.to_owned());
                index_step_readers(
                    owner,
                    &trace.trace.reads,
                    &mut readers_by_step,
                    &mut aggregate_readers,
                );
            }
            Some("success") => result.stale_outcomes.push(owner.to_owned()),
            _ => return Err(Refusal::History),
        }
    }
    propagate_step_invalidation(contract, &mut result, &readers_by_step, &aggregate_readers)?;
    if outcomes != state.nodes.len() || outcomes != state.dependency_traces.owners.len() {
        return Err(Refusal::IncompleteTrace);
    }
    let outcome_owners = result
        .retry_roots
        .iter()
        .chain(&result.stale_outcomes)
        .chain(&result.preserved_outcomes)
        .collect::<BTreeSet<_>>();
    if result
        .expected_executor_attestations
        .keys()
        .collect::<BTreeSet<_>>()
        != outcome_owners
    {
        return Err(Refusal::Executor);
    }
    result.reset_nodes = reset_schedule(
        contract,
        result.retry_roots.iter().chain(&result.stale_outcomes),
        |owner| {
            state
                .dependency_traces
                .owners
                .get(owner)
                .map(|trace| &trace.trace.reads)
        },
    )?;
    result.retry_roots.sort();
    result.stale_outcomes.sort();
    result.preserved_outcomes.sort();
    bind_candidate_payload(&mut identity, &result, limits.max_state_bytes)?;
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
    result.identity = DiagnosticIdentity(identity.finalize().into());
    Ok(result)
}

fn bind_candidate_payload(
    identity: &mut Sha256,
    result: &ResetPlanCandidate,
    limit: Option<usize>,
) -> Result<(), Refusal> {
    identity_part(
        identity,
        "source-plan-budget",
        &(
            &result.source_run_id,
            &result.source_contract_sha256,
            result.source_last_seq,
            &result.retry_roots,
            &result.stale_outcomes,
            &result.preserved_outcomes,
            &result.inherited_budget,
            &result.reset_nodes,
        ),
        limit,
    )
}

/// Index captured reads of node outputs/statuses. Empty-path reads are
/// aggregate; the `steps` root is aggregate only when its collection is consumed.
fn index_step_readers(
    owner: &str,
    reads: &BTreeSet<ReadDependency>,
    readers_by_step: &mut BTreeMap<String, BTreeSet<String>>,
    aggregate_readers: &mut BTreeSet<String>,
) {
    for read in reads {
        match step_read(read) {
            StepRead::None => {}
            StepRead::Aggregate => {
                aggregate_readers.insert(owner.to_owned());
            }
            StepRead::Named(step) => {
                readers_by_step
                    .entry(step.to_owned())
                    .or_default()
                    .insert(owner.to_owned());
            }
        }
    }
}

// Share the captured-read interpretation with stale closure. A Present lookup
// of the steps root only traverses an ancestor; it does not consume the root.
enum StepRead<'a> {
    None,
    Aggregate,
    Named(&'a str),
}
fn step_read(read: &ReadDependency) -> StepRead<'_> {
    if !matches!(read.kind, ReadKind::Lookup | ReadKind::Collection) {
        return StepRead::None;
    }
    if read.path.is_empty()
        || (read.path.as_slice() == ["steps"]
            && !(read.kind == ReadKind::Lookup && read.identity == ReadIdentity::Present))
    {
        StepRead::Aggregate
    } else if read.path.first().is_some_and(|root| root == "steps")
        && let Some(step) = read.path.get(1)
    {
        StepRead::Named(step)
    } else {
        StepRead::None
    }
}

/// Resolve both node IDs and exact contract output aliases. Collisions are
/// retained, not overwritten: a consumed ambiguous name must refuse even when
/// only one of its possible owners is reset.
fn reset_schedule<'a>(
    contract: &Contract,
    owners: impl Iterator<Item = &'a String>,
    reads: impl Fn(&str) -> Option<&'a BTreeSet<ReadDependency>>,
) -> Result<BTreeMap<String, ResetNode>, Refusal> {
    let mut aliases = BTreeMap::<&str, BTreeSet<&str>>::new();
    for (owner, node) in &contract.graph.nodes {
        for alias in [owner.as_str(), node.output.as_deref().unwrap_or(&node.id)] {
            aliases.entry(alias).or_default().insert(owner);
        }
    }
    let mut reset = BTreeMap::new();
    for owner in owners {
        let node = contract.graph.nodes.get(owner).ok_or(Refusal::Executor)?;
        let output_alias = node.output.as_deref().unwrap_or(&node.id);
        // The eventual overlay removes both possible step-output keys. Neither
        // may also name a different node's output, or clearing one reset node
        // could erase a preserved result even when no trace read that key.
        for name in [owner.as_str(), output_alias] {
            let targets = aliases.get(name).ok_or(Refusal::ResetDependency)?;
            if targets.len() != 1 || !targets.contains(owner.as_str()) {
                return Err(Refusal::ResetDependency);
            }
        }
        reset.insert(
            owner.clone(),
            ResetNode {
                output_alias: output_alias.to_owned(),
                predecessors: BTreeSet::new(),
            },
        );
    }
    let reset_ids = reset.keys().cloned().collect::<BTreeSet<_>>();
    for (owner, node) in &mut reset {
        node.predecessors = reset_predecessors(
            reads(owner).ok_or(Refusal::IncompleteTrace)?,
            &aliases,
            &reset_ids,
        )?;
    }
    // The child enforces two gates before advancing a Pending reset node: the
    // captured-read gate (all predecessors terminal) and the contract `needs`
    // gate. Model their union over the reset set as a least fixpoint of nodes
    // that can reach a terminal state. A node qualifies once all its reset
    // predecessors are terminal and its needs gate can open: AllSucceeded and
    // NoneFailed wait for every need; AnySucceeded needs a preserved source
    // success outside the reset set, or all needs terminal (the child opens the
    // gate only on an actual Success, else skips once every need is terminal).
    // Needs outside the reset set are preserved source successes (already
    // terminal).
    // Multi-need AnySucceeded cannot be flattened into mandatory AND edges
    // without over-refusing plans that have a free alternative, so the gate is
    // evaluated directly. Any reset node left out has no runnable order -- the
    // child would stall with "no runnable node remains" -- so refuse. The
    // serialized reset payload keeps only captured reads.
    let mut terminal = BTreeSet::<&str>::new();
    loop {
        let mut grew = false;
        for (owner, node) in &reset {
            if terminal.contains(owner.as_str()) {
                continue;
            }
            if !node
                .predecessors
                .iter()
                .all(|dep| terminal.contains(dep.as_str()))
            {
                continue;
            }
            let is_terminal =
                |need: &String| !reset_ids.contains(need) || terminal.contains(need.as_str());
            let needs_open = match contract.graph.nodes.get(owner) {
                None => return Err(Refusal::Executor),
                Some(def) if def.needs.is_empty() => true,
                Some(def) => match def.on_deps {
                    contract::OnDeps::AllSucceeded | contract::OnDeps::NoneFailed => {
                        def.needs.iter().all(is_terminal)
                    }
                    contract::OnDeps::AnySucceeded => {
                        // The child opens an AnySucceeded gate only on an actual
                        // `Success` (needs_satisfied), or skips the node when every
                        // need is terminal and none succeeded
                        // (should_skip_by_dependencies). A reset need that merely
                        // reaches a terminal state may end Failed/Skipped, which
                        // opens neither path. So the gate is provably open only if
                        // some need is a preserved source success outside the reset
                        // set, or every need is terminal. Counting one reset need's
                        // arbitrary terminal state would admit a plan where the
                        // opener ends non-Success and the child stalls.
                        def.needs.iter().any(|need| !reset_ids.contains(need))
                            || def.needs.iter().all(is_terminal)
                    }
                },
            };
            if needs_open {
                terminal.insert(owner.as_str());
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
    if terminal.len() != reset.len() {
        return Err(Refusal::ResetDependency);
    }
    Ok(reset)
}

fn reset_predecessors(
    reads: &BTreeSet<ReadDependency>,
    aliases: &BTreeMap<&str, BTreeSet<&str>>,
    reset_ids: &BTreeSet<String>,
) -> Result<BTreeSet<String>, Refusal> {
    let mut predecessors = BTreeSet::new();
    for read in reads {
        match step_read(read) {
            StepRead::None => {}
            // Aggregate identity contains no per-entry provenance. Expanding
            // it to every reset node can create a dense graph and a self-cycle;
            // refuse rather than guess or allocate a quadratic edge set.
            StepRead::Aggregate => return Err(Refusal::ResetDependency),
            StepRead::Named(alias) => {
                let targets = aliases.get(alias).ok_or(Refusal::ResetDependency)?;
                if targets.len() != 1 {
                    return Err(Refusal::ResetDependency);
                }
                let target = *targets.first().ok_or(Refusal::ResetDependency)?;
                if reset_ids.contains(target) {
                    predecessors.insert(target.to_owned());
                }
            }
        }
    }
    Ok(predecessors)
}

/// Compute the transitive outcomes that may change when a retry root (or an
/// already-stale success) runs again. `needs` are eligibility gates, not data
/// reads: conservatively stale every preserved success with a changed gate
/// prerequisite. Do not assume another successful prerequisite will keep an
/// `AnySucceeded` gate open, or preserve a gate whose semantics are uncertain.
/// A rerun or changed eligibility can change output, status, or reason, so both
/// the node id and output alias are possible writes to captured readers.
fn propagate_step_invalidation(
    contract: &Contract,
    result: &mut ResetPlanCandidate,
    readers_by_step: &BTreeMap<String, BTreeSet<String>>,
    aggregate_readers: &BTreeSet<String>,
) -> Result<(), Refusal> {
    // Keep scheduler edges out of the captured-reader index. Index only
    // preserved successes: roots and already-stale nodes are queued below.
    let mut gated_by_step = BTreeMap::<String, BTreeSet<String>>::new();
    for owner in &result.preserved_outcomes {
        let node = contract.graph.nodes.get(owner).ok_or(Refusal::Executor)?;
        for need in &node.needs {
            if !contract.graph.nodes.contains_key(need) {
                return Err(Refusal::Executor);
            }
            gated_by_step
                .entry(need.clone())
                .or_default()
                .insert(owner.clone());
        }
    }
    let mut invalidated = result
        .retry_roots
        .iter()
        .chain(&result.stale_outcomes)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut pending = VecDeque::from_iter(invalidated.iter().cloned());
    let mut aggregate_processed = false;
    while let Some(changed_owner) = pending.pop_front() {
        let node = contract
            .graph
            .nodes
            .get(&changed_owner)
            .ok_or(Refusal::Executor)?;
        if let Some(dependents) = gated_by_step.get(&changed_owner) {
            for dependent in dependents {
                mark_stale(dependent, &mut invalidated, result, &mut pending);
            }
        }
        let output_name = node.output.as_deref().unwrap_or(&node.id);
        for name in [changed_owner.as_str(), output_name] {
            if let Some(readers) = readers_by_step.get(name) {
                for reader in readers {
                    mark_stale(reader, &mut invalidated, result, &mut pending);
                }
            }
        }
        if !aggregate_processed {
            aggregate_processed = true;
            for reader in aggregate_readers {
                mark_stale(reader, &mut invalidated, result, &mut pending);
            }
        }
    }
    Ok(())
}

fn mark_stale(
    owner: &str,
    invalidated: &mut BTreeSet<String>,
    result: &mut ResetPlanCandidate,
    pending: &mut VecDeque<String>,
) {
    if invalidated.insert(owner.to_owned()) {
        result
            .preserved_outcomes
            .retain(|preserved| preserved != owner);
        result.stale_outcomes.push(owner.to_owned());
        pending.push_back(owner.to_owned());
    }
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

/// Check the exact package file consumed by an executor. `contract.sha256`
/// binds the manifest and therefore the requested relative schema path; the
/// private read trace binds the raw bytes actually parsed. Unrelated package
/// files do not affect this executor's result.
fn package_observation(
    path: &Utf8Path,
    contract: &Contract,
    read: &ReadDependency,
    limit: usize,
) -> Result<Option<[u8; 32]>, Refusal> {
    let run_dir = path
        .parent()
        .and_then(Utf8Path::parent)
        .ok_or(Refusal::SourceBinding)?;
    let package_root = run_dir.join("generator-package");
    if contract.root != package_root {
        return Err(Refusal::SourceBinding);
    }
    let (_, relative) = file_read_identity(read)?;
    let file = match FsGateway::open_diagnostic_read_under_root(&contract.root, &relative) {
        Ok(file) => file,
        Err(GatewayError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(Refusal::InputIo),
    };
    opened_file_digest(file, limit).map(Some)
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

/// Validate the durable elapsed evidence for the source's budget mode.
///
/// ActiveExecution runs carry a closed active ledger. WallClock runs carry no
/// ledger at all: their exact consumption is the durable `started_at` anchor,
/// which the reset child inherits, so `wall_elapsed` charges the same window
/// to the child. Never accept a ledger in a mode that cannot produce one, or
/// an unanchored WallClock source whose consumption cannot be inherited.
fn exact_elapsed(budget: &BudgetState) -> bool {
    match budget.elapsed_budget_mode {
        policy::ElapsedBudgetMode::ActiveExecution => budget
            .elapsed
            .as_ref()
            .is_some_and(|elapsed| elapsed.active_started_at.is_none()),
        policy::ElapsedBudgetMode::WallClock => {
            budget.elapsed.is_none()
                && budget
                    .started_at
                    .as_deref()
                    .is_some_and(|started| chrono::DateTime::parse_from_rfc3339(started).is_ok())
        }
    }
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
        || !exact_elapsed(budget)
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

/// Reproduce the engine's file-input path projection for read comparison.
/// The declared bytes stay authoritative for all digest checks; this only
/// restores the view the step actually observed when it captured reads.
fn materialized_input_view(
    contract: &Contract,
    inputs: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, Refusal> {
    let mut view = inputs.clone();
    for field in contract
        .manifest
        .inputs
        .stages
        .iter()
        .flat_map(|stage| &stage.fields)
        .filter(|field| field.kind == FieldType::File)
    {
        let Some(value) = inputs.get(&field.id) else {
            continue;
        };
        let limit = contract
            .manifest
            .runtime
            .file_input_limit_bytes
            .ok_or(Refusal::Limit)?;
        let file =
            model::FileValue::from_value_with_limit(value, limit).map_err(|error| match error {
                model::FileValueError::TooLarge { .. } => Refusal::Limit,
                _ => Refusal::InputIdentity,
            })?;
        if !policy::is_safe_relative_path(&field.id)
            || field.id.contains('/')
            || !policy::is_safe_path_component(&file.name)
        {
            return Err(Refusal::InputIdentity);
        }
        view.insert(
            field.id.clone(),
            Value::String(format!("files/{}/{}", field.id, file.name)),
        );
    }
    Ok(view)
}

/// Only admitted FileValues can explain workspace reads. This is a diagnostic
/// binding, not authorization to continue or restore any workspace state.
fn workspace_observation(
    fs: &FsGateway,
    read: &ReadDependency,
    contract: &Contract,
    original_inputs: &Value,
) -> Result<Option<[u8; 32]>, Refusal> {
    let (expected, relative) = file_read_identity(read)?;
    let limit = contract
        .manifest
        .runtime
        .file_input_limit_bytes
        .ok_or(Refusal::Limit)?;
    let [root, field_id, name] = read.path.as_slice() else {
        return Err(Refusal::InputIdentity);
    };
    if root != "files" || !policy::is_safe_path_component(field_id) {
        return Err(Refusal::InputIdentity);
    }
    let mut fields = contract
        .manifest
        .inputs
        .stages
        .iter()
        .flat_map(|stage| &stage.fields)
        .filter(|field| &field.id == field_id);
    let field = fields.next().ok_or(Refusal::InputIdentity)?;
    if fields.next().is_some() || field.kind != FieldType::File {
        return Err(Refusal::InputIdentity);
    }
    let value = original_inputs
        .get(&field.id)
        .ok_or(Refusal::InputIdentity)?;
    let invalid_file = |error| match error {
        model::FileValueError::TooLarge { .. } => Refusal::Limit,
        _ => Refusal::InputIdentity,
    };
    // Parsing normalizes base64 and checks the name and decoded size under
    // the explicit bound; decoding uses that same bound, never a fallback.
    let file = model::FileValue::from_value_with_limit(value, limit).map_err(invalid_file)?;
    let canonical = serde_json::to_value(&file).map_err(|_| Refusal::InputIdentity)?;
    if &canonical != value || &file.name != name {
        return Err(Refusal::InputIdentity);
    }
    let bytes = file.decode_with_limit(limit).map_err(invalid_file)?;
    if <[u8; 32]>::from(Sha256::digest(&bytes)) != expected {
        return Err(Refusal::InputIdentity);
    }
    let file = match fs.open_diagnostic_read(&relative) {
        Ok(file) => file,
        Err(GatewayError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(Refusal::InputIo),
    };
    opened_file_digest(file, limit).map(Some)
}

fn file_read_identity(read: &ReadDependency) -> Result<([u8; 32], String), Refusal> {
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
    Ok((*expected, read.path.join("/")))
}

fn opened_file_digest(mut file: std::fs::File, limit: usize) -> Result<[u8; 32], Refusal> {
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
    Ok(hash.finalize().into())
}

#[cfg(test)]
mod tests;
