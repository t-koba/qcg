use super::*;
use crate::{Engine, RunOptions, StepContext, StepError, StepExecutor, StepOutcome};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use contract::NodeDef;
#[cfg(unix)]
use contract::Permissions;
use serde_json::json;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

/// A real executor: all runtime reads pass through ValueBag or are explicitly
/// recorded as raw workspace bytes. The second node produces a failed check.
struct Probe(Option<&'static str>);
#[async_trait]
impl StepExecutor for Probe {
    fn type_id(&self) -> &'static str {
        "test.probe"
    }
    fn continuation_semantics_version(&self) -> Option<&'static str> {
        self.0
    }
    fn continuation_safety(&self, _: &NodeDef) -> ContinuationSafety {
        ContinuationSafety::ReadOnly
    }
    fn continuation_read_coverage(&self, _: &NodeDef) -> ContinuationReadCoverage {
        ContinuationReadCoverage::Complete
    }
    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let _ = ctx.vars.get_path("inputs.private");
        #[cfg(unix)]
        {
            let mut file = ctx.run.fs.open_read("private.json").unwrap();
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            ctx.vars
                .read_recorder()
                .for_owner(ReadOwner::Node(node.id.clone()))
                .record(
                    &["private.json".into()],
                    ReadKind::WorkspaceFile,
                    ReadIdentity::Digest(Sha256::digest(&bytes).into()),
                );
        }
        if node.id == "b" {
            Ok(StepOutcome::check_failed(vec![model::Finding::error(
                "probe check failure".into(),
                None,
                None,
            )]))
        } else {
            Ok(StepOutcome::success(None))
        }
    }
}
struct ReadOnlyWithoutCoverage;
#[async_trait]
impl StepExecutor for ReadOnlyWithoutCoverage {
    fn continuation_semantics_version(&self) -> Option<&'static str> {
        Some("1")
    }
    fn type_id(&self) -> &'static str {
        "test.probe"
    }
    fn continuation_safety(&self, _: &NodeDef) -> ContinuationSafety {
        ContinuationSafety::ReadOnly
    }
    async fn execute(
        &self,
        _: &mut StepContext<'_>,
        _: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        unreachable!()
    }
}
fn registry() -> StepRegistry {
    let mut registry = StepRegistry::new();
    registry.register(Probe(Some("1")));
    registry
}

struct Fixture {
    _dir: crate::test_support::TestDir,
    path: Utf8PathBuf,
    workspace: Utf8PathBuf,
    contract: Contract,
    events: Vec<Value>,
    state: RunState,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_version(Some("1")).await
    }
    async fn with_version(version: Option<&'static str>) -> Self {
        let dir = crate::test_support::TestDir::create("source-diagnostic");
        let root = Utf8PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        std::fs::write(
            root.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "probe"
name = "Probe"
version = "0.1.0"
description = "Source-only diagnostic fixture"
[runtime]
file_input_limit_bytes = 1024
[permissions]
fs_read = ["workspace"]
fs_write = []
network = []
commands = []
containers = { enabled = false }
side_effects = "none"
[[inputs.stages]]
id = "source"
fields = [{ id = "private", type = "string" }]
[[flow]]
id = "a"
type = "test.probe"
[[flow]]
id = "b"
type = "test.probe"
"#,
        )
        .unwrap();
        let contract = Contract::load(&root).unwrap();
        let run_dir = root.join("source-probe");
        let metadata = run_dir.join("meta");
        let workspace = run_dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("private.json"), b"{}").unwrap();
        let options = RunOptions {
            elapsed_budget_mode: policy::ElapsedBudgetMode::ActiveExecution,
            output_dir: workspace.clone(),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            max_total_steps: 100,
            max_parallel_steps: 1,
            parallel_compatibility: policy::scheduling::ParallelCompatibility::default(),
            llm_provider: None,
            llm_seed_override: None,
            run_refs: BTreeMap::new(),
            reuse: None,
            cancellation: CancellationToken::new(),
            shutdown: None,
            scheduler_yield: CancellationToken::new(),
        };
        let mut executing_registry = StepRegistry::new();
        executing_registry.register(Probe(version));
        let failure = Engine::new(executing_registry)
            .run_with_id(
                "source-probe".into(),
                metadata.clone(),
                contract.clone(),
                BTreeMap::from([("private".into(), json!("original"))]),
                options,
            )
            .await
            .expect_err("b must fail");
        assert!(
            failure.to_string().contains("probe check failure"),
            "fixture failed before check: {failure}"
        );
        let path = metadata.join("journal.jsonl");
        let state = RunState::fold_journal(&path).unwrap();
        let events = read_journal_values(&path, JournalLimits::default())
            .unwrap()
            .events;
        Self {
            _dir: dir,
            path,
            workspace,
            contract,
            events,
            state,
        }
    }
    fn fs(&self) -> FsGateway {
        FsGateway::new(self.workspace.clone(), &self.contract.manifest.permissions)
    }
    fn analyze(&self) -> Result<SourceDiagnostic, Refusal> {
        analyze_source(
            &self.path,
            &self.state,
            &self.events,
            &self.contract,
            &registry(),
            &self.fs(),
            JournalLimits::default(),
        )
    }
    /// Bind a deliberately changed synthetic source journal, preserving the
    /// original read evidence to test currentness against its folded values.
    fn rewrite_source(&mut self) {
        std::fs::write(
            &self.path,
            self.events
                .iter()
                .map(|event| format!("{event}\n"))
                .collect::<String>(),
        )
        .unwrap();
        crate::journal::writer::write_clean_shutdown_marker(&self.path).unwrap();
        let traces = self.state.dependency_traces.clone();
        self.state = RunState::fold_values(&self.events).unwrap();
        self.state.dependency_traces = traces;
    }
}

#[tokio::test]
async fn source_diagnostic_refuses_a_different_run_workspace() {
    let f = Fixture::new().await;
    let other_workspace = f._dir.path().join("other-run/workspace");
    std::fs::create_dir_all(&other_workspace).unwrap();
    // Matching bytes must not make an unrelated workspace look source-bound.
    std::fs::write(other_workspace.join("private.json"), b"{}").unwrap();
    let other_fs = FsGateway::new(
        Utf8PathBuf::from_path_buf(other_workspace).unwrap(),
        &f.contract.manifest.permissions,
    );
    assert_eq!(
        analyze_source(
            &f.path,
            &f.state,
            &f.events,
            &f.contract,
            &registry(),
            &other_fs,
            JournalLimits::default(),
        )
        .unwrap_err(),
        Refusal::SourceBinding
    );
}

#[cfg(unix)]
#[tokio::test]
async fn synthetic_executor_is_source_bound_and_analysis_does_not_write() {
    let f = Fixture::new().await;
    let before = std::fs::read(&f.path).unwrap();
    let trace_before = std::fs::read(f.path.with_file_name(super::super::FILE_NAME)).unwrap();
    let plan = f.analyze().unwrap();
    assert_eq!(plan.retry_roots, ["b"]);
    assert_eq!(plan.preserved_outcomes, ["a"]);
    assert!(plan.stale_outcomes.is_empty());
    assert_eq!(plan.source_run_id, "source-probe");
    assert_eq!(plan.source_contract_sha256, f.contract.sha256);
    assert_eq!(plan.source_last_seq, f.state.last_seq);
    assert_eq!(
        serde_json::to_value(&plan.inherited_budget).unwrap(),
        serde_json::to_value(&f.state.budget).unwrap()
    );
    assert!(!format!("{plan:?}").contains("private"));
    assert_eq!(std::fs::read(&f.path).unwrap(), before);
    assert_eq!(
        std::fs::read(f.path.with_file_name(super::super::FILE_NAME)).unwrap(),
        trace_before
    );
    assert_eq!(
        std::fs::read(f.workspace.join("private.json")).unwrap(),
        b"{}"
    );
}

#[tokio::test]
async fn read_only_and_unknown_executors_do_not_attest_complete_reads() {
    let f = Fixture::new().await;
    for read_only in [false, true] {
        let mut registry = StepRegistry::new();
        if read_only {
            registry.register(ReadOnlyWithoutCoverage);
        }
        assert_eq!(
            registry.continuation_read_coverage(&f.contract.graph.nodes["a"]),
            ContinuationReadCoverage::Unsupported
        );
        assert_eq!(
            analyze_source(
                &f.path,
                &f.state,
                &f.events,
                &f.contract,
                &registry,
                &f.fs(),
                JournalLimits::default()
            )
            .unwrap_err(),
            Refusal::Executor
        );
    }
}

#[tokio::test]
async fn same_type_id_requires_the_executed_semantics_version() {
    let f = Fixture::new().await;
    assert!(f.analyze().is_ok());
    for version in [Some("2"), Some("unknown"), Some(""), None] {
        let mut changed = StepRegistry::new();
        changed.register(Probe(version));
        assert_eq!(
            analyze_source(
                &f.path,
                &f.state,
                &f.events,
                &f.contract,
                &changed,
                &f.fs(),
                JournalLimits::default(),
            )
            .unwrap_err(),
            Refusal::Executor,
        );
    }
}

#[tokio::test]
async fn complete_coverage_without_a_captured_version_cannot_gain_proof() {
    // A real ordinary run still succeeds/fails exactly as before. Reopening it
    // under a now-attesting executor cannot retroactively grant proof.
    let f = Fixture::with_version(None).await;
    assert_eq!(f.analyze().unwrap_err(), Refusal::Executor);
    for trace in f.state.dependency_traces.owners.values() {
        assert!(
            trace
                .trace
                .executor_attestations
                .values()
                .all(Option::is_none)
        );
    }
}

#[tokio::test]
async fn every_terminal_outcome_requires_complete_exact_event_evidence() {
    let mut f = Fixture::new().await;
    for owner in ["a", "b"] {
        let saved = f.state.dependency_traces.owners.remove(owner).unwrap();
        assert_eq!(f.analyze().unwrap_err(), Refusal::IncompleteTrace);
        f.state.dependency_traces.owners.insert(owner.into(), saved);
    }
    f.state
        .dependency_traces
        .owners
        .get_mut("a")
        .unwrap()
        .trace
        .incomplete_owners
        .insert(ReadOwner::Node("a".into()));
    assert!(matches!(
        f.analyze(),
        Err(Refusal::History | Refusal::IncompleteTrace)
    ));
}

#[tokio::test]
async fn identity_event_seq_contract_owner_and_type_mismatches_refuse() {
    let mut f = Fixture::new().await;
    let state = f.state.clone();
    for terminal in [
        TerminalState::Succeeded,
        TerminalState::Canceled,
        TerminalState::Interrupted,
    ] {
        f.state.terminal = Some(terminal);
        assert_eq!(f.analyze().unwrap_err(), Refusal::SourceBinding);
    }
    f.state = state.clone();
    f.state.run_id = Some("other".into());
    assert_eq!(f.analyze().unwrap_err(), Refusal::SourceBinding);
    f.state = state.clone();
    f.state.last_seq += 1;
    assert_eq!(f.analyze().unwrap_err(), Refusal::SourceBinding);
    f.state = state.clone();
    f.contract.sha256 = "other".into();
    assert_eq!(f.analyze().unwrap_err(), Refusal::SourceBinding);
    f.contract.sha256 = state.contract_sha256.clone().unwrap();
    f.events[0]["inputs"] = json!({"private":"changed without resealing"});
    assert_eq!(f.analyze().unwrap_err(), Refusal::SourceBinding);
    f.events = read_journal_values(&f.path, JournalLimits::default())
        .unwrap()
        .events;
    f.contract.graph.nodes.get_mut("a").unwrap().kind =
        serde_json::from_value(json!("other")).unwrap();
    assert_eq!(f.analyze().unwrap_err(), Refusal::Executor);
    f.contract.graph.nodes.remove("a");
    assert_eq!(f.analyze().unwrap_err(), Refusal::Executor);
    f.state.dependency_traces.owners.get_mut("a").unwrap().seq += 1;
    assert_eq!(f.analyze().unwrap_err(), Refusal::History);
}

#[tokio::test]
async fn persisted_event_mismatch_and_substituted_source_trace_refuse() {
    let mut f = Fixture::new().await;
    let start = f
        .events
        .iter()
        .position(|event| event["t"] == "step_started")
        .unwrap();
    f.events[start]["type"] = json!("other");
    f.rewrite_source();
    assert_eq!(f.analyze().unwrap_err(), Refusal::Executor);
    f.events[start]["type"] = json!("test.probe");
    f.rewrite_source();
    let reads = &mut f
        .state
        .dependency_traces
        .owners
        .get_mut("a")
        .unwrap()
        .trace
        .reads;
    reads.clear();
    assert_eq!(f.analyze().unwrap_err(), Refusal::IncompleteTrace);
    f.state = RunState::fold_journal(&f.path).unwrap();
    let terminal = f
        .events
        .iter_mut()
        .find(|event| event["t"] == "step_finished")
        .unwrap();
    terminal["output"] = json!({"tampered":true});
    f.rewrite_source();
    assert_eq!(f.analyze().unwrap_err(), Refusal::History);
}

#[cfg(unix)]
#[tokio::test]
async fn changed_and_missing_values_or_files_are_stale_not_graph_dependencies() {
    let mut f = Fixture::new().await;
    // These have no bearing on dependencies: no needs edge is consulted.
    f.contract.graph.nodes.get_mut("a").unwrap().needs = vec!["b".into()];
    for value in [json!({"private":"changed"}), json!({})] {
        f.events
            .iter_mut()
            .find(|event| event["t"] == "run_started")
            .unwrap()["inputs"] = value;
        f.rewrite_source();
        let plan = f.analyze().unwrap();
        assert_eq!(plan.stale_outcomes, ["a"]);
        assert!(plan.preserved_outcomes.is_empty());
        assert_eq!(plan.retry_roots, ["b"]);
    }
    f.events
        .iter_mut()
        .find(|event| event["t"] == "run_started")
        .unwrap()["inputs"] = json!({"private":"original"});
    f.rewrite_source();
    assert_eq!(f.analyze().unwrap().preserved_outcomes, ["a"]);
    std::fs::write(f.workspace.join("private.json"), b"{ }").unwrap();
    assert_eq!(f.analyze().unwrap().stale_outcomes, ["a"]);
    std::fs::remove_file(f.workspace.join("private.json")).unwrap();
    assert_eq!(f.analyze().unwrap().stale_outcomes, ["a"]);
}

#[cfg(unix)]
#[tokio::test]
async fn ambiguous_file_and_unsupported_identity_refuse_even_after_stale_input() {
    let mut f = Fixture::new().await;
    let denied = FsGateway::new(f.workspace.clone(), &Permissions::default());
    assert_eq!(
        analyze_source(
            &f.path,
            &f.state,
            &f.events,
            &f.contract,
            &registry(),
            &denied,
            JournalLimits::default()
        )
        .unwrap_err(),
        Refusal::InputIo
    );
    let file = f.workspace.join("private.json");
    std::fs::remove_file(&file).unwrap();
    std::os::unix::fs::symlink("unknown", &file).unwrap();
    assert_eq!(f.analyze().unwrap_err(), Refusal::InputIo);
    std::fs::remove_file(&file).unwrap();
    std::fs::create_dir(&file).unwrap();
    assert_eq!(f.analyze().unwrap_err(), Refusal::InputIo);
    std::fs::remove_dir(&file).unwrap();
    // The missing source root is unknown, not a missing dependency. It must
    // not be created by a diagnostic read.
    let root = f.workspace.join("absent-root");
    let unknown = FsGateway::new(root.clone(), &f.contract.manifest.permissions);
    let mut read = f.state.dependency_traces.owners["a"]
        .trace
        .reads
        .iter()
        .find(|read| read.kind == ReadKind::WorkspaceFile)
        .unwrap()
        .clone();
    assert_eq!(
        file_matches(&unknown, &read, Some(1024)),
        Err(Refusal::InputIo)
    );
    assert!(!root.exists());
    for identity in [
        ReadIdentity::Missing,
        ReadIdentity::Present,
        ReadIdentity::Unsupported,
    ] {
        read.identity = identity;
        assert_eq!(
            file_matches(&f.fs(), &read, Some(1024)),
            Err(Refusal::InputIdentity)
        );
    }
    // A FIFO must refuse promptly rather than waiting for a writer.
    let fifo = std::ffi::CString::new(file.as_str()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    assert_eq!(f.analyze().unwrap_err(), Refusal::InputIo);
    std::fs::remove_file(&file).unwrap();
    let trace = &mut f.state.dependency_traces.owners.get_mut("a").unwrap().trace;
    let mut read = trace
        .reads
        .iter()
        .find(|read| read.kind == ReadKind::WorkspaceFile)
        .unwrap()
        .clone();
    trace.reads.remove(&read);
    read.identity = ReadIdentity::Missing;
    trace.reads.insert(read);
    assert_eq!(f.analyze().unwrap_err(), Refusal::IncompleteTrace);
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_file_requires_explicit_byte_limit() {
    let mut f = Fixture::new().await;
    f.contract.manifest.runtime.file_input_limit_bytes = None;
    assert_eq!(f.analyze().unwrap_err(), Refusal::Limit);
    // Even a missing file cannot be classified without a configured bound.
    std::fs::remove_file(f.workspace.join("private.json")).unwrap();
    assert_eq!(f.analyze().unwrap_err(), Refusal::Limit);
}

#[tokio::test]
async fn unsupported_state_budget_and_limits_refuse() {
    let mut f = Fixture::new().await;
    let original = f.state.clone();
    let changes: &[fn(&mut RunState)] = &[
        |s| {
            s.node_executions.insert("a".into(), 2);
        },
        |s| {
            s.node_executions.insert("extra".into(), 1);
        },
        |s| {
            s.node_executions.remove("a");
        },
        |s| {
            s.checkpoints.insert(model::NodePath::root("a"), json!({}));
        },
        |s| {
            s.resource_pins.insert("private".into(), "digest".into());
        },
        |s| {
            s.latest_file_pins.insert("private".into(), "digest".into());
        },
        |s| {
            s.effects.insert("effect".into(), json!({}));
        },
        |s| {
            s.operation_attempts.insert("operation".into(), 1);
        },
        |s| {
            s.cancel_requested = true;
        },
        |s| {
            s.pending_seq = Some(1);
        },
        |s| {
            s.foreach_reports.insert("iteration".into(), json!({}));
        },
    ];
    for change in changes {
        f.state = original.clone();
        change(&mut f.state);
        assert_eq!(f.analyze().unwrap_err(), Refusal::UnsupportedState);
    }
    let changes: &[fn(&mut BudgetState)] = &[
        |b| b.legacy_step_budget_unknown = true,
        |b| b.legacy_budget_resume_pending = true,
        |b| b.unresolved_llm_cost = true,
        |b| b.unresolved_search_cost = true,
        |b| b.step_budget_seed = None,
        |b| b.step_budget_seed = Some(usize::MAX),
        |b| b.elapsed = None,
        |b| b.elapsed_budget_mode = policy::ElapsedBudgetMode::WallClock,
    ];
    for change in changes {
        f.state = original.clone();
        change(&mut f.state.budget);
        assert_eq!(f.analyze().unwrap_err(), Refusal::Budget);
    }
    f.state = original;
    let limits = JournalLimits {
        max_state_bytes: Some(1),
        ..JournalLimits::default()
    };
    assert_eq!(
        analyze_source(
            &f.path,
            &f.state,
            &f.events,
            &f.contract,
            &registry(),
            &f.fs(),
            limits
        )
        .unwrap_err(),
        Refusal::Limit
    );
    // A newline alone is not a clean shutdown proof.
    std::fs::remove_file(crate::journal::writer::clean_shutdown_marker_path(&f.path)).unwrap();
    assert_eq!(f.analyze().unwrap_err(), Refusal::Unsealed);
}
