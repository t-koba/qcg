use super::*;
use crate::{
    Engine, RunOptions, StepContext, StepError, StepExecutor, StepOutcome, TemplateService,
};
use async_trait::async_trait;
use camino::Utf8PathBuf;
#[cfg(unix)]
use contract::Permissions;
use contract::{NodeDef, RuntimeLimits, ValueBag};
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
            let mut file = ctx.run.fs.open_read("files/upload/private.json").unwrap();
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            ctx.vars
                .read_recorder()
                .for_owner(ReadOwner::Node(node.id.clone()))
                .record(
                    &["files".into(), "upload".into(), "private.json".into()],
                    ReadKind::WorkspaceFile,
                    ReadIdentity::Digest(Sha256::digest(&bytes).into()),
                );
        }
        if ctx.run.contract.root.file_name() == Some("generator-package") {
            let mut file =
                files::open_read_nofollow(&ctx.run.contract.root.join("schema.json")).unwrap();
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            ctx.vars
                .read_recorder()
                .for_owner(ReadOwner::Node(node.id.clone()))
                .record(
                    &["schema.json".into()],
                    ReadKind::PackageFile,
                    ReadIdentity::Digest(Sha256::digest(&bytes).into()),
                );
        }
        if node.output.as_deref() == Some("a_result") {
            let _ = ctx.vars.get_path("steps.b.output");
        }
        if node.id == "b" {
            Ok(StepOutcome::check_failed(vec![model::Finding::error(
                "probe check failure".into(),
                None,
                None,
            )]))
        } else if node.output.as_deref() == Some("a_result") {
            Ok(StepOutcome::success(Some(json!({"value": "source"}))))
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
        Self::with_package(version, false).await
    }
    async fn with_package(version: Option<&'static str>, package: bool) -> Self {
        Self::with_options(version, package, false).await
    }
    async fn with_output_dependency() -> Self {
        Self::with_options(Some("1"), false, true).await
    }
    async fn with_options(
        version: Option<&'static str>,
        package: bool,
        output_dependency: bool,
    ) -> Self {
        let dir = crate::test_support::TestDir::create("source-diagnostic");
        let root = Utf8PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let run_dir = root.join("source-probe");
        let package_root = if package {
            run_dir.join("generator-package")
        } else {
            root.clone()
        };
        std::fs::create_dir_all(&package_root).unwrap();
        std::fs::write(package_root.join("schema.json"), b"{}").unwrap();
        let mut manifest = r#"
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
fields = [{ id = "private", type = "string" }, { id = "upload", type = "file" }]
[[flow]]
id = "a"
type = "test.probe"
[[flow]]
id = "b"
type = "test.probe"
"#
        .to_owned();
        if output_dependency {
            let replaced = manifest.replace(
                "id = \"a\"\ntype = \"test.probe\"\n",
                "id = \"a\"\ntype = \"test.probe\"\noutput = \"a_result\"\n",
            );
            assert_ne!(
                replaced, manifest,
                "fixture alias must be included in its contract hash"
            );
            manifest = replaced;
        }
        std::fs::write(package_root.join(contract::MANIFEST_FILE), manifest).unwrap();
        let contract = Contract::load(&package_root).unwrap();
        let metadata = run_dir.join("meta");
        let workspace = run_dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
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
                BTreeMap::from([
                    ("private".into(), json!("original")),
                    (
                        "upload".into(),
                        json!({"name": "private.json", "text": "{}"}),
                    ),
                ]),
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
    fn original_inputs(&self) -> &Value {
        &self
            .events
            .iter()
            .find(|event| event["t"] == "run_started")
            .unwrap()["inputs"]
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
    std::fs::create_dir_all(other_workspace.join("files/upload")).unwrap();
    std::fs::write(other_workspace.join("files/upload/private.json"), b"{}").unwrap();
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
        std::fs::read(f.workspace.join("files/upload/private.json")).unwrap(),
        b"{}"
    );
}

#[tokio::test]
async fn retry_root_invalidates_success_that_read_its_missing_output() {
    // a reads b's output while absent. A retry may introduce it, so a cannot
    // be preserved just because its source-time missing read still matches.
    let f = Fixture::with_output_dependency().await;
    let plan = f.analyze().unwrap();
    assert_eq!(plan.retry_roots, ["b"]);
    assert_eq!(plan.stale_outcomes, ["a"]);
    assert!(plan.preserved_outcomes.is_empty());
}

#[tokio::test]
async fn stale_output_propagates_to_transitive_alias_consumers() {
    let f = Fixture::new().await;
    let mut contract = f.contract.clone();
    contract.graph.nodes.get_mut("a").unwrap().output = Some("a_result".into());
    let mut c = contract.graph.nodes["b"].clone();
    c.id = "c".into();
    c.output = Some("c_result".into());
    contract.graph.nodes.insert(c.id.clone(), c);

    let dependency = |owner: &str, step: &str| ReadDependency {
        owner: ReadOwner::Node(owner.into()),
        path: vec!["steps".into(), step.into(), "output".into()],
        kind: ReadKind::Lookup,
        identity: ReadIdentity::Missing,
    };
    let mut readers_by_step = BTreeMap::new();
    let mut aggregate_readers = BTreeSet::new();
    index_step_readers(
        "a",
        &BTreeSet::from([dependency("a", "b")]),
        &mut readers_by_step,
        &mut aggregate_readers,
    );
    index_step_readers(
        "c",
        &BTreeSet::from([dependency("c", "a_result")]),
        &mut readers_by_step,
        &mut aggregate_readers,
    );
    let mut result = SourceDiagnostic {
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["b".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into(), "c".into()],
        inherited_budget: f.state.budget.clone(),
    };

    propagate_step_invalidation(&contract, &mut result, &readers_by_step, &aggregate_readers)
        .unwrap();
    assert_eq!(result.stale_outcomes, ["a", "c"]);
    assert!(result.preserved_outcomes.is_empty());
}

#[tokio::test]
async fn any_succeeded_gate_invalidation_reaches_gates_and_captured_readers() {
    let f = Fixture::new().await;
    let mut contract = f.contract.clone();
    for id in [
        "c",
        "d",
        "output_reader",
        "status_reader",
        "reader_gate",
        "unrelated",
    ] {
        let mut node = contract.graph.nodes["a"].clone();
        node.id = id.into();
        contract.graph.nodes.insert(id.into(), node);
    }
    let c = contract.graph.nodes.get_mut("c").unwrap();
    c.needs = vec!["a".into(), "b".into()];
    c.on_deps = contract::OnDeps::AnySucceeded;
    c.output = Some("c_result".into());
    contract.graph.nodes.get_mut("d").unwrap().needs = vec!["c".into()];
    contract.graph.nodes.get_mut("reader_gate").unwrap().needs = vec!["output_reader".into()];

    let mut readers_by_step = BTreeMap::new();
    let mut aggregate_readers = BTreeSet::new();
    for (owner, step, field) in [
        ("output_reader", "c_result", "output"),
        ("status_reader", "d", "status"),
    ] {
        index_step_readers(
            owner,
            &BTreeSet::from([ReadDependency {
                owner: ReadOwner::Node(owner.into()),
                path: vec!["steps".into(), step.into(), field.into()],
                kind: ReadKind::Lookup,
                identity: ReadIdentity::Missing,
            }]),
            &mut readers_by_step,
            &mut aggregate_readers,
        );
    }
    // A succeeded, B failed, C succeeded through AnySucceeded. A is now
    // stale and may fail/skip on retry while B remains failed: C may skip.
    // Neither C nor D captured any data read of its gate prerequisites.
    let mut result = SourceDiagnostic {
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["b".into()],
        stale_outcomes: vec!["a".into()],
        preserved_outcomes: [
            "c",
            "d",
            "output_reader",
            "status_reader",
            "reader_gate",
            "unrelated",
        ]
        .map(String::from)
        .into(),
        inherited_budget: f.state.budget.clone(),
    };
    // First isolate the already-stale seed, then the failed retry root.
    // Either prerequisite changing must invalidate C without a captured read.
    for (retry_roots, stale_outcomes, preserved_a) in [
        (vec![], vec!["a".into()], false),
        (vec!["b".into()], vec![], true),
    ] {
        let mut seeded = SourceDiagnostic {
            retry_roots,
            stale_outcomes,
            preserved_outcomes: result.preserved_outcomes.clone(),
            source_run_id: result.source_run_id.clone(),
            source_contract_sha256: result.source_contract_sha256.clone(),
            source_last_seq: result.source_last_seq,
            inherited_budget: result.inherited_budget.clone(),
        };
        if preserved_a {
            seeded.preserved_outcomes.push("a".into());
        }
        propagate_step_invalidation(&contract, &mut seeded, &readers_by_step, &aggregate_readers)
            .unwrap();
        assert!(seeded.stale_outcomes.iter().any(|owner| owner == "c"));
    }
    propagate_step_invalidation(&contract, &mut result, &readers_by_step, &aggregate_readers)
        .unwrap();
    result.stale_outcomes.sort();
    assert_eq!(
        result.stale_outcomes,
        [
            "a",
            "c",
            "d",
            "output_reader",
            "reader_gate",
            "status_reader"
        ]
    );
    assert_eq!(result.retry_roots, ["b"]);
    assert_eq!(result.preserved_outcomes, ["unrelated"]);
    assert!(!readers_by_step.contains_key("a"));
    assert!(!readers_by_step.contains_key("b"));

    // An unresolved scheduler prerequisite cannot justify preservation.
    contract.graph.nodes.get_mut("unrelated").unwrap().needs = vec!["unknown".into()];
    assert_eq!(
        propagate_step_invalidation(&contract, &mut result, &readers_by_step, &aggregate_readers),
        Err(Refusal::Executor)
    );
}

#[tokio::test]
async fn steps_ancestor_presence_does_not_invalidate_an_unrelated_precise_read() {
    let f = Fixture::new().await;
    let mut contract = f.contract.clone();
    let mut c = contract.graph.nodes["b"].clone();
    c.id = "c".into();
    contract.graph.nodes.insert(c.id.clone(), c);
    let mut bag = ValueBag::default();
    bag.set_step_status("c", "success");
    TemplateService
        .render_bag_scoped(
            ReadOwner::Node("a".into()),
            "{{ steps.c.status }}",
            &bag,
            &RuntimeLimits::default(),
        )
        .unwrap();
    let reads = bag.read_trace().reads;
    assert!(reads.iter().any(|read| {
        read.path == ["steps"]
            && read.kind == ReadKind::Lookup
            && read.identity == ReadIdentity::Present
    }));
    assert!(
        reads
            .iter()
            .any(|read| { read.path == ["steps", "c", "status"] && read.kind == ReadKind::Lookup })
    );
    let mut readers_by_step = BTreeMap::new();
    let mut aggregate_readers = BTreeSet::new();
    index_step_readers("a", &reads, &mut readers_by_step, &mut aggregate_readers);
    assert!(aggregate_readers.is_empty());
    assert_eq!(readers_by_step["c"], BTreeSet::from(["a".into()]));

    let mut result = SourceDiagnostic {
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["b".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into()],
        inherited_budget: f.state.budget.clone(),
    };
    propagate_step_invalidation(&contract, &mut result, &readers_by_step, &aggregate_readers)
        .unwrap();
    assert!(result.stale_outcomes.is_empty());
    assert_eq!(result.preserved_outcomes, ["a"]);

    let mut result = SourceDiagnostic {
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["c".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into()],
        inherited_budget: f.state.budget.clone(),
    };
    propagate_step_invalidation(&contract, &mut result, &readers_by_step, &aggregate_readers)
        .unwrap();
    assert_eq!(result.stale_outcomes, ["a"]);
    assert!(result.preserved_outcomes.is_empty());
}

#[tokio::test]
async fn actual_steps_collection_read_is_invalidated_by_any_retry() {
    let f = Fixture::new().await;
    let contract = f.contract.clone();
    let mut bag = ValueBag::default();
    bag.set_step_status("c", "success");
    TemplateService
        .render_bag_scoped(
            ReadOwner::Node("a".into()),
            "{{ steps }}",
            &bag,
            &RuntimeLimits::default(),
        )
        .unwrap();
    let reads = bag.read_trace().reads;
    assert!(
        reads
            .iter()
            .any(|read| { read.path == ["steps"] && read.kind == ReadKind::Collection })
    );
    let mut readers_by_step = BTreeMap::new();
    let mut aggregate_readers = BTreeSet::new();
    index_step_readers("a", &reads, &mut readers_by_step, &mut aggregate_readers);
    assert_eq!(aggregate_readers, BTreeSet::from(["a".into()]));

    let uncertain_root = ReadDependency {
        owner: ReadOwner::Node("a".into()),
        path: vec!["steps".into()],
        kind: ReadKind::Lookup,
        identity: ReadIdentity::Missing,
    };
    let mut uncertain_aggregate = BTreeSet::new();
    index_step_readers(
        "a",
        &BTreeSet::from([uncertain_root]),
        &mut BTreeMap::new(),
        &mut uncertain_aggregate,
    );
    assert_eq!(uncertain_aggregate, BTreeSet::from(["a".into()]));

    let mut result = SourceDiagnostic {
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["b".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into()],
        inherited_budget: f.state.budget.clone(),
    };
    propagate_step_invalidation(&contract, &mut result, &readers_by_step, &aggregate_readers)
        .unwrap();
    assert_eq!(result.stale_outcomes, ["a"]);
    assert!(result.preserved_outcomes.is_empty());
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
async fn changed_and_missing_values_or_files_are_stale_without_gate_edges() {
    let mut f = Fixture::new().await;
    // Isolate captured-input currentness from scheduler-gate invalidation.
    let original_inputs = f.original_inputs().clone();
    for mut value in [json!({"private":"changed"}), json!({})] {
        value["upload"] = original_inputs["upload"].clone();
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
        .unwrap()["inputs"] = original_inputs;
    f.rewrite_source();
    assert_eq!(f.analyze().unwrap().preserved_outcomes, ["a"]);
    std::fs::write(f.workspace.join("files/upload/private.json"), b"{ }").unwrap();
    assert_eq!(f.analyze().unwrap().stale_outcomes, ["a"]);
    std::fs::remove_file(f.workspace.join("files/upload/private.json")).unwrap();
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
    let file = f.workspace.join("files/upload/private.json");
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
    let original_inputs = f.original_inputs();
    assert_eq!(
        file_matches(&unknown, &read, &f.contract, original_inputs),
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
            file_matches(&f.fs(), &read, &f.contract, original_inputs),
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
async fn workspace_file_binding_requires_exact_declared_path_and_value_digest() {
    let f = Fixture::new().await;
    let mut read = ReadDependency {
        owner: ReadOwner::Node("a".into()),
        path: vec!["files".into(), "upload".into(), "private.json".into()],
        kind: ReadKind::WorkspaceFile,
        identity: ReadIdentity::Digest(Sha256::digest(b"{}").into()),
    };
    assert_eq!(
        file_matches(&f.fs(), &read, &f.contract, f.original_inputs()),
        Ok(true)
    );
    // Both encodings bind the decoded bytes, not the serialized input value.
    let base64_inputs = json!({"upload": {"name": "private.json", "content_base64": "e30="}});
    assert_eq!(
        file_matches(&f.fs(), &read, &f.contract, &base64_inputs),
        Ok(true)
    );
    for path in [
        vec!["private.json"],
        vec!["files", "unknown", "private.json"],
        vec!["files", "upload", "other.json"],
    ] {
        let relative = path.join("/");
        let target = f.workspace.join(&relative);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, b"{}").unwrap();
        read.path = path.into_iter().map(String::from).collect();
        // Matching source-workspace bytes alone cannot preserve an arbitrary
        // read, an undeclared field, or a different name for a declared file.
        assert_eq!(
            file_matches(&f.fs(), &read, &f.contract, f.original_inputs()),
            Err(Refusal::InputIdentity)
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_file_binding_refuses_missing_invalid_ambiguous_or_over_limit_inputs() {
    let mut f = Fixture::new().await;
    let read = ReadDependency {
        owner: ReadOwner::Node("a".into()),
        path: vec!["files".into(), "upload".into(), "private.json".into()],
        kind: ReadKind::WorkspaceFile,
        identity: ReadIdentity::Digest(Sha256::digest(b"{}").into()),
    };
    for inputs in [
        json!({}),
        json!({"upload": "files/upload/private.json"}),
        json!({"upload": {"name": "../private.json", "text": "{}"}}),
        json!({"upload": {"name": "private.json", "content_base64": "invalid"}}),
        json!({"upload": {"name": "private.json", "text": "{}", "content_base64": "e30="}}),
    ] {
        assert_eq!(
            file_matches(&f.fs(), &read, &f.contract, &inputs),
            Err(Refusal::InputIdentity)
        );
    }
    let inputs = f.original_inputs().clone();
    let fields = &mut f.contract.manifest.inputs.stages[0].fields;
    let upload = fields
        .iter()
        .position(|field| field.id == "upload")
        .unwrap();
    fields[upload].kind = FieldType::Json;
    assert_eq!(
        file_matches(&f.fs(), &read, &f.contract, &inputs),
        Err(Refusal::InputIdentity)
    );
    let fields = &mut f.contract.manifest.inputs.stages[0].fields;
    fields[upload].kind = FieldType::File;
    fields.push(fields[upload].clone());
    assert_eq!(
        file_matches(&f.fs(), &read, &f.contract, &inputs),
        Err(Refusal::InputIdentity)
    );
    f.contract.manifest.inputs.stages[0].fields.pop();
    f.contract.manifest.runtime.file_input_limit_bytes = Some(1);
    assert_eq!(
        file_matches(&f.fs(), &read, &f.contract, &inputs),
        Err(Refusal::Limit)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn source_diagnostic_refuses_workspace_digest_not_bound_to_original_filevalue() {
    let mut f = Fixture::new().await;
    assert_eq!(f.analyze().unwrap().preserved_outcomes, ["a"]);
    // The trace still matches the actual workspace file. Only the admitted
    // value differs, and no executor lookup of `upload` can mask this gate.
    f.events
        .iter_mut()
        .find(|event| event["t"] == "run_started")
        .unwrap()["inputs"]["upload"]["text"] = json!("different admitted bytes");
    f.rewrite_source();
    assert_eq!(f.analyze().unwrap_err(), Refusal::InputIdentity);
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_file_requires_explicit_byte_limit() {
    let mut f = Fixture::new().await;
    f.contract.manifest.runtime.file_input_limit_bytes = None;
    assert_eq!(f.analyze().unwrap_err(), Refusal::Limit);
    // Even a missing file cannot be classified without a configured bound.
    std::fs::remove_file(f.workspace.join("files/upload/private.json")).unwrap();
    assert_eq!(f.analyze().unwrap_err(), Refusal::Limit);
}

#[tokio::test]
async fn hook_and_foreach_contracts_are_unsupported() {
    let f = Fixture::new().await;

    let mut hooked = f.contract.clone();
    hooked.manifest.hooks.run_started.push(contract::HookDef {
        id: "notice".into(),
        kind: contract::StepType::literal("test.probe"),
        on_error: contract::HookErrorPolicy::Fail,
        retry: None,
        params: Default::default(),
    });
    assert_eq!(
        supported_state(&f.state, &hooked),
        Err(Refusal::UnsupportedState)
    );

    let mut foreach = f.contract.clone();
    foreach.graph.nodes.get_mut("a").unwrap().kind = contract::StepType::literal("foreach");
    assert_eq!(
        supported_state(&f.state, &foreach),
        Err(Refusal::UnsupportedState)
    );
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
            s.historical_file_pins.insert(
                "private".into(),
                std::collections::BTreeSet::from(["digest".into()]),
            );
        },
        |s| {
            s.removed_files.insert("gone.txt".into());
        },
        |s| {
            s.nodes.insert(
                model::NodePath::root("a"),
                crate::NodeOutcome::Success {
                    output: None,
                    files: vec![crate::FilePin {
                        path: camino::Utf8PathBuf::from("out/result.json"),
                        sha256: "a".repeat(64),
                    }],
                },
            );
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
        |s| {
            s.pending_interactions.insert(
                model::NodePath::root("a"),
                crate::state::Interaction::Question {
                    question: api::FormSpec::default(),
                },
            );
        },
        |s| {
            s.interaction_seqs.insert(model::NodePath::root("a"), 1);
        },
        |s| {
            s.answers
                .insert("question".into(), json!({"answer": "yes"}));
        },
        |s| {
            s.confirmations.insert("confirmation".into(), true);
        },
        |s| {
            s.hooks_settled.insert("hook.run_started.notice".into());
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

#[cfg(unix)]
#[tokio::test]
async fn package_file_currentness_uses_exact_consumed_bytes_under_source_root() {
    let mut f = Fixture::with_package(Some("1"), true).await;
    assert!(
        f.state.dependency_traces.owners["a"]
            .trace
            .reads
            .iter()
            .any(|read| read.kind == ReadKind::PackageFile)
    );
    let plan = f.analyze().expect("unchanged package file is current");
    assert_eq!(plan.preserved_outcomes, ["a"]);
    assert_eq!(plan.retry_roots, ["b"]);
    assert!(plan.stale_outcomes.is_empty());

    // The contract hash covers the manifest, not this schema. The private
    // package-file trace is the exact dependency proof for this executor.
    let original_contract = f.contract.sha256.clone();
    std::fs::write(f.contract.root.join("schema.json"), b"{ }").unwrap();
    assert_eq!(
        Contract::load(&f.contract.root).unwrap().sha256,
        original_contract
    );
    let plan = f.analyze().expect("changed file is a known stale input");
    assert_eq!(plan.stale_outcomes, ["a"]);
    assert_eq!(plan.retry_roots, ["b"]);

    // Workspace and package inputs are independent; an unchanged schema does
    // not hide a changed workspace source.
    std::fs::write(f.contract.root.join("schema.json"), b"{}").unwrap();
    std::fs::write(
        f.workspace.join("files/upload/private.json"),
        b"changed workspace",
    )
    .unwrap();
    let plan = f.analyze().expect("changed workspace file is stale");
    assert_eq!(plan.stale_outcomes, ["a"]);

    std::fs::write(f.workspace.join("files/upload/private.json"), b"{}").unwrap();
    std::fs::remove_file(f.contract.root.join("schema.json")).unwrap();
    assert_eq!(f.analyze().unwrap().stale_outcomes, ["a"]);

    // A symlink substitution is not treated as a content match, even when it
    // points at bytes equal to the captured schema.
    let schema = f.contract.root.join("schema.json");
    let target = f.contract.root.join("schema-target.json");
    std::fs::write(&target, b"{}").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &schema).unwrap();
    assert_eq!(f.analyze().unwrap_err(), Refusal::InputIo);

    f.contract.root = f.workspace.clone();
    assert_eq!(f.analyze().unwrap_err(), Refusal::SourceBinding);
}
