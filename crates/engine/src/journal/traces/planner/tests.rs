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
            // This executor reads one exact snapshot path and records it,
            // including absence. ValueBag::get_path intentionally adds a whole
            // steps collection read when canonical IDs are not yet known.
            let snapshot = ctx.vars.template_snapshot();
            ctx.vars
                .read_recorder()
                .for_owner(ReadOwner::Node(node.id.clone()))
                .record(
                    &["steps".into(), "b".into(), "output".into()],
                    ReadKind::Lookup,
                    ReadIdentity::of(snapshot.pointer("/steps/b/output")),
                );
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

fn probe_options(output_dir: Utf8PathBuf) -> RunOptions {
    RunOptions {
        elapsed_budget_mode: policy::ElapsedBudgetMode::ActiveExecution,
        output_dir,
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
    }
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
            manifest = format!("parallel = [\"a\", \"b\"]\n{replaced}");
        }
        std::fs::write(package_root.join(contract::MANIFEST_FILE), manifest).unwrap();
        let contract = Contract::load(&package_root).unwrap();
        let metadata = run_dir.join("meta");
        let workspace = run_dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let options = probe_options(workspace.clone());
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
    fn analyze(&self) -> Result<ResetPlanCandidate, Refusal> {
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
        serde_json::to_value(&plan).unwrap()["source_run_id"],
        "source-probe"
    );
    assert_eq!(
        serde_json::to_value(&plan).unwrap()["source_last_seq"],
        f.state.last_seq
    );
    assert_eq!(
        plan.reset_nodes,
        BTreeMap::from([(
            "b".into(),
            ResetNode {
                output_alias: "b".into(),
                predecessors: BTreeSet::new(),
            }
        )])
    );
    assert_eq!(
        serde_json::to_value(&plan.inherited_budget).unwrap(),
        serde_json::to_value(&f.state.budget).unwrap()
    );
    assert_eq!(plan.identity, f.analyze().unwrap().identity);
    assert_eq!(
        format!("{:?}", plan.identity),
        "DiagnosticIdentity([redacted])"
    );
    assert_eq!(
        format!("{plan:?}"),
        "ResetPlanCandidate { retry_roots: 1, stale_outcomes: 0, preserved_outcomes: 1, .. }"
    );
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

#[cfg(unix)]
#[tokio::test]
async fn identity_binds_observed_bytes_and_missing_state_not_just_plan_sets() {
    let f = Fixture::with_package(Some("1"), true).await;
    let original = f.analyze().unwrap().identity;
    assert_eq!(original, f.analyze().unwrap().identity);
    // Exercise both no-follow file readers. Every mutation after the first
    // leaves the retry/stale/preserved sets unchanged, including empty versus
    // missing. Hashing only currentness booleans would miss these changes.
    for path in [
        f.workspace.join("files/upload/private.json"),
        f.contract.root.join("schema.json"),
    ] {
        let mut identities = Vec::new();
        for bytes in [Some(b"{ }".as_slice()), Some(b"{  }"), Some(b""), None] {
            if let Some(bytes) = bytes {
                std::fs::write(&path, bytes).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
            let plan = f.analyze().unwrap();
            assert_eq!(plan.retry_roots, ["b"]);
            assert_eq!(plan.stale_outcomes, ["a"]);
            assert!(plan.preserved_outcomes.is_empty());
            assert_ne!(plan.identity, original);
            assert!(identities.iter().all(|previous| previous != &plan.identity));
            assert_eq!(plan.identity, f.analyze().unwrap().identity);
            identities.push(plan.identity);
        }
        std::fs::write(&path, b"{}").unwrap();
        assert_eq!(original, f.analyze().unwrap().identity);
    }
}

#[tokio::test]
async fn identity_changes_with_validated_source_inputs_or_refuses_unbound_source() {
    let mut f = Fixture::new().await;
    let original = f.analyze().unwrap().identity;
    let inputs = f.original_inputs().clone();
    let start = f
        .events
        .iter()
        .position(|event| event["t"] == "run_started")
        .unwrap();
    f.events[start]["inputs"]["private"] = json!("first changed value");
    // A changed caller view without the same persisted source is not accepted.
    assert_eq!(f.analyze().unwrap_err(), Refusal::SourceBinding);
    f.rewrite_source();
    let first = f.analyze().unwrap();
    assert_ne!(first.identity, original);
    f.events[start]["inputs"]["private"] = json!("second changed value");
    f.rewrite_source();
    let second = f.analyze().unwrap();
    assert_eq!(first.retry_roots, second.retry_roots);
    assert_eq!(first.stale_outcomes, second.stale_outcomes);
    assert_eq!(first.preserved_outcomes, second.preserved_outcomes);
    assert_ne!(first.identity, second.identity);
    f.events[start]["inputs"] = inputs;
    f.rewrite_source();
    assert_eq!(original, f.analyze().unwrap().identity);
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
    assert_eq!(
        serde_json::to_value(&plan.reset_nodes).unwrap(),
        json!({
            "a": {"output_alias": "a_result", "predecessors": ["b"]},
            "b": {"output_alias": "b", "predecessors": []},
        })
    );
    let payload_identity = |candidate: &ResetPlanCandidate| {
        let mut hash = Sha256::new();
        bind_candidate_payload(&mut hash, candidate, None).unwrap();
        hash.finalize()
    };
    let original = payload_identity(&plan);
    for field in ["alias", "gates", "source", "seq"] {
        let mut changed = plan.clone();
        match field {
            "alias" => changed.reset_nodes.get_mut("a").unwrap().output_alias = "other".into(),
            "gates" => changed
                .reset_nodes
                .get_mut("a")
                .unwrap()
                .predecessors
                .clear(),
            "source" => changed.source_run_id.push_str("-other"),
            _ => changed.source_last_seq += 1,
        }
        assert_ne!(original, payload_identity(&changed), "{field}");
    }
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
    let mut result = ResetPlanCandidate {
        identity: DiagnosticIdentity([0; 32]),
        required_files: Some(BTreeMap::new()),
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["b".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into(), "c".into()],
        inherited_budget: f.state.budget.clone(),
        reset_nodes: BTreeMap::new(),
        expected_executor_attestations: BTreeMap::new(),
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
    let mut result = ResetPlanCandidate {
        identity: DiagnosticIdentity([0; 32]),
        required_files: Some(BTreeMap::new()),
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
        reset_nodes: BTreeMap::new(),
        expected_executor_attestations: BTreeMap::new(),
    };
    // First isolate the already-stale seed, then the failed retry root.
    // Either prerequisite changing must invalidate C without a captured read.
    for (retry_roots, stale_outcomes, preserved_a) in [
        (vec![], vec!["a".into()], false),
        (vec!["b".into()], vec![], true),
    ] {
        let mut seeded = ResetPlanCandidate {
            identity: DiagnosticIdentity([0; 32]),
            required_files: Some(BTreeMap::new()),
            retry_roots,
            stale_outcomes,
            preserved_outcomes: result.preserved_outcomes.clone(),
            source_run_id: result.source_run_id.clone(),
            source_contract_sha256: result.source_contract_sha256.clone(),
            source_last_seq: result.source_last_seq,
            inherited_budget: result.inherited_budget.clone(),
            reset_nodes: BTreeMap::new(),
            expected_executor_attestations: BTreeMap::new(),
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

    let mut result = ResetPlanCandidate {
        identity: DiagnosticIdentity([0; 32]),
        required_files: Some(BTreeMap::new()),
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["b".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into()],
        inherited_budget: f.state.budget.clone(),
        reset_nodes: BTreeMap::new(),
        expected_executor_attestations: BTreeMap::new(),
    };
    propagate_step_invalidation(&contract, &mut result, &readers_by_step, &aggregate_readers)
        .unwrap();
    assert!(result.stale_outcomes.is_empty());
    assert_eq!(result.preserved_outcomes, ["a"]);

    let mut result = ResetPlanCandidate {
        identity: DiagnosticIdentity([0; 32]),
        required_files: Some(BTreeMap::new()),
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["c".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into()],
        inherited_budget: f.state.budget.clone(),
        reset_nodes: BTreeMap::new(),
        expected_executor_attestations: BTreeMap::new(),
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

    let mut result = ResetPlanCandidate {
        identity: DiagnosticIdentity([0; 32]),
        required_files: Some(BTreeMap::new()),
        source_run_id: "synthetic".into(),
        source_contract_sha256: contract.sha256.clone(),
        source_last_seq: 0,
        retry_roots: vec!["b".into()],
        stale_outcomes: Vec::new(),
        preserved_outcomes: vec!["a".into()],
        inherited_budget: f.state.budget.clone(),
        reset_nodes: BTreeMap::new(),
        expected_executor_attestations: BTreeMap::new(),
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
        workspace_observation(&unknown, &read, &f.contract, original_inputs),
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
            workspace_observation(&f.fs(), &read, &f.contract, original_inputs),
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
        workspace_observation(&f.fs(), &read, &f.contract, f.original_inputs()),
        Ok(Some(Sha256::digest(b"{}").into()))
    );
    // Both encodings bind the decoded bytes, not the serialized input value.
    let base64_inputs = json!({"upload": {"name": "private.json", "content_base64": "e30="}});
    assert_eq!(
        workspace_observation(&f.fs(), &read, &f.contract, &base64_inputs),
        Ok(Some(Sha256::digest(b"{}").into()))
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
            workspace_observation(&f.fs(), &read, &f.contract, f.original_inputs()),
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
            workspace_observation(&f.fs(), &read, &f.contract, &inputs),
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
        workspace_observation(&f.fs(), &read, &f.contract, &inputs),
        Err(Refusal::InputIdentity)
    );
    let fields = &mut f.contract.manifest.inputs.stages[0].fields;
    fields[upload].kind = FieldType::File;
    fields.push(fields[upload].clone());
    assert_eq!(
        workspace_observation(&f.fs(), &read, &f.contract, &inputs),
        Err(Refusal::InputIdentity)
    );
    f.contract.manifest.inputs.stages[0].fields.pop();
    f.contract.manifest.runtime.file_input_limit_bytes = Some(1);
    assert_eq!(
        workspace_observation(&f.fs(), &read, &f.contract, &inputs),
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
async fn reset_file_requirements_bind_present_reads_without_publishing_paths() {
    let f = Fixture::with_package(Some("1"), true).await;
    let candidate = f.analyze().unwrap();
    let requirements = |candidate: &ResetPlanCandidate| {
        candidate
            .required_file_observations()
            .unwrap()
            .map(|(path, sha256)| (path.to_owned(), sha256.map(str::to_owned)))
            .collect::<BTreeMap<_, _>>()
    };
    let expected = BTreeMap::from([
        (
            Utf8PathBuf::from("generator-package/schema.json"),
            Some(hex::encode(Sha256::digest(b"{}"))),
        ),
        (
            Utf8PathBuf::from("workspace/files/upload/private.json"),
            Some(hex::encode(Sha256::digest(b"{}"))),
        ),
    ]);
    // Both outcomes consumed both files: identical observations are deduplicated.
    assert_eq!(requirements(&candidate), expected);
    assert_eq!(requirements(&candidate.clone()), expected);
    let wire = serde_json::to_string(&candidate).unwrap();
    let debug = format!("{candidate:?}");
    let overlay = reset_child_events(&f, &candidate)
        .pop()
        .unwrap()
        .to_string();
    for text in [&wire, &debug, &overlay] {
        assert!(!text.contains("required_files"));
        for path in expected.keys() {
            assert!(!text.contains(path.as_str()));
        }
    }

    // Retain the observed current bytes, even when different from the captured
    // read and therefore stale. Failed roots' reads are requirements as well.
    for relative in expected.keys() {
        let path = f.path.parent().unwrap().parent().unwrap().join(relative);
        std::fs::write(&path, b"changed").unwrap();
        let changed = f.analyze().unwrap();
        assert_eq!(
            requirements(&changed)[relative],
            Some(hex::encode(Sha256::digest(b"changed")))
        );
        assert_ne!(changed.identity, candidate.identity);
        assert_eq!(changed.stale_outcomes, ["a"]);
        std::fs::remove_file(&path).unwrap();
        let missing = f.analyze().unwrap();
        let missing_requirements = requirements(&missing);
        assert_eq!(missing_requirements[relative], None);
        let read_kind = if relative.starts_with("generator-package") {
            ReadKind::PackageFile
        } else {
            ReadKind::WorkspaceFile
        };
        let read = f.state.dependency_traces.owners["b"]
            .trace
            .reads
            .iter()
            .find(|read| read.kind == read_kind)
            .unwrap();
        let mut missing = missing;
        missing.require_file(read, None).unwrap();
        assert_eq!(
            missing.require_file(read, Some(Sha256::digest(b"{}").into())),
            Err(Refusal::InputIdentity)
        );
        assert_eq!(requirements(&missing), missing_requirements);
        let mut present = candidate.clone();
        assert_eq!(
            present.require_file(read, None),
            Err(Refusal::InputIdentity)
        );
        assert_eq!(requirements(&present), expected);
        for text in [
            serde_json::to_string(&missing).unwrap(),
            format!("{missing:?}"),
        ] {
            assert!(!text.contains("required_files"));
            assert!(!text.contains(relative.as_str()));
        }
        assert_eq!(missing.stale_outcomes, ["a"]);
        std::fs::write(&path, b"{}").unwrap();
    }

    // Bounded deterministic reproduction of two reads observing different
    // bytes for the same source path; refusal must not overwrite the first.
    let mut conflicting = candidate.clone();
    let read = f.state.dependency_traces.owners["b"]
        .trace
        .reads
        .iter()
        .find(|read| read.kind == ReadKind::PackageFile)
        .unwrap();
    conflicting
        .require_file(read, Some(Sha256::digest(b"{}").into()))
        .unwrap();
    assert_eq!(
        conflicting.require_file(read, Some(Sha256::digest(b"different").into())),
        Err(Refusal::InputIdentity)
    );
    assert_eq!(requirements(&conflicting), expected);
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

#[tokio::test]
async fn reset_schedule_resolves_exact_captured_names_and_refuses_uncertain_edges() {
    let f = Fixture::new().await;
    let mut contract = f.contract.clone();
    for id in ["c", "preserved"] {
        let mut node = contract.graph.nodes["b"].clone();
        node.id = id.into();
        contract.graph.nodes.insert(id.into(), node);
    }
    contract.graph.nodes.get_mut("b").unwrap().output = Some("b_result".into());
    // Scheduler needs are not evidence of value reads.
    contract.graph.nodes.get_mut("b").unwrap().needs = vec!["a".into()];
    let read = |owner: &str, path: &[&str], kind, identity| ReadDependency {
        owner: ReadOwner::Node(owner.into()),
        path: path.iter().map(|part| (*part).to_owned()).collect(),
        kind,
        identity,
    };
    let missing = |owner: &str, name: &str, field: &str| {
        read(
            owner,
            &["steps", name, field],
            ReadKind::Lookup,
            ReadIdentity::Missing,
        )
    };
    let owners = ["c", "a", "b", "a"].map(String::from);
    let mut reads = BTreeMap::from([
        (
            "a",
            BTreeSet::from([
                missing("a", "c", "reason"),
                missing("a", "b_result", "output"),
                read(
                    "a",
                    &["steps", "b", "status"],
                    ReadKind::Lookup,
                    ReadIdentity::of(Some(&json!("failed"))),
                ),
                missing("a", "preserved", "output"),
                read("a", &["steps"], ReadKind::Lookup, ReadIdentity::Present),
            ]),
        ),
        ("b", BTreeSet::new()),
        ("c", BTreeSet::from([missing("c", "b", "status")])),
    ]);
    let schedule = |contract: &Contract, reads: &BTreeMap<&str, BTreeSet<ReadDependency>>| {
        reset_schedule(contract, owners.iter(), |owner| reads.get(owner))
    };
    let reset = schedule(&contract, &reads).unwrap();
    assert_eq!(
        serde_json::to_value(&reset).unwrap(),
        json!({
            "a": {"output_alias": "a", "predecessors": ["b", "c"]},
            "b": {"output_alias": "b_result", "predecessors": []},
            "c": {"output_alias": "c", "predecessors": ["b"]},
        })
    );
    assert!(
        reset_schedule(&contract, [].iter(), |_| None)
            .unwrap()
            .is_empty()
    );
    for name in ["unknown", "a", "b"] {
        let saved = reads.insert("b", BTreeSet::from([missing("b", name, "output")]));
        assert_eq!(
            schedule(&contract, &reads),
            Err(Refusal::ResetDependency),
            "{name}"
        );
        reads.insert("b", saved.unwrap());
    }
    // Both duplicate output aliases and alias/node-ID collisions refuse.
    // Without the collision, a's existing read is acyclic and succeeds.
    for alias in ["b_result", "b"] {
        let mut ambiguous = contract.clone();
        ambiguous.graph.nodes.get_mut("c").unwrap().output = Some(alias.into());
        assert_eq!(schedule(&ambiguous, &reads), Err(Refusal::ResetDependency));
    }
    // A reset node's own output key must be unique even when no trace reads
    // it: the future overlay clears that key and must not erase a preserved node.
    let no_reads = BTreeMap::from([
        ("a", BTreeSet::new()),
        ("b", BTreeSet::new()),
        ("c", BTreeSet::new()),
    ]);
    let mut ambiguous_output_alias = contract.clone();
    ambiguous_output_alias
        .graph
        .nodes
        .get_mut("a")
        .unwrap()
        .output = Some("shared".into());
    ambiguous_output_alias
        .graph
        .nodes
        .get_mut("preserved")
        .unwrap()
        .output = Some("shared".into());
    assert_eq!(
        schedule(&ambiguous_output_alias, &no_reads),
        Err(Refusal::ResetDependency)
    );
    let mut ambiguous_node_id = contract.clone();
    ambiguous_node_id.graph.nodes.get_mut("a").unwrap().output = Some("preserved".into());
    assert_eq!(
        schedule(&ambiguous_node_id, &no_reads),
        Err(Refusal::ResetDependency)
    );
    // Consumed collections/roots do not retain per-entry provenance. Refuse
    // them directly instead of guessing edges or building a dense reset graph.
    // An ancestor Present lookup is not consumed and remains a precise no-edge.
    let reset_ids = reset.keys().cloned().collect();
    for path in [vec!["steps"], vec![]] {
        for kind in [ReadKind::Collection, ReadKind::Lookup] {
            let aggregate =
                BTreeSet::from([read("a", &path, kind, ReadIdentity::of(Some(&json!({}))))]);
            assert_eq!(
                reset_predecessors(&aggregate, &BTreeMap::new(), &reset_ids),
                Err(Refusal::ResetDependency)
            );
            reads.insert("a", aggregate);
            assert_eq!(schedule(&contract, &reads), Err(Refusal::ResetDependency));
        }
    }
}

/// Mirrors the service's typed candidate + sorted manifest identity, without
/// making the candidate decodable or adding a reset caller to the runtime.
fn reset_child_events(f: &Fixture, candidate: &ResetPlanCandidate) -> Vec<Value> {
    #[derive(Serialize)]
    struct File {
        path: String,
        length: u64,
        sha256: String,
    }
    let files = vec![File {
        path: "meta/journal.jsonl".into(),
        length: 42,
        sha256: "a".repeat(64),
    }];
    let candidate_wire = serde_json::to_value(candidate).unwrap();
    let binding = json!({"binding_version": 1, "plan_digest": hex::encode(Sha256::digest(
        serde_json::to_vec(&("qcg-prepared-reset-candidate-v1", candidate, &files)).unwrap()
    )), "reset_candidate_sha256": hex::encode(Sha256::digest(
        serde_json::to_vec(&candidate_wire).unwrap()
    ))});
    let payload = candidate.fork_overlay_evidence(binding.clone()).unwrap();
    let mut events = f
        .events
        .iter()
        .filter(|e| {
            !matches!(
                e["t"].as_str(),
                Some(
                    "run_error"
                        | "run_finished"
                        | "run_canceled"
                        | "run_interrupted"
                        | "run_waiting"
                )
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    for event in &mut events {
        event["run_id"] = json!("child");
    }
    let seq = f.state.last_seq + 1;
    events.push(
        json!({"t": "run_forked", "seq": seq, "ts": "2026-01-01T00:00:00Z",
            "run_id": "child", "trace_id": "trace", "span_id": "span",
            "source_run_id": candidate.source_run_id, "source_seq": candidate.source_last_seq,
            "source_contract_sha256": candidate.source_contract_sha256,
            "binding": binding, "reset_overlay_sha256": overlay::digest(&payload).unwrap(),
        }),
    );
    events.push(
        json!({"t": "child_reset_overlay", "seq": seq + 1, "ts": "2026-01-01T00:00:00Z",
            "run_id": "child", "trace_id": "trace", "span_id": "span", "overlay": payload,
        }),
    );
    events
}

fn append_child_queue(f: &Fixture, events: &mut Vec<Value>) {
    let mut queued = f
        .events
        .iter()
        .find(|event| event["t"] == "run_started")
        .unwrap()
        .clone();
    queued["t"] = json!("run_queued");
    queued["seq"] = json!(events.last().unwrap()["seq"].as_u64().unwrap() + 1);
    queued["run_id"] = json!("child");
    queued["parent_run_id"] = json!("source");
    queued
        .as_object_mut()
        .unwrap()
        .remove("step_budget_baseline");
    events.push(queued);
}

#[tokio::test]
async fn child_reset_overlay_refuses_inherited_budget_mismatch() {
    let f = Fixture::new().await;
    let candidate = f.analyze().unwrap();
    let events = reset_child_events(&f, &candidate);
    let mut state = RunState::fold_values(&events[..events.len() - 2]).unwrap();
    state.budget.cost_microusd = state.budget.cost_microusd.saturating_add(1);
    state.apply(&events[events.len() - 2]).unwrap();
    assert!(state.apply(events.last().unwrap()).is_err());
}

#[tokio::test]
async fn child_reset_overlay_clears_only_exact_projection_and_suppresses_replay() {
    for stale in [false, true] {
        let f = if stale {
            Fixture::with_output_dependency().await
        } else {
            Fixture::new().await
        };
        let candidate = f.analyze().unwrap();
        let events = reset_child_events(&f, &candidate);
        let overlay = events.last().unwrap();
        let before_source = std::fs::read(&f.path).unwrap();
        let mut state = RunState::fold_values(&events[..events.len() - 2]).unwrap();
        // Explicitly stress liabilities and history: the projection has no
        // authority over any of these, regardless of their amounts.
        state.dependency_traces = f.state.dependency_traces.clone();
        state
            .reset_gates
            .insert("obsolete_gate".into(), BTreeSet::from(["old".into()]));
        state.vars.set_step_output("b", json!("old root output"));
        state
            .vars
            .set_step_output("a", json!("old canonical output"));
        state.vars.set_step_output("unrelated", json!("retained"));
        state.vars.set_step_outcome("a/child", "success", None);
        state
            .vars
            .set_step_outcome("a", "success", Some(json!("old reason")));
        state
            .reset_expected_attestations
            .insert("obsolete_owner".into(), [0; 32]);
        let before = serde_json::to_value(&state).unwrap();
        let traces = format!("{:?}", state.dependency_traces);
        state.apply(&events[events.len() - 2]).unwrap();
        state.apply(overlay).unwrap();
        assert!(!state.reset_gates.contains_key("obsolete_gate"));
        assert_eq!(
            state.reset_expected_attestations,
            candidate.expected_executor_attestations
        );
        let after = serde_json::to_value(&state).unwrap();
        for (key, value) in before.as_object().unwrap() {
            if !["nodes", "vars", "last_seq"].contains(&key.as_str()) {
                assert_eq!(&after[key], value, "preserve {key}");
            }
        }
        assert_eq!(format!("{:?}", state.dependency_traces), traces);
        assert_eq!(
            state.vars.template_snapshot()["steps"]["unrelated"]["output"],
            "retained"
        );
        assert_eq!(
            state.vars.template_snapshot()["steps"]["a/child"]["status"],
            "success"
        );
        assert!(!state.nodes.contains_key(&model::NodePath::root("b")));
        assert!(state.vars.template_snapshot()["steps"].get("b").is_none());
        if stale {
            assert!(state.vars.template_snapshot()["steps"].get("a").is_none());
            assert!(
                state.vars.template_snapshot()["steps"]
                    .get("a_result")
                    .is_none()
            );
            assert!(state.nodes.is_empty());
        } else {
            assert_eq!(
                state.vars.template_snapshot()["steps"]["a"]["output"],
                "old canonical output"
            );
            assert_eq!(
                state.vars.template_snapshot()["steps"]["a"]["reason"],
                "old reason"
            );
        }
        let replay = crate::engine::JournalReplay::from_state(state);
        assert!(!replay.steps.contains_key("b"));
        assert_eq!(replay.steps.contains_key("a"), !stale);
        assert_eq!(std::fs::read(&f.path).unwrap(), before_source);
        // Folding, persistence and recovery derive the same gates/removals.
        let folded = RunState::fold_admission_values(&events).unwrap();
        assert_eq!(folded.reset_gates, replay.state.reset_gates);
    }
}

#[tokio::test]
async fn child_reset_overlay_preserves_nonzero_billing_and_elapsed_liabilities() {
    // Keep the source-bound integration above authoritative for eligibility and
    // binding. This synthetic state only exercises the overlay's narrow
    // projection against meaningful accounting values, including liabilities
    // that make a real source ineligible for continuation.
    let f = Fixture::new().await;
    let mut candidate = f.analyze().unwrap();
    candidate.inherited_budget.llm_calls = 2;
    candidate.inherited_budget.tokens_input = 17;
    candidate.inherited_budget.tokens_output = 9;
    candidate.inherited_budget.tokens_cached_input = 4;
    candidate.inherited_budget.tokens_cache_creation_input = 3;
    candidate.inherited_budget.cost_microusd = 125;
    candidate.inherited_budget.unresolved_llm_cost = true;
    candidate.inherited_budget.unresolved_search_cost = true;
    candidate.inherited_budget.elapsed = Some(api::ElapsedAccountedEventData {
        active_nanoseconds: 987_654_321,
        active_started_at: None,
    });

    let events = reset_child_events(&f, &candidate);
    let mut state = RunState::fold_values(&events[..events.len() - 2]).unwrap();
    // Bind this synthetic projection to the same candidate budget; no source
    // analysis or continuation eligibility is inferred from these values.
    state.budget = candidate.inherited_budget.clone();
    state.apply(&events[events.len() - 2]).unwrap();
    let before = serde_json::to_value(&state).unwrap();
    state.apply(events.last().unwrap()).unwrap();
    let after = serde_json::to_value(&state).unwrap();

    assert_eq!(after["budget"], before["budget"]);
    for (key, value) in before.as_object().unwrap() {
        if !["nodes", "vars", "last_seq"].contains(&key.as_str()) {
            assert_eq!(&after[key], value, "preserve {key}");
        }
    }
    assert!(state.budget.steps_executed > 0);
    assert!(state.budget.budget_charged > 0);
    assert_eq!(state.budget.llm_calls, 2);
    assert_eq!(state.budget.tokens_input, 17);
    assert_eq!(state.budget.tokens_output, 9);
    assert_eq!(state.budget.tokens_cached_input, 4);
    assert_eq!(state.budget.tokens_cache_creation_input, 3);
    assert_eq!(state.budget.cost_microusd, 125);
    assert!(state.budget.unresolved_llm_cost);
    assert!(state.budget.unresolved_search_cost);
    assert_eq!(
        state.budget.elapsed.as_ref().unwrap().active_nanoseconds,
        987_654_321
    );
}

#[tokio::test]
async fn child_reset_overlay_refuses_missing_mismatched_duplicate_or_out_of_window_evidence() {
    let f = Fixture::with_output_dependency().await;
    let candidate = f.analyze().unwrap();
    let events = reset_child_events(&f, &candidate);
    assert!(RunState::fold_admission_values(&events).is_ok());
    assert!(RunState::fold_values(&events).is_err());
    assert!(RunState::fold_values(&events[..events.len() - 1]).is_err());
    let fork_index = events.len() - 2;
    let overlay_index = events.len() - 1;
    for (index, pointer, value) in [
        (fork_index, "/source_run_id", json!("foreign")),
        (fork_index, "/source_seq", json!(0)),
        (fork_index, "/source_contract_sha256", json!("b".repeat(64))),
        (fork_index, "/binding/plan_digest", json!("b".repeat(64))),
        (fork_index, "/reset_overlay_sha256", Value::Null),
        (overlay_index, "/run_id", json!("foreign")),
        (overlay_index, "/overlay/version", json!(2)),
        (overlay_index, "/overlay/version", json!(3)),
        (
            overlay_index,
            "/overlay/candidate/source_last_seq",
            json!(0),
        ),
        (
            overlay_index,
            "/overlay/candidate/reset_nodes/a/output_alias",
            json!("other"),
        ),
        (
            overlay_index,
            "/overlay/candidate/reset_nodes/a/predecessors",
            json!([]),
        ),
        (
            overlay_index,
            "/overlay/binding/reset_candidate_sha256",
            json!("c".repeat(64)),
        ),
        (overlay_index, "/overlay/binding/binding_version", json!(2)),
    ] {
        let mut changed = events.clone();
        *changed[index].pointer_mut(pointer).unwrap() = value;
        assert!(
            RunState::fold_admission_values(&changed).is_err(),
            "{pointer}"
        );
    }
    for key in ["candidate", "binding", "version"] {
        let mut malformed = events.clone();
        malformed[overlay_index]["overlay"]
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(
            RunState::fold_admission_values(&malformed).is_err(),
            "missing {key}"
        );
    }
    let mut extra_field = events.clone();
    extra_field[overlay_index]["overlay"]["unexpected"] = json!(true);
    extra_field[fork_index]["reset_overlay_sha256"] =
        json!(overlay::digest(&extra_field[overlay_index]["overlay"]).unwrap());
    assert!(RunState::fold_admission_values(&extra_field).is_err());
    let mut missing_binding = events.clone();
    missing_binding[fork_index]
        .as_object_mut()
        .unwrap()
        .remove("binding");
    assert!(RunState::fold_admission_values(&missing_binding).is_err());
    let mut no_marker = events.clone();
    no_marker[fork_index]
        .as_object_mut()
        .unwrap()
        .remove("reset_overlay_sha256");
    assert!(RunState::fold_admission_values(&no_marker).is_err());
    let mut duplicate = events.clone();
    let mut extra = events[overlay_index].clone();
    extra["seq"] = json!(f.state.last_seq + 3);
    duplicate.push(extra);
    assert!(RunState::fold_admission_values(&duplicate).is_err());
    let mut outside = events.clone();
    outside[overlay_index]["seq"] = json!(f.state.last_seq + 3);
    outside.insert(
        overlay_index,
        json!({"t": "audit_degraded", "seq": f.state.last_seq+2,
        "ts": "2026-01-01T00:00:00Z", "run_id": "child", "trace_id": "trace", "span_id": "span"}),
    );
    assert!(RunState::fold_admission_values(&outside).is_err());
    let mut reintroduced = events.clone();
    append_child_queue(&f, &mut reintroduced);
    let queue_seq = reintroduced.last().unwrap()["seq"].as_u64().unwrap();
    let patch = json!({"t": "state_patched", "seq": queue_seq + 1,
        "ts": "2026-01-01T00:00:00Z", "run_id": "child", "trace_id": "trace", "span_id": "span",
        "step_outputs": {"a_result": "stale"}});
    let mut state = RunState::fold_values(&reintroduced).unwrap();
    let before = state.vars.template_snapshot();
    assert!(state.apply(&patch).is_err());
    assert_eq!(state.vars.template_snapshot(), before);
    reintroduced.push(patch);
    assert!(RunState::fold_values(&reintroduced).is_err());
}

#[tokio::test]
async fn child_reset_overlay_refuses_contract_mismatch_between_child_state_and_candidate() {
    let f = Fixture::new().await;
    let genuine = f.analyze().unwrap();
    // A candidate claiming a different source contract, with internally
    // consistent binding/overlay regenerated by the helper, must still be
    // refused at adoption: the child's own folded contract (from run_started)
    // is the independent re-attestation point. This isolates the distinct
    // `state.contract_sha256 != candidate.source_contract_sha256` guard.
    let mut forged = genuine.clone();
    forged.source_contract_sha256 = "b".repeat(64);
    let events = reset_child_events(&f, &forged);
    let mut state = RunState::fold_values(&events[..events.len() - 2]).unwrap();
    let fork = &events[events.len() - 2];
    assert_eq!(
        fork["source_contract_sha256"],
        json!(forged.source_contract_sha256)
    );
    state.apply(fork).unwrap();
    assert_ne!(
        state.contract_sha256.as_deref(),
        Some(forged.source_contract_sha256.as_str())
    );
    assert!(state.apply(events.last().unwrap()).is_err());

    // The genuine candidate's compatible claim still folds to completion.
    let genuine_events = reset_child_events(&f, &genuine);
    assert!(RunState::fold_admission_values(&genuine_events).is_ok());
}

#[tokio::test]
async fn child_reset_overlay_refuses_state_patches_without_changing_ordinary_forks() {
    let f = Fixture::with_output_dependency().await;
    let candidate = f.analyze().unwrap();
    let events = reset_child_events(&f, &candidate);
    let fork_index = events.len() - 2;
    let overlay_index = events.len() - 1;
    RunState::fold_admission_values(&events).unwrap();
    let mut queued = events.clone();
    append_child_queue(&f, &mut queued);
    RunState::fold_values(&queued).unwrap();

    for payload in [
        json!({"inputs": {"changed": "new input"}}),
        json!({"step_outputs": {"unrelated": "new output"}}),
        json!({"step_statuses": {"unrelated": "success"}}),
        // Even an empty forged event is not part of reset admission: the
        // service represents an empty patch by emitting no event at all.
        json!({}),
    ] {
        let mut patch = json!({"t": "state_patched", "seq": f.state.last_seq + 2,
            "ts": "2026-01-01T00:00:00Z", "run_id": "child",
            "trace_id": "trace", "span_id": "span"});
        patch
            .as_object_mut()
            .unwrap()
            .extend(payload.as_object().unwrap().clone());
        let mut state = RunState::fold_admission_values(&events[..overlay_index]).unwrap();
        let before_vars = state.vars.template_snapshot();
        let before_inputs = state.inputs.clone();
        assert!(state.apply(&patch).is_err(), "{payload}");
        assert_eq!(state.vars.template_snapshot(), before_vars);
        assert_eq!(state.inputs, before_inputs);

        let mut patched = events.clone();
        patched[overlay_index]["seq"] = json!(f.state.last_seq + 3);
        patched.insert(overlay_index, patch.clone());
        assert!(
            RunState::fold_admission_values(&patched).is_err(),
            "{payload}"
        );
        append_child_queue(&f, &mut patched);
        assert!(RunState::fold_values(&patched).is_err(), "{payload}");

        // The same patch is still accepted by an ordinary fork, with no
        // reset binding or overlay to constrain its admission.
        let mut ordinary = events[..overlay_index].to_vec();
        let fork = ordinary[fork_index].as_object_mut().unwrap();
        fork.remove("binding");
        fork.remove("reset_overlay_sha256");
        ordinary.push(patch);
        let state = RunState::fold_admission_values(&ordinary).unwrap();
        let snapshot = state.vars.template_snapshot();
        if payload.get("inputs").is_some() {
            assert_eq!(state.inputs.as_ref().unwrap()["changed"], "new input");
        }
        if payload.get("step_outputs").is_some() {
            assert_eq!(snapshot["steps"]["unrelated"]["output"], "new output");
        }
        if payload.get("step_statuses").is_some() {
            assert_eq!(snapshot["steps"]["unrelated"]["status"], "success");
        }
        append_child_queue(&f, &mut ordinary);
        RunState::fold_values(&ordinary).unwrap();
    }
}

#[tokio::test]
async fn child_reset_overlay_captured_gate_requires_all_settled_targets() {
    use contract::NodeState;
    let f = Fixture::with_output_dependency().await;
    let events = reset_child_events(&f, &f.analyze().unwrap());
    let mut state = RunState::fold_admission_values(&events).unwrap();
    assert_eq!(state.reset_gates["a"], BTreeSet::from(["b".into()]));
    let mut states = BTreeMap::from([("a".into(), NodeState::Pending)]);
    assert!(!overlay::settled(&state.reset_gates, "a", &states));
    states.insert("b".into(), NodeState::Pending);
    assert!(!overlay::settled(&state.reset_gates, "a", &states));
    states.insert("b".into(), NodeState::Running);
    assert!(!overlay::settled(&state.reset_gates, "a", &states));
    states.insert("b".into(), NodeState::Success);
    assert!(overlay::settled(&state.reset_gates, "a", &states));
    state
        .reset_gates
        .get_mut("a")
        .unwrap()
        .insert("missing".into());
    assert!(!overlay::settled(&state.reset_gates, "a", &states));
}

#[tokio::test]
async fn child_reset_overlay_scheduler_runs_captured_predecessor_before_reader() {
    struct Retry;
    #[async_trait]
    impl StepExecutor for Retry {
        fn type_id(&self) -> &'static str {
            "test.probe"
        }
        fn continuation_semantics_version(&self) -> Option<&'static str> {
            Some("1")
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
            if node.id == "a" {
                let snapshot = ctx.vars.template_snapshot();
                assert_eq!(snapshot["steps"]["b"]["status"], "success");
                assert_eq!(snapshot["steps"]["b"]["output"], 42);
            }
            Ok(StepOutcome::success(Some(json!(42))))
        }
    }
    let f = Fixture::with_output_dependency().await;
    let mut events = reset_child_events(&f, &f.analyze().unwrap());
    append_child_queue(&f, &mut events);
    let (meta, workspace) = stage_child(&f, &events);
    let path = meta.join("journal.jsonl");
    let mut registry = StepRegistry::new();
    registry.register(Retry);
    let result = Engine::new(registry)
        .run_with_id(
            "child".into(),
            meta,
            f.contract.clone(),
            BTreeMap::new(),
            probe_options(workspace),
        )
        .await;
    let after = read_journal_values(&path, JournalLimits::default())
        .unwrap()
        .events;
    assert!(result.is_ok(), "{result:?}; {:?}", &after[events.len()..]);
    assert_eq!(&after[..events.len()], &events);
    let started = after[events.len()..]
        .iter()
        .filter(|e| e["t"] == "step_started")
        .map(|e| e["node"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(started, ["b", "a"]);
    let state = RunState::fold_values(&after).unwrap();
    assert_eq!(state.node_executions["a"], 2);
    assert_eq!(state.node_executions["b"], 2);
}

/// Node-specific declarations let preserved-only and reset-only downgrades be
/// distinguished, while semantics tokens remain executor-wide by design.
struct ResetProbe {
    version: Option<&'static str>,
    safety: ContinuationSafety,
    coverage: ContinuationReadCoverage,
    changed_owner: &'static str,
    executed: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}
#[async_trait]
impl StepExecutor for ResetProbe {
    fn type_id(&self) -> &'static str {
        "test.probe"
    }
    fn continuation_semantics_version(&self) -> Option<&'static str> {
        self.version
    }
    fn continuation_safety(&self, node: &NodeDef) -> ContinuationSafety {
        if node.id == self.changed_owner {
            self.safety
        } else {
            ContinuationSafety::ReadOnly
        }
    }
    fn continuation_read_coverage(&self, node: &NodeDef) -> ContinuationReadCoverage {
        if node.id == self.changed_owner {
            self.coverage
        } else {
            ContinuationReadCoverage::Complete
        }
    }
    async fn execute(
        &self,
        _: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        self.executed.lock().unwrap().push(node.id.clone());
        Ok(StepOutcome::success(Some(json!(42))))
    }
}

fn stage_child(f: &Fixture, events: &[Value]) -> (Utf8PathBuf, Utf8PathBuf) {
    let child = f
        .workspace
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("child");
    let meta = child.join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    std::fs::write(
        meta.join("journal.jsonl"),
        events
            .iter()
            .map(|event| format!("{event}\n"))
            .collect::<String>(),
    )
    .unwrap();
    (meta, child.join("workspace"))
}

#[tokio::test]
async fn reset_executor_proofs_revalidate_after_full_refold_before_execution() {
    use ContinuationReadCoverage::{Complete, Unsupported as Incomplete};
    use ContinuationSafety::{ReadOnly, Unsupported as Unsafe};
    // Each case starts from journal bytes, without a usable state cache. The
    // corrupt-cache cases reproduce process restart without cache assistance.
    for corrupt_cache in [false, true] {
        for (version, safety, coverage) in [
            (Some("1"), ReadOnly, Complete),
            (Some("2"), ReadOnly, Complete),
            (None, ReadOnly, Complete),
            (Some(""), ReadOnly, Complete),
            (Some("1"), Unsafe, Complete),
            (Some("1"), ReadOnly, Incomplete),
        ] {
            for owner in ["a", "b"] {
                let f = Fixture::new().await;
                let candidate = f.analyze().unwrap();
                assert_eq!(
                    candidate
                        .expected_executor_attestations
                        .keys()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                    ["a", "b"]
                );
                let mut events = reset_child_events(&f, &candidate);
                append_child_queue(&f, &mut events);
                let (meta, workspace) = stage_child(&f, &events);
                let folded = RunState::fold_journal(&meta.join("journal.jsonl")).unwrap();
                assert_eq!(
                    folded.reset_expected_attestations,
                    candidate.expected_executor_attestations
                );
                let cached: RunState =
                    serde_json::from_value(serde_json::to_value(&folded).unwrap()).unwrap();
                assert!(cached.reset_expected_attestations.is_empty());
                if corrupt_cache {
                    std::fs::write(meta.join("state.json"), b"unusable").unwrap();
                }
                let executed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
                let mut registry = StepRegistry::new();
                registry.register(ResetProbe {
                    version,
                    safety,
                    coverage,
                    changed_owner: owner,
                    executed: executed.clone(),
                });
                let matches = version == Some("1") && safety == ReadOnly && coverage == Complete;
                // Node-specific downgrade affects only the selected owner.
                if version == Some("1") && !matches {
                    let other = if owner == "a" { "b" } else { "a" };
                    assert_eq!(
                        registry.continuation_attestation(&f.contract.graph.nodes[other]),
                        Some(candidate.expected_executor_attestations[other])
                    );
                }
                let result = Engine::new(registry)
                    .run_with_id(
                        "child".into(),
                        meta.clone(),
                        f.contract.clone(),
                        BTreeMap::new(),
                        probe_options(workspace),
                    )
                    .await;
                let after =
                    read_journal_values(&meta.join("journal.jsonl"), JournalLimits::default())
                        .unwrap()
                        .events;
                if matches {
                    assert!(result.is_ok(), "{result:?}");
                    assert_eq!(*executed.lock().unwrap(), ["b"]); // a remains preserved
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("reset executor proof")
                    );
                    assert!(executed.lock().unwrap().is_empty());
                    assert_eq!(after, events); // no execution/hook events before refusal
                }
            }
        }
    }
}

#[tokio::test]
async fn reset_executor_proofs_require_exact_overlay_owner_coverage() {
    let f = Fixture::new().await;
    for mutation in [
        "missing-preserved",
        "missing-reset",
        "extra",
        "empty",
        "none",
    ] {
        let mut candidate = f.analyze().unwrap();
        match mutation {
            "missing-preserved" => {
                candidate.expected_executor_attestations.remove("a");
            }
            "missing-reset" => {
                candidate.expected_executor_attestations.remove("b");
            }
            "extra" => {
                candidate
                    .expected_executor_attestations
                    .insert("extra".into(), [0; 32]);
            }
            "empty" => {
                candidate
                    .expected_executor_attestations
                    .insert(String::new(), [0; 32]);
            }
            "none" => candidate.expected_executor_attestations.clear(),
            _ => unreachable!(),
        }
        // Rebind all hashes so coverage, rather than a stale digest, refuses.
        assert!(
            RunState::fold_admission_values(&reset_child_events(&f, &candidate)).is_err(),
            "{mutation}"
        );
    }
}

#[tokio::test]
async fn ordinary_fork_does_not_require_reset_executor_proofs() {
    let f = Fixture::new().await;
    let mut events = reset_child_events(&f, &f.analyze().unwrap());
    events.pop(); // ordinary fork: no reset binding or overlay
    let fork = events.last_mut().unwrap().as_object_mut().unwrap();
    fork.remove("binding");
    fork.remove("reset_overlay_sha256");
    append_child_queue(&f, &mut events);
    let (meta, workspace) = stage_child(&f, &events);
    assert!(
        RunState::fold_journal(&meta.join("journal.jsonl"))
            .unwrap()
            .reset_expected_attestations
            .is_empty()
    );
    let executed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = StepRegistry::new();
    registry.register(ResetProbe {
        version: None,
        safety: ContinuationSafety::Unsupported,
        coverage: ContinuationReadCoverage::Unsupported,
        changed_owner: "b",
        executed: executed.clone(),
    });
    let result = Engine::new(registry)
        .run_with_id(
            "child".into(),
            meta,
            f.contract,
            BTreeMap::new(),
            probe_options(workspace),
        )
        .await;
    // Ordinary replay retains the failed outcome too; it must not silently
    // acquire reset semantics or impose continuation declarations.
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("b: probe check failure")
    );
    assert!(executed.lock().unwrap().is_empty());
}
