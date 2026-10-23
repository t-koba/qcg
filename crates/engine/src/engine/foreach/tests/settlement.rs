use super::*;
use crate::engine::tests::{
    TestForeachStep, foreach_items_input, foreach_node, manifest, node, run_options, temp_run_dir,
    test_contract,
};
use crate::{StepContext, StepExecutor, StepRegistry};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

struct TempCleanup(Utf8PathBuf);
impl Drop for TempCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct MixedChild {
    checks: Arc<AtomicUsize>,
    interactions: Arc<AtomicUsize>,
    confirmation: bool,
}
#[async_trait]
impl StepExecutor for MixedChild {
    fn type_id(&self) -> &'static str {
        "test.mixed_child"
    }
    crate::parallel_step_traits!();
    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        _node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        if ctx.vars.item() == Some(&json!("check")) {
            self.checks.fetch_add(1, Ordering::SeqCst);
            // A visible effect must not happen again after answering the sibling.
            std::fs::write(ctx.run.workspace.join("checked.txt"), b"checked once")?;
            return Ok(StepOutcome::CheckFailed {
                findings: vec![],
                output: Some(json!({"failed": true})),
                files: vec![ctx.run.workspace.join("checked.txt")],
            });
        }
        self.interactions.fetch_add(1, Ordering::SeqCst);
        if ctx.run.answers.contains_key("shared-id")
            || ctx.run.confirmations.contains_key("shared-id")
        {
            return Ok(StepOutcome::success(None));
        }
        if self.confirmation {
            Ok(StepOutcome::NeedsConfirm {
                confirm: api::ConfirmSpec {
                    id: "shared-id".into(),
                    title: "private prompt".into(),
                    kind: "test".into(),
                    target: "private target".into(),
                    dry_run: false,
                    details: None,
                    operation_digest: "a".repeat(64),
                    scope: contract::SideEffectScope::Invocation,
                },
                files: vec![],
            })
        } else {
            Ok(StepOutcome::NeedsUser {
                question: api::FormSpec {
                    id: "shared-id".into(),
                    title: "private prompt".into(),
                    title_i18n: Default::default(),
                    fields: vec![],
                },
                files: vec![],
            })
        }
    }
}

// Drive the real iteration futures to completion before either join is consumed.
// This fixes the completed set while permuting its JoinSet report order; no sleeps.
#[tokio::test]
async fn mixed_completed_foreach_reports_are_order_independent_and_replay_once() {
    for confirmation in [false, true] {
        for check_index in [0, 1] {
            let first = mixed_case(confirmation, check_index, 0, false).await;
            let reversed = mixed_case(confirmation, check_index, 1, false).await;
            assert_eq!(
                first, reversed,
                "canonical outcome facts changed with report order"
            );
        }
    }
}

async fn mixed_case(
    confirmation: bool,
    check_index: usize,
    first_index: usize,
    nested: bool,
) -> BTreeMap<String, Value> {
    let dir = temp_run_dir(&format!(
        "settlement-{confirmation}-{check_index}-{first_index}-{nested}"
    ));
    let _cleanup = TempCleanup(dir.clone());
    let workspace = dir.join("workspace");
    let metadata = dir.join("meta");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&metadata).unwrap();
    let mut parent = foreach_node("each", "body", "inputs.items", 2, 10);
    let child = node("child", "test.mixed_child");
    let mut manifest = manifest(vec![parent.clone()]);
    manifest.inputs = foreach_items_input();
    manifest.blocks.insert("body".into(), vec![child.clone()]);
    if nested {
        manifest
            .blocks
            .insert("outer_body".into(), vec![parent.clone()]);
        manifest.flow = vec![foreach_node("outer", "outer_body", "['outer']", 1, 10)];
        parent.id = "outer[0]/each".into();
    }
    let owner = parent.id.clone();
    let contract = test_contract(&dir, manifest);
    let checks = Arc::new(AtomicUsize::new(0));
    let interactions = Arc::new(AtomicUsize::new(0));
    let mut registry = StepRegistry::new();
    registry.register(TestForeachStep);
    registry.register(MixedChild {
        checks: checks.clone(),
        interactions: interactions.clone(),
        confirmation,
    });
    let engine = Engine::new(registry);
    let context = RunContext::for_tests(
        "mixed".into(),
        workspace.clone(),
        metadata.clone(),
        contract.clone(),
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .unwrap();
    let journal =
        JournalWriter::create(&metadata.join("journal.jsonl"), "mixed", false, None).unwrap();
    let items = if check_index == 0 {
        vec!["check", "ask"]
    } else {
        vec!["ask", "check"]
    };
    let inputs = BTreeMap::from([("items".into(), json!(items))]);
    journal.event("run_started", json!({ "generator": "scheduler-test@0.1.0", "generator_path": contract.root, "contract_sha256": contract.sha256, "inputs": inputs, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION, "elapsed_budget_mode": "wall_clock", "retention_days": contract.manifest.retention.days })).unwrap();
    journal
        .event(
            "step_started",
            json!({"node": owner, "type": "test.foreach", "attempt": 1}),
        )
        .unwrap();
    let mut vars = ValueBag::with_inputs(inputs.clone());
    let mut tasks = JoinSet::new();
    let mut indices = HashMap::new();
    for index in [first_index, 1 - first_index] {
        let engine = engine.clone();
        let context = context.clone();
        let journal = journal.clone_for_parallel().unwrap();
        let parent = parent.clone();
        let block = vec![child.clone()];
        let mut vars = vars.clone();
        let item = json!(items[index]);
        let task = tasks.spawn(async move {
            let mut owner = String::new();
            let result = engine
                .execute_foreach_iteration(
                    &context,
                    &journal,
                    &mut vars,
                    &mut BudgetTracker::new(100, 0),
                    &mut owner,
                    ForeachIteration {
                        node: &parent,
                        block: &block,
                        index,
                        item,
                    },
                )
                .await;
            (index, owner, vars, result)
        });
        indices.insert(task.id(), index);
        completed(&task).await;
    }
    let (id, report) = tasks.join_next_with_id().await.unwrap().unwrap();
    assert_eq!(report.0, first_index);
    indices.remove(&id);
    let mut files = vec![];
    let mut reporter = ForeachReporter {
        vars: &mut vars,
        files: &mut files,
        journal: &journal,
        node: &owner,
        workspace: &workspace,
    };
    let first = reporter
        .record(Ok(report), first_index, false, false)
        .unwrap()
        .unwrap();
    let mut terminal = reporter
        .drain(&mut tasks, &mut indices, false)
        .await
        .unwrap();
    terminal.insert(first_index, first);
    let selected = select_foreach_terminal(terminal, files).unwrap();
    assert_eq!(
        matches!(selected, StepOutcome::CheckFailed { .. }),
        check_index == 0
    );
    if nested {
        let mut root_reporter = ForeachReporter {
            vars: &mut vars,
            files: &mut vec![],
            journal: &journal,
            node: "outer",
            workspace: &workspace,
        };
        root_reporter
            .record(
                Ok((
                    0,
                    owner.clone(),
                    ValueBag::default(),
                    Ok((vec![], Some(selected.clone()))),
                )),
                0,
                false,
                false,
            )
            .unwrap();
        journal.event("step_finished", json!({"node": "outer", "status": "check_failed", "reason": {"code":"check_failed", "message":"nested check"}})).unwrap();
    }
    // Reproduce the enclosing scheduler's durable classification before resuming.
    match selected {
        StepOutcome::CheckFailed {
            findings, output, ..
        } => {
            journal.event("step_finished", json!({"node": owner, "status": "check_failed", "findings": findings, "failed_output": output, "failed_files": []})).unwrap();
        }
        StepOutcome::NeedsUser { question, .. } => {
            journal.event("step_finished", json!({"node": owner, "status": "needs_user", "question": question, "files": []})).unwrap();
        }
        StepOutcome::NeedsConfirm { confirm, .. } => {
            journal
                .event(
                    "confirm_request",
                    json!({"node": owner, "confirm": confirm, "files": []}),
                )
                .unwrap();
        }
        _ => panic!("unexpected classification"),
    }
    let state = journal.state();
    assert!(state.unresolved_tasks.is_empty());
    assert_eq!(state.foreach_reports.len(), if nested { 3 } else { 2 });
    for index in 0..2 {
        let fact = &state.foreach_reports[&format!("{owner}[{index}]")];
        assert_eq!(fact["owner"], format!("{owner}[{index}]/child"));
        assert_eq!(fact["index"], index);
        assert!(!fact.to_string().contains(workspace.as_str()));
        if index != check_index {
            assert_eq!(
                fact[if confirmation {
                    "confirm_id"
                } else {
                    "question_id"
                }],
                "shared-id"
            );
            assert!(!fact.to_string().contains("private prompt"));
        }
    }
    let restored: RunState = serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
    assert_eq!(restored.foreach_reports, state.foreach_reports);
    assert!(state.historical_file_pins.contains_key("checked.txt"));
    drop(journal);
    let mut options = run_options(workspace.clone());
    if confirmation {
        options.confirmations.insert("shared-id".into(), true);
    } else {
        options.answers.insert("shared-id".into(), json!({}));
    }
    let result = engine
        .run_with_id("mixed".into(), metadata.clone(), contract, inputs, options)
        .await;
    assert!(
        result.is_err(),
        "the settled check must still fail the parent"
    );
    assert_eq!(
        checks.load(Ordering::SeqCst),
        1,
        "completed failure re-executed"
    );
    assert_eq!(
        interactions.load(Ordering::SeqCst),
        2,
        "answered owner was not re-entered"
    );
    assert_eq!(
        std::fs::read(workspace.join("checked.txt")).unwrap(),
        b"checked once"
    );
    let after = RunState::fold_journal(&metadata.join("journal.jsonl")).unwrap();
    assert_eq!(
        after.budget.steps_failed,
        state.budget.steps_failed + if nested { 2 } else { 1 }
    );
    assert!(after.pending_interactions.is_empty());
    let events = read_journal_values(&metadata.join("journal.jsonl"), JournalLimits::default())
        .unwrap()
        .events;
    assert_eq!(
        events
            .iter()
            .filter(|e| e["t"] == "step_started"
                && e["node"] == format!("{owner}[{check_index}]/child"))
            .count(),
        1
    );
    let mut canonical = state.foreach_reports;
    for fact in canonical.values_mut() {
        fact.as_object_mut().unwrap().remove("seq");
    }
    canonical
}

#[tokio::test]
async fn answered_nested_owner_reenters_without_repeating_the_completed_check() {
    mixed_case(false, 0, 1, true).await;
}
