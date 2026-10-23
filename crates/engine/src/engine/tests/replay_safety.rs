use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

struct InterruptedIteration {
    entered: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl StepExecutor for InterruptedIteration {
    fn type_id(&self) -> &'static str {
        "test.interrupted_iteration"
    }
    crate::parallel_step_traits!();
    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        _node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if ctx.vars.item() == Some(&json!("slow")) {
            self.entered.add_permits(1);
            std::future::pending().await
        } else {
            self.entered.acquire().await.unwrap().forget();
            Ok(StepOutcome::NeedsUser {
                question: api::FormSpec {
                    id: "iteration-question".into(),
                    title: "Continue?".into(),
                    title_i18n: Default::default(),
                    fields: vec![],
                },
                files: vec![],
            })
        }
    }
}

#[tokio::test]
async fn foreach_unreported_iteration_blocks_parent_replay() {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        foreach_unreported_iteration(),
    )
    .await
    .unwrap();
}

async fn foreach_unreported_iteration() {
    let dir = temp_run_dir("foreach-unreported-replay");
    let _guard = TempGuard(dir.clone());
    let mut manifest = manifest(vec![foreach_node("each", "body", "inputs.items", 2, 10)]);
    manifest.inputs = foreach_items_input();
    manifest.blocks.insert(
        "body".into(),
        vec![node("child", "test.interrupted_iteration")],
    );
    let contract = test_contract(&dir, manifest);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = StepRegistry::new();
    registry.register(TestForeachStep);
    registry.register(InterruptedIteration {
        entered: Arc::new(Semaphore::new(0)),
        calls: calls.clone(),
    });
    let engine = Engine::new(registry);
    let inputs = BTreeMap::from([("items".into(), json!(["question", "slow"]))]);
    let run = |answers| {
        let mut options = run_options(dir.join("workspace"));
        options.answers = answers;
        engine.run_with_id(
            "foreach-replay".into(),
            dir.join("meta"),
            contract.clone(),
            inputs.clone(),
            options,
        )
    };
    assert!(matches!(
        run(BTreeMap::new()).await,
        Err(EngineError::NeedsUser { .. })
    ));
    let state = RunState::fold_journal(&dir.join("meta/journal.jsonl")).unwrap();
    let marker = &state.unresolved_tasks["each[1]"];
    assert_eq!(marker["node"], "each");
    assert_eq!(marker["index"], 1);
    assert_eq!(marker["classification"], "scheduler_abort");
    assert_eq!(marker["drain_cause"], "fail_fast");
    // The enclosing needs_user report must not erase an aborted sibling's facts.
    assert_eq!(state.unresolved_tasks.len(), 1);
    assert!(matches!(
        run(BTreeMap::new()).await,
        Err(EngineError::NeedsUser { .. })
    ));
    let error = run(BTreeMap::from([("iteration-question".into(), json!({}))]))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unreported task `each[1]`"),
        "{error}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        journal_events(&dir)
            .iter()
            .filter(|e| e["t"] == "step_started" && e["node"] == "each")
            .count(),
        1
    );
    let restored: RunState = serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
    assert_eq!(restored.unresolved_tasks["each[1]"], *marker);
}

#[test]
fn older_journal_later_outcomes_clear_unreported_markers() {
    for (kind, owner, node) in [
        ("parallel_task_joined", "slow", "slow"),
        ("foreach_sibling_drained", "each[2]", "each"),
    ] {
        let mut state = RunState::default();
        state
            .apply(
                &json!({"t": kind, "seq": 1, "node": node, "iteration": owner,
            "classification": "scheduler_abort", "drain_cause": "fail_fast"}),
            )
            .unwrap();
        assert_eq!(state.unresolved_tasks.len(), 1);
        state
            .apply(&json!({"t": "step_started", "seq": 2, "node": node}))
            .unwrap();
        state
            .apply(
                &json!({"t": "parallel_replay_authorized", "seq": 3, "node": node,
            "scope": "interrupted_task"}),
            )
            .unwrap();
        assert_eq!(
            state.unresolved_tasks.len(),
            1,
            "starting/authorizing is not settlement"
        );
        state
            .apply(&json!({"t": "step_finished", "seq": 4, "node": "other", "status": "success"}))
            .unwrap();
        assert_eq!(
            state.unresolved_tasks.len(),
            1,
            "another owner cannot settle this task"
        );
        state
            .apply(&json!({"t": "step_finished", "seq": 5, "node": node, "status": "success"}))
            .unwrap();
        assert!(
            state.unresolved_tasks.is_empty(),
            "an old journal's completed replay must settle its marker"
        );
    }
}

#[test]
fn unreported_task_causes_are_not_step_failures() {
    for classification in [
        "scheduler_abort",
        "parent_canceled",
        "task_panic",
        "task_join_error",
        "task_canceled",
        "future_classification",
    ] {
        let state = RunState::fold_values(&[
            json!({"t": "parallel_task_joined", "seq": 1, "ts": "2026-01-01T00:00:00Z", "run_id": "test", "trace_id": "trace", "span_id": "span",
            "node": "task", "classification": classification, "cancellation_cause": "none"}),
        ])
        .unwrap();
        assert_eq!(
            state.unresolved_tasks["task"]["classification"],
            classification
        );
        assert!(state.nodes.is_empty());
        assert_eq!(state.budget.steps_failed, 0);
    }
    let missing_class = RunState::fold_values(&[json!({"t": "parallel_task_joined", "seq": 1, "ts": "2026-01-01T00:00:00Z", "run_id": "test", "trace_id": "trace", "span_id": "span",
        "node": "task"})]).unwrap();
    assert!(missing_class.unresolved_tasks.contains_key("task"));
    let state = RunState::fold_values(&[json!({"t": "parallel_task_joined", "seq": 1, "ts": "2026-01-01T00:00:00Z", "run_id": "test", "trace_id": "trace", "span_id": "span",
        "node": "task", "classification": "task_canceled", "cancellation_cause": "reported_by_task"})]).unwrap();
    assert_eq!(
        state.unresolved_tasks["task"]["cancellation_cause"], "reported_by_task",
        "reported cancellation still has unknown effects without a settled outcome"
    );
}

// A mixed wave proves that one executor's declaration cannot authorize any
// part of a set containing an unresolved owner without such evidence.
struct MixedReplayStep {
    safe: bool,
    entered: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl StepExecutor for MixedReplayStep {
    fn type_id(&self) -> &'static str {
        if self.safe {
            "test.replay_safe"
        } else {
            "test.replay_unknown"
        }
    }
    crate::parallel_step_traits!();
    fn replay_safe_after_interruption(&self) -> bool {
        self.safe
    }
    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if node.id == "question" {
            if ctx.run.answers.contains_key("mixed-question") {
                return Ok(StepOutcome::Success {
                    output: None,
                    files: vec![],
                });
            }
            self.entered.acquire_many(2).await.unwrap().forget();
            Ok(StepOutcome::NeedsUser {
                question: api::FormSpec {
                    id: "mixed-question".into(),
                    title: "Continue?".into(),
                    title_i18n: Default::default(),
                    fields: vec![],
                },
                files: vec![],
            })
        } else {
            self.entered.add_permits(1);
            std::future::pending().await
        }
    }
}

#[tokio::test]
async fn replay_preflight_refuses_entire_mixed_set_before_authorization() {
    let dir = temp_run_dir("mixed-unresolved-replay");
    let _guard = TempGuard(dir.clone());
    let mut manifest = manifest(vec![
        node("allowed", "test.replay_safe"),
        node("blocked", "test.replay_unknown"),
        node("question", "test.replay_unknown"),
    ]);
    manifest.parallel = vec!["allowed".into(), "blocked".into(), "question".into()];
    let contract = test_contract(&dir, manifest);
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let mut registry = StepRegistry::new();
    for safe in [true, false] {
        registry.register(MixedReplayStep {
            safe,
            entered: entered.clone(),
            calls: calls.clone(),
        });
    }
    let engine = Engine::new(registry);
    let run = |answers| {
        let mut options = run_options(dir.join("workspace"));
        options.max_parallel_steps = 3;
        options.answers = answers;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            engine.run_with_id(
                "mixed-replay".into(),
                dir.join("meta"),
                contract.clone(),
                BTreeMap::new(),
                options,
            ),
        )
    };
    assert!(matches!(
        run(BTreeMap::new()).await.unwrap(),
        Err(EngineError::NeedsUser { .. })
    ));
    assert_eq!(
        RunState::fold_journal(&dir.join("meta/journal.jsonl"))
            .unwrap()
            .unresolved_tasks
            .len(),
        2
    );
    let error = run(BTreeMap::from([("mixed-question".into(), json!({}))]))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("unreported task `blocked`"));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let events = journal_events(&dir);
    assert!(!events.iter().any(|e| e["scope"] == "interrupted_task"));
    assert_eq!(
        events.iter().filter(|e| e["t"] == "step_started").count(),
        3
    );
}
