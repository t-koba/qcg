use super::*;
use model::{BudgetExhaustion, FailureCode, FailureDetail, NodePath};

struct MeteredBudgetStep(Arc<AtomicUsize>);

#[async_trait]
impl StepExecutor for MeteredBudgetStep {
    crate::step_type_id!("test.budget_meter");
    crate::parallel_step_traits!();

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        // A bounded durable-usage seam feeds the real checkpoint enforcement.
        // No error or failure detail is fabricated by this executor.
        ctx.journal
            .event(
                "llm_call",
                json!({"node":node.id,"provider":"test","model":"test","max_tokens":1,
            "tokens":llm::TokenUsage {input:1_000_001,..Default::default()},"cost_microusd":1_000_001}),
            )
            .map_err(|e| StepError::failed(&node.id, e.to_string()))?;
        ctx.step_checkpoint(node).await?;
        Ok(StepOutcome::success(None))
    }
}

#[tokio::test]
async fn typed_budget_failures_survive_execution_journal_and_replay() {
    for resource in ["tokens", "cost_microusd"] {
        for path in ["sequential", "parallel", "foreach", "repair"] {
            let name = format!("typed-budget-{resource}-{path}");
            let root = temp_run_dir(&name);
            let _guard = TempGuard(root.clone());
            let calls = Arc::new(AtomicUsize::new(0));
            let mut meter = node("meter", "test.budget_meter");
            meter.retry = Some(RetryPolicy {
                max_attempts: 3,
                ..Default::default()
            });
            let mut spec = match path {
                "foreach" => {
                    let mut spec =
                        manifest(vec![foreach_node("each", "body", "inputs.items", 1, 1)]);
                    spec.inputs = foreach_items_input();
                    spec.blocks.insert("body".into(), vec![meter]);
                    spec
                }
                "repair" => {
                    let mut broken = node("broken", "test.check_fail");
                    broken.on_fail = Some(OnFail::Repair {
                        repair: "meter".into(),
                        recheck: "recheck".into(),
                        max_attempts: 1,
                        on_exhausted: ExhaustedAction::Fail,
                    });
                    manifest(vec![broken, meter, node("recheck", "test.pass")])
                }
                _ => manifest(vec![meter, node("sibling", "test.pass")]),
            };
            if path == "parallel" {
                spec.parallel = vec!["meter".into(), "sibling".into()];
            }
            if resource == "tokens" {
                spec.budget.max_tokens = Some(1_000_000);
            } else {
                spec.budget.max_cost_usd = Some(1.0);
            }
            let mut registry = test_registry();
            registry.register(MeteredBudgetStep(calls.clone()));
            registry.register(TestForeachStep);
            let engine = Engine::new(registry);
            let mut options = run_options(root.join("workspace"));
            options.max_parallel_steps = 2;
            let progress = engine
                .advance_with_id(
                    name.clone(),
                    root.join("meta"),
                    test_contract(&root, spec),
                    if path == "foreach" {
                        BTreeMap::from([("items".into(), json!([0]))])
                    } else {
                        BTreeMap::new()
                    },
                    options,
                )
                .await;
            let Progress::Failed(failure) = progress else {
                panic!("expected failure: {progress:?}")
            };
            let expected = BudgetExhaustion {
                used_subsec_nanoseconds: None,
                resource: resource.into(),
                used: 1_000_001,
                allowed: 1_000_000,
            };
            assert_eq!(
                failure.code,
                FailureCode::BudgetExceeded,
                "{path}: {failure:?}"
            );
            assert_eq!(failure.kind, RunFailureKind::Execution);
            assert_eq!(failure.exhaustion.as_ref(), Some(&expected), "{path}");
            assert_eq!(
                failure.message,
                format!("run budget `{resource}` exceeded: 1000001 > 1000000")
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "budget errors must not retry"
            );
            let state = RunState::fold_journal(&root.join("meta/journal.jsonl")).unwrap();
            let meter_path = match path {
                "foreach" => "each[0]/meter",
                "repair" => "broken@repair.1/meter",
                _ => "meter",
            };
            let crate::NodeOutcome::Failed { reason } = state
                .nodes
                .get(&NodePath::root(meter_path))
                .unwrap_or_else(|| panic!("{path}: missing {meter_path}: {:?}", state.nodes))
            else {
                panic!("missing meter failure")
            };
            assert_eq!(reason, &failure.detail);
            assert_eq!(
                state
                    .vars
                    .get_path(&format!("steps.{meter_path}.reason.exhaustion.used")),
                Some(&json!(1_000_001))
            );
            let events: Vec<Value> = std::fs::read_to_string(root.join("meta/journal.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let finished = events
                .iter()
                .find(|e| {
                    e["t"] == "step_finished" && e["node"] == meter_path && e["status"] == "failed"
                })
                .unwrap();
            let decoded = api::RunEvent::from_flat(finished).unwrap();
            assert_eq!(
                serde_json::from_value::<FailureDetail>(
                    serde_json::to_value(decoded.data).unwrap()["reason"].clone()
                )
                .unwrap(),
                failure.detail
            );
            if path != "repair" {
                let terminal = events.iter().find(|e| e["t"] == "run_finished").unwrap();
                assert!(
                    terminal["failures"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|f| f["failure"]["exhaustion"] == json!(expected))
                );
            }
        }
    }
}

#[test]
fn only_typed_budget_errors_project_exhaustion() {
    use super::super::types::failure_detail_for_error;
    let typed = EngineError::Step(StepError::BudgetExceeded {
        resource: "tokens",
        used: 9,
        limit: 8,
    });
    let detail = failure_detail_for_error(&typed);
    assert_eq!(detail.exhaustion.as_ref().unwrap().allowed, 8);
    assert_eq!(
        failure_detail_for_error(&EngineError::ResumableAgent(Box::new(typed))),
        detail
    );
    for error in [
        EngineError::Failed(detail.message.clone()),
        EngineError::Step(StepError::AgentResourceExhausted {
            node: "n".into(),
            message: "local".into(),
            resource: "tokens",
            used: 9,
            allowed: 8,
        }),
    ] {
        assert!(failure_detail_for_error(&error).exhaustion.is_none());
    }
}

struct ProviderContextStep(Arc<AtomicUsize>);
#[async_trait]
impl StepExecutor for ProviderContextStep {
    crate::step_type_id!("test.context_exhausted");
    crate::parallel_step_traits!();
    async fn execute(
        &self,
        _: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(StepError::ProviderContextExhausted {
            node: node.id.clone(),
        })
    }
}

#[tokio::test]
async fn provider_context_exhaustion_is_nonretryable_and_survives_canonical_reports_without_fake_capacity()
 {
    for parallel in [false, true] {
        let root = temp_run_dir("provider-context-exhaustion");
        let _guard = TempGuard(root.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut exhausted = node("exhausted", "test.context_exhausted");
        exhausted.retry = Some(RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        });
        let mut spec = manifest(vec![exhausted, node("sibling", "test.pass")]);
        if parallel {
            spec.parallel = vec!["exhausted".into(), "sibling".into()];
        }
        let mut registry = test_registry();
        registry.register(ProviderContextStep(calls.clone()));
        let mut options = run_options(root.join("workspace"));
        options.max_parallel_steps = 2;
        let progress = Engine::new(registry)
            .advance_with_id(
                "provider-context-test".into(),
                root.join("meta"),
                test_contract(&root, spec),
                BTreeMap::new(),
                options,
            )
            .await;
        let Progress::Failed(failure) = progress else {
            panic!("expected failure: {progress:?}")
        };
        assert_eq!(failure.code, FailureCode::ProviderContextExhausted);
        assert_eq!(failure.kind, RunFailureKind::Execution);
        assert!(failure.exhaustion.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let state = RunState::fold_journal(&root.join("meta/journal.jsonl")).unwrap();
        let crate::NodeOutcome::Failed { reason } = &state.nodes[&NodePath::root("exhausted")]
        else {
            panic!("failed node required")
        };
        assert_eq!(reason, &failure.detail);
        let value = serde_json::to_value(reason).unwrap();
        assert_eq!(value["code"], "provider_context_exhausted");
        assert!(value.get("exhaustion").is_none());
        let events: Vec<Value> = std::fs::read_to_string(root.join("meta/journal.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(!events.iter().any(|e| e["t"] == "step_retry"));
        let finished = events
            .iter()
            .find(|e| e["t"] == "step_finished" && e["node"] == "exhausted")
            .unwrap();
        assert_eq!(finished["reason"], value);
        assert!(finished["failed_output"].is_null());
    }
}
