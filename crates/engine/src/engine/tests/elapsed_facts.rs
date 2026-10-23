use super::*;

#[tokio::test(start_paused = true)]
async fn elapsed_facts_survive_in_flight_settlement_and_interaction_in_both_modes() {
    for mode in [
        policy::ElapsedBudgetMode::WallClock,
        policy::ElapsedBudgetMode::ActiveExecution,
    ] {
        for interaction in [false, true] {
            let root = temp_run_dir("elapsed-facts");
            let mut spec = manifest(vec![node(
                "work",
                if interaction {
                    "test.delayed_user"
                } else {
                    "test.cooperative_wait"
                },
            )]);
            spec.budget.max_elapsed_seconds = Some(1);
            let contract = test_contract(&root, spec);
            let mut registry = StepRegistry::new();
            registry.register(TestDelayedUserStep);
            registry.register(CooperativeWaitStep {
                calls: Arc::new(AtomicUsize::new(0)),
            });
            let error = Engine::new(registry)
                .run_with_id(
                    "elapsed-facts".into(),
                    root.join("meta"),
                    contract,
                    BTreeMap::new(),
                    RunOptions {
                        elapsed_budget_mode: mode,
                        ..run_options(root.join("workspace"))
                    },
                )
                .await
                .unwrap_err();
            let events = journal_events(&root);
            if interaction {
                assert!(matches!(error, EngineError::NeedsUser { .. }));
                let marker = events
                    .iter()
                    .find(|e| e["t"] == "elapsed_exceeded")
                    .unwrap();
                assert_eq!(
                    marker["exhaustion"],
                    json!({"resource":"elapsed_seconds", "used":1, "allowed":1, "used_subsec_nanoseconds":100_000_000})
                );
                assert_eq!(marker["during"], "interaction");
                // The existing internal event stays opaque through the API, without losing facts.
                let event = api::RunEvent::from_flat(marker).unwrap();
                assert_eq!(
                    serde_json::to_value(event).unwrap()["data"]["exhaustion"],
                    marker["exhaustion"]
                );
            } else {
                let expected = json!({"resource":"elapsed_seconds", "used":1, "allowed":1, "used_subsec_nanoseconds":0});
                assert_eq!(
                    serde_json::to_value(failure_detail_for_error(&error)).unwrap()["exhaustion"],
                    expected
                );
                let step = events
                    .iter()
                    .find(|e| e["t"] == "step_finished" && e["node"] == "work")
                    .unwrap();
                assert_eq!(step["reason"]["exhaustion"], expected);
                let terminal = events.iter().find(|e| e["t"] == "run_finished").unwrap();
                assert_eq!(terminal["status"], "failed");
                for failure in terminal["failures"].as_array().unwrap() {
                    assert_eq!(failure["failure"]["exhaustion"], expected, "{failure}");
                }
                for event in [step, terminal] {
                    let typed = api::RunEvent::from_flat(event).unwrap();
                    let encoded = serde_json::to_value(typed).unwrap();
                    assert!(encoded.to_string().contains("used_subsec_nanoseconds"));
                }
                let state = RunState::fold_journal(&root.join("meta/journal.jsonl")).unwrap();
                assert!(matches!(state.terminal, Some(crate::TerminalState::Failed)));
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn elapsed_failure_projection_keeps_exact_components_without_nanosecond_overflow() {
    let used = std::time::Duration::new(u64::MAX - 1, 987654321);
    for error in [
        StepError::ElapsedExpiredBeforeStart {
            limit_secs: u64::MAX - 2,
            used,
        },
        StepError::ElapsedExceeded {
            node: "node".into(),
            limit_secs: u64::MAX - 2,
            used,
        },
    ] {
        let detail = failure_detail_for_error(&EngineError::Step(error));
        assert_eq!(detail.code, model::FailureCode::ElapsedExceeded);
        let facts = detail.exhaustion.unwrap();
        assert_eq!(facts.used, u64::MAX - 1);
        assert_eq!(facts.allowed, u64::MAX - 2);
        assert_eq!(facts.used_subsec_nanoseconds, Some(987654321));
    }
    let legacy: model::BudgetExhaustion =
        serde_json::from_value(json!({"resource":"tokens", "used":2, "allowed":1})).unwrap();
    assert!(legacy.used_subsec_nanoseconds.is_none());
    assert!(
        serde_json::to_value(legacy)
            .unwrap()
            .get("used_subsec_nanoseconds")
            .is_none()
    );
}
