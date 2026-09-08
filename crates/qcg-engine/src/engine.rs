mod checkpoint;
mod execute;
mod foreach;
mod repair;
mod repair_support;
mod replay;
mod run;
mod run_context;
mod types;

#[cfg(test)]
pub(crate) use checkpoint::*;
#[cfg(test)]
pub(crate) use replay::*;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ResourceSnapshot, RunState, StepContext, StepError, StepExecutor, StepOutcome,
        StepRegistry, StepTraits,
    };
    use async_trait::async_trait;
    use camino::Utf8PathBuf;
    use qcg_contract::{
        AssetSpec, Contract, ExhaustedAction, FailurePolicy, FieldType, GeneratorMeta, Graph,
        InputField, InputSpec, JournalPolicy, Manifest, NodeDef, OnDeps, OnFail, OutputSpec,
        Permissions, RetryPolicy, RuntimeLimits, SecretRef, StepType,
    };
    use qcg_types::{Finding, OutputManifest};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn budget_tracker_rejects_after_global_limit() {
        let budget = BudgetTracker::new(2, 0);
        assert!(budget.consume("first").is_ok());
        assert!(budget.consume("second").is_ok());
        let error = budget
            .consume("third")
            .expect_err("third step should exceed the run budget");
        assert!(error.to_string().contains("global step budget exceeded"));
    }

    #[test]
    fn checkpoint_accounting_enforces_output_bounds() {
        let limits = RuntimeLimits {
            output_file_limit_bytes: Some(4),
            output_total_limit_bytes: Some(5),
            output_artifact_limit: Some(2),
            ..RuntimeLimits::default()
        };
        let mut accounting = CheckpointAccounting::default();
        accounting
            .record(camino::Utf8Path::new("one.txt"), 4, &limits)
            .expect("first output should fit");
        let error = accounting
            .record(camino::Utf8Path::new("two.txt"), 2, &limits)
            .expect_err("total output limit should be enforced");
        assert!(error.to_string().contains("output bytes exceed 5"));
        let error = accounting
            .record(camino::Utf8Path::new("too-large"), 5, &limits)
            .expect_err("per-file output limit should be enforced");
        assert!(error.to_string().contains("exceeds 4 bytes"));
    }

    #[test]
    fn generator_cannot_claim_llm_provider_credential_environment() {
        let provider = qcg_llm::LlmRouter::parse_text(
            r#"
[[provider]]
id = "secure"
api = "chat_completions"
base_url = "https://example.test/v1"
api_key_env = "QCG_SECURE_API_KEY"
"#,
        )
        .expect("provider registry should parse");
        let mut manifest = manifest(vec![]);
        manifest.secrets.insert(
            "stolen_provider_key".into(),
            SecretRef {
                env: Some("QCG_SECURE_API_KEY".into()),
                file_env: None,
            },
        );
        let contract = Contract {
            root: Utf8PathBuf::from("generator"),
            graph: Graph::build(&manifest).expect("empty graph should build"),
            manifest,
            sha256: "test".into(),
        };

        let mut registry = StepRegistry::new();
        registry.reserve_secret_env_names(qcg_llm::LlmProvider::credential_env_names(&provider));
        let error = registry
            .validate_contract(&contract)
            .expect_err("provider credentials must remain unavailable to generator secrets");
        assert!(error.to_string().contains("reserved provider credential"));
        assert!(error.to_string().contains("QCG_SECURE_API_KEY"));
    }

    #[tokio::test]
    async fn scheduler_join_truth_table_and_skip_propagation() {
        struct Case {
            name: &'static str,
            left: &'static str,
            right: &'static str,
            on_deps: OnDeps,
            expect_join_runs: bool,
            expect_success: bool,
        }

        let cases = vec![
            Case {
                name: "all_success_runs",
                left: "test.pass",
                right: "test.pass",
                on_deps: OnDeps::AllSucceeded,
                expect_join_runs: true,
                expect_success: true,
            },
            Case {
                name: "all_failed_skips_join",
                left: "test.pass",
                right: "test.check_fail",
                on_deps: OnDeps::AllSucceeded,
                expect_join_runs: false,
                expect_success: false,
            },
            Case {
                name: "any_one_success_runs",
                left: "test.pass",
                right: "test.check_fail",
                on_deps: OnDeps::AnySucceeded,
                expect_join_runs: true,
                expect_success: false,
            },
            Case {
                name: "any_no_success_skips_join",
                left: "test.check_fail",
                right: "test.check_fail",
                on_deps: OnDeps::AnySucceeded,
                expect_join_runs: false,
                expect_success: false,
            },
        ];

        for case in cases {
            let manifest = manifest(vec![
                node("left", case.left),
                node("right", case.right),
                NodeDef {
                    id: "join".into(),
                    kind: StepType::from("test.pass"),
                    needs: vec!["left".into(), "right".into()],
                    on_deps: case.on_deps,
                    ..node("join", "test.pass")
                },
            ]);
            let run_dir = temp_run_dir(case.name);
            let result = run_manifest(manifest, run_dir.clone(), 1).await;
            assert_eq!(
                result.is_ok(),
                case.expect_success,
                "case {} should have expected final status",
                case.name
            );
            let events = journal_events(&run_dir);
            assert_eq!(
                has_event(&events, "step_finished", "join"),
                case.expect_join_runs,
                "case {} join execution mismatch",
                case.name
            );
            if !case.expect_join_runs {
                assert!(
                    has_event(&events, "step_skipped", "join"),
                    "case {} should skip join",
                    case.name
                );
            }
        }
    }

    #[tokio::test]
    async fn scheduler_when_skip_propagates_to_dependents() {
        let mut skipped = node("skipped", "test.pass");
        skipped.when = Some(qcg_contract::Expr("inputs.enabled".into()));
        let manifest = manifest(vec![
            skipped,
            NodeDef {
                id: "dependent".into(),
                kind: StepType::from("test.pass"),
                needs: vec!["skipped".into()],
                ..node("dependent", "test.pass")
            },
        ]);
        let run_dir = temp_run_dir("when-skip-propagates");
        let result = run_manifest(manifest, run_dir.clone(), 1).await;
        assert!(result.is_ok(), "skip propagation failed: {result:?}");
        let events = journal_events(&run_dir);
        assert!(has_event(&events, "step_skipped", "skipped"));
        assert!(has_event(&events, "step_skipped", "dependent"));
    }

    #[tokio::test]
    async fn scheduler_repair_cycle_marks_original_step_repaired() {
        let mut broken = node("broken", "test.check_fail");
        broken.on_fail = Some(OnFail::Repair {
            repair: "repair".into(),
            recheck: "recheck".into(),
            max_attempts: 1,
            on_exhausted: ExhaustedAction::Fail,
        });
        let manifest = manifest(vec![
            broken,
            node("repair", "test.pass"),
            node("recheck", "test.pass"),
        ]);
        let run_dir = temp_run_dir("repair-cycle-repaired");
        let result = run_manifest(manifest, run_dir.clone(), 1).await;
        assert!(result.is_ok(), "repair cycle failed: {result:?}");
        let events = journal_events(&run_dir);
        assert!(has_status(&events, "step_finished", "broken", "repaired"));
        assert!(has_event(&events, "repair_attempt_started", "broken"));
        assert!(has_status(
            &events,
            "repair_attempt_finished",
            "broken",
            "repaired"
        ));
    }

    #[tokio::test]
    async fn repair_exhaustion_returns_the_declared_typed_form() {
        let mut broken = node("broken", "test.check_fail");
        broken.on_fail = Some(OnFail::Repair {
            repair: "repair".into(),
            recheck: "recheck".into(),
            max_attempts: 1,
            on_exhausted: ExhaustedAction::AskUser {
                title: Some("Choose a recovery".into()),
                fields: vec![InputField {
                    id: "decision".into(),
                    label: Some("Decision".into()),
                    label_i18n: Default::default(),
                    description: Some("Select the recovery action".into()),
                    description_i18n: Default::default(),
                    placeholder: None,
                    placeholder_i18n: Default::default(),
                    kind: FieldType::Select,
                    required: true,
                    default: None,
                    pattern: None,
                    options: vec!["retry".into(), "stop".into()],
                    option_labels_i18n: Default::default(),
                    min_items: None,
                    item_type: None,
                    schema: None,
                    ui: Default::default(),
                }],
            },
        });
        let manifest = manifest(vec![
            broken,
            node("repair", "test.pass"),
            node("recheck", "test.check_fail"),
        ]);
        let run_dir = temp_run_dir("repair-exhausted-ask-user");
        let error = run_manifest(manifest, run_dir, 1)
            .await
            .expect_err("repair exhaustion should pause for the declared form");
        let EngineError::NeedsUser {
            question_id,
            question,
        } = error
        else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(question_id, "broken:repair_exhausted");
        assert_eq!(question.title, "Choose a recovery");
        assert_eq!(question.fields[0].id, "decision");
        assert_eq!(question.fields[0].options, ["retry", "stop"]);
    }

    #[tokio::test]
    async fn scheduler_parallel_wave_records_parallel_execution() {
        let mut manifest = manifest(vec![node("a", "test.pass"), node("b", "test.pass")]);
        manifest.parallel = vec!["a".into(), "b".into()];
        let run_dir = temp_run_dir("parallel-wave-records");
        let result = run_manifest(manifest, run_dir.clone(), 4).await;
        assert!(result.is_ok());
        let events = journal_events(&run_dir);
        let parallel_finished = events.iter().filter(|event| {
            event.get("t").and_then(Value::as_str) == Some("step_finished")
                && event.get("parallel").and_then(Value::as_bool) == Some(true)
        });
        assert_eq!(parallel_finished.count(), 2);
    }

    #[tokio::test]
    async fn parallel_wave_records_every_started_node_before_reporting_failure() {
        let mut manifest = manifest(vec![
            node("broken", "test.check_fail"),
            node("sibling", "test.pass"),
        ]);
        manifest.parallel = vec!["broken".into(), "sibling".into()];
        let run_dir = temp_run_dir("parallel-wave-failure-records");
        let result = run_manifest(manifest, run_dir.clone(), 4).await;
        assert!(result.is_err());
        let events = journal_events(&run_dir);
        assert!(has_event(&events, "step_finished", "broken"));
        assert!(has_event(&events, "step_finished", "sibling"));
    }

    #[tokio::test]
    async fn journal_replay_reuses_successful_steps_without_reexecuting_them() {
        let manifest = manifest(vec![node("a", "test.pass"), node("b", "test.pass")]);
        let run_dir = temp_run_dir("journal-replay");
        let _ = std::fs::remove_dir_all(&run_dir);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut registry = StepRegistry::new();
        registry.register(CountingPassStep {
            calls: Arc::clone(&counter),
        });
        for _ in 0..2 {
            let graph = Graph::build(&manifest).expect("test graph should build");
            let contract = Contract {
                root: run_dir.clone(),
                manifest: manifest.clone(),
                graph,
                sha256: "test".into(),
            };
            Engine::new(registry.clone())
                .run_with_id(
                    "journal-replay".into(),
                    run_dir.join("meta"),
                    contract,
                    BTreeMap::new(),
                    RunOptions {
                        output_dir: run_dir.join("workspace"),
                        json_events: false,
                        event_sender: None,
                        interactive: false,
                        answers: BTreeMap::new(),
                        confirmations: BTreeMap::new(),
                        max_total_steps: 100,
                        max_parallel_steps: 1,
                        llm_provider: None,
                        llm_seed_override: None,
                        cancellation: CancellationToken::new(),
                    },
                )
                .await
                .expect("both runs should finish");
        }
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        let events = journal_events(&run_dir);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.get("t").and_then(Value::as_str) == Some("step_replayed"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn journal_replay_rejects_contract_drift_before_executing_steps() {
        let manifest = manifest(vec![node("a", "test.pass")]);
        let run_dir = temp_run_dir("journal-contract-drift");
        let _ = std::fs::remove_dir_all(&run_dir);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut registry = StepRegistry::new();
        registry.register(CountingPassStep {
            calls: Arc::clone(&counter),
        });
        let graph = Graph::build(&manifest).expect("test graph should build");
        let options = || RunOptions {
            output_dir: run_dir.join("workspace"),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            max_total_steps: 100,
            max_parallel_steps: 1,
            llm_provider: None,
            llm_seed_override: None,
            cancellation: CancellationToken::new(),
        };
        Engine::new(registry.clone())
            .run_with_id(
                "journal-contract-drift".into(),
                run_dir.join("meta"),
                Contract {
                    root: run_dir.clone(),
                    manifest: manifest.clone(),
                    graph: graph.clone(),
                    sha256: "original".into(),
                },
                BTreeMap::new(),
                options(),
            )
            .await
            .expect("initial run should finish");
        let error = Engine::new(registry)
            .run_with_id(
                "journal-contract-drift".into(),
                run_dir.join("meta"),
                Contract {
                    root: run_dir.clone(),
                    manifest,
                    graph,
                    sha256: "changed".into(),
                },
                BTreeMap::new(),
                options(),
            )
            .await
            .expect_err("changed contract must not resume");
        assert!(
            error
                .to_string()
                .contains("contract changed while resuming")
        );
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn resume_rejects_resource_content_divergence() {
        let mut state = RunState {
            run_id: Some("resource-drift".into()),
            ..RunState::default()
        };
        state
            .resource_pins
            .insert("guide".into(), "original".into());
        let snapshots = vec![ResourceSnapshot {
            name: "guide".into(),
            resource_type: "file".into(),
            source: crate::ResourceSnapshotSource::Path {
                path: Utf8PathBuf::from("guide.md"),
            },
            snapshot: None,
            sha256: "changed".into(),
            bytes: 7,
            files: Vec::new(),
            cache: crate::ResourceCacheStatus::Local,
            pin_sha256: None,
            trust: "trusted".into(),
            llm_visible: true,
        }];
        let error = verify_resource_pins(&state, &snapshots)
            .expect_err("changed resource content must not resume");
        assert!(error.to_string().contains("resource `guide` changed"));
    }

    async fn run_manifest(
        manifest: Manifest,
        output_dir: Utf8PathBuf,
        max_parallel_steps: usize,
    ) -> Result<OutputManifest, EngineError> {
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: output_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        Engine::new(test_registry())
            .run_with_id(
                format!("test-{}", output_dir.file_name().unwrap_or("run")),
                output_dir.join("meta"),
                contract,
                BTreeMap::new(),
                RunOptions {
                    output_dir: output_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps,
                    llm_provider: None,
                    llm_seed_override: None,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
    }

    fn test_registry() -> StepRegistry {
        let mut registry = StepRegistry::new();
        registry.register(TestPassStep);
        registry.register(TestCheckFailStep);
        registry
    }

    struct TestPassStep;

    #[derive(Clone)]
    struct CountingPassStep {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StepExecutor for CountingPassStep {
        fn type_id(&self) -> &'static str {
            "test.pass"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(StepOutcome::Success {
                output: Some(json!({ "node": node.id })),
                files: vec![],
            })
        }
    }

    #[async_trait]
    impl StepExecutor for TestPassStep {
        fn type_id(&self) -> &'static str {
            "test.pass"
        }

        fn traits(&self) -> StepTraits {
            StepTraits {
                parallel_safe: true,
                ..StepTraits::default()
            }
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::Success {
                output: Some(json!({ "node": node.id })),
                files: vec![],
            })
        }
    }

    struct TestCheckFailStep;
    #[async_trait]
    impl StepExecutor for TestCheckFailStep {
        fn type_id(&self) -> &'static str {
            "test.check_fail"
        }

        fn traits(&self) -> StepTraits {
            StepTraits {
                parallel_safe: true,
                ..StepTraits::default()
            }
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::CheckFailed {
                findings: vec![Finding {
                    severity: qcg_types::Severity::Error,
                    message: "check failed".into(),
                    location: None,
                    raw_output: None,
                }],
                output: None,
                files: vec![],
            })
        }
    }

    #[derive(Clone)]
    struct FlakyStep {
        calls: Arc<AtomicUsize>,
        fail_first: usize,
        permanent: bool,
        slow_secs: u64,
    }

    #[async_trait]
    impl StepExecutor for FlakyStep {
        fn type_id(&self) -> &'static str {
            "test.flaky"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.slow_secs > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(self.slow_secs)).await;
            }
            if self.permanent || call < self.fail_first {
                return Err(StepError::failed(
                    &node.id,
                    format!("flaky failure {}", call + 1),
                ));
            }
            Ok(StepOutcome::Success {
                output: None,
                files: vec![],
            })
        }
    }

    #[derive(Clone)]
    struct IoFailStep {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StepExecutor for IoFailStep {
        fn type_id(&self) -> &'static str {
            "test.io_fail"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(StepError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected io failure",
            )))
        }
    }

    fn retry_node(
        id: &str,
        kind: &str,
        max_attempts: u32,
        backoff_ms: u64,
        timeout_secs: Option<u64>,
    ) -> NodeDef {
        let mut result = node(id, kind);
        result.retry = Some(RetryPolicy {
            max_attempts,
            backoff_ms,
            timeout_secs,
        });
        result
    }

    fn retry_event_count(events: &[Value], node_id: &str) -> usize {
        events
            .iter()
            .filter(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_retry")
                    && event.get("node").and_then(Value::as_str) == Some(node_id)
            })
            .count()
    }

    #[tokio::test]
    async fn retry_succeeds_after_transient_failures() {
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 2,
            permanent: false,
            slow_secs: 0,
        };
        let manifest = manifest(vec![retry_node("flaky", "test.flaky", 3, 0, None)]);
        let run_dir = temp_run_dir("retry-transient");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(flaky);
        Engine::new(registry)
            .run_with_id(
                "retry-transient".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                RunOptions {
                    output_dir: run_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps: 1,
                    llm_provider: None,
                    llm_seed_override: None,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect("transient failures within budget should succeed");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let events = journal_events(&run_dir);
        assert_eq!(retry_event_count(&events, "flaky"), 2);
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn retry_exhaustion_fails_after_max_attempts() {
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: true,
            slow_secs: 0,
        };
        let manifest = manifest(vec![retry_node("flaky", "test.flaky", 2, 0, None)]);
        let run_dir = temp_run_dir("retry-exhaustion");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(flaky);
        let error = Engine::new(registry)
            .run_with_id(
                "retry-exhaustion".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                RunOptions {
                    output_dir: run_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps: 1,
                    llm_provider: None,
                    llm_seed_override: None,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect_err("exhausted retries should fail the run");
        assert!(error.to_string().contains("flaky failure 2"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let events = journal_events(&run_dir);
        assert_eq!(retry_event_count(&events, "flaky"), 1);
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn non_execution_errors_fail_fast_without_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let manifest = manifest(vec![retry_node("iofail", "test.io_fail", 5, 0, None)]);
        let run_dir = temp_run_dir("retry-non-execution");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(IoFailStep {
            calls: Arc::clone(&calls),
        });
        Engine::new(registry)
            .run_with_id(
                "retry-non-execution".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                RunOptions {
                    output_dir: run_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps: 1,
                    llm_provider: None,
                    llm_seed_override: None,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect_err("non-execution errors should fail without retry");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = journal_events(&run_dir);
        assert_eq!(retry_event_count(&events, "iofail"), 0);
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn per_attempt_timeout_fails_slow_nodes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 30,
        };
        let manifest = manifest(vec![retry_node("slow", "test.flaky", 1, 0, Some(1))]);
        let run_dir = temp_run_dir("retry-timeout");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(flaky);
        let error = Engine::new(registry)
            .run_with_id(
                "retry-timeout".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                RunOptions {
                    output_dir: run_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps: 1,
                    llm_provider: None,
                    llm_seed_override: None,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect_err("slow nodes should hit the per-attempt timeout");
        assert!(error.to_string().contains("timed out after 1s"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn node_timeout_stops_only_the_node_and_adopts_cooperative_settlement() {
        // A09: a node timeout cancels a node-scoped stop signal, never the
        // whole run. A cooperatively settling node is adopted instead of
        // abandoned.
        struct CooperativeStep {
            observed_stop: Arc<AtomicBool>,
        }

        #[async_trait]
        impl StepExecutor for CooperativeStep {
            fn type_id(&self) -> &'static str {
                "test.cooperative"
            }

            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                tokio::select! {
                    _ = ctx.run.cancellation.cancelled() => {
                        self.observed_stop.store(true, Ordering::SeqCst);
                        Err(StepError::failed(&node.id, "stopped cooperatively"))
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        Ok(StepOutcome::Success {
                            output: None,
                            files: vec![],
                        })
                    }
                }
            }
        }

        let observed_stop = Arc::new(AtomicBool::new(false));
        let manifest = manifest(vec![retry_node(
            "cooperative",
            "test.cooperative",
            1,
            0,
            Some(1),
        )]);
        let run_dir = temp_run_dir("node-timeout-cooperative");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(CooperativeStep {
            observed_stop: Arc::clone(&observed_stop),
        });
        let run_cancellation = CancellationToken::new();
        let error = Engine::new(registry)
            .run_with_id(
                "node-timeout-cooperative".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                RunOptions {
                    output_dir: run_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps: 1,
                    llm_provider: None,
                    llm_seed_override: None,
                    cancellation: run_cancellation.clone(),
                },
            )
            .await
            .expect_err("cooperative stop still fails the timed-out node");
        assert!(
            error.to_string().contains("stopped cooperatively"),
            "grace-period settlement must be adopted, got: {error}"
        );
        assert!(
            observed_stop.load(Ordering::SeqCst),
            "node timeout must signal the node-scoped stop"
        );
        assert!(
            !run_cancellation.is_cancelled(),
            "node timeout must not cancel the whole run"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn sequential_step_records_started_before_execution_finishes() {
        struct GateStep;

        #[async_trait]
        impl StepExecutor for GateStep {
            fn type_id(&self) -> &'static str {
                "test.gate"
            }

            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                std::fs::create_dir_all(&ctx.run.workspace)
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                std::fs::write(ctx.run.workspace.join("started.marker"), "started")
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                for _ in 0..500 {
                    if ctx.run.workspace.join("release.marker").exists() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                if !ctx.run.workspace.join("release.marker").exists() {
                    return Err(StepError::failed(&node.id, "gate was never released"));
                }
                Ok(StepOutcome::Success {
                    output: None,
                    files: vec![],
                })
            }
        }

        let run_dir = temp_run_dir("step-started-order");
        let manifest = manifest(vec![node("gated", "test.gate")]);
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(GateStep);
        let engine = Engine::new(registry);
        let meta_dir = run_dir.join("meta");
        let workspace_dir = run_dir.join("workspace");
        let task = tokio::spawn(async move {
            engine
                .run_with_id(
                    "step-started-order".into(),
                    meta_dir,
                    contract,
                    BTreeMap::new(),
                    RunOptions {
                        output_dir: workspace_dir,
                        json_events: false,
                        event_sender: None,
                        interactive: false,
                        answers: BTreeMap::new(),
                        confirmations: BTreeMap::new(),
                        max_total_steps: 100,
                        max_parallel_steps: 1,
                        llm_provider: None,
                        llm_seed_override: None,
                        cancellation: CancellationToken::new(),
                    },
                )
                .await
        });
        // Wait until the step body is actually executing.
        let mut started = false;
        for _ in 0..500 {
            if run_dir.join("workspace/started.marker").exists() {
                started = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(started, "gated step should start executing");
        // The start event must already be journaled while the step is still
        // blocked, not after it finishes.
        assert!(
            has_event(&journal_events(&run_dir), "step_started", "gated"),
            "step_started must precede execution completion"
        );
        std::fs::write(run_dir.join("workspace/release.marker"), "go")
            .expect("release marker should be written");
        task.await
            .expect("run task should join")
            .expect("gated run should succeed");
        let events = journal_events(&run_dir);
        let started_seq = events
            .iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_started")
                    && event.get("node").and_then(Value::as_str) == Some("gated")
            })
            .and_then(|event| event.get("seq").and_then(Value::as_u64))
            .expect("step_started should carry seq");
        let finished_seq = events
            .iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_finished")
                    && event.get("node").and_then(Value::as_str) == Some("gated")
            })
            .and_then(|event| event.get("seq").and_then(Value::as_u64))
            .expect("step_finished should carry seq");
        assert!(
            started_seq < finished_seq,
            "step_started must order before step_finished"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    fn manifest(flow: Vec<NodeDef>) -> Manifest {
        Manifest {
            generator: GeneratorMeta {
                id: "scheduler-test".into(),
                name: "Scheduler Test".into(),
                version: "0.1.0".into(),
                description: String::new(),
                authors: vec![],
                qcg_version: String::new(),
            },
            permissions: Permissions {
                fs_write: vec!["workspace".into()],
                ..Permissions::default()
            },
            llm: None,
            inputs: InputSpec::default(),
            resources: BTreeMap::new(),
            tools: BTreeMap::new(),
            secrets: BTreeMap::new(),
            runtime: Default::default(),
            budget: Default::default(),
            flow,
            parallel: Vec::new(),
            blocks: BTreeMap::new(),
            outputs: OutputSpec { extras: vec![] },
            failure: FailurePolicy::default(),
            journal: JournalPolicy::default(),
            assets: AssetSpec::default(),
            dependencies: Default::default(),
        }
    }

    fn node(id: &str, kind: &str) -> NodeDef {
        NodeDef {
            id: id.into(),
            kind: StepType::from(kind),
            needs: vec![],
            when: None,
            on_deps: OnDeps::default(),
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: None,
            params: Default::default(),
        }
    }

    fn temp_run_dir(name: &str) -> Utf8PathBuf {
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-engine-{name}-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn journal_events(run_dir: &Utf8PathBuf) -> Vec<Value> {
        let source = std::fs::read_to_string(run_dir.join("meta/journal.jsonl"))
            .expect("journal should be readable");
        source
            .lines()
            .map(|line| serde_json::from_str(line).expect("journal line should be JSON"))
            .collect()
    }

    fn has_event(events: &[Value], kind: &str, node: &str) -> bool {
        events.iter().any(|event| {
            event.get("t").and_then(Value::as_str) == Some(kind)
                && event.get("node").and_then(Value::as_str) == Some(node)
        })
    }

    fn has_status(events: &[Value], kind: &str, node: &str, status: &str) -> bool {
        events.iter().any(|event| {
            event.get("t").and_then(Value::as_str) == Some(kind)
                && event.get("node").and_then(Value::as_str) == Some(node)
                && event.get("status").and_then(Value::as_str) == Some(status)
        })
    }
}
