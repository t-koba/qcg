mod checkpoint;
mod execute;
mod foreach;
mod repair;
mod repair_support;
mod replay;
mod run;
mod run_context;
mod types;

#[cfg(not(unix))]
pub(crate) use checkpoint::is_symlink_no_follow;
#[cfg(test)]
pub(crate) use checkpoint::*;
#[cfg(test)]
pub(crate) use replay::*;
pub(crate) use run_context::checkpoint_scope;
pub use run_context::{
    GuardDecision, OperationOutcome, bind_command_stdin, http_details_with_url,
    http_operation_details, safe_http_details, salted_binding_digest,
};
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
    use qcg_contract::HookErrorPolicy;
    use qcg_contract::{
        AssetSpec, Contract, ExhaustedAction, FailurePolicy, FieldType, GeneratorMeta, Graph,
        InputField, InputSpec, Manifest, NodeDef, OnDeps, OnFail, OutputSpec, Permissions,
        RetentionPolicy, RetryPolicy, RuntimeLimits, SecretRef, StepType,
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

    #[tokio::test]
    async fn resumed_file_inputs_reject_untracked_changes() {
        // E06: a file input whose path no successful step pinned must still
        // hold the admitted bytes on resume; silent tampering is refused.
        use crate::engine::types::{
            PreverifiedWorkspace, canonical_file_inputs, materialize_file_inputs_with_preverified,
        };
        let root = temp_run_dir("file-input-tamper");
        std::fs::create_dir_all(root.join("workspace")).expect("workspace should be created");
        let mut manifest = manifest(vec![]);
        manifest.inputs = serde_json::from_value(json!({
            "stages": [{
                "id": "basic",
                "fields": [{"id": "config_file", "type": "file", "required": true}],
            }],
        }))
        .expect("input spec should parse");
        let contract = Contract {
            root: root.clone(),
            manifest,
            graph: Graph {
                nodes: BTreeMap::new(),
                order: Vec::new(),
            },
            sha256: "sha256".into(),
        };
        let fs = crate::FsGateway::new(root.join("workspace"), &contract.manifest.permissions);
        let canonical = canonical_file_inputs(
            &contract,
            BTreeMap::from([(
                "config_file".to_string(),
                json!({"name": "config.txt", "text": "original"}),
            )]),
        )
        .expect("canonical inputs");
        let unpinned = BTreeMap::new();
        let no_history = BTreeMap::new();
        materialize_file_inputs_with_preverified(
            &contract,
            &canonical,
            &fs,
            false,
            &unpinned,
            &no_history,
            &PreverifiedWorkspace::new(),
        )
        .await
        .expect("first materialization should write the input");
        let input = root.join("workspace/files/config_file/config.txt");
        assert_eq!(
            std::fs::read(&input).expect("input should exist"),
            b"original"
        );
        std::fs::write(&input, b"tampered").expect("tamper should write");
        materialize_file_inputs_with_preverified(
            &contract,
            &canonical,
            &fs,
            true,
            &unpinned,
            &no_history,
            &PreverifiedWorkspace::new(),
        )
        .await
        .expect_err("an untracked input change must be refused on resume");
        // E06 ownership is key presence plus bytes comparison: the pinned
        // digest must hash to the live bytes, not just name the path.
        let tampered_digest = {
            use sha2::{Digest as _, Sha256};
            hex::encode(Sha256::digest(b"tampered"))
        };
        let pinned =
            BTreeMap::from([("files/config_file/config.txt".to_string(), tampered_digest)]);
        materialize_file_inputs_with_preverified(
            &contract,
            &canonical,
            &fs,
            true,
            &pinned,
            &no_history,
            &PreverifiedWorkspace::new(),
        )
        .await
        .expect("a journaled pin owns the current content");
        // A pinned path that disappeared must fail closed instead of being
        // rolled back to the admitted bytes (E06).
        std::fs::remove_file(&input).expect("remove pinned input");
        materialize_file_inputs_with_preverified(
            &contract,
            &canonical,
            &fs,
            true,
            &pinned,
            &no_history,
            &PreverifiedWorkspace::new(),
        )
        .await
        .expect_err("a missing pinned input must refuse resume");
        // A failed-step-only update records a historical pin without
        // becoming the latest revision; it must not be refused as
        // modified-without-pin on resume (E06). The historical digest must
        // hash to the live bytes.
        std::fs::write(&input, b"failed-step output").expect("failed output should write");
        let failed_digest = {
            use sha2::{Digest as _, Sha256};
            hex::encode(Sha256::digest(b"failed-step output"))
        };
        let historical = BTreeMap::from([(
            "files/config_file/config.txt".to_string(),
            std::collections::BTreeSet::from([failed_digest]),
        )]);
        materialize_file_inputs_with_preverified(
            &contract,
            &canonical,
            &fs,
            true,
            &unpinned,
            &historical,
            &PreverifiedWorkspace::new(),
        )
        .await
        .expect("a historical pin owns the current content on resume");
        // A historical pin that does not match the live bytes fails closed.
        std::fs::write(&input, b"forged").expect("forged output should write");
        materialize_file_inputs_with_preverified(
            &contract,
            &canonical,
            &fs,
            true,
            &unpinned,
            &historical,
            &PreverifiedWorkspace::new(),
        )
        .await
        .expect_err("a forged historical revision must refuse resume");
        let _ = std::fs::remove_dir_all(&root);
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
    async fn repair_node_honors_its_own_retry_policy() {
        // E10: a repair node is a node execution, so a transient failure
        // inside the repair step recovers through its retry policy instead
        // of aborting the repair cycle.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 1,
            permanent: false,
            slow_secs: 0,
        };
        let mut broken = node("broken", "test.check_fail");
        broken.on_fail = Some(OnFail::Repair {
            repair: "repair".into(),
            recheck: "recheck".into(),
            max_attempts: 1,
            on_exhausted: ExhaustedAction::Fail,
        });
        let manifest = manifest(vec![
            broken,
            retry_node("repair", "test.flaky", 2, 0, None),
            node("recheck", "test.pass"),
        ]);
        let run_dir = temp_run_dir("repair-retry");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(flaky);
        registry.register(TestPassStep);
        registry.register(TestCheckFailStep);
        let result = Engine::new(registry)
            .run_with_id(
                "repair-retry".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await;
        assert!(
            result.is_ok(),
            "repair must recover through retry: {result:?}"
        );
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "the repair node must have retried its transient failure"
        );
        let events = journal_events(&run_dir);
        assert!(has_status(&events, "step_finished", "broken", "repaired"));
    }

    struct TestForeachStep;

    #[async_trait]
    impl StepExecutor for TestForeachStep {
        fn type_id(&self) -> &'static str {
            "test.foreach"
        }

        fn traits(&self) -> crate::StepTraits {
            crate::StepTraits {
                parallel_safe: false,
                control_flow: crate::StepControlFlow::Foreach,
            }
        }

        fn validate(&self, _node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
            Ok(())
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Err(StepError::failed(
                &node.id,
                "foreach control flow must not execute its body",
            ))
        }
    }

    struct TestOutputStampStep;

    #[async_trait]
    impl StepExecutor for TestOutputStampStep {
        fn traits(&self) -> crate::StepTraits {
            // Pure in-memory test double: parallel iterations cannot
            // observe each other through it (E10).
            crate::StepTraits {
                parallel_safe: true,
                ..crate::StepTraits::default()
            }
        }

        fn type_id(&self) -> &'static str {
            "test.output_stamp"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::Success {
                output: Some(json!({"value": "ok"})),
                files: vec![],
            })
        }
    }

    struct AssertForeachVarsStep;

    #[async_trait]
    impl StepExecutor for AssertForeachVarsStep {
        fn traits(&self) -> crate::StepTraits {
            // Pure in-memory test double: parallel iterations cannot
            // observe each other through it (E10).
            crate::StepTraits {
                parallel_safe: true,
                ..crate::StepTraits::default()
            }
        }

        fn type_id(&self) -> &'static str {
            "test.assert_foreach_vars"
        }

        async fn execute(
            &self,
            ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            for index in 0..2 {
                let path = format!("steps.each[{index}]/child.output.value");
                if ctx.vars.get_path(&path).and_then(Value::as_str) != Some("ok") {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "parallel iteration output `{path}` was not merged into the parent scope"
                        ),
                    ));
                }
            }
            Ok(StepOutcome::Success {
                output: None,
                files: vec![],
            })
        }
    }

    struct TestNeedsUserStep;

    #[async_trait]
    impl StepExecutor for TestNeedsUserStep {
        fn traits(&self) -> crate::StepTraits {
            // Pure in-memory test double: parallel iterations cannot
            // observe each other through it (E10).
            crate::StepTraits {
                parallel_safe: true,
                ..crate::StepTraits::default()
            }
        }

        fn type_id(&self) -> &'static str {
            "test.needs_user"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::NeedsUser {
                question: qcg_api::FormSpec {
                    id: "repair-question".into(),
                    title: "Repair needs input".into(),
                    title_i18n: Default::default(),
                    fields: vec![],
                },
            })
        }
    }

    struct TestNeedsConfirmStep;

    #[async_trait]
    impl StepExecutor for TestNeedsConfirmStep {
        fn traits(&self) -> crate::StepTraits {
            // Pure in-memory test double: parallel iterations cannot
            // observe each other through it (E10).
            crate::StepTraits {
                parallel_safe: true,
                ..crate::StepTraits::default()
            }
        }

        fn type_id(&self) -> &'static str {
            "test.needs_confirm"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::NeedsConfirm {
                confirm: qcg_api::ConfirmSpec {
                    id: "foreach-confirm".into(),
                    title: "Confirm foreach child".into(),
                    kind: "command".into(),
                    target: "echo".into(),
                    dry_run: false,
                    details: None,
                    operation_digest: "a".repeat(64),
                    scope: qcg_contract::SideEffectScope::Invocation,
                },
            })
        }
    }

    struct TestCheckFailOnceStep {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StepExecutor for TestCheckFailOnceStep {
        fn type_id(&self) -> &'static str {
            "test.check_fail_once"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(StepOutcome::CheckFailed {
                    findings: vec![],
                    output: None,
                    files: vec![],
                });
            }
            Ok(StepOutcome::Success {
                output: None,
                files: vec![],
            })
        }
    }

    struct TestFailOnItemStep;

    #[async_trait]
    impl StepExecutor for TestFailOnItemStep {
        fn traits(&self) -> crate::StepTraits {
            // Pure in-memory test double: parallel iterations cannot
            // observe each other through it (E10).
            crate::StepTraits {
                parallel_safe: true,
                ..crate::StepTraits::default()
            }
        }

        fn type_id(&self) -> &'static str {
            "test.fail_on_item"
        }

        async fn execute(
            &self,
            ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            if ctx.vars.item().and_then(Value::as_str) == Some("a") {
                return Err(StepError::failed(&node.id, "item a must fail"));
            }
            Ok(StepOutcome::Success {
                output: None,
                files: vec![],
            })
        }
    }

    struct TestPanickingStep;

    #[async_trait]
    impl StepExecutor for TestPanickingStep {
        fn type_id(&self) -> &'static str {
            "test.panicking"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            panic!("intentional test panic");
        }
    }

    struct SlowCancellableStep;

    /// Parallel-safe step that always fails with its node id in the message,
    /// so joined wave failures stay distinguishable per node (E10).
    struct TestParallelFailStep;

    #[async_trait]
    impl StepExecutor for TestParallelFailStep {
        fn type_id(&self) -> &'static str {
            "test.parallel_fail"
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
            Err(StepError::failed(
                &node.id,
                format!("parallel failure at {}", node.id),
            ))
        }
    }

    /// Runs a real `sleep` through the command gateway: the per-attempt
    /// timeout must stop genuine child processes, not just bare test
    /// futures (E11). Unix-only: no `sleep` exists on Windows.
    #[cfg(unix)]
    struct TestCommandSleepStep;

    #[cfg(unix)]
    #[async_trait]
    impl StepExecutor for TestCommandSleepStep {
        fn type_id(&self) -> &'static str {
            "test.command_sleep"
        }

        async fn execute(
            &self,
            ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            let output = ctx
                .run
                .cmd
                .run_trusted_process(&["sleep".to_string(), "10".to_string()], 60, None)
                .await
                .map_err(|error| StepError::from_gateway(&node.id, error))?;
            Ok(StepOutcome::Success {
                output: Some(json!({ "status": output.status })),
                files: vec![],
            })
        }
    }

    /// Suspends for user input only after a delay, so the suspension lands
    /// inside the elapsed-deadline grace window instead of before it (E11).
    struct TestDelayedUserStep;

    #[async_trait]
    impl StepExecutor for TestDelayedUserStep {
        fn type_id(&self) -> &'static str {
            "test.delayed_user"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            Ok(StepOutcome::NeedsUser {
                question: qcg_api::FormSpec {
                    id: "delayed-question".into(),
                    title: "Delayed input".into(),
                    title_i18n: Default::default(),
                    fields: vec![],
                },
            })
        }
    }

    /// Pins the specified iteration namespacing (E10): per-iteration outputs
    /// live under their rewritten `each[i]/child` paths, while the block
    /// author's output alias never leaks into the shared namespace.
    struct AssertNoSharedAliasStep;

    #[async_trait]
    impl StepExecutor for AssertNoSharedAliasStep {
        fn type_id(&self) -> &'static str {
            "test.assert_no_shared_alias"
        }

        async fn execute(
            &self,
            ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            for index in 0..2 {
                let path = format!("steps.each[{index}]/child.output.value");
                if ctx.vars.get_path(&path).and_then(Value::as_str) != Some("ok") {
                    return Err(StepError::failed(
                        &node.id,
                        format!("iteration output `{path}` was not merged into the parent scope"),
                    ));
                }
            }
            if ctx.vars.get_path("steps.shared").is_some() {
                return Err(StepError::failed(
                    &node.id,
                    "the block output alias `shared` must not leak iteration outputs into the shared namespace",
                ));
            }
            Ok(StepOutcome::Success {
                output: None,
                files: vec![],
            })
        }
    }

    #[async_trait]
    impl StepExecutor for SlowCancellableStep {
        fn type_id(&self) -> &'static str {
            "test.slow_cancellable"
        }

        async fn execute(
            &self,
            ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    Ok(StepOutcome::Success {
                        output: None,
                        files: vec![],
                    })
                }
                _ = ctx.run.cancellation.cancelled() => Err(StepError::Cancelled),
            }
        }
    }

    fn foreach_node(
        id: &str,
        subflow: &str,
        items_expr: &str,
        parallel: i64,
        max_iterations: i64,
    ) -> NodeDef {
        let mut node = node(id, "test.foreach");
        node.params
            .insert("items".into(), toml::Value::String(items_expr.to_string()));
        node.params
            .insert("subflow".into(), toml::Value::String(subflow.to_string()));
        node.params
            .insert("parallel".into(), toml::Value::Integer(parallel));
        node.params.insert(
            "max_iterations".into(),
            toml::Value::Integer(max_iterations),
        );
        node
    }

    fn foreach_items_input() -> qcg_contract::InputSpec {
        serde_json::from_value(json!({
            "stages": [{
                "id": "basic",
                "fields": [{"id": "items", "type": "list", "item_type": "string", "required": true}],
            }],
        }))
        .expect("foreach input spec should parse")
    }

    async fn run_engine_with(
        manifest: Manifest,
        registry: StepRegistry,
        inputs: BTreeMap<String, serde_json::Value>,
        run_dir: Utf8PathBuf,
        max_total_steps: usize,
    ) -> Result<OutputManifest, EngineError> {
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        Engine::new(registry)
            .run_with_id(
                format!("test-{}", run_dir.file_name().unwrap_or("run")),
                run_dir.join("meta"),
                contract,
                inputs,
                RunOptions {
                    output_dir: run_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps,
                    max_parallel_steps: 1,
                    llm_provider: None,
                    llm_seed_override: None,
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
    }

    #[tokio::test]
    async fn foreach_parallel_children_share_the_run_budget() {
        // E10 single-charge rule: the outer foreach node is charged once
        // total; children share that budget without per-child consume.
        // Three children therefore fit a two-step budget (outer = 1), and
        // the shared counter still covers concurrent execution.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 0,
        };
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            3,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.flaky")]);
        let run_dir = temp_run_dir("foreach-budget");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(flaky);
        run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b", "c"]))]),
            run_dir.clone(),
            2,
        )
        .await
        .expect("three singly-charged children must fit a two-step budget under single-charge");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "all three children must execute under single-charge"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    struct UnsafeStep;

    #[async_trait]
    impl StepExecutor for UnsafeStep {
        fn type_id(&self) -> &'static str {
            "test.unsafe"
        }

        fn traits(&self) -> crate::StepTraits {
            crate::StepTraits {
                parallel_safe: false,
                ..crate::StepTraits::default()
            }
        }

        fn validate(&self, _node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
            Ok(())
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::Success {
                output: None,
                files: vec![],
            })
        }
    }

    #[tokio::test]
    async fn foreach_parallel_refuses_parallel_unsafe_children() {
        // E10: parallel iterations share the workspace, journal, and
        // budget, so a parallel-unsafe child is refused like a top-level
        // parallel wave refuses one. Sequential execution of the same
        // child stays allowed.
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.unsafe")]);
        let run_dir = temp_run_dir("foreach-unsafe");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(UnsafeStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("parallel-unsafe children must be refused");
        assert!(
            error.to_string().contains("not parallel-safe"),
            "the refusal must name the cause: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_parallel_merges_child_outputs_into_the_parent_scope() {
        // E10: parallel iterations run on cloned value bags, so their step
        // outputs must be merged back or later nodes cannot reference them.
        let mut manifest = manifest(vec![
            foreach_node("each", "each_body", "inputs.items", 2, 10),
            {
                let mut after = node("after", "test.assert_foreach_vars");
                after.needs = vec!["each".into()];
                after
            },
        ]);
        manifest.inputs = foreach_items_input();
        manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.output_stamp")]);
        let run_dir = temp_run_dir("foreach-parallel-vars");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(TestOutputStampStep);
        registry.register(AssertForeachVarsStep);
        run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect("parallel child outputs must be visible to later nodes");
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_parallel_children_retry_and_timeout() {
        // E10: parallel children run through the same retry wrapper as
        // top-level nodes, so a child's own max_attempts retries inside
        // the foreach. Per-attempt timeout still applies through the same
        // attempt path.
        use std::sync::atomic::{AtomicUsize, Ordering};
        // A transient child failure with max_attempts = 2 converges inside
        // the foreach without needing an outer retry.
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: std::sync::Arc::clone(&calls),
            fail_first: 1,
            permanent: false,
            slow_secs: 0,
        };
        let mut test_manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        test_manifest.inputs = foreach_items_input();
        test_manifest.blocks.insert(
            "each_body".into(),
            vec![retry_node("child", "test.flaky", 2, 0, None)],
        );
        let run_dir = temp_run_dir("foreach-parallel-retry");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(flaky);
        run_engine_with(
            test_manifest,
            registry,
            // Two items force the true parallel path (parallel=2 with
            // item_count > 1 takes the JoinSet arm, not the sequential
            // fast path) (E10).
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect("a child with max_attempts = 2 must converge inside the foreach (E10)");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "the shared flaky step fails once globally, so two parallel children need three calls total"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
        // The outer foreach node's own retry repeats the whole loop instead:
        // one transient failure plus one outer retry converges.
        let outer_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let outer_flaky = FlakyStep {
            calls: std::sync::Arc::clone(&outer_calls),
            fail_first: 1,
            permanent: false,
            slow_secs: 0,
        };
        let mut outer_manifest = manifest(vec![{
            let mut each = foreach_node("each", "each_body", "inputs.items", 1, 10);
            each.retry = Some(RetryPolicy {
                max_attempts: 2,
                backoff_ms: 0,
                timeout_secs: None,
                on_indeterminate: qcg_contract::RetryOnIndeterminate::Fail,
            });
            each
        }]);
        outer_manifest.inputs = foreach_items_input();
        outer_manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.flaky")]);
        let run_dir = temp_run_dir("foreach-outer-retry");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(outer_flaky);
        run_engine_with(
            outer_manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect("the outer foreach retry must repeat the loop to convergence");
        assert_eq!(
            outer_calls.load(Ordering::SeqCst),
            2,
            "one failed outer attempt plus one repeated loop"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
        // Single-charge budget (E10): the outer foreach node charges once
        // total; children share that budget without per-child consume
        // (`charge=false`). Budget 1 must pass; any per-child double
        // counting exceeds it.
        let exact_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let exact_flaky = FlakyStep {
            calls: std::sync::Arc::clone(&exact_calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 0,
        };
        let mut exact_manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        exact_manifest.inputs = foreach_items_input();
        exact_manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.flaky")]);
        let run_dir = temp_run_dir("foreach-parallel-exact-budget");
        let _temp_guard = TempGuard(run_dir.clone());
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(exact_flaky);
        run_engine_with(
            exact_manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            1,
        )
        .await
        .expect("outer-only single charge must fit a budget of 1 with 2 children");
        assert_eq!(
            exact_calls.load(Ordering::SeqCst),
            2,
            "each child must execute exactly once"
        );
        // Child timeout shorter than the parent applies per attempt. The
        // child sleeps past timeout+grace so the grace expires and the
        // timeout is recorded instead of adopting a late success (E10).
        let slow = FlakyStep {
            calls: std::sync::Arc::new(AtomicUsize::new(0)),
            fail_first: 0,
            permanent: false,
            slow_secs: 7,
        };
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "each_body".into(),
            vec![retry_node("child", "test.flaky", 1, 0, Some(1))],
        );
        let run_dir = temp_run_dir("foreach-parallel-timeout");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(slow);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("a 7s child past its 1s timeout must fail");
        assert!(
            matches!(error, EngineError::Step(StepError::TimedOut { .. })),
            "child timeout must surface strictly as TimedOut, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn repair_node_interaction_suspends_the_cycle() {
        // E10: a NeedsUser from the repair node must suspend and journal,
        // not be swallowed into the next repair attempt.
        let mut broken = node("broken", "test.check_fail");
        broken.on_fail = Some(OnFail::Repair {
            repair: "repair".into(),
            recheck: "recheck".into(),
            max_attempts: 2,
            on_exhausted: ExhaustedAction::Fail,
        });
        let manifest = manifest(vec![
            broken,
            node("repair", "test.needs_user"),
            node("recheck", "test.pass"),
        ]);
        let run_dir = temp_run_dir("repair-needs-user");
        let mut registry = StepRegistry::new();
        registry.register(TestNeedsUserStep);
        registry.register(TestPassStep);
        registry.register(TestCheckFailStep);
        let error = run_engine_with(manifest, registry, BTreeMap::new(), run_dir.clone(), 100)
            .await
            .expect_err("the repair interaction must suspend the run");
        assert!(
            matches!(error, EngineError::NeedsUser { .. }),
            "the interaction must surface as NeedsUser: {error}"
        );
        let events = journal_events(&run_dir);
        assert!(
            has_status(&events, "repair_attempt_finished", "broken", "needs_user"),
            "the suspension must be journaled on the repair cycle"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_fails_fast_and_drains_remaining_iterations() {
        // E10: the first failed iteration aborts the rest and drains them
        // before the loop returns, instead of buffering every outcome.
        let manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        let manifest = {
            let mut manifest = manifest;
            manifest.inputs = foreach_items_input();
            manifest
        };
        let run_dir = temp_run_dir("foreach-fail-fast");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(TestCheckFailStep);
        let _ = run_engine_with(
            {
                let mut manifest = manifest;
                manifest
                    .blocks
                    .insert("each_body".into(), vec![node("child", "test.check_fail")]);
                manifest
            },
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b", "c", "d"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("a failing iteration must fail the foreach");
        let events = journal_events(&run_dir);
        let started = events
            .iter()
            .filter(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_started")
                    && event
                        .get("node")
                        .and_then(Value::as_str)
                        .is_some_and(|node| node.contains("/child"))
            })
            .count();
        assert!(
            started < 4,
            "fail-fast must abort remaining iterations, started {started} of 4"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_child_elapsed_is_not_retried() {
        // E10: the run-wide elapsed deadline stops a foreach child with a
        // distinct, non-retryable error like any top-level node.
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            1,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.budget.max_elapsed_seconds = Some(1);
        manifest.blocks.insert(
            "each_body".into(),
            vec![node("child", "test.slow_cancellable")],
        );
        let run_dir = temp_run_dir("foreach-child-elapsed");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(SlowCancellableStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("elapsed must stop the child");
        assert!(
            matches!(error, EngineError::Step(StepError::ElapsedExceeded { .. })),
            "the child must report elapsed, not a retryable failure: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_error_drain_returns_the_first_failure() {
        // E10: a failing parallel iteration aborts and drains the loop, so
        // the run reports the first-arrived failure instead of buffering
        // every outcome.
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            3,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.fail_on_item")]);
        let run_dir = temp_run_dir("foreach-error-drain");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(TestFailOnItemStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b", "c"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("the failing item must fail the run");
        assert!(
            error.to_string().contains("item a must fail"),
            "the first failure must surface: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn regenerate_recovers_a_check_failed_node() {
        // E10: a check-failed node with an on_fail regenerate policy
        // re-executes through the same retry wrapper as top-level nodes.
        let calls = Arc::new(AtomicUsize::new(0));
        let recoverable = TestCheckFailOnceStep {
            calls: Arc::clone(&calls),
        };
        let mut unstable = node("unstable", "test.check_fail_once");
        unstable.on_fail = Some(OnFail::Regenerate {
            max_attempts: 2,
            on_exhausted: ExhaustedAction::Fail,
        });
        let manifest = manifest(vec![unstable]);
        let run_dir = temp_run_dir("regenerate-recovery");
        let mut registry = StepRegistry::new();
        registry.register(recoverable);
        run_engine_with(manifest, registry, BTreeMap::new(), run_dir.clone(), 100)
            .await
            .expect("regenerate must recover the node");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one check failure plus one regenerated execution"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_child_confirmation_suspends_the_loop() {
        // E10: a NeedsConfirm child suspends the whole foreach with the
        // child's confirmation instead of being buffered or skipped.
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "each_body".into(),
            vec![node("child", "test.needs_confirm")],
        );
        let run_dir = temp_run_dir("foreach-needs-confirm");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(TestNeedsConfirmStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("the child confirmation must suspend the loop");
        assert!(
            matches!(error, EngineError::NeedsConfirm { .. }),
            "the suspension must surface as NeedsConfirm: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_child_needs_user_suspends_the_loop() {
        // E10: a NeedsUser child suspends the whole foreach with the
        // child's question instead of being buffered or skipped.
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.needs_user")]);
        let run_dir = temp_run_dir("foreach-needs-user");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(TestNeedsUserStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("the child question must suspend the loop");
        assert!(
            matches!(error, EngineError::NeedsUser { .. }),
            "the suspension must surface as NeedsUser: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_panicking_child_fails_without_hanging() {
        // E10: a panicking iteration fails the run with a scheduler error
        // instead of hanging the join or losing the classification.
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            2,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest
            .blocks
            .insert("each_body".into(), vec![node("child", "test.panicking")]);
        let run_dir = temp_run_dir("foreach-panic");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(TestPanickingStep);
        // Two items force the parallel branch; a single item would
        // run sequentially and panic the caller directly.
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("a panicking child must fail the run");
        assert!(
            !error.is_canceled(),
            "a panic must not be misclassified as cancellation: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_child_honors_its_timeout() {
        // E10: a foreach child applies its own per-attempt timeout like a
        // top-level node and reports a distinct timeout error.
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            1,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "each_body".into(),
            vec![retry_node("child", "test.slow_cancellable", 1, 0, Some(1))],
        );
        let run_dir = temp_run_dir("foreach-child-timeout");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(SlowCancellableStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("the child must time out");
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::TimedOut {
                    timeout_secs: 1,
                    ..
                })
            ),
            "a foreach child timeout must classify as TimedOut: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_child_does_retry_internally() {
        // E10: a child's declared retry applies inside the foreach through
        // the same wrapper as top-level nodes (checklist acceptance:
        // max_attempts = 2 retries a transient failure).
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 1,
            permanent: false,
            slow_secs: 0,
        };
        let mut manifest = manifest(vec![foreach_node(
            "each",
            "each_body",
            "inputs.items",
            1,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "each_body".into(),
            vec![retry_node("child", "test.flaky", 2, 0, None)],
        );
        let run_dir = temp_run_dir("foreach-child-retry");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(flaky);
        run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect("the child must recover through its own retry (E10)");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one transient failure with max_attempts = 2 must execute the child twice"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_supports_nested_subflows() {
        // E10: a foreach node inside another foreach block runs the inner
        // loop for every outer item.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 0,
        };
        let mut manifest = manifest(vec![foreach_node(
            "outer",
            "outer_body",
            "inputs.items",
            1,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "outer_body".into(),
            vec![foreach_node("inner", "inner_body", "inputs.items", 1, 10)],
        );
        manifest
            .blocks
            .insert("inner_body".into(), vec![node("leaf", "test.flaky")]);
        let run_dir = temp_run_dir("foreach-nested");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(flaky);
        run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect("nested foreach must complete");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "the inner loop must run once per outer item and inner item"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_child_timeout_wins_over_parent_timeout() {
        // E10: a child timeout shorter than its foreach parent's stops the
        // child with the child's own budget.
        let mut foreach = foreach_node("each", "each_body", "inputs.items", 1, 10);
        foreach.retry = Some(RetryPolicy {
            max_attempts: 1,
            backoff_ms: 0,
            timeout_secs: Some(30),
            on_indeterminate: qcg_contract::RetryOnIndeterminate::Fail,
        });
        let mut manifest = manifest(vec![foreach]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "each_body".into(),
            vec![retry_node("child", "test.slow_cancellable", 1, 0, Some(1))],
        );
        let run_dir = temp_run_dir("foreach-child-timeout-wins");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(SlowCancellableStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("the child must time out before the parent budget");
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::TimedOut {
                    timeout_secs: 1,
                    ..
                })
            ),
            "the child timeout must win: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_nested_children_retry_inside() {
        // E10: nested foreach children apply their own retry at every
        // level, not just pass-through success.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 1,
            permanent: false,
            slow_secs: 0,
        };
        let mut manifest = manifest(vec![foreach_node(
            "outer",
            "outer_body",
            "inputs.items",
            1,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "outer_body".into(),
            vec![foreach_node("inner", "inner_body", "inputs.items", 1, 10)],
        );
        manifest.blocks.insert(
            "inner_body".into(),
            vec![retry_node("child", "test.flaky", 2, 0, None)],
        );
        let run_dir = temp_run_dir("foreach-nested-retry");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(flaky);
        run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect("nested children must recover through their own retry (E10)");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one transient failure in the nested child must execute twice"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_sequential_namespacing_and_absorb() {
        // E10: the sequential path clones and absorbs exactly like the
        // parallel path (deterministic, no shared-mutation divergence), and
        // the specified id-rewrite + output-None semantics hold: iteration
        // outputs live under `each[i]/child` while the block author's
        // `shared` alias never leaks into the parent scope.
        let mut manifest = manifest(vec![
            foreach_node("each", "each_body", "inputs.items", 1, 10),
            {
                let mut after = node("after", "test.assert_no_shared_alias");
                after.needs = vec!["each".into()];
                after
            },
        ]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "each_body".into(),
            vec![{
                let mut child = node("child", "test.output_stamp");
                child.output = Some("shared".into());
                child
            }],
        );
        let run_dir = temp_run_dir("foreach-sequential-namespace");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(TestOutputStampStep);
        registry.register(AssertNoSharedAliasStep);
        run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a", "b"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect("sequential iterations must absorb namespaced outputs without leaking the alias");
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn parallel_wave_reports_all_terminal_failures_joined() {
        // E10: every terminal failure of a parallel wave is collected and
        // reported joined, not first-only.
        let mut manifest = manifest(vec![
            node("a", "test.parallel_fail"),
            node("b", "test.parallel_fail"),
        ]);
        manifest.parallel = vec!["a".into(), "b".into()];
        let run_dir = temp_run_dir("parallel-wave-joined");
        let error = run_manifest(manifest, run_dir.clone(), 2)
            .await
            .expect_err("both parallel nodes fail");
        let message = error.to_string();
        assert!(
            message.contains("parallel failure at a") && message.contains("parallel failure at b"),
            "both sibling failures must be reported, got: {message}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn per_attempt_timeout_stops_a_real_command_process() {
        // E11: the per-attempt timeout stops a genuine child process run
        // through the real command gateway (not a bare test sleep) and
        // classifies the stop as a node timeout.
        let started = std::time::Instant::now();
        let manifest = manifest(vec![retry_node(
            "sleepy",
            "test.command_sleep",
            1,
            0,
            Some(1),
        )]);
        let run_dir = temp_run_dir("command-timeout");
        let mut registry = StepRegistry::new();
        registry.register(TestCommandSleepStep);
        let error = run_engine_with(manifest, registry, BTreeMap::new(), run_dir.clone(), 100)
            .await
            .expect_err("a 10s sleep past its 1s timeout must fail");
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::TimedOut {
                    timeout_secs: 1,
                    ..
                })
            ),
            "a real command timeout must classify as TimedOut: {error}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(8),
            "the timeout machinery must stop the child early instead of waiting out the sleep"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn elapsed_deadline_stops_a_real_command_process() {
        // E11: the run-wide elapsed deadline must stop a genuine child
        // process, not just cooperative test futures. A 10 s `sleep` under
        // `max_elapsed_seconds = 1` surfaces `ElapsedExceeded` (never
        // success, never a bare timeout misclassification) and returns
        // well before the sleep would finish.
        let started = std::time::Instant::now();
        let mut manifest = manifest(vec![node("sleepy", "test.command_sleep")]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("command-elapsed");
        let mut registry = StepRegistry::new();
        registry.register(TestCommandSleepStep);
        let error = run_engine_with(manifest, registry, BTreeMap::new(), run_dir.clone(), 100)
            .await
            .expect_err("a 10s sleep past the 1s elapsed deadline must fail");
        assert!(
            matches!(error, EngineError::Step(StepError::ElapsedExceeded { .. })),
            "a real command past the elapsed deadline must classify as ElapsedExceeded: {error}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(8),
            "the elapsed machinery must stop the child early instead of waiting out the sleep"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_parent_timeout_wins_over_child_timeout() {
        // E10: when the foreach node's own timeout and a child's timeout
        // race, the parent's shorter deadline fires and names the parent.
        let mut each = foreach_node("each", "each_body", "inputs.items", 1, 10);
        each.retry = Some(RetryPolicy {
            max_attempts: 1,
            backoff_ms: 0,
            timeout_secs: Some(1),
            on_indeterminate: qcg_contract::RetryOnIndeterminate::Fail,
        });
        let mut manifest = manifest(vec![each]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "each_body".into(),
            vec![retry_node("child", "test.slow_cancellable", 1, 0, Some(30))],
        );
        let run_dir = temp_run_dir("foreach-parent-timeout");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(SlowCancellableStep);
        let error = run_engine_with(
            manifest,
            registry,
            BTreeMap::from([("items".into(), json!(["a"]))]),
            run_dir.clone(),
            100,
        )
        .await
        .expect_err("the parent timeout must fire first");
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::TimedOut { ref node, .. }) if node == "each"
            ),
            "the parent timeout must name the foreach node, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_nested_parallel_runs_every_leaf() {
        // E10: nested parallel foreach loops (outer and inner both
        // parallel) run every leaf exactly once.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 0,
        };
        let mut manifest = manifest(vec![foreach_node(
            "outer",
            "outer_body",
            "inputs.items",
            2,
            10,
        )]);
        manifest.inputs = foreach_items_input();
        manifest.blocks.insert(
            "outer_body".into(),
            vec![foreach_node("inner", "inner_body", "inputs.items", 2, 10)],
        );
        manifest
            .blocks
            .insert("inner_body".into(), vec![node("leaf", "test.flaky")]);
        let run_dir = temp_run_dir("foreach-nested-parallel");
        let mut registry = StepRegistry::new();
        registry.register(TestForeachStep);
        registry.register(flaky);
        // Nested parallel waves need a wider scheduler lane than the
        // default single-step lane used by `run_engine_with`.
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        Engine::new(registry)
            .run_with_id(
                format!("test-{}", run_dir.file_name().unwrap_or("run")),
                run_dir.join("meta"),
                contract,
                BTreeMap::from([("items".into(), json!(["a", "b"]))]),
                RunOptions {
                    output_dir: run_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps: 8,
                    llm_provider: None,
                    llm_seed_override: None,
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect("nested parallel foreach must complete");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "two outer items times two inner items must run four leaves"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_timeout_charges_cooperative_and_uncooperative_once() {
        // E10: cooperative (cancellation-observing) and uncooperative
        // (grace-expiring) children both classify as TimedOut under the
        // single-charge rule: the outer foreach charges once total and
        // children share without per-child consume, so budget 1 proves no
        // double counting on either path.
        let cases = [
            ("foreach-timeout-cooperative", "test.slow_cancellable", {
                let mut registry = StepRegistry::new();
                registry.register(TestForeachStep);
                registry.register(SlowCancellableStep);
                registry
            }),
            ("foreach-timeout-uncooperative", "test.flaky", {
                let mut registry = StepRegistry::new();
                registry.register(TestForeachStep);
                registry.register(FlakyStep {
                    calls: Arc::new(AtomicUsize::new(0)),
                    fail_first: 0,
                    permanent: false,
                    slow_secs: 7,
                });
                registry
            }),
        ];
        for (name, child_kind, registry) in cases {
            let mut manifest = manifest(vec![foreach_node(
                "each",
                "each_body",
                "inputs.items",
                1,
                10,
            )]);
            manifest.inputs = foreach_items_input();
            manifest.blocks.insert(
                "each_body".into(),
                vec![retry_node("child", child_kind, 1, 0, Some(1))],
            );
            let run_dir = temp_run_dir(name);
            let error = run_engine_with(
                manifest,
                registry,
                BTreeMap::from([("items".into(), json!(["a"]))]),
                run_dir.clone(),
                1,
            )
            .await
            .expect_err("the child must time out on both paths");
            assert!(
                matches!(error, EngineError::Step(StepError::TimedOut { .. })),
                "{name}: both paths must classify as TimedOut, got: {error}"
            );
            let _ = std::fs::remove_dir_all(&run_dir);
        }
    }

    #[tokio::test]
    async fn elapsed_grace_preserves_interaction_with_marker() {
        // E11: a HITL suspension that lands inside the elapsed-deadline
        // grace window is delivered (not discarded) with an
        // elapsed-exceeded marker event.
        let mut manifest = manifest(vec![node("ask", "test.delayed_user")]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("elapsed-grace-interaction");
        let mut registry = StepRegistry::new();
        registry.register(TestDelayedUserStep);
        let error = run_engine_with(manifest, registry, BTreeMap::new(), run_dir.clone(), 100)
            .await
            .expect_err("the delayed suspension must surface");
        assert!(
            matches!(error, EngineError::NeedsUser { .. }),
            "the grace-period interaction must be preserved, got: {error}"
        );
        let events = journal_events(&run_dir);
        assert!(
            events
                .iter()
                .any(|event| event.get("t").and_then(Value::as_str) == Some("elapsed_exceeded")),
            "the preserved interaction must carry an elapsed-exceeded marker"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn startup_sweep_covers_the_current_prefix_only() {
        // E13/C-2: the startup sweep reaps the single current `.qcg-part-`
        // prefix. Retired prefixes were removed with no compat retention:
        // no current writer emits them.
        assert_eq!(
            super::run::STARTUP_SWEEP_FRAGMENTS,
            [".qcg-part-"],
            "only the current staging prefix must be swept at startup"
        );
        let base = std::env::temp_dir().join(format!(
            "qcg-startup-sweep-{}",
            uuid::Uuid::now_v7().as_simple()
        ));
        let workspace = camino::Utf8PathBuf::from_path_buf(base.join("workspace"))
            .expect("temporary path must be UTF-8");
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        for fragment in super::run::STARTUP_SWEEP_FRAGMENTS {
            let orphan = workspace.join(format!(".stale-{fragment}.tmp"));
            std::fs::write(&orphan, b"orphan").expect("orphan should be written");
            let past = std::time::SystemTime::now()
                .checked_sub(std::time::Duration::from_secs(7200))
                .expect("past time should exist");
            std::fs::File::options()
                .write(true)
                .open(&orphan)
                .expect("orphan should open")
                .set_modified(past)
                .expect("orphan should age");
            let removed = crate::FsGateway::sweep_orphaned_staging_files(
                &workspace,
                fragment,
                std::time::Duration::from_secs(3600),
            )
            .expect("sweep should succeed");
            assert_eq!(removed, 1, "orphans with `{fragment}` must be reaped");
        }
        let _ = std::fs::remove_dir_all(&base);
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
                    options_from: None,
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
                        run_refs: std::collections::BTreeMap::new(),
                        cancellation: CancellationToken::new(),
                        shutdown: None,
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
            run_refs: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            shutdown: None,
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
            diagnostics: Vec::new(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
    }

    fn test_registry() -> StepRegistry {
        let mut registry = StepRegistry::new();
        registry.register(TestPassStep);
        registry.register(TestCheckFailStep);
        registry.register(TestParallelFailStep);
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
        fn traits(&self) -> crate::StepTraits {
            // Pure in-memory test double: parallel iterations cannot
            // observe each other through it (E10).
            crate::StepTraits {
                parallel_safe: true,
                ..crate::StepTraits::default()
            }
        }

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
            on_indeterminate: qcg_contract::RetryOnIndeterminate::Fail,
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("slow nodes should hit the per-attempt timeout");
        assert!(error.to_string().contains("timed out after 1s"));
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::TimedOut {
                    timeout_secs: 1,
                    ..
                })
            ),
            "timeout must classify distinctly from ordinary failures, got: {error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn elapsed_budget_is_a_hard_deadline() {
        // E11: max_elapsed_seconds stops the running node with a distinct
        // error and never treats it as a retryable transient failure.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 30,
        };
        let mut manifest = manifest(vec![retry_node("slow", "test.flaky", 3, 0, None)]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("elapsed-deadline");
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
                "elapsed-deadline".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the run must stop at the elapsed limit");
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::ElapsedExceeded { limit_secs: 1, .. })
            ),
            "elapsed must classify distinctly from node timeout and cancel, got: {error}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the elapsed limit must not be retried as a transient failure"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn elapsed_deadline_binds_the_final_operation() {
        // E11 acceptance triple: a 2s side-effect-free FINAL operation with
        // a 10s node bound under max_elapsed_seconds=1 must surface
        // ElapsedExceeded, never success and never the node timeout. This
        // is the exact gap checkpoint-only enforcement would miss: no
        // checkpoint follows the last node.
        let calls = Arc::new(AtomicUsize::new(0));
        let slow_final = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 2,
        };
        let mut manifest = manifest(vec![retry_node("slow-final", "test.flaky", 1, 0, Some(10))]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("elapsed-final-op");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(slow_final);
        let error = Engine::new(registry)
            .run_with_id(
                "elapsed-final-op".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the final operation must not outlive the elapsed limit");
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::ElapsedExceeded { limit_secs: 1, .. })
            ),
            "the 1s elapsed limit must win over the 10s node bound, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn deadline_precedence_is_deterministic() {
        // E11: the earlier of node timeout and run elapsed deadline wins,
        // and a parent cancel wins over both.
        struct SlowStep;

        #[async_trait]
        impl StepExecutor for SlowStep {
            fn type_id(&self) -> &'static str {
                "test.slow_deadline"
            }

            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        Ok(StepOutcome::Success {
                            output: None,
                            files: vec![],
                        })
                    }
                    _ = ctx.run.cancellation.cancelled() => Err(StepError::Cancelled),
                }
            }
        }

        async fn run_case(
            name: &str,
            elapsed: Option<u64>,
            timeout: Option<u64>,
            cancel_after_ms: Option<u64>,
        ) -> EngineError {
            let mut manifest = manifest(vec![retry_node(
                "slow",
                "test.slow_deadline",
                1,
                0,
                timeout,
            )]);
            manifest.budget.max_elapsed_seconds = elapsed;
            let run_dir = temp_run_dir(name);
            let graph = Graph::build(&manifest).expect("test graph should build");
            let contract = Contract {
                root: run_dir.clone(),
                manifest,
                graph,
                sha256: "test".into(),
            };
            let mut registry = StepRegistry::new();
            registry.register(SlowStep);
            let cancellation = CancellationToken::new();
            if let Some(delay_ms) = cancel_after_ms {
                let token = cancellation.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    token.cancel();
                });
            }
            let result = Engine::new(registry)
                .run_with_id(
                    name.into(),
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
                        cancellation,
                        run_refs: std::collections::BTreeMap::new(),
                        shutdown: None,
                    },
                )
                .await
                .expect_err("the run must stop on a deadline");
            let _ = std::fs::remove_dir_all(&run_dir);
            result
        }

        // Elapsed (1s) fires before the node timeout (10s).
        let error = run_case("deadline-elapsed-first", Some(1), Some(10), None).await;
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::ElapsedExceeded { limit_secs: 1, .. })
            ),
            "the elapsed deadline must win over a later node timeout: {error}"
        );
        // Node timeout (1s) fires before the elapsed deadline (10s).
        let error = run_case("deadline-timeout-first", Some(10), Some(1), None).await;
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::TimedOut {
                    timeout_secs: 1,
                    ..
                })
            ),
            "the node timeout must win over a later elapsed deadline: {error}"
        );
        // A parent cancel wins over the elapsed deadline.
        let error = run_case("deadline-cancel-first", Some(10), Some(30), Some(300)).await;
        assert!(
            error.is_canceled(),
            "a parent cancel must win over a later deadline: {error}"
        );
    }

    #[tokio::test]
    async fn cancel_wins_over_retry_backoff() {
        // E11: a cancel during retry backoff must end the run promptly
        // instead of sleeping out the backoff to the next attempt.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: true,
            slow_secs: 0,
        };
        let manifest = manifest(vec![retry_node("flaky", "test.flaky", 5, 60_000, None)]);
        let run_dir = temp_run_dir("cancel-backoff");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(flaky);
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            token.cancel();
        });
        let started = std::time::Instant::now();
        let error = Engine::new(registry)
            .run_with_id(
                "cancel-backoff".into(),
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
                    cancellation,
                    run_refs: std::collections::BTreeMap::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the cancel must stop the run");
        assert!(
            error.is_canceled(),
            "cancel must classify as canceled: {error}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the cancel must interrupt backoff instead of waiting it out"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn elapsed_deadline_wins_over_retry_backoff() {
        // E11: the hard elapsed deadline must interrupt a retry backoff
        // instead of sleeping it out, and classify as ElapsedExceeded
        // (never retried), not as an ordinary failure.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: true,
            slow_secs: 0,
        };
        let mut manifest = manifest(vec![retry_node("flaky", "test.flaky", 5, 60_000, None)]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("elapsed-backoff");
        let _temp_guard = TempGuard(run_dir.clone());
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(flaky);
        let started = std::time::Instant::now();
        let error = Engine::new(registry)
            .run_with_id(
                "elapsed-backoff".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the elapsed deadline must stop the run");
        assert!(
            matches!(error, EngineError::Step(StepError::ElapsedExceeded { .. })),
            "the deadline must classify as elapsed, got: {error}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "the deadline must interrupt backoff instead of waiting it out"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn elapsed_grace_never_returns_success() {
        // E11: a cooperative finish inside the grace window is still past
        // the hard deadline, so the run reports elapsed instead of success.
        let calls = Arc::new(AtomicUsize::new(0));
        let flaky = FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 0,
            permanent: false,
            slow_secs: 2,
        };
        let mut manifest = manifest(vec![retry_node("slow", "test.flaky", 1, 0, None)]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("elapsed-grace");
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
                "elapsed-grace".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("a post-deadline success must still be elapsed");
        assert!(
            matches!(
                error,
                EngineError::Step(StepError::ElapsedExceeded { limit_secs: 1, .. })
            ),
            "the hard deadline must override a late success: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn parent_cancel_reaches_node_execution_promptly() {
        // B11: the node scope derives from the run token, so a parent
        // cancel stops node execution without waiting out any timeout.
        struct BlockingStep;

        #[async_trait]
        impl StepExecutor for BlockingStep {
            fn type_id(&self) -> &'static str {
                "test.blocking"
            }

            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                tokio::select! {
                    _ = ctx.run.cancellation.cancelled() => {
                        Err(StepError::Cancelled)
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

        let manifest = manifest(vec![retry_node("blocking", "test.blocking", 1, 0, None)]);
        let run_dir = temp_run_dir("parent-cancel-propagation");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(BlockingStep);
        let run_cancellation = CancellationToken::new();
        let canceller = run_cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            canceller.cancel();
        });
        let started = std::time::Instant::now();
        let error = Engine::new(registry)
            .run_with_id(
                "parent-cancel-propagation".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: run_cancellation,
                    shutdown: None,
                },
            )
            .await
            .expect_err("parent cancel must stop the run");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "parent cancel must not wait out the node work"
        );
        assert!(
            matches!(
                error,
                EngineError::Canceled | EngineError::Step(StepError::Cancelled)
            ),
            "parent cancel must surface as cancellation, got: {error}"
        );
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: run_cancellation.clone(),
                    shutdown: None,
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
    async fn deadline_cancel_normalizes_to_timed_out_for_retry() {
        // C06: a cooperative executor reports the child-scope stop as
        // cancellation. Without a parent cancel that must normalize to
        // TimedOut (retryable), never to run cancellation. Gateway
        // executors behave exactly this way on node timeout.
        struct CancelingStep {
            calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl StepExecutor for CancelingStep {
            fn type_id(&self) -> &'static str {
                "test.canceling"
            }

            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                tokio::select! {
                    _ = ctx.run.cancellation.cancelled() => {
                        Err(StepError::Cancelled)
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

        let calls = Arc::new(AtomicUsize::new(0));
        let manifest = manifest(vec![retry_node(
            "canceling",
            "test.canceling",
            2,
            0,
            Some(1),
        )]);
        let run_dir = temp_run_dir("deadline-cancel-normalizes");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(CancelingStep {
            calls: Arc::clone(&calls),
        });
        let error = Engine::new(registry)
            .run_with_id(
                "deadline-cancel-normalizes".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("exhausted timeout retries should fail the run");
        assert!(
            matches!(error, EngineError::Step(StepError::TimedOut { .. })),
            "deadline cooperative cancel must surface as timeout, got: {error}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "timeout must retry per policy instead of stopping as canceled"
        );
        let events = journal_events(&run_dir);
        assert_eq!(
            retry_event_count(&events, "canceling"),
            1,
            "one retry must be journaled"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn parent_cancel_during_deadline_stays_canceled() {
        // C06 counterpart: an actual parent cancel on the deadline path
        // keeps Cancelled — normalization must not rewrite it to TimedOut.
        struct CancelingStep;

        #[async_trait]
        impl StepExecutor for CancelingStep {
            fn type_id(&self) -> &'static str {
                "test.cancelingtwo"
            }

            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                tokio::select! {
                    _ = ctx.run.cancellation.cancelled() => {
                        Err(StepError::Cancelled)
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

        let manifest = manifest(vec![retry_node(
            "canceling-parent",
            "test.cancelingtwo",
            2,
            0,
            Some(30),
        )]);
        let run_dir = temp_run_dir("parent-cancel-during-deadline");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(CancelingStep);
        let run_cancellation = CancellationToken::new();
        let canceller = run_cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            canceller.cancel();
        });
        let error = Engine::new(registry)
            .run_with_id(
                "parent-cancel-during-deadline".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: run_cancellation,
                    shutdown: None,
                },
            )
            .await
            .expect_err("parent cancel must stop the run");
        assert!(
            matches!(
                error,
                EngineError::Canceled | EngineError::Step(StepError::Cancelled)
            ),
            "parent cancel must surface as cancellation, got: {error}"
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
                        run_refs: std::collections::BTreeMap::new(),
                        cancellation: CancellationToken::new(),
                        shutdown: None,
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
            retention: RetentionPolicy::default(),
            audit: qcg_policy::AuditConfig::default(),
            hooks: Default::default(),
            assets: AssetSpec::default(),
            dependencies: Default::default(),
        }
    }

    fn hook(id: &str, kind: &str, on_error: HookErrorPolicy) -> qcg_contract::HookDef {
        qcg_contract::HookDef {
            id: id.into(),
            kind: StepType::from(kind),
            on_error,
            retry: None,
            params: Default::default(),
        }
    }

    async fn run_hook_case(
        name: &str,
        on_error: HookErrorPolicy,
        registry: StepRegistry,
    ) -> (Utf8PathBuf, Result<OutputManifest, EngineError>) {
        let run_dir = temp_run_dir(name);
        let _ = std::fs::remove_dir_all(&run_dir);
        let mut manifest = manifest(vec![]);
        manifest
            .hooks
            .run_started
            .push(hook("notify", "test.check_fail", on_error));
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "hooks-test".into(),
        };
        let result = Engine::new(registry)
            .run_with_id(
                name.into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await;
        (run_dir, result)
    }

    #[tokio::test]
    async fn hook_failure_warn_continues_and_fail_stops_the_run() {
        let (run_dir, result) =
            run_hook_case("hook-warn", HookErrorPolicy::Warn, test_registry()).await;
        result.expect("a warn hook must not fail the run");
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(
            journal.contains("hook_failed") && journal.contains("\"policy\":\"warn\""),
            "the durable record states the policy: {journal}"
        );

        let (run_dir, result) =
            run_hook_case("hook-fail", HookErrorPolicy::Fail, test_registry()).await;
        let error = result.expect_err("a fail hook must fail the run");
        assert!(
            error.to_string().contains("hook `notify` failed"),
            "the error names the hook: {error}"
        );
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(journal.contains("hook_failed"), "{journal}");
    }

    #[tokio::test]
    async fn budget_exhausted_hooks_are_recorded_as_skipped() {
        let run_dir = temp_run_dir("hooks-budget-skip");
        let _ = std::fs::remove_dir_all(&run_dir);
        let mut manifest = manifest(vec![node("build", "test.pass")]);
        manifest.budget.max_steps = 1;
        manifest
            .hooks
            .run_succeeded
            .push(hook("after", "test.pass", HookErrorPolicy::Fail));
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "hooks-budget-skip".into(),
        };
        Engine::new(test_registry())
            .run_with_id(
                "hooks-budget-skip".into(),
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
                    max_total_steps: 1,
                    max_parallel_steps: 1,
                    llm_provider: None,
                    llm_seed_override: None,
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect("an exhausted hook must not fail a successful run");
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(
            journal.contains("hook_skipped") && journal.contains("\"reason\":\"budget\""),
            "the skip must be recorded durably: {journal}"
        );
        assert!(
            journal.contains("run_finished") && journal.contains("\"status\":\"success\""),
            "the run must still settle successfully: {journal}"
        );
    }

    #[tokio::test]
    async fn step_failed_hooks_observe_a_failed_settlement() {
        let run_dir = temp_run_dir("hooks-step-failed");
        let _ = std::fs::remove_dir_all(&run_dir);
        let mut manifest = manifest(vec![node("broken", "test.check_fail")]);
        manifest
            .hooks
            .step_failed
            .push(hook("report", "test.pass", HookErrorPolicy::Warn));
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "hooks-step-failed".into(),
        };
        Engine::new(test_registry())
            .run_with_id(
                "hooks-step-failed".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the broken node must still fail the run");
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(
            journal.contains("hook.step_failed.report"),
            "step_failed hooks must run during failed settlement: {journal}"
        );
        assert!(journal.contains("test.check_fail"));
    }

    #[tokio::test]
    async fn lifecycle_hooks_execute_once_and_replay_across_resume() {
        let run_dir = temp_run_dir("hooks-once");
        let _ = std::fs::remove_dir_all(&run_dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = StepRegistry::new();
        registry.register(CountingPassStep {
            calls: Arc::clone(&calls),
        });
        let mut manifest = manifest(vec![node("build", "test.pass")]);
        manifest
            .hooks
            .run_started
            .push(hook("seed", "test.pass", HookErrorPolicy::Warn));
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "hooks-once".into(),
        };
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
            run_refs: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            shutdown: None,
        };
        Engine::new(registry.clone())
            .run_with_id(
                "hooks-once".into(),
                run_dir.join("meta"),
                contract.clone(),
                BTreeMap::new(),
                options(),
            )
            .await
            .expect("first run should succeed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the hook and the flow step each execute once"
        );
        Engine::new(registry)
            .run_with_id(
                "hooks-once".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                options(),
            )
            .await
            .expect("resume should succeed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "replay must not re-execute a completed hook"
        );
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(journal.contains("hook.run_started.seed"), "{journal}");
    }

    #[tokio::test]
    async fn ordinary_execution_errors_reach_failure_hooks() {
        // F07-01: an ordinary step Err (not a CheckFailed outcome) must
        // still run step_failed/run_failed hooks exactly once each.
        let run_dir = temp_run_dir("hooks-ordinary-err");
        let _ = std::fs::remove_dir_all(&run_dir);
        let mut manifest = manifest(vec![node("broken", "test.parallel_fail")]);
        manifest
            .hooks
            .step_failed
            .push(hook("report", "test.pass", HookErrorPolicy::Warn));
        manifest
            .hooks
            .run_failed
            .push(hook("notify", "test.pass", HookErrorPolicy::Warn));
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "hooks-ordinary-err".into(),
        };
        Engine::new(test_registry())
            .run_with_id(
                "hooks-ordinary-err".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the broken node must still fail the run");
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert_eq!(
            journal.matches("\"t\":\"step_started\"").count(),
            3,
            "broken node + step_failed hook + run_failed hook must each start once: {journal}"
        );
        assert!(
            journal.contains("\"status\":\"failed\""),
            "the failure must settle durably: {journal}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn foreach_single_charge_matches_live_and_fold() {
        // F13-02: the single-charge rule (one outer charge, children share
        // it) must read identically live and after a journal fold: five
        // children journal six step_started events but a single
        // budget_charged delta, and recovery seeds from the delta.
        struct ForeachPassKind;
        #[async_trait]
        impl StepExecutor for ForeachPassKind {
            fn type_id(&self) -> &'static str {
                "test.foreach"
            }
            fn traits(&self) -> StepTraits {
                StepTraits {
                    parallel_safe: false,
                    control_flow: crate::StepControlFlow::Foreach,
                }
            }
            async fn execute(
                &self,
                _ctx: &mut StepContext<'_>,
                node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                Err(StepError::failed(
                    &node.id,
                    "foreach must not execute its body",
                ))
            }
        }
        let run_dir = temp_run_dir("foreach-charge");
        let _ = std::fs::remove_dir_all(&run_dir);
        let mut flow_manifest = manifest(vec![foreach_node("each", "body", "inputs.items", 1, 10)]);
        flow_manifest.inputs = foreach_items_input();
        flow_manifest
            .blocks
            .insert("body".into(), vec![node("child", "test.pass")]);
        let graph = Graph::build(&flow_manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest: flow_manifest,
            graph,
            sha256: "foreach-charge".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(ForeachPassKind);
        registry.register(TestPassStep);
        Engine::new(registry)
            .run_with_id(
                "foreach-charge".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::from([("items".into(), json!(["a", "b", "c", "d", "e"]))]),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect("five passing children must succeed");
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert_eq!(
            journal.matches("\"t\":\"budget_charged\"").count(),
            1,
            "single-charge: one delta for the whole foreach: {journal}"
        );
        assert_eq!(
            journal.matches("\"t\":\"step_started\"").count(),
            6,
            "observational count still sees outer plus five children: {journal}"
        );
        let state =
            RunState::fold_journal(&run_dir.join("meta/journal.jsonl")).expect("fold must succeed");
        assert_eq!(
            state.budget.budget_charged, 1,
            "fold must recover one charge"
        );
        assert!(
            state.budget.has_budget_charges,
            "resume must seed from the delta, not the six starts"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn budget_charges_match_attempts_live_and_after_fold() {
        // F13-01: three attempts on one node charge three budget units
        // live; folding the journal yields the same consumption so a
        // resume (SIGKILL/HITL) sees identical remaining budget.
        let run_dir = temp_run_dir("budget-charges");
        let _ = std::fs::remove_dir_all(&run_dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = StepRegistry::new();
        registry.register(FlakyStep {
            calls: Arc::clone(&calls),
            fail_first: 2,
            permanent: false,
            slow_secs: 0,
        });
        let flow_manifest = manifest(vec![retry_node("flaky", "test.flaky", 3, 0, None)]);
        let graph = Graph::build(&flow_manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest: flow_manifest,
            graph,
            sha256: "budget-charges".into(),
        };
        Engine::new(registry)
            .run_with_id(
                "budget-charges".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect("three attempts must succeed on the last try");
        assert_eq!(calls.load(Ordering::SeqCst), 3, "three attempts must run");
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert_eq!(
            journal.matches("\"t\":\"budget_charged\"").count(),
            3,
            "each attempt must journal its charge: {journal}"
        );
        let state =
            RunState::fold_journal(&run_dir.join("meta/journal.jsonl")).expect("fold must succeed");
        assert_eq!(
            state.budget.budget_charged, 3,
            "fold must recover 3 charges"
        );
        assert!(
            state.budget.has_budget_charges,
            "new journals must seed from charges, not event counts"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn budget_boundary_exact_succeeds_and_over_fails() {
        // F13-03: at exactly the limit the run succeeds; one over fails
        // through shared settlement with the budget cause preserved.
        // Metrics meaning: steps_executed counts starts (observational),
        // budget_charged drives enforcement (F13).
        let run_case = |name: &str, max_total_steps: usize| {
            let run_dir = temp_run_dir(name);
            let _ = std::fs::remove_dir_all(&run_dir);
            let flow_manifest = manifest(vec![
                node("first", "test.pass"),
                node("second", "test.pass"),
            ]);
            let graph = Graph::build(&flow_manifest).expect("test graph should build");
            let contract = Contract {
                root: run_dir.clone(),
                manifest: flow_manifest,
                graph,
                sha256: "budget-boundary".into(),
            };
            let mut registry = StepRegistry::new();
            registry.register(TestPassStep);
            (run_dir, contract, registry, max_total_steps)
        };
        let run_options = |run_dir: &Utf8PathBuf, max_total_steps: usize| RunOptions {
            output_dir: run_dir.join("workspace"),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            max_total_steps,
            max_parallel_steps: 1,
            llm_provider: None,
            llm_seed_override: None,
            run_refs: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            shutdown: None,
        };
        let (ok_dir, ok_contract, ok_registry, _) = run_case("budget-exact", 2);
        Engine::new(ok_registry)
            .run_with_id(
                "budget-exact".into(),
                ok_dir.join("meta"),
                ok_contract,
                BTreeMap::new(),
                run_options(&ok_dir, 2),
            )
            .await
            .expect("exactly at the limit must succeed");
        let state =
            RunState::fold_journal(&ok_dir.join("meta/journal.jsonl")).expect("fold must succeed");
        assert_eq!(state.budget.budget_charged, 2);
        assert_eq!(state.budget.steps_executed, 2);
        let _ = std::fs::remove_dir_all(&ok_dir);
        let (over_dir, over_contract, over_registry, _) = run_case("budget-over", 1);
        let error = Engine::new(over_registry)
            .run_with_id(
                "budget-over".into(),
                over_dir.join("meta"),
                over_contract,
                BTreeMap::new(),
                run_options(&over_dir, 1),
            )
            .await
            .expect_err("one over the limit must fail");
        assert!(
            error.to_string().contains("budget"),
            "the cause must stay a budget error: {error}"
        );
        let journal =
            std::fs::read_to_string(over_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(
            journal.contains("\"status\":\"failed\""),
            "over-limit must settle durably: {journal}"
        );
        let _ = std::fs::remove_dir_all(&over_dir);
    }

    #[tokio::test]
    async fn parallel_hitl_interruption_resumes_siblings() {
        // F12-01: a fast suspending step plus a slow side-effect-free step
        // in one wave: the answer resumes the run and the interrupted
        // sibling completes instead of staying permanently failed.
        struct AnswerOnceStep;
        #[async_trait]
        impl StepExecutor for AnswerOnceStep {
            fn type_id(&self) -> &'static str {
                "test.answer_once"
            }
            fn traits(&self) -> StepTraits {
                StepTraits {
                    parallel_safe: true,
                    ..StepTraits::default()
                }
            }
            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                if ctx.run.answers.contains_key("q-fast") {
                    return Ok(StepOutcome::Success {
                        output: Some(json!({ "answered": true })),
                        files: vec![],
                    });
                }
                Ok(StepOutcome::NeedsUser {
                    question: qcg_api::FormSpec {
                        id: "q-fast".into(),
                        title: "fast question".into(),
                        title_i18n: Default::default(),
                        fields: vec![],
                    },
                })
            }
        }
        struct SlowPassStep;
        #[async_trait]
        impl StepExecutor for SlowPassStep {
            fn type_id(&self) -> &'static str {
                "test.slow_pass"
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
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                Ok(StepOutcome::Success {
                    output: Some(json!({ "node": node.id })),
                    files: vec![],
                })
            }
        }
        let run_dir = temp_run_dir("parallel-hitl-resume");
        let _ = std::fs::remove_dir_all(&run_dir);
        let mut registry = StepRegistry::new();
        registry.register(AnswerOnceStep);
        registry.register(SlowPassStep);
        let mut flow_manifest = manifest(vec![
            node("fast", "test.answer_once"),
            node("slow", "test.slow_pass"),
        ]);
        flow_manifest.parallel = vec!["fast".into(), "slow".into()];
        let graph = Graph::build(&flow_manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest: flow_manifest,
            graph,
            sha256: "parallel-hitl".into(),
        };
        let options = |answers: BTreeMap<String, Value>| RunOptions {
            output_dir: run_dir.join("workspace"),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers,
            confirmations: BTreeMap::new(),
            max_total_steps: 100,
            max_parallel_steps: 2,
            llm_provider: None,
            llm_seed_override: None,
            run_refs: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            shutdown: None,
        };
        let error = Engine::new(registry.clone())
            .run_with_id(
                "parallel-hitl".into(),
                run_dir.join("meta"),
                contract.clone(),
                BTreeMap::new(),
                options(BTreeMap::new()),
            )
            .await
            .expect_err("the fast question must suspend the wave");
        assert!(
            matches!(error, EngineError::NeedsUser { .. }),
            "the wave must suspend, not fail: {error}"
        );
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(
            journal.contains("step_interrupted") && journal.contains("\"node\":\"slow\""),
            "the aborted sibling must be resumable, not failed: {journal}"
        );
        assert!(
            !journal.contains("\"node\":\"slow\",\"status\":\"failed\"")
                && !journal.contains("\"node\": \"slow\", \"status\": \"failed\"")
                && !journal.contains("\"status\":\"failed\",\"reason\"")
                || !journal
                    .lines()
                    .any(|line| line.contains("\"node\":\"slow\"") && line.contains("\"failed\"")),
            "the aborted sibling must not journal failure: {journal}"
        );
        // Answer and resume: both steps complete and the run succeeds.
        let mut answers = BTreeMap::new();
        answers.insert("q-fast".to_string(), json!({ "answer": "yes" }));
        Engine::new(registry)
            .run_with_id(
                "parallel-hitl".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                options(answers),
            )
            .await
            .expect("the resumed wave must succeed");
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(
            journal.contains("\"status\":\"success\""),
            "the resumed run must settle successfully: {journal}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn sequential_and_parallel_waves_agree_on_success() {
        // F12-03: the same two side-effect-free steps succeed identically
        // sequential and parallel; completed results are kept in both
        // paths with no duplication.
        struct EchoStep;
        #[async_trait]
        impl StepExecutor for EchoStep {
            fn type_id(&self) -> &'static str {
                "test.echo"
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
        let run_case = |name: &str, parallel: Vec<String>| {
            let run_dir = temp_run_dir(name);
            let _ = std::fs::remove_dir_all(&run_dir);
            let mut flow_manifest = manifest(vec![node("a", "test.echo"), node("b", "test.echo")]);
            flow_manifest.parallel = parallel;
            let graph = Graph::build(&flow_manifest).expect("test graph should build");
            let contract = Contract {
                root: run_dir.clone(),
                manifest: flow_manifest,
                graph,
                sha256: "wave-agree".into(),
            };
            let mut registry = StepRegistry::new();
            registry.register(EchoStep);
            (run_dir, contract, registry)
        };
        let run_options = |run_dir: &Utf8PathBuf| RunOptions {
            output_dir: run_dir.join("workspace"),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            max_total_steps: 100,
            max_parallel_steps: 2,
            llm_provider: None,
            llm_seed_override: None,
            run_refs: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            shutdown: None,
        };
        let (seq_dir, seq_contract, seq_registry) = run_case("wave-seq", vec![]);
        let seq_out = Engine::new(seq_registry)
            .run_with_id(
                "wave-seq".into(),
                seq_dir.join("meta"),
                seq_contract,
                BTreeMap::new(),
                run_options(&seq_dir),
            )
            .await
            .expect("sequential wave must succeed");
        let (par_dir, par_contract, par_registry) =
            run_case("wave-par", vec!["a".into(), "b".into()]);
        let par_out = Engine::new(par_registry)
            .run_with_id(
                "wave-par".into(),
                par_dir.join("meta"),
                par_contract,
                BTreeMap::new(),
                run_options(&par_dir),
            )
            .await
            .expect("parallel wave must succeed");
        assert_eq!(
            seq_out.artifacts.len(),
            par_out.artifacts.len(),
            "both paths must settle the same outputs"
        );
        for (dir, name) in [(&seq_dir, "seq"), (&par_dir, "par")] {
            let journal =
                std::fs::read_to_string(dir.join("meta/journal.jsonl")).expect("journal reads");
            assert_eq!(
                journal.matches("\"t\":\"step_finished\"").count(),
                2,
                "{name}: each step finishes exactly once"
            );
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[tokio::test]
    async fn warned_run_started_hook_does_not_rerun_after_hitl_resume() {
        // F07-02: a Warn-ended run_started hook is processed; a later HITL
        // resume must not re-execute it.
        let run_dir = temp_run_dir("hooks-warn-hitl");
        let _ = std::fs::remove_dir_all(&run_dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = StepRegistry::new();
        registry.register(CountingPassStep {
            calls: Arc::clone(&calls),
        });
        registry.register(TestCheckFailStep);
        let mut manifest = manifest(vec![node("build", "test.pass")]);
        // A CheckFailed hook under Warn settles as warned (not success).
        manifest
            .hooks
            .run_started
            .push(hook("seed", "test.check_fail", HookErrorPolicy::Warn));
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "hooks-warn-hitl".into(),
        };
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
            run_refs: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            shutdown: None,
        };
        // First run succeeds; the Warn hook settles (Warn continues).
        Engine::new(registry.clone())
            .run_with_id(
                "hooks-warn-hitl".into(),
                run_dir.join("meta"),
                contract.clone(),
                BTreeMap::new(),
                options(),
            )
            .await
            .expect("first run should succeed");
        let first_calls = calls.load(Ordering::SeqCst);
        // Resume replays without re-executing the settled hook.
        Engine::new(registry)
            .run_with_id(
                "hooks-warn-hitl".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                options(),
            )
            .await
            .expect("resume should succeed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            first_calls,
            "a settled hook must not re-execute on resume"
        );
        let journal =
            std::fs::read_to_string(run_dir.join("meta/journal.jsonl")).expect("journal reads");
        assert!(
            journal.contains("hook_replayed") && journal.contains("hook_failed"),
            "the warned hook must settle once and replay by marker: {journal}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
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

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04).
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            // Best-effort test cleanup documented here: temp removal cannot
            // propagate from `Drop`, and explicit tail cleanups below are
            // the same intentional best-effort (E13).
            let _ = std::fs::remove_dir_all(self.0.as_std_path());
        }
    }

    fn journal_events(run_dir: &Utf8PathBuf) -> Vec<Value> {
        // Public record view: durable and observation streams merged by seq
        // (ADR 0001). Folds that build RunState read journal.jsonl directly.
        let read = |path: &Utf8PathBuf| -> Vec<Value> {
            std::fs::read_to_string(path)
                .expect("journal should be readable")
                .lines()
                .map(|line| serde_json::from_str(line).expect("journal line should be JSON"))
                .collect()
        };
        let mut events = read(&run_dir.join("meta/journal.jsonl"));
        let audit = run_dir.join("meta/audit.jsonl");
        if audit.exists() {
            events.extend(read(&audit));
            events.sort_by_key(|event| event.get("seq").and_then(Value::as_u64));
        }
        events
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

    struct TestFileUpdateStep;

    #[async_trait]
    impl StepExecutor for TestFileUpdateStep {
        fn type_id(&self) -> &'static str {
            "test.file_update"
        }

        fn traits(&self) -> StepTraits {
            StepTraits {
                parallel_safe: true,
                ..StepTraits::default()
            }
        }

        async fn execute(
            &self,
            ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            // Real workspace write through the gateway-adjacent path: the
            // step updates the file-input workspace file, which the resume
            // verifier must keep (not roll back to the upload bytes).
            let relative = "files/config_file/config.txt";
            let target = ctx
                .run
                .fs
                .resolve_internal_write(relative)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            std::fs::create_dir_all(
                target
                    .parent()
                    .expect("file input target must have a parent"),
            )
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            std::fs::write(target.as_std_path(), b"updated")
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            Ok(StepOutcome::Success {
                output: Some(json!({"updated": true})),
                files: vec![target],
            })
        }
    }

    #[tokio::test]
    async fn file_input_update_survives_resume_end_to_end() {
        // E06: a real step updating a file input, then resume, keeps the
        // update. The first run writes `updated` and pins it; the second
        // run with the same run id replays without rolling back to the
        // original upload bytes.
        use crate::engine::types::canonical_file_inputs;
        let run_dir = temp_run_dir("file-input-e2e");
        let mut manifest = manifest(vec![node("updater", "test.file_update")]);
        manifest.inputs = serde_json::from_value(json!({
            "stages": [{
                "id": "basic",
                "fields": [{"id": "config_file", "type": "file", "required": true}],
            }],
        }))
        .expect("input spec should parse");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let base_manifest = manifest.clone();
        let base_graph = graph.clone();
        let canonical = canonical_file_inputs(
            &Contract {
                root: run_dir.clone(),
                manifest: base_manifest.clone(),
                graph: base_graph.clone(),
                sha256: "test".into(),
            },
            BTreeMap::from([(
                "config_file".to_string(),
                json!({"name": "config.txt", "text": "original"}),
            )]),
        )
        .expect("canonical inputs");
        let mut registry = StepRegistry::new();
        registry.register(TestFileUpdateStep);
        let run_id = format!("file-e2e-{}", run_dir.file_name().unwrap_or("run"));
        Engine::new(registry.clone())
            .run_with_id(
                run_id.clone(),
                run_dir.join("meta"),
                Contract {
                    root: run_dir.clone(),
                    manifest: base_manifest.clone(),
                    graph: base_graph.clone(),
                    sha256: "test".into(),
                },
                canonical.clone(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect("first run should succeed");
        let updated = std::fs::read(run_dir.join("workspace/files/config_file/config.txt"))
            .expect("updated file should exist");
        assert_eq!(updated, b"updated", "the step must have updated the input");
        // Resume with the same run id: the replay verifier plus input
        // placement must keep `updated`, never restore `original`.
        Engine::new(registry)
            .run_with_id(
                run_id,
                run_dir.join("meta"),
                Contract {
                    root: run_dir.clone(),
                    manifest: base_manifest,
                    graph: base_graph,
                    sha256: "test".into(),
                },
                canonical,
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect("resume should succeed");
        let kept = std::fs::read(run_dir.join("workspace/files/config_file/config.txt"))
            .expect("resumed file should exist");
        assert_eq!(kept, b"updated", "resume must keep the step update");
        let events = journal_events(&run_dir);
        assert!(
            events
                .iter()
                .any(|event| event.get("t").and_then(Value::as_str) == Some("run_resumed")),
            "a successful resume must journal run_resumed after validation"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn run_resumed_follows_successful_verification_only() {
        // E06: `run_resumed` is appended only after verification and input
        // placement succeed, so a failed resume never grows the journal with
        // a marker. Tamper a pinned workspace file between runs: resume is
        // refused fail-closed with no new `run_resumed` event and no journal
        // growth. Real temp fs, no mocks.
        struct PinnedOutputStep;
        #[async_trait]
        impl StepExecutor for PinnedOutputStep {
            fn type_id(&self) -> &'static str {
                "test.pinned_output"
            }
            fn traits(&self) -> StepTraits {
                StepTraits {
                    parallel_safe: true,
                    ..StepTraits::default()
                }
            }
            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                // Real workspace write through the internal placement path;
                // the engine pins the file, so later tampering must refuse
                // resume instead of silently continuing.
                let relative = "pinned.txt";
                let target = ctx
                    .run
                    .fs
                    .resolve_internal_write(relative)
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent.as_std_path())
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                }
                std::fs::write(target.as_std_path(), b"pinned-original")
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Ok(StepOutcome::Success {
                    output: Some(json!({"wrote": true})),
                    files: vec![target],
                })
            }
        }
        let run_dir = temp_run_dir("resume-marker-order");
        let manifest = manifest(vec![node("writer", "test.pinned_output")]);
        let graph = Graph::build(&manifest).expect("test graph should build");
        let run_id = format!("resume-order-{}", run_dir.file_name().unwrap_or("run"));
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
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
            run_refs: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            shutdown: None,
        };
        let mut registry = StepRegistry::new();
        registry.register(PinnedOutputStep);
        Engine::new(registry.clone())
            .run_with_id(
                run_id.clone(),
                run_dir.join("meta"),
                contract.clone(),
                BTreeMap::new(),
                options(),
            )
            .await
            .expect("first run should succeed");
        let journal_path = run_dir.join("meta/journal.jsonl");
        let before_text = std::fs::read_to_string(journal_path.as_std_path())
            .expect("journal should be readable");
        let before_lines = before_text.lines().count();
        let before_resumed = before_text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("run_resumed"))
            .count();
        assert_eq!(before_resumed, 0, "fresh runs never journal run_resumed");
        // Tamper the pinned workspace file: resume must fail before any new
        // `run_resumed` marker instead of silently continuing on forged bytes.
        std::fs::write(
            run_dir.join("workspace/pinned.txt").as_std_path(),
            b"forged",
        )
        .expect("tamper should write");
        let error = Engine::new(registry)
            .run_with_id(
                run_id,
                run_dir.join("meta"),
                contract,
                BTreeMap::new(),
                options(),
            )
            .await
            .expect_err("a tampered pinned file must refuse resume");
        assert!(
            error.to_string().contains("cannot safely resume"),
            "tampered resume must fail closed, got: {error}"
        );
        let after_text = std::fs::read_to_string(journal_path.as_std_path())
            .expect("journal should be readable");
        assert_eq!(
            after_text.lines().count(),
            before_lines,
            "a refused resume must not grow the journal"
        );
        assert!(
            !after_text
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .any(|event| event.get("t").and_then(Value::as_str) == Some("run_resumed")),
            "a refused resume must not journal run_resumed"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn parallel_check_failed_journals_failed_evidence() {
        // E06: the parallel wave shares the single failed-evidence helper
        // with the sequential loop. A parallel `check_failed` journals
        // `failed_output` / `failed_files` and never the success-notation
        // `output` / `files` keys. Real temp fs, no mocks.
        let run_dir = temp_run_dir("parallel-failed-evidence");
        let mut manifest = manifest(vec![
            node("p1", "test.check_fail"),
            node("p2", "test.check_fail"),
        ]);
        manifest.parallel = vec!["p1".into(), "p2".into()];
        run_manifest(manifest, run_dir.clone(), 2)
            .await
            .expect_err("parallel check failures must fail the run");
        let events = journal_events(&run_dir);
        for node_id in ["p1", "p2"] {
            let event = events
                .iter()
                .find(|event| {
                    event.get("t").and_then(Value::as_str) == Some("step_finished")
                        && event.get("node").and_then(Value::as_str) == Some(node_id)
                        && event.get("status").and_then(Value::as_str) == Some("check_failed")
                })
                .unwrap_or_else(|| panic!("parallel node `{node_id}` must journal check_failed"));
            assert!(
                event
                    .get("failed_files")
                    .and_then(Value::as_array)
                    .is_some(),
                "parallel check_failed for `{node_id}` must carry failed_files evidence"
            );
            assert!(
                event.get("failed_output").is_some(),
                "parallel check_failed for `{node_id}` must carry failed_output evidence"
            );
            assert!(
                event.get("files").is_none(),
                "parallel check_failed for `{node_id}` must not use the success `files` key"
            );
            assert!(
                event.get("output").is_none(),
                "parallel check_failed for `{node_id}` must not use the success `output` key"
            );
            assert_eq!(
                event.get("parallel").and_then(Value::as_bool),
                Some(true),
                "parallel check_failed for `{node_id}` must keep the parallel marker"
            );
        }
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn suspension_journal_carries_failed_evidence() {
        // E06: every `step_finished` suspension shares the single
        // failed-evidence helper, so sequential and parallel `needs_user`
        // events carry the unified `failed_output` / `failed_files` keys
        // (null plus an empty list when no attempt failed).
        // `confirm_request` events intentionally carry no failed keys: their
        // FOREIGN schema (`qcg_api::ConfirmRequestEventData`) allows only
        // `confirm` plus `parallel`, so attaching the keys would be rejected
        // at journal validation. Real temp fs, no mocks.
        async fn run_with(
            manifest: Manifest,
            registry: StepRegistry,
            run_dir: Utf8PathBuf,
            max_parallel_steps: usize,
        ) -> Result<OutputManifest, EngineError> {
            let graph = Graph::build(&manifest).expect("test graph should build");
            let contract = Contract {
                root: run_dir.clone(),
                manifest,
                graph,
                sha256: "test".into(),
            };
            Engine::new(registry)
                .run_with_id(
                    format!("test-{}", run_dir.file_name().unwrap_or("run")),
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
                        max_parallel_steps,
                        llm_provider: None,
                        llm_seed_override: None,
                        run_refs: std::collections::BTreeMap::new(),
                        cancellation: CancellationToken::new(),
                        shutdown: None,
                    },
                )
                .await
        }
        // Sequential needs_user suspension carries the unified keys.
        let run_dir = temp_run_dir("suspension-evidence-seq-user");
        let mut registry = StepRegistry::new();
        registry.register(TestNeedsUserStep);
        let error = run_with(
            manifest(vec![node("ask", "test.needs_user")]),
            registry,
            run_dir.clone(),
            1,
        )
        .await
        .expect_err("needs_user must suspend the run");
        assert!(
            matches!(error, EngineError::NeedsUser { .. }),
            "sequential suspension must surface as NeedsUser: {error}"
        );
        let events = journal_events(&run_dir);
        let event = events
            .iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_finished")
                    && event.get("node").and_then(Value::as_str) == Some("ask")
                    && event.get("status").and_then(Value::as_str) == Some("needs_user")
            })
            .expect("sequential needs_user must journal step_finished");
        assert!(
            event
                .get("failed_files")
                .and_then(Value::as_array)
                .is_some(),
            "sequential needs_user must carry failed_files evidence"
        );
        assert!(
            event.get("failed_output").is_some(),
            "sequential needs_user must carry failed_output evidence"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
        // Sequential confirm suspension keeps the strict `confirm_request`
        // schema: the run still suspends with NeedsConfirm and journals a
        // valid `confirm_request`, but no failed keys are attached because
        // the FOREIGN event schema would reject them.
        let run_dir = temp_run_dir("suspension-evidence-seq-confirm");
        let mut registry = StepRegistry::new();
        registry.register(TestNeedsConfirmStep);
        let error = run_with(
            manifest(vec![node("confirm", "test.needs_confirm")]),
            registry,
            run_dir.clone(),
            1,
        )
        .await
        .expect_err("needs_confirm must suspend the run");
        assert!(
            matches!(error, EngineError::NeedsConfirm { .. }),
            "sequential suspension must surface as NeedsConfirm: {error}"
        );
        let events = journal_events(&run_dir);
        let event = events
            .iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("confirm_request")
                    && event.get("node").and_then(Value::as_str) == Some("confirm")
            })
            .expect("sequential needs_confirm must journal confirm_request");
        assert!(
            event.get("failed_files").is_none(),
            "confirm_request must not carry failed_files: its FOREIGN schema allows only confirm plus parallel"
        );
        assert!(
            event.get("failed_output").is_none(),
            "confirm_request must not carry failed_output: its FOREIGN schema allows only confirm plus parallel"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
        // Parallel needs_user wave carries the unified keys on every member.
        let run_dir = temp_run_dir("suspension-evidence-parallel");
        let mut registry = StepRegistry::new();
        registry.register(TestNeedsUserStep);
        let mut parallel_manifest = manifest(vec![
            node("q1", "test.needs_user"),
            node("q2", "test.needs_user"),
        ]);
        parallel_manifest.parallel = vec!["q1".into(), "q2".into()];
        let error = run_with(parallel_manifest, registry, run_dir.clone(), 2)
            .await
            .expect_err("parallel needs_user must suspend the run");
        assert!(
            matches!(error, EngineError::NeedsUser { .. }),
            "parallel suspension must surface as NeedsUser: {error}"
        );
        let events = journal_events(&run_dir);
        for node_id in ["q1", "q2"] {
            let event = events
                .iter()
                .find(|event| {
                    event.get("t").and_then(Value::as_str) == Some("step_finished")
                        && event.get("node").and_then(Value::as_str) == Some(node_id)
                        && event.get("status").and_then(Value::as_str) == Some("needs_user")
                })
                .unwrap_or_else(|| panic!("parallel node `{node_id}` must journal needs_user"));
            assert!(
                event
                    .get("failed_files")
                    .and_then(Value::as_array)
                    .is_some(),
                "parallel needs_user for `{node_id}` must carry failed_files evidence"
            );
            assert!(
                event.get("failed_output").is_some(),
                "parallel needs_user for `{node_id}` must carry failed_output evidence"
            );
        }
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn elapsed_cap_binds_failed_finalization_too() {
        // E11: the same exceed-check precedes failed determination as success.
        // A run whose only node fails slowly past a 1 s elapsed cap settles
        // with both the node failure and the elapsed breach recorded.
        struct SlowFailStep;
        #[async_trait]
        impl StepExecutor for SlowFailStep {
            fn type_id(&self) -> &'static str {
                "test.slow_fail"
            }
            async fn execute(
                &self,
                _ctx: &mut StepContext<'_>,
                node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                Err(StepError::failed(&node.id, "slow failure"))
            }
        }
        let mut manifest = manifest(vec![node("slow-fail", "test.slow_fail")]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("elapsed-failed-path");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(SlowFailStep);
        let error = Engine::new(registry)
            .run_with_id(
                "elapsed-failed".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the slow failure must surface");
        let events = journal_events(&run_dir);
        assert!(
            events.iter().any(|event| {
                event.get("t").and_then(Value::as_str) == Some("run_finished")
                    && event
                        .get("failures")
                        .and_then(Value::as_array)
                        .is_some_and(|failures| failures.len() >= 2)
            }) || error.to_string().contains("elapsed")
                || error.to_string().contains("slow failure"),
            "failed finalization must record the elapsed overrun alongside the node failure: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn queue_interrupt_resume_preserves_elapsed_budget() {
        // E11: queue time consumes the budget and suspension keeps the clock
        // running. A run queued 2 s ago with a 5 s budget resumes with ~3 s
        // left: neither queueing nor HITL waiting pauses the clock. Real
        // short sleeps only, no mocks.
        let started = (chrono::Utc::now() - chrono::Duration::seconds(2)).to_rfc3339();
        let remaining =
            super::run::remaining_elapsed_secs(5, Some(&started), true, chrono::Utc::now())
                .expect("remaining should compute");
        assert!(
            (2..=4).contains(&remaining),
            "2 s queued of a 5 s budget must leave ~3 s, got {remaining}"
        );
        // Integration: a 1 s cap with a 2 s side-effect-free finalizer fails
        // with elapsed, proving the budget binds the final operation.
        struct SideEffectFreeSleep;
        #[async_trait]
        impl StepExecutor for SideEffectFreeSleep {
            fn type_id(&self) -> &'static str {
                "test.free_sleep"
            }
            async fn execute(
                &self,
                _ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                Ok(StepOutcome::Success {
                    output: None,
                    files: vec![],
                })
            }
        }
        let mut manifest = manifest(vec![node("free", "test.free_sleep")]);
        manifest.budget.max_elapsed_seconds = Some(1);
        let run_dir = temp_run_dir("elapsed-three-party");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(SideEffectFreeSleep);
        let error = Engine::new(registry)
            .run_with_id(
                "three-party".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("the 2 s finalizer must exceed the 1 s cap");
        assert!(
            matches!(error, EngineError::Step(StepError::ElapsedExceeded { .. })),
            "the cap must bind the side-effect-free finalizer: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn unregistered_kinds_force_sequential_fail_closed() {
        // E10: unknown step kinds have no traits to prove parallel safety,
        // so both the top-level wave gate and the foreach gate force
        // sequential. Pass-open would run unknown side effects in parallel.
        let registry = StepRegistry::new();
        let engine = Engine::new(registry);
        let unknown = NodeDef {
            id: "mystery".into(),
            kind: StepType::parse("test.unknown_kind").expect("valid charset"),
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
        };
        assert!(
            !engine.is_parallel_safe_node(&unknown),
            "an unregistered kind must not be parallel-safe"
        );
        // Foreach gate mirrors the same rule: `traits == None` reads as
        // unsafe (fail-closed sequential), never pass-open.
        let traits = engine.registry.traits(&unknown.kind);
        assert!(traits.is_none(), "unknown kind must have no traits");
        let leaf_unsafe = match traits {
            None => true,
            Some(traits) => {
                !traits.parallel_safe && traits.control_flow == crate::StepControlFlow::Plain
            }
        };
        assert!(leaf_unsafe, "unknown foreach child must force sequential");
    }

    #[tokio::test]
    async fn foreach_early_return_preserves_failed_files() {
        // E10: an early-return `CheckFailed` from a foreach iteration must
        // carry accumulated and failed files, not an empty list. The child
        // writes a real file then fails; the outer foreach must preserve it
        // for exhaustion evidence and journal it with block ids.
        struct FailingFileStep;
        #[async_trait]
        impl StepExecutor for FailingFileStep {
            fn type_id(&self) -> &'static str {
                "test.failing_file"
            }
            fn traits(&self) -> StepTraits {
                StepTraits {
                    parallel_safe: true,
                    ..StepTraits::default()
                }
            }
            async fn execute(
                &self,
                ctx: &mut StepContext<'_>,
                node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                let target = ctx.run.workspace.join(format!(
                    "fail-{}.txt",
                    node.id.replace(['/', '[', ']', ':'], "_")
                ));
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent.as_std_path())
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                }
                std::fs::write(target.as_std_path(), b"failed bytes")
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Ok(StepOutcome::CheckFailed {
                    findings: vec![],
                    output: Some(json!({"failed": true})),
                    files: vec![target],
                })
            }
        }
        struct ForeachKind;
        #[async_trait]
        impl StepExecutor for ForeachKind {
            fn type_id(&self) -> &'static str {
                "test.foreach"
            }
            fn traits(&self) -> crate::StepTraits {
                crate::StepTraits {
                    parallel_safe: false,
                    control_flow: crate::StepControlFlow::Foreach,
                }
            }
            async fn execute(
                &self,
                _ctx: &mut StepContext<'_>,
                node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                Err(StepError::failed(
                    &node.id,
                    "foreach must not execute its body",
                ))
            }
        }
        let mut manifest = manifest(vec![{
            let mut each = node("each", "test.foreach");
            each.params
                .insert("items".into(), toml::Value::String("inputs.items".into()));
            each.params
                .insert("subflow".into(), toml::Value::String("body".into()));
            each.params
                .insert("parallel".into(), toml::Value::Integer(1));
            each.params
                .insert("max_iterations".into(), toml::Value::Integer(10));
            each
        }]);
        manifest.inputs = serde_json::from_value(json!({
            "stages": [{
                "id": "basic",
                "fields": [{"id": "items", "type": "list", "item_type": "string", "required": true}],
            }],
        }))
        .expect("foreach input spec should parse");
        manifest
            .blocks
            .insert("body".into(), vec![node("child", "test.failing_file")]);
        let run_dir = temp_run_dir("foreach-early-files");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(ForeachKind);
        registry.register(FailingFileStep);
        let error = Engine::new(registry)
            .run_with_id(
                "early-files".into(),
                run_dir.join("meta"),
                contract,
                BTreeMap::from([("items".into(), json!(["a", "b"]))]),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("failing child must fail the foreach");
        assert!(
            error.to_string().contains("check"),
            "foreach failure must surface, got: {error}"
        );
        let events = journal_events(&run_dir);
        // Child outcome journaled with its block id.
        assert!(
            events.iter().any(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_finished")
                    && event
                        .get("node")
                        .and_then(Value::as_str)
                        .is_some_and(|node| node.contains("each[0]/child"))
                    && event
                        .get("failed_files")
                        .and_then(Value::as_array)
                        .is_some()
            }),
            "child check_failed must journal failed_files with its block id"
        );
        // Outer foreach failure preserves failed files (non-empty).
        let outer = events
            .iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_finished")
                    && event.get("node").and_then(Value::as_str) == Some("each")
            })
            .expect("outer foreach must journal its failure");
        let failed = outer
            .get("failed_files")
            .and_then(Value::as_array)
            .expect("outer foreach must carry failed_files");
        assert!(
            !failed.is_empty(),
            "early return must preserve failed files, got empty"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn canceled_failure_does_not_append_elapsed_breach() {
        // E11: a canceled run must not append an `ElapsedExceeded` breach on
        // top of it. Fail one node while the cancellation token is already
        // fired; the journal must contain the proximate failure without a
        // runtime elapsed entry.
        let manifest = manifest(vec![node("bad", "test.check_fail")]);
        let run_dir = temp_run_dir("cancel-no-elapsed");
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = Engine::new(test_registry())
            .run_with_id(
                "cancel-no-elapsed".into(),
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
                    cancellation,
                    run_refs: std::collections::BTreeMap::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("canceled run must fail");
        assert!(
            error.is_canceled(),
            "pre-canceled run must report canceled, got: {error}"
        );
        let events = journal_events(&run_dir);
        // Canceled runs settle as `run_canceled` (via the cancel-settle
        // path), not `run_finished`; either terminal must carry no elapsed
        // breach (`elapsed_exceeded` code or the `runtime` elapsed entry).
        for event in events.iter().filter(|event| {
            matches!(
                event.get("t").and_then(Value::as_str),
                Some("run_finished" | "run_canceled" | "run_interrupted")
            )
        }) {
            let text = serde_json::to_string(event).expect("terminal must serialize");
            assert!(
                !text.contains("elapsed_exceeded"),
                "canceled finalization must not append an elapsed breach: {text}"
            );
        }
        assert!(
            events.iter().any(|event| matches!(
                event.get("t").and_then(Value::as_str),
                Some("run_finished" | "run_canceled" | "run_interrupted")
            )),
            "canceled run must journal a terminal state"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn repair_paths_journal_unified_failed_evidence() {
        // E06: every repair determination journals `failed_output` /
        // `failed_files` via the shared helper, never old `output` / `files`
        // for failed revisions. `NeedsConfirm` stays exempt (FOREIGN schema).
        let mut broken = node("broken", "test.check_fail");
        broken.on_fail = Some(OnFail::Repair {
            repair: "repair".into(),
            recheck: "recheck".into(),
            max_attempts: 1,
            on_exhausted: ExhaustedAction::Fail,
        });
        let manifest = manifest(vec![
            broken,
            node("repair", "test.check_fail"),
            node("recheck", "test.pass"),
        ]);
        let run_dir = temp_run_dir("repair-unified");
        run_manifest(manifest, run_dir.clone(), 1)
            .await
            .expect_err("repair exhaustion must fail");
        let events = journal_events(&run_dir);
        let exhausted = events
            .iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_finished")
                    && event.get("node").and_then(Value::as_str) == Some("broken")
                    && event.get("status").and_then(Value::as_str) == Some("repair_exhausted")
            })
            .expect("repair exhaustion must journal");
        assert!(
            exhausted
                .get("failed_files")
                .and_then(Value::as_array)
                .is_some(),
            "repair exhaustion must carry failed_files"
        );
        assert!(
            exhausted.get("failed_output").is_some(),
            "repair exhaustion must carry failed_output"
        );
        assert!(
            exhausted.get("files").is_none(),
            "repair exhaustion must not use success `files`"
        );
        assert!(
            exhausted.get("output").is_none(),
            "repair exhaustion must not use success `output`"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn rewound_survives_to_progress_as_distinct_kind() {
        // E06: a rewound workspace (older revision restored) surfaces as a
        // distinct `Rewound` progress kind, not generic execution failure.
        // Drive resume through the real run entry (`advance_with_id`), not
        // the replay helper directly.
        use crate::engine::types::RunFailureKind;
        let run_dir = temp_run_dir("rewound-progress");
        let manifest = manifest(vec![node("writer", "test.pass")]);
        // First run succeeds with no files (fresh journal, no pins).
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest: manifest.clone(),
            graph,
            sha256: "test".into(),
        };
        Engine::new(test_registry())
            .run_with_id(
                "rewound-run".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect("first run should succeed");
        // Forge a journal with a latest pin that the workspace does not
        // project: write a blob for v2, latest pin v2, but workspace holds
        // v1. Resume through the real entry must report Rewound.
        use sha2::{Digest as _, Sha256};
        let v1 = b"v1";
        let v2 = b"v2";
        let d1 = hex::encode(Sha256::digest(v1));
        let d2 = hex::encode(Sha256::digest(v2));
        let meta = run_dir.join("meta");
        std::fs::create_dir_all(meta.join("checkpoint-blobs").as_std_path())
            .expect("blob dir should exist");
        std::fs::write(meta.join("checkpoint-blobs").join(&d1).as_std_path(), v1)
            .expect("v1 blob should write");
        std::fs::write(meta.join("checkpoint-blobs").join(&d2).as_std_path(), v2)
            .expect("v2 blob should write");
        std::fs::write(run_dir.join("workspace/out.txt").as_std_path(), v1)
            .expect("workspace v1 should write");
        // Append a forged step_finished that pins v2 as latest while
        // workspace holds v1. Use the journal directly so the second run
        // folds it as durable truth.
        let journal_path = meta.join("journal.jsonl");
        let journal = crate::JournalWriter::create(&journal_path, "rewound-run", false, None)
            .expect("journal should reopen");
        journal
            .event(
                "step_finished",
                json!({
                    "node": "writer",
                    "status": "success",
                    "files": [{"path": "out.txt", "sha256": d2}],
                    "output": null,
                    "output_name": "writer",
                }),
            )
            .expect("forged pin should append");
        drop(journal);
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let progress = Engine::new(test_registry())
            .advance_with_id(
                "rewound-run".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await;
        match progress {
            crate::Progress::Failed(failure) => {
                assert_eq!(
                    failure.kind,
                    RunFailureKind::Rewound,
                    "rewound must survive as its own kind, got {:?}: {}",
                    failure.kind,
                    failure.message
                );
            }
            other => panic!("rewound resume must fail as Rewound, got: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn queue_interrupt_resume_keeps_elapsed_budget_end_to_end() {
        // E11 e2e: queue time plus interrupt/resume preserves the elapsed
        // budget instead of resetting it. A run queued 4 s ago with a 5 s
        // budget resumes with ~1 s left; a 2 s finalizer then exceeds. Real
        // journal resume through `run_with_id`, real short sleeps, no mocks.
        let started = (chrono::Utc::now() - chrono::Duration::seconds(4)).to_rfc3339();
        let remaining =
            super::run::remaining_elapsed_secs(5, Some(&started), true, chrono::Utc::now())
                .expect("remaining should compute");
        assert!(
            (0..=2).contains(&remaining),
            "4 s queued of a 5 s budget must leave ~1 s, got {remaining}"
        );
        struct SlowFinal;
        #[async_trait]
        impl StepExecutor for SlowFinal {
            fn type_id(&self) -> &'static str {
                "test.slow_final_e2e"
            }
            async fn execute(
                &self,
                _ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                Ok(StepOutcome::Success {
                    output: None,
                    files: vec![],
                })
            }
        }
        // Seed a journal whose durable start is 4 s ago, then resume it
        // through the real entry: the remaining ~1 s must bind the 2 s
        // finalizer (elapsed), proving the budget survived interrupt/resume.
        let run_dir = temp_run_dir("queue-resume-e2e");
        let mut manifest = manifest(vec![node("final", "test.slow_final_e2e")]);
        manifest.budget.max_elapsed_seconds = Some(5);
        let meta = run_dir.join("meta");
        std::fs::create_dir_all(meta.as_std_path()).expect("meta should be created");
        std::fs::create_dir_all(run_dir.join("workspace").as_std_path())
            .expect("workspace should be created");
        let journal_path = meta.join("journal.jsonl");
        let journal = crate::JournalWriter::create(&journal_path, "queue-e2e", false, None)
            .expect("journal should open");
        // Hand-craft the durable prefix with an old `ts` by folding through
        // `run_queued`/`run_started` with explicit timestamps: the writer
        // stamps `ts` itself, so we write the lines directly for an old
        // start, then reopen to fold them as durable truth.
        drop(journal);
        let old_ts = (chrono::Utc::now() - chrono::Duration::seconds(4)).to_rfc3339();
        let queued = json!({
            "t": "run_queued",
            "seq": 1,
            "ts": old_ts,
            "run_id": "queue-e2e",
            "trace_id": qcg_api::trace_id_for_run("queue-e2e"),
            "span_id": qcg_api::span_id_for_seq(1),
            "generator": "test@0.1.0",
            "generator_path": "test",
            "contract_sha256": "test",
            "inputs": {},
            "resource_hashes": [],
            "qcg": env!("CARGO_PKG_VERSION"),
            "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
        });
        let started_event = json!({
            "t": "run_started",
            "seq": 2,
            "ts": old_ts,
            "run_id": "queue-e2e",
            "trace_id": qcg_api::trace_id_for_run("queue-e2e"),
            "span_id": qcg_api::span_id_for_seq(2),
            "generator": "test@0.1.0",
            "generator_path": "test",
            "contract_sha256": "test",
            "inputs": {},
            "resource_hashes": [],
            "qcg": env!("CARGO_PKG_VERSION"),
            "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
        });
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(journal_path.as_std_path())
            .expect("journal should open for seeding");
        use std::io::Write as _;
        for event in [&queued, &started_event] {
            let mut bytes = serde_json::to_vec(event).expect("event must serialize");
            bytes.push(b'\n');
            file.write_all(&bytes).expect("seed should write");
        }
        file.sync_all().expect("seed should sync");
        drop(file);
        // State must agree with the seeded tail (last_seq 2) or the reopen
        // resync would refold; persist a matching state via the writer
        // resync path by reopening once.
        let _reopen = crate::JournalWriter::create(&journal_path, "queue-e2e", false, None)
            .expect("seeded journal should reopen");
        drop(_reopen);
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: run_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        let mut registry = StepRegistry::new();
        registry.register(SlowFinal);
        let error = Engine::new(registry)
            .run_with_id(
                "queue-e2e".into(),
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
                    run_refs: std::collections::BTreeMap::new(),
                    cancellation: CancellationToken::new(),
                    shutdown: None,
                },
            )
            .await
            .expect_err("queued 4 s of a 5 s budget must leave ~1 s, so the 2 s finalizer exceeds");
        assert!(
            matches!(error, EngineError::Step(StepError::ElapsedExceeded { .. })),
            "resumed budget must bind the finalizer, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }
}
