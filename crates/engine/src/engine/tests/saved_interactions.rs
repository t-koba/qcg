use super::*;
use crate::{Interaction, JournalWriter};

fn question(id: &str) -> Value {
    json!({"id": id, "title": format!("Question {id}"), "fields": []})
}

fn confirmation(id: &str) -> Value {
    json!({
        "id": id, "title": format!("Confirm {id}"), "kind": "command",
        "target": "echo", "dry_run": false, "operation_digest": "a".repeat(64),
        "scope": "invocation"
    })
}

fn writer(path: &Utf8Path) -> JournalWriter {
    JournalWriter::create(path, "saved-interactions", false, None).unwrap()
}

fn interaction_id(interaction: &Interaction) -> &str {
    match interaction {
        Interaction::Question { question } => &question.id,
        Interaction::Confirmation { confirm } => &confirm.id,
    }
}

fn save_prompt(journal: &JournalWriter, owner: &str, confirm: bool, id: &str) {
    if confirm {
        journal
            .event(
                "confirm_request",
                json!({"node": owner, "confirm": confirmation(id), "files": []}),
            )
            .unwrap();
    } else {
        journal
            .event(
                "step_finished",
                json!({"node": owner, "status": "needs_user", "question": question(id)}),
            )
            .unwrap();
        journal
            .event(
                "run_waiting",
                json!({"question_id": id, "question": question(id)}),
            )
            .unwrap();
    }
}

fn start_run(journal: &JournalWriter, contract: &Contract, root: &Utf8Path) {
    journal
        .event(
            "run_started",
            json!({
                "generator": "scheduler-test@0.1.0", "generator_path": root,
                "contract_sha256": contract.sha256, "inputs": {}, "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION, "elapsed_budget_mode": "wall_clock"
            }),
        )
        .unwrap();
}

#[test]
fn saved_interactions_terminal_reports_retire_only_the_owner() {
    for status in ["success", "failed", "skipped"] {
        for confirm in [false, true] {
            for projected_owner in [false, true] {
                let dir = crate::test_support::TestDir::create("saved-terminal");
                let path = Utf8PathBuf::from_path_buf(dir.path().join("journal.jsonl")).unwrap();
                let journal = writer(&path);
                journal
                    .event(
                        "user_answered",
                        json!({"question_id": "answered", "values": {"x": 1}}),
                    )
                    .unwrap();
                journal
                    .event(
                        "user_confirmed",
                        json!({"confirmation_id": "approved", "approved": true}),
                    )
                    .unwrap();
                let (retiring, surviving) = if projected_owner {
                    ("a-owner", "z-owner")
                } else {
                    ("z-owner", "a-owner")
                };
                for owner in ["z-owner", "a-owner"] {
                    // Type, not just ID, is part of the prompt identity.
                    save_prompt(
                        &journal,
                        owner,
                        if owner == retiring { confirm } else { !confirm },
                        "shared",
                    );
                }
                let before = journal.state();
                let survivor = model::NodePath::root(surviving);
                let settle = |owner| {
                    journal
                        .event(
                            if status == "skipped" {
                                "step_skipped"
                            } else {
                                "step_finished"
                            },
                            if status == "skipped" { json!({"node": owner, "reason": model::FailureDetail::new(model::FailureCode::ExecutionFailed, "skipped")}) } else { json!({"node": owner, "status": status}) },
                        )
                        .unwrap();
                };
                settle(retiring);
                let check = |state: &RunState| {
                    assert_eq!(state.pending_interactions.len(), 1);
                    assert_eq!(
                        serde_json::to_value(&state.pending_interactions[&survivor]).unwrap(),
                        serde_json::to_value(&before.pending_interactions[&survivor]).unwrap()
                    );
                    assert_eq!(state.pending_seq, Some(before.interaction_seqs[&survivor]));
                    assert_eq!(
                        serde_json::to_value(&state.pending).unwrap(),
                        serde_json::to_value(Some(&before.pending_interactions[&survivor]))
                            .unwrap()
                    );
                    assert!(
                        !state
                            .interaction_seqs
                            .contains_key(&model::NodePath::root(retiring))
                    );
                    assert_eq!(state.answers, before.answers);
                    assert_eq!(state.confirmations, before.confirmations);
                    assert_eq!(
                        state
                            .vars
                            .get_path(&format!("steps.{retiring}.status"))
                            .and_then(Value::as_str),
                        Some(status)
                    );
                };
                check(&journal.state());
                check(&RunState::fold_journal(&path).unwrap());
                settle(surviving);
                let state = journal.state();
                assert!(state.pending_interactions.is_empty());
                assert!(state.pending.is_none());
                assert!(state.pending_seq.is_none());
            }
        }
    }
}

struct SuppliedAnswerSettlement {
    fail: bool,
    inspected: Arc<AtomicBool>,
}

#[async_trait]
impl StepExecutor for SuppliedAnswerSettlement {
    fn type_id(&self) -> &'static str {
        "test.supplied_settlement"
    }
    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        if node.id.as_str() != "owner" {
            // Observe immediately after terminal settlement, before run_finished can mask stale pending.
            let state = ctx.journal.state();
            assert!(state.pending_interactions.is_empty());
            assert!(state.pending.is_none());
            assert!(state.pending_seq.is_none());
            self.inspected.store(true, Ordering::SeqCst);
        } else {
            assert!(
                ctx.run.answers.contains_key("supplied")
                    || ctx.run.confirmations.contains_key("supplied")
            );
            if self.fail {
                return Err(StepError::failed(&node.id, "expected failure"));
            }
        }
        Ok(StepOutcome::success(None))
    }
}

#[tokio::test]
async fn saved_interactions_direct_supplied_answers_retire_at_terminal_settlement() {
    for status in ["success", "failed", "skipped"] {
        for confirm in [false, true] {
            let dir = crate::test_support::TestDir::create("saved-direct-answer");
            let root = Utf8Path::from_path(dir.path()).unwrap();
            let mut owner = node("owner", "test.supplied_settlement");
            if status == "skipped" {
                owner.when = Some(contract::Expr("false".into()));
            }
            let mut manifest = manifest(vec![owner]);
            let inspect = hook("inspect", "test.supplied_settlement", HookErrorPolicy::Fail);
            if status == "failed" {
                manifest.hooks.run_failed.push(inspect);
            } else {
                manifest.hooks.run_succeeded.push(inspect);
            }
            let contract = test_contract(root, manifest);
            let path = root.join("meta/journal.jsonl");
            let journal = writer(&path);
            start_run(&journal, &contract, root);
            save_prompt(&journal, "owner", confirm, "supplied");
            drop(journal);
            let mut options = run_options(root.join("workspace"));
            if confirm {
                options.confirmations.insert("supplied".into(), true);
            } else {
                options.answers.insert("supplied".into(), json!({}));
            }
            let inspected = Arc::new(AtomicBool::new(false));
            let mut registry = StepRegistry::new();
            registry.register(SuppliedAnswerSettlement {
                fail: status == "failed",
                inspected: inspected.clone(),
            });
            let result = Engine::new(registry)
                .run_with_id(
                    "saved-interactions".into(),
                    root.join("meta"),
                    contract,
                    BTreeMap::new(),
                    options,
                )
                .await;
            assert_eq!(result.is_err(), status == "failed", "{result:?}");
            assert!(
                inspected.load(Ordering::SeqCst),
                "{status} confirm={confirm}: {result:?}"
            );
            let events = crate::read_journal_values(&path, crate::JournalLimits::default())
                .unwrap()
                .events;
            assert!(!events.iter().any(|event| matches!(
                event["t"].as_str(),
                Some("user_answered" | "user_confirmed")
            )));
        }
    }
}

#[test]
fn saved_interactions_survive_fold_and_both_restart_paths() {
    for confirm in [false, true] {
        let dir = crate::test_support::TestDir::create("saved-fold");
        let path = Utf8PathBuf::from_path_buf(dir.path().join("journal.jsonl")).unwrap();
        let journal = writer(&path);
        journal
            .event(
                "step_finished",
                json!({
                    "node": "z-owner", "status": "needs_user", "question": question("q-z")
                }),
            )
            .unwrap();
        if confirm {
            journal
                .event(
                    "confirm_request",
                    json!({
                        "node": "a-owner", "confirm": confirmation("c-a")
                    }),
                )
                .unwrap();
        } else {
            journal
                .event(
                    "step_finished",
                    json!({
                        "node": "a-owner", "status": "needs_user", "question": question("q-a")
                    }),
                )
                .unwrap();
        }
        // A later request for z must not reorder the canonical owner projection.
        journal
            .event(
                "run_waiting",
                json!({
                    "question_id": "q-z", "question": question("q-z")
                }),
            )
            .unwrap();
        // Unrelated success must not erase that API prompt either.
        journal
            .event(
                "step_finished",
                json!({"node": "other", "status": "success"}),
            )
            .unwrap();
        let check = |state: &RunState| {
            let entries: Vec<_> = state
                .pending_interactions
                .iter()
                .map(|(node, interaction)| (node.as_str(), interaction_id(interaction)))
                .collect();
            assert_eq!(
                entries,
                vec![
                    ("a-owner", if confirm { "c-a" } else { "q-a" }),
                    ("z-owner", "q-z")
                ]
            );
            assert_eq!(
                interaction_id(state.pending.as_ref().unwrap()),
                if confirm { "c-a" } else { "q-a" }
            );
        };
        check(&RunState::fold_journal(&path).unwrap());
        drop(journal);
        check(&writer(&path).state()); // persisted state cache
        std::fs::remove_file(path.with_file_name("state.json")).unwrap();
        check(&writer(&path).state()); // journal-only reconstruction
        writer(&path)
            .event("user_answered", json!({"question_id": "q-z", "values": {}}))
            .unwrap();
        let state = writer(&path).state();
        assert_eq!(state.pending_interactions.len(), 1);
        assert!(
            state
                .pending_interactions
                .contains_key(&model::NodePath::root("a-owner"))
        );
        assert_eq!(
            state.pending_seq,
            state
                .interaction_seqs
                .get(&model::NodePath::root("a-owner"))
                .copied()
        );
        assert!(state.pending.is_some());
        // Direct callers can supply answers without separate response events.
        // An actual completed report retires only that owner's saved prompt.
        writer(&path)
            .event(
                "step_finished",
                json!({"node": "a-owner", "status": "success"}),
            )
            .unwrap();
        assert!(writer(&path).state().pending_interactions.is_empty());
    }
}

#[test]
fn saved_interactions_answers_remove_only_matching_type_and_id() {
    for answer_question in [false, true] {
        let dir = crate::test_support::TestDir::create("saved-answer");
        let path = Utf8PathBuf::from_path_buf(dir.path().join("journal.jsonl")).unwrap();
        let journal = writer(&path);
        // Same ID deliberately: type is part of interaction identity.
        journal
            .event(
                "step_finished",
                json!({
                    "node": "question-owner", "status": "needs_user", "question": question("shared")
                }),
            )
            .unwrap();
        journal
            .event(
                "confirm_request",
                json!({
                    "node": "confirm-owner", "confirm": confirmation("shared"), "files": []
                }),
            )
            .unwrap();
        if !answer_question {
            journal
                .event(
                    "run_waiting",
                    json!({
                        "question_id": "shared", "question": question("shared")
                    }),
                )
                .unwrap();
        }
        let generations = journal.state().interaction_seqs;
        let (kind, payload, remaining) = if answer_question {
            (
                "user_answered",
                json!({"question_id": "shared", "values": {}}),
                "confirm-owner",
            )
        } else {
            (
                "user_confirmed",
                json!({"confirmation_id": "shared", "approved": false}),
                "question-owner",
            )
        };
        journal.event(kind, payload).unwrap();
        let state = RunState::fold_journal(&path).unwrap();
        assert_eq!(state.pending_interactions.len(), 1);
        assert!(
            state
                .pending_interactions
                .contains_key(&model::NodePath::root(remaining))
        );
        assert_eq!(
            state.pending_seq,
            generations.get(&model::NodePath::root(remaining)).copied(),
            "the survivor keeps its original installing generation"
        );
        assert_eq!(interaction_id(state.pending.as_ref().unwrap()), "shared");
        let (kind, payload) = if answer_question {
            (
                "user_confirmed",
                json!({"confirmation_id": "shared", "approved": true}),
            )
        } else {
            (
                "user_answered",
                json!({"question_id": "shared", "values": {}}),
            )
        };
        journal.event(kind, payload).unwrap();
        let state = writer(&path).state();
        assert!(state.pending_interactions.is_empty());
        assert!(state.pending.is_none());
        assert!(state.pending_seq.is_none());
    }
}

struct MustNotReenter;

#[async_trait]
impl StepExecutor for MustNotReenter {
    fn type_id(&self) -> &'static str {
        "test.saved_interaction"
    }
    async fn execute(
        &self,
        _: &mut StepContext<'_>,
        _: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        panic!("saved interaction must be surfaced without re-executing its owner")
    }
}

#[tokio::test]
async fn saved_interactions_resume_in_owner_order_without_reexecution() {
    for first_is_confirm in [false, true] {
        let dir = crate::test_support::TestDir::create("saved-resume");
        let root = Utf8Path::from_path(dir.path()).unwrap();
        let contract = test_contract(
            root,
            manifest(vec![
                node("z-owner", "test.saved_interaction"),
                node("a-owner", "test.saved_interaction"),
            ]),
        );
        let path = root.join("meta/journal.jsonl");
        let journal = writer(&path);
        start_run(&journal, &contract, root);
        // Reports arrive in reverse owner order.
        save_prompt(
            &journal,
            "z-owner",
            !first_is_confirm,
            if first_is_confirm {
                "saved-question"
            } else {
                "saved-confirm"
            },
        );
        save_prompt(
            &journal,
            "a-owner",
            first_is_confirm,
            if first_is_confirm {
                "saved-confirm"
            } else {
                "saved-question"
            },
        );
        drop(journal);
        // Force the new projection to come from durable events, not state.json.
        std::fs::remove_file(path.with_file_name("state.json")).unwrap();
        let mut registry = StepRegistry::new();
        registry.register(MustNotReenter);
        let options = run_options(root.join("workspace"));
        for owner in 0..2 {
            let expected_confirm = if owner == 0 {
                first_is_confirm
            } else {
                !first_is_confirm
            };
            let expected_id = if expected_confirm {
                "saved-confirm"
            } else {
                "saved-question"
            };
            let before = writer(&path).state();
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(
                before.pending.as_ref().map(interaction_id),
                Some(expected_id)
            );
            for _ in 0..3 {
                let progress = Engine::new(registry.clone())
                    .advance_with_id(
                        "saved-interactions".into(),
                        root.join("meta"),
                        contract.clone(),
                        BTreeMap::new(),
                        options.clone(),
                    )
                    .await;
                match progress {
                    Progress::Suspended(interaction) => {
                        assert_eq!(interaction_id(&interaction), expected_id);
                        assert_eq!(
                            matches!(interaction, Interaction::Confirmation { .. }),
                            expected_confirm
                        );
                    }
                    other => panic!("expected saved suspension, got {other:?}"),
                }
                let after = writer(&path).state();
                assert_eq!(
                    after.pending.as_ref().map(interaction_id),
                    Some(expected_id),
                    "surfaced prompts must remain answerable through the authoritative projection"
                );
                assert_eq!(
                    std::fs::read(&path).unwrap(),
                    bytes,
                    "observation must not append journal events"
                );
                assert_eq!(after.pending_seq, before.pending_seq);
                assert_eq!(
                    serde_json::to_value(after).unwrap(),
                    serde_json::to_value(&before).unwrap()
                );
            }
            if owner == 0 {
                let (kind, payload) = if expected_confirm {
                    (
                        "user_confirmed",
                        json!({"confirmation_id": expected_id, "approved": true}),
                    )
                } else {
                    (
                        "user_answered",
                        json!({"question_id": expected_id, "values": {}}),
                    )
                };
                writer(&path).event(kind, payload).unwrap();
            }
        }
        let events = crate::read_journal_values(&path, crate::JournalLimits::default())
            .unwrap()
            .events;
        assert!(!events.iter().any(|event| event["t"] == "step_started"));
        assert_eq!(writer(&path).state().pending_interactions.len(), 1);
    }
}

#[test]
fn saved_interactions_rebuild_legacy_cache_and_keep_recorded_generations() {
    let dir = crate::test_support::TestDir::create("saved-legacy");
    let path = Utf8PathBuf::from_path_buf(dir.path().join("journal.jsonl")).unwrap();
    let journal = writer(&path);
    save_prompt(&journal, "a-owner", true, "a");
    let a_seq = journal.state().pending_seq;
    save_prompt(&journal, "z-owner", true, "z");
    // Old runtimes emitted another request on observation. Its recorded
    // generation must remain authoritative; do not rewrite existing journals.
    save_prompt(&journal, "z-owner", true, "z");
    let original = journal.state();
    let z_seq = original.interaction_seqs[&model::NodePath::root("z-owner")];
    let mut legacy = serde_json::to_value(&original).unwrap();
    legacy["schema_version"] = json!(5);
    legacy.as_object_mut().unwrap().remove("interaction_seqs");
    legacy["pending"] =
        serde_json::to_value(&original.pending_interactions[&model::NodePath::root("z-owner")])
            .unwrap();
    legacy["pending_seq"] = json!(z_seq);
    drop(journal);
    std::fs::write(
        path.with_file_name("state.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();
    let journal = writer(&path);
    assert_eq!(journal.state().pending_seq, a_seq);
    assert_eq!(journal.state().interaction_seqs, original.interaction_seqs);
    journal
        .event(
            "user_confirmed",
            json!({"confirmation_id": "a", "approved": true}),
        )
        .unwrap();
    assert_eq!(journal.state().pending_seq, Some(z_seq));
    assert_eq!(
        interaction_id(journal.state().pending.as_ref().unwrap()),
        "z"
    );
    journal
        .event(
            "user_confirmed",
            json!({"confirmation_id": "z", "approved": false}),
        )
        .unwrap();
    assert!(journal.state().pending.is_none());
    assert!(journal.state().interaction_seqs.is_empty());
    assert_eq!(
        journal.state().confirmations,
        BTreeMap::from([("a".into(), true), ("z".into(), false)])
    );
}

#[test]
fn saved_interactions_never_invent_missing_generations() {
    let mut state = RunState::default();
    state
        .apply(&json!({"t": "confirm_request", "node": "z-owner", "confirm": confirmation("z")}))
        .unwrap();
    state.apply(&json!({"t": "confirm_request", "seq": 7, "node": "a-owner", "confirm": confirmation("a")})).unwrap();
    state
        .apply(&json!({"t": "user_confirmed", "seq": 8, "confirmation_id": "a", "approved": true}))
        .unwrap();
    assert_eq!(interaction_id(state.pending.as_ref().unwrap()), "z");
    assert_eq!(state.pending_seq, None);
    assert!(state.interaction_seqs.is_empty());
}
