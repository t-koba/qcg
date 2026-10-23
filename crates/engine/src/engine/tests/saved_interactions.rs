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
        // The one-at-a-time API can show z while both owners retain reports.
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
            assert_eq!(interaction_id(state.pending.as_ref().unwrap()), "q-z");
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
        assert!(state.pending.is_none());
        assert!(state.pending_seq.is_none());
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
        let projected_seq = journal.state().pending_seq;
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
            state.pending_seq, projected_seq,
            "a different projected prompt is not cleared"
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
        // Reverse report order and a misleading last global projection.
        let (first, second) = if first_is_confirm {
            ("a-owner", "z-owner")
        } else {
            ("z-owner", "a-owner")
        };
        journal
            .event(
                "step_finished",
                json!({
                    "node": second, "status": "needs_user", "question": question("saved-question")
                }),
            )
            .unwrap();
        journal
            .event(
                "confirm_request",
                json!({
                    "node": first, "confirm": confirmation("saved-confirm"), "files": []
                }),
            )
            .unwrap();
        drop(journal);
        // Force the new projection to come from durable events, not state.json.
        std::fs::remove_file(path.with_file_name("state.json")).unwrap();
        let mut registry = StepRegistry::new();
        registry.register(MustNotReenter);
        let options = run_options(root.join("workspace"));
        for attempt in 0..3 {
            let progress = Engine::new(registry.clone())
                .advance_with_id(
                    "saved-interactions".into(),
                    root.join("meta"),
                    contract.clone(),
                    BTreeMap::new(),
                    options.clone(),
                )
                .await;
            let expected_confirm = if attempt < 2 {
                first_is_confirm
            } else {
                !first_is_confirm
            };
            let expected_id = if expected_confirm {
                "saved-confirm"
            } else {
                "saved-question"
            };
            match progress {
                Progress::Suspended(interaction) => {
                    assert_eq!(interaction_id(&interaction), expected_id);
                    assert_eq!(
                        matches!(interaction, Interaction::Confirmation { .. }),
                        expected_confirm
                    );
                    if let Interaction::Question { question } = &interaction {
                        writer(&path)
                            .event(
                                "run_waiting",
                                json!({"question_id": question.id, "question": question}),
                            )
                            .unwrap();
                    }
                }
                other => panic!("expected saved suspension, got {other:?}"),
            }
            let state = writer(&path).state();
            assert_eq!(interaction_id(state.pending.as_ref().unwrap()), expected_id);
            if attempt == 1 {
                let (kind, payload) = if first_is_confirm {
                    (
                        "user_confirmed",
                        json!({"confirmation_id": "saved-confirm", "approved": true}),
                    )
                } else {
                    (
                        "user_answered",
                        json!({"question_id": "saved-question", "values": {}}),
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
