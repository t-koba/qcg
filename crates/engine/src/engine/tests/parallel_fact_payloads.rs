use super::*;

#[tokio::test]
async fn parallel_fact_payloads_reference_only_typed_interactions_and_agent_owner() {
    let dir = std::env::temp_dir().join(format!("wave-payloads-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
    let journal = JournalWriter::create(&path, "wave-payloads", false, None).unwrap();
    let confirm = api::ConfirmSpec {
        id: "confirmation".into(),
        title: "private title".into(),
        kind: "command".into(),
        target: "private target".into(),
        dry_run: false,
        details: Some(json!({"private": "details"})),
        operation_digest: "digest".into(),
        scope: contract::SideEffectScope::Invocation,
    };
    let question = api::FormSpec {
        id: "question".into(),
        title: "private prompt".into(),
        title_i18n: Default::default(),
        fields: vec![],
    };
    let outcomes = [
        (
            "confirm",
            Ok(StepOutcome::NeedsConfirm {
                confirm: confirm.clone(),
                files: vec![],
            }),
        ),
        ("check", Ok(StepOutcome::check_failed(vec![]))),
        ("error", Err(EngineError::Failed("private error".into()))),
        ("cancel", Err(EngineError::Canceled)),
        (
            "agent",
            Err(EngineError::ResumableAgent(Box::new(EngineError::Failed(
                "private agent detail".into(),
            )))),
        ),
        (
            "error_question",
            Err(EngineError::NeedsUser {
                question_id: question.id.clone(),
                question: Box::new(question),
            }),
        ),
        (
            "error_confirm",
            Err(EngineError::NeedsConfirm {
                confirm_id: confirm.id.clone(),
                confirm: Box::new(confirm),
            }),
        ),
    ];
    let mut tasks = JoinSet::new();
    let mut nodes = BTreeMap::new();
    for (id, outcome) in outcomes {
        let node: NodeDef =
            serde_json::from_value(json!({"id": id, "type": "llm.agent", "params": {}})).unwrap();
        let task = tasks.spawn(async move { (node, outcome) });
        nodes.insert(task.id(), id.into());
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(joined) = tasks.join_next_with_id().await {
            record_wave_join(&journal, &mut nodes, &joined, "fail_fast", "fail_fast").unwrap();
        }
    })
    .await
    .unwrap();
    let task = tasks.spawn(std::future::pending::<(
        NodeDef,
        Result<StepOutcome, EngineError>,
    )>());
    nodes.insert(task.id(), "startup_cancelled".into());
    tasks.abort_all();
    let joined = tasks.join_next_with_id().await.unwrap();
    record_wave_join(&journal, &mut nodes, &joined, "wave_start_failure", "wave_start_failure")
        .unwrap();
    assert!(nodes.is_empty());
    let events = crate::read_journal_values(&path, crate::JournalLimits::default())
        .unwrap()
        .events;
    assert_eq!(events.len(), 8);
    let records: BTreeMap<_, _> = events
        .iter()
        .map(|event| (event["node"].as_str().unwrap(), event))
        .collect();
    assert_eq!(records["confirm"]["classification"], "completed_terminal");
    assert_eq!(records["confirm"]["outcome"], "needs_confirm");
    assert_eq!(records["confirm"]["confirm_id"], "confirmation");
    assert_eq!(records["check"]["outcome"], "check_failed");
    assert_eq!(records["error"]["classification"], "task_error");
    assert_eq!(records["error"]["code"], "execution_failed");
    assert_eq!(records["cancel"]["classification"], "task_canceled");
    assert_eq!(records["cancel"]["cancellation_cause"], "reported_by_task");
    assert_eq!(records["agent"]["continuation_owner"], "agent");
    assert_eq!(records["error_question"]["classification"], "interaction_pending");
    assert_eq!(records["error_question"]["question_id"], "question");
    assert!(records["error_question"].get("code").is_none());
    assert_eq!(records["error_confirm"]["classification"], "interaction_pending");
    assert_eq!(records["error_confirm"]["confirm_id"], "confirmation");
    assert_eq!(records["startup_cancelled"]["classification"], "scheduler_abort");
    assert_eq!(records["startup_cancelled"]["cancellation_cause"], "wave_start_failure");
    assert!(records["error_confirm"].get("code").is_none());
    for (node, event) in records {
        assert!(!event.to_string().contains("private"));
        assert!(event.to_string().len() < 1024);
        if node != "agent" {
            assert!(
                event.get("continuation_owner").is_none(),
                "agent node kind alone is not continuation evidence"
            );
        }
        api::RunEvent::from_flat(event).unwrap();
    }
    crate::RunState::fold_journal(&path).unwrap();
    drop(journal);
    let _ = std::fs::remove_dir_all(dir);
}
