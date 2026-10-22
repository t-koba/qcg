use super::*;
use tokio::sync::Semaphore;

// A pending sibling announces entry before the terminal/panicking task settles.
// No sleep or scheduler-speed assumption determines which task is aborted.
struct WaveFactStep(Arc<Semaphore>);

#[async_trait]
impl StepExecutor for WaveFactStep {
    fn type_id(&self) -> &'static str {
        "test.wave_fact"
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
        match node.id.as_str() {
            "terminal" | "panic" => {
                self.0.acquire().await.unwrap().forget();
                if node.id == "panic" {
                    panic!("private panic detail");
                }
                Ok(StepOutcome::NeedsUser {
                    question: api::FormSpec {
                        id: "wave-question".into(),
                        title: "private prompt".into(),
                        title_i18n: Default::default(),
                        fields: vec![],
                    },
                    files: vec![],
                })
            }
            "success" | "other_success" => Ok(StepOutcome::Success {
                output: Some(json!({"private": "output"})),
                files: vec![],
            }),
            _ => {
                self.0.add_permits(1);
                std::future::pending().await
            }
        }
    }
}

fn setup(ids: &[&str]) -> (Manifest, StepRegistry, Arc<Semaphore>) {
    let mut manifest = manifest(ids.iter().map(|id| node(id, "test.wave_fact")).collect());
    manifest.parallel = ids.iter().map(|id| (*id).into()).collect();
    let ready = Arc::new(Semaphore::new(0));
    let mut registry = StepRegistry::new();
    registry.register(WaveFactStep(ready.clone()));
    (manifest, registry, ready)
}

fn facts(events: &[Value]) -> Vec<&Value> {
    events
        .iter()
        .filter(|event| event["t"] == "parallel_task_joined")
        .collect()
}

fn fact<'a>(events: &'a [Value], node: &str) -> &'a Value {
    let found: Vec<_> = facts(events)
        .into_iter()
        .filter(|event| event["node"] == node)
        .collect();
    assert_eq!(found.len(), 1, "one join fact for {node}");
    found[0]
}

fn assert_join_before_settlement(events: &[Value]) {
    let last_join = events
        .iter()
        .rposition(|event| event["t"] == "parallel_task_joined")
        .unwrap();
    let first_settlement = events
        .iter()
        .position(|event| {
            matches!(
                event["t"].as_str(),
                Some("step_finished" | "step_interrupted")
            )
        })
        .unwrap();
    assert!(
        last_join < first_settlement,
        "facts are recorded at joins, not synthesized during node-order settlement"
    );
    for event in facts(events) {
        let text = event.to_string();
        assert!(text.len() < 1024);
        assert!(!text.contains("private"));
        assert!(event.get("task_id").is_none());
        assert!(event.get("continuation_owner").is_none());
    }
}

#[tokio::test]
async fn parallel_facts_attribute_fail_fast_abort_beside_interaction() {
    let (manifest, registry, _) = setup(&["terminal", "pending"]);
    let dir = temp_run_dir("wave-facts-terminal");
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_wave(manifest, registry, dir.clone()),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(EngineError::NeedsUser { .. })));
    let events = journal_events(&dir);
    assert_eq!(facts(&events).len(), 2);
    let terminal = fact(&events, "terminal");
    assert_eq!(terminal["classification"], "completed_terminal");
    assert_eq!(terminal["outcome"], "needs_user");
    assert_eq!(terminal["question_id"], "wave-question");
    let pending = fact(&events, "pending");
    assert_eq!(pending["classification"], "scheduler_abort");
    assert_eq!(pending["cancellation_cause"], "fail_fast");
    assert_join_before_settlement(&events);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn parallel_facts_attribute_panic_without_payload() {
    let (manifest, registry, _) = setup(&["panic", "pending"]);
    let dir = temp_run_dir("wave-facts-panic");
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_wave(manifest, registry, dir.clone()),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    let events = journal_events(&dir);
    assert_eq!(facts(&events).len(), 2);
    assert_eq!(fact(&events, "panic")["classification"], "task_panic");
    assert_eq!(
        fact(&events, "pending")["classification"],
        "scheduler_abort"
    );
    assert_join_before_settlement(&events);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn parallel_facts_attribute_parent_cancellation() {
    let (manifest, registry, ready) = setup(&["pending", "other_pending"]);
    let dir = temp_run_dir("wave-facts-parent");
    let contract = test_contract(&dir, manifest);
    let cancellation = CancellationToken::new();
    let engine = Engine::new(registry);
    let mut options = run_options(dir.join("workspace"));
    options.cancellation = cancellation.clone();
    options.max_parallel_steps = 2;
    let run = engine.run_with_id(
        "wave-parent".into(),
        dir.join("meta"),
        contract,
        BTreeMap::new(),
        options,
    );
    let cancel = async {
        ready.acquire_many(2).await.unwrap().forget();
        cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(run, cancel)
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(EngineError::Canceled)));
    let events = journal_events(&dir);
    assert_eq!(facts(&events).len(), 2);
    for id in ["pending", "other_pending"] {
        let joined = fact(&events, id);
        assert_eq!(joined["classification"], "parent_canceled");
        assert_eq!(joined["drain_cause"], "parent_cancellation");
        assert_eq!(joined["cancellation_cause"], "parent_cancellation");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn parallel_facts_success_replays_and_streams_without_state_changes() {
    let (manifest, registry, _) = setup(&["success", "other_success"]);
    let dir = temp_run_dir("wave-facts-replay");
    for _ in 0..2 {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_wave(manifest.clone(), registry.clone(), dir.clone()),
        )
        .await
        .unwrap()
        .unwrap();
    }
    let events = journal_events(&dir);
    assert_eq!(
        facts(&events).len(),
        2,
        "replay does not execute the wave again"
    );
    assert_join_before_settlement(&events);
    let mut state = RunState::default();
    for event in &events {
        let mut expected = state.clone();
        state.apply(event).unwrap();
        if event["t"] == "parallel_task_joined" {
            expected.last_seq = state.last_seq;
            assert_eq!(
                serde_json::to_value(&state).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
            assert_eq!(event["classification"], "completed_success");
            let streamed = api::RunEvent::from_flat(event).unwrap();
            assert_eq!(streamed.kind, "parallel_task_joined");
            assert_eq!(
                streamed.path.unwrap().to_string(),
                event["node"].as_str().unwrap()
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

async fn run_wave(
    manifest: Manifest,
    registry: StepRegistry,
    dir: Utf8PathBuf,
) -> Result<OutputManifest, EngineError> {
    let contract = test_contract(&dir, manifest);
    let mut options = run_options(dir.join("workspace"));
    options.max_parallel_steps = 2;
    Engine::new(registry)
        .run_with_id(
            "wave-facts".into(),
            dir.join("meta"),
            contract,
            BTreeMap::new(),
            options,
        )
        .await
}
