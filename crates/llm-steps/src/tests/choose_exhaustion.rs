use super::*;
use async_trait::async_trait;
use engine::{Engine, NodeOutcome, Progress, RunOptions, RunState};
use llm::{
    Capabilities, ChatContent, ChatRequest, ChatResponse, LlmError, LlmProvider, TokenUsage,
};
use model::{BudgetExhaustion, FailureCode, NodePath};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ChoiceProvider(AtomicUsize);

#[async_trait]
impl LlmProvider for ChoiceProvider {
    fn id(&self) -> &str {
        "fake"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        let attempt = self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ChatResponse {
            content: vec![ChatContent::Text(
                if attempt.is_multiple_of(2) {
                    "invalid"
                } else {
                    "yes"
                }
                .into(),
            )],
            usage: TokenUsage {
                input: 3,
                output: 2,
                ..Default::default()
            },
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
}

struct Fixture(Utf8PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn choose_local_resource_exhaustion_retains_exact_terminal_facts() {
    // First response costs 5 and is invalid. At cap=5 equality permits the
    // second attempt, whose valid choice must not be accepted at cumulative 10.
    // At cap=10 that same choice succeeds; node retries keep their old policy.
    for (cap, retries, expected_calls, max_iterations) in [
        (4, 1, 1, 3),
        (5, 1, 2, 3),
        (5, 2, 4, 3),
        (10, 1, 2, 3),
        (100, 1, 1, 1),
    ] {
        let fixture = Fixture(
            Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-choose-exhaustion-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        )))
            .unwrap(),
        );
        let root = &fixture.0;
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(root.join("prompt.txt"), "Choose yes").unwrap();
        std::fs::write(
            root.join(contract::MANIFEST_FILE),
            format!(
                r#"
[generator]
id = "choose-test"
name = "Choose test"
version = "0.1.0"
[llm]
model = {{ provider = "fake", model = "test" }}
max_tokens = 8
[audit]
level = "standard"
[budget]
max_tokens = 1000000
[permissions]
fs_write = ["workspace"]
fs_read = ["workspace"]
network = []
commands = []
containers = {{ enabled = false, images = [], on_missing = "error" }}
side_effects = "none"
[[flow]]
id = "choice"
type = "llm.choose"
[flow.retry]
max_attempts = {retries}
backoff_ms = 0
on_indeterminate = "fail"
[flow.params]
prompt = "prompt.txt"
options = ["yes"]
max_iterations = {max_iterations}
max_tokens_total = {cap}
output_file = "out.txt"
"#
            ),
        )
        .unwrap();
        let contract = Contract::load(root.clone()).unwrap();
        let runtime = Arc::new(LlmRuntime::builtins());
        let mut registry = StepRegistry::new();
        registry.register(crate::choose::LlmChooseStep { runtime });
        let provider = Arc::new(ChoiceProvider(AtomicUsize::new(0)));
        let options = RunOptions {
            elapsed_budget_mode: Default::default(),
            output_dir: root.join("workspace"),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            max_total_steps: 100,
            max_parallel_steps: 1,
            parallel_compatibility: Default::default(),
            llm_provider: Some(provider.clone()),
            llm_seed_override: None,
            run_refs: BTreeMap::new(),
            cancellation: Default::default(),
            shutdown: None,
            scheduler_yield: Default::default(),
        };
        let progress = Engine::new(registry)
            .advance_with_id(
                "choose-test".into(),
                root.join("meta"),
                contract,
                BTreeMap::new(),
                options,
            )
            .await;
        assert_eq!(provider.0.load(Ordering::SeqCst), expected_calls);
        let state = RunState::fold_journal(&root.join("meta/journal.jsonl")).unwrap();
        assert_eq!(state.budget.llm_calls, expected_calls as u64);
        assert_eq!(state.budget.tokens_input, 3 * expected_calls as u64);
        assert_eq!(state.budget.tokens_output, 2 * expected_calls as u64);
        if cap == 10 {
            assert!(matches!(progress, Progress::Done(_)), "{progress:?}");
            assert_eq!(
                std::fs::read_to_string(root.join("workspace/out.txt")).unwrap(),
                "yes"
            );
            continue;
        }
        let Progress::Failed(failure) = progress else {
            panic!("{progress:?}")
        };
        let (resource, used, allowed, expected_message) = if cap == 100 {
            (
                "iterations",
                1,
                1,
                "step `choice` failed: LLM did not choose one of declared options after 1 attempt(s)".to_string(),
            )
        } else {
            let used = if cap == 4 { 5 } else { 10 };
            (
                "tokens_total",
                used,
                cap,
                format!("step `choice` failed: llm.choose token budget exceeded: {used} > {cap}"),
            )
        };
        assert_eq!(failure.code, FailureCode::ExecutionFailed);
        assert_eq!(
            failure.exhaustion,
            Some(BudgetExhaustion {
                resource: resource.into(),
                used,
                allowed,
                used_subsec_nanoseconds: None,
            })
        );
        assert_eq!(failure.message, expected_message);
        assert!(!root.join("workspace/out.txt").exists());
        let NodeOutcome::Failed { reason } = &state.nodes[&NodePath::root("choice")] else {
            panic!("expected failed choice")
        };
        assert_eq!(reason, &failure.detail);
        let events: Vec<Value> = std::fs::read_to_string(root.join("meta/journal.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let finished = events
            .iter()
            .rev()
            .find(|e| e["t"] == "step_finished")
            .unwrap();
        assert_eq!(
            finished["reason"],
            serde_json::to_value(&failure.detail).unwrap()
        );
        if cap == 100 {
            assert_eq!(
                finished["reason"]["exhaustion"],
                serde_json::json!({"resource": "iterations", "used": 1, "allowed": 1})
            );
        }
        let terminal = events.iter().find(|e| e["t"] == "run_finished").unwrap();
        assert!(
            terminal["failures"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry["failure"] == serde_json::to_value(&failure.detail).unwrap()
                })
        );
        if cap == 100 {
            assert_eq!(
                terminal["failures"][0]["failure"]["exhaustion"],
                serde_json::json!({"resource": "iterations", "used": 1, "allowed": 1})
            );
        }
        let audit: Vec<Value> = std::fs::read_to_string(root.join("meta/audit.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            audit.iter().filter(|e| e["t"] == "step_retry").count(),
            retries - 1
        );
        // Choose has no agent policy/event; its canonical failure is sufficient.
        assert!(
            !audit
                .iter()
                .chain(&events)
                .any(|e| e["t"] == "agent_failed")
        );
        api::RunEvent::from_flat(finished).unwrap();
        api::RunEvent::from_flat(terminal).unwrap();
    }
}
