//! The deployment ceiling is a validation boundary, not a decoration: a real
//! `llm.agent` run must be able to dispatch the eleventh call of a contract
//! tool declared at 11 when the ceiling is raised to 20.

use super::*;
use async_trait::async_trait;
use engine::{Engine, Progress, RunOptions, RunState};
use llm::{
    Capabilities, ChatContent, ChatRequest, ChatResponse, LlmError, LlmProvider, TokenUsage,
};
use std::sync::atomic::{AtomicUsize, Ordering};

const ELEVENTH_CALL: usize = 11;

/// One installation for this test binary. The value must match the other
/// raised-ceiling tests in `tests.rs` so repeated installation is idempotent.
fn install_raised_deployment_limits() {
    let raised = policy::OperationalLimits {
        agent_tool_calls: 20,
        agent_iterations: 64,
        fallback_models: 16,
        web_search_results: 40,
        retry_attempts: 32,
        retry_backoff_ms: 120_000,
        patch_target_bytes: 16 * 1024 * 1024,
        audit_tool_value_bytes: 128 * 1024,
        direct_mcp_tool_value_bytes: 128 * 1024,
        ..policy::OperationalLimits::default()
    };
    // Idempotent: the same value may be installed by preload + router.
    match policy::set_deployment_operational_limits(raised) {
        Ok(()) => {}
        Err(_) if policy::operational_limits() == &raised => {}
        Err(error) => panic!("raised ceiling must install: {error}"),
    }
    assert_eq!(policy::operational_limits().agent_tool_calls, 20);
}

struct Fixture(Utf8PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Contract with one bounded agent tool at 11 calls. The tool delegates no
/// further tools: the specialist path is in-process, network-free and writes
/// nothing, so eleven dispatches are harmless in the fixture workspace.
fn fixture() -> (Fixture, Contract) {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "llm-steps-raised-ceiling-{}-{}",
        std::process::id(),
        uuid_suffix()
    )))
    .expect("utf8 temp path");
    let fixture = Fixture(root.clone());
    std::fs::create_dir_all(root.join("workspace")).expect("workspace");
    std::fs::write(root.join("prompt.txt"), "Delegate eleven benign tasks.").expect("prompt");
    std::fs::write(
        root.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "raised-ceiling"
name = "Raised ceiling"
version = "0.1.0"
[llm]
model = { provider = "fake", model = "test" }
max_tokens = 256
[audit]
level = "standard"
[budget]
max_tokens = 1000000
[permissions]
fs_write = []
fs_read = []
network = []
commands = []
containers = { enabled = false, images = [], on_missing = "error" }
side_effects = "none"
[[flow]]
id = "agent"
type = "llm.agent"
[flow.params]
prompt = "prompt.txt"
max_iterations = 13
max_tokens_total = 4096
max_tool_calls_total = 11
[[flow.params.tools]]
kind = "agent"
name = "researcher"
instructions = "Return a short benign answer."
tools = []
max_calls = 11
max_iterations = 1
max_tokens_total = 1024
max_tool_calls_total = 11
"#,
    )
    .expect("manifest");
    let contract = Contract::load(root.clone()).expect("contract");
    (fixture, contract)
}

fn agent_node(contract: &Contract) -> NodeDef {
    let mut node = super::agent_node();
    node.params = contract.manifest.flow[0].params.clone();
    node
}

fn text_response(text: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ChatContent::Text(text.into())],
        usage: TokenUsage {
            input: 1,
            output: 1,
            ..Default::default()
        },
        stop: StopReason::EndTurn,
        provider_state: None,
        web_search: None,
    }
}

/// Parent turns emit eleven ordered calls to `researcher`, then finish.
/// Specialist turns are identified by the system prompt installed by the
/// real specialist path; they only return text, never another tool call.
struct EleventhCallProvider {
    parent_turns: AtomicUsize,
    specialist_turns: AtomicUsize,
}

impl EleventhCallProvider {
    fn new() -> Self {
        Self {
            parent_turns: AtomicUsize::new(0),
            specialist_turns: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl LlmProvider for EleventhCallProvider {
    fn id(&self) -> &str {
        "fake"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    async fn complete(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
        if request
            .system
            .as_deref()
            .is_some_and(|system| system.contains("bounded specialist"))
        {
            self.specialist_turns.fetch_add(1, Ordering::SeqCst);
            return Ok(text_response("specialist complete"));
        }
        let index = self.parent_turns.fetch_add(1, Ordering::SeqCst);
        if index >= ELEVENTH_CALL {
            return Ok(text_response("done"));
        }
        Ok(ChatResponse {
            content: vec![ChatContent::ToolCall {
                id: format!("call-{:02}", index + 1),
                name: "researcher".into(),
                args: json!({ "task": index + 1 }),
            }],
            usage: TokenUsage {
                input: 1,
                output: 1,
                ..Default::default()
            },
            stop: StopReason::ToolUse,
            provider_state: None,
            web_search: None,
        })
    }
}

fn audit_events(root: &Utf8PathBuf) -> Vec<Value> {
    std::fs::read_to_string(root.join("meta/audit.jsonl"))
        .expect("audit journal")
        .lines()
        .map(|line| serde_json::from_str(line).expect("audit event"))
        .collect()
}

#[tokio::test]
async fn eleventh_agent_tool_call_dispatches_under_raised_deployment_ceiling() {
    install_raised_deployment_limits();
    let (fixture, contract) = fixture();
    let root = &fixture.0;
    let provider = Arc::new(EleventhCallProvider::new());
    let mut registry = StepRegistry::new();
    crate::register_llm_steps(&mut registry, Arc::new(LlmRuntime::builtins()));
    let options = RunOptions {
        elapsed_budget_mode: Default::default(),
        output_dir: root.join("workspace"),
        json_events: false,
        event_sender: None,
        interactive: false,
        answers: BTreeMap::new(),
        confirmations: BTreeMap::new(),
        max_total_steps: 10,
        max_parallel_steps: 1,
        parallel_compatibility: Default::default(),
        llm_provider: Some(provider.clone()),
        llm_seed_override: None,
        run_refs: BTreeMap::new(),
        reuse: None,
        cancellation: Default::default(),
        shutdown: None,
        scheduler_yield: Default::default(),
    };
    let progress = Engine::new(registry)
        .advance_with_id(
            "raised-ceiling".into(),
            root.join("meta"),
            contract,
            BTreeMap::new(),
            options,
        )
        .await;
    assert!(matches!(progress, Progress::Done(_)), "{progress:?}");

    let events = audit_events(root);
    let dispatched: Vec<&Value> = events
        .iter()
        .filter(|event| event["t"] == "tool_call" && event["status"] == "succeeded")
        .collect();
    assert_eq!(
        dispatched.len(),
        ELEVENTH_CALL,
        "the real run must dispatch exactly eleven calls, not just validate them"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["t"] == "tool_call")
            .count(),
        ELEVENTH_CALL,
        "every tool dispatch must succeed"
    );
    for (index, event) in dispatched.iter().enumerate() {
        assert_eq!(event["tool"], "researcher");
        assert!(
            event["agent"].is_null(),
            "top-level dispatch, not delegated"
        );
        assert_eq!(event["id"], format!("call-{:02}", index + 1));
        assert_eq!(event["phase"], "completed");
    }
    assert_eq!(
        dispatched[ELEVENTH_CALL - 1]["id"],
        "call-11",
        "eleventh call identity must be visible in the audit trail"
    );
    assert!(
        !events
            .iter()
            .any(|event| event["t"] == "tool_call" && event["status"] == "failed"),
        "no dispatch may fail at the eleventh call"
    );
    assert_eq!(
        provider.parent_turns.load(Ordering::SeqCst),
        ELEVENTH_CALL + 1
    );
    assert_eq!(
        provider.specialist_turns.load(Ordering::SeqCst),
        ELEVENTH_CALL
    );
    let state = RunState::fold_journal(&root.join("meta/journal.jsonl")).unwrap();
    assert_eq!(state.nodes.len(), 1, "one bounded agent node ran");
}

#[test]
fn deployment_ceiling_keeps_refusing_declarations_above_raised_limits() {
    install_raised_deployment_limits();
    let (_fixture, contract) = fixture();
    let runtime = LlmRuntime::builtins();
    let node = agent_node(&contract);
    let mut registry = StepRegistry::new();
    crate::register_llm_steps(&mut registry, Arc::new(runtime));
    let executor = registry
        .get(&node.kind)
        .expect("llm.agent should be registered");

    // The declaration used by the real run validates under the raised ceiling.
    executor
        .validate(&node, &contract)
        .expect("eleventh tool call must validate with a raised ceiling");

    // Values above the configured ceiling stay refused, never clamped.
    let mut over = super::agent_node();
    over.params = serde_json::from_value(json!({
        "prompt": "prompt.txt",
        "max_iterations": 13,
        "max_tokens_total": 4096,
        "max_tool_calls_total": 11,
        "tools": [{
            "kind": "agent",
            "name": "researcher",
            "instructions": "Return a short benign answer.",
            "tools": [],
            "max_calls": 21,
            "max_iterations": 1,
            "max_tokens_total": 1024,
            "max_tool_calls_total": 21
        }]
    }))
    .expect("params");
    let error = executor
        .validate(&over, &contract)
        .expect_err("twenty-first call must be refused above the raised ceiling");
    assert!(error.to_string().contains("between 1 and 20"), "{error}");

    // Analogous second cap: retry bounds validate against the same raised
    // ceiling. A manifest with twenty attempts validates; thirty-three is
    // refused above the thirty-two ceiling.
    let mut manifest_twenty = contract.manifest.clone();
    manifest_twenty.flow[0].retry = Some(contract::RetryPolicy {
        max_attempts: 20,
        backoff_ms: 0,
        timeout_secs: Some(30),
        on_indeterminate: contract::RetryOnIndeterminate::Fail,
    });
    manifest_twenty
        .validate()
        .expect("twenty retry attempts must validate with a raised ceiling");

    let mut manifest_over = contract.manifest.clone();
    manifest_over.flow[0].retry = Some(contract::RetryPolicy {
        max_attempts: 33,
        backoff_ms: 0,
        timeout_secs: Some(30),
        on_indeterminate: contract::RetryOnIndeterminate::Fail,
    });
    let error = manifest_over
        .validate()
        .expect_err("thirty-three attempts must be refused above the raised ceiling");
    assert!(error.to_string().contains("between 1 and 32"), "{error}");
}
