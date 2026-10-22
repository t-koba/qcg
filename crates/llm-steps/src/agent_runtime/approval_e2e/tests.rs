//! A real loopback MCP transport, using the same rmcp server as mcp's tests.
use super::*;
use crate::agent::{LlmAgentStep, McpSuspendedCall};
use crate::tool_events::execute_mcp_tool;
use engine::StepExecutor;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ElicitRequest, ElicitRequestParams,
    InputRequest, InputRequiredResult, ListToolsResult, PaginatedRequestParams, ServerCapabilities,
    ServerConfig, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{ErrorData, ServerHandler};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct CountingTool(Arc<Mutex<Vec<Value>>>);

impl ServerHandler for CountingTool {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "echo",
                "Capture original arguments, optionally requesting user input.",
                json!({"type": "object"}).as_object().unwrap().clone(),
            )],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let args = Value::Object(request.arguments.clone().unwrap_or_default());
        self.0.lock().unwrap().push(args.clone());
        if args["ask"] == true && request.input_responses.is_none() {
            let input = InputRequest::Elicitation(ElicitRequest::new(
                ElicitRequestParams::FormElicitationParams {
                    meta: None,
                    message: "Provide a name".into(),
                    requested_schema: serde_json::from_value(json!({
                        "type": "object", "properties": {"name": {"type": "string"}},
                        "required": ["name"]
                    }))
                    .unwrap(),
                },
            ));
            return Ok(InputRequiredResult::new(
                Some(BTreeMap::from([("profile".into(), input)])),
                Some("opaque-state".into()),
            )
            .into());
        }
        if args["ask"] == true {
            assert_eq!(request.request_state.as_deref(), Some("opaque-state"));
            assert!(request.input_responses.unwrap().contains_key("profile"));
        }
        Ok(CallToolResult::structured(json!({"done": true})).into())
    }
}

struct MockMcp {
    hits: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
    runtime: Arc<llm::LlmRuntime>,
}

impl Drop for MockMcp {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockMcp {
    async fn start() -> Self {
        let hits = Arc::new(Mutex::new(vec![]));
        let tool = CountingTool(hits.clone());
        let service: StreamableHttpService<CountingTool, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(tool.clone()),
                Arc::new(LocalSessionManager::default()),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(false)
                    .with_json_response(true),
            );
        let app = axum::Router::new().nest_service("/mcp", service);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let spec = serde_json::from_value(json!({
            "id": "mock", "transport": "streamable_http", "lifecycle": "discover",
            "url": format!("http://{address}/mcp"), "auth": "none",
            "allowed_hosts": ["127.0.0.1"], "timeout_seconds": 3
        }))
        .unwrap();
        let mut runtime = test_runtime();
        runtime.mcp = mcp::McpRuntime::from_specs(vec![spec]).unwrap();
        Self {
            hits,
            task,
            runtime: Arc::new(runtime),
        }
    }

    fn hits(&self) -> Vec<Value> {
        self.hits.lock().unwrap().clone()
    }
}

fn tools() -> Vec<ToolDecl> {
    vec![
        serde_json::from_value(json!({
            "kind": "mcp", "name": "remote", "server": "mock", "tool": "echo",
            "side_effects": true, "max_calls": 10
        }))
        .unwrap(),
    ]
}

fn run(harness: &ApprovalHarness, answers: BTreeMap<String, Value>) -> RunContext {
    std::fs::write(harness.root.join("prompt.md"), "Resume the saved tool.").unwrap();
    let mut contract = harness.contract();
    contract.manifest.permissions.side_effects = SideEffects::Allowed;
    contract.manifest.llm = Some(serde_json::from_value(json!({"max_tokens": 1000})).unwrap());
    RunContext::for_tests(
        harness.run_id.clone(),
        harness.root.join("workspace"),
        harness.root.join("meta"),
        contract,
        BTreeMap::new(),
        answers,
    )
    .unwrap()
}

// The gateway is needed only for secret scanning: these saved calls never
// request an LLM completion, and successful calls stop at the one-turn bound.
fn context<'a>(
    run: &'a RunContext,
    journal: &'a JournalWriter,
    vars: &'a mut contract::ValueBag,
    runtime: &llm::LlmRuntime,
) -> StepContext<'a> {
    StepContext {
        run,
        journal,
        vars,
        llm: Some(engine::LlmGateway::new(
            runtime.provider.clone(),
            &run.secrets,
            journal,
            run.cancellation.clone(),
            policy::LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: None,
                require_pricing: false,
            },
            vec![],
        )),
    }
}

fn payload_bytes(harness: &ApprovalHarness) -> Vec<Vec<u8>> {
    std::fs::read_dir(harness.root.join("meta/pending-payloads"))
        .unwrap()
        .map(|entry| std::fs::read(entry.unwrap().path()).unwrap())
        .collect()
}

fn checkpoint(call: llm::ChatToolCall) -> AgentCheckpoint {
    AgentCheckpoint {
        messages: vec![],
        next_turn: 0,
        tokens_total: 0,
        tool_calls_total: 0,
        tool_call_counts: BTreeMap::new(),
        pending_side_effect: Some(call),
        pending_confirm: None,
        pending_mcp_call: None,
        used_calls: BTreeMap::new(),
        removed_files: BTreeSet::new(),
    }
}

fn original(ask: bool) -> llm::ChatToolCall {
    llm::ChatToolCall {
        id: "call-1".into(),
        name: "remote".into(),
        args: json!({"token": "original-test-value", "ask": ask}),
    }
}

fn journal(harness: &ApprovalHarness) -> JournalWriter {
    JournalWriter::create(&harness.journal_path(), &harness.run_id, false, None).unwrap()
}

fn node() -> NodeDef {
    let mut node = ApprovalHarness::node();
    node.params = serde_json::from_value(json!({
        "prompt": "prompt.md", "max_iterations": 1,
        "max_tokens_total": 1000, "tools": [{
            "kind": "mcp", "name": "remote", "server": "mock", "tool": "echo",
            "side_effects": true, "max_calls": 10
        }]
    }))
    .unwrap();
    node
}

#[tokio::test]
async fn missing_or_corrupt_mcp_payload_refuses_fresh_agent_recovery_without_remote_calls() {
    for damage in ["missing", "corrupt", "mismatched"] {
        let harness = ApprovalHarness::setup(&format!("mcp-{damage}"));
        let mock = MockMcp::start().await;
        let node = node();
        let tools = tools();
        let operation_id = engine::operation_id_for(&harness.run_id, &node.id, "call-1");
        {
            let run = run(&harness, BTreeMap::new());
            let journal = journal(&harness);
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = context(&run, &journal, &mut vars, &mock.runtime);
            record_agent_checkpoint(
                &ctx,
                &node,
                0,
                "before_side_effect",
                &checkpoint(original(false)),
                &tools,
            )
            .unwrap();
            if damage == "missing" {
                run.clear_pending_tool_payload(&operation_id);
            } else if damage == "corrupt" {
                let path = std::fs::read_dir(harness.root.join("meta/pending-payloads"))
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .path();
                std::fs::write(path, b"{not json").unwrap();
                assert!(run.load_pending_tool_payload(&node, &operation_id).is_err());
            } else {
                // A valid sidecar envelope containing a different call is unverifiable.
                run.store_pending_tool_payload(
                    &node,
                    &operation_id,
                    &json!({
                        "id": "foreign-call", "name": "remote", "args": {"token": "wrong"}
                    }),
                )
                .unwrap();
            }
        }
        let run = run(&harness, BTreeMap::new());
        let journal = journal(&harness);
        let before = payload_bytes(&harness);
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let mut ctx = context(&run, &journal, &mut vars, &mock.runtime);
        let result = LlmAgentStep {
            runtime: mock.runtime.clone(),
        }
        .execute(&mut ctx, &node)
        .await;
        assert!(
            mock.hits().is_empty(),
            "unresolved payload reached MCP: {:?}",
            mock.hits()
        );
        assert!(
            matches!(result, Err(StepError::Refused { ref message, .. }) if message.contains("placeholders")),
            "unexpected error: {:?}",
            result.err()
        );
        assert_eq!(harness.operation_started_count(&operation_id), 0);
        assert!(
            !journal
                .state()
                .operation_records
                .contains_key(&operation_id)
        );
        assert_eq!(
            payload_bytes(&harness),
            before,
            "re-checkpointing must not store placeholders as the original payload"
        );
    }
}

#[tokio::test]
async fn verified_mcp_sidecar_executes_original_bytes_and_redacted_success_resends() {
    let harness = ApprovalHarness::setup("mcp-original");
    let mock = MockMcp::start().await;
    let node = node();
    let tools = tools();
    let call = original(false);
    let operation_id = engine::operation_id_for(&harness.run_id, &node.id, &call.id);
    {
        let run = run(&harness, BTreeMap::new());
        let journal = journal(&harness);
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let ctx = context(&run, &journal, &mut vars, &mock.runtime);
        record_agent_checkpoint(
            &ctx,
            &node,
            0,
            "before_side_effect",
            &checkpoint(call.clone()),
            &tools,
        )
        .unwrap();
    }
    {
        let run = run(&harness, BTreeMap::new());
        let journal = journal(&harness);
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let mut ctx = context(&run, &journal, &mut vars, &mock.runtime);
        // One turn, deliberately no next model turn. The tool must complete first.
        let result = LlmAgentStep {
            runtime: mock.runtime.clone(),
        }
        .execute(&mut ctx, &node)
        .await;
        assert!(
            matches!(result, Err(StepError::Failed { ref message, .. }) if message.contains("max_iterations")),
            "unexpected error: {:?}",
            result.err()
        );
        assert_eq!(mock.hits(), vec![call.args.clone()]);
        assert_eq!(
            journal.state().operation_records[&operation_id].status,
            engine::OperationStatus::Succeeded
        );
        // Simulate a crash before the completed-turn checkpoint supersedes the pending call.
        record_agent_checkpoint(
            &ctx,
            &node,
            0,
            "before_side_effect",
            &checkpoint(call.clone()),
            &tools,
        )
        .unwrap();
        run.clear_pending_tool_payload(&operation_id);
    }
    let run = run(&harness, BTreeMap::new());
    let journal = journal(&harness);
    let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
    let mut ctx = context(&run, &journal, &mut vars, &mock.runtime);
    let result = LlmAgentStep {
        runtime: mock.runtime.clone(),
    }
    .execute(&mut ctx, &node)
    .await;
    assert!(
        matches!(result, Err(StepError::Failed { ref message, .. }) if message.contains("max_iterations")),
        "unexpected error: {:?}",
        result.err()
    );
    assert_eq!(mock.hits(), vec![call.args]);
    assert_eq!(harness.operation_started_count(&operation_id), 1);
    assert!(
        run.load_pending_tool_payload(&node, &operation_id)
            .unwrap()
            .is_none()
    );
    assert!(!harness.journal_text().contains("original-test-value"));
}

#[tokio::test]
async fn actual_mcp_continuation_refuses_redacted_resume_but_accepts_original_bytes() {
    for redacted in [true, false] {
        let harness = ApprovalHarness::setup(if redacted {
            "mcp-redacted-resume"
        } else {
            "mcp-original-resume"
        });
        let mock = MockMcp::start().await;
        let node = node();
        let tools = tools();
        let call = original(true);
        let operation_id = engine::operation_id_for(&harness.run_id, &node.id, &call.id);
        let question_id = {
            let run = run(&harness, BTreeMap::new());
            let journal = journal(&harness);
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = context(&run, &journal, &mut vars, &mock.runtime);
            let mcp = McpAgentTools::prepare(&ctx, &node, &mock.runtime, &tools)
                .await
                .unwrap();
            let outcome =
                execute_mcp_tool(&ctx, &node, &mcp, &call.name, call.args.clone(), &call.id)
                    .await
                    .unwrap();
            let AgentToolOutcome::NeedsUser(form) = outcome else {
                panic!("mock must actually suspend")
            };
            assert_eq!(journal.state().mcp_pending.len(), 1);
            let mut stored = checkpoint(call.clone());
            stored.pending_side_effect = None;
            stored.pending_mcp_call = Some(McpSuspendedCall {
                name: call.name.clone(),
                id: call.id.clone(),
                args: call.args.clone(),
                question_id: form.id.clone(),
                builtin: false,
                content_hash: String::new(),
            });
            record_agent_checkpoint(&ctx, &node, 0, "waiting_for_user", &stored, &tools).unwrap();
            form.id
        };
        let run = run(
            &harness,
            BTreeMap::from([(question_id, json!({"response_0": {"name": "Ada"}}))]),
        );
        let journal = journal(&harness);
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let mut ctx = context(&run, &journal, &mut vars, &mock.runtime);
        if redacted {
            // Full agent checkpoint recovery, including saved-question binding checks.
            let result = LlmAgentStep {
                runtime: mock.runtime.clone(),
            }
            .execute(&mut ctx, &node)
            .await;
            assert!(
                matches!(result, Err(StepError::Refused { ref message, .. }) if message.contains("placeholders")),
                "unexpected error: {:?}",
                result.err()
            );
            assert_eq!(mock.hits(), vec![call.args]);
        } else {
            let mcp = McpAgentTools::prepare(&ctx, &node, &mock.runtime, &tools)
                .await
                .unwrap();
            let outcome =
                execute_mcp_tool(&ctx, &node, &mcp, &call.name, call.args.clone(), &call.id)
                    .await
                    .unwrap();
            assert!(matches!(outcome, AgentToolOutcome::OperationResult { .. }));
            assert_eq!(mock.hits(), vec![call.args.clone(), call.args]);
            assert!(journal.state().mcp_pending.is_empty());
        }
        assert_eq!(
            harness.operation_started_count(&operation_id),
            1,
            "continuations must not re-guard"
        );
        assert!(!harness.journal_text().contains("original-test-value"));
    }
}

#[tokio::test]
async fn redacted_clean_retry_preserves_record_and_verified_original_can_retry() {
    let harness = ApprovalHarness::setup("mcp-clean-retry");
    let mock = MockMcp::start().await;
    let node = node();
    let tools = tools();
    let call = original(false);
    let operation_id = engine::operation_id_for(&harness.run_id, &node.id, &call.id);
    {
        let run = run(&harness, BTreeMap::new());
        let journal = journal(&harness);
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let ctx = context(&run, &journal, &mut vars, &mock.runtime);
        run.guard_external_operation(
            &journal,
            &node,
            "mcp:remote",
            "remote",
            &Some(crate::tool_events::canonical_mcp_key_args(&call.args)),
            &call.id,
        )
        .unwrap();
        run.finish_external_operation_with(
            &journal,
            &node,
            &operation_id,
            engine::OperationOutcome::CleanError,
        )
        .unwrap();
        record_agent_checkpoint(
            &ctx,
            &node,
            0,
            "before_side_effect",
            &checkpoint(call.clone()),
            &tools,
        )
        .unwrap();
        run.clear_pending_tool_payload(&operation_id);
    }
    {
        let run = run(&harness, BTreeMap::new());
        let journal = journal(&harness);
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let mut ctx = context(&run, &journal, &mut vars, &mock.runtime);
        let before =
            serde_json::to_value(&journal.state().operation_records[&operation_id]).unwrap();
        let result = LlmAgentStep {
            runtime: mock.runtime.clone(),
        }
        .execute(&mut ctx, &node)
        .await;
        assert!(
            matches!(result, Err(StepError::Refused { ref message, .. }) if message.contains("placeholders")),
            "unexpected error: {:?}",
            result.err()
        );
        assert!(mock.hits().is_empty());
        assert_eq!(harness.operation_started_count(&operation_id), 1);
        assert_eq!(
            serde_json::to_value(&journal.state().operation_records[&operation_id]).unwrap(),
            before
        );
        assert!(
            run.load_pending_tool_payload(&node, &operation_id)
                .unwrap()
                .is_none()
        );
        // Supply the real original bytes, not the journal copy, for a legitimate retry.
        record_agent_checkpoint(
            &ctx,
            &node,
            0,
            "before_side_effect",
            &checkpoint(call.clone()),
            &tools,
        )
        .unwrap();
    }
    let run = run(&harness, BTreeMap::new());
    let journal = journal(&harness);
    let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
    let mut ctx = context(&run, &journal, &mut vars, &mock.runtime);
    let result = LlmAgentStep {
        runtime: mock.runtime.clone(),
    }
    .execute(&mut ctx, &node)
    .await;
    assert!(
        matches!(result, Err(StepError::Failed { ref message, .. }) if message.contains("max_iterations")),
        "unexpected error: {:?}",
        result.err()
    );
    assert_eq!(mock.hits(), vec![call.args]);
    assert_eq!(harness.operation_started_count(&operation_id), 2);
    assert_eq!(
        journal.state().operation_records[&operation_id].status,
        engine::OperationStatus::Succeeded
    );
}

/// The agent's own tools removed a file it wrote: the file is not an
/// output of the step, so the step reports no file and the pin step has
/// nothing to fail on. The removal is detected where the removal
/// happens, by the command tool, and it is a recorded fact rather than an
/// inference from the disk.
#[tokio::test]
async fn a_file_the_agents_command_removed_is_not_reported() {
    let harness = ApprovalHarness::setup("command-removal");
    let run = run(&harness, BTreeMap::new());
    let journal = journal(&harness);
    let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
    let ctx = context(&run, &journal, &mut vars, &llm::LlmRuntime::builtins());
    let mut writes = AgentWrites::default();
    // The agent wrote the file, and its command removed it again.
    let target = run.fs.resolve_write("out/note.txt").unwrap();
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"written by the agent").unwrap();
    writes.record_write("out/note.txt", target.clone());
    assert_eq!(
        writes.reported(),
        vec![target.clone()],
        "the file is an output of the step until the agent removes it"
    );
    let present_before = writes.present(&ctx);
    assert!(
        present_before.contains("out/note.txt"),
        "the file is present before the command call"
    );
    // The command removes it, and the removal is recorded.
    std::fs::remove_file(&target).unwrap();
    writes.record_removals(&ctx, &present_before);
    assert!(
        writes.reported().is_empty(),
        "a file the agent's own command removed is not an output of the step"
    );
}
