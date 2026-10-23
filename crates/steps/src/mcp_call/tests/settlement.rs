//! Real MCP execution with a bounded injected close failure at the settlement seam.
use super::direct::execute_direct_mcp_call;
use super::settlement::settle_direct_mcp;
use super::types::McpCallParams;
use super::validate::DirectMcpCallOutcome;
use api::McpCleanupPolicy::{self, FailFast, PreserveExecution};
use camino::Utf8PathBuf;
use contract::{Contract, NodeDef, ValueBag};
use engine::{JournalWriter, RunContext, StepContext, StepError, StepOutcome};
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
use serde_json::{Value, json};
use std::collections::BTreeMap;
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
        if args["large"] == true {
            return Ok(CallToolResult::structured(json!({"text": "x".repeat(70 * 1024)})).into());
        }
        Ok(CallToolResult::structured(json!({"done": true})).into())
    }
}

struct MockMcp {
    hits: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
    runtime: mcp::McpRuntime,
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

        Self {
            hits,
            task,
            runtime: mcp::McpRuntime::from_specs(vec![spec]).unwrap(),
        }
    }
}

struct Harness {
    root: Utf8PathBuf,
    run: RunContext,
    node: NodeDef,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Harness {
    fn new() -> Self {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("mcp-cleanup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "cleanup-test"
name = "Cleanup test"
version = "0.1.0"
[[flow]]
id = "call"
type = "mcp.call"
[flow.params]
server = "mock"
tool = "echo"
"#,
        )
        .unwrap();
        let contract = Contract::load(root.clone()).unwrap();
        let node = contract.manifest.flow[0].clone();
        let run = RunContext::for_tests(
            "cleanup-test".into(),
            root.join("workspace"),
            root.join("meta"),
            contract,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .unwrap();
        Self { root, run, node }
    }

    fn journal(&self) -> JournalWriter {
        JournalWriter::create(
            &self.root.join("journal.jsonl"),
            &self.run.run_id,
            false,
            None,
        )
        .unwrap()
    }

    fn records(&self, kind: &str) -> Vec<Value> {
        // The cleanup record is durable, not a best-effort observation.
        std::fs::read_to_string(self.root.join("journal.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|record| record["t"] == kind)
            .collect()
    }
}

fn params(policy: Option<McpCleanupPolicy>, optional: bool) -> McpCallParams {
    serde_json::from_value(json!({"server": "mock", "tool": "echo", "arguments": {},
        "optional": optional, "cleanup_failure": policy}))
    .unwrap()
}

fn settle(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    policy: Option<McpCleanupPolicy>,
    optional: bool,
    execution: Result<DirectMcpCallOutcome, StepError>,
) -> Result<StepOutcome, StepError> {
    let cleanup_error = StepError::failed(&node.id, "injected session close failure");
    settle_direct_mcp(
        ctx,
        node,
        &params(policy, optional),
        execution,
        &json!({}),
        Some(&cleanup_error),
        0,
    )
}

async fn execute(
    mock: &MockMcp,
    ctx: &StepContext<'_>,
    node: &NodeDef,
    ask: bool,
) -> Result<DirectMcpCallOutcome, StepError> {
    let session = mock
        .runtime
        .connect(
            "mock",
            &mcp::McpAccess {
                network_hosts: ["127.0.0.1".to_owned()].into(),
                commands: Vec::new(),
                workspace: ctx.run.workspace.as_std_path().to_owned(),
            },
            ctx.run.cancellation.clone(),
        )
        .await
        .unwrap();
    let result = execute_direct_mcp_call(
        &session,
        "echo",
        json!({"ask": ask}),
        ctx,
        node,
        3,
        1024 * 1024,
    )
    .await;
    session.close().await.unwrap();
    result
}

async fn execute_large(
    mock: &MockMcp,
    ctx: &StepContext<'_>,
    node: &NodeDef,
) -> Result<DirectMcpCallOutcome, StepError> {
    let session = mock
        .runtime
        .connect(
            "mock",
            &mcp::McpAccess {
                network_hosts: ["127.0.0.1".to_owned()].into(),
                commands: Vec::new(),
                workspace: ctx.run.workspace.as_std_path().to_owned(),
            },
            ctx.run.cancellation.clone(),
        )
        .await
        .unwrap();
    let result = execute_direct_mcp_call(
        &session,
        "echo",
        json!({"ask": false, "large": true}),
        ctx,
        node,
        3,
        1024 * 1024,
    )
    .await;
    session.close().await.unwrap();
    result
}

#[tokio::test]
async fn completed_result_survives_every_cleanup_disposition_and_replays_without_dispatch() {
    let mock = MockMcp::start().await;
    for policy in [Some(PreserveExecution), Some(FailFast), None] {
        let harness = Harness::new();
        let journal = harness.journal();
        let mut vars = ValueBag::with_inputs(BTreeMap::new());
        let ctx = StepContext {
            run: &harness.run,
            journal: &journal,
            vars: &mut vars,
            llm: None,
        };
        let execution = execute(&mock, &ctx, &harness.node, false).await;
        let Ok(DirectMcpCallOutcome::Complete(ref raw)) = execution else {
            panic!("{execution:?}")
        };
        let raw = raw.clone();
        let before = mock.hits.lock().unwrap().len();
        let settled = settle(&ctx, &harness.node, policy, false, execution);
        assert_disposition(&settled, policy);
        let records = harness.records("mcp_cleanup_settled");
        assert_eq!(records.len(), 1);
        let fact = &records[0];
        assert_eq!(fact["status"], "succeeded");
        assert_eq!(fact["result"]["value"], raw);
        assert_policy(fact, policy);
        let audit = std::fs::read_to_string(harness.root.join("audit.jsonl")).unwrap();
        let call: Value = audit
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|record| record["t"] == "tool_call")
            .unwrap();
        assert_eq!(call["result"]["value"], raw);
        assert_policy(&call, policy);
        drop(journal);
        // Reopen from disk: the external-operation guard returns the cached
        // completed result, rather than sending a second tools/call.
        let journal = harness.journal();
        let ctx = StepContext {
            run: &harness.run,
            journal: &journal,
            vars: &mut vars,
            llm: None,
        };
        let replay = execute(&mock, &ctx, &harness.node, false).await.unwrap();
        assert!(matches!(replay, DirectMcpCallOutcome::Complete(value) if value == raw));
        assert_eq!(mock.hits.lock().unwrap().len(), before);
    }
}

#[tokio::test]
async fn pending_continuation_survives_every_cleanup_disposition() {
    let mock = MockMcp::start().await;
    for policy in [Some(PreserveExecution), Some(FailFast), None] {
        let harness = Harness::new();
        let journal = harness.journal();
        let mut vars = ValueBag::with_inputs(BTreeMap::new());
        let ctx = StepContext {
            run: &harness.run,
            journal: &journal,
            vars: &mut vars,
            llm: None,
        };
        let execution = execute(&mock, &ctx, &harness.node, true).await;
        let Ok(DirectMcpCallOutcome::NeedsUser(ref question)) = execution else {
            panic!("{execution:?}")
        };
        let question_id = question.id.clone();
        let pending = journal.state().mcp_pending;
        assert_eq!(pending.len(), 1);
        let settled = settle(&ctx, &harness.node, policy, false, execution);
        assert_disposition(&settled, policy);
        if policy == Some(PreserveExecution) {
            assert!(
                matches!(settled, Ok(StepOutcome::NeedsUser { question, .. }) if question.id == question_id)
            );
        }
        assert_eq!(journal.state().mcp_pending, pending);
        let fact = &harness.records("mcp_cleanup_settled")[0];
        assert_eq!(fact["status"], "needs_user");
        assert_policy(fact, policy);
        drop(journal);
        assert_eq!(harness.journal().state().mcp_pending, pending);
    }
}

fn assert_disposition(result: &Result<StepOutcome, StepError>, policy: Option<McpCleanupPolicy>) {
    match policy {
        Some(PreserveExecution) => assert!(result.is_ok(), "{result:?}"),
        Some(FailFast) => assert!(
            matches!(result, Err(StepError::Refused { message, .. }) if message.contains("fail_fast"))
        ),
        None => assert!(
            matches!(result, Err(StepError::Refused { message, .. }) if message.contains("cleanup settlement policy required"))
        ),
    }
}

fn assert_policy(fact: &Value, policy: Option<McpCleanupPolicy>) {
    assert_eq!(fact["cleanup_error"]["code"], "cleanup_failed");
    if let Some(policy) = policy {
        assert_eq!(
            fact["cleanup_settlement"]["policy"],
            serde_json::to_value(policy).unwrap()
        );
        assert_eq!(fact["cleanup_settlement"]["source"], "node_parameter");
    } else {
        assert!(
            fact["cleanup_settlement"].is_null(),
            "missing policy is not a selected disposition"
        );
    }
}

#[test]
fn execution_errors_keep_their_own_handling_including_optional_degradation() {
    for policy in [Some(PreserveExecution), Some(FailFast), None] {
        for optional in [false, true] {
            let harness = Harness::new();
            let journal = harness.journal();
            let mut vars = ValueBag::with_inputs(BTreeMap::new());
            let ctx = StepContext {
                run: &harness.run,
                journal: &journal,
                vars: &mut vars,
                llm: None,
            };
            let settled = settle(
                &ctx,
                &harness.node,
                policy,
                optional,
                Err(StepError::failed("call", "execution failed")),
            );
            if optional {
                assert!(matches!(
                    settled,
                    Ok(StepOutcome::Success { output: None, .. })
                ));
            } else {
                assert!(
                    matches!(settled, Err(StepError::Failed { message, .. }) if message == "execution failed")
                );
            }
            let fact = &harness.records("mcp_cleanup_settled")[0];
            assert_eq!(fact["status"], if optional { "degraded" } else { "failed" });
            assert_eq!(fact["cleanup_error"]["code"], "cleanup_failed");
            assert!(
                fact["cleanup_settlement"].is_null(),
                "execution errors never select a cleanup policy"
            );
        }
    }
}

#[test]
fn missing_policy_is_not_defaulted_and_invalid_values_are_rejected() {
    let harness = Harness::new();
    let mut node = harness.node.clone();
    assert!(
        super::validate::mcp_call_params(&node)
            .unwrap()
            .cleanup_failure
            .is_none()
    );
    node.params.insert(
        "cleanup_failure".into(),
        toml::Value::String("ignore".into()),
    );
    assert!(super::validate::mcp_call_params(&node).is_err());
}

#[test]
fn cleanup_success_needs_no_policy() {
    let harness = Harness::new();
    let journal = harness.journal();
    let mut vars = ValueBag::with_inputs(BTreeMap::new());
    let ctx = StepContext {
        run: &harness.run,
        journal: &journal,
        vars: &mut vars,
        llm: None,
    };
    let outcome = settle_direct_mcp(
        &ctx,
        &harness.node,
        &params(None, false),
        Ok(DirectMcpCallOutcome::Complete(json!({"ok": true}))),
        &json!({}),
        None,
        0,
    )
    .unwrap();
    assert!(
        matches!(outcome, StepOutcome::Success { output: Some(value), .. } if value["result"]["ok"] == true)
    );
    assert!(harness.records("mcp_cleanup_settled").is_empty());
}

#[tokio::test]
async fn oversized_results_stay_bounded_in_events_but_replay_exact() {
    let mock = MockMcp::start().await;
    let harness = Harness::new();
    let audit = policy::AuditPolicy::from_config(&policy::AuditConfig {
        level: policy::AuditLevel::Minimal,
        ..Default::default()
    })
    .unwrap();
    let journal = JournalWriter::create_with_policies(
        &harness.root.join("journal.jsonl"),
        &harness.run.run_id,
        false,
        None,
        engine::JournalLimits::default(),
        audit,
        policy::AuditLimits::default(),
    )
    .unwrap();
    let mut vars = ValueBag::with_inputs(BTreeMap::new());
    let ctx = StepContext {
        run: &harness.run,
        journal: &journal,
        vars: &mut vars,
        llm: None,
    };
    let execution = execute_large(&mock, &ctx, &harness.node).await;
    let Ok(DirectMcpCallOutcome::Complete(ref raw)) = execution else {
        panic!("{execution:?}");
    };
    let raw = raw.clone();
    let raw_bytes = serde_json::to_vec(&raw).unwrap().len();
    assert!(
        raw_bytes > 64 * 1024,
        "large fixture must exceed the event bound"
    );
    let before = mock.hits.lock().unwrap().len();
    let outcome = settle(&ctx, &harness.node, None, false, execution);
    assert_disposition(&outcome, None);
    let fact = &harness.records("mcp_cleanup_settled")[0];
    assert_eq!(fact["truncated"], true);
    assert_eq!(fact["result"]["value"]["bytes"], raw_bytes);
    assert!(fact["result"]["value"]["summary"].is_string());
    assert_policy(fact, None);
    assert!(!harness.root.join("audit.jsonl").exists());
    drop(journal);
    let journal = harness.journal();
    let ctx = StepContext {
        run: &harness.run,
        journal: &journal,
        vars: &mut vars,
        llm: None,
    };
    let replay = execute_large(&mock, &ctx, &harness.node).await.unwrap();
    assert!(matches!(replay, DirectMcpCallOutcome::Complete(value) if value == raw));
    assert_eq!(mock.hits.lock().unwrap().len(), before);
}
