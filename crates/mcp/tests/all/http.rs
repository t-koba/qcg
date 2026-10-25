//! Live streamable-HTTP MCP integration tests. The feature gate keeps these
//! server-backed assertions out of the default test target.
#![cfg(feature = "mcp-test-server")]

use mcp::{
    McpAccess, McpAuth, McpCallOutcome, McpRuntime, McpServerSpec, McpTransport,
    OAuthCredentialStore,
};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, CancelTaskParams, CreateTaskResult,
    DetailedTask, ElicitRequest, ElicitRequestParams, GetTaskParams, GetTaskResult, InputRequest,
    InputRequiredResult, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig,
    Task, TaskPayload, TaskStatus, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{ErrorData, ServerHandler};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
struct SessionServer {
    session_number: u64,
    calls: Arc<AtomicU64>,
}

impl ServerHandler for SessionServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: vec![
                Tool::new(
                    "echo",
                    "Echo one string.",
                    object(json!({
                        "type": "object",
                        "properties": { "value": { "type": "string" } },
                        "required": ["value"],
                        "additionalProperties": false
                    }))?,
                ),
                Tool::new(
                    "large",
                    "Return a large result.",
                    object(json!({
                        "type": "object",
                        "additionalProperties": false
                    }))?,
                ),
                Tool::new(
                    "slow",
                    "Wait before returning.",
                    object(json!({
                        "type": "object",
                        "additionalProperties": false
                    }))?,
                ),
                Tool::new(
                    "elicit",
                    "Require one MRTR form response.",
                    object(json!({
                        "type": "object",
                        "additionalProperties": false
                    }))?,
                ),
            ],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name == "large" {
            return Ok(CallToolResult::structured(json!({
                "value": "x".repeat(128 * 1024),
            }))
            .into());
        }
        if request.name == "slow" {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            return Ok(CallToolResult::structured(json!({ "finished": true })).into());
        }
        if request.name == "elicit" {
            if let Some(responses) = request.input_responses {
                return Ok(CallToolResult::structured(json!({
                    "response": responses.get("profile")
                }))
                .into());
            }
            let elicitation = InputRequest::Elicitation(ElicitRequest::new(
                ElicitRequestParams::FormElicitationParams {
                    meta: None,
                    message: "Provide a display name".into(),
                    requested_schema: serde_json::from_value(json!({
                        "type": "object",
                        "properties": { "name": { "type": "string" } },
                        "required": ["name"]
                    }))
                    .expect("test elicitation schema"),
                },
            ));
            return Ok(InputRequiredResult::new(
                Some(BTreeMap::from([("profile".into(), elicitation)])),
                Some("opaque-state".into()),
            )
            .into());
        }
        let value = request
            .arguments
            .as_ref()
            .and_then(|arguments| arguments.get("value"))
            .and_then(Value::as_str)
            .ok_or_else(|| ErrorData::invalid_params("value must be a string", None))?;
        let call_number = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CallToolResult::structured(json!({
            "session": self.session_number,
            "call": call_number,
            "value": value,
        }))
        .into())
    }
}

#[tokio::test]
async fn modern_mrtr_is_exposed_for_durable_hitl_and_can_resume() {
    let cancellation = CancellationToken::new();
    let service: StreamableHttpService<SessionServer, LocalSessionManager> =
        StreamableHttpService::new(
            || {
                Ok(SessionServer {
                    session_number: 1,
                    calls: Arc::new(AtomicU64::new(0)),
                })
            },
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true)
                .with_cancellation_token(cancellation.child_token()),
        );
    let app = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_cancellation = cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
            .unwrap();
    });
    let runtime = McpRuntime::from_specs(vec![spec_with_lifecycle(
        "mrtr",
        &format!("http://{address}/mcp"),
        mcp::McpLifecycle::Discover,
    )])
    .unwrap();
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().unwrap(),
    };
    let session = runtime
        .connect("mrtr", &access, CancellationToken::new())
        .await
        .unwrap();
    let McpCallOutcome::InputRequired(required) = session
        .call_tool_with_input("elicit", json!({}), None, None)
        .await
        .unwrap()
    else {
        panic!("first MRTR response must require input");
    };
    assert_eq!(required.request_state.as_deref(), Some("opaque-state"));
    assert!(required.input_requests.contains_key("profile"));
    let McpCallOutcome::Complete(completed) = session
        .call_tool_with_input(
            "elicit",
            json!({}),
            Some(BTreeMap::from([(
                "profile".into(),
                json!({"action":"accept","content":{"name":"Ada"}}),
            )])),
            required.request_state,
        )
        .await
        .unwrap()
    else {
        panic!("answered MRTR response must complete");
    };
    assert_eq!(
        completed["structuredContent"]["response"]["content"]["name"],
        "Ada"
    );
    cancellation.cancel();
    server.await.unwrap();
}

fn object(value: Value) -> Result<Map<String, Value>, ErrorData> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ErrorData::internal_error("test schema must be an object", None))
}

fn spec(id: &str, url: &str) -> McpServerSpec {
    spec_with_lifecycle(id, url, mcp::McpLifecycle::Initialize)
}

fn spec_with_lifecycle(id: &str, url: &str, lifecycle: mcp::McpLifecycle) -> McpServerSpec {
    McpServerSpec {
        id: id.to_string(),
        transport: McpTransport::StreamableHttp,
        lifecycle,
        url: Some(url.to_string()),
        command: vec![],
        env: BTreeMap::new(),
        env_from: BTreeMap::new(),
        headers: BTreeMap::new(),
        auth: McpAuth::None,
        credential_env: None,
        auth_header: None,
        auth_prefix: String::new(),
        oauth_scopes: vec![],
        oauth_client_id_env: None,
        oauth_client_secret_env: None,
        oauth_store: OAuthCredentialStore::Memory,
        allowed_hosts: vec!["127.0.0.1".into()],
        allowed_destinations: Vec::new(),
        destination_decider: Vec::new(),
        destination_decider_timeout_seconds: None,
        timeout_seconds: 5,
        max_response_bytes: 64 * 1024,
        tools_list_page_limit: 100,
        oauth_state_ttl_seconds: 600,
        task_poll_interval_ms: 250,
    }
}

#[tokio::test]
async fn malformed_success_json_fails_immediately_instead_of_timing_out() {
    let app = axum::Router::new().route(
        "/mcp",
        axum::routing::post(|| async {
            (
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                r#"{"jsonrpc":"2.0","result":}"#,
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("malformed MCP server should run");
    });
    let runtime = McpRuntime::from_specs(vec![spec("malformed", &format!("http://{address}/mcp"))])
        .expect("MCP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let started = std::time::Instant::now();
    let error = match runtime
        .connect("malformed", &access, CancellationToken::new())
        .await
    {
        Ok(_) => panic!("malformed JSON-RPC must fail during initialization"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("invalid JSON-RPC"), "{error}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(4),
        "malformed response should fail before the five-second profile timeout"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn streamable_http_sessions_use_real_protocol_and_keep_state_isolated() {
    let next_session = Arc::new(AtomicU64::new(0));
    let factory_counter = next_session.clone();
    let cancellation = CancellationToken::new();
    let service: StreamableHttpService<SessionServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || {
                Ok(SessionServer {
                    session_number: factory_counter.fetch_add(1, Ordering::SeqCst) + 1,
                    calls: Arc::new(AtomicU64::new(0)),
                })
            },
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(cancellation.child_token()),
        );
    let app = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let server_cancellation = cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
            .expect("MCP test server should run");
    });

    let url = format!("http://{address}/mcp");
    let runtime =
        McpRuntime::from_specs(vec![spec("shared", &url)]).expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let (one, two) = tokio::join!(
        runtime.connect("shared", &access, CancellationToken::new()),
        runtime.connect("shared", &access, CancellationToken::new()),
    );
    let one = one.expect("first HTTP MCP session should initialize");
    let two = two.expect("second HTTP MCP session should initialize");
    assert_eq!(one.protocol_version().as_deref(), Some("2025-11-25"));
    assert_eq!(two.protocol_version().as_deref(), Some("2025-11-25"));
    let tools = one.list_tools().await.expect("tools/list");
    assert_eq!(tools[0].name, "echo");
    assert_eq!(tools[1].name, "large");

    let (one_result, two_result) = tokio::join!(
        one.call_tool("echo", json!({ "value": "one" })),
        two.call_tool("echo", json!({ "value": "two" })),
    );
    let one_result = one_result.expect("first call should complete");
    let two_result = two_result.expect("second call should complete");
    assert_ne!(
        one_result["structuredContent"]["session"],
        two_result["structuredContent"]["session"]
    );
    assert_eq!(one_result["structuredContent"]["call"], 1);
    assert_eq!(two_result["structuredContent"]["call"], 1);

    one.close().await.expect("first session should close");
    two.close().await.expect("second session should close");
    cancellation.cancel();
    server.await.expect("server task should stop");
}

#[tokio::test]
async fn streamable_http_bounds_raw_json_before_deserialization() {
    let cancellation = CancellationToken::new();
    let service: StreamableHttpService<SessionServer, LocalSessionManager> =
        StreamableHttpService::new(
            || {
                Ok(SessionServer {
                    session_number: 1,
                    calls: Arc::new(AtomicU64::new(0)),
                })
            },
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true)
                .with_cancellation_token(cancellation.child_token()),
        );
    let app = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let server_cancellation = cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
            .expect("MCP test server should run");
    });

    let url = format!("http://{address}/mcp");
    let runtime = McpRuntime::from_specs(vec![spec_with_lifecycle(
        "bounded",
        &url,
        mcp::McpLifecycle::Discover,
    )])
    .expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let session = runtime
        .connect("bounded", &access, CancellationToken::new())
        .await
        .expect("stateless MCP session should initialize");
    assert_eq!(session.protocol_version().as_deref(), Some("2026-07-28"));
    let error = session
        .call_tool("large", json!({}))
        .await
        .expect_err("raw JSON response must be bounded before deserialization");
    assert!(
        error.to_string().contains("exceeded 65536 bytes"),
        "{error}"
    );

    cancellation.cancel();
    server.await.expect("server task should stop");
}

#[tokio::test]
async fn streamable_http_call_honors_run_cancellation() {
    let server_cancellation = CancellationToken::new();
    let service: StreamableHttpService<SessionServer, LocalSessionManager> =
        StreamableHttpService::new(
            || {
                Ok(SessionServer {
                    session_number: 1,
                    calls: Arc::new(AtomicU64::new(0)),
                })
            },
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(server_cancellation.child_token()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let shutdown = server_cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().nest_service("/mcp", service))
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .expect("MCP test server should run");
    });

    let runtime = McpRuntime::from_specs(vec![spec("cancel", &format!("http://{address}/mcp"))])
        .expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let run_cancellation = CancellationToken::new();
    let session = runtime
        .connect("cancel", &access, run_cancellation.clone())
        .await
        .expect("MCP session should initialize");
    let cancel = run_cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
    });
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        session.call_tool("slow", json!({})),
    )
    .await
    .expect("cancellation should stop the call promptly")
    .expect_err("the call should be canceled");
    assert!(matches!(error, mcp::McpError::Canceled));

    drop(session);
    server_cancellation.cancel();
    server.await.expect("server task should stop");
}

#[derive(Debug, Clone)]
struct TaskStallServer {
    get_calls: Arc<AtomicU64>,
    cancel_calls: Arc<AtomicU64>,
}

impl ServerHandler for TaskStallServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tasks()
                .build(),
        )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "task-slow",
                "Stalled task.",
                object(json!({
                    "type": "object",
                    "additionalProperties": false
                }))?,
            )],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name != "task-slow" {
            return Err(ErrorData::invalid_params("unknown tool", None));
        }
        Ok(CreateTaskResult::new(Task::new(
            "task-1",
            TaskStatus::Working,
            "2025-11-25T10:30:00Z",
            "2025-11-25T10:40:00Z",
        ))
        .into())
    }

    async fn get_task(
        &self,
        _request: GetTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetTaskResult, ErrorData> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        Ok(GetTaskResult::new(DetailedTask::new(
            Task::new(
                "task-1",
                TaskStatus::Working,
                "2025-11-25T10:30:00Z",
                "2025-11-25T10:40:00Z",
            ),
            TaskPayload::Working,
        )))
    }

    async fn cancel_task(
        &self,
        _request: CancelTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        Ok(())
    }
}

#[tokio::test]
async fn streamable_http_task_poll_honors_run_cancellation() {
    let get_calls = Arc::new(AtomicU64::new(0));
    let cancel_calls = Arc::new(AtomicU64::new(0));
    let state = TaskStallServer {
        get_calls: Arc::clone(&get_calls),
        cancel_calls: Arc::clone(&cancel_calls),
    };
    let server_cancellation = CancellationToken::new();
    let service: StreamableHttpService<TaskStallServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(state.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(server_cancellation.child_token()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().nest_service("/mcp", service))
            .await
            .expect("MCP test server should run");
    });

    let mut profile = spec("task-cancel", &format!("http://{address}/mcp"));
    profile.timeout_seconds = 15;
    profile.task_poll_interval_ms = 50;
    let runtime = McpRuntime::from_specs(vec![profile]).expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let run_cancellation = CancellationToken::new();
    let session = runtime
        .connect("task-cancel", &access, run_cancellation.clone())
        .await
        .expect("MCP session should initialize");
    // Pinned, not spawned: the session must outlive the call or its drop
    // tears down the transport the detached cleanup travels on.
    let call = session.call_tool("task-slow", json!({}));
    tokio::pin!(call);
    let waited = std::time::Instant::now();
    while get_calls.load(Ordering::SeqCst) == 0 {
        if waited.elapsed() > std::time::Duration::from_secs(5) {
            panic!("stalled poll never began");
        }
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
            _ = &mut call => panic!("stalled call finished before poll began"),
        }
    }
    let started = std::time::Instant::now();
    run_cancellation.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), call)
        .await
        .expect("stalled poll and cleanup must stay bounded")
        .expect_err("stalled task must cancel");
    assert!(
        matches!(error, mcp::McpError::Canceled),
        "stalled task must stay Canceled, got: {error}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "cancellation must win well before the 15s profile timeout"
    );
    assert!(
        get_calls.load(Ordering::SeqCst) >= 1,
        "cancel must happen after the poll began"
    );
    // Detached remote cleanup travels over HTTP after Canceled returns.
    let cleanup_waited = std::time::Instant::now();
    while cancel_calls.load(Ordering::SeqCst) == 0 {
        if cleanup_waited.elapsed() > std::time::Duration::from_secs(2) {
            panic!("bounded remote cleanup must be attempted");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn streamable_http_task_poll_honors_cancellation_near_deadline() {
    // Short profiles must not turn observed cancellation into TimedOut: the
    // 5s cleanup bound alone exceeds the whole operation budget here, so
    // cleanup stays detached and Canceled wins.
    let get_calls = Arc::new(AtomicU64::new(0));
    let cancel_calls = Arc::new(AtomicU64::new(0));
    let state = TaskStallServer {
        get_calls: Arc::clone(&get_calls),
        cancel_calls: Arc::clone(&cancel_calls),
    };
    let server_cancellation = CancellationToken::new();
    let service: StreamableHttpService<TaskStallServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(state.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(server_cancellation.child_token()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().nest_service("/mcp", service))
            .await
            .expect("MCP test server should run");
    });

    let mut profile = spec("task-deadline", &format!("http://{address}/mcp"));
    profile.timeout_seconds = 3;
    profile.task_poll_interval_ms = 50;
    let runtime = McpRuntime::from_specs(vec![profile]).expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let run_cancellation = CancellationToken::new();
    let session = runtime
        .connect("task-deadline", &access, run_cancellation.clone())
        .await
        .expect("MCP session should initialize");
    // Pinned, not spawned: the session must outlive the call or its drop
    // tears down the transport the detached cleanup travels on.
    let call = session.call_tool("task-slow", json!({}));
    tokio::pin!(call);
    let waited = std::time::Instant::now();
    while get_calls.load(Ordering::SeqCst) == 0 {
        if waited.elapsed() > std::time::Duration::from_secs(5) {
            panic!("stalled poll never began");
        }
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
            _ = &mut call => panic!("stalled call finished before poll began"),
        }
    }
    let started = std::time::Instant::now();
    run_cancellation.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), call)
        .await
        .expect("cancellation must beat the 3s profile deadline")
        .expect_err("stalled task must cancel");
    assert!(
        matches!(error, mcp::McpError::Canceled),
        "cancellation near the deadline must stay Canceled, got: {error}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "cancellation must win before the 3s profile timeout"
    );
    let cleanup_waited = std::time::Instant::now();
    while cancel_calls.load(Ordering::SeqCst) == 0 {
        if cleanup_waited.elapsed() > std::time::Duration::from_secs(2) {
            panic!("bounded remote cleanup must be attempted");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    server.abort();
    let _ = server.await;
}

#[derive(Debug, Clone, Default)]
struct TaskInputServer;

impl ServerHandler for TaskInputServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tasks()
                .build(),
        )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "task-ask",
                "Task that wants input.",
                object(json!({
                    "type": "object",
                    "additionalProperties": false
                }))?,
            )],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name != "task-ask" {
            return Err(ErrorData::invalid_params("unknown tool", None));
        }
        Ok(CreateTaskResult::new(Task::new(
            "task-1",
            TaskStatus::Working,
            "2025-11-25T10:30:00Z",
            "2025-11-25T10:40:00Z",
        ))
        .into())
    }

    async fn get_task(
        &self,
        _request: GetTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetTaskResult, ErrorData> {
        Ok(GetTaskResult::new(DetailedTask::new(
            Task::new(
                "task-1",
                TaskStatus::InputRequired,
                "2025-11-25T10:30:00Z",
                "2025-11-25T10:40:00Z",
            ),
            TaskPayload::InputRequired {
                input_requests: BTreeMap::new(),
            },
        )))
    }

    async fn cancel_task(
        &self,
        _request: CancelTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        Ok(())
    }
}

#[tokio::test]
async fn streamable_http_task_input_required_fails_at_hitl_boundary() {
    // Only direct (synchronous) elicitation rides the durable journaled
    // HITL path: a polled task that asks for interactive input must fail
    // explicitly instead of suspending on an in-memory handle.
    let server_cancellation = CancellationToken::new();
    let service: StreamableHttpService<TaskInputServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(TaskInputServer),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(server_cancellation.child_token()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let shutdown = server_cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().nest_service("/mcp", service))
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .expect("MCP test server should run");
    });

    let mut profile = spec("task-input", &format!("http://{address}/mcp"));
    profile.timeout_seconds = 15;
    profile.task_poll_interval_ms = 50;
    let runtime = McpRuntime::from_specs(vec![profile]).expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let session = runtime
        .connect("task-input", &access, CancellationToken::new())
        .await
        .expect("MCP session should initialize");
    let error = session
        .call_tool("task-ask", json!({}))
        .await
        .expect_err("task input must fail, never suspend");
    assert!(
        matches!(&error, mcp::McpError::Transport(message) if message.contains("outside the HITL boundary")),
        "task input must fail at the HITL boundary, got: {error}"
    );

    drop(session);
    server_cancellation.cancel();
    server.await.expect("server task should stop");
}

#[derive(Debug, Clone, Default)]
struct TaskRestartServer {
    calls: Arc<AtomicU64>,
    latest: Arc<std::sync::Mutex<String>>,
    polls: Arc<AtomicU64>,
}

impl ServerHandler for TaskRestartServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tasks()
                .build(),
        )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "task-again",
                "Task that completes once re-issued.",
                object(json!({
                    "type": "object",
                    "additionalProperties": false
                }))?,
            )],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name != "task-again" {
            return Err(ErrorData::invalid_params("unknown tool", None));
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let task_id = format!("task-{call}");
        *self.latest.lock().expect("task ledger should unlock") = task_id.clone();
        Ok(CreateTaskResult::new(Task::new(
            task_id,
            TaskStatus::Working,
            "2025-11-25T10:30:00Z",
            "2025-11-25T10:40:00Z",
        ))
        .into())
    }

    async fn get_task(
        &self,
        _request: GetTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetTaskResult, ErrorData> {
        // The orphaned generation never completes for its lost client:
        // only a re-issued call (a second task) observes completion, with
        // its own fresh task id echoed in the result.
        self.polls.fetch_add(1, Ordering::SeqCst);
        let latest = self
            .latest
            .lock()
            .expect("task ledger should unlock")
            .clone();
        if self.calls.load(Ordering::SeqCst) < 2 {
            return Ok(GetTaskResult::new(DetailedTask::new(
                Task::new(
                    latest,
                    TaskStatus::Working,
                    "2025-11-25T10:30:00Z",
                    "2025-11-25T10:40:00Z",
                ),
                TaskPayload::Working,
            )));
        }
        let result = json!({
            "content": [],
            "structuredContent": { "task": latest },
        })
        .as_object()
        .cloned()
        .expect("task result should be an object");
        Ok(GetTaskResult::new(DetailedTask::new(
            Task::new(
                latest,
                TaskStatus::Completed,
                "2025-11-25T10:30:00Z",
                "2025-11-25T10:40:00Z",
            ),
            TaskPayload::Completed { result },
        )))
    }

    async fn cancel_task(
        &self,
        _request: CancelTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        Ok(())
    }
}

#[tokio::test]
async fn streamable_http_task_poll_restarts_with_a_fresh_task() {
    // Task handles live in memory: losing the session mid-poll (a restart)
    // cannot resume the orphaned server task. The next call starts over
    // with a new task instead of hanging on the dead handle.
    let calls = Arc::new(AtomicU64::new(0));
    let polls = Arc::new(AtomicU64::new(0));
    let state = TaskRestartServer {
        calls: Arc::clone(&calls),
        latest: Arc::new(std::sync::Mutex::new(String::new())),
        polls: Arc::clone(&polls),
    };
    let server_cancellation = CancellationToken::new();
    let service: StreamableHttpService<TaskRestartServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(state.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(server_cancellation.child_token()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let shutdown = server_cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().nest_service("/mcp", service))
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .expect("MCP test server should run");
    });

    let mut profile = spec("task-restart", &format!("http://{address}/mcp"));
    profile.timeout_seconds = 15;
    profile.task_poll_interval_ms = 50;
    let runtime = McpRuntime::from_specs(vec![profile]).expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    // First generation, scoped so the restart drops it whole: the pinned
    // (never spawned) future models the process dying mid-poll with the
    // task handle still in memory.
    {
        let session = runtime
            .connect("task-restart", &access, CancellationToken::new())
            .await
            .expect("MCP session should initialize");
        let call = session.call_tool("task-again", json!({}));
        tokio::pin!(call);
        let waited = std::time::Instant::now();
        while polls.load(Ordering::SeqCst) == 0 {
            if waited.elapsed() > std::time::Duration::from_secs(5) {
                panic!("restart fixture never began polling");
            }
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
                _ = &mut call => panic!("orphan generation must stay in flight"),
            }
        }
        // Still pending while its task reports Working: genuinely mid-poll.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut call)
                .await
                .is_err(),
            "the doomed call must still be polling"
        );
    }
    // Restart: the in-flight future and its session are gone; task-1 is
    // orphaned with no client handle left to resume it.
    drop(runtime);
    // Next generation: a fresh runtime and session re-issue the call.
    let mut profile = spec("task-restart", &format!("http://{address}/mcp"));
    profile.timeout_seconds = 15;
    profile.task_poll_interval_ms = 50;
    let runtime = McpRuntime::from_specs(vec![profile]).expect("HTTP profile should load");
    let session = runtime
        .connect("task-restart", &access, CancellationToken::new())
        .await
        .expect("MCP session should initialize");
    let completed = session
        .call_tool("task-again", json!({}))
        .await
        .expect("re-issued call must complete");
    assert_eq!(
        completed["structuredContent"]["task"],
        json!("task-2"),
        "restart must complete the fresh task, got: {completed}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "restart must re-issue, never resume the orphan"
    );

    drop(session);
    server_cancellation.cancel();
    server.await.expect("server task should stop");
}

#[tokio::test]
async fn streamable_http_timeout_closes_the_session() {
    let server_cancellation = CancellationToken::new();
    let service: StreamableHttpService<SessionServer, LocalSessionManager> =
        StreamableHttpService::new(
            || {
                Ok(SessionServer {
                    session_number: 1,
                    calls: Arc::new(AtomicU64::new(0)),
                })
            },
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(server_cancellation.child_token()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener.local_addr().expect("listener has an address");
    let shutdown = server_cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().nest_service("/mcp", service))
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .expect("MCP test server should run");
    });

    let mut profile = spec("timeout", &format!("http://{address}/mcp"));
    profile.timeout_seconds = 1;
    let runtime = McpRuntime::from_specs(vec![profile]).expect("HTTP profile should load");
    let access = McpAccess {
        network_hosts: BTreeSet::from(["127.0.0.1".into()]),
        commands: vec![],
        workspace: std::env::current_dir().expect("workspace should exist"),
    };
    let session = runtime
        .connect("timeout", &access, CancellationToken::new())
        .await
        .expect("MCP session should initialize");
    let error = session
        .call_tool("slow", json!({}))
        .await
        .expect_err("the slow call should time out");
    assert!(matches!(error, mcp::McpError::TimedOut { seconds: 1 }));
    assert!(
        session
            .call_tool("echo", json!({ "value": "late" }))
            .await
            .is_err(),
        "a timed-out session must not be reused"
    );

    drop(session);
    server_cancellation.cancel();
    server.await.expect("server task should stop");
}
