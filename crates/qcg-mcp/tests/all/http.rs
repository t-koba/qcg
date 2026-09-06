#![cfg(feature = "mcp-test-server")]

use qcg_mcp::{
    McpAccess, McpAuth, McpCallOutcome, McpRuntime, McpServerSpec, McpTransport,
    OAuthCredentialStore,
};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ElicitRequest, ElicitRequestParams,
    InputRequest, InputRequiredResult, ListToolsResult, PaginatedRequestParams, ServerCapabilities,
    ServerInfo, Tool,
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
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
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
        qcg_mcp::McpLifecycle::Discover,
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
    spec_with_lifecycle(id, url, qcg_mcp::McpLifecycle::Initialize)
}

fn spec_with_lifecycle(id: &str, url: &str, lifecycle: qcg_mcp::McpLifecycle) -> McpServerSpec {
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
        timeout_seconds: 5,
        max_response_bytes: 64 * 1024,
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
        qcg_mcp::McpLifecycle::Discover,
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
    assert!(matches!(error, qcg_mcp::McpError::Canceled));

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
    assert!(matches!(error, qcg_mcp::McpError::TimedOut { seconds: 1 }));
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
