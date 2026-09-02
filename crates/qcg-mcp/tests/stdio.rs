#![cfg(feature = "mcp-test-server")]

use qcg_mcp::{
    McpAccess, McpAuth, McpCommandAccess, McpRuntime, McpServerSpec, McpTransport,
    OAuthCredentialStore,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const TEST_SERVER: &str = env!("CARGO_BIN_EXE_qcg_mcp_stdio_test_server");

fn command(session_id: &str) -> Vec<String> {
    vec![
        TEST_SERVER.to_owned(),
        "--session-id".to_owned(),
        session_id.to_owned(),
    ]
}

fn spec(id: &str, command: Vec<String>) -> McpServerSpec {
    McpServerSpec {
        id: id.to_owned(),
        transport: McpTransport::Stdio,
        lifecycle: qcg_mcp::McpLifecycle::Initialize,
        url: None,
        command,
        env: BTreeMap::new(),
        env_from: BTreeMap::new(),
        headers: BTreeMap::new(),
        auth: McpAuth::None,
        credential_env: None,
        auth_header: None,
        auth_prefix: String::new(),
        oauth_scopes: Vec::new(),
        oauth_client_id_env: None,
        oauth_client_secret_env: None,
        oauth_store: OAuthCredentialStore::Memory,
        allowed_hosts: Vec::new(),
        timeout_seconds: 5,
        max_response_bytes: 64 * 1024,
    }
}

fn access(commands: &[Vec<String>]) -> McpAccess {
    McpAccess {
        network_hosts: BTreeSet::new(),
        commands: commands
            .iter()
            .cloned()
            .map(McpCommandAccess::trusted_host)
            .collect(),
        workspace: std::env::current_dir().expect("test workspace should be available"),
    }
}

fn structured_content(response: &Value) -> &Value {
    &response["structuredContent"]
}

#[tokio::test]
async fn stdio_child_initializes_lists_tools_and_calls_echo() {
    let server_command = command("single-session");
    let runtime = McpRuntime::from_specs(vec![spec("single", server_command.clone())])
        .expect("stdio profile should be valid");
    let session = runtime
        .connect(
            "single",
            &access(std::slice::from_ref(&server_command)),
            CancellationToken::new(),
        )
        .await
        .expect("real stdio MCP child should initialize");

    let tools = session.list_tools().await;
    let call = session.call_tool("echo", json!({"value": "hello"})).await;
    session
        .close()
        .await
        .expect("stdio child should close cleanly");

    let tools = tools.expect("tools/list should succeed after initialize");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "echo");
    assert_eq!(tools[0].input_schema["required"], json!(["value"]));

    let call = call.expect("tools/call should succeed");
    assert_eq!(structured_content(&call)["sessionId"], "single-session");
    assert_eq!(structured_content(&call)["callCount"], 1);
    assert_eq!(structured_content(&call)["value"], "hello");
}

#[tokio::test]
async fn concurrent_stdio_sessions_keep_process_state_isolated() {
    let server_command = command("shared-profile");
    let runtime = McpRuntime::from_specs(vec![spec("shared", server_command.clone())])
        .expect("stdio profile should be valid");
    let access = access(std::slice::from_ref(&server_command));
    let session_a = runtime
        .connect("shared", &access, CancellationToken::new())
        .await
        .expect("session a should initialize");
    let session_b = runtime
        .connect("shared", &access, CancellationToken::new())
        .await
        .expect("session b should initialize");

    let (first_a, first_b) = tokio::join!(
        session_a.call_tool("echo", json!({"value": "a-1"})),
        session_b.call_tool("echo", json!({"value": "b-1"})),
    );
    let second_a = session_a.call_tool("echo", json!({"value": "a-2"})).await;
    let second_b = session_b.call_tool("echo", json!({"value": "b-2"})).await;

    session_a
        .close()
        .await
        .expect("session a child should close cleanly");
    session_b
        .close()
        .await
        .expect("session b child should close cleanly");

    let first_a = first_a.expect("session a first call should succeed");
    let first_b = first_b.expect("session b first call should succeed");
    let second_a = second_a.expect("session a second call should succeed");
    let second_b = second_b.expect("session b second call should succeed");

    assert_eq!(structured_content(&first_a)["sessionId"], "shared-profile");
    assert_eq!(structured_content(&first_a)["callCount"], 1);
    assert_eq!(structured_content(&first_a)["value"], "a-1");
    assert_eq!(structured_content(&second_a)["sessionId"], "shared-profile");
    assert_eq!(structured_content(&second_a)["callCount"], 2);
    assert_eq!(structured_content(&second_a)["value"], "a-2");

    assert_eq!(structured_content(&first_b)["sessionId"], "shared-profile");
    assert_eq!(structured_content(&first_b)["callCount"], 1);
    assert_eq!(structured_content(&first_b)["value"], "b-1");
    assert_eq!(structured_content(&second_b)["sessionId"], "shared-profile");
    assert_eq!(structured_content(&second_b)["callCount"], 2);
    assert_eq!(structured_content(&second_b)["value"], "b-2");
}

#[tokio::test]
async fn stdio_close_reaps_child_process() {
    let server_command = command("close-session");
    let runtime = McpRuntime::from_specs(vec![spec("close", server_command.clone())])
        .expect("stdio profile should be valid");
    let session = runtime
        .connect(
            "close",
            &access(std::slice::from_ref(&server_command)),
            CancellationToken::new(),
        )
        .await
        .expect("close session should initialize");

    let close_result = tokio::time::timeout(Duration::from_secs(2), session.close())
        .await
        .expect("closing a stdio child should be bounded");
    close_result.expect("stdio child process should be reaped on close");
}
