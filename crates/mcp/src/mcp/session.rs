use crate::bounded_http::BoundedHttpClient;
use crate::bounded_stdio::CloseReceipt;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, CancelTaskParams, GetTaskParams,
    InputResponses, PaginatedRequestParams, ProtocolVersion, TaskPayload, Tool,
};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt as _, RoleClient, RunningService};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::error::{McpCallOutcome, McpError, McpInputRequired};
use super::profile::{CredentialGuard, McpProfile};
use super::runtime::McpClient;
use super::spec::{TASK_POLL_INTERVAL_MS_MAX, TASK_POLL_INTERVAL_MS_MIN};
use super::transport::McpLifecycle;
use super::validate::{guarded_transport_error, reject_credential_reflection};

/// Deterministic failure in the owned session close path, for tests only.
/// Compiled solely with the `test-probe` feature; production builds never
/// consult it and keep the real transport close result.
#[cfg(feature = "test-probe")]
pub(crate) mod close_probe {
    use std::cell::Cell;

    thread_local! {
        static FAIL_CLOSE: Cell<bool> = const { Cell::new(false) };
    }

    /// Makes an owned stdio transport's close fail for as long as this guard
    /// is alive, so the session cannot confirm that its child was cleaned up.
    pub struct FailClose;

    pub fn fail_close() -> FailClose {
        FAIL_CLOSE.set(true);
        FailClose
    }

    pub(crate) fn should_fail_close() -> bool {
        FAIL_CLOSE.get()
    }

    impl Drop for FailClose {
        fn drop(&mut self) {
            FAIL_CLOSE.set(false);
        }
    }
}

#[cfg(feature = "test-probe")]
pub use close_probe::fail_close;

pub struct McpSession {
    pub(crate) profile: McpProfile,
    pub(crate) client: Option<RunningService<RoleClient, McpClient>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) credential_guard: CredentialGuard,
    pub(crate) active_sessions: Option<Arc<AtomicUsize>>,
    owned_close_receipt: Option<CloseReceipt>,
}

impl McpSession {
    pub(crate) async fn serve<T, E, A>(
        profile: McpProfile,
        transport: T,
        cancellation: CancellationToken,
        credential_guard: CredentialGuard,
    ) -> Result<Self, McpError>
    where
        T: rmcp::transport::IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let seconds = profile.spec.timeout_seconds;
        let sensitive_values =
            bounded_credential_values(&credential_guard, &cancellation, seconds).await?;
        let client = tokio::select! {
            _ = cancellation.cancelled() => return Err(McpError::Canceled),
            result = tokio::time::timeout(
                Duration::from_secs(seconds),
                McpClient.serve_with_lifecycle(
                    transport,
                    client_lifecycle_mode(profile.spec.lifecycle),
                ),
            ) => {
                result
                    .map_err(|_| McpError::TimedOut { seconds })?
                    .map_err(|error| guarded_transport_error(error, &sensitive_values))?
            }
        };
        Ok(Self {
            profile,
            client: Some(client),
            cancellation,
            credential_guard,
            active_sessions: None,
            owned_close_receipt: None,
        })
    }

    /// Attaches the owned stdio transport's close outcome. HTTP sessions keep
    /// the default `None` and their existing close semantics.
    pub fn with_owned_close_receipt(mut self, receipt: CloseReceipt) -> Self {
        self.owned_close_receipt = Some(receipt);
        self
    }

    async fn sensitive_values(&self) -> Result<Vec<String>, McpError> {
        bounded_credential_values(
            &self.credential_guard,
            &self.cancellation,
            self.profile.spec.timeout_seconds,
        )
        .await
    }

    async fn sensitive_values_for_close(&self) -> Result<Vec<String>, McpError> {
        let seconds = self.profile.spec.timeout_seconds;
        tokio::time::timeout(Duration::from_secs(seconds), self.credential_guard.values())
            .await
            .map_err(|_| McpError::TimedOut { seconds })?
    }

    pub fn server_id(&self) -> &str {
        self.profile.id()
    }

    pub fn protocol_version(&self) -> Option<String> {
        self.client
            .as_ref()
            .and_then(|client| client.peer().peer_info())
            .map(|info| info.protocol_version.as_str().to_string())
    }

    pub async fn list_tools(&self) -> Result<Vec<McpTool>, McpError> {
        let seconds = self.profile.spec.timeout_seconds;
        let mut sensitive_values = self.sensitive_values().await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
        let page_limit = self.profile.spec.tools_list_page_limit;
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut tools = Vec::new();
        for _ in 0..page_limit {
            let result = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.cancel_transport();
                    return Err(McpError::Canceled);
                },
                result = tokio::time::timeout_at(
                    deadline,
                    self.client.as_ref().expect("active MCP client").list_tools(Some(
                        PaginatedRequestParams::default().with_cursor(cursor.clone()),
                    )),
                ) => {
                    match result {
                        Ok(result) => result
                            .map_err(|error| guarded_transport_error(error, &sensitive_values))?,
                        Err(_) => {
                            self.cancel_transport();
                            return Err(McpError::TimedOut { seconds });
                        }
                    }
                }
            };
            sensitive_values = self.sensitive_values().await?;
            tools.extend(result.tools.into_iter().map(McpTool::from));
            let encoded_size = serde_json::to_vec(&tools)
                .map_err(|error| McpError::Transport(error.to_string()))?
                .len();
            reject_credential_reflection(&tools, &sensitive_values)?;
            if encoded_size > self.profile.spec.max_response_bytes {
                return Err(McpError::Transport(format!(
                    "MCP server `{}` tool list exceeded {} bytes",
                    self.profile.id(),
                    self.profile.spec.max_response_bytes
                )));
            }
            let Some(next) = result.next_cursor else {
                return Ok(tools);
            };
            if !seen_cursors.insert(next.clone()) {
                return Err(McpError::Transport(format!(
                    "MCP server `{}` repeated a tools/list cursor",
                    self.profile.id()
                )));
            }
            cursor = Some(next);
        }
        Err(McpError::Transport(format!(
            "MCP server `{}` tools/list exceeded {page_limit} pages",
            self.profile.id()
        )))
    }

    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<Value, McpError> {
        match self
            .call_tool_with_input(name, arguments, None, None)
            .await?
        {
            McpCallOutcome::Complete(value) => Ok(value),
            McpCallOutcome::InputRequired(_) => Err(McpError::Transport(
                "MCP tool requires input; use the resumable call interface".into(),
            )),
        }
    }

    pub async fn call_tool_with_input(
        &self,
        name: &str,
        arguments: Value,
        input_responses: Option<InputResponses>,
        request_state: Option<String>,
    ) -> Result<McpCallOutcome, McpError> {
        let arguments = arguments.as_object().cloned().ok_or_else(|| {
            McpError::Configuration(format!("MCP tool `{name}` arguments must be a JSON object"))
        })?;
        let seconds = self.profile.spec.timeout_seconds;
        let mut sensitive_values = self.sensitive_values().await?;
        let mut params = CallToolRequestParams::new(name.to_string()).with_arguments(arguments);
        params.input_responses = input_responses;
        params.request_state = request_state;
        let result = match tokio::time::timeout(
            Duration::from_secs(seconds),
            self.call_tool_with_tasks(params, &sensitive_values),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                self.cancel_transport();
                return Err(McpError::TimedOut { seconds });
            }
        };
        sensitive_values = self.sensitive_values().await?;
        let result = match result {
            ToolCallResult::Complete(result) => result,
            ToolCallResult::InputRequired(input) => {
                let value = serde_json::to_value(&input)
                    .map_err(|error| McpError::Transport(error.to_string()))?;
                let encoded = serde_json::to_vec(&value)
                    .map_err(|error| McpError::Transport(error.to_string()))?;
                if encoded.len() > self.profile.spec.max_response_bytes {
                    return Err(McpError::Transport(format!(
                        "MCP server `{}` input request exceeded {} bytes",
                        self.profile.id(),
                        self.profile.spec.max_response_bytes
                    )));
                }
                reject_credential_reflection(&value, &sensitive_values)?;
                return Ok(McpCallOutcome::InputRequired(input));
            }
        };
        let value = serde_json::to_value(&result)
            .map_err(|error| McpError::Transport(error.to_string()))?;
        let encoded =
            serde_json::to_vec(&value).map_err(|error| McpError::Transport(error.to_string()))?;
        if encoded.len() > self.profile.spec.max_response_bytes {
            return Err(McpError::Transport(format!(
                "MCP server `{}` tool result exceeded {} bytes",
                self.profile.id(),
                self.profile.spec.max_response_bytes
            )));
        }
        reject_credential_reflection(&value, &sensitive_values)?;
        if result.is_error == Some(true) {
            return Err(McpError::ToolFailed {
                tool: name.to_string(),
                result: value,
            });
        }
        Ok(McpCallOutcome::Complete(value))
    }

    async fn call_tool_with_tasks(
        &self,
        params: CallToolRequestParams,
        sensitive_values: &[String],
    ) -> Result<ToolCallResult, McpError> {
        let client = self.client.as_ref().expect("active MCP client");
        let initial = tokio::select! {
            _ = self.cancellation.cancelled() => {
                self.cancel_transport();
                return Err(McpError::Canceled);
            }
            result = client.peer().call_tool_once(params) => {
                result.map_err(|error| guarded_transport_error(error, sensitive_values))?
            }
        };
        let task = match initial {
            CallToolResponse::Complete(result) => return Ok(ToolCallResult::Complete(result)),
            CallToolResponse::InputRequired(result) => {
                let input_requests = result
                    .input_requests
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(id, request)| {
                        serde_json::to_value(request)
                            .map(|request| (id, request))
                            .map_err(|error| McpError::Transport(error.to_string()))
                    })
                    .collect::<Result<_, _>>()?;
                let input = McpInputRequired {
                    input_requests,
                    request_state: result.request_state,
                };
                let encoded = serde_json::to_vec(&input)
                    .map_err(|error| McpError::Transport(error.to_string()))?;
                if encoded.len() > self.profile.spec.max_response_bytes {
                    return Err(McpError::Transport(format!(
                        "MCP server `{}` input request exceeded {} bytes",
                        self.profile.id(),
                        self.profile.spec.max_response_bytes
                    )));
                }
                reject_credential_reflection(&input, sensitive_values)?;
                return Ok(ToolCallResult::InputRequired(input));
            }
            CallToolResponse::Task(task) => task.task,
            _ => {
                return Err(McpError::Transport(
                    "MCP tool returned an unsupported asynchronous response".into(),
                ));
            }
        };
        let task_id = task.task_id;
        let mut poll_interval = task
            .poll_interval_ms
            .unwrap_or(self.profile.spec.task_poll_interval_ms)
            .clamp(TASK_POLL_INTERVAL_MS_MIN, TASK_POLL_INTERVAL_MS_MAX);
        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.best_effort_cancel_task(&task_id);
                    return Err(McpError::Canceled);
                }
                _ = tokio::time::sleep(Duration::from_millis(poll_interval)) => {}
            }
            let detailed = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.best_effort_cancel_task(&task_id);
                    return Err(McpError::Canceled);
                }
                result = client.peer().get_task(GetTaskParams::new(task_id.clone())) => {
                    result
                        .map_err(|error| guarded_transport_error(error, sensitive_values))?
                        .task
                }
            };
            poll_interval = detailed
                .task
                .poll_interval_ms
                .unwrap_or(poll_interval)
                .clamp(TASK_POLL_INTERVAL_MS_MIN, TASK_POLL_INTERVAL_MS_MAX);
            match detailed.payload {
                TaskPayload::Working => {}
                TaskPayload::Completed { result } => {
                    return serde_json::from_value(Value::Object(result))
                        .map(ToolCallResult::Complete)
                        .map_err(|_| {
                            McpError::Transport(
                                "MCP task completed with an invalid tool result".into(),
                            )
                        });
                }
                TaskPayload::InputRequired { .. } => {
                    return Err(McpError::Transport(
                        "MCP task requested interactive input outside the HITL boundary".into(),
                    ));
                }
                TaskPayload::Failed { error } => {
                    let error = Value::Object(error);
                    reject_credential_reflection(&error, sensitive_values)?;
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("MCP task failed");
                    return Err(McpError::ToolFailed {
                        tool: "task".into(),
                        result: serde_json::json!({
                            "content": [{ "type": "text", "text": message }],
                            "isError": true,
                            "_meta": { "runtime": { "taskError": error } }
                        }),
                    });
                }
                TaskPayload::Cancelled => return Err(McpError::Canceled),
                _ => {
                    return Err(McpError::Transport(
                        "MCP task returned an unsupported status payload".into(),
                    ));
                }
            }
        }
    }

    /// Best-effort remote task cleanup, detached so an observed
    /// cancellation keeps its `Canceled` classification even against short
    /// profile deadlines: awaiting cleanup inside the profile timeout lets
    /// the deadline overwrite `Canceled` with `TimedOut`. `tasks/cancel` is
    /// cooperative, so acknowledgement never proves work stopped. The
    /// detached attempt stays bounded by its own 5s timeout, matching
    /// session close.
    fn best_effort_cancel_task(&self, task_id: &str) {
        if let Some(client) = self.client.as_ref() {
            let peer = client.peer().clone();
            let task_id = task_id.to_owned();
            tokio::spawn(async move {
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    peer.cancel_task(CancelTaskParams::new(task_id)),
                )
                .await;
            });
        }
    }

    fn cancel_transport(&self) {
        if let Some(client) = self.client.as_ref() {
            client.cancellation_token().cancel();
        }
    }

    pub async fn close(mut self) -> Result<(), McpError> {
        let sensitive_values = self.sensitive_values_for_close().await?;
        let result = self
            .client
            .take()
            .expect("active MCP client")
            .close_with_timeout(Duration::from_secs(5))
            .await
            .map_err(|error| guarded_transport_error(error, &sensitive_values))?;
        if result.is_none() {
            return Err(McpError::TimedOut { seconds: 5 });
        }
        // The SDK service loop logs a failed transport close but still
        // reports a completed close. Only a confirmed owned close may settle
        // the launch; without a receipt (HTTP) the SDK result stands alone.
        if let Some(receipt) = &self.owned_close_receipt
            && !receipt.confirmed()
        {
            return Err(McpError::Transport(
                "owned MCP stdio transport close was not confirmed".into(),
            ));
        }
        Ok(())
    }
}

enum ToolCallResult {
    Complete(CallToolResult),
    InputRequired(McpInputRequired),
}

/// Maps the configured profile lifecycle to the pinned rmcp handshake.
///
/// No implicit fallback exists: each profile negotiates exactly the lifecycle
/// it selects.
pub(crate) fn client_lifecycle_mode(lifecycle: McpLifecycle) -> ClientLifecycleMode {
    match lifecycle {
        McpLifecycle::Initialize => ClientLifecycleMode::Initialize,
        McpLifecycle::Discover => ClientLifecycleMode::Discover {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        },
    }
}

async fn bounded_credential_values(
    guard: &CredentialGuard,
    cancellation: &CancellationToken,
    seconds: u64,
) -> Result<Vec<String>, McpError> {
    tokio::select! {
        _ = cancellation.cancelled() => Err(McpError::Canceled),
        result = tokio::time::timeout(Duration::from_secs(seconds), guard.values()) => {
            result.map_err(|_| McpError::TimedOut { seconds })?
        }
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        self.cancellation.cancel();
        drop(self.client.take());
        if let Some(active_sessions) = self.active_sessions.take() {
            active_sessions.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct McpTool {
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub input_schema: Value,
    pub output_schema: Option<Value>,
}

impl From<Tool> for McpTool {
    fn from(tool: Tool) -> Self {
        Self {
            name: tool.name.into_owned(),
            title: tool.title,
            description: tool.description.map(|value| value.into_owned()),
            input_schema: Value::Object((*tool.input_schema).clone()),
            output_schema: tool
                .output_schema
                .map(|value| Value::Object((*value).clone())),
        }
    }
}

pub(crate) fn mcp_http_client(profile: &McpProfile) -> Result<BoundedHttpClient, McpError> {
    BoundedHttpClient::new(
        profile.spec.timeout_seconds,
        profile.spec.max_response_bytes,
    )
    .map_err(|error| McpError::Configuration(error.to_string()))
}
