use crate::bounded_http::BoundedHttpClient;
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
use super::runtime::QcgMcpClient;
use super::transport::McpLifecycle;
use super::validate::{guarded_transport_error, reject_credential_reflection};

const MAX_TOOL_LIST_PAGES: usize = 100;

pub struct McpSession {
    pub(crate) profile: McpProfile,
    pub(crate) client: Option<RunningService<RoleClient, QcgMcpClient>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) credential_guard: CredentialGuard,
    pub(crate) active_sessions: Option<Arc<AtomicUsize>>,
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
                QcgMcpClient.serve_with_lifecycle(
                    transport,
                    match profile.spec.lifecycle {
                        McpLifecycle::Initialize => ClientLifecycleMode::Initialize,
                        McpLifecycle::Discover => ClientLifecycleMode::Discover {
                            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                        },
                    },
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
        })
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
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut tools = Vec::new();
        for _ in 0..MAX_TOOL_LIST_PAGES {
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
            "MCP server `{}` tools/list exceeded {MAX_TOOL_LIST_PAGES} pages",
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
                return Ok(ToolCallResult::InputRequired(McpInputRequired {
                    input_requests,
                    request_state: result.request_state,
                }));
            }
            CallToolResponse::Task(task) => task.task,
            _ => {
                return Err(McpError::Transport(
                    "MCP tool returned an unsupported asynchronous response".into(),
                ));
            }
        };
        let task_id = task.task_id;
        let mut poll_interval = task.poll_interval_ms.unwrap_or(250).clamp(50, 5_000);
        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    let _ = client
                        .peer()
                        .cancel_task(CancelTaskParams::new(task_id.clone()))
                        .await;
                    return Err(McpError::Canceled);
                }
                _ = tokio::time::sleep(Duration::from_millis(poll_interval)) => {}
            }
            let detailed = client
                .peer()
                .get_task(GetTaskParams::new(task_id.clone()))
                .await
                .map_err(|error| guarded_transport_error(error, sensitive_values))?
                .task;
            poll_interval = detailed
                .task
                .poll_interval_ms
                .unwrap_or(poll_interval)
                .clamp(50, 5_000);
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
                        "MCP task requested interactive input outside the qcg HITL boundary".into(),
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
                            "_meta": { "qcg": { "taskError": error } }
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
        Ok(())
    }
}

enum ToolCallResult {
    Complete(CallToolResult),
    InputRequired(McpInputRequired),
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
