use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallEventData {
    #[serde(default)]
    pub server: Option<String>,
    pub tool: String,
    pub id: String,
    pub status: ToolCallStatus,
    pub phase: ToolCallPhase,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub error: Option<ToolCallError>,
    /// Cleanup failure retained beside the execution outcome.
    #[serde(default)]
    pub cleanup_error: Option<ToolCallError>,
    /// Explicit cleanup policy and its configuration source; absent when none was supplied.
    #[serde(default)]
    pub cleanup_settlement: Option<McpCleanupSettlement>,
    pub duration_ms: u64,
    pub arguments: Value,
    pub result: Value,
    pub sources: Vec<ToolCallSource>,
    pub truncated: bool,
}
/// Explicit per-node settlement of a failed direct MCP session cleanup.
#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpCleanupPolicy {
    PreserveExecution,
    FailFast,
}
#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct McpCleanupSettlement {
    pub policy: McpCleanupPolicy,
    pub source: McpCleanupPolicySource,
}
#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpCleanupPolicySource {
    NodeParameter,
}
#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    Succeeded,
    Failed,
    NeedsUser,
    NeedsConfirmation,
    Degraded,
}
#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallPhase {
    InputValidation,
    InputGuardrail,
    Execution,
    OutputGuardrail,
    Completed,
}
#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallError {
    pub code: ToolCallErrorCode,
    pub message: String,
}
#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallErrorCode {
    InvalidArguments,
    GuardrailRejected,
    ExecutionFailed,
    OutputRejected,
    Cancelled,
    BudgetExceeded,
    ToolReportedError,
    /// Per-attempt node timeout (E11), distinct from `ExecutionFailed`.
    TimedOut,
    /// Run-wide elapsed budget stop (E11), distinct from timeout/cancel.
    ElapsedExceeded,
    /// Session cleanup failed; the call outcome stands separately.
    CleanupFailed,
}
#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallSource {
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(cleanup_error: Option<ToolCallError>) -> ToolCallEventData {
        ToolCallEventData {
            server: Some("mcp:exa/exa_search".into()),
            tool: "mcp:exa/exa_search".into(),
            id: "mcp-1".into(),
            status: ToolCallStatus::Succeeded,
            phase: ToolCallPhase::Completed,
            agent: None,
            error: None,
            cleanup_error,
            cleanup_settlement: None,
            duration_ms: 12,
            arguments: json!({"argument_names": []}),
            result: json!({"bytes": 2, "value": {}}),
            sources: Vec::new(),
            truncated: false,
        }
    }

    #[test]
    fn the_cleanup_failure_is_retained_beside_the_execution_outcome() {
        let cleanup = ToolCallError {
            code: ToolCallErrorCode::CleanupFailed,
            message: "MCP session close timed out".into(),
        };
        let payload = serde_json::to_value(event(Some(cleanup))).expect("event serializes");
        assert_eq!(
            payload["cleanup_error"]["code"],
            json!("cleanup_failed"),
            "the cleanup failure keeps its own code"
        );
        assert_eq!(payload["status"], json!("succeeded"));
        let read: ToolCallEventData = serde_json::from_value(payload).expect("event deserializes");
        assert_eq!(
            read.cleanup_error.map(|error| error.code),
            Some(ToolCallErrorCode::CleanupFailed)
        );
    }

    #[test]
    fn an_event_without_the_cleanup_fact_still_reads() {
        let mut payload = serde_json::to_value(event(None)).expect("event serializes");
        payload
            .as_object_mut()
            .expect("object")
            .remove("cleanup_error");
        let read: ToolCallEventData =
            serde_json::from_value(payload).expect("an older event still reads");
        assert!(read.cleanup_error.is_none());
    }
}
