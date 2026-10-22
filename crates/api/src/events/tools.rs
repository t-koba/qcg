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
    /// The session cleanup that followed the tool call failed. The
    /// execution outcome stands on its own; the cleanup failure is
    /// retained here so it can be settled separately.
    #[serde(default)]
    pub cleanup_error: Option<ToolCallError>,
    pub duration_ms: u64,
    pub arguments: Value,
    pub result: Value,
    pub sources: Vec<ToolCallSource>,
    pub truncated: bool,
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
    /// A per-attempt node timeout fired (E11). Distinct from
    /// `ExecutionFailed` so timeouts are observable and retryable.
    TimedOut,
    /// The run-wide elapsed budget stopped this call (E11). Distinct from
    /// node timeout and cancellation.
    ElapsedExceeded,
    /// The session cleanup that followed the tool call failed. The call's
    /// own outcome is reported separately and stands.
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
