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
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallSource {
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
}
