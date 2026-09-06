use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailViolationEventData {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailStage {
    Input,
    Output,
    ToolInput,
    ToolOutput,
}

#[derive(Debug, Clone, Copy, Default, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailErrorPolicy {
    #[default]
    Fail,
    Block,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailErrorKind {
    InvalidConfiguration,
    Evaluation,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailEvaluatedEventData {
    pub guardrail: String,
    pub kind: String,
    pub stage: GuardrailStage,
    #[serde(default)]
    pub tool: Option<String>,
    pub passed: bool,
    pub tripwire: bool,
    #[serde(default)]
    pub violation: Option<GuardrailViolationEventData>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailErrorEventData {
    pub guardrail: String,
    pub kind: String,
    pub stage: GuardrailStage,
    #[serde(default)]
    pub tool: Option<String>,
    pub error_kind: GuardrailErrorKind,
    pub code: String,
    pub message: String,
    pub policy: GuardrailErrorPolicy,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailTripwireEventData {
    pub guardrail: String,
    pub kind: String,
    pub stage: GuardrailStage,
    #[serde(default)]
    pub tool: Option<String>,
    pub violation: GuardrailViolationEventData,
}
