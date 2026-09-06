use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuredOutputMode {
    #[default]
    Auto,
    NativeStrict,
    NativeCompatible,
    Prompt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub severity: Severity,
    pub message: String,
    pub location: Option<String>,
    pub raw_output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct RunMetrics {
    #[serde(default)]
    pub steps_total: u64,
    #[serde(default)]
    pub steps_succeeded: u64,
    #[serde(default)]
    pub steps_failed: u64,
    #[serde(default)]
    pub steps_skipped: u64,
    #[serde(default)]
    pub steps_executed: u64,
    #[serde(default)]
    pub repair_attempts: u64,
    #[serde(default)]
    pub regenerate_attempts: u64,
    #[serde(default)]
    pub llm_calls: u64,
    #[serde(default)]
    pub tokens_input: u64,
    #[serde(default)]
    pub tokens_output: u64,
    /// Input tokens served from provider cache, included in `tokens_input`.
    #[serde(default)]
    pub tokens_cached_input: u64,
    #[serde(default)]
    pub tokens_total: u64,
    #[serde(default)]
    pub cost_microusd: u64,
    #[serde(default)]
    pub duration_ms: u64,
}
