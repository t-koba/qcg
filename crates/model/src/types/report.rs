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

impl Finding {
    /// Construct a finding with error severity.
    pub fn error(message: String, location: Option<String>, raw_output: Option<String>) -> Self {
        Self {
            severity: Severity::Error,
            message,
            location,
            raw_output,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default)]
pub struct RunMetrics {
    pub steps_total: u64,
    pub steps_succeeded: u64,
    pub steps_failed: u64,
    pub steps_skipped: u64,
    pub steps_executed: u64,
    /// Durable step-budget consumption (F13): sum of `budget_charged`
    /// deltas. `steps_executed` counts `step_started` events (observational,
    /// includes uncharged foreach children); this counter drives
    /// `max_steps` enforcement and survives restarts identically.
    pub budget_charged: u64,
    pub repair_attempts: u64,
    pub regenerate_attempts: u64,
    pub llm_calls: u64,
    pub tokens_input: u64,
    pub tokens_output: u64,
    /// Input tokens served from provider cache, included in `tokens_input`.
    pub tokens_cached_input: u64,
    /// Input tokens used to create cache entries, included in total input.
    pub tokens_cache_creation_input: u64,
    pub tokens_total: u64,
    pub cost_microusd: u64,
    pub duration_ms: u64,
}
