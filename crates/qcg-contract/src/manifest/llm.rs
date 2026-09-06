use qcg_types::{ResponseVerbosity, StructuredOutputMode, ToolChoice};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::resources::{default_max_total_steps, default_template_fuel, default_timeout_seconds};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmConfig {
    #[serde(default)]
    pub model: Option<ModelRef>,
    #[serde(default)]
    pub models: Vec<ModelRef>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stop_sequences: Vec<String>,
    #[serde(default)]
    pub max_context_bytes: Option<usize>,
    #[serde(default)]
    pub max_context_tokens: Option<usize>,
    #[serde(default)]
    pub max_media_bytes: Option<usize>,
    #[serde(default)]
    pub context_overflow: ContextOverflowPolicy,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub retry_prompt: Option<String>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub reasoning_effort: Option<qcg_types::ReasoningEffort>,
    #[serde(default)]
    pub structured_output: StructuredOutputMode,
    #[serde(default)]
    pub tool_choice: Option<ToolChoice>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub verbosity: Option<ResponseVerbosity>,
}

/// Per-invocation LLM policy layered over the generator-wide `[llm]` defaults.
///
/// The provider registry advertises transport capabilities. This value only
/// selects behavior for one node or specialist invocation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmRequestPolicy {
    #[serde(default)]
    pub clear: Vec<LlmRequestControl>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub reasoning_effort: Option<qcg_types::ReasoningEffort>,
    #[serde(default)]
    pub structured_output: Option<StructuredOutputMode>,
    #[serde(default)]
    pub tool_choice: Option<ToolChoice>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub verbosity: Option<ResponseVerbosity>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub max_context_bytes: Option<usize>,
    #[serde(default)]
    pub max_context_tokens: Option<usize>,
    #[serde(default)]
    pub max_media_bytes: Option<usize>,
    #[serde(default)]
    pub context_overflow: Option<ContextOverflowPolicy>,
    #[serde(default)]
    pub retry_prompt: Option<String>,
}

/// Optional inherited request controls that an inner policy layer may omit.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum LlmRequestControl {
    Temperature,
    TopP,
    StopSequences,
    Seed,
    ReasoningEffort,
    ToolChoice,
    ParallelToolCalls,
    Verbosity,
}

impl LlmRequestControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Temperature => "temperature",
            Self::TopP => "top_p",
            Self::StopSequences => "stop_sequences",
            Self::Seed => "seed",
            Self::ReasoningEffort => "reasoning_effort",
            Self::ToolChoice => "tool_choice",
            Self::ParallelToolCalls => "parallel_tool_calls",
            Self::Verbosity => "verbosity",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContextOverflowPolicy {
    #[default]
    Error,
    TruncateHead,
    TruncateTail,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRef {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub input_cost_per_million_usd: Option<f64>,
    #[serde(default)]
    pub output_cost_per_million_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLimits {
    #[serde(default = "default_timeout_seconds")]
    pub command_timeout_seconds: u64,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub command_input_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub command_output_limit_bytes: Option<usize>,
    #[serde(default = "default_timeout_seconds")]
    pub http_timeout_seconds: u64,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub http_body_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub http_redirect_limit: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub file_input_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub file_count_limit: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub input_total_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub output_file_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub output_total_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub output_artifact_limit: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub template_source_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub template_context_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub journal_event_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub journal_total_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub journal_event_count_limit: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub state_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub template_output_limit_bytes: Option<usize>,
    #[serde(default = "default_template_fuel")]
    pub template_fuel: u64,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            command_timeout_seconds: default_timeout_seconds(),
            command_input_limit_bytes: None,
            command_output_limit_bytes: None,
            http_timeout_seconds: default_timeout_seconds(),
            http_body_limit_bytes: None,
            http_redirect_limit: None,
            file_input_limit_bytes: None,
            file_count_limit: None,
            input_total_limit_bytes: None,
            output_file_limit_bytes: None,
            output_total_limit_bytes: None,
            output_artifact_limit: None,
            template_source_limit_bytes: None,
            template_context_limit_bytes: None,
            journal_event_limit_bytes: None,
            journal_total_limit_bytes: None,
            journal_event_count_limit: None,
            state_limit_bytes: None,
            template_output_limit_bytes: None,
            template_fuel: default_template_fuel(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunBudget {
    #[serde(default = "default_max_total_steps")]
    pub max_steps: usize,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub max_cost_usd: Option<f64>,
    #[serde(default)]
    pub max_elapsed_seconds: Option<u64>,
}

impl Default for RunBudget {
    fn default() -> Self {
        Self {
            max_steps: default_max_total_steps(),
            max_tokens: None,
            max_cost_usd: None,
            max_elapsed_seconds: None,
        }
    }
}
