use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Provider-neutral reasoning effort requested for a model invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

/// Provider-neutral policy for whether a model may call an exposed tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(ToolChoiceMode),
    Tool { tool: String },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoiceMode {
    None,
    #[default]
    Auto,
    Required,
}

impl ToolChoice {
    pub const fn none() -> Self {
        Self::Mode(ToolChoiceMode::None)
    }

    pub const fn auto() -> Self {
        Self::Mode(ToolChoiceMode::Auto)
    }

    pub const fn required() -> Self {
        Self::Mode(ToolChoiceMode::Required)
    }
}

impl Default for ToolChoice {
    fn default() -> Self {
        Self::auto()
    }
}

/// Provider-neutral control for the detail of a model response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResponseVerbosity {
    Low,
    Medium,
    High,
}

impl ReasoningEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
