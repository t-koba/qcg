//! Agent failure taxonomy shared by the contract policy table, the agent
//! executor, and the `agent_failed` wire event.

use schemars::JsonSchema;
use serde::Serialize;

#[derive(
    Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentFailureCode {
    ToolFailed,
    GuardrailRejected,
    TokenBudgetExceeded,
    ToolCallBudgetExceeded,
    IterationBudgetExceeded,
    RunBudgetExceeded,
    ValidationFailed,
    ProviderFailed,
    Cancelled,
}

impl AgentFailureCode {
    pub fn is_recoverable(self) -> bool {
        !matches!(self, Self::RunBudgetExceeded | Self::Cancelled)
    }

    pub fn policy_code(self) -> Option<RecoverableAgentFailureCode> {
        match self {
            Self::ToolFailed => Some(RecoverableAgentFailureCode::ToolFailed),
            Self::GuardrailRejected => Some(RecoverableAgentFailureCode::GuardrailRejected),
            Self::TokenBudgetExceeded => Some(RecoverableAgentFailureCode::TokenBudgetExceeded),
            Self::ToolCallBudgetExceeded => {
                Some(RecoverableAgentFailureCode::ToolCallBudgetExceeded)
            }
            Self::IterationBudgetExceeded => {
                Some(RecoverableAgentFailureCode::IterationBudgetExceeded)
            }
            Self::ValidationFailed => Some(RecoverableAgentFailureCode::ValidationFailed),
            Self::ProviderFailed => Some(RecoverableAgentFailureCode::ProviderFailed),
            Self::RunBudgetExceeded | Self::Cancelled => None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "snake_case")]
pub enum RecoverableAgentFailureCode {
    ToolFailed,
    GuardrailRejected,
    TokenBudgetExceeded,
    ToolCallBudgetExceeded,
    IterationBudgetExceeded,
    ValidationFailed,
    ProviderFailed,
}

#[derive(Debug, Clone, Copy, Default, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentFailureAction {
    Fail,
    #[default]
    ReturnError,
}
