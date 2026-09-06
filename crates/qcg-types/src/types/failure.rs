use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::path::NodePath;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    WhenFalse,
    DependencyUnsatisfied,
    NoDependencySucceeded,
    CheckFailed,
    ExecutionFailed,
    RepairExhausted,
    SchedulerFailed,
    Canceled,
    BudgetExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DependencyStatus {
    Skipped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DependencyFailure {
    pub path: NodePath,
    pub status: DependencyStatus,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FailureDetail {
    pub code: FailureCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<DependencyFailure>,
}

impl FailureDetail {
    pub fn new(code: FailureCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            dependencies: Vec::new(),
        }
    }

    pub fn execution(message: impl Into<String>) -> Self {
        Self::new(FailureCode::ExecutionFailed, message)
    }
}

impl fmt::Display for FailureDetail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::ops::Deref for FailureDetail {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl From<String> for FailureDetail {
    fn from(message: String) -> Self {
        Self::execution(message)
    }
}

impl From<&str> for FailureDetail {
    fn from(message: &str) -> Self {
        Self::execution(message)
    }
}
