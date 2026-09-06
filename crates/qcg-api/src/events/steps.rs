use crate::FormSpec;
use qcg_types::{FailureDetail, RunMetrics};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepStartedEventData {
    #[serde(rename = "type")]
    pub step_type: String,
    pub attempt: u32,
    #[serde(default)]
    pub parallel: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Success,
    Repaired,
    Routed,
    RepairExhausted,
    AnsweredOnFail,
    Regenerated,
    RegenerateExhausted,
    NeedsUser,
    NeedsConfirm,
    CheckFailed,
    Failed,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepFinishedEventData {
    pub status: StepStatus,
    #[serde(default)]
    pub files: Vec<Value>,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub failed_output: Option<Value>,
    #[serde(default)]
    pub output_name: Option<String>,
    #[serde(default)]
    pub failed_files: Vec<Value>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub findings: Vec<Value>,
    #[serde(default)]
    pub reason: Option<FailureDetail>,
    #[serde(default)]
    pub answer: Option<Value>,
    #[serde(default)]
    pub question: Option<FormSpec>,
    #[serde(default)]
    pub parallel: bool,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepReplayedEventData {
    pub status: StepStatus,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReasonEventData {
    pub reason: FailureDetail,
    /// Accumulated cost metrics, present when the writer attached them.
    #[serde(default)]
    pub metrics: RunMetrics,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForeachIterationEventData {
    pub index: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForeachBudgetEventData {
    pub requested_iterations: usize,
    pub executed_iterations: usize,
    pub max_iterations: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RepairAttemptStartedEventData {
    pub repair: String,
    pub recheck: String,
    pub attempt: u32,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepRetryEventData {
    pub attempt: u32,
    pub max_attempts: u32,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegenerateAttemptStartedEventData {
    pub attempt: u32,
    pub max_attempts: u32,
}
