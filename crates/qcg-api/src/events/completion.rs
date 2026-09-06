use qcg_types::{FailureDetail, NodePath, RunMetrics};
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunErrorEventData {
    pub error: String,
    /// Accumulated cost metrics, present when the writer attached them.
    #[serde(default)]
    pub metrics: RunMetrics,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunCompletionStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunFinishedEventData {
    pub status: RunCompletionStatus,
    #[serde(default)]
    pub reason: Option<FailureDetail>,
    #[serde(default)]
    pub failures: Vec<RunNodeFailureEventData>,
    pub metrics: RunMetrics,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunNodeFailureEventData {
    pub path: NodePath,
    pub failure: FailureDetail,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LaggedEventData {
    pub action: LaggedAction,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LaggedAction {
    ResyncSnapshot,
}
