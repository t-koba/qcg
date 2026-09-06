use crate::{ConfirmSpec, FormSpec};
use qcg_types::FailureDetail;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Success,
    Repaired,
    RepairFailed,
    RecheckFailed,
    CheckFailed,
    NeedsUser,
    NeedsConfirm,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttemptFinishedEventData {
    pub attempt: u32,
    pub status: AttemptStatus,
    #[serde(default)]
    pub reason: Option<FailureDetail>,
    #[serde(default)]
    pub findings: Vec<Value>,
    #[serde(default)]
    pub question: Option<FormSpec>,
    #[serde(default)]
    pub confirm: Option<ConfirmSpec>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TokenUsageEventData {
    pub input: u64,
    pub output: u64,
    /// Reasoning tokens included in `output`.
    #[serde(default)]
    pub reasoning: u64,
    /// Input tokens served from provider cache, included in `input`.
    #[serde(default)]
    pub cached_input: u64,
}
