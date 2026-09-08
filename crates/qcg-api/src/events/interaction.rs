use crate::{ConfirmSpec, FormSpec};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolBackendResolvedEventData {
    pub tool: String,
    pub backend: String,
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserInteractionEventData {
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutOfContractEventData {
    pub policy: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConfirmRequestEventData {
    pub confirm: ConfirmSpec,
    #[serde(default)]
    pub parallel: bool,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectDecision {
    Denied,
    Allowed,
    ApprovedByUser,
    DeniedByUser,
    ConfirmationRequired,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SideEffectEventData {
    pub kind: String,
    pub target: String,
    pub decision: SideEffectDecision,
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub details: Option<Value>,
    /// Canonical digest binding the approval to the exact operation (A06).
    #[serde(default)]
    pub operation_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DryRunEventData {
    pub kind: String,
    pub target: String,
    #[serde(default)]
    pub details: Option<Value>,
    #[serde(default)]
    pub operation_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunWaitingEventData {
    pub question_id: String,
    pub question: FormSpec,
}
