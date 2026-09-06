use qcg_types::ArtifactPreview;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    #[serde(default)]
    pub extras: Vec<OutputExtraDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputExtraDef {
    pub glob: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub preview: ArtifactPreview,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FailurePolicy {
    #[serde(default)]
    pub default: FailureAction,
    #[serde(default)]
    pub by_kind: BTreeMap<FailureKind, FailureAction>,
}

impl FailurePolicy {
    pub fn action(&self, kind: FailureKind) -> FailureAction {
        self.by_kind.get(&kind).copied().unwrap_or(self.default)
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FailureAction {
    Reject,
    Clarify,
    Clamp,
    #[default]
    Fail,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Schema,
    Range,
    Permission,
    OutOfContract,
    Execution,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JournalPolicy {
    #[serde(default)]
    pub retain_days: Option<u32>,
}
