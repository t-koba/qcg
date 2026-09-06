use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

use super::resources::ResourceEventData;

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunStartedEventData {
    pub generator: String,
    pub generator_path: String,
    pub contract_sha256: String,
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub answers: BTreeMap<String, Value>,
    #[serde(default)]
    pub confirmations: BTreeMap<String, bool>,
    #[serde(default)]
    pub resource_hashes: Vec<ResourceEventData>,
    pub qcg: String,
    pub schema_version: u32,
    #[serde(default)]
    pub retain_days: Option<u64>,
    /// Scheduling priority recorded when the run was (re)queued.
    #[serde(default)]
    pub priority: i32,
    /// Parent run id for runs created by fork.
    #[serde(default)]
    pub parent_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphResolvedEventData {
    pub nodes: Vec<String>,
}
