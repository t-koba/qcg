use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

use super::resources::ResourceEventData;

/// Current journal record schema version. The engine writes it in the run
/// identity record and refuses to fold a journal or state whose version it
/// does not implement, so an incompatible format fails closed instead of
/// being reinterpreted.
pub const JOURNAL_SCHEMA_VERSION: u32 = 1;

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
    pub retention_days: Option<u64>,
    /// Scheduling priority recorded when the run was (re)queued.
    #[serde(default)]
    pub priority: i32,
    /// Free-form run metadata admitted with the run. Metadata only: qcg
    /// never reads identity or enforces authorization from labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Per-run audit level raise recorded at admission; it can only raise
    /// observation persistence.
    #[serde(default)]
    pub audit_raise: Option<qcg_policy::AuditLevel>,
    /// Parent run id for runs created by fork.
    #[serde(default)]
    pub parent_run_id: Option<String>,
    /// Effective step budget after resolving the deployment ceiling.
    #[serde(default)]
    pub effective_max_total_steps: Option<usize>,
    /// Origin of the effective budget for audit display.
    #[serde(default)]
    pub effective_policy_origin: Option<String>,
    /// Durable admission instant for queue ordering. Written by fork
    /// admissions and requeues; absent entries fall back to the event
    /// timestamp (C04). Required to be explicit when present, never
    /// defaulted silently on write.
    #[serde(default)]
    pub queued_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphResolvedEventData {
    pub nodes: Vec<String>,
}
