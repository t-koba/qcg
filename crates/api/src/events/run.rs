use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use super::resources::ResourceEventData;

/// Current journal record schema version. The engine writes it in the run
/// identity record and refuses to fold a journal or state whose version it
/// does not implement, so an incompatible format fails closed instead of
/// being reinterpreted.
pub const JOURNAL_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunStartedEventData {
    pub generator: String,
    pub generator_path: String,
    pub contract_sha256: String,
    /// Full package identity for API runs. Absent on legacy/direct runs;
    /// those runs cannot be recovered or forked by the service.
    #[serde(default)]
    pub package_sha256: Option<String>,
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub answers: BTreeMap<String, Value>,
    #[serde(default)]
    pub confirmations: BTreeMap<String, bool>,
    #[serde(default)]
    pub resource_hashes: Vec<ResourceEventData>,
    pub schema_version: u32,
    /// Schema-1 absence means historical wall-clock accounting.
    #[schemars(required)]
    pub elapsed_budget_mode: policy::ElapsedBudgetMode,
    #[serde(default)]
    pub retention_days: Option<u64>,
    /// Scheduling priority recorded when the run was (re)queued.
    #[serde(default)]
    pub priority: i32,
    /// Free-form run metadata admitted with the run. Metadata only: the runtime
    /// never reads identity or enforces authorization from labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Per-run audit level raise recorded at admission; it can only raise
    /// observation persistence.
    #[serde(default)]
    pub audit_raise: Option<policy::AuditLevel>,
    /// Parent run id for runs created by fork.
    #[serde(default)]
    pub parent_run_id: Option<String>,
    /// Effective step budget after resolving the deployment ceiling.
    #[serde(default)]
    pub effective_max_total_steps: Option<usize>,
    /// Origin of the effective budget for audit display.
    #[serde(default)]
    pub effective_policy_origin: Option<String>,
    /// Durable FIFO ordering key. Scheduler preemption may preserve the
    /// prior key; fresh/fork admissions establish a new one. Absent entries
    /// use the actual event timestamp (C04), which is never backdated.
    #[serde(default)]
    pub queued_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphResolvedEventData {
    pub nodes: Vec<String>,
}

/// Single durable active elapsed ledger, preserving subsecond consumption.
#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ElapsedAccountedEventData {
    pub active_nanoseconds: u64,
    #[serde(deserialize_with = "required_active_start")]
    #[schemars(required, schema_with = "nullable_active_start_schema")]
    pub active_started_at: Option<String>,
}

fn nullable_active_start_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    <Option<String> as schemars::JsonSchema>::json_schema(generator)
}

fn required_active_start<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    let value = Option::<String>::deserialize(d)?;
    if let Some(timestamp) = &value {
        chrono::DateTime::parse_from_rfc3339(timestamp).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}
