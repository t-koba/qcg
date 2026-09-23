//! Event classification and audit policy.
//!
//! The durable/observation split is a mechanism fact, not a configuration
//! choice: a record kind is durable exactly when `RunState::apply` folds it.
//! Observation kinds are listed here so every layer (contract validation,
//! engine writer, service readers) agrees on what audit policy may filter.
//! Any kind not listed is durable, so a newly added durable record is never
//! silently dropped by an unclassified audit policy.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Record classification. `Durable` records build `RunState` and are never
/// filtered. `Observation` records carry audit detail only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventClass {
    Durable,
    Observation,
}

/// Observation record kinds. Keep in sync with `RunState::apply`: a kind
/// that folds must never appear here, and a kind that does not fold must.
pub const OBSERVATION_EVENT_KINDS: &[&str] = &[
    "graph_resolved",
    "step_retry",
    "step_replayed",
    "foreach_iteration",
    "foreach_budget_exhausted",
    "repair_attempt_finished",
    "regenerate_attempt_finished",
    "llm_delta",
    "agent_delegated",
    "agent_completed",
    "agent_failed",
    "agent_handoff",
    "context_compacted",
    "llm_validation_failed",
    "llm_route_failed",
    "tool_call",
    "guardrail_evaluated",
    "guardrail_error",
    "guardrail_tripwire",
    "tool_backend_resolved",
    "user_interaction",
    "out_of_contract",
    "side_effect",
    "dry_run",
    "artifact",
];

/// Classifies a record kind. Unknown kinds are durable (fail-safe).
pub fn event_class(kind: &str) -> EventClass {
    if OBSERVATION_EVENT_KINDS.contains(&kind) {
        EventClass::Observation
    } else {
        EventClass::Durable
    }
}

/// Per-class audit persistence mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditMode {
    /// Persist the record as written (default; matches the pre-policy
    /// behavior where every observation record was stored).
    #[default]
    Full,
    /// Persist a digest of the payload instead of its content.
    Digest,
    /// Do not persist the record. It is also not broadcast: a filtered
    /// record is indistinguishable from a record the run never emitted.
    Off,
}

/// Audit preset declared by the contract designer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditLevel {
    /// Persist every observation record (default).
    #[default]
    Standard,
    /// Persist no observation record unless a class override raises it.
    Minimal,
}

impl AuditLevel {
    pub fn default_mode(self) -> AuditMode {
        match self {
            Self::Standard => AuditMode::Full,
            Self::Minimal => AuditMode::Off,
        }
    }
}

/// Deployment-wide audit floor. A floor can only tighten, never loosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditFloor {
    /// No floor: the contract decides.
    #[default]
    Minimal,
    /// Every observation record is persisted regardless of contract policy.
    Standard,
}

impl AuditFloor {
    /// Applies the floor to a policy. The floor is expressed as the minimum
    /// per-class mode; `Minimal` imposes no constraint.
    pub fn apply(self, policy: &mut AuditPolicy) {
        if self == Self::Standard {
            policy.default_mode = AuditMode::Full;
            policy.classes.retain(|_, mode| *mode == AuditMode::Full);
        }
    }
}

/// Contract `[audit]` section.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditConfig {
    #[serde(default)]
    pub level: AuditLevel,
    /// Per-kind overrides. Keys must be observation kinds.
    #[serde(default)]
    pub classes: BTreeMap<String, AuditMode>,
    /// Total audit bytes retained for the run before degradation.
    #[serde(default)]
    pub max_bytes: Option<usize>,
    /// Audit record count retained for the run before degradation.
    #[serde(default)]
    pub max_events: Option<usize>,
}

impl AuditConfig {
    /// Raises this configuration to the deployment floor. A `Standard` floor
    /// forces every observation class to `Full` and discards lower overrides.
    pub fn apply_floor(&mut self, floor: AuditFloor) {
        if floor == AuditFloor::Standard {
            self.level = AuditLevel::Standard;
            self.classes.retain(|_, mode| *mode == AuditMode::Full);
        }
    }
}

/// Resolved audit policy handed to the writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditPolicy {
    pub default_mode: AuditMode,
    pub classes: BTreeMap<String, AuditMode>,
}

impl Default for AuditPolicy {
    fn default() -> Self {
        Self {
            default_mode: AuditMode::Full,
            classes: BTreeMap::new(),
        }
    }
}

impl AuditPolicy {
    /// Validates class keys and resolves the preset. Durable kinds are
    /// rejected: accepting them would claim a filter that cannot exist.
    pub fn from_config(config: &AuditConfig) -> Result<Self, String> {
        for kind in config.classes.keys() {
            if event_class(kind) == EventClass::Durable {
                return Err(format!(
                    "audit class `{kind}` is a durable record and cannot be filtered"
                ));
            }
        }
        Ok(Self {
            default_mode: config.level.default_mode(),
            classes: config.classes.clone(),
        })
    }

    /// Raises the policy to the deployment floor. A `Standard` floor forces
    /// every observation class to `Full`, discarding lower overrides.
    pub fn apply_floor(&mut self, floor: AuditFloor) {
        floor.apply(self);
    }

    pub fn mode_for(&self, kind: &str) -> AuditMode {
        self.classes.get(kind).copied().unwrap_or(self.default_mode)
    }

    pub fn is_full(&self) -> bool {
        self.default_mode == AuditMode::Full
            && self.classes.values().all(|mode| *mode == AuditMode::Full)
    }
}

/// Audit retention bounds. Breaching one degrades audit persistence for the
/// run (recorded durably) instead of failing the run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuditLimits {
    pub max_event_bytes: Option<usize>,
    pub max_total_bytes: Option<usize>,
    pub max_event_count: Option<usize>,
}

impl AuditLimits {
    pub fn from_config(config: &AuditConfig) -> Self {
        Self {
            max_event_bytes: None,
            max_total_bytes: config.max_bytes,
            max_event_count: config.max_events,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        for (resource, value) in [
            ("audit max_bytes", self.max_total_bytes),
            ("audit max_events", self.max_event_count),
        ] {
            if value == Some(0) {
                return Err(resource);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_fold_kinds_are_classified_durable() {
        for kind in [
            "run_queued",
            "run_started",
            "run_resumed",
            "step_started",
            "step_finished",
            "step_skipped",
            "llm_call",
            "agent_checkpoint",
            "resource",
            "confirm_request",
            "run_waiting",
            "user_answered",
            "user_confirmed",
            "user_cancel_requested",
            "mcp_input_pending",
            "mcp_continuation_consumed",
            "mcp_continuation_resumed",
            "operation_started",
            "operation_finished",
            "run_finished",
            "run_error",
            "run_canceled",
            "run_interrupted",
            "state_patched",
            "repair_attempt_started",
            "regenerate_attempt_started",
            "audit_degraded",
        ] {
            assert_eq!(
                event_class(kind),
                EventClass::Durable,
                "`{kind}` is folded into RunState and must be durable"
            );
        }
    }

    #[test]
    fn observation_kinds_are_classified_observation() {
        for kind in OBSERVATION_EVENT_KINDS {
            assert_eq!(
                event_class(kind),
                EventClass::Observation,
                "`{kind}` is listed as observation"
            );
        }
    }

    #[test]
    fn minimal_level_disables_observation_unless_overridden() {
        let config = AuditConfig {
            level: AuditLevel::Minimal,
            classes: BTreeMap::from([("side_effect".to_string(), AuditMode::Full)]),
            ..Default::default()
        };
        let policy = AuditPolicy::from_config(&config).expect("config should resolve");
        assert_eq!(policy.mode_for("llm_delta"), AuditMode::Off);
        assert_eq!(policy.mode_for("side_effect"), AuditMode::Full);
        assert_eq!(policy.mode_for("tool_call"), AuditMode::Off);
    }

    #[test]
    fn durable_class_override_is_rejected() {
        let config = AuditConfig {
            classes: BTreeMap::from([("run_finished".to_string(), AuditMode::Off)]),
            ..Default::default()
        };
        let error = AuditPolicy::from_config(&config).expect_err("durable class must be rejected");
        assert!(
            error.contains("run_finished"),
            "the error names the kind: {error}"
        );
    }

    #[test]
    fn standard_floor_forces_full_and_discards_lower_overrides() {
        let config = AuditConfig {
            level: AuditLevel::Minimal,
            classes: BTreeMap::from([
                ("llm_delta".to_string(), AuditMode::Off),
                ("tool_call".to_string(), AuditMode::Digest),
            ]),
            ..Default::default()
        };
        let mut policy = AuditPolicy::from_config(&config).expect("config should resolve");
        policy.apply_floor(AuditFloor::Standard);
        assert_eq!(policy.mode_for("llm_delta"), AuditMode::Full);
        assert_eq!(policy.mode_for("tool_call"), AuditMode::Full);
        assert!(policy.is_full());
    }

    #[test]
    fn zero_audit_limit_is_invalid() {
        let limits = AuditLimits {
            max_total_bytes: Some(0),
            ..Default::default()
        };
        assert_eq!(limits.validate(), Err("audit max_bytes"));
    }
}
