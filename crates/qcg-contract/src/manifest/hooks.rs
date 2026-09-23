//! Run lifecycle hooks (ADR 0001: contract-declared lifecycle nodes).
//!
//! A hook binds an existing step type to a run lifecycle event. Hooks are
//! ordinary bounded steps: they charge the run budget, pass the same
//! permission gates, journal `step_started`/`step_finished`, and replay
//! exactly once across resumes. Policy (which hooks exist, what happens on
//! failure) lives in the contract; the execution mechanism lives in the
//! engine.
//!
//! Hooks may not suspend (`ask_user`, `await`, or any runtime elicitation):
//! a run must never wait on a human from inside a lifecycle hook, because
//! the hook runs outside the scheduler's suspension handling. The contract
//! rejects the known suspension step types up front and the engine fails a
//! hook that suspends at runtime.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::nodes::{NodeDef, StepType, validate_node_id};
use super::resources::RetryPolicy;
use super::validate::Manifest;
use crate::ContractError;

/// Lifecycle hook bindings. Each entry is an inline step definition.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HooksConfig {
    #[serde(default)]
    pub run_started: Vec<HookDef>,
    #[serde(default)]
    pub run_succeeded: Vec<HookDef>,
    #[serde(default)]
    pub run_failed: Vec<HookDef>,
    /// Runs once during failed settlement, after inputs are still available
    /// and before `run_failed` hooks. Failures are published as the
    /// `step_failed` variable so a hook can render them.
    #[serde(default)]
    pub step_failed: Vec<HookDef>,
}

impl HooksConfig {
    /// Every hook with its event name, in declaration order.
    pub fn entries(&self) -> impl Iterator<Item = (&'static str, &HookDef)> {
        self.run_started
            .iter()
            .map(|hook| ("run_started", hook))
            .chain(
                self.run_succeeded
                    .iter()
                    .map(|hook| ("run_succeeded", hook)),
            )
            .chain(self.run_failed.iter().map(|hook| ("run_failed", hook)))
            .chain(self.step_failed.iter().map(|hook| ("step_failed", hook)))
    }

    pub fn for_event(&self, event: &str) -> &[HookDef] {
        match event {
            "run_started" => &self.run_started,
            "run_succeeded" => &self.run_succeeded,
            "run_failed" => &self.run_failed,
            "step_failed" => &self.step_failed,
            _ => &[],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.run_started.is_empty()
            && self.run_succeeded.is_empty()
            && self.run_failed.is_empty()
            && self.step_failed.is_empty()
    }
}

/// What a hook failure does to the run. Required: a hook never fails
/// silently, and the designer always states the intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HookErrorPolicy {
    /// The run fails with the hook error.
    Fail,
    /// The run continues; the failure is recorded as a durable `hook_failed`
    /// record.
    Warn,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HookDef {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: StepType,
    /// `fail` or `warn`; no default, so the policy is always explicit.
    pub on_error: HookErrorPolicy,
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
    #[serde(default)]
    #[schemars(skip)]
    pub params: toml::Table,
}

impl HookDef {
    /// Synthetic graph node id for this hook. The `hook.` prefix keeps hook
    /// records distinct from flow nodes in `RunState.nodes`, which is also
    /// what makes exactly-once replay work across resumes.
    pub fn node_id(&self, event: &str) -> String {
        format!("hook.{event}.{}", self.id)
    }

    /// Converts the hook into the node shape the engine executor consumes.
    pub fn to_node(&self, event: &str) -> NodeDef {
        NodeDef {
            id: self.node_id(event),
            kind: self.kind.clone(),
            needs: Vec::new(),
            when: None,
            on_deps: super::nodes::OnDeps::default(),
            context: Vec::new(),
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: self.retry.clone(),
            params: self.params.clone(),
        }
    }
}

/// Step types that cannot run as hooks: they suspend the run.
pub const SUSPENSION_STEP_TYPES: &[&str] = &["ask_user", "await", "foreach"];

pub struct HooksRule;

impl HooksRule {
    pub fn validate(manifest: &Manifest) -> Result<(), ContractError> {
        let mut ids: BTreeSet<&str> = manifest.flow.iter().map(|node| node.id.as_str()).collect();
        for (event, hook) in manifest.hooks.entries() {
            validate_node_id(&hook.id).map_err(ContractError::Invalid)?;
            if hook.id.trim().is_empty() {
                return Err(ContractError::Invalid("hook id is required".into()));
            }
            if !ids.insert(hook.id.as_str()) {
                return Err(ContractError::Invalid(format!(
                    "duplicate node or hook id `{}`",
                    hook.id
                )));
            }
            if SUSPENSION_STEP_TYPES.contains(&hook.kind.as_str()) {
                return Err(ContractError::Invalid(format!(
                    "hook `{}` uses step type `{}`, which can suspend the run; hooks must not suspend",
                    hook.id, hook.kind
                )));
            }
            if let Some(retry) = &hook.retry {
                if retry.max_attempts == 0 || retry.max_attempts > qcg_policy::MAX_RETRY_ATTEMPTS {
                    return Err(ContractError::Invalid(format!(
                        "hook `{}` retry.max_attempts must be between 1 and {}",
                        hook.id,
                        qcg_policy::MAX_RETRY_ATTEMPTS
                    )));
                }
                if retry.backoff_ms > qcg_policy::MAX_RETRY_BACKOFF_MS {
                    return Err(ContractError::Invalid(format!(
                        "hook `{}` retry.backoff_ms must not exceed {}",
                        hook.id,
                        qcg_policy::MAX_RETRY_BACKOFF_MS
                    )));
                }
                if retry.timeout_secs.is_some_and(|timeout| timeout == 0) {
                    return Err(ContractError::Invalid(format!(
                        "hook `{}` retry.timeout_secs must be at least 1 when set",
                        hook.id
                    )));
                }
            }
            let _ = event;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_with_hooks_toml(hooks: &str) -> Manifest {
        let text = format!(
            r#"
[generator]
id = "hooks-test"
name = "Hooks Test"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "flow_node"
type = "write"

[flow.params]
content = "x"
output_file = "x.txt"
{hooks}
"#
        );
        toml::from_str(&text).expect("manifest should parse")
    }

    #[test]
    fn hooks_parse_and_validate() {
        let manifest = manifest_with_hooks_toml(
            r#"
[[hooks.run_started]]
id = "seed"
type = "write"
on_error = "fail"

[hooks.run_started.params]
content = "seed"
output_file = "seed.txt"

[[hooks.run_failed]]
id = "notify"
type = "render"
on_error = "warn"
"#,
        );
        HooksRule::validate(&manifest).expect("valid hooks should pass");
        assert_eq!(manifest.hooks.run_started[0].id, "seed");
        assert_eq!(
            manifest.hooks.run_failed[0].node_id("run_failed"),
            "hook.run_failed.notify"
        );
    }

    #[test]
    fn hook_error_policy_is_required() {
        let text = r#"
[generator]
id = "hooks-test"
name = "Hooks Test"
version = "0.1.0"
qcg_version = "^0.1"

[[hooks.run_started]]
id = "seed"
type = "write"
"#;
        let error =
            toml::from_str::<Manifest>(text).expect_err("on_error must be required by the schema");
        assert!(
            format!("{error}").contains("on_error"),
            "the error names the missing field: {error}"
        );
    }

    #[test]
    fn hook_cannot_shadow_a_flow_node_or_another_hook() {
        let manifest = manifest_with_hooks_toml(
            r#"
[[hooks.run_started]]
id = "flow_node"
type = "write"
on_error = "fail"
"#,
        );
        let error = HooksRule::validate(&manifest).expect_err("duplicate id must fail");
        assert!(format!("{error}").contains("duplicate"), "{error}");
    }

    #[test]
    fn hook_cannot_suspend() {
        let manifest = manifest_with_hooks_toml(
            r#"
[[hooks.run_started]]
id = "ask"
type = "ask_user"
on_error = "fail"
"#,
        );
        let error = HooksRule::validate(&manifest).expect_err("ask_user hook must be rejected");
        assert!(format!("{error}").contains("suspend"), "{error}");
    }

    #[test]
    fn hook_retry_bounds_are_enforced() {
        let manifest = manifest_with_hooks_toml(
            r#"
[[hooks.run_started]]
id = "seed"
type = "write"
on_error = "fail"
[hooks.run_started.retry]
max_attempts = 17
backoff_ms = 0
on_indeterminate = "fail"
"#,
        );
        let error = HooksRule::validate(&manifest).expect_err("retry bounds must apply");
        assert!(format!("{error}").contains("max_attempts"), "{error}");
    }
}
