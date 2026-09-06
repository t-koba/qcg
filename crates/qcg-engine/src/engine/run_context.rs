use crate::{JournalWriter, StepError};
use qcg_api::ConfirmSpec;
use qcg_contract::NodeDef;
use qcg_contract::SideEffects;
use serde_json::{Value, json};

use super::types::{EngineError, RunContext};

impl RunContext {
    pub(crate) fn checkpoint(&self) -> Result<(), EngineError> {
        if self.cancellation.is_cancelled() {
            Err(EngineError::Canceled)
        } else {
            Ok(())
        }
    }

    pub fn require_side_effect(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        kind: &str,
        target: &str,
        details: Option<Value>,
    ) -> Result<Option<ConfirmSpec>, StepError> {
        let id = format!("{}:{kind}", node.id);
        let policy = &self.contract.manifest.permissions.side_effects;
        match policy {
            SideEffects::None => {
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "denied", "policy": "none", "details": details }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Err(StepError::failed(
                    &node.id,
                    format!(
                        "side effect `{kind}` to `{target}` is not allowed by permissions.side_effects=none"
                    ),
                ))
            }
            SideEffects::Allowed => {
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "allowed", "policy": "allowed", "details": details }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Ok(None)
            }
            SideEffects::Confirm | SideEffects::DryRunFirst => {
                if self.confirmations.get(&id).copied().unwrap_or(false) {
                    journal
                        .event(
                            "side_effect",
                            json!({ "node": node.id, "kind": kind, "target": target, "decision": "approved_by_user", "policy": format!("{policy:?}"), "details": details }),
                        )
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                    return Ok(None);
                }
                let dry_run = matches!(policy, SideEffects::DryRunFirst);
                if dry_run {
                    journal
                        .event(
                            "dry_run",
                            json!({ "node": node.id, "kind": kind, "target": target, "details": details.clone() }),
                        )
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                }
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "confirmation_required", "policy": format!("{policy:?}"), "dry_run": dry_run, "details": details.clone() }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Ok(Some(ConfirmSpec {
                    id,
                    title: format!("Confirm side effect `{kind}` for node `{}`", node.id),
                    kind: kind.to_string(),
                    target: target.to_string(),
                    dry_run,
                    details,
                }))
            }
        }
    }
}
