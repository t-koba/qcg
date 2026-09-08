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

    /// Canonical digest binding the approval to the exact operation content.
    /// The confirmation id embeds this digest so an approval for target A
    /// can never authorize a regenerated target B (A06). Serialization
    /// failures fail closed instead of digesting empty bytes, which would
    /// alias distinct operations to one id.
    pub fn operation_digest(target: &str, details: &Option<Value>) -> Result<String, StepError> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(target.as_bytes());
        hasher.update([0]);
        // Canonical JSON with sorted keys: serde_json::Map is a BTreeMap so
        // to_vec is deterministic for the same logical content.
        if let Some(details) = details {
            let bytes = serde_json::to_vec(details).map_err(|error| {
                StepError::failed(
                    "digest",
                    format!("operation details are not serializable: {error}"),
                )
            })?;
            hasher.update(&bytes);
        }
        Ok(hex::encode(hasher.finalize()))
    }

    /// Guards an external side effect against duplicate execution across
    /// retries and restarts. Returns the stable operation id to use as the
    /// remote idempotency key. A `started`-without-`finished` operation
    /// refuses automatic replay: the remote may have executed while the
    /// result was lost, so a human must decide (indeterminate result).
    /// The journaled attempt is derived from durable state (prior guards of
    /// the same id plus one), never caller-supplied, so a stale retry can
    /// neither reset nor forge the generation.
    pub fn guard_external_operation(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        kind: &str,
        target: &str,
        details: &Option<Value>,
    ) -> Result<String, StepError> {
        let digest = Self::operation_digest(target, details)?;
        let operation_id = crate::operation_id_for(&self.run_id, &node.id, &digest);
        let attempt = journal
            .state()
            .operation_attempts
            .get(&operation_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        match journal
            .state()
            .operations
            .get(&operation_id)
            .map(String::as_str)
        {
            Some("finished:success") => {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "operation `{operation_id}` already finished; replay must reuse the recorded result"
                    ),
                ));
            }
            Some(status) if status.starts_with("finished:") => {
                // Previous attempt finished with an error: allow retry with
                // the same id so the remote deduplicates via the idempotency
                // key.
            }
            Some("started") => {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "operation `{operation_id}` ({kind} to `{target}`) has an indeterminate result after interruption; refusing automatic replay"
                    ),
                ));
            }
            _ => {}
        }
        journal
            .event(
                "operation_started",
                json!({
                    "node": node.id,
                    "kind": kind,
                    "target": target,
                    "operation_id": operation_id,
                    "operation_digest": digest,
                    "attempt": attempt,
                }),
            )
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        Ok(operation_id)
    }

    pub fn finish_external_operation(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        operation_id: &str,
    ) -> Result<(), StepError> {
        self.finish_external_operation_with_status(journal, node, operation_id, "success")
    }

    pub fn finish_external_operation_with_status(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        operation_id: &str,
        status: &str,
    ) -> Result<(), StepError> {
        journal
            .event(
                "operation_finished",
                json!({ "node": node.id, "operation_id": operation_id, "status": status }),
            )
            .map_err(|error| StepError::failed(&node.id, error.to_string()))
    }

    pub fn require_side_effect(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        kind: &str,
        target: &str,
        details: Option<Value>,
    ) -> Result<Option<ConfirmSpec>, StepError> {
        let digest = Self::operation_digest(target, &details)?;
        let id = format!("{}:{kind}:{}", node.id, &digest[..16]);
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
                            json!({ "node": node.id, "kind": kind, "target": target, "decision": "approved_by_user", "policy": format!("{policy:?}"), "details": details, "operation_digest": digest }),
                        )
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                    return Ok(None);
                }
                let dry_run = matches!(policy, SideEffects::DryRunFirst);
                if dry_run {
                    journal
                        .event(
                            "dry_run",
                            json!({ "node": node.id, "kind": kind, "target": target, "details": details.clone(), "operation_digest": digest }),
                        )
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                }
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "confirmation_required", "policy": format!("{policy:?}"), "dry_run": dry_run, "details": details.clone(), "operation_digest": digest }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Ok(Some(ConfirmSpec {
                    id,
                    title: format!("Confirm side effect `{kind}` for node `{}`", node.id),
                    kind: kind.to_string(),
                    target: target.to_string(),
                    dry_run,
                    details,
                    operation_digest: digest,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_digest_binds_target_and_details() {
        let a = RunContext::operation_digest("echo a", &Some(json!({"argv": ["echo", "a"]})))
            .expect("digest should compute");
        let b = RunContext::operation_digest("echo b", &Some(json!({"argv": ["echo", "b"]})))
            .expect("digest should compute");
        assert_ne!(a, b);
        let a_again = RunContext::operation_digest("echo a", &Some(json!({"argv": ["echo", "a"]})))
            .expect("digest should compute");
        assert_eq!(a, a_again);
    }
}
