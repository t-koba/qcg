use crate::{JournalWriter, StepError};
use qcg_api::ConfirmSpec;
use qcg_contract::NodeDef;
use qcg_contract::RetryOnIndeterminate;
use qcg_contract::SideEffects;
use serde_json::{Value, json};

use super::types::{EngineError, RunContext};

/// What the operation guard decides for one invocation. Invocation identity
/// and content digest are separate dimensions: a new invocation always
/// proceeds (possibly duplicating a remote call the guard cannot see),
/// while the same invocation reuses its id and, when finished, its cached
/// result (B07).
pub enum GuardDecision {
    /// Execute the remote operation under this id.
    Proceed { operation_id: String },
    /// The same invocation already succeeded: return the cached result
    /// without touching the remote again.
    Resend { operation_id: String, result: Value },
}

/// How an external operation finished, classified by what the caller could
/// prove about remote effects (B08). Timeouts, disconnects, and kills are
/// `Indeterminate`, never clean: only evidence of non-application qualifies
/// as clean.
pub enum OperationOutcome {
    Success { result: Option<Value> },
    CleanError,
    Indeterminate { reason: String },
}

/// Pure settlement half of [`RunContext::guard_external_operation`]: no
/// journaling, so the full guard matrix is unit-testable without a writer.
#[derive(Debug, PartialEq)]
enum GuardVerdict {
    /// Journal `operation_started` and proceed.
    Start,
    /// Journal `operation_repeated` then `operation_started`, then proceed:
    /// at-least-once was explicitly opted in for an indeterminate outcome.
    Repeat,
    /// Return the cached result without executing.
    Resend { result: Value },
    /// Fail with this reason.
    Refuse { reason: String },
}

/// Pure decision over one durable operation record. `digest` is the current
/// invocation's content digest; a stored record with a different digest is
/// a changed-content resend and is always refused.
fn decide_operation_guard(
    record: Option<&crate::OperationRecord>,
    digest: &str,
    on_indeterminate: RetryOnIndeterminate,
) -> GuardVerdict {
    let Some(record) = record else {
        return GuardVerdict::Start;
    };
    if record.digest != digest {
        return GuardVerdict::Refuse {
            reason: "admitted for different content; a changed-content resend is refused".into(),
        };
    }
    match record.status {
        // Started without finish: indeterminate unless explicitly repeated.
        crate::OperationStatus::Started => match on_indeterminate {
            RetryOnIndeterminate::Repeat => GuardVerdict::Repeat,
            RetryOnIndeterminate::Fail => GuardVerdict::Refuse {
                reason: "has an indeterminate result after interruption; refusing automatic replay"
                    .into(),
            },
        },
        crate::OperationStatus::Succeeded => match &record.result {
            Some(result) => GuardVerdict::Resend {
                result: result.clone(),
            },
            // Succeeded without a cached result (too large or legacy):
            // re-executing would duplicate unknown-safe work, so route to
            // explicit manual recovery instead.
            None => GuardVerdict::Refuse {
                reason: "already finished without a cached result; manual recovery required".into(),
            },
        },
        // Proven nothing was applied: retry under the same id so the
        // remote still deduplicates if the proof was wrong.
        crate::OperationStatus::FailedClean => GuardVerdict::Start,
        crate::OperationStatus::FailedIndeterminate => match on_indeterminate {
            RetryOnIndeterminate::Repeat => GuardVerdict::Repeat,
            RetryOnIndeterminate::Fail => GuardVerdict::Refuse {
                reason: "has an indeterminate result after interruption; refusing automatic replay"
                    .into(),
            },
        },
    }
}

impl OperationOutcome {
    /// Conservative taxonomy for gateway failures. Denials, validation, and
    /// malformed requests prove nothing was sent (`Clean`); timeouts,
    /// transport I/O, and container failures do not (`Indeterminate`).
    /// `side_effect_free` covers safe methods (GET/HEAD), where even a
    /// transport failure cannot have applied an effect. Cancellation never
    /// reaches here: cancelled operations finish nothing.
    pub fn gateway_error(error: &crate::GatewayError, side_effect_free: bool) -> Self {
        use crate::GatewayError;
        if side_effect_free {
            return OperationOutcome::CleanError;
        }
        match error {
            GatewayError::EmptyCommand
            | GatewayError::CommandDenied { .. }
            | GatewayError::CommandArgsDenied { .. }
            | GatewayError::CommandPathDenied { .. }
            | GatewayError::CommandIsolationMissing { .. }
            | GatewayError::ContainerRuntimeMissing { .. }
            | GatewayError::ContainerImageMissing { .. }
            | GatewayError::CommandInputTooLarge { .. }
            | GatewayError::NetworkDenied { .. }
            | GatewayError::UnsupportedUrl { .. }
            | GatewayError::HttpRequestBodyTooLarge { .. }
            | GatewayError::FsReadDenied
            | GatewayError::FsWriteDenied
            | GatewayError::PathDenied { .. } => OperationOutcome::CleanError,
            GatewayError::Http(error) if error.is_builder() || error.is_request() => {
                OperationOutcome::CleanError
            }
            GatewayError::Canceled => OperationOutcome::Indeterminate {
                reason: "cancelled operation reached error classification".into(),
            },
            _ => OperationOutcome::Indeterminate {
                reason: format!("gateway failure may have applied effects: {error}"),
            },
        }
    }
}

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
    ///
    /// `invocation_id` identifies the calling invocation: agent tool calls
    /// pass their stable call id, single-shot steps pass a content-derived
    /// identity. The same invocation reuses its id (and cached result)
    /// across resends; a new invocation always gets a fresh id even for
    /// identical content (B07).
    pub fn guard_external_operation(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        kind: &str,
        target: &str,
        details: &Option<Value>,
        invocation_id: &str,
    ) -> Result<GuardDecision, StepError> {
        let digest = Self::operation_digest(target, details)?;
        let operation_id = crate::operation_id_for(&self.run_id, &node.id, &digest, invocation_id);
        let attempt = journal
            .state()
            .operation_attempts
            .get(&operation_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let record = journal
            .state()
            .operation_records
            .get(&operation_id)
            .cloned();
        let policy = node
            .retry
            .as_ref()
            .map(|retry| retry.on_indeterminate)
            .unwrap_or(RetryOnIndeterminate::Fail);
        let start_fresh = |journal: &JournalWriter| -> Result<GuardDecision, StepError> {
            journal
                .event(
                    "operation_started",
                    json!({
                        "node": node.id,
                        "kind": kind,
                        "target": target,
                        "operation_id": operation_id.clone(),
                        "operation_digest": digest,
                        "attempt": attempt,
                    }),
                )
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            Ok(GuardDecision::Proceed {
                operation_id: operation_id.clone(),
            })
        };
        match decide_operation_guard(record.as_ref(), &digest, policy) {
            GuardVerdict::Start => start_fresh(journal),
            GuardVerdict::Repeat => {
                // At-least-once is explicitly opted in: re-execute and
                // record the acknowledged double-apply risk.
                journal
                    .event(
                        "operation_repeated",
                        json!({
                            "node": node.id,
                            "operation_id": operation_id,
                            "attempt": attempt,
                            "policy": "repeat",
                        }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                start_fresh(journal)
            }
            GuardVerdict::Resend { result } => Ok(GuardDecision::Resend {
                operation_id: operation_id.clone(),
                result,
            }),
            GuardVerdict::Refuse { reason } => Err(StepError::Refused {
                node: node.id.clone(),
                message: format!("operation `{operation_id}` ({kind} to `{target}`) {reason}"),
            }),
        }
    }

    pub fn finish_external_operation(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        operation_id: &str,
        result: Option<Value>,
    ) -> Result<(), StepError> {
        self.finish_external_operation_with(
            journal,
            node,
            operation_id,
            OperationOutcome::Success { result },
        )
    }

    pub fn finish_external_operation_with(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        operation_id: &str,
        outcome: OperationOutcome,
    ) -> Result<(), StepError> {
        let (status, reason, result) = match outcome {
            OperationOutcome::Success { result } => (
                "success",
                None,
                result.as_ref().and_then(crate::cacheable_operation_result),
            ),
            OperationOutcome::CleanError => ("clean", None, None),
            OperationOutcome::Indeterminate { reason } => ("indeterminate", Some(reason), None),
        };
        let mut payload =
            json!({ "node": node.id, "operation_id": operation_id, "status": status });
        if let Some(reason) = reason {
            payload["reason"] = Value::String(reason);
        }
        if let Some(result) = result {
            payload["result"] = result;
        }
        journal
            .event("operation_finished", payload)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))
    }

    /// Completion record for an error path that already returns its own
    /// error: a failed record must neither replace the original error nor
    /// vanish silently, so it is surfaced as a warning instead.
    pub fn finish_external_operation_with_warn(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        operation_id: &str,
        outcome: OperationOutcome,
    ) {
        if let Err(error) =
            self.finish_external_operation_with(journal, node, operation_id, outcome)
        {
            tracing::warn!(
                node = %node.id,
                operation_id = %operation_id,
                %error,
                "operation completion record failed; the original step error is returned"
            );
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
    use crate::OperationRecord;
    use crate::OperationStatus;

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

    fn record(digest: &str, status: OperationStatus, result: Option<Value>) -> OperationRecord {
        OperationRecord {
            digest: digest.into(),
            status,
            result,
        }
    }

    #[test]
    fn guard_matrix_separates_invocation_content_and_policy() {
        use RetryOnIndeterminate::{Fail, Repeat};
        // Fresh invocations always start, under either policy.
        assert_eq!(
            decide_operation_guard(None, "digest", Fail),
            GuardVerdict::Start
        );
        assert_eq!(
            decide_operation_guard(None, "digest", Repeat),
            GuardVerdict::Start
        );
        // Changed content under the same invocation is always refused.
        for status in [
            OperationStatus::Started,
            OperationStatus::Succeeded,
            OperationStatus::FailedClean,
            OperationStatus::FailedIndeterminate,
        ] {
            let changed = record("other-digest", status, Some(json!({"ok": true})));
            assert!(
                matches!(
                    decide_operation_guard(Some(&changed), "digest", Repeat),
                    GuardVerdict::Refuse { .. }
                ),
                "changed content must be refused under {status:?}"
            );
        }
        // Started without finish: indeterminate unless explicitly repeated.
        let started = record("digest", OperationStatus::Started, None);
        assert!(matches!(
            decide_operation_guard(Some(&started), "digest", Fail),
            GuardVerdict::Refuse { .. }
        ));
        assert_eq!(
            decide_operation_guard(Some(&started), "digest", Repeat),
            GuardVerdict::Repeat
        );
        // Succeeded with a cached result resends it without executing.
        let done = record(
            "digest",
            OperationStatus::Succeeded,
            Some(json!({"ok": true})),
        );
        assert_eq!(
            decide_operation_guard(Some(&done), "digest", Fail),
            GuardVerdict::Resend {
                result: json!({"ok": true})
            }
        );
        // Succeeded without a cached result routes to manual recovery,
        // never to silent re-execution.
        let uncached = record("digest", OperationStatus::Succeeded, None);
        assert!(matches!(
            decide_operation_guard(Some(&uncached), "digest", Repeat),
            GuardVerdict::Refuse { .. }
        ));
        // Clean failures retry under the same id.
        let clean = record("digest", OperationStatus::FailedClean, None);
        assert_eq!(
            decide_operation_guard(Some(&clean), "digest", Fail),
            GuardVerdict::Start
        );
        // Indeterminate failures follow the opt-in policy.
        let unknown = record("digest", OperationStatus::FailedIndeterminate, None);
        assert!(matches!(
            decide_operation_guard(Some(&unknown), "digest", Fail),
            GuardVerdict::Refuse { .. }
        ));
        assert_eq!(
            decide_operation_guard(Some(&unknown), "digest", Repeat),
            GuardVerdict::Repeat
        );
    }

    #[test]
    fn sequential_same_content_calls_converge_and_changed_content_starts_fresh() {
        use crate::TemplateService;
        use crate::engine::checkpoint::CheckpointAccounting;
        use camino::Utf8PathBuf;
        use qcg_contract::{
            AssetSpec, Contract, FailurePolicy, GeneratorMeta, Graph, InputSpec, JournalPolicy,
            Manifest, OnDeps, OutputSpec, Permissions, StepType,
        };
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tokio_util::sync::CancellationToken;

        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf())
            .expect("temporary path must be UTF-8");
        let workspace = root.join("workspace");
        let metadata = root.join("meta");
        let manifest = Manifest {
            generator: GeneratorMeta {
                id: "resend-test".into(),
                name: "Resend Test".into(),
                version: "0.1.0".into(),
                description: String::new(),
                authors: vec![],
                qcg_version: String::new(),
            },
            permissions: Permissions {
                fs_write: vec!["workspace".into()],
                ..Permissions::default()
            },
            llm: None,
            inputs: InputSpec::default(),
            resources: std::collections::BTreeMap::new(),
            tools: std::collections::BTreeMap::new(),
            secrets: std::collections::BTreeMap::new(),
            runtime: Default::default(),
            budget: Default::default(),
            flow: Vec::new(),
            parallel: Vec::new(),
            blocks: std::collections::BTreeMap::new(),
            outputs: OutputSpec { extras: vec![] },
            failure: FailurePolicy::default(),
            journal: JournalPolicy::default(),
            assets: AssetSpec::default(),
            dependencies: Default::default(),
        };
        let permissions = manifest.permissions.clone();
        let contract = Contract {
            root: Utf8PathBuf::from("resend-test"),
            graph: Graph::build(&manifest).expect("empty graph should build"),
            manifest,
            sha256: "test".into(),
        };
        let node = qcg_contract::NodeDef {
            id: "resend-node".into(),
            kind: StepType::from("test.pass"),
            needs: vec![],
            when: None,
            on_deps: OnDeps::default(),
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: None,
            params: Default::default(),
        };
        let ctx = RunContext {
            run_id: "resend-test".into(),
            contract,
            workspace: workspace.clone(),
            metadata: metadata.clone(),
            fs: crate::FsGateway::new(workspace.clone(), &permissions),
            cmd: crate::CmdGateway::new(permissions.clone(), workspace.clone()),
            http: crate::HttpGateway::new(permissions, Duration::from_secs(5), None, None)
                .expect("test HTTP gateway should build"),
            secrets: crate::SecretStore::from_values(std::collections::BTreeMap::new()),
            interactive: false,
            answers: std::collections::BTreeMap::new(),
            confirmations: std::collections::BTreeMap::new(),
            llm_provider: None,
            llm_seed_override: None,
            templates: TemplateService,
            cancellation: CancellationToken::new(),
            snapshot_source: None,
            replayed_steps: Arc::new(std::collections::BTreeMap::new()),
            checkpoint_accounting: Arc::new(Mutex::new(CheckpointAccounting::default())),
        };
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "resend-test",
            false,
            None,
        )
        .expect("test journal should open");
        let details = Some(json!({"argv": ["echo", "hi"]}));

        // First invocation executes.
        let first = ctx
            .guard_external_operation(&journal, &node, "command", "echo", &details, "call-1")
            .expect("first invocation should proceed");
        let GuardDecision::Proceed { operation_id: id1 } = first else {
            panic!("first invocation must proceed");
        };
        ctx.finish_external_operation(&journal, &node, &id1, Some(json!({"ok": true})))
            .expect("first finish should journal");

        // Same content under a new invocation also executes, with a fresh id.
        let second = ctx
            .guard_external_operation(&journal, &node, "command", "echo", &details, "call-2")
            .expect("second invocation should proceed");
        let GuardDecision::Proceed { operation_id: id2 } = second else {
            panic!("second invocation must proceed");
        };
        assert_ne!(id1, id2, "distinct invocations must not share an id");
        ctx.finish_external_operation(&journal, &node, &id2, Some(json!({"ok": true})))
            .expect("second finish should journal");

        // Resending the first invocation returns the cached result.
        match ctx
            .guard_external_operation(&journal, &node, "command", "echo", &details, "call-1")
            .expect("resend should converge")
        {
            GuardDecision::Resend {
                operation_id,
                result,
            } => {
                assert_eq!(operation_id, id1);
                assert_eq!(result, json!({"ok": true}));
            }
            GuardDecision::Proceed { .. } => panic!("resend must not re-execute"),
        }

        // Changed content under the same invocation is a new operation:
        // the id binds the digest, so an approval for the old content can
        // never authorize the new content. It proceeds under a fresh id
        // requiring its own execution and approval.
        let changed = Some(json!({"argv": ["echo", "other"]}));
        match ctx.guard_external_operation(&journal, &node, "command", "echo", &changed, "call-1") {
            Ok(GuardDecision::Proceed { operation_id }) => {
                assert_ne!(operation_id, id1);
                assert_ne!(operation_id, id2);
            }
            Err(error) => panic!("changed content must start a new operation, got {error}"),
            Ok(GuardDecision::Resend { .. }) => panic!("changed content must not resend"),
        }

        // Exactly three executions happened: two for the shared content
        // (one per invocation) plus one for the changed content. The
        // same-invocation resend added no new start.
        let source = std::fs::read_to_string(metadata.join("journal.jsonl"))
            .expect("journal should be readable");
        let starts = source
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("operation_started"))
            .count();
        assert_eq!(starts, 3, "resend must not start an extra execution");
    }
}
