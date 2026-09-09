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
#[derive(Debug)]
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
    ///
    /// For HTTP, only `is_builder` (the request was never built) and
    /// `is_connect` (no connection was established, so no byte reached
    /// any server) prove non-application. `is_request` is NOT such proof:
    /// reqwest wraps every error from executing the request future —
    /// including a disconnect after the server applied the effect — as
    /// `Kind::Request` (C02). At-least-once repetition stays available
    /// through the explicit `Repeat` policy, never through silent
    /// reclassification.
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
            GatewayError::Http(error) if error.is_builder() || error.is_connect() => {
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
    ///
    /// Records written under superseded id schemes (v2 with a truncated
    /// invocation fragment, v1 with no invocation dimension) are
    /// recognized on lookup in that order, so an unfinished pre-upgrade
    /// side effect refuses automatic replay instead of silently
    /// re-executing under the new id (C03). A current-scheme record naming
    /// a different invocation fails closed: the key is a full hash of the
    /// invocation, so a mismatch is corruption, not a new operation.
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
        let operation_id = crate::operation_id_for(&self.run_id, &node.id, invocation_id);
        let records = &journal.state().operation_records;
        let record = records
            .get(&operation_id)
            .or_else(|| {
                records.get(&crate::operation_id_for_v2(
                    &self.run_id,
                    &node.id,
                    &digest,
                    invocation_id,
                ))
            })
            .or_else(|| {
                records.get(&crate::legacy_operation_id_for(
                    &self.run_id,
                    &node.id,
                    &digest,
                ))
            })
            .cloned();
        if let Some(record) = &record
            && records.contains_key(&operation_id)
            && !record.invocation.is_empty()
            && record.invocation != invocation_id
        {
            return Err(StepError::Refused {
                node: node.id.clone(),
                message: format!(
                    "operation `{operation_id}` ({kind} to `{target}`) names a different invocation; refusing as corrupt"
                ),
            });
        }
        let attempt = journal
            .state()
            .operation_attempts
            .get(&operation_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
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
                        "invocation_id": invocation_id,
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
            invocation: String::new(),
        }
    }

    /// Full guard harness: a live run context plus its journal directory.
    /// The caller opens the journal when ready so pre-existing journal
    /// content (legacy upgrades, crash recovery) folds first.
    #[allow(clippy::too_many_lines)]
    fn guard_harness(
        run_id: &str,
    ) -> (
        tempfile::TempDir,
        camino::Utf8PathBuf,
        RunContext,
        qcg_contract::NodeDef,
    ) {
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
            run_id: run_id.into(),
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
        (dir, metadata, ctx, node)
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
    fn sequential_same_content_calls_converge_and_changed_content_is_refused() {
        let (_dir, metadata, ctx, node) = guard_harness("resend-test");
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
        // Changed content under the same invocation is refused, not a new
        // operation: the id binds run, node, and invocation only, so the
        // stored digest comparison is always reached (C04). An approval
        // for the old content can never authorize the new content.
        let changed = Some(json!({"argv": ["echo", "other"]}));
        match ctx.guard_external_operation(&journal, &node, "command", "echo", &changed, "call-1") {
            Err(crate::StepError::Refused { .. }) => {}
            Err(error) => panic!("changed content must be refused, got {error}"),
            Ok(_) => panic!("changed content must be refused, not started"),
        }

        // Exactly two executions happened: two for the shared content
        // (one per invocation). The same-invocation resend and the
        // changed-content resend added no new starts.
        let source = std::fs::read_to_string(metadata.join("journal.jsonl"))
            .expect("journal should be readable");
        let starts = source
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("operation_started"))
            .count();
        assert_eq!(starts, 2, "refused resends must not start executions");
    }

    #[test]
    fn colliding_invocation_fragments_get_distinct_ids() {
        // C04: call-28383 and call-78343 share the 8-hex invocation
        // fragment c7cdf209. Full-hash ids must keep them apart so neither
        // call reuses or refuses on the other's record.
        let first = crate::operation_id_for("run", "node", "call-28383");
        let second = crate::operation_id_for("run", "node", "call-78343");
        assert_ne!(
            first, second,
            "distinct invocations must never share an operation id"
        );
    }

    #[test]
    fn legacy_started_operations_refuse_replay_after_upgrade() {
        // C03: a v1 (pre-invocation) operation_started without a finish,
        // written by an older binary, must refuse automatic replay through
        // the real guard instead of silently re-executing under the new id.
        let (_dir, metadata, ctx, node) = guard_harness("legacy-run");
        let target = "echo";
        let details = Some(json!({"argv": ["echo", "hi"]}));
        let digest = RunContext::operation_digest(target, &details).expect("digest should compute");
        let legacy_id = crate::legacy_operation_id_for("legacy-run", "resend-node", &digest);
        let journal_path = metadata.join("journal.jsonl");
        std::fs::create_dir_all(&metadata).expect("meta dir should exist");
        let mut journal_text = String::new();
        let event = json!({
            "t": "operation_started",
            "ts": "2026-09-09T00:00:01Z",
            "seq": 1,
            "run_id": "legacy-run",
            "trace_id": "trace",
            "span_id": "span2",
            "parent_span_id": "span1",
            "node": "resend-node",
            "kind": "command",
            "target": target,
            "operation_id": legacy_id,
            "operation_digest": digest,
            "attempt": 1,
        });
        journal_text.push_str(&serde_json::to_string(&event).expect("line should serialize"));
        journal_text.push('\n');
        std::fs::write(&journal_path, journal_text).expect("legacy journal should be written");
        let journal = crate::JournalWriter::create(&journal_path, "legacy-run", false, None)
            .expect("legacy journal should open");
        // The new id finds nothing, yet the guard must still refuse: the
        // legacy record is recognized on lookup (C03).
        let error = ctx
            .guard_external_operation(&journal, &node, "command", target, &details, "call-9")
            .expect_err("legacy unfinished work must refuse automatic replay");
        assert!(
            matches!(error, crate::StepError::Refused { .. }),
            "legacy started operation must be refused, got: {error}"
        );
        // …and no new execution was journaled.
        let source = std::fs::read_to_string(&journal_path).expect("journal should be readable");
        assert_eq!(
            source
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|event| event.get("t").and_then(Value::as_str) == Some("operation_started"))
                .count(),
            1,
            "refused replay must not journal a new start"
        );
    }

    #[test]
    fn legacy_succeeded_operations_resend_their_cached_result() {
        // C03: a v1 success with a cached result is attributable on resume
        // (same run, node, and content): the guard returns it instead of
        // re-executing. Without a cached result it refuses for manual
        // recovery, exactly like a current-scheme uncached success.
        let (_dir, metadata, ctx, node) = guard_harness("legacy-run");
        let target = "echo";
        let details = Some(json!({"argv": ["echo", "hi"]}));
        let digest = RunContext::operation_digest(target, &details).expect("digest should compute");
        let legacy_id = crate::legacy_operation_id_for("legacy-run", "resend-node", &digest);
        let journal_path = metadata.join("journal.jsonl");
        std::fs::create_dir_all(&metadata).expect("meta dir should exist");
        let mut journal_text = String::new();
        for event in [
            json!({
                "t": "operation_started",
                "ts": "2026-09-09T00:00:00Z",
                "seq": 1,
                "run_id": "legacy-run",
                "trace_id": "trace",
                "span_id": "span1",
                "node": "resend-node",
                "kind": "command",
                "target": target,
                "operation_id": legacy_id,
                "operation_digest": digest,
                "attempt": 1,
            }),
            json!({
                "t": "operation_finished",
                "ts": "2026-09-09T00:00:01Z",
                "seq": 2,
                "run_id": "legacy-run",
                "trace_id": "trace",
                "span_id": "span2",
                "parent_span_id": "span1",
                "node": "resend-node",
                "operation_id": legacy_id,
                "status": "success",
                "result": {"ok": true},
            }),
        ] {
            journal_text.push_str(&serde_json::to_string(&event).expect("line should serialize"));
            journal_text.push('\n');
        }
        std::fs::write(&journal_path, journal_text).expect("legacy journal should be written");
        let journal = crate::JournalWriter::create(&journal_path, "legacy-run", false, None)
            .expect("legacy journal should open");
        match ctx
            .guard_external_operation(&journal, &node, "command", target, &details, "call-9")
            .expect("legacy success should converge")
        {
            GuardDecision::Resend { result, .. } => {
                assert_eq!(result, json!({"ok": true}));
            }
            GuardDecision::Proceed { .. } => panic!("legacy success must not re-execute"),
        }
    }

    /// C02: only pre-send reqwest failures classify as clean. A server
    /// that accepts a POST and disconnects without responding produces a
    /// `Kind::Request` error (the old code read that as clean and
    /// retried); it must be indeterminate so the default policy refuses
    /// the replay. A refused connection (nothing could be sent) stays
    /// clean. All errors below come from real sockets, never fabricated.
    #[tokio::test]
    async fn http_error_taxonomy_separates_unsent_from_unknown() {
        use crate::{GatewayError, HttpGateway, HttpRequest};
        use std::collections::BTreeMap;
        use std::io::Read as _;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let permissions = qcg_contract::Permissions {
            network: vec!["*".into()],
            ..Default::default()
        };
        let gateway = HttpGateway::new(
            permissions,
            std::time::Duration::from_secs(10),
            None,
            Some(0),
        )
        .expect("test gateway should build");
        let post = |url: String| HttpRequest {
            method: "POST".into(),
            url,
            headers: BTreeMap::new(),
            sensitive_query: BTreeMap::new(),
            body: Some(b"apply".to_vec()),
            follow_redirects: false,
            idempotency_key: None,
        };

        // Counting server: records the request, then disconnects without
        // responding, exactly like a crash after applying the effect.
        let applied = Arc::new(AtomicUsize::new(0));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
        let port = listener
            .local_addr()
            .expect("loopback should have a port")
            .port();
        let server_applied = Arc::clone(&applied);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("client should connect");
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") && head.len() < 65536 {
                match stream.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.extend_from_slice(&byte),
                }
            }
            if head.starts_with(b"POST") {
                server_applied.fetch_add(1, Ordering::SeqCst);
            }
            // Drop without responding: the client observes a disconnect.
        });
        let error = gateway
            .request(post(format!("http://127.0.0.1:{port}/apply")))
            .await
            .expect_err("disconnect without response must fail");
        server.join().expect("server thread should finish");
        assert_eq!(
            applied.load(Ordering::SeqCst),
            1,
            "server must have seen the request"
        );
        let GatewayError::Http(reqwest_error) = &error else {
            panic!("disconnect must surface as an HTTP error, got: {error}");
        };
        // The old taxonomy read exactly this shape as clean.
        assert!(
            reqwest_error.is_request(),
            "post-send disconnect must be a Request-kind error"
        );
        assert!(
            !reqwest_error.is_connect(),
            "an established connection must not read as a connect failure"
        );
        assert!(
            matches!(
                OperationOutcome::gateway_error(&error, false),
                OperationOutcome::Indeterminate { .. }
            ),
            "unknown remote effects must be indeterminate, got: {error}"
        );
        // …while a refused connection proves nothing was sent.
        let refused_port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
            let port = probe.local_addr().expect("port should be known").port();
            drop(probe);
            port
        };
        let error = gateway
            .request(post(format!("http://127.0.0.1:{refused_port}/apply")))
            .await
            .expect_err("refused connection must fail");
        let GatewayError::Http(reqwest_error) = &error else {
            panic!("refused connection must surface as an HTTP error, got: {error}");
        };
        assert!(
            reqwest_error.is_connect(),
            "refused connection must read as a connect failure"
        );
        assert!(
            matches!(
                OperationOutcome::gateway_error(&error, false),
                OperationOutcome::CleanError
            ),
            "provably unsent requests stay clean, got: {error}"
        );
        // A request that was never built is likewise clean.
        let builder_error =
            reqwest::Proxy::all("not a url %%").expect_err("bad proxy must fail to build");
        assert!(
            builder_error.is_builder(),
            "proxy misconfiguration must be a builder error"
        );
        assert!(
            matches!(
                OperationOutcome::gateway_error(&GatewayError::Http(builder_error), false),
                OperationOutcome::CleanError
            ),
            "unbuilt requests stay clean"
        );
        // Consequence for the guard: the indeterminate disconnect refuses
        // replay under the default policy, while a clean failure retries.
        let digest = "digest";
        let unknown = record(digest, OperationStatus::FailedIndeterminate, None);
        assert!(
            matches!(
                decide_operation_guard(Some(&unknown), digest, RetryOnIndeterminate::Fail),
                GuardVerdict::Refuse { .. }
            ),
            "indeterminate disconnect must refuse automatic replay"
        );
        let clean = record(digest, OperationStatus::FailedClean, None);
        assert_eq!(
            decide_operation_guard(Some(&clean), digest, RetryOnIndeterminate::Fail),
            GuardVerdict::Start
        );
    }
}
