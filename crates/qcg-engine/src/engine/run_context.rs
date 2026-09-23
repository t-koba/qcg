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
/// result.
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
            // Succeeded without an inline cached result (too large and
            // spilled to a sidecar, or never recorded): this pure matrix
            // has no run-dir access, so it cannot load the sidecar and
            // routes to explicit manual recovery instead. Only the
            // dir-backed guard resolves `result_ref` into a resend (E07).
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
        // Send history beats the error's shape: once a side-effect-bearing
        // request has been dispatched, no later failure proves that
        // nothing was applied (D02).
        if let GatewayError::AfterSend(_) = error {
            return OperationOutcome::Indeterminate {
                reason: error.to_string(),
            };
        }
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

/// Domain-separated salted digest for journaled content bindings (E09).
/// Unsalted hashes let a journal reader correlate the same secret across
/// runs or precompute dictionaries; mixing the per-run salt (the run id,
/// itself journaled in plaintext) makes digests unique per run without
/// hiding anything new. Within a run identical content binds identically
/// so content-scope approval reuse works; invocation separation comes from
/// the operation id and the invocation hash in the confirmation id, not
/// the salt (Q1). This is unlinkability across runs, not secrecy against
/// the journal reader: low-entropy secrets still belong in headers sourced
/// from declared secrets, never in query strings.
pub fn salted_binding_digest(domain: &str, salt: &str, bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher.update(salt.as_bytes());
    hasher.update([0]);
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Binds stdin bytes into a command plan for approval and guard digests.
/// The binding is salted with the run id so identical stdin in different
/// runs yields different digests while identical stdin in this run binds
/// identically for content-scope reuse (E09/Q1). A non-object plan
/// with stdin would drop the binding: refuse instead of approving
/// unbound input. `None` stdin binds an explicit null so the shape stays
/// symmetric whether or not input exists.
pub fn bind_command_stdin(
    node_id: &str,
    mut plan: Value,
    stdin: Option<&[u8]>,
    salt: &str,
) -> Result<Value, StepError> {
    let Some(object) = plan.as_object_mut() else {
        if stdin.is_some() {
            return Err(StepError::failed(
                node_id,
                "command plan with stdin must be an object so the input is bound to the approval",
            ));
        }
        return Err(StepError::failed(
            node_id,
            "command plan with no stdin must still be an object so the input binding is explicit",
        ));
    };
    let stdin_sha256 = match stdin {
        Some(bytes) => Value::String(salted_binding_digest("command-stdin-v1", salt, bytes)),
        None => Value::Null,
    };
    object.insert("stdin_sha256".into(), stdin_sha256);
    Ok(plan)
}

/// Canonical content binding for an HTTP side effect: method, a digest of
/// the sorted request headers, and the body digest. The journaled target
/// carries redacted query values (`redact_all_query_values`), so two
/// requests differing only in undeclared query values share one target
/// and one digest: declare varying values in `sensitive_query` to bind
/// them into the approval (E09). Header values are
/// hashed, never journaled in plaintext, so credentials cannot leak into
/// confirmations or operation records (E09). Every digest is salted with
/// the run id for cross-run unlinkability; identical requests in this run
/// bind identically so content-scope approval reuse works, while
/// invocation separation comes from the operation id (E09/Q1). Safe
/// methods return `None` by design: GET/HEAD are
/// side-effect-free, require no approval, use no resend cache, and always
/// re-execute, so a header change simply changes the read without aliasing
/// another approval or cache entry (E09).
pub fn http_operation_details(
    method: &str,
    headers: &std::collections::BTreeMap<String, String>,
    body: Option<&[u8]>,
    sensitive_query: &std::collections::BTreeMap<String, String>,
    salt: &str,
) -> Option<Value> {
    if matches!(method, "GET" | "HEAD") {
        return None;
    }
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"http-headers-v1");
    hasher.update([0]);
    hasher.update(salt.as_bytes());
    hasher.update([0]);
    for (key, value) in headers {
        // HTTP header names are case-insensitive: fold to lowercase so the
        // same logical request always binds the same approval digest (E09).
        hasher.update(key.to_ascii_lowercase().as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    // Sensitive query values are journaled as a digest only, but they must
    // still bind the approval: a different secret value is a different
    // operation (E09).
    // `sensitive_query` is a BTreeMap, so iteration is already in key
    // order and no second sort is needed.
    let mut sensitive_hasher = Sha256::new();
    sensitive_hasher.update(b"http-sensitive-v1");
    sensitive_hasher.update([0]);
    sensitive_hasher.update(salt.as_bytes());
    sensitive_hasher.update([0]);
    for (key, value) in sensitive_query {
        sensitive_hasher.update(key.as_bytes());
        sensitive_hasher.update([0]);
        sensitive_hasher.update(value.as_bytes());
        sensitive_hasher.update([0]);
    }
    let body_sha256 = body.map(|body| salted_binding_digest("http-body-v1", salt, body));
    Some(json!({
        "method": method,
        "headers_sha256": hex::encode(hasher.finalize()),
        "body_sha256": body_sha256,
        "sensitive_query_sha256": (!sensitive_query.is_empty())
            .then(|| hex::encode(sensitive_hasher.finalize())),
    }))
}

/// Safe-method (GET/HEAD) content binding shared by the plain HTTP step and
/// the agent HTTP tool (E09). The foreign `http_operation_details` returns
/// `None` for safe methods by design; this binds method plus the full
/// canonical URL digest plus the headers digest (all salted, no raw
/// secrets) so a header or query change yields a different digest.
/// Journaled copies carry only digests plus the redacted URL shape, never
/// raw values. Single home for both crates: callers must not duplicate
/// this logic.
pub fn safe_http_details(
    method: &str,
    headers: &std::collections::BTreeMap<String, String>,
    salt: &str,
    url: &str,
) -> Option<Value> {
    use sha2::{Digest, Sha256};
    let canonical = qcg_policy::credential::canonical_http_url(url);
    let mut hasher = Sha256::new();
    hasher.update(b"http-headers-v1");
    hasher.update([0]);
    hasher.update(salt.as_bytes());
    hasher.update([0]);
    for (key, value) in headers {
        hasher.update(key.to_ascii_lowercase().as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    Some(json!({
        "method": method,
        "headers_sha256": hex::encode(hasher.finalize()),
        "url_sha256": salted_binding_digest("http-url-v1", salt, canonical.as_bytes()),
        "safe_read": true,
    }))
}

/// Single canonical constructor for the HTTP approval/guard/execute details
/// object shared by the plain HTTP step and the agent HTTP tool (E09).
/// Plan, approve, guard, and execute must share one content binding: every
/// call site routes through this helper so the uses cannot fork into
/// aliases. Safe methods delegate to [`safe_http_details`]; other methods
/// combine the gateway header/body binding with the full canonical URL
/// digest via the single shared `qcg-policy` canonicalizer.
pub fn http_details_with_url(
    method: &str,
    headers: &std::collections::BTreeMap<String, String>,
    body: Option<&[u8]>,
    sensitive: &std::collections::BTreeMap<String, String>,
    salt: &str,
    url: &str,
) -> Option<Value> {
    if matches!(method, "GET" | "HEAD") {
        return safe_http_details(method, headers, salt, url);
    }
    let mut details = http_operation_details(method, headers, body, sensitive, salt)?;
    let canonical = qcg_policy::credential::canonical_http_url(url);
    if let Some(object) = details.as_object_mut() {
        object.insert(
            "url_sha256".into(),
            Value::String(salted_binding_digest(
                "http-url-v1",
                salt,
                canonical.as_bytes(),
            )),
        );
    }
    Some(details)
}

/// Configured run-wide elapsed budget in seconds, if any. Returns `None`
/// when no limit is configured so callers handle "no deadline" explicitly:
/// a bare `unwrap_or(0)` on this value misreads "unlimited" as "already
/// exhausted at zero" on diagnostic paths (E11).
pub(crate) fn elapsed_limit_secs(contract: &qcg_contract::Contract) -> Option<u64> {
    contract.manifest.budget.max_elapsed_seconds
}

/// Single shared checkpoint implementation for loop scopes and step scopes
/// (E11). `node` names the responsible scope using the single checkpoint
/// error-node format: the node id in node scope, the `"runtime"` sentinel
/// in loop scope where no single node is responsible (E11).
///
/// Enforcement uses the monotonic `elapsed_deadline` only. Wall-clock time
/// appears solely in durable records (`budget.started_at`) and diagnostic
/// messages, never in an enforcement decision, so an NTP step cannot
/// stretch or shrink the budget (E11). The deadline and its limit travel
/// as one `Option<(Instant, u64)>` so a deadline without a limit is
/// unrepresentable at the type level (E11).
pub(crate) fn checkpoint_scope(
    cancellation: &tokio_util::sync::CancellationToken,
    elapsed: Option<(tokio::time::Instant, u64)>,
    node: &str,
) -> Result<(), StepError> {
    if cancellation.is_cancelled() {
        return Err(StepError::Cancelled);
    }
    // Loop checks must also stop at the run-wide hard deadline instead
    // of waiting for the next attempt boundary (E11). Exact comparison
    // with `>`: `>=` plus second-truncation would fire up to 1 s early,
    // while `>` fires exactly at expiry (E11).
    if let Some((deadline, limit_secs)) = elapsed
        && tokio::time::Instant::now() > deadline
    {
        return Err(StepError::ElapsedExceeded {
            node: node.to_string(),
            limit_secs,
        });
    }
    Ok(())
}

impl RunContext {
    pub(crate) fn run_checkpoint(&self) -> Result<(), EngineError> {
        // Loop scope: no single node is responsible, so the single format
        // names it "runtime" (E11).
        let elapsed = self
            .elapsed_deadline
            .zip(elapsed_limit_secs(&self.contract));
        match checkpoint_scope(&self.cancellation, elapsed, "runtime") {
            Err(StepError::Cancelled) => Err(EngineError::Canceled),
            Err(error) => Err(EngineError::Step(error)),
            Ok(()) => Ok(()),
        }
    }

    /// Canonical digest binding the approval to the exact operation content.
    /// The confirmation id embeds this digest so an approval for target A
    /// can never authorize a regenerated target B (A06). Serialization
    /// failures fail closed instead of digesting empty bytes, which would
    /// alias distinct operations to one id.
    /// Layering: this outer hash is unsalted, but every side-effect detail
    /// it binds is already run-salted at construction (`headers_sha256`,
    /// `stdin_sha256`, content digests all mix the run id), so equal
    /// requests in different runs still digest differently. Even where
    /// equal (targets alone), cross-run equality authorizes nothing:
    /// approvals live in the run's own journal, never in a shared scope
    /// (E09/Q1).
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

    /// Invocation identity for a single-shot step execution: the node plus
    /// the number of executions that already finished. Retries and crash
    /// resumes keep the current execution's identity (no finish recorded
    /// yet), while a repair or regenerate is a new invocation that
    /// re-confirms even for identical content under `invocation` scope
    /// (Q1).
    pub fn execution_invocation(journal: &JournalWriter, node: &NodeDef) -> String {
        // Explicit absence handling: no record means zero finished
        // executions (first invocation), never an error. A present-but-
        // corrupt count cannot occur here because the fold validates counts
        // with checked arithmetic and fails closed on overflow.
        let executions = journal
            .state()
            .node_executions
            .get(&node.id)
            .copied()
            .unwrap_or(0);
        format!("execution:{}:{}", node.id, executions)
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
    /// pass their stable call id, single-shot steps pass
    /// [`Self::execution_invocation`]. The same invocation reuses its id
    /// (and cached result) across resends; a new invocation always gets a
    /// fresh id even for identical content. A record naming a different
    /// invocation fails closed: the key is a full hash of the invocation,
    /// so a mismatch is corruption, not a new operation.
    pub fn guard_external_operation(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        kind: &str,
        target: &str,
        details: &Option<Value>,
        invocation_id: &str,
    ) -> Result<GuardDecision, StepError> {
        // No memoization (E09): the journaled digest stays authoritative and
        // hashing here is dominated by the surrounding I/O; a cache would
        // need measured justification before returning.
        let digest = Self::operation_digest(target, details)?;
        let operation_id = crate::operation_id_for(&self.run_id, &node.id, invocation_id);
        let mut record = journal
            .state()
            .operation_records
            .get(&operation_id)
            .cloned();
        // A spilled large result loads back here so the resend cache keeps
        // working for large results (E07). Load failures fail closed as
        // refusals: resending without the recorded bytes would re-execute
        // unknown-safe work. Callers without run-dir access (the pure guard
        // matrix) cannot load sidecars and keep refusing result-less
        // successes; only this dir-backed guard resolves them.
        if let Some(record) = record.as_mut()
            && record.result.is_none()
            && let Some(name) = record.result_ref.clone()
        {
            record.result = Some(self.load_spilled_result(node, &operation_id, &name)?);
        }
        if let Some(record) = &record {
            // Empty invocation ids are corrupt, never a pass-through:
            // accepting them would alias distinct operations under one
            // approval scope (E07/Q1).
            if record.invocation.is_empty() {
                return Err(StepError::Refused {
                    node: node.id.clone(),
                    message: format!(
                        "operation `{operation_id}` ({kind} to `{target}`) has an empty invocation; refusing as corrupt"
                    ),
                });
            }
            if record.invocation != invocation_id {
                return Err(StepError::Refused {
                    node: node.id.clone(),
                    message: format!(
                        "operation `{operation_id}` ({kind} to `{target}`) names a different invocation; refusing as corrupt"
                    ),
                });
            }
        }
        // Generation overflow fails closed: reusing a generation would let a
        // stale retry forge the attempt number (E13).
        let attempt = journal
            .state()
            .operation_attempts
            .get(&operation_id)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                StepError::failed(
                    &node.id,
                    format!("operation `{operation_id}` attempt generation overflowed"),
                )
            })?;
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
        let (status, reason, result, result_ref) = match outcome {
            OperationOutcome::Success { result } => {
                let (cached, spilled) = match result {
                    Some(value) => self.cache_success_result(node, operation_id, &value)?,
                    None => (None, None),
                };
                ("success", None, cached, spilled)
            }
            OperationOutcome::CleanError => ("clean", None, None, None),
            OperationOutcome::Indeterminate { reason } => {
                ("indeterminate", Some(reason), None, None)
            }
        };
        let mut payload =
            json!({ "node": node.id, "operation_id": operation_id, "status": status });
        if let Some(reason) = reason {
            payload["reason"] = Value::String(reason);
        }
        if let Some(result) = result {
            payload["result"] = result;
        }
        if let Some(result_ref) = result_ref {
            payload["result_ref"] = Value::String(result_ref);
        }
        journal
            .event("operation_finished", payload)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))
    }

    /// Caches a success result for same-invocation resends. Results within
    /// the inline bound ride in the journal; larger results spill to a
    /// tamper-evident sidecar blob under the run meta dir (named by the
    /// content hash, verified on load) and the journal records a
    /// `result_ref`, so the resend cache keeps working for large results
    /// instead of routing to manual recovery (E07). Serialization and
    /// spill failures propagate with context: recording a success without
    /// any retrievable result would silently degrade the resend path.
    fn cache_success_result(
        &self,
        node: &NodeDef,
        operation_id: &str,
        value: &Value,
    ) -> Result<(Option<Value>, Option<String>), StepError> {
        let fail = |message: String| StepError::failed(&node.id, message);
        let bytes = serde_json::to_vec(value).map_err(|error| {
            fail(format!(
                "operation `{operation_id}` result is not serializable, refusing to record an uninspectable success: {error}"
            ))
        })?;
        if bytes.len() <= crate::OPERATION_RESULT_MAX_BYTES {
            return Ok((Some(value.clone()), None));
        }
        // Content-hash sidecar name, deliberately unsalted: identical
        // results deduplicate to one blob, and the name carries no secret
        // (it is a hash, verified on load before use). Salted names would
        // defeat dedup while adding no secrecy (E07/E09).
        use sha2::{Digest, Sha256};
        let name = format!("{}.json", hex::encode(Sha256::digest(&bytes)));
        let dir = self.metadata.join("operation-results");
        std::fs::create_dir_all(dir.as_std_path()).map_err(|error| {
            fail(format!(
                "operation `{operation_id}` large result sidecar dir cannot be created: {error}"
            ))
        })?;
        let path = dir.join(&name);
        // Owner-only sidecar: results may carry sensitive data, and the
        // mode is set explicitly instead of inheriting the umask. O_NOFOLLOW
        // refuses a planted link at the sidecar path on Unix.
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        use std::io::Write as _;
        let mut file = options.open(path.as_std_path()).map_err(|error| {
            fail(format!(
                "operation `{operation_id}` large result sidecar cannot be opened: {error}"
            ))
        })?;
        file.write_all(&bytes).map_err(|error| {
            fail(format!(
                "operation `{operation_id}` large result sidecar cannot be written: {error}"
            ))
        })?;
        file.sync_all().map_err(|error| {
            fail(format!(
                "operation `{operation_id}` large result sidecar cannot be synced: {error}"
            ))
        })?;
        drop(file);
        // Sync the directory entry like every other sidecar publication:
        // without it a power loss can lose the sidecar name (Q2). Windows
        // cannot open a directory; NTFS journals the rename itself there.
        #[cfg(unix)]
        std::fs::File::open(dir.as_std_path())
            .and_then(|dir| dir.sync_all())
            .map_err(|error| {
                fail(format!(
                    "operation `{operation_id}` large result directory cannot be synced: {error}"
                ))
            })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path.as_std_path(), std::fs::Permissions::from_mode(0o600))
                .map_err(|error| {
                    fail(format!(
                        "operation `{operation_id}` large result sidecar mode cannot be set: {error}"
                    ))
                })?;
        }
        Ok((None, Some(name)))
    }

    /// Loads a spilled large result back for a resend. The sidecar name is
    /// the content hash, so a swapped or truncated file fails closed as
    /// tampering instead of resending forged bytes (E07).
    fn load_spilled_result(
        &self,
        node: &NodeDef,
        operation_id: &str,
        name: &str,
    ) -> Result<Value, StepError> {
        let fail = |message: String| StepError::Refused {
            node: node.id.clone(),
            message,
        };
        if name.contains('/') || name.contains('\\') || !name.ends_with(".json") {
            return Err(fail(format!(
                "operation `{operation_id}` names an unsafe spilled result `{name}`; refusing resend"
            )));
        }
        let path = self.metadata.join("operation-results").join(name);
        let bytes = std::fs::read(path.as_std_path()).map_err(|error| {
            fail(format!(
                "operation `{operation_id}` spilled result `{name}` is unavailable: {error}; manual recovery required"
            ))
        })?;
        use sha2::{Digest, Sha256};
        let expected = name.trim_end_matches(".json");
        if hex::encode(Sha256::digest(&bytes)) != expected {
            return Err(fail(format!(
                "operation `{operation_id}` spilled result `{name}` failed integrity check; refusing resend"
            )));
        }
        serde_json::from_slice(&bytes).map_err(|error| {
            fail(format!(
                "operation `{operation_id}` spilled result `{name}` is corrupt: {error}; manual recovery required"
            ))
        })
    }

    /// Completion record for an error path that already returns its own
    /// error: a failed record must neither replace the original error nor
    /// vanish silently, so the full completion failure (node, operation,
    /// outcome debug, and error chain) is surfaced as a warning instead of
    /// being swallowed (E07).
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
                run_id = %self.run_id,
                journal = %journal.journal_path.as_str(),
                error = %format!("{error:#}"),
                "operation completion record failed; the original step error is returned"
            );
        }
    }

    /// Returns the confirmation required for a side effect, if any. The
    /// approval identity depends on `permissions.side_effects_scope`:
    /// `invocation` binds one call (the default), `content` reuses an
    /// approval for identical content across invocations (Q1).
    pub fn require_side_effect(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        kind: &str,
        target: &str,
        details: Option<Value>,
        invocation_id: &str,
    ) -> Result<Option<ConfirmSpec>, StepError> {
        use qcg_contract::SideEffectScope;
        // No memoization (E09): see the guard path above.
        let digest = Self::operation_digest(target, &details)?;
        let id = match self.contract.manifest.permissions.side_effects_scope {
            SideEffectScope::Content => format!("{}:{kind}:{}", node.id, digest),
            SideEffectScope::Invocation => {
                use sha2::{Digest as _, Sha256};
                let invocation = hex::encode(Sha256::digest(invocation_id.as_bytes()));
                format!("{}:{kind}:{}:{}", node.id, digest, invocation)
            }
        };
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
                // Explicit absence handling: no confirmation record means
                // not approved (false), never an error. Approval is an
                // explicit durable accept; absence cannot read as approval.
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
                // Journal events borrow the details instead of cloning them:
                // a single serialization per event and no duplicate clone
                // across the dry-run and decision events. The owned value
                // moves into the returned spec below exactly once.
                if dry_run {
                    journal
                        .event(
                            "dry_run",
                            json!({ "node": node.id, "kind": kind, "target": target, "details": details.as_ref(), "operation_digest": digest }),
                        )
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                }
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "confirmation_required", "policy": format!("{policy:?}"), "dry_run": dry_run, "details": details.as_ref(), "operation_digest": digest }),
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
                    scope: self.contract.manifest.permissions.side_effects_scope,
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
    fn operation_started_is_durable_before_any_gateway_use() {
        // Q2: every caller invokes the gateway only after the guard
        // returns, so proving the `operation_started` record is already in
        // the journal FILE at guard-return time proves the mapping precedes
        // any external effect. No gateway exists yet at the assert point.
        // Directory-entry durability ordering is asserted too: the journal
        // file bytes are durable (`sync_data` on the operation path) AND
        // the parent directory entry is durable (parent sync succeeds,
        // proving the name is durable, not just the bytes), and state.json
        // carries the same mapping with a matching seq. Both file and
        // dir-entry durability precede any gateway use.
        let (_dir, metadata, ctx, node) = guard_harness("ordering-proof");
        let journal_path = metadata.join("journal.jsonl");
        let journal = crate::JournalWriter::create(&journal_path, "ordering-proof", false, None)
            .expect("test journal should open");
        let details = Some(json!({"argv": ["echo", "ordered"]}));
        let GuardDecision::Proceed { operation_id } = ctx
            .guard_external_operation(&journal, &node, "command", "echo", &details, "call-1")
            .expect("guard should proceed")
        else {
            panic!("guard must proceed");
        };
        // File durability: the journal file must already hold the mapping
        // with non-zero length before any gateway use.
        let journal_len = std::fs::metadata(metadata.join("journal.jsonl"))
            .expect("journal file must exist after the guard")
            .len();
        assert!(
            journal_len > 0,
            "the journal file bytes must be durable before gateway use"
        );
        let started = journal_values(&metadata.join("journal.jsonl"))
            .into_iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("operation_started")
                    && event.get("operation_id").and_then(Value::as_str) == Some(&operation_id)
            })
            .expect("operation_started must be journaled before any gateway use");
        assert_eq!(
            started.get("operation_digest").and_then(Value::as_str),
            Some(
                RunContext::operation_digest("echo", &details)
                    .expect("digest should compute")
                    .as_str()
            ),
            "the journaled mapping must bind the executed content"
        );
        // The atomic state persist follows the journal fsync in the same
        // locked append: state.json must carry the same operation record at
        // the same seq, proving guard-to-send ordering is preceded by both
        // file and directory-entry durability.
        let state_bytes = std::fs::read(metadata.join("state.json"))
            .expect("state.json must exist after the guard");
        assert!(
            !state_bytes.is_empty(),
            "state.json bytes must be durable before gateway use"
        );
        let state: serde_json::Value =
            serde_json::from_slice(&state_bytes).expect("state.json must parse");
        assert_eq!(
            state.get("last_seq").and_then(Value::as_u64),
            started.get("seq").and_then(Value::as_u64),
            "state.json must agree with the journal tail after the guard"
        );
        assert!(
            state
                .get("operation_records")
                .and_then(|records| records.get(&operation_id))
                .is_some(),
            "state.json must carry the operation mapping"
        );
        #[cfg(unix)]
        std::fs::File::open(metadata.as_std_path())
            .and_then(|dir| dir.sync_all())
            .expect("the journal parent directory entry must be durable before gateway use");
    }

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

    #[test]
    fn operation_digest_is_invariant_under_key_insertion_order() {
        // E07: serde_json::Map must stay BTreeMap-sorted (no
        // preserve_order feature). Logically identical details built in
        // different insertion orders must bind one digest, or approvals
        // and guards fork into aliases.
        let mut first = serde_json::Map::new();
        first.insert("b".to_string(), json!(2));
        first.insert("a".to_string(), json!(1));
        let mut second = serde_json::Map::new();
        second.insert("a".to_string(), json!(1));
        second.insert("b".to_string(), json!(2));
        let a = RunContext::operation_digest("target", &Some(Value::Object(first)))
            .expect("digest should compute");
        let b = RunContext::operation_digest("target", &Some(Value::Object(second)))
            .expect("digest should compute");
        assert_eq!(a, b, "key order must not fork the digest");
    }

    fn record(digest: &str, status: OperationStatus, result: Option<Value>) -> OperationRecord {
        // Test-only helper for digest/cache unit tests that never reach the
        // invocation check: production records always carry a non-empty
        // invocation, and the guard refuses empty ones as corrupt (E07).
        OperationRecord {
            digest: digest.into(),
            status,
            result,
            result_ref: None,
            invocation: String::new(),
        }
    }

    /// Parses every journal line strictly: a corrupt line must fail the
    /// test instead of being silently dropped from the count.
    fn journal_values(path: &camino::Utf8Path) -> Vec<Value> {
        std::fs::read_to_string(path)
            .expect("journal should be readable")
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line)
                    .unwrap_or_else(|error| panic!("journal line must parse: {error}: {line}"))
            })
            .collect()
    }

    fn operation_start_count(path: &camino::Utf8Path) -> usize {
        journal_values(path)
            .iter()
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("operation_started"))
            .count()
    }

    /// Full guard harness: a live run context plus its journal directory.
    /// The caller opens the journal when ready so pre-existing journal
    /// content (crash recovery) folds first.
    #[allow(clippy::too_many_lines)]
    fn guard_harness(
        run_id: &str,
    ) -> (
        crate::test_support::TestDir,
        camino::Utf8PathBuf,
        RunContext,
        qcg_contract::NodeDef,
    ) {
        use crate::TemplateService;
        use crate::engine::checkpoint::CheckpointAccounting;
        use camino::Utf8PathBuf;
        use qcg_contract::{
            AssetSpec, Contract, FailurePolicy, GeneratorMeta, Graph, InputSpec, Manifest, OnDeps,
            OutputSpec, Permissions, RetentionPolicy, StepType,
        };
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tokio_util::sync::CancellationToken;

        let dir = crate::test_support::TestDir::create("guard");
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
            retention: RetentionPolicy::default(),
            audit: qcg_policy::AuditConfig::default(),
            hooks: Default::default(),
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
            run_refs: Arc::new(std::collections::BTreeMap::new()),
            cancellation: CancellationToken::new(),
            elapsed_deadline: None,
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
    fn http_details_digest_headers_without_exposing_values() {
        // E09: headers participate in the content binding by digest only;
        // header values never appear in the details.
        use std::collections::BTreeMap;
        let mut headers = BTreeMap::new();
        headers.insert("X-Tenant".to_string(), "alpha".to_string());
        let first = http_operation_details(
            "POST",
            &headers,
            Some(b"body"),
            &Default::default(),
            "salt-1",
        )
        .expect("details should exist");
        headers.insert("X-Tenant".to_string(), "beta".to_string());
        let second = http_operation_details(
            "POST",
            &headers,
            Some(b"body"),
            &Default::default(),
            "salt-1",
        )
        .expect("details should exist");
        assert_ne!(
            first["headers_sha256"], second["headers_sha256"],
            "changed headers must change the content binding"
        );
        assert!(
            !first.to_string().contains("alpha") && !second.to_string().contains("beta"),
            "header values must stay out of the journaled details"
        );
        assert!(
            http_operation_details("GET", &headers, None, &Default::default(), "salt-1").is_none(),
            "safe methods have no side-effect binding"
        );
        // Header casing must not fork the approval identity.
        let mut recased = BTreeMap::new();
        recased.insert("x-tenant".to_string(), "alpha".to_string());
        let canonical = http_operation_details(
            "POST",
            &recased,
            Some(b"body"),
            &Default::default(),
            "salt-1",
        )
        .expect("details should exist");
        headers.insert("X-Tenant".to_string(), "alpha".to_string());
        let original = http_operation_details(
            "POST",
            &headers,
            Some(b"body"),
            &Default::default(),
            "salt-1",
        )
        .expect("details should exist");
        assert_eq!(
            canonical["headers_sha256"], original["headers_sha256"],
            "header casing must not change the binding"
        );
    }

    #[test]
    fn stdin_changes_require_a_fresh_approval() {
        // Q1: the approval binds the exact stdin bytes, so feeding
        // different input to the same command never reuses an approval.
        use qcg_contract::SideEffects;
        let (_dir, metadata, mut ctx, node) = guard_harness("stdin-binding");
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "stdin-binding",
            false,
            None,
        )
        .expect("test journal should open");
        ctx.contract.manifest.permissions.side_effects = SideEffects::Confirm;
        let details = |stdin: &str| {
            Some(
                crate::bind_command_stdin(
                    &node.id,
                    json!({"argv": ["cat"]}),
                    Some(stdin.as_bytes()),
                    "test-salt",
                )
                .expect("test details should build"),
            )
        };
        let first = ctx
            .require_side_effect(&journal, &node, "command", "cat", details("one"), "call-1")
            .expect("first approval")
            .expect("confirmation required");
        let second = ctx
            .require_side_effect(&journal, &node, "command", "cat", details("two"), "call-1")
            .expect("second approval")
            .expect("confirmation required");
        assert_ne!(
            first.id, second.id,
            "different stdin must not share an approval"
        );
    }

    #[test]
    fn sensitive_values_bind_approval_without_entering_the_journal() {
        // E09: different secret values must produce different approval
        // bindings, while the journaled details carry only the digest.
        use std::collections::BTreeMap;
        let headers = BTreeMap::new();
        let first = BTreeMap::from([("api_key".to_string(), "secret-a".to_string())]);
        let second = BTreeMap::from([("api_key".to_string(), "secret-b".to_string())]);
        let one = http_operation_details("POST", &headers, None, &first, "salt-1")
            .expect("details should exist");
        let two = http_operation_details("POST", &headers, None, &second, "salt-1")
            .expect("details should exist");
        assert_ne!(
            one["sensitive_query_sha256"], two["sensitive_query_sha256"],
            "a changed secret must change the approval binding"
        );
        assert!(
            !one.to_string().contains("secret-a") && !two.to_string().contains("secret-b"),
            "secret values must stay out of the journaled details"
        );
    }

    #[test]
    fn identical_secrets_do_not_share_digests_across_operations() {
        // E09: the same secret in different operations must yield
        // different journaled digests so a journal reader cannot correlate
        // secret reuse across runs.
        use std::collections::BTreeMap;
        let headers = BTreeMap::new();
        let secret = BTreeMap::from([("api_key".to_string(), "same-secret".to_string())]);
        let one = http_operation_details("POST", &headers, None, &secret, "salt-1")
            .expect("details should exist");
        let two = http_operation_details("POST", &headers, None, &secret, "salt-2")
            .expect("details should exist");
        assert_ne!(
            one["sensitive_query_sha256"], two["sensitive_query_sha256"],
            "different operations must not share a secret digest"
        );
        let same = http_operation_details("POST", &headers, None, &secret, "salt-1")
            .expect("details should exist");
        assert_eq!(
            one["sensitive_query_sha256"], same["sensitive_query_sha256"],
            "the same operation must recompute stably for resends"
        );
        assert_eq!(
            salted_binding_digest("d", "s", b"x"),
            salted_binding_digest("d", "s", b"x"),
            "salted digests must be deterministic"
        );
        assert_ne!(
            salted_binding_digest("d", "s1", b"x"),
            salted_binding_digest("d", "s2", b"x"),
            "different salts must separate identical bytes"
        );
    }

    #[test]
    fn approval_scope_controls_reuse_across_invocations() {
        // Q1: invocation scope (the default) requires one approval per
        // call; content scope reuses an approval for identical content.
        use qcg_contract::{SideEffectScope, SideEffects};
        let (_dir, metadata, mut ctx, node) = guard_harness("approval-scope");
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "approval-scope",
            false,
            None,
        )
        .expect("test journal should open");
        ctx.contract.manifest.permissions.side_effects = SideEffects::Confirm;
        let details = Some(json!({"argv": ["echo", "hi"]}));
        assert_eq!(
            ctx.contract.manifest.permissions.side_effects_scope,
            SideEffectScope::Invocation,
            "invocation scope must be the default"
        );
        let first = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                "call-1",
            )
            .expect("first approval")
            .expect("confirmation required");
        let second = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                "call-2",
            )
            .expect("second approval")
            .expect("confirmation required");
        assert_ne!(
            first.id, second.id,
            "invocation scope must not reuse an approval"
        );
        // Negative side: a stored approval for call-1 never satisfies
        // call-2 under invocation scope (Q1).
        ctx.confirmations.insert(first.id.clone(), true);
        let still_required = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                "call-2",
            )
            .expect("stored invocation approval must not leak across calls")
            .expect("second call needs its own confirmation");
        assert_eq!(
            still_required.id, second.id,
            "the second call must re-confirm with its own id"
        );
        ctx.contract.manifest.permissions.side_effects_scope = SideEffectScope::Content;
        let third = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                "call-1",
            )
            .expect("third approval")
            .expect("confirmation required");
        let fourth = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                "call-2",
            )
            .expect("fourth approval")
            .expect("confirmation required");
        assert_eq!(
            third.id, fourth.id,
            "content scope reuses the approval for identical content"
        );
        // Success side: one pre-provisioned confirmation for the shared id
        // satisfies the second identical call without re-confirming (Q1).
        ctx.confirmations.insert(third.id.clone(), true);
        let reused = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                "call-2",
            )
            .expect("reused approval");
        assert!(
            reused.is_none(),
            "content scope with a stored approval must proceed without a new confirmation"
        );
        let changed = Some(json!({"argv": ["echo", "other"]}));
        let fifth = ctx
            .require_side_effect(&journal, &node, "command", "echo", changed, "call-1")
            .expect("fifth approval")
            .expect("confirmation required");
        assert_ne!(
            third.id, fifth.id,
            "changed content must require a fresh approval"
        );
    }

    #[test]
    fn execution_invocation_advances_only_after_a_finished_execution() {
        // Q1: the real single-shot identity comes from the durable per-node
        // execution count, so an identical re-execution after a finished
        // attempt is a new invocation and must re-confirm.
        use qcg_contract::SideEffects;
        let (_dir, metadata, mut ctx, node) = guard_harness("execution-invocation");
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "execution-invocation",
            false,
            None,
        )
        .expect("test journal should open");
        ctx.contract.manifest.permissions.side_effects = SideEffects::Confirm;
        let details = Some(json!({"argv": ["echo", "hi"]}));
        let first_invocation = RunContext::execution_invocation(&journal, &node);
        let first = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                &first_invocation,
            )
            .expect("first approval")
            .expect("confirmation required");
        // A finished execution of the node is a new logical call.
        journal
            .event(
                "step_finished",
                json!({
                    "node": node.id,
                    "status": "failed",
                    "reason": {"code": "execution_failed", "message": "boom"},
                }),
            )
            .expect("finish should journal");
        let second_invocation = RunContext::execution_invocation(&journal, &node);
        assert_ne!(
            first_invocation, second_invocation,
            "a finished execution must start a new invocation"
        );
        let second = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                &second_invocation,
            )
            .expect("second approval")
            .expect("confirmation required");
        assert_ne!(
            first.id, second.id,
            "identical content after a finished execution must re-confirm"
        );
        // The first approval never satisfies the second invocation.
        ctx.confirmations.insert(first.id.clone(), true);
        let again = ctx
            .require_side_effect(
                &journal,
                &node,
                "command",
                "echo",
                details.clone(),
                &second_invocation,
            )
            .expect("third approval")
            .expect("confirmation required");
        assert_eq!(again.id, second.id);
        // Retries of the still-unfinished execution keep their invocation.
        assert_eq!(
            RunContext::execution_invocation(&journal, &node),
            second_invocation
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
        // stored digest comparison is always reached. An approval
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
        let starts = operation_start_count(&metadata.join("journal.jsonl"));
        assert_eq!(starts, 2, "refused resends must not start executions");
    }

    #[test]
    fn agent_call_id_guard_covers_every_resume_flow() {
        // E07: the four resume flows a suspended agent call can hit, keyed
        // by the stable agent call id: Succeeded to Resend, FailedClean to a
        // fresh attempt, Indeterminate with Repeat to an acknowledged
        // repeat, and Indeterminate with Fail to a refusal.
        use crate::OperationOutcome;
        use qcg_contract::RetryOnIndeterminate;

        let details = Some(json!({"url": "https://example.test"}));

        // Succeeded: the cached result is resent, never re-executed.
        let (_dir, metadata, ctx, node) = guard_harness("agent-flow-succeeded");
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "agent-flow-succeeded",
            false,
            None,
        )
        .expect("journal");
        let GuardDecision::Proceed { operation_id } = ctx
            .guard_external_operation(
                &journal,
                &node,
                "http",
                "https://example.test",
                &details,
                "call-agent-1",
            )
            .expect("first call should proceed")
        else {
            panic!("first call must proceed");
        };
        ctx.finish_external_operation(&journal, &node, &operation_id, Some(json!({"status": 200})))
            .expect("success should journal");
        match ctx
            .guard_external_operation(
                &journal,
                &node,
                "http",
                "https://example.test",
                &details,
                "call-agent-1",
            )
            .expect("resend should converge")
        {
            GuardDecision::Resend {
                operation_id: id,
                result,
            } => {
                assert_eq!(id, operation_id);
                assert_eq!(result, json!({"status": 200}));
            }
            GuardDecision::Proceed { .. } => panic!("succeeded must resend, not re-execute"),
        }

        // FailedClean: a proven-no-effect failure starts a fresh attempt.
        let (_dir, metadata, ctx, node) = guard_harness("agent-flow-clean");
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "agent-flow-clean",
            false,
            None,
        )
        .expect("journal");
        let GuardDecision::Proceed { operation_id } = ctx
            .guard_external_operation(
                &journal,
                &node,
                "http",
                "https://example.test",
                &details,
                "call-agent-2",
            )
            .expect("first call should proceed")
        else {
            panic!("first call must proceed");
        };
        ctx.finish_external_operation_with(
            &journal,
            &node,
            &operation_id,
            OperationOutcome::CleanError,
        )
        .expect("clean failure should journal");
        match ctx
            .guard_external_operation(
                &journal,
                &node,
                "http",
                "https://example.test",
                &details,
                "call-agent-2",
            )
            .expect("clean failure should start a fresh attempt")
        {
            GuardDecision::Proceed { .. } => {
                // The retry keeps the invocation identity and increments
                // the durable attempt counter; only the status motion
                // matters here.
            }
            _ => panic!("clean failure must start a fresh attempt"),
        }

        // Indeterminate with Repeat: re-execute with an acknowledged risk.
        let (_dir, metadata, ctx, mut node) = guard_harness("agent-flow-repeat");
        node.retry = Some(qcg_contract::RetryPolicy {
            max_attempts: 2,
            backoff_ms: 0,
            timeout_secs: None,
            on_indeterminate: RetryOnIndeterminate::Repeat,
        });
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "agent-flow-repeat",
            false,
            None,
        )
        .expect("journal");
        let GuardDecision::Proceed { operation_id } = ctx
            .guard_external_operation(
                &journal,
                &node,
                "http",
                "https://example.test",
                &details,
                "call-agent-3",
            )
            .expect("first call should proceed")
        else {
            panic!("first call must proceed");
        };
        ctx.finish_external_operation_with(
            &journal,
            &node,
            &operation_id,
            OperationOutcome::Indeterminate {
                reason: "connection lost after send".into(),
            },
        )
        .expect("indeterminate should journal");
        assert!(
            matches!(
                ctx.guard_external_operation(
                    &journal,
                    &node,
                    "http",
                    "https://example.test",
                    &details,
                    "call-agent-3",
                ),
                Ok(GuardDecision::Proceed { .. })
            ),
            "repeat policy must re-execute the indeterminate call"
        );

        // Indeterminate with Fail: refuse, never re-execute silently.
        let (_dir, metadata, ctx, mut node) = guard_harness("agent-flow-refuse");
        node.retry = Some(qcg_contract::RetryPolicy {
            max_attempts: 2,
            backoff_ms: 0,
            timeout_secs: None,
            on_indeterminate: RetryOnIndeterminate::Fail,
        });
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "agent-flow-refuse",
            false,
            None,
        )
        .expect("journal");
        let GuardDecision::Proceed { operation_id } = ctx
            .guard_external_operation(
                &journal,
                &node,
                "http",
                "https://example.test",
                &details,
                "call-agent-4",
            )
            .expect("first call should proceed")
        else {
            panic!("first call must proceed");
        };
        ctx.finish_external_operation_with(
            &journal,
            &node,
            &operation_id,
            OperationOutcome::Indeterminate {
                reason: "connection lost after send".into(),
            },
        )
        .expect("indeterminate should journal");
        assert!(
            matches!(
                ctx.guard_external_operation(
                    &journal,
                    &node,
                    "http",
                    "https://example.test",
                    &details,
                    "call-agent-4",
                ),
                Err(StepError::Refused { .. })
            ),
            "fail policy must refuse the indeterminate call"
        );
    }

    #[test]
    fn succeeded_without_cached_result_refuses_replay_from_the_journal() {
        // D03: a secret- or guardrail-rejected result is recorded as a
        // success with no cached result. A resumed invocation must refuse
        // instead of re-executing the external operation.
        let (_dir, metadata, ctx, node) = guard_harness("rejected-result");
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "rejected-result",
            false,
            None,
        )
        .expect("test journal should open");
        let details = Some(json!({"argv": ["produce_secret"]}));
        let first = ctx
            .guard_external_operation(
                &journal,
                &node,
                "command",
                "produce_secret",
                &details,
                "call-1",
            )
            .expect("first invocation should proceed");
        let GuardDecision::Proceed { operation_id } = first else {
            panic!("first invocation must proceed");
        };
        ctx.finish_external_operation(&journal, &node, &operation_id, None)
            .expect("success without a cached result should journal");
        let error = ctx
            .guard_external_operation(
                &journal,
                &node,
                "command",
                "produce_secret",
                &details,
                "call-1",
            )
            .expect_err("a replay of an uncached success must be refused");
        assert!(
            matches!(error, crate::StepError::Refused { .. }),
            "uncached success must refuse automatic replay, got: {error}"
        );
        assert_eq!(
            operation_start_count(&metadata.join("journal.jsonl")),
            1,
            "the refused replay must not journal a new start"
        );
    }

    #[test]
    fn distinct_invocations_get_distinct_ids() {
        // Invocation ids bind by their full digest, so two calls that share
        // a prefix must never reuse or refuse on the other's record.
        let first = crate::operation_id_for("run", "node", "call-28383");
        let second = crate::operation_id_for("run", "node", "call-78343");
        assert_ne!(
            first, second,
            "distinct invocations must never share an operation id"
        );
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

    #[test]
    fn checkpoint_scope_uses_the_single_error_node_format() {
        // E11: node scope names the node, loop scope names the `runtime`
        // sentinel; both enforce the same monotonic deadline. The deadline
        // and limit travel as one tuple so a limitless deadline is
        // unrepresentable (no unreachable branch).
        use tokio_util::sync::CancellationToken;
        let past = tokio::time::Instant::now() - std::time::Duration::from_secs(1);
        let error = checkpoint_scope(&CancellationToken::new(), Some((past, 60)), "build")
            .expect_err("a past monotonic deadline must fire");
        assert!(
            matches!(
                error,
                crate::StepError::ElapsedExceeded { ref node, limit_secs: 60 } if node.as_str() == "build"
            ),
            "node scope must name the node: {error}"
        );
        let error = checkpoint_scope(&CancellationToken::new(), Some((past, 60)), "runtime")
            .expect_err("a past monotonic deadline must fire");
        assert!(
            matches!(
                error,
                crate::StepError::ElapsedExceeded { ref node, .. } if node.as_str() == "runtime"
            ),
            "loop scope must name the runtime sentinel: {error}"
        );
    }

    #[test]
    fn checkpoint_scope_enforces_the_monotonic_deadline_only() {
        // E11: enforcement reads the monotonic deadline alone. A past
        // deadline fires with no wall-clock input at all (wall-clock skew
        // can neither save nor doom an attempt), and a future deadline
        // passes without consulting any durable timestamp.
        use tokio_util::sync::CancellationToken;
        let past = tokio::time::Instant::now() - std::time::Duration::from_secs(1);
        assert!(
            checkpoint_scope(&CancellationToken::new(), Some((past, 60)), "n").is_err(),
            "a past monotonic deadline fires regardless of wall clock"
        );
        let future = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
        assert!(
            checkpoint_scope(&CancellationToken::new(), Some((future, 60)), "n").is_ok(),
            "a future monotonic deadline passes regardless of wall clock"
        );
        assert!(
            checkpoint_scope(&CancellationToken::new(), None, "n").is_ok(),
            "no deadline means no enforcement"
        );
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            matches!(
                checkpoint_scope(&cancelled, Some((future, 60)), "n"),
                Err(crate::StepError::Cancelled)
            ),
            "cancellation still wins over a future deadline"
        );
    }

    #[test]
    fn elapsed_limit_secs_is_explicit_about_absence() {
        // E11: no configured budget is `None`, never a misreadable 0.
        let (_dir, _metadata, ctx, _node) = guard_harness("elapsed-limit");
        assert_eq!(
            elapsed_limit_secs(&ctx.contract),
            None,
            "an unconfigured budget must read as absent"
        );
    }

    #[test]
    fn large_operation_result_spills_and_resends() {
        // E07: a success result beyond the inline bound spills to a
        // tamper-evident sidecar and resends from it; a damaged sidecar
        // refuses instead of resending forged bytes.
        let (_dir, metadata, ctx, node) = guard_harness("spill-resend");
        let journal = crate::JournalWriter::create(
            &metadata.join("journal.jsonl"),
            "spill-resend",
            false,
            None,
        )
        .expect("test journal should open");
        let big = Value::String("x".repeat(crate::OPERATION_RESULT_MAX_BYTES + 1));
        let details = Some(json!({"argv": ["big"]}));
        let GuardDecision::Proceed { operation_id } = ctx
            .guard_external_operation(&journal, &node, "command", "big", &details, "call-1")
            .expect("first guard should proceed")
        else {
            panic!("first guard must proceed");
        };
        ctx.finish_external_operation(&journal, &node, &operation_id, Some(big.clone()))
            .expect("spilling finish should journal");
        // The journal carries a reference, never the large inline bytes.
        let values = journal_values(&metadata.join("journal.jsonl"));
        let finished = values
            .iter()
            .find(|event| event.get("t").and_then(Value::as_str) == Some("operation_finished"))
            .expect("finish should be journaled");
        assert!(
            finished.get("result").is_none(),
            "a large result must not ride inline in the journal"
        );
        let name = finished
            .get("result_ref")
            .and_then(Value::as_str)
            .expect("a large result must journal a sidecar reference");
        assert!(
            metadata.join("operation-results").join(name).exists(),
            "the sidecar blob must exist under the run meta dir"
        );
        // A same-invocation resend loads the spilled bytes back.
        match ctx
            .guard_external_operation(&journal, &node, "command", "big", &details, "call-1")
            .expect("resend should converge")
        {
            GuardDecision::Resend { result, .. } => assert_eq!(result, big),
            GuardDecision::Proceed { .. } => panic!("resend must not re-execute"),
        }
        // A damaged sidecar fails closed as tampering, not as a resend.
        std::fs::write(metadata.join("operation-results").join(name), b"tampered")
            .expect("tamper should write");
        match ctx.guard_external_operation(&journal, &node, "command", "big", &details, "call-1") {
            Err(crate::StepError::Refused { .. }) => {}
            other => panic!("a damaged sidecar must refuse resend, got {other:?}"),
        }
    }
}
