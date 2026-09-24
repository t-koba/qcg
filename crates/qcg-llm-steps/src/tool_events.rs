use qcg_api::{ToolCallError, ToolCallErrorCode, ToolCallEventData, ToolCallPhase, ToolCallStatus};
use qcg_contract::NodeDef;
use qcg_engine::{ResultExt, StepContext, StepError, tool_call_sources};
use qcg_llm::{ChatMessage, ChatToolCall};
use qcg_mcp::McpCallOutcome;
use serde_json::{Value, json};
use sha2::Digest;
use std::collections::BTreeMap;
use std::time::Instant;

use crate::mcp_forms::{mcp_form_spec, mcp_input_responses, mcp_question_id};
use crate::mcp_tools::{AgentToolOutcome, McpAgentTools};
use crate::prompting::utf8_head;
use qcg_policy::TOOL_EVENT_VALUE_LIMIT_BYTES;

pub(crate) struct ToolCallEventOutcome {
    pub(crate) status: ToolCallStatus,
    pub(crate) phase: ToolCallPhase,
    pub(crate) error: Option<ToolCallError>,
    pub(crate) duration: std::time::Duration,
}

pub(crate) fn tool_call_outcome(
    status: ToolCallStatus,
    phase: ToolCallPhase,
    error: Option<ToolCallError>,
    started: Instant,
) -> ToolCallEventOutcome {
    ToolCallEventOutcome {
        status,
        phase,
        error,
        duration: started.elapsed(),
    }
}

pub(crate) fn tool_call_event(
    node: &str,
    agent: Option<&str>,
    server: Option<&str>,
    call: &ChatToolCall,
    result: &Value,
    outcome: ToolCallEventOutcome,
) -> Result<Value, serde_json::Error> {
    let sources = tool_call_sources(result);
    // Journaled arguments never carry plaintext secrets: HTTP query,
    // credential headers, and bodies plus `fs.write` contents are redacted
    // via the shared gateway helpers, and any other credential-like keys
    // are redacted generically (E09). Every remaining URL query VALUE is
    // additionally redacted by default (keys stay visible) so an
    // undeclared secret never reaches the journal (E09-1). Execution
    // always uses the raw call; only this journaled copy is redacted.
    let mut redacted_args =
        qcg_engine::redact_credential_values(&qcg_engine::redact_fs_write_args_for_journal(
            &qcg_engine::redact_http_args_for_journal(&call.args),
        ));
    if let Some(url) = redacted_args
        .get("url")
        .and_then(Value::as_str)
        .map(str::to_owned)
        && let Some(object) = redacted_args.as_object_mut()
    {
        object.insert(
            "url".into(),
            Value::String(qcg_policy::redact_all_query_values(&url)),
        );
    }
    let (arguments, arguments_truncated) = bounded_event_value(&redacted_args)?;
    // E09-3: journaled results never carry plaintext credential headers or
    // query values. Authorization-like response headers are redacted and
    // result URLs are query-redacted before the explicit 32 KiB truncation
    // below; truncation bounds size, redaction removes secrets. The live
    // tool result keeps full headers for execution.
    let mut redacted_result = result.clone();
    if let Some(url) = redacted_result
        .get("url")
        .and_then(Value::as_str)
        .map(str::to_owned)
        && let Some(object) = redacted_result.as_object_mut()
    {
        object.insert(
            "url".into(),
            Value::String(qcg_policy::redact_all_query_values(&url)),
        );
    }
    if let Some(headers) = redacted_result
        .get("headers")
        .and_then(Value::as_object)
        .map(|headers| {
            headers
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<std::collections::BTreeMap<String, Value>>()
        })
    {
        let string_headers = headers
            .iter()
            .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_owned())))
            .collect::<std::collections::BTreeMap<String, String>>();
        let redacted_headers = qcg_policy::redact_header_values(&string_headers);
        if let Some(object) = redacted_result.as_object_mut()
            && let Some(headers_value) = object.get_mut("headers").and_then(Value::as_object_mut)
        {
            for (key, value) in redacted_headers {
                headers_value.insert(key, Value::String(value));
            }
        }
    }
    let (result, result_truncated) = bounded_event_value(&redacted_result)?;
    let data = ToolCallEventData {
        server: server.map(str::to_owned),
        tool: call.name.clone(),
        id: call.id.clone(),
        status: outcome.status,
        phase: outcome.phase,
        agent: agent.map(str::to_owned),
        error: outcome.error,
        duration_ms: outcome.duration.as_millis().min(u128::from(u64::MAX)) as u64,
        arguments,
        result,
        sources: serde_json::from_value(Value::Array(sources))?,
        truncated: arguments_truncated || result_truncated,
    };
    let mut event = serde_json::to_value(data)?;
    event["node"] = Value::String(node.to_owned());
    Ok(event)
}

pub(crate) fn tool_call_error(code: ToolCallErrorCode, error: &StepError) -> ToolCallError {
    let code = match error {
        StepError::Cancelled => ToolCallErrorCode::Cancelled,
        StepError::BudgetExceeded { .. } => ToolCallErrorCode::BudgetExceeded,
        StepError::TimedOut { .. } => ToolCallErrorCode::TimedOut,
        StepError::ElapsedExceeded { .. } => ToolCallErrorCode::ElapsedExceeded,
        _ => code,
    };
    // E09-2: truncation is not redaction. URL secrets and credential
    // assignments are stripped from the message first; only then is the
    // length bounded.
    let message = qcg_policy::redact_credential_assignments_in_text(
        &qcg_policy::redact_urls_in_text(&error.to_string()),
    );
    ToolCallError {
        code,
        message: utf8_head(&message, 2_048).to_string(),
    }
}

pub(crate) fn record_tool_call_failure(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    call: &ChatToolCall,
    error: &StepError,
    failure: ToolCallFailure<'_>,
) -> Result<(), StepError> {
    let event = tool_call_event(
        &node.id,
        failure.agent,
        None,
        call,
        &Value::Null,
        ToolCallEventOutcome {
            status: ToolCallStatus::Failed,
            phase: failure.phase,
            error: Some(tool_call_error(failure.code, error)),
            duration: failure.started.elapsed(),
        },
    )?;
    ctx.journal.event("tool_call", event).step_err(&node.id)
}

pub(crate) fn record_tool_call_failures(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    calls: &[ChatToolCall],
    error: &StepError,
    agent: Option<&str>,
    phase: ToolCallPhase,
    code: ToolCallErrorCode,
) -> Result<(), StepError> {
    for call in calls {
        record_tool_call_failure(
            ctx,
            node,
            call,
            error,
            tool_call_failure(agent, phase, code, Instant::now()),
        )?;
    }
    Ok(())
}

pub(crate) struct ToolCallFailure<'a> {
    agent: Option<&'a str>,
    phase: ToolCallPhase,
    code: ToolCallErrorCode,
    started: Instant,
}

pub(crate) fn tool_call_failure(
    agent: Option<&str>,
    phase: ToolCallPhase,
    code: ToolCallErrorCode,
    started: Instant,
) -> ToolCallFailure<'_> {
    ToolCallFailure {
        agent,
        phase,
        code,
        started,
    }
}

pub(crate) fn tool_reported_error(result: &Value) -> ToolCallError {
    let message = result
        .pointer("/error/message")
        .and_then(Value::as_str)
        .or_else(|| result.pointer("/content/0/text").and_then(Value::as_str))
        .unwrap_or("tool returned an error result");
    // Tool-reported text is untrusted and may echo URLs with secrets or
    // credential assignments; redact both shapes before journaling,
    // matching the gateway redaction (E09).
    let message = qcg_policy::redact_credential_assignments_in_text(
        &qcg_policy::redact_urls_in_text(message),
    );
    ToolCallError {
        code: ToolCallErrorCode::ToolReportedError,
        message: utf8_head(&message, 2_048).to_string(),
    }
}

pub(crate) fn bounded_event_value(value: &Value) -> Result<(Value, bool), serde_json::Error> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() <= TOOL_EVENT_VALUE_LIMIT_BYTES {
        Ok((value.clone(), false))
    } else {
        Ok((
            json!({
                "type": match value {
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                    Value::String(_) => "string",
                    Value::Bool(_) => "boolean",
                    Value::Number(_) => "number",
                    Value::Null => "null",
                },
                "bytes": bytes.len(),
                "truncated": true,
            }),
            true,
        ))
    }
}

/// Redacted args for Debug display (E09h). Mirrors the journal redaction
/// in [`tool_call_event`]: HTTP query values, credential headers, bodies,
/// and generic credential-like keys are redacted, so a Debug dump never
/// carries plaintext secrets. Execution always uses the raw args; only
/// this Debug copy is redacted.
#[allow(dead_code)]
pub(crate) fn redacted_tool_call_args_for_debug(call: &ChatToolCall) -> Value {
    let mut redacted =
        qcg_engine::redact_credential_values(&qcg_engine::redact_fs_write_args_for_journal(
            &qcg_engine::redact_http_args_for_journal(&call.args),
        ));
    if let Some(url) = redacted
        .get("url")
        .and_then(Value::as_str)
        .map(str::to_owned)
        && let Some(object) = redacted.as_object_mut()
    {
        object.insert(
            "url".into(),
            Value::String(qcg_policy::redact_all_query_values(&url)),
        );
    }
    redacted
}

/// Redacting Debug wrapper for [`ChatToolCall`] (E09h). The orphan rule
/// forbids implementing `Debug` for the foreign `qcg_llm` type itself, so
/// all log sites must format through this wrapper (or the field-by-field
/// redaction above): URL, header, body, and args values never reach logs
/// in plaintext. Direct `{:?}` on `ChatToolCall` must never be logged.
#[allow(dead_code)]
pub(crate) struct RedactedToolCall<'a>(pub &'a ChatToolCall);

impl std::fmt::Debug for RedactedToolCall<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChatToolCall")
            .field("id", &self.0.id)
            .field("name", &self.0.name)
            .field("args", &redacted_tool_call_args_for_debug(self.0))
            .finish()
    }
}

/// Redacting Debug wrapper for [`ChatMessage`] (E09h). Content is
/// credential/URL-redacted and length-bounded (shape plus hash beyond 256
/// chars); tool calls delegate to the redacted form above; provider state
/// shows shape only (count), never values. Direct `{:?}` on `ChatMessage`
/// must never be logged.
#[allow(dead_code)]
pub(crate) struct RedactedMessage<'a>(pub &'a ChatMessage);

impl std::fmt::Debug for RedactedMessage<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted_content = qcg_policy::redact_credential_assignments_in_text(
            &qcg_policy::redact_urls_in_text(&self.0.content),
        );
        let content = crate::prompting::truncate_with_digest(&redacted_content, 256);
        let tool_calls: Vec<RedactedToolCall<'_>> =
            self.0.tool_calls.iter().map(RedactedToolCall).collect();
        formatter
            .debug_struct("ChatMessage")
            .field("role", &self.0.role)
            .field("content", &content)
            .field("tool_calls", &tool_calls)
            .field("tool_call_id", &self.0.tool_call_id)
            .field(
                "provider_state",
                &self
                    .0
                    .provider_state
                    .as_ref()
                    .map(|state| format!("[{} items]", state.len())),
            )
            .finish()
    }
}

pub(crate) async fn execute_mcp_tool(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    mcp: &McpAgentTools,
    alias: &str,
    args: Value,
    call_id: &str,
) -> Result<AgentToolOutcome, StepError> {
    let mut input_responses = None;
    let mut request_state: Option<String> = None;
    // The alias-to-server mapping is resolved per execution; binding the
    // server into the key keeps a re-resolved alias from resuming a foreign
    // continuation. An undeclared alias fails here, never with a placeholder
    // server that would blur key ownership.
    let server = mcp
        .server_for(alias)
        .ok_or_else(|| StepError::failed(&node.id, format!("unknown MCP tool alias `{alias}`")))?;
    match resume_agent_mcp_continuation(ctx, node, server, alias, &args, call_id)? {
        ContinuationDecision::Fresh => {}
        ContinuationDecision::Resume {
            request_state: resumed_state,
            input_responses: resumed_responses,
        } => {
            // E07 guard verification on continuation resume: recompute the
            // canonical (redacted) args and compare against the stored
            // descriptor before resuming. A mismatch (tampered or
            // regenerated args reusing the same call id) refuses fail-closed
            // instead of resuming a foreign continuation. This duplicates
            // the lookup-time check in `decide_agent_mcp_continuation` as
            // defense in depth at the resume site itself.
            let pending_key = pending_key_for_agent_mcp(&node.id, server, alias, call_id, &args)?;
            let state = ctx.journal.state();
            let pending = state.mcp_pending.get(&pending_key).ok_or_else(|| {
                StepError::failed(
                    &node.id,
                    format!(
                        "MCP continuation `{pending_key}` vanished between lookup and resume; refusing resume"
                    ),
                )
            })?;
            if pending.get("arguments") != Some(&canonical_mcp_key_args(&args)) {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "MCP continuation `{pending_key}` holds different arguments; refusing resume"
                    ),
                ));
            }
            request_state = resumed_state;
            input_responses = resumed_responses;
        }
    }
    // Fresh invocations retire superseded pendings for the same node and
    // alias first: agent turns run one call at a time, so an unconsumed
    // pending under a different call id is definitionally abandoned by a
    // regenerated call, and leaving it would leak resume state forever.
    // Only fresh invocations guard: a resumption continues the
    // already-started operation under the same call id, and re-guarding
    // would misread its own Started record as a refusal (E07). The resumed
    // continuation key already binds node, server, alias, call id, and
    // arguments, so a foreign resume cannot reach this path.
    let fresh = request_state.is_none() && input_responses.is_none();
    if fresh {
        supersede_agent_mcp_continuations(ctx, node, server, alias, call_id, &args)?;
    }
    // Agent invocations identify by call id: the checkpoint re-issues the
    // exact suspended call on resume, so the recomputed id below matches
    // the suspend-time guard id without storing it.
    // E07d: the guard stores the canonical redacted form from the start
    // (single form everywhere) so a post-restart resume re-issuing the
    // checkpointed redacted copy recomputes the identical digest instead
    // of mismatching raw-vs-redacted and refusing.
    let operation_id = if fresh {
        let details = Some(canonical_mcp_key_args(&args));
        match ctx.run.guard_external_operation(
            ctx.journal,
            node,
            &format!("mcp:{alias}"),
            alias,
            &details,
            call_id,
        )? {
            qcg_engine::GuardDecision::Proceed { operation_id } => Some(operation_id),
            // Same invocation already succeeded: return the cached result
            // without touching the remote again.
            qcg_engine::GuardDecision::Resend { result, .. } => {
                return Ok(AgentToolOutcome::Result(result));
            }
        }
    } else {
        None
    };
    // Recompute for finish binding (guard returns the stable id). A
    // resumed invocation reuses the same call id, so the guard id is the
    // deterministic function of run, node, and call id below.
    let operation_id = operation_id.or_else(|| {
        Some(qcg_engine::operation_id_for(
            &ctx.run.run_id,
            &node.id,
            call_id,
        ))
    });
    // A post-restart resume re-issues the journaled (redacted) call copy:
    // executing it would send `[REDACTED]` placeholders as real arguments.
    // Refuse fail-closed instead; the caller re-issues with original bytes
    // (E07). In-process resumes carry the raw memory checkpoint and never
    // trip this gate.
    if !fresh && qcg_engine::contains_redaction_marker(&args.to_string()) {
        return Err(StepError::Refused {
            node: node.id.clone(),
            message: format!(
                "MCP call `{alias}` holds redacted arguments after a restart; refusing to execute placeholders remotely"
            ),
        });
    }
    for _round in 0..10 {
        // The node-scoped token: node timeout stops this wait without
        // touching the run, and parent cancellation propagates. The
        // session's older connect-time token cannot cover either.
        let call_result = mcp
            .call(
                node,
                alias,
                args.clone(),
                input_responses.take(),
                request_state.take(),
                &ctx.run.cancellation,
            )
            .await;
        let outcome = match call_result {
            Ok(outcome) => outcome,
            Err(error) => {
                // Transport failures may have applied remote effects:
                // indeterminate, never clean. Cancellation finishes
                // nothing and propagates without a completion record.
                if !matches!(error, StepError::Cancelled)
                    && let Some(operation_id) = &operation_id
                {
                    ctx.run.finish_external_operation_with_warn(
                        ctx.journal,
                        node,
                        operation_id,
                        qcg_engine::OperationOutcome::Indeterminate {
                            reason: format!("MCP call failed: {error}"),
                        },
                    );
                }
                return Err(error);
            }
        };
        match outcome {
            McpCallOutcome::Complete(value) => {
                // Consume the continuation first so a repeated identical
                // call starts a fresh remote request instead of resuming a
                // completed one.
                consume_agent_mcp_continuation(ctx, node, server, alias, call_id, &args)?;
                return Ok(match operation_id {
                    Some(operation_id) => AgentToolOutcome::OperationResult {
                        value,
                        operation_id,
                    },
                    None => AgentToolOutcome::Result(value),
                });
            }
            McpCallOutcome::InputRequired(required) => {
                let loop_state = required.request_state.clone();
                if required.input_requests.is_empty() {
                    request_state = loop_state;
                    tokio::task::yield_now().await;
                    continue;
                }
                let question_id = mcp_question_id(&node.id, alias, call_id, &args, &required)?;
                // Fail closed: only the full per-call question id is
                // honored. Legacy bare `node:alias` answers never complete
                // a new question (E08/Q1).
                let answer = ctx.run.answers.get(&question_id);
                let Some(answer) = answer else {
                    // Journaled MCP arguments never carry plaintext
                    // secrets (E09): the stored descriptor keeps the
                    // canonical (redacted) form only, and the pending key
                    // hashes that same form, so the resume-time
                    // recomputation from the checkpointed copy agrees
                    // (E07-5/E08-3). The live `args` keeps raw values for
                    // execution.
                    let journal_args = canonical_mcp_key_args(&args);
                    ctx.journal
                        .event(
                            "mcp_input_pending",
                            json!({
                                "node": node.id,
                                "pending_key": pending_key_for_agent_mcp(&node.id, server, alias, call_id, &args)?,
                                "question_id": question_id,
                                "server": server,
                                "alias": alias,
                                "call_id": call_id,
                                "arguments": journal_args,
                                "request_state": required.request_state,
                                "input_requests": required.input_requests,
                            }),
                        )
                        .step_err(&node.id)?;
                    return Ok(AgentToolOutcome::NeedsUser(mcp_form_spec(
                        question_id,
                        alias,
                        &required,
                    )?));
                };
                request_state = loop_state;
                input_responses = Some(mcp_input_responses(&required, answer)?);
            }
        }
    }
    Err(StepError::failed(
        &node.id,
        format!("MCP tool `{alias}` exceeded 10 input-required rounds"),
    ))
}

/// Canonical args for MCP continuation identity (E07-5/E08-3). The
/// pending key and the stored descriptor arguments both derive from this
/// form — never from raw secrets — so a resume re-issuing the checkpointed
/// (redacted) copy recomputes the identical key. Redaction is idempotent,
/// therefore canonicalizing the already-redacted resume args yields the
/// same bytes as canonicalizing the original suspension args. The live
/// remote call always uses the raw args; only identity and journal copies
/// use this form. This mirrors the checkpoint redaction in
/// `record_agent_checkpoint`, which must stay byte-identical.
pub(crate) fn canonical_mcp_key_args(args: &Value) -> Value {
    let redacted = qcg_engine::redact_credential_values(args);
    if redacted.get("url").and_then(Value::as_str).is_some() {
        qcg_engine::redact_http_args_for_journal(&redacted)
    } else {
        redacted
    }
}

/// Invocation-scoped continuation key binding run node, resolved server,
/// tool alias, model call id, and arguments. Repeated calls with identical
/// arguments get distinct continuations, and a re-resolved alias never
/// resumes a foreign server's continuation.
pub(crate) fn pending_key_for_agent_mcp(
    node_id: &str,
    server: &str,
    alias: &str,
    call_id: &str,
    args: &Value,
) -> Result<String, StepError> {
    // E07-5/E08-3: the key hashes the canonical (redacted) args, so the
    // suspension-time key and the resume-time recomputation agree even
    // though the journal never holds the original secrets.
    // Serialization of `Value` is infallible in practice; on theoretical
    // failure fail closed instead of hashing a shared marker that would
    // alias distinct failures onto one pending key (E07).
    let canonical = canonical_mcp_key_args(args);
    let bytes =
        serde_json::to_vec(&json!({"server": server, "call_id": call_id, "args": canonical}))
            .map_err(|error| {
                StepError::failed(
                    node_id,
                    format!("failed to serialize MCP pending key: {error}"),
                )
            })?;
    let invocation_hash = hex::encode(sha2::Sha256::digest(bytes));
    Ok(format!(
        "{node_id}:agentmcp:{alias}:{invocation_hash}#__mcp_pending"
    ))
}

/// Retires abandoned pendings for the same node and alias under a different
/// key. Only the current invocation key survives; each retired key journals
/// a consumed marker so a later restart never resumes it.
fn supersede_agent_mcp_continuations(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    server: &str,
    alias: &str,
    call_id: &str,
    args: &Value,
) -> Result<(), StepError> {
    let current = pending_key_for_agent_mcp(&node.id, server, alias, call_id, args)?;
    let state = ctx.journal.state();
    let stale: Vec<String> = state
        .mcp_pending
        .iter()
        .filter(|(key, pending)| {
            key.as_str() != current
                && pending.get("node").and_then(Value::as_str) == Some(node.id.as_str())
                && pending.get("server").and_then(Value::as_str) == Some(server)
                && pending.get("alias").and_then(Value::as_str) == Some(alias)
        })
        .map(|(key, _)| key.clone())
        .collect();
    for key in stale {
        ctx.journal
            .event(
                "mcp_continuation_consumed",
                json!({ "node": node.id, "pending_key": key }),
            )
            .step_err(&node.id)?;
    }
    Ok(())
}

/// Marks the invocation continuation consumed so later calls never resume
/// a completed remote request.
fn consume_agent_mcp_continuation(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    server: &str,
    alias: &str,
    call_id: &str,
    args: &Value,
) -> Result<(), StepError> {
    ctx.journal
        .event(
            "mcp_continuation_consumed",
            json!({
                "node": node.id,
                "pending_key": pending_key_for_agent_mcp(&node.id, server, alias, call_id, args)?,
            }),
        )
        .step_err(&node.id)
}

/// Resume decision for one MCP invocation: start fresh, continue a
/// suspended remote request, or refuse when a previous resume proves a
/// crash mid-remote-call with an indeterminate remote outcome.
enum ContinuationDecision {
    Fresh,
    Resume {
        request_state: Option<String>,
        input_responses: Option<BTreeMap<String, Value>>,
    },
}

/// Pure lookup half of [`resume_agent_mcp_continuation`]: decides from
/// folded state alone, without touching the journal, so the full decision
/// matrix is unit-testable without a step harness.
/// Debug is redacting (E09h): input responses may carry user answers.
#[derive(PartialEq)]
enum AgentResumeLookup {
    Absent,
    Resume {
        pending_key: String,
        question_id: String,
        request_state: Option<String>,
        input_responses: BTreeMap<String, Value>,
    },
}

impl std::fmt::Debug for AgentResumeLookup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => formatter.write_str("Absent"),
            Self::Resume {
                pending_key,
                question_id,
                request_state,
                input_responses,
            } => {
                use sha2::Digest as _;
                // Display-only hash (E07): never an identity key, so the
                // infallible-practice marker cannot alias a pending key.
                // The marker name differs from the identity-failure markers
                // on purpose, so even theoretical failures hash into a
                // disjoint bucket.
                let digest = hex::encode(sha2::Sha256::digest(
                    serde_json::to_vec(input_responses)
                        .unwrap_or_else(|_| Vec::from(b"{\"display_serialization_failed\":true}")),
                ));
                formatter
                    .debug_struct("Resume")
                    .field("pending_key", pending_key)
                    .field("question_id", question_id)
                    .field("request_state", &request_state.as_ref().map(|_| "[STATE]"))
                    .field("input_responses_sha256", &digest)
                    .finish()
            }
        }
    }
}

fn decide_agent_mcp_continuation(
    state: &qcg_engine::RunState,
    answers: &BTreeMap<String, Value>,
    node_id: &str,
    server: &str,
    alias: &str,
    args: &Value,
    call_id: &str,
) -> Result<AgentResumeLookup, String> {
    // Exact-key lookup in the typed journal continuation store. The key
    // embeds node, server, alias, call id, and the canonical (redacted)
    // arguments; consumed continuations are removed from the store on
    // completion, so cross-node reuse, repeated-call reuse, and
    // completed-request reuse are all impossible.
    let pending_key = pending_key_for_agent_mcp(node_id, server, alias, call_id, args)
        .map_err(|error| error.to_string())?;
    let Some(pending) = state.mcp_pending.get(&pending_key) else {
        return Ok(AgentResumeLookup::Absent);
    };
    for (field, expected) in [
        ("node", node_id),
        ("server", server),
        ("alias", alias),
        ("call_id", call_id),
    ] {
        if pending.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(format!(
                "MCP continuation `{pending_key}` does not match this invocation; refusing resume"
            ));
        }
    }
    // E07-5/E08-3: the descriptor holds the canonical (redacted) args, so
    // the live args are canonicalized before comparison. A resume
    // re-issuing the checkpointed copy compares equal; genuinely different
    // args still refuse.
    if pending.get("arguments") != Some(&canonical_mcp_key_args(args)) {
        return Err(format!(
            "MCP continuation `{pending_key}` holds different arguments; refusing resume"
        ));
    }
    // Every writer records the suspending question: a pending continuation
    // without one is corrupt, and starting fresh would duplicate the remote
    // call while orphaning this state. (An unanswered question below is the
    // legitimate waiting path, not corruption.)
    let Some(question_id) = pending.get("question_id").and_then(Value::as_str) else {
        return Err(format!(
            "MCP continuation `{pending_key}` has no question; refusing resume"
        ));
    };
    // The pending continuation is bound to its question: an answer for
    // a different question never resumes this request. Legacy bare keys
    // never resume (E08/Q1).
    let Some(answer) = answers.get(question_id) else {
        return Ok(AgentResumeLookup::Absent);
    };
    // A prior resume of this same pending question proves the remote call
    // was already re-entered once; seeing the pending again means a crash
    // mid-call with an unknowable remote outcome. Fail for human triage
    // instead of replaying blindly.
    if state
        .mcp_resumed
        .get(&pending_key)
        .is_some_and(|resumed| resumed == question_id)
    {
        return Err(format!(
            "MCP continuation `{pending_key}` was already resumed for question `{question_id}`; the remote result is indeterminate after interruption"
        ));
    }
    let request_state = pending
        .get("request_state")
        .and_then(Value::as_str)
        .map(str::to_string);
    // Fail closed on a missing descriptor field (G-1): a pending
    // continuation without `input_requests` is corrupt; defaulting to
    // empty would resume with no inputs instead of refusing.
    let Some(input_requests) = pending
        .get("input_requests")
        .and_then(Value::as_object)
        .cloned()
    else {
        return Err(format!(
            "MCP continuation `{pending_key}` has no input requests; refusing resume"
        ));
    };
    let values = answer
        .as_object()
        .ok_or_else(|| "MCP input-required answer must be an object".to_string())?;
    let mut sorted_ids: Vec<&String> = input_requests.keys().collect();
    sorted_ids.sort();
    let mut responses = BTreeMap::new();
    for (index, id) in sorted_ids.into_iter().enumerate() {
        let field = format!("response_{index}");
        let value = values
            .get(&field)
            .cloned()
            .ok_or_else(|| format!("MCP answer omitted `{field}`"))?;
        responses.insert(id.clone(), json!({ "action": "accept", "content": value }));
    }
    Ok(AgentResumeLookup::Resume {
        pending_key,
        question_id: question_id.to_string(),
        request_state,
        input_responses: responses,
    })
}

fn resume_agent_mcp_continuation(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    server: &str,
    alias: &str,
    args: &Value,
    call_id: &str,
) -> Result<ContinuationDecision, StepError> {
    let state = ctx.journal.state();
    let lookup = decide_agent_mcp_continuation(
        &state,
        &ctx.run.answers,
        &node.id,
        server,
        alias,
        args,
        call_id,
    )
    .map_err(|message| StepError::failed(&node.id, message))?;
    let AgentResumeLookup::Resume {
        pending_key,
        question_id,
        request_state,
        input_responses,
    } = lookup
    else {
        return Ok(ContinuationDecision::Fresh);
    };
    // Record the resume before touching the remote so a crash from here on
    // is distinguishable from a clean suspension.
    ctx.journal
        .event(
            "mcp_continuation_resumed",
            json!({
                "node": node.id,
                "pending_key": pending_key,
                "question_id": question_id,
            }),
        )
        .step_err(&node.id)?;
    Ok(ContinuationDecision::Resume {
        request_state,
        input_responses: Some(input_responses),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcg_engine::RunState;

    fn pending(
        node: &str,
        server: &str,
        alias: &str,
        call_id: &str,
        args: Value,
        question_id: Option<&str>,
    ) -> (String, Value) {
        let key = pending_key_for_agent_mcp(node, server, alias, call_id, &args)
            .expect("pending key serialization is infallible in tests");
        let mut descriptor = json!({
            "node": node,
            "server": server,
            "alias": alias,
            "call_id": call_id,
            "arguments": args,
            "request_state": "state-1",
            "input_requests": {
                "only": {"method": "elicitation/create"},
            },
        });
        if let Some(question_id) = question_id {
            descriptor["question_id"] = json!(question_id);
        }
        (key, descriptor)
    }

    fn state_with(key: &str, descriptor: Value) -> RunState {
        let mut state = RunState::default();
        state.mcp_pending.insert(key.to_string(), descriptor);
        state
    }

    fn decide(
        state: &RunState,
        answers: &BTreeMap<String, Value>,
        node: &str,
        server: &str,
        alias: &str,
        args: &Value,
        call_id: &str,
    ) -> Result<AgentResumeLookup, String> {
        decide_agent_mcp_continuation(state, answers, node, server, alias, args, call_id)
    }

    #[test]
    fn pending_keys_differ_by_call_id() {
        // E08: the continuation key names the invocation, so two model
        // calls with identical arguments never share a pending.
        let args = json!({"q": "x"});
        let (first, _) = pending("node", "server", "alias", "call-1", args.clone(), None);
        let (second, _) = pending("node", "server", "alias", "call-2", args, None);
        assert_ne!(first, second);
    }

    #[test]
    fn redacted_resume_recomputes_the_same_key() {
        // E07-5/E08-3: suspension hashes the canonical (redacted) args, so
        // a resume re-issuing the checkpointed copy — whose secrets are
        // already placeholders — recomputes the identical key instead of
        // losing the answer to a forked identity.
        let original = json!({"query": "x", "api_key": "s3cret"});
        let redacted_copy = canonical_mcp_key_args(&original);
        assert_ne!(
            redacted_copy, original,
            "the canonical form must not carry the secret"
        );
        assert!(
            !redacted_copy.to_string().contains("s3cret"),
            "no plaintext secret in the canonical form: {redacted_copy}"
        );
        let first = pending_key_for_agent_mcp("node", "server", "alias", "call-1", &original)
            .expect("pending key serialization is infallible in tests");
        let second = pending_key_for_agent_mcp("node", "server", "alias", "call-1", &redacted_copy)
            .expect("pending key serialization is infallible in tests");
        assert_eq!(
            first, second,
            "suspension and redacted reissue must share one key"
        );
        // The stored descriptor (canonical form) matches the reissued
        // copy, so the answered continuation resumes.
        let key = first;
        let descriptor = json!({
            "node": "node",
            "server": "server",
            "alias": "alias",
            "call_id": "call-1",
            "arguments": redacted_copy,
            "question_id": "q-9",
            "request_state": "state-1",
            "input_requests": {
                "only": {"method": "elicitation/create"},
            },
        });
        let state = state_with(&key, descriptor);
        let answers = BTreeMap::from([("q-9".to_string(), json!({"response_0": "go"}))]);
        assert!(
            matches!(
                decide(
                    &state,
                    &answers,
                    "node",
                    "server",
                    "alias",
                    &redacted_copy,
                    "call-1"
                ),
                Ok(AgentResumeLookup::Resume { .. })
            ),
            "the redacted reissue must resume its own answered continuation"
        );
    }

    #[test]
    fn bare_answer_keys_are_refused() {
        // Backward compatibility is not preserved: a legacy bare
        // `node:alias` answer never resumes a suspension (E08/Q1).
        let args = json!({"q": "x"});
        let (key, descriptor) = pending(
            "node",
            "server",
            "alias",
            "call-1",
            args.clone(),
            Some("node:mcp:alias:deadbeef"),
        );
        let state = state_with(&key, descriptor);
        let legacy = BTreeMap::from([("node:alias".to_string(), json!({"response_0": "go"}))]);
        assert!(
            matches!(
                decide(&state, &legacy, "node", "server", "alias", &args, "call-1"),
                Ok(AgentResumeLookup::Absent)
            ),
            "legacy answer must not resume"
        );
        let both = BTreeMap::from([
            ("node:alias".to_string(), json!({"response_0": "legacy"})),
            (
                "node:mcp:alias:deadbeef".to_string(),
                json!({"response_0": "fresh"}),
            ),
        ]);
        match decide(&state, &both, "node", "server", "alias", &args, "call-1") {
            Ok(AgentResumeLookup::Resume {
                input_responses, ..
            }) => {
                assert_eq!(
                    input_responses
                        .get("only")
                        .and_then(|response| response.get("content"))
                        .cloned(),
                    Some(json!("fresh")),
                    "the new-style answer must win when present"
                );
            }
            other => panic!("expected resume, got {other:?}"),
        }
    }

    #[test]
    fn answered_agent_continuation_resumes() {
        let args = json!({"q": "x"});
        let (key, descriptor) = pending(
            "node",
            "server",
            "alias",
            "call-1",
            args.clone(),
            Some("q-1"),
        );
        let state = state_with(&key, descriptor);
        let answers = BTreeMap::from([("q-1".to_string(), json!({"response_0": "go"}))]);
        match decide(&state, &answers, "node", "server", "alias", &args, "call-1") {
            Ok(AgentResumeLookup::Resume {
                pending_key,
                question_id,
                request_state,
                input_responses,
            }) => {
                assert_eq!(pending_key, key);
                assert_eq!(question_id, "q-1");
                assert_eq!(request_state.as_deref(), Some("state-1"));
                assert_eq!(
                    input_responses
                        .get("only")
                        .and_then(|response| response.get("content"))
                        .cloned(),
                    Some(json!("go"))
                );
            }
            other => panic!("expected resume, got {other:?}"),
        }
    }

    #[test]
    fn regenerated_call_id_misses_instead_of_resuming_foreign_state() {
        // The model regenerating a call mints a fresh call id, which keys a
        // different continuation: the old pending is left for supersede
        // retirement, never resumed under the new id.
        let args = json!({"q": "x"});
        let (key, descriptor) = pending(
            "node",
            "server",
            "alias",
            "call-1",
            args.clone(),
            Some("q-1"),
        );
        let state = state_with(&key, descriptor);
        assert_eq!(
            decide(
                &state,
                &BTreeMap::new(),
                "node",
                "server",
                "alias",
                &args,
                "call-2"
            ),
            Ok(AgentResumeLookup::Absent)
        );
    }

    #[test]
    fn single_shot_approval_never_authorizes_an_agent_call() {
        // Q1/E08: single-shot `mcp.call` approvals bind
        // `execution:<node>:<count>` while agent continuations bind
        // `<node>:agentmcp:<alias>:<hash>#__mcp_pending`: the namespaces
        // never intersect, so one cannot authorize the other.
        let args = json!({"q": "x"});
        let agent_key = pending_key_for_agent_mcp("node", "server", "alias", "call-1", &args)
            .expect("pending key serialization is infallible in tests");
        assert!(
            agent_key.contains(":agentmcp:") && agent_key.ends_with("#__mcp_pending"),
            "agent continuation keys must carry their namespace: {agent_key}"
        );
        assert!(
            !agent_key.starts_with("execution:"),
            "agent keys must never collide with single-shot execution ids: {agent_key}"
        );
        // A re-resolved server yields a different key: the old approval
        // cannot authorize the new server's call.
        let other = pending_key_for_agent_mcp("node", "server-2", "alias", "call-1", &args)
            .expect("pending key serialization is infallible in tests");
        assert_ne!(agent_key, other, "server must separate continuations");
    }

    #[test]
    fn cross_node_agent_continuation_misses() {
        let args = json!({"q": "x"});
        let (key, descriptor) = pending(
            "node",
            "server",
            "alias",
            "call-1",
            args.clone(),
            Some("q-1"),
        );
        let state = state_with(&key, descriptor);
        assert_eq!(
            decide(
                &state,
                &BTreeMap::new(),
                "other-node",
                "server",
                "alias",
                &args,
                "call-1"
            ),
            Ok(AgentResumeLookup::Absent)
        );
    }

    #[test]
    fn corrupt_or_replayed_agent_continuation_is_refused() {
        let args = json!({"q": "x"});
        let (key, descriptor) = pending("node", "server", "alias", "call-1", args.clone(), None);
        let state = state_with(&key, descriptor);
        assert!(
            decide(
                &state,
                &BTreeMap::new(),
                "node",
                "server",
                "alias",
                &args,
                "call-1"
            )
            .is_err()
        );
        let (key, descriptor) = pending(
            "node",
            "server",
            "alias",
            "call-1",
            args.clone(),
            Some("q-1"),
        );
        let mut state = state_with(&key, descriptor);
        state.mcp_resumed.insert(key.clone(), "q-1".into());
        let answers = BTreeMap::from([("q-1".to_string(), json!({"response_0": "go"}))]);
        assert!(decide(&state, &answers, "node", "server", "alias", &args, "call-1").is_err());
    }

    #[test]
    fn tool_call_event_redacts_secrets_from_journal() {
        // E09/E09-1: the journaled tool_call arguments must not contain
        // plaintext query secrets or credential headers — including
        // undeclared query values, which are redacted by default.
        use qcg_llm::ChatToolCall;
        let call = ChatToolCall {
            id: "call-1".into(),
            name: "fetch".into(),
            args: json!({
                "url": "https://example.test/search?q=x&api_key=s3cret",
                "headers": {"Authorization": "Bearer s3cret"},
                "sensitive_query": ["api_key"],
            }),
        };
        let event = tool_call_event(
            "node",
            None,
            None,
            &call,
            &json!({"ok": true}),
            tool_call_outcome(
                qcg_api::ToolCallStatus::Succeeded,
                qcg_api::ToolCallPhase::Completed,
                None,
                Instant::now(),
            ),
        )
        .expect("event should build");
        let event_str = event.to_string();
        assert!(
            !event_str.contains("s3cret"),
            "journaled tool_call must not leak the secret: {event_str}"
        );
        assert!(
            !event_str.contains("q=x"),
            "undeclared query values must be redacted by default: {event_str}"
        );
    }

    #[test]
    fn tool_call_error_strips_query_values_before_truncating() {
        // E09-2: truncation is not redaction — credentials and query values
        // are stripped from error strings first.
        let error = tool_call_error(
            ToolCallErrorCode::ExecutionFailed,
            &qcg_engine::StepError::Failed {
                node: "n".into(),
                message: "tool `fetch` url `https://example.test/search?q=x&api_key=s3cret` is outside declared hosts".into(),
            },
        );
        assert!(
            !error.message.contains("s3cret"),
            "error text must not echo the secret: {}",
            error.message
        );
        assert!(
            error.message.contains("api_key="),
            "parameter names stay visible: {}",
            error.message
        );
    }

    #[test]
    fn tool_call_error_preserves_timeout_and_elapsed() {
        // E11: timeout and elapsed classifications must survive the tool
        // event mapping instead of collapsing to ExecutionFailed.
        let timed_out = tool_call_error(
            ToolCallErrorCode::ExecutionFailed,
            &qcg_engine::StepError::TimedOut {
                node: "n".into(),
                timeout_secs: 1,
            },
        );
        assert_eq!(timed_out.code, ToolCallErrorCode::TimedOut);
        let elapsed = tool_call_error(
            ToolCallErrorCode::ExecutionFailed,
            &qcg_engine::StepError::ElapsedExceeded {
                node: "n".into(),
                limit_secs: 1,
            },
        );
        assert_eq!(elapsed.code, ToolCallErrorCode::ElapsedExceeded);
    }

    #[test]
    fn redacting_debug_covers_url_header_body_and_args() {
        // E09h: args-carrying types must never reach logs in plaintext.
        // The orphan rule forbids a direct `Debug` impl on the foreign
        // `qcg_llm` types, so this pins the redacting wrappers that all log
        // sites must use. Sentinel secrets cover URL query values, credential
        // headers, bodies, and generic credential-like args.
        use qcg_llm::{ChatMessage, ChatToolCall};
        let call = ChatToolCall {
            id: "call-sentinel".into(),
            name: "fetch-sentinel".into(),
            args: json!({
                "url": "https://example.test/search?q=SENTINEL_URL_SECRET&api_key=SENTINEL_KEY_SECRET",
                "headers": {
                    "Authorization": "Bearer SENTINEL_HEADER_SECRET",
                    "X-Tenant": "alpha"
                },
                "body": "token=SENTINEL_BODY_SECRET",
                "api_key": "SENTINEL_ARGS_SECRET",
            }),
        };
        let debug = format!("{:?}", super::RedactedToolCall(&call));
        for sentinel in [
            "SENTINEL_URL_SECRET",
            "SENTINEL_KEY_SECRET",
            "SENTINEL_HEADER_SECRET",
            "SENTINEL_BODY_SECRET",
            "SENTINEL_ARGS_SECRET",
        ] {
            assert!(
                !debug.contains(sentinel),
                "redacted tool-call Debug must not leak {sentinel}: {debug}"
            );
        }
        assert!(debug.contains("fetch-sentinel"), "{debug}");
        assert!(debug.contains("call-sentinel"), "{debug}");
        assert!(debug.contains("api_key="), "keys stay visible: {debug}");
        let message = ChatMessage::assistant_tool_calls("hello".to_string(), vec![call]);
        let debug = format!("{:?}", super::RedactedMessage(&message));
        assert!(
            !debug.contains("SENTINEL"),
            "redacted message Debug must not leak sentinels: {debug}"
        );
        // Content secrets are redacted too, with shape preserved.
        let secret_message = ChatMessage::text(
            "user",
            "deploy with api_key=SENTINEL_CONTENT_SECRET at https://example.test/?token=SENTINEL_URL2",
        );
        let debug = format!("{:?}", super::RedactedMessage(&secret_message));
        assert!(!debug.contains("SENTINEL_CONTENT_SECRET"), "{debug}");
        assert!(!debug.contains("SENTINEL_URL2"), "{debug}");
        assert!(debug.contains("api_key="), "keys stay visible: {debug}");
    }

    #[test]
    fn approval_path_production_code_uses_no_debug_formatting() {
        // E09h: the approval-path files must never Debug-format values in
        // production code: a derived or ad-hoc `{:?}` on an args-carrying
        // type would log secrets in plaintext. The redacting wrappers above
        // are the only sanctioned Debug path. This scans the production
        // part (before `#[cfg(test)]`) of each approval-path file for `:?}`
        // outside comments; a hit fails loudly so the author either uses
        // the wrappers or documents why the formatted type is secret-free.
        // Covers the agent approval path plus the engine gateway command
        // planning path (`gateway/command.rs`, yours — verify + pin). The
        // engine HTTP gateway (`gateway/http.rs`) Debug derives are FOREIGN
        // (not yours): reported separately, never edited here.
        for file in [
            "agent.rs",
            "agent_runtime.rs",
            "tool_events.rs",
            "gateway/command.rs",
        ] {
            let source = match file {
                "agent.rs" => include_str!("agent.rs"),
                "agent_runtime.rs" => include_str!("agent_runtime.rs"),
                "gateway/command.rs" => include_str!("../../qcg-engine/src/gateway/command.rs"),
                _ => include_str!("tool_events.rs"),
            };
            let production = source.split("#[cfg(test)]").next().unwrap_or(source);
            for (index, line) in production.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                assert!(
                    !line.contains(":?}"),
                    "approval-path production code must not Debug-format (use the Redacted wrappers): {file}:{}: {line}",
                    index + 1,
                );
            }
        }
    }
}
