use qcg_api::{ToolCallError, ToolCallErrorCode, ToolCallEventData, ToolCallPhase, ToolCallStatus};
use qcg_contract::NodeDef;
use qcg_engine::{ResultExt, RunContext, StepContext, StepError, tool_call_sources};
use qcg_llm::ChatToolCall;
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
    let (arguments, arguments_truncated) = bounded_event_value(&call.args)?;
    let (result, result_truncated) = bounded_event_value(result)?;
    let data = ToolCallEventData {
        server: server.map(str::to_owned),
        tool: call.name.clone(),
        id: call.id.clone(),
        status: outcome.status,
        phase: outcome.phase,
        agent: agent.map(str::to_owned),
        error: outcome.error,
        duration_ms: u64::try_from(outcome.duration.as_millis()).unwrap_or(u64::MAX),
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
        _ => code,
    };
    ToolCallError {
        code,
        message: utf8_head(&error.to_string(), 2_048).to_string(),
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
    ToolCallError {
        code: ToolCallErrorCode::ToolReportedError,
        message: utf8_head(message, 2_048).to_string(),
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
            request_state = resumed_state;
            input_responses = resumed_responses;
        }
    }
    // Fresh invocations retire superseded pendings for the same node and
    // alias first: agent turns run one call at a time, so an unconsumed
    // pending under a different call id is definitionally abandoned by a
    // regenerated call, and leaving it would leak resume state forever.
    // Then guard the remote operation; resumptions continue the
    // already-started operation instead of re-guarding it.
    let fresh = request_state.is_none() && input_responses.is_none();
    if fresh {
        supersede_agent_mcp_continuations(ctx, node, server, alias, call_id, &args)?;
    }
    // Agent invocations identify by call id: the checkpoint re-issues the
    // exact suspended call on resume, so the recomputed id below matches
    // the suspend-time guard id without storing it.
    let operation_id = if fresh {
        let details = Some(args.clone());
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
    // Recompute for finish binding (guard returns the stable id).
    let operation_id = match operation_id {
        Some(id) => Some(id),
        None => {
            let details = Some(args.clone());
            let digest = RunContext::operation_digest(alias, &details)?;
            Some(qcg_engine::operation_id_for(
                &ctx.run.run_id,
                &node.id,
                &digest,
                call_id,
            ))
        }
    };
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
                if let Some(operation_id) = &operation_id {
                    ctx.run.finish_external_operation(
                        ctx.journal,
                        node,
                        operation_id,
                        Some(value.clone()),
                    )?;
                }
                return Ok(AgentToolOutcome::Result(value));
            }
            McpCallOutcome::InputRequired(required) => {
                let loop_state = required.request_state.clone();
                if required.input_requests.is_empty() {
                    request_state = loop_state;
                    tokio::task::yield_now().await;
                    continue;
                }
                let question_id = mcp_question_id(&node.id, alias, call_id, &args, &required);
                let Some(answer) = ctx.run.answers.get(&question_id) else {
                    ctx.journal
                        .event(
                            "mcp_input_pending",
                            json!({
                                "node": node.id,
                                "pending_key": pending_key_for_agent_mcp(&node.id, server, alias, call_id, &args),
                                "question_id": question_id,
                                "server": server,
                                "alias": alias,
                                "call_id": call_id,
                                "arguments": args,
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

/// Invocation-scoped continuation key binding run node, resolved server,
/// tool alias, model call id, and arguments. Repeated calls with identical
/// arguments get distinct continuations, and a re-resolved alias never
/// resumes a foreign server's continuation.
fn pending_key_for_agent_mcp(
    node_id: &str,
    server: &str,
    alias: &str,
    call_id: &str,
    args: &Value,
) -> String {
    let invocation_hash = &hex::encode(sha2::Sha256::digest(
        serde_json::to_vec(&json!({"server": server, "call_id": call_id, "args": args}))
            .unwrap_or_default(),
    ))[..16];
    format!("{node_id}:agentmcp:{alias}:{invocation_hash}#__mcp_pending")
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
    let current = pending_key_for_agent_mcp(&node.id, server, alias, call_id, args);
    let state = ctx.journal.state();
    let stale: Vec<String> = state
        .mcp_pending
        .iter()
        .filter(|(key, pending)| {
            key.as_str() != current
                && pending.get("node").and_then(Value::as_str) == Some(node.id.as_str())
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
                "pending_key": pending_key_for_agent_mcp(&node.id, server, alias, call_id, args),
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
#[derive(Debug, PartialEq)]
enum AgentResumeLookup {
    Absent,
    Resume {
        pending_key: String,
        question_id: String,
        request_state: Option<String>,
        input_responses: BTreeMap<String, Value>,
    },
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
    // embeds node, server, alias, call id, and arguments; consumed
    // continuations are removed from the store on completion, so cross-node
    // reuse, repeated-call reuse, and completed-request reuse are all
    // impossible.
    let pending_key = pending_key_for_agent_mcp(node_id, server, alias, call_id, args);
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
    if pending.get("arguments") != Some(args) {
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
    // a different question never resumes this request.
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
    let input_requests = pending
        .get("input_requests")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
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
        let key = pending_key_for_agent_mcp(node, server, alias, call_id, &args);
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
}
