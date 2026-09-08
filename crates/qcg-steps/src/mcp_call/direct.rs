use qcg_api::{FormSpec, ToolCallErrorCode, ToolCallPhase, ToolCallStatus};
use qcg_api::{ToolCallError, ToolCallEventData, ToolCallSource};
use qcg_contract::InputField;
use qcg_contract::{FieldType, NodeDef};
use qcg_engine::{ResultExt, RunContext, StepContext, StepError, tool_call_sources};
use qcg_mcp::{McpCallOutcome, McpError, McpInputRequired, McpSession};
use qcg_policy::validate_bounded_json_schema;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::validate::DirectMcpCallOutcome;

pub(crate) async fn execute_direct_mcp_call(
    session: &McpSession,
    tool_name: &str,
    arguments: Value,
    ctx: &StepContext<'_>,
    node: &NodeDef,
    timeout_seconds: u64,
    result_limit_bytes: usize,
) -> Result<DirectMcpCallOutcome, StepError> {
    let tools = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_seconds),
        session.list_tools(),
    )
    .await
    .map_err(|_| StepError::failed(&node.id, "MCP tools/list timed out"))?
    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let tool = tools
        .iter()
        .find(|tool| tool.name == tool_name)
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!(
                    "MCP server {} does not expose tool {}",
                    session.server_id(),
                    tool_name
                ),
            )
        })?;
    validate_bounded_json_schema(&tool.input_schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("MCP tool {tool_name} input schema is invalid or unsafe: {error}"),
        )
    })?;
    let input_validator = jsonschema::validator_for(&tool.input_schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("MCP tool {} input schema is invalid: {error}", tool_name),
        )
    })?;
    if let Err(error) = input_validator.validate(&arguments) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "MCP tool {} arguments failed input schema at {}: {error}",
                tool_name,
                error.instance_path()
            ),
        ));
    }
    let output_validator = tool
        .output_schema
        .as_ref()
        .map(|schema| {
            validate_bounded_json_schema(schema).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("MCP tool {tool_name} output schema is invalid or unsafe: {error}"),
                )
            })?;
            jsonschema::validator_for(schema).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("MCP tool {tool_name} output schema is invalid: {error}"),
                )
            })
        })
        .transpose()?;
    let mut input_responses = None;
    let mut request_state: Option<String> = None;
    // Resume a previously interrupted input-required round instead of
    // starting a new remote request. The typed journal continuation store
    // carries the pending descriptor, so a HITL restart continues the same
    // remote call with its original request_state.
    match resume_direct_mcp_continuation(ctx, node, session.server_id(), tool_name, &arguments)? {
        ContinuationDecision::Fresh => {}
        ContinuationDecision::Resume {
            request_state: resumed_state,
            input_responses: resumed_responses,
        } => {
            request_state = resumed_state;
            input_responses = resumed_responses;
        }
    }
    let fresh = request_state.is_none() && input_responses.is_none();
    let details = Some(arguments.clone());
    let operation_id = if fresh {
        Some(ctx.run.guard_external_operation(
            ctx.journal,
            node,
            &format!("mcp:{}/{}", session.server_id(), tool_name),
            &format!("{}/{}", session.server_id(), tool_name),
            &details,
        )?)
    } else {
        None
    };
    let operation_id = match operation_id {
        Some(id) => Some(id),
        None => {
            let digest = RunContext::operation_digest(
                &format!("{}/{}", session.server_id(), tool_name),
                &details,
            )?;
            Some(qcg_engine::operation_id_for(
                &ctx.run.run_id,
                &node.id,
                &digest,
            ))
        }
    };
    for _ in 0..10 {
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(timeout_seconds),
            session.call_tool_with_input(
                tool_name,
                arguments.clone(),
                input_responses.take(),
                request_state.take(),
            ),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                if let Some(operation_id) = &operation_id {
                    let _ = ctx.run.finish_external_operation_with_status(
                        ctx.journal,
                        node,
                        operation_id,
                        "error",
                    );
                }
                return Err(StepError::failed(&node.id, "MCP tools/call timed out"));
            }
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(McpError::ToolFailed { result, .. }) => McpCallOutcome::Complete(result),
            Err(error) => {
                if let Some(operation_id) = &operation_id {
                    let _ = ctx.run.finish_external_operation_with_status(
                        ctx.journal,
                        node,
                        operation_id,
                        "error",
                    );
                }
                return Err(StepError::failed(&node.id, error.to_string()));
            }
        };
        match outcome {
            McpCallOutcome::Complete(value) => {
                let fail = |node: &NodeDef, message: String| {
                    if let Some(operation_id) = &operation_id {
                        let _ = ctx.run.finish_external_operation_with_status(
                            ctx.journal,
                            node,
                            operation_id,
                            "error",
                        );
                    }
                    StepError::failed(&node.id, message)
                };
                let size = serde_json::to_vec(&value)?.len();
                if size > result_limit_bytes {
                    return Err(fail(
                        node,
                        format!(
                            "MCP tool {} result exceeded {result_limit_bytes} bytes",
                            tool_name
                        ),
                    ));
                }
                if !value
                    .get("isError")
                    .is_some_and(|is_error| is_error == &Value::Bool(true))
                    && let Some(validator) = &output_validator
                {
                    let structured = value
                        .get("structuredContent")
                        .or_else(|| value.get("structured_content"))
                        .ok_or_else(|| {
                            fail(
                                node,
                                format!(
                                    "MCP tool {tool_name} declared outputSchema but omitted structuredContent"
                                ),
                            )
                        })?;
                    if let Err(error) = validator.validate(structured) {
                        return Err(fail(
                            node,
                            format!(
                                "MCP tool {tool_name} result failed output schema at {}: {error}",
                                error.instance_path()
                            ),
                        ));
                    }
                }
                consume_direct_mcp_continuation(
                    ctx,
                    node,
                    session.server_id(),
                    tool_name,
                    &arguments,
                )?;
                if let Some(operation_id) = &operation_id {
                    ctx.run
                        .finish_external_operation(ctx.journal, node, operation_id)?;
                }
                return Ok(DirectMcpCallOutcome::Complete(value));
            }
            McpCallOutcome::InputRequired(required) => {
                // Preserve current state for the next loop round so an empty
                // follow-up (server awaiting only) continues correctly.
                let loop_state = required.request_state.clone();
                if required.input_requests.is_empty() {
                    request_state = loop_state;
                    continue;
                }
                let question_id = direct_mcp_question_id(
                    &node.id,
                    session.server_id(),
                    tool_name,
                    &arguments,
                    &required,
                );
                let Some(answer) = ctx.run.answers.get(&question_id) else {
                    // Durably record the continuation before suspending so a
                    // restart resumes this remote call instead of starting a
                    // new one with request_state=None.
                    let cont_key =
                        mcp_continuation_key(&node.id, session.server_id(), tool_name, &arguments);
                    ctx.journal
                        .event(
                            "mcp_input_pending",
                            json!({
                                "node": node.id,
                                "pending_key": mcp_pending_reserved_key(&cont_key),
                                "question_id": question_id,
                                "server": session.server_id(),
                                "tool": tool_name,
                                "arguments": arguments,
                                "request_state": required.request_state,
                                "input_requests": required.input_requests,
                            }),
                        )
                        .step_err(&node.id)?;
                    return Ok(DirectMcpCallOutcome::NeedsUser(direct_mcp_form_spec(
                        question_id,
                        &format!("{}/{}", session.server_id(), tool_name),
                        &required,
                    )?));
                };
                request_state = loop_state;
                input_responses = Some(direct_mcp_input_responses(&required, answer, node)?);
            }
        }
    }
    Err(StepError::failed(
        &node.id,
        "MCP tool exceeded 10 input-required rounds",
    ))
}

pub(crate) fn validate_direct_mcp_result(
    node: &NodeDef,
    server: &str,
    tool: &str,
    output_schema: Option<&Value>,
    result: Value,
) -> Result<Value, StepError> {
    let object = result.as_object().ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!("MCP tool {server}/{tool} result is not an object"),
        )
    })?;
    if object
        .get("isError")
        .is_some_and(|value| value != &Value::Bool(false))
    {
        return Err(StepError::failed(
            &node.id,
            format!("MCP tool {server}/{tool} returned an error result"),
        ));
    }
    if let Some(schema) = output_schema {
        let value = object
            .get("structuredContent")
            .or_else(|| object.get("structured_content"))
            .unwrap_or(&result);
        validate_bounded_json_schema(schema).map_err(|error| {
            StepError::failed(&node.id, format!("invalid mcp.call output_schema: {error}"))
        })?;
        let validator = jsonschema::validator_for(schema).map_err(|error| {
            StepError::failed(&node.id, format!("invalid mcp.call output_schema: {error}"))
        })?;
        if let Err(error) = validator.validate(value) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "MCP tool {server}/{tool} output failed output_schema at {}: {error}",
                    error.instance_path()
                ),
            ));
        }
    }
    Ok(result)
}

fn direct_mcp_question_id(
    node_id: &str,
    server: &str,
    tool: &str,
    arguments: &Value,
    required: &McpInputRequired,
) -> String {
    let mut requests = required
        .input_requests
        .values()
        .map(|request| serde_json::to_vec(request).unwrap_or_default())
        .collect::<Vec<_>>();
    requests.sort();
    let bytes = serde_json::to_vec(&json!({
        "node": node_id,
        "server": server,
        "tool": tool,
        "arguments": arguments,
        "requests": requests,
    }))
    .unwrap_or_default();
    let digest = hex::encode(Sha256::digest(bytes));
    format!("{node_id}:mcp:{server}/{tool}:{}", &digest[..16])
}

fn direct_mcp_form_spec(
    question_id: String,
    alias: &str,
    required: &McpInputRequired,
) -> Result<FormSpec, StepError> {
    let mut fields = Vec::with_capacity(required.input_requests.len());
    for (index, (request_id, request)) in direct_mcp_requests(required).into_iter().enumerate() {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method != "elicitation/create" {
            return Err(StepError::failed(
                alias,
                format!("MCP input request `{request_id}` uses unsupported method `{method}`"),
            ));
        }
        let params = request.get("params").unwrap_or(request);
        let message = params
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("MCP tool requested structured input");
        let label = params
            .get("url")
            .and_then(Value::as_str)
            .map_or_else(|| message.to_string(), |url| format!("{message} ({url})"));
        fields.push(InputField {
            id: format!("response_{index}"),
            label: Some(label),
            label_i18n: Default::default(),
            description: Some(message.to_string()),
            description_i18n: Default::default(),
            placeholder: None,
            placeholder_i18n: Default::default(),
            kind: FieldType::Json,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: Default::default(),
            min_items: None,
            item_type: None,
            schema: params.get("requestedSchema").cloned(),
            ui: Default::default(),
        });
    }
    Ok(FormSpec {
        id: question_id,
        title: format!("MCP tool `{alias}` requires input"),
        title_i18n: Default::default(),
        fields,
    })
}

fn direct_mcp_input_responses(
    required: &McpInputRequired,
    answer: &Value,
    node: &NodeDef,
) -> Result<BTreeMap<String, Value>, StepError> {
    let values = answer.as_object().ok_or_else(|| {
        StepError::failed(&node.id, "MCP input-required answer must be an object")
    })?;
    let responses = direct_mcp_requests(required)
        .into_iter()
        .enumerate()
        .map(|(index, (id, _request))| {
            let field = format!("response_{index}");
            let value = values.get(&field).cloned().ok_or_else(|| {
                StepError::failed(&node.id, format!("MCP answer omitted {field}"))
            })?;
            Ok((id.clone(), json!({ "action": "accept", "content": value })))
        })
        .collect::<Result<BTreeMap<_, _>, StepError>>()?;
    Ok(responses)
}

fn mcp_continuation_key(node_id: &str, server: &str, tool: &str, arguments: &Value) -> String {
    let bytes = serde_json::to_vec(&json!({
        "node": node_id,
        "server": server,
        "tool": tool,
        "arguments": arguments,
    }))
    .unwrap_or_default();
    let digest = hex::encode(Sha256::digest(bytes));
    format!("{node_id}:mcpcont:{server}/{tool}:{}", &digest[..16])
}

fn mcp_pending_reserved_key(continuation_key: &str) -> String {
    format!("{continuation_key}#__mcp_pending")
}

/// Resume decision for one direct MCP call: start fresh, continue a
/// suspended remote request, or refuse when a previous resume proves a
/// crash mid-remote-call with an indeterminate remote outcome.
enum ContinuationDecision {
    Fresh,
    Resume {
        request_state: Option<String>,
        input_responses: Option<BTreeMap<String, Value>>,
    },
}

/// Pure lookup half of [`resume_direct_mcp_continuation`]: decides from
/// folded state alone, without touching the journal, so the full decision
/// matrix is unit-testable without a step harness.
#[derive(Debug, PartialEq)]
enum DirectResumeLookup {
    Absent,
    Resume {
        pending_key: String,
        question_id: String,
        request_state: Option<String>,
        input_responses: BTreeMap<String, Value>,
    },
}

fn decide_direct_mcp_continuation(
    state: &qcg_engine::RunState,
    answers: &BTreeMap<String, Value>,
    node_id: &str,
    server: &str,
    tool: &str,
    arguments: &Value,
) -> Result<DirectResumeLookup, String> {
    let cont_key = mcp_continuation_key(node_id, server, tool, arguments);
    let pending_key = mcp_pending_reserved_key(&cont_key);
    let Some(pending) = state.mcp_pending.get(&pending_key) else {
        return Ok(DirectResumeLookup::Absent);
    };
    if pending.get("node").and_then(Value::as_str) != Some(node_id) {
        return Err(format!(
            "MCP continuation `{pending_key}` does not match this invocation; refusing resume"
        ));
    }
    let pending_server = pending
        .get("server")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("MCP continuation `{pending_key}` has no server; refusing resume")
        })?;
    let pending_tool = pending
        .get("tool")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("MCP continuation `{pending_key}` has no tool; refusing resume"))?;
    if pending_server != server || pending_tool != tool {
        return Err(format!(
            "MCP continuation `{pending_key}` targets a different remote; refusing resume"
        ));
    }
    if pending.get("arguments") != Some(arguments) {
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
    let Some(answer) = answers.get(question_id) else {
        return Ok(DirectResumeLookup::Absent);
    };
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
    // Rebuild input_responses from the stored input_requests shape and
    // the user's answer values.
    let input_requests = pending
        .get("input_requests")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let values = answer
        .as_object()
        .ok_or_else(|| "MCP input-required answer must be an object".to_string())?;
    let mut responses = BTreeMap::new();
    let mut sorted_ids: Vec<&String> = input_requests.keys().collect();
    sorted_ids.sort();
    for (index, id) in sorted_ids.into_iter().enumerate() {
        let field = format!("response_{index}");
        let value = values
            .get(&field)
            .cloned()
            .ok_or_else(|| format!("MCP answer omitted {field}"))?;
        responses.insert(id.clone(), json!({ "action": "accept", "content": value }));
    }
    Ok(DirectResumeLookup::Resume {
        pending_key,
        question_id: question_id.to_string(),
        request_state,
        input_responses: responses,
    })
}

/// Finds a journaled continuation matching this exact remote call and
/// records the resume before touching the remote.
///
/// Returns the stored request_state and the user-derived input_responses so
/// the next transport call continues the original remote request instead of
/// starting a duplicate one. Lookup hits the typed journal continuation
/// store by exact key only: consumed continuations are removed on
/// completion, so cross-node reuse and completed-request reuse cannot
/// happen and no legacy scan exists to resurrect them.
fn resume_direct_mcp_continuation(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    server: &str,
    tool: &str,
    arguments: &Value,
) -> Result<ContinuationDecision, StepError> {
    let state = ctx.journal.state();
    match decide_direct_mcp_continuation(
        &state,
        &ctx.run.answers,
        &node.id,
        server,
        tool,
        arguments,
    )
    .map_err(|message| StepError::failed(&node.id, message))?
    {
        DirectResumeLookup::Absent => Ok(ContinuationDecision::Fresh),
        DirectResumeLookup::Resume {
            pending_key,
            question_id,
            request_state,
            input_responses,
        } => {
            ctx.journal
                .event(
                    "mcp_continuation_resumed",
                    json!({
                        "node": node.id,
                        "pending_key": pending_key,
                        "question_id": question_id,
                    }),
                )
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            Ok(ContinuationDecision::Resume {
                request_state,
                input_responses: Some(input_responses),
            })
        }
    }
}

/// Marks the direct-call continuation consumed so a later identical call
/// starts a fresh remote request instead of resuming a completed one.
fn consume_direct_mcp_continuation(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    server: &str,
    tool: &str,
    arguments: &Value,
) -> Result<(), StepError> {
    use qcg_engine::ResultExt as _;
    let cont_key = mcp_continuation_key(&node.id, server, tool, arguments);
    ctx.journal
        .event(
            "mcp_continuation_consumed",
            json!({
                "node": node.id,
                "pending_key": mcp_pending_reserved_key(&cont_key),
            }),
        )
        .step_err(&node.id)
}

fn direct_mcp_requests(required: &McpInputRequired) -> Vec<(&String, &Value)> {
    let mut requests = required.input_requests.iter().collect::<Vec<_>>();
    requests.sort_by_key(|(_id, request)| serde_json::to_vec(request).unwrap_or_default());
    requests
}

pub(crate) struct DirectMcpToolEvent<'a> {
    pub(crate) server: &'a str,
    pub(crate) tool: &'a str,
    pub(crate) arguments: &'a Value,
    pub(crate) result: Option<&'a Value>,
    pub(crate) error: Option<&'a StepError>,
    pub(crate) duration_ms: u64,
    pub(crate) degraded: bool,
}

pub(crate) fn record_direct_mcp_tool_event(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    details: DirectMcpToolEvent<'_>,
) -> Result<(), StepError> {
    let DirectMcpToolEvent {
        server,
        tool,
        arguments,
        result,
        error,
        duration_ms,
        degraded,
    } = details;
    let argument_bytes = serde_json::to_vec(arguments)?.len();
    let argument_names = arguments
        .as_object()
        .map(|object| {
            object
                .keys()
                .take(128)
                .map(|name| name.chars().take(256).collect::<String>())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let arguments = json!({
        "argument_names": argument_names,
        "bytes": argument_bytes,
    });
    let (status, phase, error_value) = match error {
        Some(error) if degraded => (
            ToolCallStatus::Degraded,
            ToolCallPhase::Completed,
            Some(ToolCallError {
                code: ToolCallErrorCode::ExecutionFailed,
                message: error.to_string(),
            }),
        ),
        Some(error) => (
            ToolCallStatus::Failed,
            ToolCallPhase::Execution,
            Some(ToolCallError {
                code: ToolCallErrorCode::ExecutionFailed,
                message: error.to_string(),
            }),
        ),
        None if result.is_none() => (ToolCallStatus::NeedsUser, ToolCallPhase::Execution, None),
        None => (ToolCallStatus::Succeeded, ToolCallPhase::Completed, None),
    };
    let sources = result
        .map(tool_call_sources)
        .unwrap_or_default()
        .into_iter()
        .map(serde_json::from_value::<ToolCallSource>)
        .collect::<Result<Vec<_>, _>>()?;
    let (result, result_truncated) = result
        .map(bounded_direct_mcp_event_value)
        .unwrap_or((Value::Null, false));
    let result_summary = match result {
        Value::Null => Value::Null,
        value => {
            let bytes = serde_json::to_vec(&value)?.len();
            json!({ "bytes": bytes, "value": value })
        }
    };
    let id = format!(
        "mcp-{}",
        &hex::encode(Sha256::digest(format!("{}/{}/{}", node.id, server, tool)))[..24]
    );
    let event = ToolCallEventData {
        server: Some(server.to_owned()),
        tool: format!("mcp:{server}/{tool}"),
        id,
        status,
        phase,
        agent: None,
        error: error_value,
        duration_ms,
        arguments,
        result: result_summary,
        sources,
        truncated: result_truncated,
    };
    let mut event = serde_json::to_value(event)?;
    event["node"] = Value::String(node.id.clone());
    ctx.journal
        .event("tool_call", event)
        .map_err(|journal_error| StepError::failed(&node.id, journal_error.to_string()))
}

fn bounded_direct_mcp_event_value(value: &Value) -> (Value, bool) {
    const MAX_BYTES: usize = 64 * 1024;
    let Ok(bytes) = serde_json::to_vec(value) else {
        return (json!({ "summary": "result serialization failed" }), true);
    };
    if bytes.len() <= MAX_BYTES {
        return (value.clone(), false);
    }
    (
        json!({
            "summary": "result omitted because it exceeded the event detail limit",
            "bytes": bytes.len(),
        }),
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcg_engine::RunState;

    fn pending(
        node: &str,
        server: &str,
        tool: &str,
        arguments: Value,
        question_id: Option<&str>,
    ) -> (String, Value) {
        let key = mcp_pending_reserved_key(&mcp_continuation_key(node, server, tool, &arguments));
        let mut descriptor = json!({
            "node": node,
            "server": server,
            "tool": tool,
            "arguments": arguments,
            "request_state": "state-1",
            "input_requests": {
                "b": {"method": "elicitation/create"},
                "a": {"method": "elicitation/create"},
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

    fn answers(question_id: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([(
            question_id.to_string(),
            json!({"response_0": "first", "response_1": "second"}),
        )])
    }

    #[test]
    fn absent_continuation_starts_fresh() {
        let state = RunState::default();
        assert_eq!(
            decide_direct_mcp_continuation(
                &state,
                &BTreeMap::new(),
                "node",
                "server",
                "tool",
                &json!({"q": "x"}),
            ),
            Ok(DirectResumeLookup::Absent)
        );
    }

    #[test]
    fn answered_continuation_resumes_with_rebuilt_responses() {
        let arguments = json!({"q": "x"});
        let (key, descriptor) = pending("node", "server", "tool", arguments.clone(), Some("q-1"));
        let state = state_with(&key, descriptor);
        match decide_direct_mcp_continuation(
            &state,
            &answers("q-1"),
            "node",
            "server",
            "tool",
            &arguments,
        ) {
            Ok(DirectResumeLookup::Resume {
                pending_key,
                question_id,
                request_state,
                input_responses,
            }) => {
                assert_eq!(pending_key, key);
                assert_eq!(question_id, "q-1");
                assert_eq!(request_state.as_deref(), Some("state-1"));
                // Responses follow sorted input-request order, not answer order.
                assert_eq!(
                    input_responses
                        .get("a")
                        .and_then(|response| response.get("content"))
                        .cloned(),
                    Some(json!("first"))
                );
                assert_eq!(
                    input_responses
                        .get("b")
                        .and_then(|response| response.get("content"))
                        .cloned(),
                    Some(json!("second"))
                );
            }
            other => panic!("expected resume, got {other:?}"),
        }
    }

    #[test]
    fn foreign_invocation_never_resumes() {
        let arguments = json!({"q": "x"});
        let (key, descriptor) = pending("node", "server", "tool", arguments.clone(), Some("q-1"));
        let state = state_with(&key, descriptor);
        let stored_answers = answers("q-1");
        // A different invocation computes a different key and simply misses:
        // no resume happens. Cross-node reuse with identical
        // server/tool/arguments...
        assert_eq!(
            decide_direct_mcp_continuation(
                &state,
                &stored_answers,
                "other-node",
                "server",
                "tool",
                &arguments,
            ),
            Ok(DirectResumeLookup::Absent)
        );
        // ...same node with regenerated arguments...
        assert_eq!(
            decide_direct_mcp_continuation(
                &state,
                &stored_answers,
                "node",
                "server",
                "tool",
                &json!({"q": "y"}),
            ),
            Ok(DirectResumeLookup::Absent)
        );
        // ...and the same node and arguments against a different remote.
        assert_eq!(
            decide_direct_mcp_continuation(
                &state,
                &stored_answers,
                "node",
                "other-server",
                "tool",
                &arguments,
            ),
            Ok(DirectResumeLookup::Absent)
        );
        // A record whose stored fields disagree with its own key is corrupt:
        // refusing beats resuming blindly.
        let mut tampered = state
            .mcp_pending
            .get(&key)
            .expect("pending should exist")
            .clone();
        tampered["node"] = json!("other-node");
        let mut tampered_state = RunState::default();
        tampered_state.mcp_pending.insert(key.clone(), tampered);
        assert!(
            decide_direct_mcp_continuation(
                &tampered_state,
                &stored_answers,
                "node",
                "server",
                "tool",
                &arguments,
            )
            .is_err()
        );
    }

    #[test]
    fn corrupt_or_replayed_continuation_is_refused() {
        let arguments = json!({"q": "x"});
        // Missing question id: corrupt, must not start fresh.
        let (key, descriptor) = pending("node", "server", "tool", arguments.clone(), None);
        let state = state_with(&key, descriptor);
        assert!(
            decide_direct_mcp_continuation(
                &state,
                &BTreeMap::new(),
                "node",
                "server",
                "tool",
                &arguments,
            )
            .is_err()
        );
        // Already resumed for the same question: crash mid-call with an
        // indeterminate remote outcome.
        let (key, descriptor) = pending("node", "server", "tool", arguments.clone(), Some("q-1"));
        let mut state = state_with(&key, descriptor);
        state.mcp_resumed.insert(key.clone(), "q-1".into());
        assert!(
            decide_direct_mcp_continuation(
                &state,
                &answers("q-1"),
                "node",
                "server",
                "tool",
                &arguments,
            )
            .is_err()
        );
        // A stale marker for another question does not block this resume.
        state.mcp_resumed.insert(key.clone(), "q-0".into());
        assert!(matches!(
            decide_direct_mcp_continuation(
                &state,
                &answers("q-1"),
                "node",
                "server",
                "tool",
                &arguments,
            ),
            Ok(DirectResumeLookup::Resume { .. })
        ));
    }

    #[test]
    fn unanswered_continuation_stays_on_the_waiting_path() {
        let arguments = json!({"q": "x"});
        let (key, descriptor) = pending("node", "server", "tool", arguments.clone(), Some("q-1"));
        let state = state_with(&key, descriptor);
        assert_eq!(
            decide_direct_mcp_continuation(
                &state,
                &BTreeMap::new(),
                "node",
                "server",
                "tool",
                &arguments,
            ),
            Ok(DirectResumeLookup::Absent)
        );
    }

    #[test]
    fn continuation_keys_bind_the_full_invocation() {
        let arguments = json!({"q": "x"});
        let local = mcp_continuation_key("node", "server", "tool", &arguments);
        assert_ne!(
            local,
            mcp_continuation_key("other-node", "server", "tool", &arguments)
        );
        assert_ne!(
            local,
            mcp_continuation_key("node", "server", "tool", &json!({"q": "y"}))
        );
        assert_ne!(
            local,
            mcp_continuation_key("node", "other-server", "tool", &arguments)
        );
    }
}
