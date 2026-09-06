use qcg_api::{FormSpec, ToolCallErrorCode, ToolCallPhase, ToolCallStatus};
use qcg_api::{ToolCallError, ToolCallEventData, ToolCallSource};
use qcg_contract::InputField;
use qcg_contract::{FieldType, NodeDef};
use qcg_engine::{ResultExt, StepContext, StepError, tool_call_sources};
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
    // starting a new remote request. The service injects the journaled
    // pending descriptor into answers under a reserved key (see
    // mcp_pending_reserved_key), so a HITL restart continues the same
    // remote call with its original request_state.
    if let Some((resumed_state, resumed_responses)) =
        find_resumed_mcp_continuation(ctx, session.server_id(), tool_name, &arguments)
    {
        request_state = resumed_state;
        input_responses = resumed_responses;
    }
    for _ in 0..10 {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_seconds),
            session.call_tool_with_input(
                tool_name,
                arguments.clone(),
                input_responses.take(),
                request_state.take(),
            ),
        )
        .await
        .map_err(|_| StepError::failed(&node.id, "MCP tools/call timed out"))?;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(McpError::ToolFailed { result, .. }) => McpCallOutcome::Complete(result),
            Err(error) => return Err(StepError::failed(&node.id, error.to_string())),
        };
        match outcome {
            McpCallOutcome::Complete(value) => {
                let size = serde_json::to_vec(&value)?.len();
                if size > result_limit_bytes {
                    return Err(StepError::failed(
                        &node.id,
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
                            StepError::failed(
                                &node.id,
                                format!(
                                    "MCP tool {tool_name} declared outputSchema but omitted structuredContent"
                                ),
                            )
                        })?;
                    if let Err(error) = validator.validate(structured) {
                        return Err(StepError::failed(
                            &node.id,
                            format!(
                                "MCP tool {tool_name} result failed output schema at {}: {error}",
                                error.instance_path()
                            ),
                        ));
                    }
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
                let question_id =
                    direct_mcp_question_id(&node.id, session.server_id(), tool_name, &required);
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
    required: &McpInputRequired,
) -> String {
    let mut requests = required
        .input_requests
        .values()
        .map(|request| serde_json::to_vec(request).unwrap_or_default())
        .collect::<Vec<_>>();
    requests.sort();
    let bytes = serde_json::to_vec(&json!({
        "server": server,
        "tool": tool,
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

/// Stored request_state plus user-derived input_responses for continuing the
/// original remote MCP request after a HITL restart.
type ResumedMcpContinuation = (Option<String>, Option<BTreeMap<String, Value>>);

/// Find a journaled continuation matching this exact remote call.
///
/// Returns the stored request_state and the user-derived input_responses so
/// the next transport call continues the original remote request instead of
/// starting a duplicate one.
fn find_resumed_mcp_continuation(
    ctx: &StepContext<'_>,
    server: &str,
    tool: &str,
    arguments: &Value,
) -> Option<ResumedMcpContinuation> {
    for pending in ctx
        .run
        .answers
        .iter()
        .filter(|(key, _)| key.ends_with("#__mcp_pending"))
        .map(|(_, value)| value)
    {
        let Some(pending_server) = pending.get("server").and_then(Value::as_str) else {
            continue;
        };
        let Some(pending_tool) = pending.get("tool").and_then(Value::as_str) else {
            continue;
        };
        if pending_server != server || pending_tool != tool {
            continue;
        }
        if pending.get("arguments") != Some(arguments) {
            continue;
        }
        let Some(question_id) = pending.get("question_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(answer) = ctx.run.answers.get(question_id) else {
            continue;
        };
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
        let values = answer.as_object()?;
        let mut responses = BTreeMap::new();
        let mut sorted_ids: Vec<&String> = input_requests.keys().collect();
        sorted_ids.sort();
        for (index, id) in sorted_ids.into_iter().enumerate() {
            let field = format!("response_{index}");
            let value = values.get(&field)?.clone();
            responses.insert(id.clone(), json!({ "action": "accept", "content": value }));
        }
        return Some((request_state, Some(responses)));
    }
    None
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
