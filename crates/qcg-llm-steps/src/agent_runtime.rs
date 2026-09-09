use qcg_api::FormSpec;
use qcg_api::{ToolCallErrorCode, ToolCallPhase, ToolCallStatus};
use qcg_contract::{AgentFailureAction, NodeDef, ToolDecl, validate_form_values};
use qcg_contract::{AgentFailureCode, FieldType, InputField};
use qcg_engine::{HttpRequest, ResultExt, StepContext, StepError};
use qcg_llm::{ChatMessage, ChatToolCall, LlmRuntime};
use qcg_policy::is_safe_relative_path;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Instant;

use crate::agent_tools::{dynamic_form_fields, validate_agent_tool_args};
use crate::guardrail::GuardrailDecl;
use crate::mcp_forms::mcp_argument_summary;
use crate::mcp_tools::{AgentToolOutcome, McpAgentTools};
use crate::prompting::utf8_head;
use crate::request::scan_llm_text;
use crate::search::{execute_web_search, http_body_value, url_host_matches};
use crate::specialist::{SpecialistAgentSpec, execute_specialist_agent};
use crate::tool_events::{execute_mcp_tool, tool_call_error, tool_call_event, tool_call_outcome};

#[derive(Clone, Copy)]
pub(crate) struct AgentToolServices<'a> {
    pub(crate) runtime: &'a LlmRuntime,
    pub(crate) guardrails: &'a [GuardrailDecl],
}

pub(crate) struct AgentToolInvocation<'a> {
    pub(crate) mcp: &'a McpAgentTools,
    pub(crate) tools: &'a [ToolDecl],
    pub(crate) name: &'a str,
    pub(crate) call_id: &'a str,
    pub(crate) call_number: usize,
    pub(crate) args: &'a Value,
}

pub(crate) async fn execute_agent_tool(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    services: AgentToolServices<'_>,
    invocation: AgentToolInvocation<'_>,
) -> Result<AgentToolOutcome, StepError> {
    let AgentToolInvocation {
        mcp,
        tools,
        name,
        call_id,
        call_number,
        args,
    } = invocation;
    let tool = tools
        .iter()
        .find(|tool| tool.name() == name)
        .ok_or_else(|| StepError::failed(&node.id, format!("tool `{name}` is not declared")))?;
    match tool {
        ToolDecl::FsWrite { path_prefix, .. } => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| StepError::failed(&node.id, "fs.write tool requires path"))?;
            let content = args
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| StepError::failed(&node.id, "fs.write tool requires content"))?;
            if !path_is_within_prefix(path, path_prefix) {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` path `{path}` is outside prefix `{path_prefix}`"),
                ));
            }
            let target = ctx.run.fs.resolve_write(path).step_err(&node.id)?;
            ctx.run
                .fs
                .write_file_atomic(&target, content.as_bytes())
                .await
                .map_err(|error| StepError::from_gateway(&node.id, error))?;
            Ok(AgentToolOutcome::Result(json!({ "file": path })))
        }
        ToolDecl::Command { command, .. } => {
            // Same side-effect gate as ordinary command steps (A05):
            // command allowlist alone never substitutes for the
            // permissions.side_effects policy.
            let target = command.join(" ");
            let plan = ctx
                .run
                .cmd
                .command_plan(command)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            let plan_value = Some(serde_json::to_value(&plan).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("command plan is not serializable: {error}"),
                )
            })?);
            if let Some(confirm) = ctx.run.require_side_effect(
                ctx.journal,
                node,
                "command",
                &target,
                plan_value.clone(),
            )? {
                return Ok(AgentToolOutcome::NeedsConfirm(confirm));
            }
            // Agent invocations identify by call id: the checkpoint
            // re-issues the exact suspended call on resume.
            let operation_id = match ctx.run.guard_external_operation(
                ctx.journal,
                node,
                "command",
                &target,
                &plan_value,
                call_id,
            )? {
                qcg_engine::GuardDecision::Proceed { operation_id } => operation_id,
                // Same invocation already succeeded: return the cached
                // result without touching the remote again.
                qcg_engine::GuardDecision::Resend { result, .. } => {
                    return Ok(AgentToolOutcome::Result(result));
                }
            };
            let output = match ctx.run.cmd.run(command).await {
                Ok(output) => output,
                Err(error) => {
                    // Cancellation finishes nothing: it propagates
                    // without a completion record.
                    if !matches!(error, qcg_engine::GatewayError::Canceled) {
                        ctx.run.finish_external_operation_with_warn(
                            ctx.journal,
                            node,
                            &operation_id,
                            qcg_engine::OperationOutcome::gateway_error(&error, false),
                        );
                    }
                    return Err(StepError::from_gateway(&node.id, error));
                }
            };
            let output = json!({
                "status": output.status,
                "stdout": output.stdout,
                "stderr": output.stderr,
            });
            ctx.run.finish_external_operation(
                ctx.journal,
                node,
                &operation_id,
                Some(output.clone()),
            )?;
            Ok(AgentToolOutcome::Result(output))
        }
        ToolDecl::Http { methods, hosts, .. } => {
            let method = args
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("GET")
                .to_ascii_uppercase();
            if !methods.iter().any(|allowed| allowed == &method) {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` method `{method}` is not declared"),
                ));
            }
            let url = args
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| StepError::failed(&node.id, "http tool requires url"))?;
            let host_allowed = hosts.iter().any(|host| url_host_matches(url, host));
            if !host_allowed {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` url `{url}` is outside declared hosts"),
                ));
            }
            let headers = args
                .get("headers")
                .and_then(Value::as_object)
                .map(|object| {
                    object
                        .iter()
                        .filter_map(|(key, value)| {
                            value.as_str().map(|value| (key.clone(), value.to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let body = args
                .get("body")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let http_details = if matches!(method.as_str(), "GET" | "HEAD") {
                None
            } else {
                use sha2::{Digest as _, Sha256};
                let body_digest = body
                    .as_ref()
                    .map(|body| hex::encode(Sha256::digest(body.as_bytes())));
                Some(json!({ "method": method, "body_sha256": body_digest }))
            };
            if !matches!(method.as_str(), "GET" | "HEAD")
                && let Some(confirm) = ctx.run.require_side_effect(
                    ctx.journal,
                    node,
                    "http",
                    url,
                    http_details.clone(),
                )?
            {
                return Ok(AgentToolOutcome::NeedsConfirm(confirm));
            }
            let operation_id = if matches!(method.as_str(), "GET" | "HEAD") {
                None
            } else {
                Some(
                    match ctx.run.guard_external_operation(
                        ctx.journal,
                        node,
                        "http",
                        url,
                        &http_details,
                        call_id,
                    )? {
                        qcg_engine::GuardDecision::Proceed { operation_id } => operation_id,
                        // Same invocation already succeeded: return the cached
                        // result without touching the remote again.
                        qcg_engine::GuardDecision::Resend { result, .. } => {
                            return Ok(AgentToolOutcome::Result(result));
                        }
                    },
                )
            };
            let safe_method = matches!(method.as_str(), "GET" | "HEAD");
            let output = match ctx
                .run
                .http
                .request(HttpRequest {
                    method,
                    url: url.to_string(),
                    headers,
                    sensitive_query: BTreeMap::new(),
                    body: body.map(String::into_bytes),
                    follow_redirects: false,
                    idempotency_key: operation_id.clone(),
                })
                .await
            {
                Ok(output) => output,
                Err(error) => {
                    // Cancellation finishes nothing: it propagates
                    // without a completion record.
                    if !matches!(error, qcg_engine::GatewayError::Canceled)
                        && let Some(operation_id) = operation_id
                    {
                        ctx.run.finish_external_operation_with_warn(
                            ctx.journal,
                            node,
                            &operation_id,
                            qcg_engine::OperationOutcome::gateway_error(&error, safe_method),
                        );
                    }
                    return Err(StepError::from_gateway(&node.id, error));
                }
            };
            let output = json!({
                "status": output.status,
                "url": output.url,
                "headers": output.headers,
                "body": http_body_value(&output.body),
            });
            if let Some(operation_id) = operation_id {
                ctx.run.finish_external_operation(
                    ctx.journal,
                    node,
                    &operation_id,
                    Some(output.clone()),
                )?;
            }
            Ok(AgentToolOutcome::Result(output))
        }
        ToolDecl::AskUser { .. } => {
            let question_id = format!("{}:{}", node.id, tool.name());
            let fields = dynamic_form_fields(&node.id, args)?;
            if let Some(answer) = ctx.run.answers.get(&question_id) {
                if let Some(fields) = &fields {
                    validate_form_values(fields, answer, &ctx.run.contract.manifest.runtime)
                        .map_err(|error| {
                            StepError::failed(
                                &node.id,
                                format!("invalid agent form answer: {error}"),
                            )
                        })?;
                }
                return Ok(AgentToolOutcome::Result(json!({ "answer": answer })));
            }
            let title = args
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("Agent requested input")
                .to_string();
            let options = args
                .get("options")
                .and_then(Value::as_array)
                .map(|options| {
                    options
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .filter(|options| !options.is_empty())
                .unwrap_or_default();
            let fields = fields.unwrap_or_else(|| {
                let kind = if options.is_empty() {
                    FieldType::String
                } else {
                    FieldType::Select
                };
                vec![InputField {
                    id: "answer".into(),
                    label: None,
                    label_i18n: Default::default(),
                    description: None,
                    description_i18n: Default::default(),
                    placeholder: None,
                    placeholder_i18n: Default::default(),
                    kind,
                    required: true,
                    default: None,
                    pattern: None,
                    options,
                    option_labels_i18n: Default::default(),
                    min_items: None,
                    item_type: None,
                    schema: None,
                    ui: Default::default(),
                }]
            });
            Ok(AgentToolOutcome::NeedsUser(FormSpec {
                id: question_id,
                title,
                title_i18n: Default::default(),
                fields,
            }))
        }
        ToolDecl::WebSearch { .. } => execute_web_search(
            &ctx.run.http,
            &ctx.run.secrets,
            &services.runtime.search,
            node,
            tool,
            args,
        )
        .await
        .map(AgentToolOutcome::Result),
        ToolDecl::Mcp {
            server,
            tool: remote_tool,
            side_effects,
            ..
        } => {
            if *side_effects
                && let Some(confirm) = ctx.run.require_side_effect(
                    ctx.journal,
                    node,
                    &format!("mcp:{name}"),
                    &format!("{server}/{remote_tool}"),
                    Some(mcp_argument_summary(args)),
                )?
            {
                return Ok(AgentToolOutcome::NeedsConfirm(confirm));
            }
            execute_mcp_tool(ctx, node, mcp, name, args.clone(), call_id).await
        }
        ToolDecl::Agent {
            instructions,
            tools: delegated,
            max_calls,
            max_iterations,
            max_tokens_total,
            max_tool_calls_total,
            output_schema,
            model,
            fallback_models,
            request,
            on_failure,
            handoff,
            ..
        } => {
            let result = match execute_specialist_agent(
                ctx,
                node,
                services,
                mcp,
                tools,
                SpecialistAgentSpec {
                    name,
                    invocation_id: call_id,
                    instructions,
                    delegated_names: delegated,
                    max_calls: *max_calls,
                    max_iterations: *max_iterations,
                    max_tokens_total: *max_tokens_total,
                    max_tool_calls_total: *max_tool_calls_total,
                    output_schema_path: output_schema.as_deref(),
                    model: model.as_ref(),
                    fallback_models,
                    request,
                    args,
                },
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    let code = agent_failure_code(&error);
                    let action = on_failure.action(code);
                    record_agent_failure_event(ctx, node, name, call_id, code, action, &error)?;
                    return match action {
                        AgentFailureAction::Fail => Err(error),
                        AgentFailureAction::ReturnError => {
                            Ok(AgentToolOutcome::Error(agent_error_result(
                                name,
                                code,
                                &error,
                                call_number,
                                AgentToolFailureLimits {
                                    max_calls: *max_calls,
                                    max_iterations: *max_iterations,
                                    max_tokens_total: *max_tokens_total,
                                    max_tool_calls_total: *max_tool_calls_total,
                                },
                                true,
                            )))
                        }
                    };
                }
            };
            match result {
                AgentToolOutcome::Result(value) if *handoff => Ok(AgentToolOutcome::Handoff(value)),
                result => Ok(result),
            }
        }
    }
}

pub(crate) fn validate_agent_tool_call_args(
    node: &NodeDef,
    mcp: &McpAgentTools,
    tools: &[ToolDecl],
    call: &ChatToolCall,
) -> Result<(), StepError> {
    let tool = tools
        .iter()
        .find(|tool| tool.name() == call.name)
        .ok_or_else(|| {
            StepError::failed(&node.id, format!("tool `{}` is not declared", call.name))
        })?;
    if matches!(tool, ToolDecl::Mcp { .. }) {
        mcp.validate_args(node, &call.name, &call.args)
    } else {
        validate_agent_tool_args(node, tool, &call.args)
    }
}

pub(crate) fn path_is_within_prefix(path: &str, prefix: &str) -> bool {
    let Some(prefix) = normalize_path_prefix(prefix) else {
        return false;
    };
    is_safe_relative_path(path)
        && (path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

pub(crate) fn normalize_path_prefix(prefix: &str) -> Option<&str> {
    let normalized = prefix.strip_suffix('/').unwrap_or(prefix);
    is_safe_relative_path(normalized).then_some(normalized)
}

pub(crate) fn agent_failure_code(error: &StepError) -> AgentFailureCode {
    match error {
        StepError::Cancelled => AgentFailureCode::Cancelled,
        StepError::BudgetExceeded { .. } => AgentFailureCode::RunBudgetExceeded,
        StepError::Failed { message, .. } if message.contains("token budget exceeded") => {
            AgentFailureCode::TokenBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("tool call budget exceeded") => {
            AgentFailureCode::ToolCallBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("call budget exceeded") => {
            AgentFailureCode::ToolCallBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("iteration budget exceeded") => {
            AgentFailureCode::IterationBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("guardrail") => {
            AgentFailureCode::GuardrailRejected
        }
        StepError::Failed { message, .. } if message.contains("validation") => {
            AgentFailureCode::ValidationFailed
        }
        StepError::Failed { message, .. }
            if message.contains("LLM provider") || message.contains("LLM request") =>
        {
            AgentFailureCode::ProviderFailed
        }
        _ => AgentFailureCode::ToolFailed,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AgentToolFailureLimits {
    pub(crate) max_calls: usize,
    pub(crate) max_iterations: usize,
    pub(crate) max_tokens_total: u64,
    pub(crate) max_tool_calls_total: usize,
}

pub(crate) fn agent_error_result(
    name: &str,
    code: AgentFailureCode,
    error: &StepError,
    call_number: usize,
    limits: AgentToolFailureLimits,
    retryable: bool,
) -> Value {
    json!({
        "isError": true,
        "agent": name,
        "error": {
            "code": code,
            "message": utf8_head(&error.to_string(), 2_048),
            "retryable": code.is_recoverable() && retryable && call_number < limits.max_calls,
            "call_number": call_number,
            "limits": {
                "max_calls": limits.max_calls,
                "max_iterations": limits.max_iterations,
                "max_tokens_total": limits.max_tokens_total,
                "max_tool_calls_total": limits.max_tool_calls_total,
            }
        }
    })
}

pub(crate) struct AgentToolCallFailure {
    pub(crate) code: AgentFailureCode,
    pub(crate) phase: ToolCallPhase,
    pub(crate) tool_error_code: ToolCallErrorCode,
    pub(crate) call_number: usize,
    pub(crate) retryable: bool,
    pub(crate) started: Instant,
}

pub(crate) fn recover_agent_tool_call_failure(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    tools: &[ToolDecl],
    call: &ChatToolCall,
    error: &StepError,
    failure: AgentToolCallFailure,
) -> Result<Option<ChatMessage>, StepError> {
    let Some((action, limits)) = agent_tool_failure_resolution(tools, &call.name, failure.code)
    else {
        return Ok(None);
    };
    record_agent_failure_event(ctx, node, &call.name, &call.id, failure.code, action, error)?;
    if action == AgentFailureAction::Fail {
        return Ok(None);
    }
    let result = agent_error_result(
        &call.name,
        failure.code,
        error,
        failure.call_number,
        limits,
        failure.retryable,
    );
    let encoded = serde_json::to_string(&result)?;
    scan_llm_text(ctx, node, &encoded)?;
    let event = tool_call_event(
        &node.id,
        None,
        None,
        call,
        &result,
        tool_call_outcome(
            ToolCallStatus::Failed,
            failure.phase,
            Some(tool_call_error(failure.tool_error_code, error)),
            failure.started,
        ),
    )?;
    ctx.journal.event("tool_call", event).step_err(&node.id)?;
    Ok(Some(ChatMessage::tool_result(call.id.clone(), encoded)))
}

pub(crate) fn agent_tool_failure_resolution(
    tools: &[ToolDecl],
    name: &str,
    code: AgentFailureCode,
) -> Option<(AgentFailureAction, AgentToolFailureLimits)> {
    let ToolDecl::Agent {
        max_calls,
        max_iterations,
        max_tokens_total,
        max_tool_calls_total,
        on_failure,
        ..
    } = tools.iter().find(|tool| tool.name() == name)?
    else {
        return None;
    };
    Some((
        on_failure.action(code),
        AgentToolFailureLimits {
            max_calls: *max_calls,
            max_iterations: *max_iterations,
            max_tokens_total: *max_tokens_total,
            max_tool_calls_total: *max_tool_calls_total,
        },
    ))
}

pub(crate) fn record_agent_failure_event(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    name: &str,
    call_id: &str,
    code: AgentFailureCode,
    action: AgentFailureAction,
    error: &StepError,
) -> Result<(), StepError> {
    ctx.journal
        .event(
            "agent_failed",
            json!({
                "node": node.id,
                "agent": name,
                "tool_call_id": call_id,
                "code": code,
                "action": action,
                "message": utf8_head(&error.to_string(), 2_048),
            }),
        )
        .step_err(&node.id)
}
