use qcg_contract::{Contract, NodeDef, ToolDecl};
use qcg_contract::{FieldType, InputField};
use qcg_engine::{StepError, validate_json_schema_step};
use qcg_llm::LlmRuntime;
use qcg_policy::{MAX_JSON_SCHEMA_BYTES, validate_bounded_json_schema};
use serde_json::{Value, json};
use std::collections::BTreeSet;

use crate::agent::agent_command_allowed;
use crate::agent_runtime::normalize_path_prefix;
use crate::policy::{effective_request_policy, llm_params};
use crate::prompting::read_bytes_bounded;
use crate::routes::validate_route_sequence;
use crate::search::validate_web_search_tool;
use crate::validation::{
    request_required_capabilities, resolve_model_static, validate_effective_tool_policy,
    validate_provider_requirements,
};

pub(crate) fn validate_agent_tool(
    node: &NodeDef,
    contract: &Contract,
    runtime: &LlmRuntime,
    tool: &ToolDecl,
) -> Result<(), StepError> {
    if tool.name() == "qcg_response" {
        return Err(StepError::failed(
            &node.id,
            "agent tool name `qcg_response` is reserved for structured output",
        ));
    }
    if let Some(schema) = tool.input_schema() {
        validate_local_agent_tool_schema(node, tool.name(), schema)?;
    }
    match tool {
        ToolDecl::FsWrite {
            name, path_prefix, ..
        } => {
            if path_prefix.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` requires path_prefix"),
                ));
            }
            if normalize_path_prefix(path_prefix).is_none() {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` path_prefix must be a safe relative path"),
                ));
            }
            if !contract
                .manifest
                .permissions
                .fs_write
                .iter()
                .any(|scope| scope == "workspace")
            {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "tool `{}` requires permissions.fs_write to include workspace",
                        name
                    ),
                ));
            }
        }
        ToolDecl::Command { name, command, .. } => {
            if command.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` requires command"),
                ));
            }
            if !agent_command_allowed(&contract.manifest.permissions.commands, command) {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "tool `{}` command is not allowed by permissions.commands",
                        name
                    ),
                ));
            }
        }
        ToolDecl::Http {
            name,
            methods,
            hosts,
            ..
        } => {
            if methods.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` requires at least one method"),
                ));
            }
            if hosts.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` requires at least one host"),
                ));
            }
            for host in hosts {
                if !contract
                    .manifest
                    .permissions
                    .network
                    .iter()
                    .any(|allowed| allowed == host)
                {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "tool `{}` host `{host}` is not allowed by permissions.network",
                            name
                        ),
                    ));
                }
            }
        }
        ToolDecl::AskUser { .. } => {}
        ToolDecl::WebSearch { .. } => {
            validate_web_search_tool(node, contract, &runtime.search, tool)?
        }
        ToolDecl::Mcp {
            name,
            server,
            tool,
            max_calls,
            ..
        } => {
            if server.trim().is_empty() || tool.trim().is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` requires non-empty server and tool"),
                ));
            }
            if *max_calls == 0 {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` max_calls must be greater than zero"),
                ));
            }
            let profile = runtime
                .mcp
                .resolve(server)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            match profile.transport() {
                qcg_mcp::McpTransport::StreamableHttp => {
                    for host in profile.allowed_hosts() {
                        if !contract
                            .manifest
                            .permissions
                            .network
                            .iter()
                            .any(|allowed| allowed == host)
                        {
                            return Err(StepError::failed(
                                &node.id,
                                format!(
                                    "tool `{name}` MCP server `{server}` host `{host}` is not allowed by permissions.network"
                                ),
                            ));
                        }
                    }
                }
                qcg_mcp::McpTransport::Stdio => {
                    if !agent_command_allowed(
                        &contract.manifest.permissions.commands,
                        profile.command(),
                    ) {
                        return Err(StepError::failed(
                            &node.id,
                            format!(
                                "tool `{name}` MCP server `{server}` command is not allowed by permissions.commands"
                            ),
                        ));
                    }
                }
            }
        }
        ToolDecl::Agent {
            name,
            instructions,
            tools: delegated_tools,
            max_calls,
            max_iterations,
            max_tokens_total,
            max_tool_calls_total,
            output_schema,
            model,
            fallback_models,
            request,
            ..
        } => {
            if instructions.trim().is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool `{name}` requires non-empty instructions"),
                ));
            }
            if *max_calls == 0 {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool `{name}` max_calls must be greater than zero"),
                ));
            }
            if *max_iterations == 0 {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool `{name}` max_iterations must be greater than zero"),
                ));
            }
            if *max_tokens_total == 0 {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool `{name}` max_tokens_total must be greater than zero"),
                ));
            }
            if *max_tool_calls_total == 0 {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool `{name}` max_tool_calls_total must be greater than zero"),
                ));
            }
            if let Some(path) = output_schema {
                load_agent_output_schema(contract, &node.id, name, path)?;
            }
            if let Some(model) = model {
                if model.provider.trim().is_empty() || model.model.trim().is_empty() {
                    return Err(StepError::failed(
                        &node.id,
                        format!("agent tool `{name}` model must have non-empty provider and model"),
                    ));
                }
                if model.provider.contains("{{") || model.model.contains("{{") {
                    return Err(StepError::failed(
                        &node.id,
                        format!("agent tool `{name}` model cannot be templated"),
                    ));
                }
            }
            for fallback in fallback_models {
                if fallback.provider.trim().is_empty() || fallback.model.trim().is_empty() {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "agent tool `{name}` fallback model must have non-empty provider and model"
                        ),
                    ));
                }
                if fallback.provider.contains("{{") || fallback.model.contains("{{") {
                    return Err(StepError::failed(
                        &node.id,
                        format!("agent tool `{name}` fallback models cannot be templated"),
                    ));
                }
            }
            let llm = contract
                .manifest
                .llm
                .as_ref()
                .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
            let parent_params = llm_params(node)?;
            let inherited_primary = parent_params
                .model
                .as_ref()
                .filter(|model| !model.provider.contains("{{") && !model.model.contains("{{"))
                .or(llm.model.as_ref());
            validate_route_sequence(
                node,
                model.as_ref().or(inherited_primary),
                fallback_models,
                &format!("agent tool `{name}` fallback_models"),
            )?;
            let policy = effective_request_policy(node, llm, Some(request))?;
            validate_effective_tool_policy(
                node,
                &policy,
                &delegated_tools
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )?;
            let provider = if let Some(model) = model {
                Some((model.provider.clone(), "specialist LLM provider"))
            } else {
                let params = llm_params(node)?;
                let dynamic = params.model.as_ref().is_some_and(|model| {
                    model.provider.contains("{{") || model.model.contains("{{")
                });
                if dynamic {
                    None
                } else {
                    Some((resolve_model_static(llm, runtime, node)?.0, "LLM provider"))
                }
            };
            let required = request_required_capabilities(
                &policy,
                output_schema.is_some(),
                !delegated_tools.is_empty(),
            );
            if let Some((provider, role)) = provider {
                validate_provider_requirements(runtime, node, &policy, &provider, role, &required)?;
            }
            for fallback in fallback_models {
                validate_provider_requirements(
                    runtime,
                    node,
                    &policy,
                    &fallback.provider,
                    "specialist fallback LLM provider",
                    &required,
                )?;
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_local_agent_tool_schema(
    node: &NodeDef,
    tool_name: &str,
    schema: &Value,
) -> Result<(), StepError> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(StepError::failed(
            &node.id,
            format!("tool `{tool_name}` input_schema root must have type `object`"),
        ));
    }
    validate_bounded_json_schema(schema).map_err(|message| {
        StepError::failed(
            &node.id,
            format!("tool `{tool_name}` input_schema is invalid or unsafe: {message}"),
        )
    })?;
    Ok(())
}

pub(crate) fn validate_agent_delegations(
    node: &NodeDef,
    tools: &[ToolDecl],
) -> Result<(), StepError> {
    for tool in tools {
        let ToolDecl::Agent {
            name,
            tools: delegated,
            ..
        } = tool
        else {
            continue;
        };
        let mut unique = BTreeSet::new();
        for delegated_name in delegated {
            if !unique.insert(delegated_name) {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool `{name}` delegates duplicate tool `{delegated_name}`"),
                ));
            }
            let delegated_tool = tools
                .iter()
                .find(|candidate| candidate.name() == delegated_name)
                .ok_or_else(|| {
                    StepError::failed(
                        &node.id,
                        format!("agent tool `{name}` delegates undeclared tool `{delegated_name}`"),
                    )
                })?;
            if matches!(delegated_tool, ToolDecl::Agent { .. }) {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "agent tool `{name}` cannot delegate another agent tool `{delegated_name}`"
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn load_agent_output_schema(
    contract: &Contract,
    node_id: &str,
    agent_name: &str,
    path: &str,
) -> Result<Value, StepError> {
    let schema_path = contract.resolve_package_path(path).map_err(|error| {
        StepError::failed(
            node_id,
            format!("agent tool `{agent_name}` output_schema path is invalid: {error}"),
        )
    })?;
    let metadata = std::fs::metadata(&schema_path).map_err(|error| {
        StepError::failed(
            node_id,
            format!("agent tool `{agent_name}` output_schema could not be read: {error}"),
        )
    })?;
    if metadata.len() > MAX_JSON_SCHEMA_BYTES as u64 {
        return Err(StepError::failed(
            node_id,
            format!(
                "agent tool `{agent_name}` output_schema exceeded {MAX_JSON_SCHEMA_BYTES} bytes"
            ),
        ));
    }
    let source = read_bytes_bounded(&schema_path, MAX_JSON_SCHEMA_BYTES).map_err(|error| {
        StepError::failed(
            node_id,
            format!("agent tool `{agent_name}` output_schema could not be read: {error}"),
        )
    })?;
    let source = String::from_utf8(source).map_err(|error| {
        StepError::failed(
            node_id,
            format!("agent tool `{agent_name}` output_schema is not valid UTF-8: {error}"),
        )
    })?;
    let schema: Value = serde_json::from_str(&source).map_err(|error| {
        StepError::failed(
            node_id,
            format!("agent tool `{agent_name}` output_schema is not JSON: {error}"),
        )
    })?;
    validate_bounded_json_schema(&schema).map_err(|message| {
        StepError::failed(
            node_id,
            format!("agent tool `{agent_name}` output_schema is invalid or unsafe: {message}"),
        )
    })?;
    Ok(schema)
}

pub(crate) fn validate_agent_tool_args(
    node: &NodeDef,
    tool: &ToolDecl,
    args: &Value,
) -> Result<(), StepError> {
    let schema = agent_tool_schema(tool);
    validate_json_schema_step(&node.id, &schema, args, "tool arguments").map_err(|error| {
        StepError::failed(
            &node.id,
            format!(
                "tool `{}` arguments failed schema validation: {error}",
                tool.name()
            ),
        )
    })?;
    if let ToolDecl::WebSearch { max_results, .. } = tool {
        let query = args
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if query.trim().is_empty() {
            return Err(StepError::failed(
                &node.id,
                format!("tool `{}` query must not be empty", tool.name()),
            ));
        }
        if let Some(limit) = args.get("limit").and_then(Value::as_u64)
            && (limit == 0 || limit > *max_results as u64)
        {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "tool `{}` limit must be from 1 through {max_results}",
                    tool.name()
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn agent_tool_schema(tool: &ToolDecl) -> Value {
    if let Some(schema) = tool.input_schema() {
        return schema.clone();
    }
    match tool {
        ToolDecl::FsWrite { .. } => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["path", "content"],
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" }
            }
        }),
        ToolDecl::Command { .. } => json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {}
        }),
        ToolDecl::Http { .. } => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["url"],
            "properties": {
                "method": { "type": "string" },
                "url": { "type": "string" },
                "headers": { "type": "object" },
                "body": { "type": "string" }
            }
        }),
        ToolDecl::AskUser { .. } => json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "question": { "type": "string" },
                "options": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "fields": {
                    "type": "array",
                    "items": { "type": "object" }
                }
            }
        }),
        ToolDecl::WebSearch { max_results, .. } => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["query"],
            "properties": {
                "query": { "type": "string", "minLength": 1, "maxLength": 1024 },
                "limit": { "type": "integer", "minimum": 1, "maximum": max_results }
            }
        }),
        ToolDecl::Mcp { .. } => unreachable!("MCP schemas are resolved from the server"),
        ToolDecl::Agent { .. } => json!({
            "type": "object",
            "additionalProperties": true
        }),
    }
}

pub(crate) fn dynamic_form_fields(
    node_id: &str,
    args: &Value,
) -> Result<Option<Vec<InputField>>, StepError> {
    let Some(value) = args.get("fields") else {
        return Ok(None);
    };
    let fields: Vec<InputField> = serde_json::from_value(value.clone()).map_err(|error| {
        StepError::failed(node_id, format!("agent form fields are invalid: {error}"))
    })?;
    if fields.is_empty() {
        return Err(StepError::failed(
            node_id,
            "agent form fields must not be empty",
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    for field in &fields {
        if field.id.trim().is_empty() || !ids.insert(field.id.clone()) {
            return Err(StepError::failed(
                node_id,
                "agent form field ids must be non-empty and unique",
            ));
        }
        if matches!(field.kind, FieldType::Custom(_)) {
            return Err(StepError::failed(
                node_id,
                format!(
                    "agent form field `{}` uses an unsupported custom type",
                    field.id
                ),
            ));
        }
    }
    Ok(Some(fields))
}
