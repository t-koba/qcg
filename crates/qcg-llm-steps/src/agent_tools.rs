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
use crate::prompting::read_path_bounded;
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
            if *max_calls == 0 || *max_calls > qcg_policy::MAX_AGENT_TOOL_CALLS {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "agent tool `{name}` max_calls must be between 1 and {}",
                        qcg_policy::MAX_AGENT_TOOL_CALLS
                    ),
                ));
            }
            if *max_iterations == 0 || *max_iterations > qcg_policy::MAX_AGENT_ITERATIONS {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "agent tool `{name}` max_iterations must be between 1 and {}",
                        qcg_policy::MAX_AGENT_ITERATIONS
                    ),
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
            if fallback_models.len() > qcg_policy::MAX_FALLBACK_MODELS {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "agent tool `{name}` declares more than {} fallback models",
                        qcg_policy::MAX_FALLBACK_MODELS
                    ),
                ));
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
            crate::validation::validate_effort_templates(
                node,
                llm,
                &parent_params.request,
                Some(request),
            )?;
            validate_effective_tool_policy(
                node,
                &policy,
                &delegated_tools
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )?;
            let provider = if let Some(model) = model {
                Some((
                    model.provider.clone(),
                    Some(model.model.clone()),
                    "specialist LLM provider",
                ))
            } else {
                let params = llm_params(node)?;
                let dynamic = params.model.as_ref().is_some_and(|model| {
                    model.provider.contains("{{") || model.model.contains("{{")
                });
                if dynamic {
                    None
                } else {
                    let (provider, model) = resolve_model_static(llm, runtime, node)?;
                    Some((provider, Some(model), "LLM provider"))
                }
            };
            let required = request_required_capabilities(
                &policy,
                output_schema.is_some(),
                !delegated_tools.is_empty(),
            );
            if let Some((provider, model, role)) = provider {
                validate_provider_requirements(
                    runtime,
                    node,
                    &policy,
                    &provider,
                    model.as_deref(),
                    role,
                    &required,
                )?;
            }
            for fallback in fallback_models {
                validate_provider_requirements(
                    runtime,
                    node,
                    &policy,
                    &fallback.provider,
                    Some(&fallback.model),
                    "specialist fallback LLM provider",
                    &required,
                )?;
            }
        }
        ToolDecl::Skill { .. } => {
            crate::skill_tool::validate_skill_tool(node, contract, tool)?;
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
    let source = read_path_bounded(&schema_path, MAX_JSON_SCHEMA_BYTES).map_err(|error| {
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
    let schema = agent_tool_schema(tool)?;
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

pub(crate) fn agent_tool_schema(tool: &ToolDecl) -> Result<Value, StepError> {
    if let Some(schema) = tool.input_schema() {
        return Ok(schema.clone());
    }
    Ok(match tool {
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
            // Call args are intentionally empty: the executed argv comes
            // from the declared tool plan, never from model-supplied args,
            // so there is nothing for the model to pass (E09).
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
                "body": { "type": "string" },
                // Declared sensitive query names: bound into the approval
                // digest and removed from the journaled target. Must be
                // schema-visible or callers could never declare it and the
                // read path below would stay unreachable (E09).
                "sensitive_query": { "type": "array", "items": { "type": "string" } }
            }
        }),
        ToolDecl::AskUser { .. } => json!({
            // Agent-tool-only `notes`: material context bound into the
            // minimized identity (`minimized_builtin_args` keeps
            // question/options/fields/notes). The plain `ask_user` step
            // carries no `notes` field by design: it binds `content`
            // directly as the question, while the agent tool separates the
            // displayed `question` from caller-supplied `notes` context.
            // The shapes are intentionally different; `notes` must stay
            // schema-visible here or a notes-carrying call fails validation
            // while the identity still binds it (E08a).
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
                },
                "notes": { "type": "string" }
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
        ToolDecl::Mcp { .. } => {
            // Resolved from the server by the caller (`tool_spec`
            // dispatches Mcp before reaching here). A direct call is a
            // programming error that must fail the step, never panic.
            return Err(StepError::failed(
                "agent",
                "MCP schemas are resolved from the server",
            ));
        }
        ToolDecl::Agent { .. } => json!({
            "type": "object",
            "additionalProperties": true
        }),
        ToolDecl::Skill { .. } => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["skill"],
            "properties": {
                "skill": { "type": "string", "minLength": 1 },
                "file": { "type": "string", "minLength": 1 }
            }
        }),
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use qcg_contract::{NodeDef, StepType, ToolDecl};
    use serde_json::json;

    fn agent_node() -> NodeDef {
        NodeDef {
            id: "agent".into(),
            kind: StepType::from("llm.agent"),
            needs: vec![],
            when: None,
            on_deps: qcg_contract::OnDeps::AllSucceeded,
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: None,
            params: Default::default(),
        }
    }

    fn ask_user_tool() -> ToolDecl {
        ToolDecl::AskUser {
            name: "ask".into(),
            description: None,
            input_schema: None,
        }
    }

    #[test]
    fn ask_user_schema_accepts_notes_through_real_validation_and_id_path() {
        // Gap 1 (real bug): the minimized identity binds `notes`, so the
        // schema must accept it. This drives a notes-carrying call through
        // the real validation (`validate_agent_tool_args`) plus the real
        // identity path (`minimized_builtin_args` /
        // `agent_call_identity_hash_for_tool` / `ask_user_question_id`).
        // Before the fix this failed at schema validation with
        // `additionalProperties: false` rejecting `notes`.
        let node = agent_node();
        let tool = ask_user_tool();
        let args = json!({"question": "city?", "notes": "billing"});
        validate_agent_tool_args(&node, &tool, &args)
            .expect("notes-carrying call must pass real schema validation");
        // The identity path binds notes: different notes separate.
        let canonical = crate::agent_runtime::canonical_agent_registry_args(
            &tool_decl_for(&tool),
            "ask",
            &args,
        );
        let hash =
            crate::agent_runtime::agent_call_identity_hash("agent", "run-test-1", &canonical)
                .expect("identity hash should build");
        let other = crate::agent_runtime::canonical_agent_registry_args(
            &tool_decl_for(&tool),
            "ask",
            &json!({"question": "city?", "notes": "shipping"}),
        );
        let other_hash =
            crate::agent_runtime::agent_call_identity_hash("agent", "run-test-1", &other)
                .expect("identity hash should build");
        assert_ne!(
            hash, other_hash,
            "notes change must alter the identity hash"
        );
        let first =
            crate::agent_runtime::ask_user_question_id("agent", "agent", "ask", "call-1", &args)
                .expect("question id should build");
        let second = crate::agent_runtime::ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-1",
            &json!({"question": "city?", "notes": "shipping"}),
        )
        .expect("question id should build");
        assert_ne!(first, second, "notes change must alter the question id");
        // No-notes calls still validate, and unknown fields still fail.
        validate_agent_tool_args(&node, &tool, &json!({"question": "city?"}))
            .expect("no-notes call must still validate");
        assert!(
            validate_agent_tool_args(&node, &tool, &json!({"question": "city?", "bogus": 1}))
                .is_err(),
            "unknown fields must still be rejected"
        );
        // Non-string notes fail closed.
        assert!(
            validate_agent_tool_args(&node, &tool, &json!({"question": "city?", "notes": 42}))
                .is_err(),
            "non-string notes must fail schema validation"
        );
    }

    fn tool_decl_for(tool: &ToolDecl) -> Vec<ToolDecl> {
        vec![tool.clone()]
    }
}
