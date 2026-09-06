use qcg_contract::{Contract, LlmConfig, NodeDef, ToolDecl};
use qcg_engine::{StepContext, StepError};
use qcg_llm::LlmRuntime;
use qcg_types::{StructuredOutputMode, ToolChoice};

use crate::policy::{
    EffectiveRequestPolicy, LlmParams, MediaInputKind, effective_request_policy, llm_params,
};
use crate::prompting::{load_response_schema, resolve_prompt_path};
use crate::request::validate_media_input;
use crate::routes::validate_route_sequence;

pub(crate) fn validate_llm_node(
    node: &NodeDef,
    contract: &Contract,
    runtime: &LlmRuntime,
    has_response_schema: bool,
    has_tools: bool,
) -> Result<(), StepError> {
    let llm = contract
        .manifest
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
    let params = llm_params(node)?;
    let policy = effective_request_policy(node, llm, None)?;
    validate_effective_tool_policy(
        node,
        &policy,
        &params.tools.iter().map(ToolDecl::name).collect::<Vec<_>>(),
    )?;
    if let Some(prompt) = params.prompt.as_deref() {
        resolve_prompt_path(contract, node, prompt)?;
    }
    if let Some(schema) = params.schema.as_deref() {
        load_response_schema(contract, node, schema)?;
    }
    let dynamic_model = params
        .model
        .as_ref()
        .is_some_and(|model| model.provider.contains("{{") || model.model.contains("{{"));
    validate_route_sequence(
        node,
        (!dynamic_model)
            .then(|| params.model.as_ref().or(llm.model.as_ref()))
            .flatten(),
        &params.fallback_models,
        "fallback_models",
    )?;
    if let Some(model) = params.model.as_ref().filter(|_| dynamic_model) {
        let environment = minijinja::Environment::new();
        environment
            .template_from_str(&model.provider)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        environment
            .template_from_str(&model.model)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    }
    let required = required_capabilities(node, &policy, &params, has_response_schema, has_tools)?;
    if !dynamic_model {
        let (provider_id, _) = resolve_model_static(llm, runtime, node)?;
        validate_provider_requirements(
            runtime,
            node,
            &policy,
            &provider_id,
            "LLM provider",
            &required,
        )?;
    }
    for fallback in &params.fallback_models {
        validate_provider_requirements(
            runtime,
            node,
            &policy,
            &fallback.provider,
            "fallback LLM provider",
            &required,
        )?;
    }
    Ok(())
}

pub(crate) fn validate_effective_tool_policy(
    node: &NodeDef,
    policy: &EffectiveRequestPolicy,
    tool_names: &[&str],
) -> Result<(), StepError> {
    if tool_names.is_empty()
        && (policy.tool_choice.is_some() || policy.parallel_tool_calls.is_some())
    {
        return Err(StepError::failed(
            &node.id,
            "request tool_choice and parallel_tool_calls require at least one tool",
        ));
    }
    if let Some(ToolChoice::Tool { tool }) = &policy.tool_choice
        && !tool_names.contains(&tool.as_str())
    {
        return Err(StepError::failed(
            &node.id,
            format!("request tool_choice.tool references undeclared tool `{tool}`"),
        ));
    }
    Ok(())
}

pub(crate) fn required_capabilities(
    node: &NodeDef,
    policy: &EffectiveRequestPolicy,
    params: &LlmParams,
    has_response_schema: bool,
    has_tools: bool,
) -> Result<Vec<String>, StepError> {
    let mut required = request_required_capabilities(policy, has_response_schema, has_tools);
    if !params.media.is_empty() && policy.max_media_bytes.is_none() {
        return Err(StepError::failed(
            &node.id,
            "[llm].max_media_bytes or request.max_media_bytes is required when an LLM node declares media",
        ));
    }
    for media in &params.media {
        validate_media_input(node, media)?;
        required.push(
            match media.kind {
                MediaInputKind::Image => "image_input",
                MediaInputKind::Audio => "audio_input",
                MediaInputKind::File | MediaInputKind::Video => "file_input",
            }
            .into(),
        );
    }
    required.sort();
    required.dedup();
    Ok(required)
}

pub(crate) fn request_required_capabilities(
    policy: &EffectiveRequestPolicy,
    has_response_schema: bool,
    has_tools: bool,
) -> Vec<String> {
    let mut required = policy.requires.clone();
    if policy.temperature.is_some() {
        required.push("temperature".into());
    }
    if policy.seed.is_some() {
        required.push("seed".into());
    }
    if policy.reasoning_effort.is_some() {
        required.push("reasoning_effort".into());
    }
    if policy.top_p.is_some() {
        required.push("top_p".into());
    }
    if !policy.stop_sequences.is_empty() {
        required.push("stop_sequences".into());
    }
    if policy.tool_choice.is_some() {
        required.push("tool_choice".into());
    }
    if policy.parallel_tool_calls.is_some() {
        required.push("parallel_tool_calls".into());
    }
    if policy.verbosity.is_some() {
        required.push("verbosity".into());
    }
    if has_response_schema
        && matches!(
            policy.structured_output,
            StructuredOutputMode::NativeStrict | StructuredOutputMode::NativeCompatible
        )
    {
        required.push("json_schema".into());
        if has_tools {
            required.push("structured_output_with_tools".into());
        }
    }
    if has_tools {
        required.push("tool_use".into());
    }
    if policy.stream {
        required.push("streaming".into());
    }
    required.sort();
    required.dedup();
    required
}

pub(crate) fn validate_provider_requirements(
    runtime: &LlmRuntime,
    node: &NodeDef,
    policy: &EffectiveRequestPolicy,
    provider_id: &str,
    provider_role: &str,
    required: &[String],
) -> Result<(), StepError> {
    let capabilities = runtime
        .provider
        .capabilities_for(provider_id)
        .ok_or_else(|| {
            let hint = if provider_role == "LLM provider" && runtime.registry_present {
                "enable its row in your providers.toml registry".to_string()
            } else if provider_role == "LLM provider" {
                "no providers registry was found; pass --providers <PATH>, set QCG_PROVIDERS, or place providers.toml next to the qcg binary".to_string()
            } else {
                "register it in providers.toml".to_string()
            };
            StepError::failed(
                &node.id,
                format!("{provider_role} `{provider_id}` is not registered; {hint}"),
            )
        })?;
    if let Some(error) = runtime.provider.configuration_error_for(provider_id) {
        return Err(StepError::failed(&node.id, error));
    }
    if let Some(effort) = policy.reasoning_effort
        && !capabilities.reasoning_effort.contains(&effort)
    {
        return Err(StepError::failed(
            &node.id,
            format!(
                "{provider_role} `{provider_id}` does not support reasoning_effort `{}`",
                effort
            ),
        ));
    }
    for capability in required {
        if !has_capability(&capabilities, capability) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "{provider_role} `{provider_id}` does not satisfy required capability `{capability}`"
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn resolve_model_static(
    llm: &LlmConfig,
    runtime: &LlmRuntime,
    node: &NodeDef,
) -> Result<(String, String), StepError> {
    if let Some(model) = llm_params(node)?.model {
        return Ok((model.provider, model.model));
    }
    if let Some(model) = &llm.model {
        return Ok((model.provider.clone(), model.model.clone()));
    }
    let Some(default) = &runtime.default_model else {
        return Err(StepError::failed(
            &node.id,
            "[llm].model is required because no default model is configured",
        ));
    };
    Ok((default.provider.clone(), default.model.clone()))
}

pub(crate) fn resolve_model(
    ctx: &StepContext<'_>,
    llm: &LlmConfig,
    runtime: &LlmRuntime,
    node: &NodeDef,
) -> Result<(String, String), StepError> {
    let (provider, model) = resolve_model_static(llm, runtime, node)?;
    let provider = ctx.render_inline(node, &provider)?;
    let model = ctx.render_inline(node, &model)?;
    if provider.trim().is_empty() || model.trim().is_empty() {
        return Err(StepError::failed(
            &node.id,
            "resolved LLM provider and model must be non-empty",
        ));
    }
    Ok((provider, model))
}

pub(crate) fn validate_resolved_model(
    runtime: &LlmRuntime,
    node: &NodeDef,
    policy: &EffectiveRequestPolicy,
    provider: &str,
    has_response_schema: bool,
    has_tools: bool,
) -> Result<(), StepError> {
    let params = llm_params(node)?;
    let required = required_capabilities(node, policy, &params, has_response_schema, has_tools)?;
    validate_provider_requirements(
        runtime,
        node,
        policy,
        provider,
        "resolved LLM provider",
        &required,
    )
}

pub(crate) fn has_capability(capabilities: &qcg_llm::Capabilities, name: &str) -> bool {
    match name {
        "tool_use" => capabilities.tool_use,
        "json_schema" => capabilities.json_schema,
        "structured_output_with_tools" => capabilities.structured_output_with_tools,
        "seed" => capabilities.seed,
        "reasoning_effort" => !capabilities.reasoning_effort.is_empty(),
        "image_input" => capabilities.image_input,
        "audio_input" => capabilities.audio_input,
        "file_input" => capabilities.file_input,
        "streaming" => capabilities.streaming,
        "temperature" => capabilities.temperature,
        "top_p" => capabilities.top_p,
        "stop_sequences" => capabilities.stop_sequences,
        "tool_choice" => capabilities.tool_choice,
        "parallel_tool_calls" => capabilities.parallel_tool_calls,
        "verbosity" => capabilities.verbosity,
        _ => false,
    }
}

pub(crate) fn require(node: &NodeDef, value: Option<&str>, field: &str) -> Result<(), StepError> {
    if value.unwrap_or_default().is_empty() {
        Err(StepError::failed(&node.id, format!("{field} is required")))
    } else {
        Ok(())
    }
}
