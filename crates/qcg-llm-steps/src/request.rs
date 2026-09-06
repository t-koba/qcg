use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use qcg_contract::{LlmRequestPolicy, NodeDef, ToolDecl};
use qcg_engine::{StepContext, StepError};
use qcg_llm::{ChatContentPart, ChatMessage, ChatRequest, LlmRuntime, ToolSpec};
use qcg_types::StructuredOutputMode;
use serde_json::Value;

use super::{AGENT_SYSTEM_GUARDRAIL, FILL_SYSTEM_GUARDRAIL};
use crate::agent_tools::agent_tool_schema;
use crate::mcp_tools::McpAgentTools;
use crate::policy::{
    EffectiveRequestPolicy, MediaInput, MediaInputKind, effective_request_policy, llm_params,
};
use crate::prompting::{read_bytes_bounded, resolve_workspace_read};
use crate::validation::{resolve_model, validate_resolved_model};

pub(crate) fn build_request(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    runtime: &LlmRuntime,
    prompt: String,
    response_schema: Option<Value>,
) -> Result<ChatRequest, StepError> {
    let llm = ctx
        .run
        .contract
        .manifest
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
    let policy = effective_request_policy(node, llm, None)?;
    let (provider, model) = resolve_model(ctx, llm, runtime, node)?;
    validate_resolved_model(
        runtime,
        node,
        &policy,
        &provider,
        response_schema.is_some(),
        false,
    )?;
    let structured_output = resolve_structured_output_mode(
        runtime,
        node,
        &provider,
        policy.structured_output,
        response_schema.as_ref(),
        false,
    )?;
    let system = ctx.render_inline(node, &system_prompt(&policy, FILL_SYSTEM_GUARDRAIL))?;
    let system = structured_system_prompt(system, structured_output, response_schema.as_ref())?;
    let seed = effective_seed(ctx, node, &policy)?;
    Ok(ChatRequest {
        provider,
        model,
        system: Some(system),
        messages: vec![build_user_message(ctx, node, prompt)?],
        tools: vec![],
        response_schema,
        structured_output,
        temperature: policy.temperature,
        top_p: policy.top_p,
        max_tokens: policy.max_tokens,
        stop_sequences: policy.stop_sequences,
        seed,
        reasoning_effort: policy.reasoning_effort,
        tool_choice: policy.tool_choice,
        parallel_tool_calls: policy.parallel_tool_calls,
        verbosity: policy.verbosity,
        stream: policy.stream,
    })
}

pub(crate) fn build_user_message(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    prompt: String,
) -> Result<ChatMessage, StepError> {
    let params = llm_params(node)?;
    if params.media.is_empty() {
        return Ok(ChatMessage::text("user", prompt));
    }
    let llm = ctx
        .run
        .contract
        .manifest
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
    let limit = effective_request_policy(node, llm, None)?
        .max_media_bytes
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                "[llm].max_media_bytes is required when an LLM node declares media",
            )
        })?;
    let mut total = 0_usize;
    let mut parts = vec![ChatContentPart::Text { text: prompt }];
    for media in params.media {
        validate_media_input(node, &media)?;
        let path = resolve_workspace_read(ctx, node, &media.path)?;
        let remaining = limit.saturating_sub(total);
        let bytes = read_bytes_bounded(&path, remaining).map_err(|error| {
            StepError::failed(
                &node.id,
                format!(
                    "media input `{}` could not be read within its byte limit: {error}",
                    media.path
                ),
            )
        })?;
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| StepError::failed(&node.id, "media input byte count overflowed"))?;
        if total > limit {
            return Err(StepError::failed(
                &node.id,
                format!("media inputs exceed [llm].max_media_bytes: {total} > {limit}"),
            ));
        }
        let data = BASE64.encode(bytes);
        let filename = path
            .file_name()
            .ok_or_else(|| StepError::failed(&node.id, "media input has no file name"))?
            .to_string();
        parts.push(match media.kind {
            MediaInputKind::Image => ChatContentPart::InputImage {
                media_type: media.media_type,
                data,
                detail: media.detail,
            },
            MediaInputKind::Audio => ChatContentPart::InputAudio {
                media_type: media.media_type,
                data,
            },
            MediaInputKind::File | MediaInputKind::Video => ChatContentPart::InputFile {
                media_type: media.media_type,
                data,
                filename,
            },
        });
    }
    Ok(ChatMessage::with_parts("user", parts))
}

pub(crate) fn validate_media_input(node: &NodeDef, media: &MediaInput) -> Result<(), StepError> {
    let expected_prefix = match media.kind {
        MediaInputKind::Image => "image/",
        MediaInputKind::Audio => "audio/",
        MediaInputKind::Video => "video/",
        MediaInputKind::File => "",
    };
    if media.path.trim().is_empty()
        || media.media_type.trim().is_empty()
        || (!expected_prefix.is_empty() && !media.media_type.starts_with(expected_prefix))
    {
        return Err(StepError::failed(
            &node.id,
            format!(
                "media input `{}` must declare a non-empty MIME type matching its kind",
                media.path
            ),
        ));
    }
    if media.detail.is_some() && !matches!(media.kind, MediaInputKind::Image) {
        return Err(StepError::failed(
            &node.id,
            "media detail is only valid for image inputs",
        ));
    }
    Ok(())
}

pub(crate) struct MessageRequestOptions<'a> {
    pub(crate) response_schema: Option<Value>,
    pub(crate) tools: &'a [ToolSpec],
    pub(crate) model: Option<&'a qcg_contract::ModelRef>,
    pub(crate) policy: Option<&'a LlmRequestPolicy>,
}

pub(crate) fn build_request_with_messages(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    runtime: &LlmRuntime,
    messages: Vec<ChatMessage>,
    options: MessageRequestOptions<'_>,
) -> Result<ChatRequest, StepError> {
    let MessageRequestOptions {
        response_schema,
        tools,
        model: model_override,
        policy: specialist_request,
    } = options;
    let llm = ctx
        .run
        .contract
        .manifest
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
    let policy = effective_request_policy(node, llm, specialist_request)?;
    let (provider, model) = model_override
        .map(|model| (model.provider.clone(), model.model.clone()))
        .map(Ok)
        .unwrap_or_else(|| resolve_model(ctx, llm, runtime, node))?;
    validate_resolved_model(
        runtime,
        node,
        &policy,
        &provider,
        response_schema.is_some(),
        !tools.is_empty(),
    )?;
    let structured_output = resolve_structured_output_mode(
        runtime,
        node,
        &provider,
        policy.structured_output,
        response_schema.as_ref(),
        !tools.is_empty(),
    )?;
    let system = ctx.render_inline(node, &system_prompt(&policy, AGENT_SYSTEM_GUARDRAIL))?;
    let system = structured_system_prompt(system, structured_output, response_schema.as_ref())?;
    let seed = effective_seed(ctx, node, &policy)?;
    Ok(ChatRequest {
        provider,
        model,
        system: Some(system),
        messages,
        tools: tools.to_vec(),
        response_schema,
        structured_output,
        temperature: policy.temperature,
        top_p: policy.top_p,
        max_tokens: policy.max_tokens,
        stop_sequences: policy.stop_sequences,
        seed,
        reasoning_effort: policy.reasoning_effort,
        tool_choice: policy.tool_choice,
        parallel_tool_calls: policy.parallel_tool_calls,
        verbosity: policy.verbosity,
        stream: policy.stream,
    })
}

pub(crate) fn effective_seed(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    policy: &EffectiveRequestPolicy,
) -> Result<Option<u64>, StepError> {
    if policy.reasoning_effort.is_some() && ctx.run.llm_seed_override.is_some() {
        return Err(StepError::failed(
            &node.id,
            "an LLM seed override cannot be used when reasoning_effort is set",
        ));
    }
    Ok(ctx.run.llm_seed_override.or(policy.seed))
}

pub(crate) fn resolve_structured_output_mode(
    runtime: &LlmRuntime,
    node: &NodeDef,
    provider: &str,
    configured: StructuredOutputMode,
    schema: Option<&Value>,
    has_tools: bool,
) -> Result<StructuredOutputMode, StepError> {
    if schema.is_none() {
        return Ok(configured);
    }
    if configured != StructuredOutputMode::Auto {
        if has_tools
            && matches!(
                configured,
                StructuredOutputMode::NativeStrict | StructuredOutputMode::NativeCompatible
            )
            && !runtime
                .provider
                .capabilities_for(provider)
                .is_some_and(|capabilities| capabilities.structured_output_with_tools)
        {
            return Err(StepError::failed(
                &node.id,
                "native structured output requires provider support for structured output with tools",
            ));
        }
        if configured == StructuredOutputMode::NativeStrict
            && schema.is_some_and(|schema| !qcg_llm::strict_schema_compatible(schema))
        {
            return Err(StepError::failed(
                &node.id,
                "structured_output native_strict requires a supported native schema with closed objects and every property required",
            ));
        }
        if configured == StructuredOutputMode::NativeCompatible
            && schema.is_some_and(|schema| !qcg_llm::native_schema_compatible(schema))
        {
            return Err(StepError::failed(
                &node.id,
                "structured_output native_compatible does not support one or more schema keywords; use auto or prompt",
            ));
        }
        return Ok(configured);
    }
    let supports_schema = runtime
        .provider
        .capabilities_for(provider)
        .is_some_and(|capabilities| capabilities.json_schema);
    let supports_schema_with_tools = runtime
        .provider
        .capabilities_for(provider)
        .is_some_and(|capabilities| capabilities.structured_output_with_tools);
    if !supports_schema
        || (has_tools && !supports_schema_with_tools)
        || schema.is_some_and(|schema| !qcg_llm::native_schema_compatible(schema))
    {
        Ok(StructuredOutputMode::Prompt)
    } else if schema.is_some_and(qcg_llm::strict_schema_compatible) {
        Ok(StructuredOutputMode::NativeStrict)
    } else {
        Ok(StructuredOutputMode::NativeCompatible)
    }
}

pub(crate) fn structured_system_prompt(
    mut system: String,
    mode: StructuredOutputMode,
    schema: Option<&Value>,
) -> Result<String, StepError> {
    if mode == StructuredOutputMode::Prompt
        && let Some(schema) = schema
    {
        system.push_str("\n\nReturn only JSON satisfying this JSON Schema:\n");
        system.push_str(&serde_json::to_string(schema)?);
    }
    Ok(system)
}

pub(crate) fn system_prompt(policy: &EffectiveRequestPolicy, guardrail: &str) -> String {
    let mut system = guardrail.to_string();
    for addition in policy
        .system
        .iter()
        .filter(|value| !value.trim().is_empty())
    {
        system.push_str("\n\n");
        system.push_str(addition);
    }
    system
}

pub(crate) fn scan_llm_text(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    text: &str,
) -> Result<(), StepError> {
    let gateway = ctx
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "LLM gateway is not configured"))?;
    gateway.scan_text(node, text)
}

pub(crate) fn tool_spec(tool: &ToolDecl, mcp: &McpAgentTools) -> Result<ToolSpec, StepError> {
    if matches!(tool, ToolDecl::Mcp { .. }) {
        return mcp.tool_spec(tool.name());
    }
    Ok(ToolSpec {
        name: tool.name().to_string(),
        description: tool
            .description()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("qcg agent tool kind={}", tool.kind())),
        input_schema: agent_tool_schema(tool),
    })
}
