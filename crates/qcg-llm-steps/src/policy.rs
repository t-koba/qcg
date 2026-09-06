use qcg_contract::{
    ContextOverflowPolicy, ContextRef, LlmConfig, LlmRequestControl, LlmRequestPolicy, NodeDef,
    ToolDecl,
};
use qcg_engine::StepError;
use qcg_llm::ImageDetail;
use qcg_types::{ResponseVerbosity, StructuredOutputMode, ToolChoice};
use serde::Deserialize;
use std::collections::BTreeSet;

use crate::context::effective_context_byte_limit;
use crate::guardrail::GuardrailDecl;
use crate::validation::require;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LlmParams {
    #[serde(default)]
    pub(crate) prompt: Option<String>,
    #[serde(default)]
    pub(crate) output_file: Option<String>,
    #[serde(default)]
    pub(crate) schema: Option<String>,
    #[serde(default)]
    pub(crate) context: Vec<ContextRef>,
    #[serde(default)]
    pub(crate) media: Vec<MediaInput>,
    #[serde(default)]
    pub(crate) source: Option<String>,
    #[serde(default)]
    pub(crate) target: Option<String>,
    #[serde(default)]
    pub(crate) max_iterations: Option<usize>,
    #[serde(default)]
    pub(crate) max_tokens_total: Option<u64>,
    #[serde(default)]
    pub(crate) max_tool_calls_total: Option<usize>,
    #[serde(default)]
    pub(crate) options: Vec<String>,
    #[serde(default)]
    pub(crate) tools: Vec<ToolDecl>,
    #[serde(default)]
    pub(crate) guardrails: Vec<GuardrailDecl>,
    #[serde(default)]
    pub(crate) model: Option<qcg_contract::ModelRef>,
    #[serde(default)]
    pub(crate) fallback_models: Vec<qcg_contract::ModelRef>,
    #[serde(default)]
    pub(crate) request: LlmRequestPolicy,
}

#[derive(Debug, Clone)]
pub(crate) struct EffectiveRequestPolicy {
    pub(crate) system: Vec<String>,
    pub(crate) temperature: Option<f32>,
    pub(crate) top_p: Option<f32>,
    pub(crate) max_tokens: u32,
    pub(crate) stop_sequences: Vec<String>,
    pub(crate) seed: Option<u64>,
    pub(crate) reasoning_effort: Option<qcg_types::ReasoningEffort>,
    pub(crate) structured_output: StructuredOutputMode,
    pub(crate) tool_choice: Option<ToolChoice>,
    pub(crate) parallel_tool_calls: Option<bool>,
    pub(crate) verbosity: Option<ResponseVerbosity>,
    pub(crate) stream: bool,
    pub(crate) requires: Vec<String>,
    pub(crate) max_context_bytes: Option<usize>,
    pub(crate) max_context_tokens: Option<usize>,
    pub(crate) max_media_bytes: Option<usize>,
    pub(crate) context_overflow: ContextOverflowPolicy,
    pub(crate) retry_prompt: Option<String>,
}

impl EffectiveRequestPolicy {
    pub(crate) fn from_llm(llm: &LlmConfig) -> Self {
        Self {
            system: llm.system.clone().into_iter().collect(),
            temperature: llm.temperature,
            top_p: llm.top_p,
            max_tokens: llm.max_tokens.expect("validated max_tokens"),
            stop_sequences: llm.stop_sequences.clone(),
            seed: llm.seed,
            reasoning_effort: llm.reasoning_effort,
            structured_output: llm.structured_output,
            tool_choice: llm.tool_choice.clone(),
            parallel_tool_calls: llm.parallel_tool_calls,
            verbosity: llm.verbosity,
            stream: false,
            requires: llm.requires.clone(),
            max_context_bytes: llm.max_context_bytes,
            max_context_tokens: llm.max_context_tokens,
            max_media_bytes: llm.max_media_bytes,
            context_overflow: llm.context_overflow.clone(),
            retry_prompt: llm.retry_prompt.clone(),
        }
    }

    fn apply(&mut self, policy: &LlmRequestPolicy) {
        for control in &policy.clear {
            match control {
                LlmRequestControl::Temperature => self.temperature = None,
                LlmRequestControl::TopP => self.top_p = None,
                LlmRequestControl::StopSequences => self.stop_sequences.clear(),
                LlmRequestControl::Seed => self.seed = None,
                LlmRequestControl::ReasoningEffort => self.reasoning_effort = None,
                LlmRequestControl::ToolChoice => self.tool_choice = None,
                LlmRequestControl::ParallelToolCalls => self.parallel_tool_calls = None,
                LlmRequestControl::Verbosity => self.verbosity = None,
            }
        }
        if let Some(system) = &policy.system {
            self.system.push(system.clone());
        }
        if let Some(temperature) = policy.temperature {
            self.temperature = Some(temperature);
            self.top_p = None;
            self.reasoning_effort = None;
        }
        if let Some(top_p) = policy.top_p {
            self.top_p = Some(top_p);
            self.temperature = None;
            self.reasoning_effort = None;
        }
        if let Some(max_tokens) = policy.max_tokens {
            self.max_tokens = max_tokens;
        }
        if let Some(stop_sequences) = &policy.stop_sequences {
            self.stop_sequences.clone_from(stop_sequences);
        }
        if let Some(seed) = policy.seed {
            self.seed = Some(seed);
            self.reasoning_effort = None;
        }
        if let Some(reasoning_effort) = policy.reasoning_effort {
            self.reasoning_effort = Some(reasoning_effort);
            self.temperature = None;
            self.top_p = None;
            self.seed = None;
        }
        if let Some(structured_output) = policy.structured_output {
            self.structured_output = structured_output;
        }
        if let Some(tool_choice) = &policy.tool_choice {
            self.tool_choice = Some(tool_choice.clone());
        }
        if let Some(parallel_tool_calls) = policy.parallel_tool_calls {
            self.parallel_tool_calls = Some(parallel_tool_calls);
        }
        if let Some(verbosity) = policy.verbosity {
            self.verbosity = Some(verbosity);
        }
        if let Some(stream) = policy.stream {
            self.stream = stream;
        }
        self.requires.extend(policy.requires.iter().cloned());
        if let Some(max_context_bytes) = policy.max_context_bytes {
            self.max_context_bytes = Some(max_context_bytes);
        }
        if let Some(max_context_tokens) = policy.max_context_tokens {
            self.max_context_tokens = Some(max_context_tokens);
        }
        if let Some(max_media_bytes) = policy.max_media_bytes {
            self.max_media_bytes = Some(max_media_bytes);
        }
        if let Some(context_overflow) = &policy.context_overflow {
            self.context_overflow = context_overflow.clone();
        }
        if let Some(retry_prompt) = &policy.retry_prompt {
            self.retry_prompt = Some(retry_prompt.clone());
        }
        self.requires.sort();
        self.requires.dedup();
    }
}

pub(crate) fn effective_request_policy(
    node: &NodeDef,
    llm: &LlmConfig,
    specialist: Option<&LlmRequestPolicy>,
) -> Result<EffectiveRequestPolicy, StepError> {
    let params = llm_params(node)?;
    let mut effective = EffectiveRequestPolicy::from_llm(llm);
    validate_request_policy(node, &params.request, llm.max_tokens, &effective)?;
    effective.apply(&params.request);
    if let Some(specialist) = specialist {
        validate_request_policy(node, specialist, llm.max_tokens, &effective)?;
        effective.apply(specialist);
    }
    Ok(effective)
}

pub(crate) fn validate_request_policy(
    node: &NodeDef,
    policy: &LlmRequestPolicy,
    max_tokens_limit: Option<u32>,
    inherited: &EffectiveRequestPolicy,
) -> Result<(), StepError> {
    let mut cleared = BTreeSet::new();
    for control in &policy.clear {
        if !cleared.insert(control) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "request clear contains duplicate control `{}`",
                    control.as_str()
                ),
            ));
        }
    }
    if policy
        .system
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
        || policy
            .retry_prompt
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
    {
        return Err(StepError::failed(
            &node.id,
            "request system and retry_prompt must not be empty when configured",
        ));
    }
    if policy
        .temperature
        .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
    {
        return Err(StepError::failed(
            &node.id,
            "request temperature must be finite and between 0 and 2",
        ));
    }
    if policy
        .top_p
        .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
    {
        return Err(StepError::failed(
            &node.id,
            "request top_p must be finite and between 0 and 1",
        ));
    }
    if policy.temperature.is_some() && policy.top_p.is_some() {
        return Err(StepError::failed(
            &node.id,
            "request temperature and top_p are mutually exclusive",
        ));
    }
    if policy.reasoning_effort.is_some()
        && (policy.temperature.is_some() || policy.top_p.is_some() || policy.seed.is_some())
    {
        return Err(StepError::failed(
            &node.id,
            "request reasoning_effort cannot be combined with temperature, top_p, or seed",
        ));
    }
    if policy
        .tool_choice
        .as_ref()
        .is_some_and(|choice| matches!(choice, ToolChoice::Tool { tool } if tool.trim().is_empty()))
    {
        return Err(StepError::failed(
            &node.id,
            "request tool_choice.tool must not be empty",
        ));
    }
    if policy.max_tokens == Some(0)
        || policy
            .max_tokens
            .zip(max_tokens_limit)
            .is_some_and(|(value, limit)| value > limit)
    {
        return Err(StepError::failed(
            &node.id,
            format!(
                "request max_tokens must be from 1 through {}",
                max_tokens_limit.unwrap_or(u32::MAX)
            ),
        ));
    }
    if policy.stop_sequences.as_ref().is_some_and(|values| {
        values.len() > 8
            || values
                .iter()
                .any(|value| value.is_empty() || value.len() > 1_024)
    }) {
        return Err(StepError::failed(
            &node.id,
            "request stop_sequences must contain at most 8 non-empty strings of at most 1024 bytes",
        ));
    }
    for (name, value) in [
        ("max_context_bytes", policy.max_context_bytes),
        ("max_context_tokens", policy.max_context_tokens),
        ("max_media_bytes", policy.max_media_bytes),
    ] {
        if value == Some(0) {
            return Err(StepError::failed(
                &node.id,
                format!("request {name} must be greater than zero"),
            ));
        }
    }
    let inherited_context = effective_context_byte_limit(inherited);
    let requested_context = policy
        .max_context_bytes
        .into_iter()
        .chain(
            policy
                .max_context_tokens
                .map(|tokens| tokens.saturating_mul(4)),
        )
        .min();
    if requested_context.is_some_and(|limit| limit > inherited_context) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "request context limit must not exceed inherited limit {inherited_context} bytes"
            ),
        ));
    }
    if policy
        .max_media_bytes
        .zip(inherited.max_media_bytes)
        .is_some_and(|(value, limit)| value > limit)
    {
        return Err(StepError::failed(
            &node.id,
            "request max_media_bytes must not exceed the inherited limit",
        ));
    }
    validate_capability_names(node, &policy.requires, "request.requires")
}

pub(crate) fn validate_capability_names(
    node: &NodeDef,
    capabilities: &[String],
    field: &str,
) -> Result<(), StepError> {
    let mut seen = BTreeSet::new();
    for capability in capabilities {
        if !matches!(
            capability.as_str(),
            "tool_use"
                | "json_schema"
                | "structured_output_with_tools"
                | "seed"
                | "reasoning_effort"
                | "image_input"
                | "audio_input"
                | "file_input"
                | "streaming"
                | "temperature"
                | "top_p"
                | "stop_sequences"
                | "tool_choice"
                | "parallel_tool_calls"
                | "verbosity"
        ) {
            return Err(StepError::failed(
                &node.id,
                format!("{field} contains unknown capability `{capability}`"),
            ));
        }
        if !seen.insert(capability) {
            return Err(StepError::failed(
                &node.id,
                format!("{field} contains duplicate capability `{capability}`"),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MediaInput {
    pub(crate) kind: MediaInputKind,
    pub(crate) path: String,
    pub(crate) media_type: String,
    #[serde(default)]
    pub(crate) detail: Option<ImageDetail>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MediaInputKind {
    Image,
    Audio,
    File,
    Video,
}

pub(crate) fn llm_params(node: &NodeDef) -> Result<LlmParams, StepError> {
    node.deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid LLM params: {error}")))
}

pub(crate) fn require_prompt(node: &NodeDef, params: &LlmParams) -> Result<(), StepError> {
    require(node, params.prompt.as_deref(), "prompt")
}
