use qcg_types::ToolChoice;
use std::collections::BTreeSet;

use super::contract::ContractError;
use super::validate::Manifest;

pub(crate) struct GeneratorMetadataRule;

impl GeneratorMetadataRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        if manifest.generator.id.trim().is_empty() {
            return Err(ContractError::Invalid("generator.id is required".into()));
        }
        for (id, requirement) in &manifest.dependencies {
            if id.trim().is_empty() || id.contains('/') || *id == "." || *id == ".." {
                return Err(ContractError::Invalid(format!(
                    "dependency id `{id}` must be a safe relative path component"
                )));
            }
            if id == &manifest.generator.id {
                return Err(ContractError::Invalid(format!(
                    "generator `{}` must not depend on itself",
                    manifest.generator.id
                )));
            }
            semver::VersionReq::parse(requirement).map_err(|error| {
                ContractError::Invalid(format!(
                    "dependency `{id}` version requirement `{requirement}` is invalid: {error}"
                ))
            })?;
        }
        for (secret_name, secret) in &manifest.secrets {
            let sources =
                usize::from(secret.env.is_some()) + usize::from(secret.file_env.is_some());
            if sources != 1 {
                return Err(ContractError::Invalid(format!(
                    "secret `{secret_name}` must declare exactly one of env or file_env"
                )));
            }
            let source = secret
                .source_env_name()
                .expect("exactly one secret source was validated");
            if !is_environment_variable_name(source) {
                return Err(ContractError::Invalid(format!(
                    "secret `{secret_name}` has an invalid environment variable name `{source}`"
                )));
            }
        }
        if manifest.budget.max_steps == 0 {
            return Err(ContractError::Invalid(
                "budget.max_steps must be greater than zero".into(),
            ));
        }
        if manifest.budget.max_tokens == Some(0) {
            return Err(ContractError::Invalid(
                "budget.max_tokens must be greater than zero".into(),
            ));
        }
        if manifest.budget.max_elapsed_seconds == Some(0) {
            return Err(ContractError::Invalid(
                "budget.max_elapsed_seconds must be greater than zero".into(),
            ));
        }
        if manifest
            .budget
            .max_cost_usd
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
        {
            return Err(ContractError::Invalid(
                "budget.max_cost_usd must be a finite positive number".into(),
            ));
        }
        for (name, value) in [
            (
                "runtime.command_timeout_seconds",
                manifest.runtime.command_timeout_seconds,
            ),
            (
                "runtime.http_timeout_seconds",
                manifest.runtime.http_timeout_seconds,
            ),
        ] {
            if value == 0 {
                return Err(ContractError::Invalid(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        let containers = &manifest.permissions.containers;
        if containers.enabled && containers.runtime.is_none() {
            return Err(ContractError::Invalid(
                "permissions.containers.runtime is required when containers are enabled".into(),
            ));
        }
        if !containers.enabled && containers.runtime.is_some() {
            return Err(ContractError::Invalid(
                "permissions.containers.runtime requires containers.enabled = true".into(),
            ));
        }
        if containers
            .images
            .iter()
            .any(|image| !image.contains("@sha256:"))
        {
            return Err(ContractError::Invalid(
                "permissions.containers.images must be pinned by digest".into(),
            ));
        }
        for (name, value) in [
            (
                "runtime.command_input_limit_bytes",
                manifest.runtime.command_input_limit_bytes,
            ),
            (
                "runtime.command_output_limit_bytes",
                manifest.runtime.command_output_limit_bytes,
            ),
            (
                "runtime.http_body_limit_bytes",
                manifest.runtime.http_body_limit_bytes,
            ),
            (
                "runtime.file_input_limit_bytes",
                manifest.runtime.file_input_limit_bytes,
            ),
            (
                "runtime.file_count_limit",
                manifest.runtime.file_count_limit,
            ),
            (
                "runtime.input_total_limit_bytes",
                manifest.runtime.input_total_limit_bytes,
            ),
            (
                "runtime.template_output_limit_bytes",
                manifest.runtime.template_output_limit_bytes,
            ),
            (
                "runtime.output_file_limit_bytes",
                manifest.runtime.output_file_limit_bytes,
            ),
            (
                "runtime.output_total_limit_bytes",
                manifest.runtime.output_total_limit_bytes,
            ),
            (
                "runtime.output_artifact_limit",
                manifest.runtime.output_artifact_limit,
            ),
            (
                "runtime.template_source_limit_bytes",
                manifest.runtime.template_source_limit_bytes,
            ),
            (
                "runtime.template_context_limit_bytes",
                manifest.runtime.template_context_limit_bytes,
            ),
            (
                "runtime.journal_event_limit_bytes",
                manifest.runtime.journal_event_limit_bytes,
            ),
            (
                "runtime.journal_total_limit_bytes",
                manifest.runtime.journal_total_limit_bytes,
            ),
            (
                "runtime.journal_event_count_limit",
                manifest.runtime.journal_event_count_limit,
            ),
            (
                "runtime.state_limit_bytes",
                manifest.runtime.state_limit_bytes,
            ),
        ] {
            if value == Some(0) {
                return Err(ContractError::Invalid(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        // http_redirect_limit allows zero (follow no redirects); None means unlimited.
        if manifest.runtime.template_fuel == 0 {
            return Err(ContractError::Invalid(
                "runtime.template_fuel must be greater than zero".into(),
            ));
        }
        if let Some(llm) = &manifest.llm {
            if llm
                .temperature
                .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
            {
                return Err(ContractError::Invalid(
                    "[llm].temperature must be finite and between 0 and 2".into(),
                ));
            }
            if llm
                .top_p
                .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
            {
                return Err(ContractError::Invalid(
                    "[llm].top_p must be finite and between 0 and 1".into(),
                ));
            }
            if llm.temperature.is_some() && llm.top_p.is_some() {
                return Err(ContractError::Invalid(
                    "[llm].temperature and [llm].top_p are mutually exclusive".into(),
                ));
            }
            match llm.max_tokens {
                None => {
                    return Err(ContractError::Invalid(
                        "[llm].max_tokens is required".into(),
                    ));
                }
                Some(0) => {
                    return Err(ContractError::Invalid(
                        "[llm].max_tokens must be greater than zero".into(),
                    ));
                }
                Some(_) => {}
            }
            if llm.max_context_bytes == Some(0) {
                return Err(ContractError::Invalid(
                    "[llm].max_context_bytes must be greater than zero".into(),
                ));
            }
            if llm.max_context_tokens == Some(0) {
                return Err(ContractError::Invalid(
                    "[llm].max_context_tokens must be greater than zero".into(),
                ));
            }
            if llm.max_media_bytes == Some(0) {
                return Err(ContractError::Invalid(
                    "[llm].max_media_bytes must be greater than zero".into(),
                ));
            }
            if llm.reasoning_effort.is_some() && (llm.temperature.is_some() || llm.top_p.is_some())
            {
                return Err(ContractError::Invalid(
                    "[llm].temperature and [llm].top_p must be omitted when reasoning_effort is set"
                        .into(),
                ));
            }
            if llm.reasoning_effort.is_some() && llm.seed.is_some() {
                return Err(ContractError::Invalid(
                    "[llm].seed must be omitted when reasoning_effort is set".into(),
                ));
            }
            if llm.stop_sequences.len() > 8
                || llm
                    .stop_sequences
                    .iter()
                    .any(|value| value.is_empty() || value.len() > 1_024)
            {
                return Err(ContractError::Invalid(
                    "[llm].stop_sequences must contain at most 8 non-empty strings of at most 1024 bytes"
                        .into(),
                ));
            }
            if llm.tool_choice.as_ref().is_some_and(
                |choice| matches!(choice, ToolChoice::Tool { tool } if tool.trim().is_empty()),
            ) {
                return Err(ContractError::Invalid(
                    "[llm].tool_choice.tool must not be empty".into(),
                ));
            }
            if let Some(model) = &llm.model {
                if model.provider.trim().is_empty() || model.model.trim().is_empty() {
                    return Err(ContractError::Invalid(
                        "[llm].model provider and model must not be empty".into(),
                    ));
                }
                for (name, value) in [
                    (
                        "input_cost_per_million_usd",
                        model.input_cost_per_million_usd,
                    ),
                    (
                        "output_cost_per_million_usd",
                        model.output_cost_per_million_usd,
                    ),
                ] {
                    if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
                        return Err(ContractError::Invalid(format!(
                            "model `{}/{}` {name} must be finite and non-negative",
                            model.provider, model.model
                        )));
                    }
                }
                if manifest.budget.max_cost_usd.is_some()
                    && (model.input_cost_per_million_usd.is_none()
                        || model.output_cost_per_million_usd.is_none())
                {
                    return Err(ContractError::Invalid(format!(
                        "model `{}/{}` must declare input and output pricing when budget.max_cost_usd is set",
                        model.provider, model.model
                    )));
                }
            } else if manifest.budget.max_cost_usd.is_some() {
                return Err(ContractError::Invalid(
                    "[llm].model must be declared when budget.max_cost_usd is set because the provider default does not carry pricing"
                        .into(),
                ));
            }
            let mut model_ids = BTreeSet::new();
            for model in &llm.models {
                if model.provider.trim().is_empty() || model.model.trim().is_empty() {
                    return Err(ContractError::Invalid(
                        "[llm].models provider and model must not be empty".into(),
                    ));
                }
                if !model_ids.insert((model.provider.as_str(), model.model.as_str())) {
                    return Err(ContractError::Invalid(format!(
                        "[llm].models contains duplicate model `{}/{}`",
                        model.provider, model.model
                    )));
                }
                for (name, value) in [
                    (
                        "input_cost_per_million_usd",
                        model.input_cost_per_million_usd,
                    ),
                    (
                        "output_cost_per_million_usd",
                        model.output_cost_per_million_usd,
                    ),
                ] {
                    if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
                        return Err(ContractError::Invalid(format!(
                            "model `{}/{}` {name} must be finite and non-negative",
                            model.provider, model.model
                        )));
                    }
                }
            }
            let mut requires = BTreeSet::new();
            for capability in &llm.requires {
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
                    return Err(ContractError::Invalid(format!(
                        "[llm].requires contains unknown capability `{capability}`"
                    )));
                }
                if !requires.insert(capability) {
                    return Err(ContractError::Invalid(format!(
                        "[llm].requires contains duplicate capability `{capability}`"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn is_environment_variable_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_uppercase() || (index > 0 && byte.is_ascii_digit())
        })
}
