use qcg_policy::is_safe_relative_path;
use serde::Deserialize;
use std::collections::BTreeSet;

use super::assets::{validate_artifact_pattern, validate_tool};
use super::contract::{ContractError, stripped_error_message};
use super::nodes::{ContextRef, NodeDef};
use super::resources::CommandIsolation;
use super::validate::Manifest;

pub(crate) struct FlowNodeRule;

pub(crate) struct CommandPermissionRule;

impl CommandPermissionRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for command in &manifest.permissions.commands {
            let isolation = command.isolation.as_ref().ok_or_else(|| {
                ContractError::Invalid(format!(
                    "command permission `{}` must declare isolation as `container` or `trusted_host`",
                    command.bin
                ))
            })?;
            match isolation {
                CommandIsolation::Container => {
                    let image = command.image.as_deref().ok_or_else(|| {
                        ContractError::Invalid(format!(
                            "container-isolated command `{}` must declare image",
                            command.bin
                        ))
                    })?;
                    if !image.contains("@sha256:") {
                        return Err(ContractError::Invalid(format!(
                            "container-isolated command `{}` image must be pinned by digest",
                            command.bin
                        )));
                    }
                    if !manifest.permissions.containers.enabled
                        || !manifest
                            .permissions
                            .containers
                            .images
                            .iter()
                            .any(|allowed| allowed == image)
                    {
                        return Err(ContractError::Invalid(format!(
                            "container-isolated command `{}` image `{image}` must be allowed by permissions.containers",
                            command.bin
                        )));
                    }
                }
                CommandIsolation::TrustedHost if command.image.is_some() => {
                    return Err(ContractError::Invalid(format!(
                        "trusted-host command `{}` must not declare a container image",
                        command.bin
                    )));
                }
                CommandIsolation::TrustedHost => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ForeachValidationParams {
    max_iterations: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CheckToolValidationParams {
    tool: Option<String>,
}

impl FlowNodeRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        let mut ids = BTreeSet::new();
        let mut errors = Vec::new();
        for node in &manifest.flow {
            if let Err(error) = self.validate_node(node, manifest, &mut ids) {
                errors.push(stripped_error_message(error));
            }
        }
        match errors.len() {
            0 => Ok(()),
            1 => Err(ContractError::Invalid(errors.pop().unwrap_or_default())),
            count => Err(ContractError::Invalid(format!(
                "{count} invalid flow nodes:\n{}",
                errors
                    .iter()
                    .enumerate()
                    .map(|(index, error)| format!("{}. {error}", index + 1))
                    .collect::<Vec<_>>()
                    .join("\n")
            ))),
        }
    }

    fn validate_node(
        &self,
        node: &NodeDef,
        manifest: &Manifest,
        ids: &mut BTreeSet<String>,
    ) -> Result<(), ContractError> {
        if node.id.trim().is_empty() {
            return Err(ContractError::Invalid("flow node id is required".into()));
        }
        if !ids.insert(node.id.clone()) {
            return Err(ContractError::Invalid(format!(
                "duplicate flow node `{}`",
                node.id
            )));
        }
        if node.kind.as_str() == "foreach"
            && node
                .deserialize_params::<ForeachValidationParams>()
                .map_err(|error| {
                    ContractError::Invalid(format!(
                        "foreach node `{}` has invalid params: {error}",
                        node.id
                    ))
                })?
                .max_iterations
                .is_none()
        {
            return Err(ContractError::Invalid(format!(
                "foreach node `{}` must declare max_iterations",
                node.id
            )));
        }
        if node.kind.as_str() == "check.tool" {
            let params: CheckToolValidationParams = node.deserialize_params().map_err(|error| {
                ContractError::Invalid(format!(
                    "check.tool node `{}` has invalid params: {error}",
                    node.id
                ))
            })?;
            let tool_name = params.tool.as_deref().ok_or_else(|| {
                ContractError::Invalid(format!("check.tool node `{}` must declare tool", node.id))
            })?;
            if !manifest.tools.contains_key(tool_name) {
                return Err(ContractError::Invalid(format!(
                    "check.tool node `{}` references unknown tool `{tool_name}`",
                    node.id
                )));
            }
        }
        if node.kind.is_llm() && manifest.llm.is_none() {
            return Err(ContractError::Invalid(format!(
                "node `{}` uses `{}` but [llm] is not declared",
                node.id, node.kind
            )));
        }
        for context in &node.context {
            validate_context_ref(&node.id, context, manifest)?;
        }
        if let Some(retry) = &node.retry {
            if retry.max_attempts == 0 || retry.max_attempts > 16 {
                return Err(ContractError::Invalid(format!(
                    "node `{}` retry.max_attempts must be between 1 and 16",
                    node.id
                )));
            }
            if retry.backoff_ms > 60_000 {
                return Err(ContractError::Invalid(format!(
                    "node `{}` retry.backoff_ms must not exceed 60000",
                    node.id
                )));
            }
            if retry.timeout_secs.is_some_and(|timeout| timeout == 0) {
                return Err(ContractError::Invalid(format!(
                    "node `{}` retry.timeout_secs must be at least 1 when set",
                    node.id
                )));
            }
        }
        Ok(())
    }
}

fn validate_context_ref(
    node_id: &str,
    context: &ContextRef,
    manifest: &Manifest,
) -> Result<(), ContractError> {
    let ContextRef::Resource(reference) = context else {
        if let ContextRef::Short(reference) = context {
            if reference == "inputs.*"
                || reference.starts_with("inputs.")
                || reference.starts_with("steps.")
            {
                return Ok(());
            }
            if let Some(resource) = reference.strip_prefix("resources.") {
                let name = resource.split_once('#').map_or(resource, |(name, _)| name);
                if manifest.resources.contains_key(name) {
                    return Ok(());
                }
                return Err(ContractError::Invalid(format!(
                    "node `{node_id}` context references unknown resource `{name}`"
                )));
            }
            return Err(ContractError::Invalid(format!(
                "node `{node_id}` has unsupported context reference `{reference}`"
            )));
        }
        return Ok(());
    };
    let resource = manifest.resources.get(&reference.resource).ok_or_else(|| {
        ContractError::Invalid(format!(
            "node `{node_id}` context references unknown resource `{}`",
            reference.resource
        ))
    })?;
    let select = reference.select.as_deref();
    if select.is_none() && (reference.tag.is_some() || reference.path.is_some()) {
        return Err(ContractError::Invalid(format!(
            "node `{node_id}` resource context tag/path requires select"
        )));
    }
    match resource.kind.as_str() {
        "openapi" => match select {
            None if reference.tag.is_none() && reference.path.is_none() => Ok(()),
            Some("paths") if reference.tag.is_none() && reference.path.is_none() => Ok(()),
            Some("operations") if reference.path.is_none() => Ok(()),
            _ => Err(ContractError::Invalid(format!(
                "node `{node_id}` has invalid OpenAPI selector for resource `{}`",
                reference.resource
            ))),
        },
        "skill" => match select {
            None | Some("instructions" | "meta")
                if reference.tag.is_none() && reference.path.is_none() =>
            {
                Ok(())
            }
            Some("file" | "files")
                if reference.tag.is_none()
                    && reference.path.as_deref().is_some_and(is_safe_relative_path) =>
            {
                Ok(())
            }
            _ => Err(ContractError::Invalid(format!(
                "node `{node_id}` has invalid skill selector for resource `{}`",
                reference.resource
            ))),
        },
        "dir" => match select {
            None | Some("tree" | "files")
                if reference.tag.is_none() && reference.path.is_none() =>
            {
                Ok(())
            }
            Some("file")
                if reference.tag.is_none()
                    && reference.path.as_deref().is_some_and(is_safe_relative_path) =>
            {
                Ok(())
            }
            _ => Err(ContractError::Invalid(format!(
                "node `{node_id}` has invalid directory selector for resource `{}`",
                reference.resource
            ))),
        },
        _ if select.is_none() && reference.tag.is_none() && reference.path.is_none() => Ok(()),
        _ => Err(ContractError::Invalid(format!(
            "node `{node_id}` resource `{}` does not support selectors",
            reference.resource
        ))),
    }
}

pub(crate) struct ToolRule;

impl ToolRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for (name, tool) in &manifest.tools {
            validate_tool(name, tool, &manifest.permissions)?;
        }
        Ok(())
    }
}

pub(crate) struct OutputArtifactRule;

impl OutputArtifactRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        let mut declared = BTreeSet::new();
        if let Some((block, node)) = manifest
            .blocks
            .iter()
            .flat_map(|(block, nodes)| {
                nodes
                    .iter()
                    .filter(|node| node.artifact.is_some())
                    .map(move |node| (block, node))
            })
            .next()
        {
            return Err(ContractError::Invalid(format!(
                "block `{block}` node `{}` cannot declare a top-level artifact",
                node.id
            )));
        }
        for node in manifest.flow.iter().filter(|node| node.artifact.is_some()) {
            let artifact = node.artifact.as_ref().ok_or_else(|| {
                ContractError::Invalid(format!("node `{}` lost its artifact declaration", node.id))
            })?;
            validate_artifact_mime(artifact.mime.as_deref(), &format!("node `{}`", node.id))?;
            let Some(path) = node.artifact_path_template() else {
                return Err(ContractError::Invalid(format!(
                    "node `{}` declares artifact metadata but its step has no static output_file, target, or destination parameter",
                    node.id
                )));
            };
            validate_artifact_pattern(path, "artifact path")?;
            if !declared.insert(path.to_string()) {
                return Err(ContractError::Invalid(format!(
                    "artifact path `{path}` is declared by more than one node"
                )));
            }
        }
        for extra in &manifest.outputs.extras {
            validate_artifact_pattern(&extra.glob, "output glob")?;
            validate_artifact_mime(extra.mime.as_deref(), &format!("glob `{}`", extra.glob))?;
        }
        Ok(())
    }
}

fn validate_artifact_mime(mime: Option<&str>, declaration: &str) -> Result<(), ContractError> {
    let Some(mime) = mime else {
        return Ok(());
    };
    mime.parse::<mime::Mime>().map_err(|error| {
        ContractError::Invalid(format!(
            "artifact {declaration} has invalid MIME type `{mime}`: {error}"
        ))
    })?;
    Ok(())
}
