use crate::{AssetSpec, FieldType, GeneratorMeta, InputField, InputSpec};
use camino::Utf8Path;
use qcg_policy::validate_bounded_json_schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use super::assets::{AssetRule, validate_field_value, validate_input_sizes};
use super::contract::{ContractError, stripped_error_message};
use super::flow::{CommandPermissionRule, FlowNodeRule, OutputArtifactRule, ToolRule};
use super::llm::{LlmConfig, RunBudget, RuntimeLimits};
use super::metadata::GeneratorMetadataRule;
use super::nodes::{ExhaustedAction, NodeDef, OnFail};
use super::outputs::{FailurePolicy, JournalPolicy, OutputSpec};
use super::resources::{Permissions, ResourceDef, ResourceKind, SecretRef, ToolDef};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub generator: GeneratorMeta,
    #[serde(default)]
    pub llm: Option<LlmConfig>,
    #[serde(default)]
    pub inputs: InputSpec,
    #[serde(default)]
    pub resources: BTreeMap<String, ResourceDef>,
    #[serde(default)]
    pub tools: BTreeMap<String, ToolDef>,
    #[serde(default)]
    pub permissions: Permissions,
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretRef>,
    #[serde(default)]
    pub runtime: RuntimeLimits,
    #[serde(default)]
    pub budget: RunBudget,
    #[serde(default)]
    pub flow: Vec<NodeDef>,
    #[serde(default)]
    pub parallel: Vec<String>,
    #[serde(default)]
    pub blocks: BTreeMap<String, Vec<NodeDef>>,
    #[serde(default)]
    pub outputs: OutputSpec,
    #[serde(default)]
    pub failure: FailurePolicy,
    #[serde(default)]
    pub journal: JournalPolicy,
    #[serde(default)]
    pub assets: AssetSpec,
    /// Generator dependencies: id to semver requirement, resolved at
    /// install time from configured registries. No code sharing happens at
    /// runtime; each dependency installs as an independent generator.
    #[serde(default)]
    pub dependencies: BTreeMap<String, String>,
}

impl Manifest {
    pub fn resolve_inputs(
        &self,
        mut values: BTreeMap<String, Value>,
    ) -> Result<BTreeMap<String, Value>, ContractError> {
        validate_input_sizes(&values, &self.runtime)?;
        for stage in &self.inputs.stages {
            let bag = crate::ValueBag::with_inputs(values.clone());
            if !bag.eval_bool(stage.when.as_ref()).map_err(|error| {
                ContractError::Invalid(format!(
                    "invalid stage `{}` when expression: {error}",
                    stage.id
                ))
            })? {
                continue;
            }
            for field in &stage.fields {
                if !values.contains_key(&field.id)
                    && let Some(default) = &field.default
                {
                    values.insert(field.id.clone(), default.clone());
                }
                let Some(value) = values.get(&field.id) else {
                    if field.required {
                        return Err(ContractError::Invalid(format!(
                            "required input `{}` is missing",
                            field.id
                        )));
                    }
                    continue;
                };
                validate_field_value(field, value, &self.runtime)?;
            }
        }
        for id in values.keys() {
            if !self
                .inputs
                .stages
                .iter()
                .flat_map(|stage| stage.fields.iter())
                .any(|field| field.id == *id)
            {
                return Err(ContractError::Invalid(format!("unknown input `{id}`")));
            }
        }
        validate_input_sizes(&values, &self.runtime)?;
        Ok(values)
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        let mut errors = Vec::new();
        let mut check = |result: Result<(), ContractError>| {
            if let Err(error) = result {
                errors.push(stripped_error_message(error));
            }
        };
        check(GeneratorMetadataRule.validate(self));
        check(InputRule.validate(self));
        check(ResourceRule.validate(self));
        check(FlowNodeRule.validate(self));
        check(CommandPermissionRule.validate(self));
        check(ToolRule.validate(self));
        check(OutputArtifactRule.validate(self));
        check(AssetRule.validate(self));
        match errors.len() {
            0 => Ok(()),
            1 => Err(ContractError::Invalid(errors.pop().unwrap_or_default())),
            count => Err(ContractError::Invalid(format!(
                "invalid manifest ({count} problems):\n{}",
                errors
                    .iter()
                    .enumerate()
                    .map(|(index, error)| format!("{}. {error}", index + 1))
                    .collect::<Vec<_>>()
                    .join("\n")
            ))),
        }
    }
}

pub(crate) fn validate_asset_files(
    root: &Utf8Path,
    assets: &AssetSpec,
) -> Result<(), ContractError> {
    for path in &assets.files {
        let file = crate::resolve_package_path(root, path).map_err(|error| {
            ContractError::Invalid(format!("asset file `{path}` cannot be read: {error}"))
        })?;
        if !file.is_file() {
            return Err(ContractError::Invalid(format!(
                "asset file `{path}` must be a file inside the generator package"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_resource_files(
    root: &Utf8Path,
    resources: &BTreeMap<String, ResourceDef>,
) -> Result<(), ContractError> {
    for (name, resource) in resources {
        let Some(path) = resource.path.as_deref() else {
            continue;
        };
        let resolved = crate::resolve_package_path(root, path).map_err(|error| {
            ContractError::Invalid(format!(
                "resource `{name}` path `{path}` is invalid: {error}"
            ))
        })?;
        let valid = match resource.kind.as_str() {
            "file" | "openapi" => resolved.is_file(),
            "dir" => resolved.is_dir(),
            "skill" => resolved.is_file() || resolved.is_dir(),
            _ => true,
        };
        if !valid {
            let expected = match resource.kind.as_str() {
                "dir" => "a directory",
                "skill" => "a file or directory",
                "file" | "openapi" => "a file",
                _ => "a valid package path",
            };
            return Err(ContractError::Invalid(format!(
                "resource `{name}` path `{path}` for type `{}` must be {expected}",
                resource.kind
            )));
        }
    }
    Ok(())
}

pub fn validate_form_values(
    fields: &[InputField],
    values: &Value,
    runtime: &RuntimeLimits,
) -> Result<(), ContractError> {
    let object = values
        .as_object()
        .ok_or_else(|| ContractError::Invalid("form values must be a JSON object".into()))?;
    validate_input_sizes(object, runtime)?;
    for field in fields {
        match object.get(&field.id) {
            Some(value) => validate_field_value(field, value, runtime)?,
            None if field.required => {
                return Err(ContractError::Invalid(format!(
                    "required form field `{}` is missing",
                    field.id
                )));
            }
            None => {}
        }
    }
    for id in object.keys() {
        if !fields.iter().any(|field| field.id == *id) {
            return Err(ContractError::Invalid(format!("unknown form field `{id}`")));
        }
    }
    Ok(())
}

struct InputRule;

struct ResourceRule;

impl ResourceRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for (name, resource) in &manifest.resources {
            match resource.kind {
                ResourceKind::File | ResourceKind::Dir | ResourceKind::Skill => {
                    if resource.path.is_none() || resource.url.is_some() {
                        return Err(ContractError::Invalid(format!(
                            "resource `{name}` type `{}` requires path and forbids url",
                            resource.kind
                        )));
                    }
                }
                ResourceKind::Url => {
                    if resource.url.is_none() || resource.path.is_some() {
                        return Err(ContractError::Invalid(format!(
                            "resource `{name}` type `url` requires url and forbids path"
                        )));
                    }
                }
                ResourceKind::Openapi => {
                    if resource.path.is_some() == resource.url.is_some() {
                        return Err(ContractError::Invalid(format!(
                            "resource `{name}` type `openapi` requires exactly one of path or url"
                        )));
                    }
                }
                ResourceKind::Exec => validate_exec_resource(
                    name,
                    resource,
                    &manifest.permissions,
                    &manifest.runtime,
                )?,
            }
        }
        Ok(())
    }
}

fn validate_exec_resource(
    name: &str,
    resource: &ResourceDef,
    permissions: &Permissions,
    runtime: &RuntimeLimits,
) -> Result<(), ContractError> {
    if resource.path.is_some() || resource.url.is_some() {
        return Err(ContractError::Invalid(format!(
            "resource `{name}` type `exec` forbids path and url"
        )));
    }
    if resource
        .params
        .keys()
        .any(|key| !matches!(key.as_str(), "command" | "max_bytes"))
    {
        return Err(ContractError::Invalid(format!(
            "resource `{name}` type `exec` accepts only command and max_bytes params"
        )));
    }
    let command = resource
        .params
        .get("command")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ContractError::Invalid(format!(
                "resource `{name}` type `exec` requires a command array"
            ))
        })?;
    let command = command
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>()
        .filter(|command| !command.is_empty())
        .ok_or_else(|| {
            ContractError::Invalid(format!(
                "resource `{name}` type `exec` command must contain only strings and be non-empty"
            ))
        })?;
    if resource
        .params
        .get("max_bytes")
        .is_some_and(|value| value.as_u64().is_none_or(|value| value == 0))
    {
        return Err(ContractError::Invalid(format!(
            "resource `{name}` type `exec` max_bytes must be a positive integer"
        )));
    }
    if let Some(command_limit) = runtime.command_output_limit_bytes {
        let command_limit = u64::try_from(command_limit).map_err(|_| {
            ContractError::Invalid("runtime.command_output_limit_bytes is too large".into())
        })?;
        if let Some(max_bytes) = resource.params.get("max_bytes").and_then(Value::as_u64)
            && max_bytes > command_limit
        {
            return Err(ContractError::Invalid(format!(
                "resource `{name}` type `exec` max_bytes ({max_bytes}) exceeds runtime.command_output_limit_bytes ({command_limit})"
            )));
        }
    }
    let Some((bin, args)) = command.split_first() else {
        return Err(ContractError::Invalid(format!(
            "resource `{name}` type `exec` command must not be empty"
        )));
    };
    let allowed = permissions.commands.iter().any(|permission| {
        permission.bin == *bin
            && permission.args.len() == args.len()
            && permission
                .args
                .iter()
                .zip(args)
                .all(|(pattern, actual)| pattern == "*" || pattern == actual)
    });
    if !allowed {
        return Err(ContractError::Invalid(format!(
            "resource `{name}` type `exec` command is not declared in permissions.commands"
        )));
    }
    Ok(())
}

impl InputRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        let mut stage_ids = BTreeSet::new();
        let mut field_ids = BTreeSet::new();
        for stage in &manifest.inputs.stages {
            if stage.id.trim().is_empty() || !stage_ids.insert(stage.id.as_str()) {
                return Err(ContractError::Invalid(format!(
                    "input stage id `{}` must be non-empty and unique",
                    stage.id
                )));
            }
            for field in &stage.fields {
                if field.id.trim().is_empty() || !field_ids.insert(field.id.as_str()) {
                    return Err(ContractError::Invalid(format!(
                        "input field id `{}` must be non-empty and unique",
                        field.id
                    )));
                }
                validate_input_field_contract("input", field, &manifest.runtime)?;
            }
        }
        for node in manifest
            .flow
            .iter()
            .chain(manifest.blocks.values().flatten())
        {
            let exhausted = match node.on_fail.as_ref() {
                Some(OnFail::Repair { on_exhausted, .. })
                | Some(OnFail::Regenerate { on_exhausted, .. }) => on_exhausted,
                _ => continue,
            };
            let ExhaustedAction::AskUser { fields, .. } = exhausted else {
                continue;
            };
            let mut form_field_ids = BTreeSet::new();
            for field in fields {
                if field.id.trim().is_empty() || !form_field_ids.insert(field.id.as_str()) {
                    return Err(ContractError::Invalid(format!(
                        "node `{}` on_exhausted input field id `{}` must be non-empty and unique",
                        node.id, field.id
                    )));
                }
                validate_input_field_contract(
                    &format!("node `{}` on_exhausted input", node.id),
                    field,
                    &manifest.runtime,
                )?;
            }
        }
        Ok(())
    }
}

fn validate_input_field_contract(
    scope: &str,
    field: &InputField,
    runtime: &RuntimeLimits,
) -> Result<(), ContractError> {
    if let FieldType::Custom(kind) = &field.kind {
        validate_namespaced_id(kind).map_err(|error| {
            ContractError::Invalid(format!(
                "{scope} `{}` has invalid custom field type: {error}",
                field.id
            ))
        })?;
        if field.schema.is_none() {
            return Err(ContractError::Invalid(format!(
                "{scope} `{}` custom field type `{kind}` requires schema",
                field.id
            )));
        }
    }
    if let Some(schema) = &field.schema {
        validate_bounded_json_schema(schema).map_err(|error| {
            ContractError::Invalid(format!(
                "{scope} `{}` has invalid or unsafe JSON Schema: {error}",
                field.id
            ))
        })?;
    }
    if let Some(default) = &field.default {
        validate_field_value(field, default, runtime)?;
    }
    Ok(())
}

fn validate_namespaced_id(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("identifier must not be empty".into());
    }
    if value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
    }) {
        Ok(())
    } else {
        Err(format!(
            "identifier `{value}` must use only lowercase ASCII letters, digits, `_`, and `.`"
        ))
    }
}
