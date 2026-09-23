use crate::{
    AssetSpec, FieldType, GeneratorMeta, InputField, InputSpec, is_skill_library_entry,
    parse_skill_doc, validate_skill_doc,
};
use camino::Utf8Path;
use qcg_policy::validate_bounded_json_schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use super::assets::{AssetRule, validate_field_value, validate_input_sizes};
use super::contract::{ContractError, stripped_error_message};
use super::flow::{CommandPermissionRule, FlowNodeRule, OutputArtifactRule, ToolRule};
use super::hooks::{HooksConfig, HooksRule};
use super::llm::{LlmConfig, RunBudget, RuntimeLimits};
use super::metadata::GeneratorMetadataRule;
use super::nodes::{ExhaustedAction, NodeDef, OnFail};
use super::outputs::{FailurePolicy, OutputSpec, RetentionPolicy};
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
    pub hooks: HooksConfig,
    #[serde(default)]
    pub retention: RetentionPolicy,
    /// Observation-stream policy (mechanism/policy separation, ADR 0001).
    /// Durable records are never filtered by this section; class keys that
    /// name a durable record are rejected at contract validation.
    #[serde(default)]
    pub audit: qcg_policy::AuditConfig,
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
        // File inputs count toward `runtime.file_count_limit` exactly like
        // collected artifacts and tool files do; only files in active
        // stages count, because inactive stages never materialize.
        let mut file_inputs = 0_usize;
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
                if field.kind == FieldType::File {
                    file_inputs = file_inputs.checked_add(1).ok_or_else(|| {
                        ContractError::Invalid("input file count overflowed usize".into())
                    })?;
                    if let Some(limit) = self.runtime.file_count_limit
                        && file_inputs > limit
                    {
                        return Err(ContractError::Invalid(format!(
                            "input file count {file_inputs} exceeds runtime.file_count_limit {limit}"
                        )));
                    }
                }
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
        check(HooksRule::validate(self));
        check(CommandPermissionRule.validate(self));
        check(ToolRule.validate(self));
        check(OutputArtifactRule.validate(self));
        check(AssetRule.validate(self));
        // Colon ambiguity backstop (Q1): `parallel` entries name flow nodes,
        // so they share the node-id `:` ban. Flow and block ids are checked
        // in `FlowNodeRule`; this covers the parallel list itself.
        for entry in &self.parallel {
            if entry.contains(':') {
                errors.push(format!(
                    "parallel entry `{entry}` must not contain `:`; confirmation ids use `:` as a separator"
                ));
            }
        }
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
            "dir" | "skill" | "skill_library" => resolved.is_dir(),
            _ => true,
        };
        if !valid {
            let expected = match resource.kind.as_str() {
                "dir" | "skill_library" => "a directory",
                "skill" => "a directory containing SKILL.md",
                "file" | "openapi" => "a file",
                _ => "a valid package path",
            };
            return Err(ContractError::Invalid(format!(
                "resource `{name}` path `{path}` for type `{}` must be {expected}",
                resource.kind
            )));
        }
        match resource.kind.as_str() {
            "skill" => validate_skill_resource(name, &resolved)?,
            "skill_library" => validate_skill_library(name, &resolved)?,
            _ => {}
        }
    }
    Ok(())
}

const MAX_SKILL_LIBRARY_ENTRIES: usize = 4096;

fn validate_skill_resource(name: &str, root: &Utf8Path) -> Result<(), ContractError> {
    let skill_path = root.join("SKILL.md");
    if !skill_path.is_file() {
        return Err(ContractError::Invalid(format!(
            "skill resource `{name}` must contain SKILL.md at `{skill_path}`"
        )));
    }
    read_and_validate_skill(name, &skill_path, root.file_name())
}

fn validate_skill_library(name: &str, root: &Utf8Path) -> Result<(), ContractError> {
    let entries = std::fs::read_dir(root).map_err(|error| {
        ContractError::Invalid(format!(
            "skill_library resource `{name}` cannot read `{root}`: {error}"
        ))
    })?;
    let mut inspected = 0_usize;
    for entry in entries {
        let entry = entry.map_err(|error| {
            ContractError::Invalid(format!(
                "skill_library resource `{name}` cannot read an entry: {error}"
            ))
        })?;
        inspected += 1;
        if inspected > MAX_SKILL_LIBRARY_ENTRIES {
            return Err(ContractError::Invalid(format!(
                "skill_library resource `{name}` exceeds {MAX_SKILL_LIBRARY_ENTRIES} entries"
            )));
        }
        let file_type = entry.file_type().map_err(|error| {
            ContractError::Invalid(format!(
                "skill_library resource `{name}` cannot inspect an entry: {error}"
            ))
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let Some(entry_name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !is_skill_library_entry(&entry_name) {
            continue;
        }
        let child = camino::Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ContractError::Invalid(format!(
                "skill_library resource `{name}` entry is not valid UTF-8: {}",
                path.display()
            ))
        })?;
        let skill_path = child.join("SKILL.md");
        if !skill_path.is_file() {
            continue;
        }
        let Some(dir_name) = child.file_name() else {
            continue;
        };
        read_and_validate_skill(name, &skill_path, Some(dir_name))?;
    }
    Ok(())
}

fn read_and_validate_skill(
    resource: &str,
    skill_path: &Utf8Path,
    expected_name: Option<&str>,
) -> Result<(), ContractError> {
    let source = std::fs::read_to_string(skill_path).map_err(|error| {
        ContractError::Invalid(format!(
            "skill resource `{resource}` cannot read `{skill_path}`: {error}"
        ))
    })?;
    let doc = parse_skill_doc(&source).map_err(|error| {
        ContractError::Invalid(format!(
            "skill resource `{resource}` has invalid frontmatter in `{skill_path}`: {error}"
        ))
    })?;
    validate_skill_doc(&doc, expected_name).map_err(|error| {
        ContractError::Invalid(format!(
            "skill resource `{resource}` is invalid in `{skill_path}`: {error}"
        ))
    })?;
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
                ResourceKind::File
                | ResourceKind::Dir
                | ResourceKind::Skill
                | ResourceKind::SkillLibrary => {
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
                ResourceKind::RunRef => {
                    if resource.path.is_some() || resource.url.is_some() {
                        return Err(ContractError::Invalid(format!(
                            "resource `{name}` type `run_ref` forbids path and url"
                        )));
                    }
                    resource.run_ref_params(name)?;
                }
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
    if field.options_from.is_some() {
        return Err(ContractError::Invalid(format!(
            "{scope} `{}` cannot use options_from; dynamic options are only valid for ask_user form fields",
            field.id
        )));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inputs_reject_dynamic_options() {
        let field: InputField = serde_json::from_value(serde_json::json!({
            "id": "model",
            "type": "string",
            "options_from": "steps.list.output.options"
        }))
        .expect("field must deserialize");
        let error = validate_input_field_contract("input", &field, &RuntimeLimits::default())
            .expect_err("options_from must be rejected for [inputs]");
        assert!(error.to_string().contains("ask_user"), "{error}");
    }
}
