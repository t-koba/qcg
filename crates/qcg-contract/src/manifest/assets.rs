use crate::{FieldType, InputField};
use qcg_policy::{is_safe_relative_path, validate_bounded_json_schema};
use qcg_types::{FileValue, FileValueError};
use serde_json::Value;
use std::collections::BTreeSet;

use super::contract::ContractError;
use super::llm::RuntimeLimits;
use super::resources::{Permissions, ToolBackendKind, ToolDef};
use super::validate::Manifest;

pub(crate) struct AssetRule;

impl AssetRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        let mut paths = BTreeSet::new();
        for path in &manifest.assets.files {
            validate_asset_path(path)?;
            if !paths.insert(path.clone()) {
                return Err(ContractError::Invalid(format!(
                    "duplicate asset file `{path}`"
                )));
            }
        }
        let mut dirs = BTreeSet::new();
        for dir in &manifest.assets.dirs {
            validate_asset_path(dir)?;
            if !dirs.insert(dir.clone()) {
                return Err(ContractError::Invalid(format!(
                    "duplicate asset directory `{dir}`"
                )));
            }
            if manifest.assets.files.iter().any(|file| {
                file == dir
                    || file
                        .strip_prefix(dir)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                    || dir
                        .strip_prefix(file)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            }) {
                return Err(ContractError::Invalid(format!(
                    "asset directory `{dir}` overlaps an asset file"
                )));
            }
        }
        for dir in &manifest.assets.dirs {
            if manifest.assets.dirs.iter().any(|other| {
                other != dir
                    && (dir
                        .strip_prefix(other)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                        || other
                            .strip_prefix(dir)
                            .is_some_and(|suffix| suffix.starts_with('/')))
            }) {
                return Err(ContractError::Invalid(format!(
                    "asset directory `{dir}` overlaps another asset directory"
                )));
            }
        }
        Ok(())
    }
}

fn validate_asset_path(path: &str) -> Result<(), ContractError> {
    if !is_safe_relative_path(path)
        || path.to_ascii_lowercase().contains("%2e")
        || path.to_ascii_lowercase().contains("%2f")
        || path.to_ascii_lowercase().contains("%5c")
    {
        return Err(ContractError::Invalid(format!(
            "asset path `{path}` is not a safe relative path"
        )));
    }
    Ok(())
}

pub(crate) fn validate_artifact_pattern(pattern: &str, kind: &str) -> Result<(), ContractError> {
    if !is_safe_relative_path(pattern) {
        return Err(ContractError::Invalid(format!(
            "{kind} `{pattern}` is not a safe relative path"
        )));
    }
    Ok(())
}

pub(crate) fn validate_field_value(
    field: &InputField,
    value: &Value,
    runtime: &RuntimeLimits,
) -> Result<(), ContractError> {
    match field.kind {
        FieldType::Json | FieldType::Custom(_) => {}
        FieldType::File => {
            let file = FileValue::from_value_optional_limit(value, runtime.file_input_limit_bytes)
                .map_err(|error| match error {
                    FileValueError::TooLarge {
                        actual_bytes,
                        limit_bytes,
                    } => ContractError::PayloadTooLarge {
                        field: field.id.clone(),
                        actual_bytes,
                        limit_bytes,
                    },
                    error => ContractError::Invalid(format!(
                        "input `{}` is an invalid file value: {error}",
                        field.id
                    )),
                })?;
            if let Some(pattern) = &field.pattern {
                let regex = regex::Regex::new(pattern).map_err(|error| {
                    ContractError::Invalid(format!(
                        "input `{}` has invalid pattern `{pattern}`: {error}",
                        field.id
                    ))
                })?;
                if !regex.is_match(&file.name) {
                    return Err(ContractError::Invalid(format!(
                        "input `{}` file name does not match pattern `{pattern}`",
                        field.id
                    )));
                }
            }
        }
        FieldType::String | FieldType::Text | FieldType::NaturalLanguage => {
            let text = value.as_str().ok_or_else(|| {
                ContractError::Invalid(format!("input `{}` must be a string", field.id))
            })?;
            if let Some(pattern) = &field.pattern {
                let regex = regex::Regex::new(pattern).map_err(|error| {
                    ContractError::Invalid(format!(
                        "input `{}` has invalid pattern `{pattern}`: {error}",
                        field.id
                    ))
                })?;
                if !regex.is_match(text) {
                    return Err(ContractError::Invalid(format!(
                        "input `{}` does not match pattern `{pattern}`",
                        field.id
                    )));
                }
            }
        }
        FieldType::Number => {
            if !value.is_number() {
                return Err(ContractError::Invalid(format!(
                    "input `{}` must be a number",
                    field.id
                )));
            }
        }
        FieldType::Boolean => {
            if !value.is_boolean() {
                return Err(ContractError::Invalid(format!(
                    "input `{}` must be a boolean",
                    field.id
                )));
            }
        }
        FieldType::Select => {
            let text = value.as_str().ok_or_else(|| {
                ContractError::Invalid(format!("input `{}` must be a string", field.id))
            })?;
            if !field.options.is_empty() && !field.options.iter().any(|option| option == text) {
                return Err(ContractError::Invalid(format!(
                    "input `{}` must be one of: {}",
                    field.id,
                    field.options.join(", ")
                )));
            }
        }
        FieldType::Multiselect | FieldType::List => {
            let items = value.as_array().ok_or_else(|| {
                ContractError::Invalid(format!("input `{}` must be an array", field.id))
            })?;
            if let Some(min_items) = field.min_items
                && items.len() < min_items
            {
                return Err(ContractError::Invalid(format!(
                    "input `{}` must contain at least {min_items} item(s)",
                    field.id
                )));
            }
            if matches!(field.kind, FieldType::Multiselect) && !field.options.is_empty() {
                for item in items {
                    let text = item.as_str().ok_or_else(|| {
                        ContractError::Invalid(format!(
                            "input `{}` multiselect items must be strings",
                            field.id
                        ))
                    })?;
                    if !field.options.iter().any(|option| option == text) {
                        return Err(ContractError::Invalid(format!(
                            "input `{}` item `{text}` must be one of: {}",
                            field.id,
                            field.options.join(", ")
                        )));
                    }
                }
            }
        }
    }
    if let Some(schema) = &field.schema {
        validate_bounded_json_schema(schema).map_err(|error| {
            ContractError::Invalid(format!(
                "input `{}` has invalid or unsafe JSON Schema: {error}",
                field.id
            ))
        })?;
        let validator = jsonschema::validator_for(schema)
            .expect("bounded JSON Schema was compiled during contract validation");
        if let Err(error) = validator.validate(value) {
            return Err(ContractError::Invalid(format!(
                "input `{}` does not satisfy its JSON Schema at `{}`: {error}",
                field.id,
                error.instance_path()
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_input_sizes<'a>(
    values: impl IntoIterator<Item = (&'a String, &'a Value)>,
    runtime: &RuntimeLimits,
) -> Result<(), ContractError> {
    let mut total = 0_usize;
    for (field, value) in values {
        let bytes = serialized_value_size_optional(value, runtime.file_input_limit_bytes).map_err(
            |error| ContractError::PayloadTooLarge {
                field: field.clone(),
                actual_bytes: error,
                limit_bytes: runtime.file_input_limit_bytes.unwrap_or(usize::MAX),
            },
        )?;
        total = total
            .checked_add(bytes)
            .ok_or_else(|| ContractError::PayloadTooLarge {
                field: "inputs".into(),
                actual_bytes: usize::MAX,
                limit_bytes: runtime.input_total_limit_bytes.unwrap_or(usize::MAX),
            })?;
        if runtime
            .input_total_limit_bytes
            .is_some_and(|limit| total > limit)
        {
            return Err(ContractError::PayloadTooLarge {
                field: "inputs".into(),
                actual_bytes: total,
                limit_bytes: runtime.input_total_limit_bytes.unwrap_or(usize::MAX),
            });
        }
    }
    Ok(())
}

fn serialized_value_size_optional(value: &Value, limit: Option<usize>) -> Result<usize, usize> {
    struct Counter {
        bytes: usize,
        limit: Option<usize>,
    }

    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = self.bytes.saturating_add(bytes.len());
            self.bytes = next;
            if self.limit.is_some_and(|limit| next > limit) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "serialized JSON value exceeds limit",
                ));
            }
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter { bytes: 0, limit };
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => Ok(counter.bytes),
        Err(_error) if limit.is_some_and(|limit| counter.bytes > limit) => Err(counter.bytes),
        Err(_) => Err(counter.bytes),
    }
}

pub(crate) fn validate_tool(
    name: &str,
    tool: &ToolDef,
    permissions: &Permissions,
) -> Result<(), ContractError> {
    if tool.kind.trim().is_empty() {
        return Err(ContractError::Invalid(format!(
            "tool `{name}` kind is required"
        )));
    }
    if tool.command.is_empty() {
        return Err(ContractError::Invalid(format!(
            "tool `{name}` command must not be empty"
        )));
    }
    if tool.timeout_seconds == 0 {
        return Err(ContractError::Invalid(format!(
            "tool `{name}` timeout_seconds must be greater than zero"
        )));
    }
    if tool.output_limit_bytes == 0 {
        return Err(ContractError::Invalid(format!(
            "tool `{name}` output_limit_bytes must be greater than zero"
        )));
    }
    let available = available_tool_backends(tool);
    if available.is_empty() {
        return Err(ContractError::Invalid(format!(
            "tool `{name}` must declare at least one backend"
        )));
    }
    let allowed = if tool.resolution.allowed_backends.is_empty() {
        available.clone()
    } else {
        tool.resolution.allowed_backends.clone()
    };
    for backend in &allowed {
        if !available.contains(backend) {
            return Err(ContractError::Invalid(format!(
                "tool `{name}` resolution allows undeclared backend `{backend}`"
            )));
        }
    }
    for backend in &tool.resolution.preferred_backends {
        if !allowed.contains(backend) {
            return Err(ContractError::Invalid(format!(
                "tool `{name}` resolution prefers backend `{backend}` outside allowed_backends"
            )));
        }
    }
    if tool.backends.host.is_some() && !host_tool_command_allowed(permissions, tool) {
        return Err(ContractError::Invalid(format!(
            "tool `{name}` host backend command is not allowed by permissions.commands"
        )));
    }
    if let Some(container) = &tool.backends.container {
        if !permissions.containers.enabled {
            return Err(ContractError::Invalid(format!(
                "tool `{name}` container backend requires permissions.containers.enabled"
            )));
        }
        if !permissions
            .containers
            .images
            .iter()
            .any(|image| image == &container.image)
        {
            return Err(ContractError::Invalid(format!(
                "tool `{name}` container image `{}` is not allowed by permissions.containers.images",
                container.image
            )));
        }
    }
    Ok(())
}

fn available_tool_backends(tool: &ToolDef) -> Vec<ToolBackendKind> {
    let mut backends = Vec::new();
    if tool.backends.bundled.is_some() {
        backends.push(ToolBackendKind::Bundled);
    }
    if tool.backends.container.is_some() {
        backends.push(ToolBackendKind::Container);
    }
    if tool.backends.host.is_some() {
        backends.push(ToolBackendKind::Host);
    }
    backends
}

fn host_tool_command_allowed(permissions: &Permissions, tool: &ToolDef) -> bool {
    let Some(host) = &tool.backends.host else {
        return true;
    };
    let Some((_, args)) = tool.command.split_first() else {
        return false;
    };
    permissions.commands.iter().any(|permission| {
        permission.bin == host.bin
            && permission.args.len() == args.len()
            && permission
                .args
                .iter()
                .zip(args)
                .all(|(pattern, actual)| pattern == "*" || pattern == actual)
    })
}
