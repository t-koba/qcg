use crate::{
    AssetSpec, FieldType, GeneratorMeta, InputField, InputSpec, is_skill_library_entry,
    parse_skill_doc, validate_skill_doc,
};
use camino::Utf8Path;
use policy::{is_safe_path_component, validate_bounded_json_schema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;

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
    pub audit: policy::AuditConfig,
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
        // HTTP/CLI raw JSON number bounds are enforced before conversion to
        // `Value`; callers supplying pre-parsed values have lost token text.
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
        check(
            policy::AuditPolicy::from_config(&self.audit)
                .map(|_| ())
                .map_err(ContractError::Invalid),
        );
        check(GeneratorMetadataRule.validate(self));
        check(InputRule.validate(self));
        check(ResourceRule.validate(self));
        check(FlowNodeRule.validate(self));
        check(HooksRule::validate(self));
        check(CommandPermissionRule.validate(self));
        check(validate_container_network(self));
        check(validate_container_resources(self));
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

/// A container destination is a single canonical host already present in
/// the existing network allowlist. A wildcard ceiling never qualifies:
/// granting a container one destination must not widen it to everything.
fn validate_container_network(manifest: &Manifest) -> Result<(), ContractError> {
    let Some(host) = manifest.permissions.containers.network.destination_host() else {
        return Ok(());
    };
    if host == "*" {
        return Err(ContractError::Invalid(
            "container destination must name one canonical host, not a wildcard".into(),
        ));
    }
    policy::validate_host(host).map_err(|error| {
        ContractError::Invalid(format!(
            "container destination `{host}` is not a canonical host: {error}"
        ))
    })?;
    if !manifest
        .permissions
        .network
        .iter()
        .any(|entry| entry == host)
    {
        return Err(ContractError::Invalid(format!(
            "container destination `{host}` must be listed explicitly in permissions.network"
        )));
    }
    // Managed-instance families have no destination-scoped network argv in
    // this release; refuse the declaration before any daemon call instead of
    // silently ignoring it and running with no network.
    if matches!(
        manifest.permissions.containers.runtime,
        Some(super::resources::ContainerRuntime::Incus | super::resources::ContainerRuntime::Lxd)
    ) {
        return Err(ContractError::Invalid(
            "container destination requires a Docker-family runtime".into(),
        ));
    }
    Ok(())
}

/// Resource bounds are values, so the same closed grammar is checked at
/// contract load and again in the gateway before any daemon call. Managed-
/// instance families have no matching runner argv here: a non-default bound
/// is refused rather than silently ignored.
fn validate_container_resources(manifest: &Manifest) -> Result<(), ContractError> {
    let resources = &manifest.permissions.containers.resources;
    resources.validate().map_err(ContractError::Invalid)?;
    if !resources.is_default()
        && matches!(
            manifest.permissions.containers.runtime,
            Some(
                super::resources::ContainerRuntime::Incus | super::resources::ContainerRuntime::Lxd
            )
        )
    {
        return Err(ContractError::Invalid(
            "container resource bounds require a Docker-family runtime".into(),
        ));
    }
    Ok(())
}

/// Raw JSON number boundary for values that feed contract evaluation.
///
/// Integers must fit in `[-2^63, 2^64-1]`; decimals and exponents must be
/// finite `f64`. Raw tokens are checked before parsing to `Value` so an
/// oversized integer cannot round to `f64` first. Strings (including
/// numeric-looking keys and string values) are skipped via string-aware
/// scanning.
const JSON_NUMBER_RANGE_MSG: &str = "numbers must be integers in [-2^63, 2^64-1] or finite floats";

/// Deserializes a run-input map while preserving raw JSON number tokens.
///
/// Each value is captured as `Box<RawValue>`, its raw bytes are scanned for
/// number tokens with strings skipped, then accepted values are parsed to
/// ordinary `Value`. Only JSON deserializers carry raw tokens; `Value`
/// deserializers re-serialize already-rounded numbers and cannot recover the
/// original token.
pub fn deserialize_verified_inputs<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_verified_values(deserializer, "input")
}

/// Deserializes an answer map while preserving and validating raw JSON numbers.
pub fn deserialize_verified_answers<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_verified_values(deserializer, "answer")
}

/// Deserializes a step-output patch map while preserving and validating raw JSON numbers.
pub fn deserialize_verified_step_outputs<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_verified_values(deserializer, "step output")
}

fn deserialize_verified_values<'de, D>(
    deserializer: D,
    value_kind: &str,
) -> Result<BTreeMap<String, Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: BTreeMap<String, Box<serde_json::value::RawValue>> =
        BTreeMap::deserialize(deserializer)?;
    verified_values_from_raw_map(raw, value_kind).map_err(serde::de::Error::custom)
}

/// Converts raw JSON input values to ordinary `Value` after range checks.
pub fn verified_inputs_from_raw_map(
    raw: BTreeMap<String, Box<serde_json::value::RawValue>>,
) -> Result<BTreeMap<String, Value>, ContractError> {
    verified_values_from_raw_map(raw, "input")
}

fn verified_values_from_raw_map(
    raw: BTreeMap<String, Box<serde_json::value::RawValue>>,
    value_kind: &str,
) -> Result<BTreeMap<String, Value>, ContractError> {
    let mut out = BTreeMap::new();
    for (id, value) in raw {
        let text = value.get();
        validate_json_number_tokens(text)
            .map_err(|message| ContractError::Invalid(format!("{value_kind} `{id}` {message}")))?;
        let parsed: Value = serde_json::from_str(text).map_err(|error| {
            ContractError::Invalid(format!("{value_kind} `{id}` is invalid JSON: {error}"))
        })?;
        out.insert(id, parsed);
    }
    Ok(out)
}

/// Validates one scalar `--input key=value` raw text after JSON parsing.
///
/// Call only when `serde_json::from_str` succeeded; plain strings that fail
/// parsing are not JSON numbers. The whole raw text is scanned so objects or
/// arrays passed as a scalar still reject embedded out-of-range numbers.
pub fn validate_input_scalar_token(raw: &str) -> Result<(), ContractError> {
    validate_json_number_tokens(raw).map_err(ContractError::Invalid)
}

/// Deserializes JSON after the number-token guard accepted every number in it.
///
/// Out-of-range integers and non-finite floats fail before conversion to the
/// target type, so a typed reader cannot round a token the product refuses.
pub fn parse_json_guarded<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let raw: &serde_json::value::RawValue =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    validate_json_number_tokens(raw.get())?;
    serde_json::from_str(raw.get()).map_err(|error| error.to_string())
}

/// Validates the number tokens in already-validated raw JSON before conversion to `Value`.
///
/// Callers must first ensure the text is syntactically valid JSON (for example, by
/// deserializing it as `RawValue`). Strings are skipped so numeric-looking text is preserved.
pub fn validate_json_number_tokens(raw: &str) -> Result<(), String> {
    let bytes = raw.as_bytes();
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len()
                && matches!(bytes[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
            {
                index += 1;
            }
            validate_number_lexeme(&raw[start..index])?;
            continue;
        }
        index += 1;
    }
    Ok(())
}

fn validate_number_lexeme(lexeme: &str) -> Result<(), String> {
    // `RawValue` has already validated JSON syntax, so this token is a valid
    // JSON number; only its exact integer range or finite-float conversion is
    // left to check.
    let supported = if lexeme.contains(['.', 'e', 'E']) {
        lexeme.parse::<f64>().is_ok_and(f64::is_finite)
    } else {
        lexeme.parse::<i64>().is_ok() || lexeme.parse::<u64>().is_ok()
    };
    if supported {
        Ok(())
    } else {
        Err(JSON_NUMBER_RANGE_MSG.to_string())
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

/// Per-resource selected-content budget, shared with the engine's directory
/// resource loader. An absent param means no explicit budget; a present value
/// must be a positive integer, matching the engine's `max_selected_bytes`
/// rule instead of introducing a second contract-side policy.
fn resource_selected_byte_limit(
    name: &str,
    resource: &ResourceDef,
) -> Result<Option<usize>, ContractError> {
    let Some(value) = resource.params.get("max_selected_bytes") else {
        return Ok(None);
    };
    let limit = value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ContractError::Invalid(format!(
                "resource `{name}` param `max_selected_bytes` must be a positive integer fitting usize"
            ))
        })?;
    Ok(Some(limit))
}

pub(crate) fn validate_resource_files(
    root: &Utf8Path,
    resources: &BTreeMap<String, ResourceDef>,
) -> Result<(), ContractError> {
    for (name, resource) in resources {
        let selected_byte_limit = resource_selected_byte_limit(name, resource)?;
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
            "skill" => validate_skill_resource(name, &resolved, selected_byte_limit)?,
            "skill_library" => validate_skill_library(name, &resolved, selected_byte_limit)?,
            _ => {}
        }
    }
    Ok(())
}

fn validate_skill_resource(
    name: &str,
    root: &Utf8Path,
    selected_byte_limit: Option<usize>,
) -> Result<(), ContractError> {
    let skill_path = root.join("SKILL.md");
    if !skill_path.is_file() {
        return Err(ContractError::Invalid(format!(
            "skill resource `{name}` must contain SKILL.md at `{skill_path}`"
        )));
    }
    read_and_validate_skill(name, &skill_path, root.file_name(), selected_byte_limit)
}

/// Counts every directory and file below `root`, matching the engine's
/// bounded skill-library walk so validation and execution apply the same
/// ceiling to the same tree.
fn count_library_entries(name: &str, root: &Utf8Path, ceiling: usize) -> Result<(), ContractError> {
    let mut pending = vec![root.to_owned()];
    let mut inspected = 0_usize;
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir).map_err(|error| {
            ContractError::Invalid(format!(
                "skill_library resource `{name}` cannot read `{dir}`: {error}"
            ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                ContractError::Invalid(format!(
                    "skill_library resource `{name}` cannot read an entry: {error}"
                ))
            })?;
            inspected = inspected.saturating_add(1);
            if inspected > ceiling {
                return Err(ContractError::Invalid(format!(
                    "skill_library resource `{name}` exceeds {ceiling} entries"
                )));
            }
            let file_type = entry.file_type().map_err(|error| {
                ContractError::Invalid(format!(
                    "skill_library resource `{name}` cannot inspect an entry: {error}"
                ))
            })?;
            if file_type.is_symlink() {
                return Err(ContractError::Invalid(format!(
                    "skill_library resource `{name}` contains a symbolic link"
                )));
            }
            if !file_type.is_dir() && !file_type.is_file() {
                return Err(ContractError::Invalid(format!(
                    "skill_library resource `{name}` contains an unsupported entry"
                )));
            }
            if file_type.is_dir() {
                pending.push(
                    camino::Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                        ContractError::Invalid(format!(
                            "skill_library resource `{name}` entry is not valid UTF-8: {}",
                            path.display()
                        ))
                    })?,
                );
            }
        }
    }
    Ok(())
}

fn validate_skill_library(
    name: &str,
    root: &Utf8Path,
    selected_byte_limit: Option<usize>,
) -> Result<(), ContractError> {
    // One deployment-owned ceiling, shared with the engine's bounded skill
    // library walk. A local CLI contract load and a served admission use the
    // same resolved value; a registry override applies to both.
    let ceiling = policy::operational_limits().max_skill_library_entries;
    count_library_entries(name, root, ceiling)?;
    let entries = std::fs::read_dir(root).map_err(|error| {
        ContractError::Invalid(format!(
            "skill_library resource `{name}` cannot read `{root}`: {error}"
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            ContractError::Invalid(format!(
                "skill_library resource `{name}` cannot read an entry: {error}"
            ))
        })?;
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
        // Admission enforces only that SKILL.md is readable UTF-8 within the
        // resource's selected-bytes budget (both hard errors). Content
        // classification has one owner: engine catalog construction isolates
        // malformed members with diagnostics and keeps them unselectable, so
        // parsing the content here would discard it.
        read_skill_source(name, &skill_path, selected_byte_limit)?;
    }
    Ok(())
}

/// Reads SKILL.md without accepting a substitute: every read/encoding fault
/// stays a hard contract error for both `skill` and `skill_library`. With a
/// configured selected-bytes budget, at most one byte past the budget is read
/// so an oversized file is refused before it is fully allocated. The size check
/// runs before decoding, so a boundary that splits a multi-byte character is
/// reported as oversize rather than as a broken encoding in a valid file.
fn read_skill_source(
    resource: &str,
    skill_path: &Utf8Path,
    selected_byte_limit: Option<usize>,
) -> Result<String, ContractError> {
    let read_error = |error: std::io::Error| {
        ContractError::Invalid(format!(
            "skill resource `{resource}` cannot read `{skill_path}`: {error}"
        ))
    };
    let Some(limit) = selected_byte_limit else {
        return std::fs::read_to_string(skill_path).map_err(read_error);
    };
    let file = std::fs::File::open(skill_path).map_err(read_error)?;
    let read_limit = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(read_error)?;
    if bytes.len() > limit {
        return Err(ContractError::Invalid(format!(
            "skill resource `{resource}` SKILL.md `{skill_path}` exceeds max_selected_bytes {limit}"
        )));
    }
    String::from_utf8(bytes)
        .map_err(|error| read_error(std::io::Error::new(std::io::ErrorKind::InvalidData, error)))
}

/// Content-only check, kept separate from the read so a library can skip one
/// malformed SKILL.md without hiding a filesystem failure. The explicit
/// `skill` resource maps the same errors to a hard contract error.
fn validate_skill_content(
    resource: &str,
    skill_path: &Utf8Path,
    expected_name: Option<&str>,
    source: &str,
) -> Result<(), String> {
    let doc = parse_skill_doc(source).map_err(|error| {
        format!("skill resource `{resource}` has invalid frontmatter in `{skill_path}`: {error}")
    })?;
    validate_skill_doc(&doc, expected_name).map_err(|error| {
        format!("skill resource `{resource}` is invalid in `{skill_path}`: {error}")
    })?;
    Ok(())
}

fn read_and_validate_skill(
    resource: &str,
    skill_path: &Utf8Path,
    expected_name: Option<&str>,
    selected_byte_limit: Option<usize>,
) -> Result<(), ContractError> {
    let source = read_skill_source(resource, skill_path, selected_byte_limit)?;
    validate_skill_content(resource, skill_path, expected_name, &source)
        .map_err(ContractError::Invalid)
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
            if !is_safe_path_component(name) {
                return Err(ContractError::Invalid(format!(
                    "resource name `{name}` must be a safe path component"
                )));
            }
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

    fn parse_verified(text: &str) -> Result<BTreeMap<String, Value>, ContractError> {
        let raw: BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_str(text).expect("test JSON must parse as raw map");
        verified_inputs_from_raw_map(raw)
    }

    #[derive(Debug, serde::Deserialize)]
    struct VerifiedHolder {
        #[serde(deserialize_with = "super::deserialize_verified_inputs")]
        inputs: BTreeMap<String, Value>,
    }

    #[test]
    fn raw_inputs_accept_integer_bounds_and_finite_floats() {
        let values = parse_verified(
            r#"{"payload": {"numbers": [-9223372036854775808, 18446744073709551615,
                18446744073709551616.0, -9223372036854775809.0, 1e100, -1E100, 1.25]}}"#,
        )
        .expect("exact bounds and finite floats must pass raw validation");
        let numbers = values["payload"]["numbers"]
            .as_array()
            .expect("numbers must be an array");
        assert_eq!(numbers[0].as_i64(), Some(i64::MIN));
        assert_eq!(numbers[1].as_u64(), Some(u64::MAX));
        assert!(
            numbers[2..].iter().all(|value| value.is_f64()),
            "decimals and exponents stay floats: {numbers:?}"
        );
        // The serde entry point preserves the same boundary.
        let holder: VerifiedHolder = serde_json::from_str(
            r#"{"inputs": {"payload": {"numbers": [-9223372036854775808, 1.25]}}}"#,
        )
        .expect("deserialize_with must accept in-range raw inputs");
        assert_eq!(
            holder.inputs["payload"]["numbers"][0].as_i64(),
            Some(i64::MIN)
        );
    }

    #[test]
    fn raw_inputs_reject_nested_out_of_range_integers() {
        for token in [
            "-9223372036854775809",
            "18446744073709551616",
            "18446744073709551617",
        ] {
            let error = parse_verified(&format!(
                r#"{{"payload": {{"nested": [{{"number": {token}}}]}}}}"#
            ))
            .expect_err("nested out-of-range integer must be rejected");
            assert!(
                error
                    .to_string()
                    .contains("input `payload` numbers must be integers"),
                "unexpected message for {token}: {error}"
            );
        }
    }

    #[test]
    fn raw_inputs_preserve_numeric_strings_and_keys() {
        let values = parse_verified(
            r#"{"18446744073709551616": "18446744073709551616",
                "payload": {"text": "1e9999", "escaped": "quoted: \"18446744073709551616\" remains text",
                    "nested": ["18446744073709551616"]}}"#,
        )
        .expect("numeric-looking strings and keys must not be rejected");
        assert_eq!(values["payload"]["text"], Value::String("1e9999".into()));
        assert_eq!(
            values["payload"]["escaped"],
            Value::String("quoted: \"18446744073709551616\" remains text".into())
        );
    }

    #[test]
    fn raw_inputs_reject_nonfinite_floats() {
        for token in ["1e9999", "-1e9999", "1E9999"] {
            let error = parse_verified(&format!(
                r#"{{"payload": {{"nested": [{{"number": {token}}}]}}}}"#
            ))
            .expect_err("non-finite float must be rejected");
            assert!(
                error.to_string().contains("numbers must be integers"),
                "unexpected message for {token}: {error}"
            );
        }
        // The serde entry point rejects the same token.
        let error = serde_json::from_str::<VerifiedHolder>(
            r#"{"inputs": {"payload": {"nested": [{"number": 1e9999}]}}}"#,
        )
        .expect_err("deserialize_with must reject non-finite floats");
        assert!(
            error.to_string().contains("numbers must be integers"),
            "unexpected serde message: {error}"
        );
    }

    #[test]
    fn scalar_token_validator_covers_cli_numbers() {
        validate_input_scalar_token("42").expect("in-range integer must pass");
        validate_input_scalar_token("1.25").expect("finite float must pass");
        validate_input_scalar_token("1e100").expect("finite exponent must pass");
        validate_input_scalar_token(r#""18446744073709551616""#).expect("numeric string must pass");
        for token in ["18446744073709551616", "-9223372036854775809", "1e9999"] {
            validate_input_scalar_token(token)
                .expect_err("out-of-range scalar token must be rejected");
        }
    }

    #[test]
    fn skill_library_validation_uses_the_deployment_ceiling() {
        let ceiling = policy::operational_limits().max_skill_library_entries;
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-skill-library-ceiling-{}", std::process::id())),
        )
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("library root should be created");
        for index in 0..ceiling {
            std::fs::create_dir(root.join(format!("e{index}")))
                .expect("library entry should be created");
        }
        validate_skill_library("lib", &root, None)
            .expect("exactly the ceiling must still validate");
        std::fs::create_dir(root.join("over")).expect("extra entry should be created");
        let error = validate_skill_library("lib", &root, None)
            .expect_err("one entry over the deployment ceiling must be refused");
        assert!(
            error.to_string().contains(&ceiling.to_string()),
            "the error must name the configured ceiling: {error}"
        );
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[test]
    fn skill_library_ceiling_counts_nested_entries_like_execution() {
        let ceiling = policy::operational_limits().max_skill_library_entries;
        let skill_count = ceiling / 2;
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-skill-library-nested-ceiling-{}",
            std::process::id()
        )))
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("library root should be created");
        for index in 0..skill_count {
            let child = root.join(format!("skill-{index}"));
            std::fs::create_dir(&child).expect("skill directory should be created");
            std::fs::write(
                child.join("SKILL.md"),
                format!("---\nname: skill-{index}\ndescription: Demo skill\n---\n\nDo this.\n"),
            )
            .expect("SKILL.md should be written");
        }
        validate_skill_library("lib", &root, None)
            .expect("skill dirs and their files exactly at the ceiling must validate");
        std::fs::write(root.join("extra.txt"), "extra").expect("extra entry should be created");
        validate_skill_library("lib", &root, None)
            .expect_err("one recursive entry over the ceiling must be refused");
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn skill_library_validation_refuses_unsupported_entries_like_execution() {
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-skill-library-fifo-{}", std::process::id())),
        )
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("library root should be created");
        let fifo = root.join("pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo should run");
        assert!(status.success(), "mkfifo failed: {status}");
        let error = validate_skill_library("lib", &root, None)
            .expect_err("an unsupported entry must be refused before execution sees it");
        assert!(
            error.to_string().contains("unsupported"),
            "unexpected refusal: {error}"
        );
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn skill_library_validation_refuses_symlinks_like_execution() {
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-skill-library-symlink-{}", std::process::id())),
        )
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("library root should be created");
        std::os::unix::fs::symlink("nowhere", root.join("link"))
            .expect("symlink should be created");
        let error = validate_skill_library("lib", &root, None)
            .expect_err("a symlink must be refused before execution sees it");
        assert!(
            error.to_string().contains("symbolic link"),
            "unexpected refusal: {error}"
        );
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[test]
    fn skill_library_validation_isolates_malformed_members_but_explicit_skill_fails() {
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-skill-library-malformed-{}",
            std::process::id()
        )))
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("library root should be created");
        let broken = root.join("broken");
        std::fs::create_dir_all(&broken).expect("malformed skill directory should be created");
        std::fs::write(broken.join("SKILL.md"), "plain markdown, no frontmatter\n")
            .expect("malformed SKILL.md should be written");
        let good = root.join("good");
        std::fs::create_dir_all(&good).expect("valid skill directory should be created");
        std::fs::write(
            good.join("SKILL.md"),
            "---\nname: good\ndescription: Good.\n---\n\nDo this.\n",
        )
        .expect("valid SKILL.md should be written");

        validate_skill_library("lib", &root, None)
            .expect("one malformed member must not invalidate the rest of the library");
        let error = validate_skill_resource("strict", &broken, None)
            .expect_err("an explicit skill must reject the same malformed content");
        assert!(
            error.to_string().contains("frontmatter"),
            "explicit skill should report the parse fault: {error}"
        );

        // A different content-format fault is isolated the same way.
        std::fs::write(broken.join("SKILL.md"), "---\nname: broken\n---\n")
            .expect("missing-description SKILL.md should be written");
        validate_skill_library("lib", &root, None)
            .expect("a missing description must also be isolated per member");
        let error = validate_skill_resource("strict", &broken, None)
            .expect_err("an explicit skill must reject a missing description");
        assert!(
            error.to_string().contains("description"),
            "explicit skill should report the missing description: {error}"
        );
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[test]
    fn skill_library_validation_keeps_non_utf8_skill_content_hard() {
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-skill-library-non-utf8-{}", std::process::id())),
        )
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("library root should be created");
        let broken = root.join("broken");
        std::fs::create_dir_all(&broken).expect("malformed skill directory should be created");
        std::fs::write(broken.join("SKILL.md"), [0xff, 0xfe])
            .expect("non-UTF-8 SKILL.md should be written");
        let error = validate_skill_library("lib", &root, None)
            .expect_err("a non-UTF-8 SKILL.md is an I/O fault, not a content fault");
        assert!(
            error.to_string().contains("cannot read"),
            "unexpected refusal: {error}"
        );
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    fn skill_resource_def(
        kind: &str,
        path: &str,
        max_selected_bytes: Option<usize>,
    ) -> ResourceDef {
        let params = match max_selected_bytes {
            Some(limit) => serde_json::json!({ "max_selected_bytes": limit }),
            None => serde_json::json!({}),
        };
        serde_json::from_value(serde_json::json!({
            "type": kind,
            "path": path,
            "params": params,
        }))
        .expect("test resource must deserialize")
    }

    fn admission_temp_root(label: &str) -> camino::Utf8PathBuf {
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-admission-{label}-{}", std::process::id())),
        )
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary root should be created");
        root
    }

    fn budgeted_resources(
        name: &str,
        kind: &str,
        path: &str,
        limit: usize,
    ) -> BTreeMap<String, ResourceDef> {
        BTreeMap::from([(
            name.to_string(),
            skill_resource_def(kind, path, Some(limit)),
        )])
    }

    #[test]
    fn resource_selected_byte_limit_requires_a_positive_usize() {
        let mut resource = skill_resource_def("skill", "skill", None);
        assert_eq!(
            resource_selected_byte_limit("demo", &resource).expect("absent param is unbounded"),
            None
        );
        for invalid in [
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("128"),
        ] {
            resource
                .params
                .insert("max_selected_bytes".into(), invalid.clone());
            let error = resource_selected_byte_limit("demo", &resource)
                .expect_err("an invalid budget must be a hard contract error");
            assert!(
                error.to_string().contains("max_selected_bytes"),
                "error must name the param: {error}"
            );
        }
        resource
            .params
            .insert("max_selected_bytes".into(), serde_json::json!(128));
        assert_eq!(
            resource_selected_byte_limit("demo", &resource).expect("positive integer must pass"),
            Some(128)
        );
    }

    #[test]
    fn admission_applies_selected_bytes_to_standalone_skill() {
        let root = admission_temp_root("standalone-bytes");
        let skill = root.join("skill");
        std::fs::create_dir(&skill).expect("skill directory should be created");
        let source = "---\nname: demo\ndescription: Demo.\n---\nBody é.\n";
        let skill_file = skill.join("SKILL.md");
        std::fs::write(&skill_file, source).expect("SKILL.md should be written");
        let len = source.len();

        let exact = budgeted_resources("demo", "skill", "skill", len);
        validate_resource_files(&root, &exact).expect("the exact byte count must validate");
        let tight = budgeted_resources("demo", "skill", "skill", len - 1);
        let error = validate_resource_files(&root, &tight)
            .expect_err("one byte under the source length must be refused");
        assert!(
            error.to_string().contains("max_selected_bytes"),
            "the error must name the configured budget: {error}"
        );

        // A budget whose one-byte over-read ends inside a multi-byte character
        // must report the size fault, not an encoding fault on a valid file.
        let split = source.find('é').expect("source has a multi-byte character");
        let split_budget = budgeted_resources("demo", "skill", "skill", split);
        let split_error = validate_resource_files(&root, &split_budget)
            .expect_err("a budget that splits a character must still be refused");
        assert!(
            split_error.to_string().contains("max_selected_bytes"),
            "oversize must be reported before decoding: {split_error}"
        );
        assert!(
            !split_error.to_string().contains("UTF-8"),
            "a valid file must not be reported as an encoding fault: {split_error}"
        );
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[test]
    fn admission_applies_selected_bytes_to_library_members() {
        let root = admission_temp_root("library-bytes");
        let library = root.join("lib");
        let member = library.join("alpha");
        std::fs::create_dir_all(&member).expect("library member should be created");
        let source = "---\nname: alpha\ndescription: Alpha.\n---\nBody.\n";
        std::fs::write(member.join("SKILL.md"), source).expect("SKILL.md should be written");
        let len = source.len();

        let tight = budgeted_resources("lib", "skill_library", "lib", len - 1);
        let error = validate_resource_files(&root, &tight)
            .expect_err("an oversized library member must be refused");
        assert!(
            error.to_string().contains("max_selected_bytes"),
            "the error must name the configured budget: {error}"
        );
        let exact = budgeted_resources("lib", "skill_library", "lib", len);
        validate_resource_files(&root, &exact)
            .expect("an exact-bound library member must validate");
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[test]
    fn admission_keeps_content_classification_out_of_the_bounded_reader() {
        let root = admission_temp_root("content-faults");
        let library = root.join("lib");
        let member = library.join("alpha");
        std::fs::create_dir_all(&member).expect("library member should be created");
        let skill_file = member.join("SKILL.md");
        let resources = budgeted_resources("lib", "skill_library", "lib", 1024);

        std::fs::write(&skill_file, "plain markdown, no frontmatter\n")
            .expect("readable malformed SKILL.md should be written");
        validate_resource_files(&root, &resources)
            .expect("content parsing stays with the engine, not admission");

        std::fs::write(&skill_file, [0xff, 0xfe]).expect("non-UTF-8 SKILL.md should be written");
        let error = validate_resource_files(&root, &resources)
            .expect_err("non-UTF-8 SKILL.md remains a hard error");
        assert!(
            error.to_string().contains("cannot read"),
            "unexpected refusal: {error}"
        );
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }
}
