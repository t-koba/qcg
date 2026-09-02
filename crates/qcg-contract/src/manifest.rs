use crate::graph::Graph;
use camino::{Utf8Path, Utf8PathBuf};
pub use qcg_types::{
    AgentFailureAction, AgentFailureCode, ArtifactPreview, AssetSpec, Expr, FieldType, FileValue,
    FileValueError, GeneratorMeta, InputField, InputSpec, InputStage, RecoverableAgentFailureCode,
    ResponseVerbosity, StructuredOutputMode, ToolChoice, is_safe_relative_path,
};
use schemars::JsonSchema;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned, de::Error as DeError,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read as _;

pub const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_JSON_SCHEMA_BYTES: usize = 256 * 1024;
pub const MAX_JSON_SCHEMA_DEPTH: usize = 64;
pub const MAX_JSON_SCHEMA_NODES: usize = 8_192;
pub const MAX_JSON_SCHEMA_OBJECT_MEMBERS: usize = 1_024;
pub const MAX_JSON_SCHEMA_STRING_BYTES: usize = 16 * 1024;
pub const MAX_RUNTIME_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;
pub const MAX_RUNTIME_LIMIT_BYTES: usize = 1024 * 1024 * 1024;
pub const MAX_RUNTIME_COUNT_LIMIT: usize = 1_000_000;
pub const MAX_RUNTIME_HTTP_REDIRECTS: usize = 32;
pub const MAX_RUNTIME_TEMPLATE_FUEL: u64 = 100_000_000;
pub const MAX_BUDGET_STEPS: usize = 1_000_000;
pub const MAX_BUDGET_TOKENS: u64 = 10_000_000_000;
pub const MAX_BUDGET_ELAPSED_SECONDS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("failed to read manifest `{path}`: {source}")]
    Read {
        path: Utf8PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse manifest `{path}`: {source}")]
    Parse {
        path: Utf8PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid manifest: {0}")]
    Invalid(String),
    #[error("input `{field}` is too large: {actual_bytes} bytes exceeds {limit_bytes} bytes")]
    PayloadTooLarge {
        field: String,
        actual_bytes: usize,
        limit_bytes: usize,
    },
    #[error("invalid graph: {0}")]
    Graph(String),
}

#[derive(Debug, Clone)]
pub struct Contract {
    pub root: Utf8PathBuf,
    pub manifest: Manifest,
    pub graph: Graph,
    pub sha256: String,
}

impl Contract {
    pub fn load(root: impl AsRef<Utf8Path>) -> Result<Self, ContractError> {
        let root = root.as_ref().to_path_buf();
        let manifest_path = root.join("qcg.toml");
        let source = read_manifest_bounded(&manifest_path)?;
        let manifest: Manifest =
            toml::from_str(&source).map_err(|source| ContractError::Parse {
                path: manifest_path.clone(),
                source,
            })?;
        let graph = Graph::build(&manifest)
            .map_err(|error| ContractError::Graph(with_line_hint(&source, &error)))?;
        manifest
            .validate()
            .map_err(|error| error.with_line_hint(&source))?;
        validate_qcg_version(&manifest.generator.qcg_version, &manifest.generator.id)?;
        validate_asset_files(&root, &manifest.assets)?;
        validate_resource_files(&root, &manifest.resources)?;
        let sha256 = hex::encode(Sha256::digest(source.as_bytes()));
        Ok(Self {
            root,
            manifest,
            graph,
            sha256,
        })
    }

    pub fn line_hint(&self, message: &str) -> String {
        let source = read_manifest_bounded(&self.root.join("qcg.toml")).unwrap_or_default();
        with_line_hint(&source, message)
    }

    /// Resolve an existing path declared by this generator package.
    pub fn resolve_package_path(
        &self,
        relative: &str,
    ) -> Result<Utf8PathBuf, crate::PackagePathError> {
        crate::resolve_package_path(&self.root, relative)
    }
}

fn read_manifest_bounded(path: &Utf8Path) -> Result<String, ContractError> {
    read_manifest_with_limit(path, MAX_MANIFEST_BYTES)
}

fn read_manifest_with_limit(path: &Utf8Path, max_bytes: usize) -> Result<String, ContractError> {
    let file = fs::File::open(path).map_err(|source| ContractError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let limit = u64::try_from(max_bytes)
        .expect("manifest byte limit must fit in u64")
        .saturating_add(1);
    let mut source = String::new();
    file.take(limit)
        .read_to_string(&mut source)
        .map_err(|source| ContractError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    if source.len() > max_bytes {
        return Err(ContractError::Invalid(format!(
            "manifest `{path}` exceeds {max_bytes} bytes"
        )));
    }
    Ok(source)
}

fn validate_qcg_version(requirement: &str, generator_id: &str) -> Result<(), ContractError> {
    let requirement = requirement.trim();
    if requirement.is_empty() {
        return Err(ContractError::Invalid(format!(
            "generator `{generator_id}` must declare generator.qcg_version"
        )));
    }
    let requirement = semver::VersionReq::parse(requirement).map_err(|error| {
        ContractError::Invalid(format!(
            "generator.qcg_version `{requirement}` is invalid: {error}"
        ))
    })?;
    let current = semver::Version::parse(env!("CARGO_PKG_VERSION")).map_err(|error| {
        ContractError::Invalid(format!("qcg runtime version is invalid: {error}"))
    })?;
    if requirement.matches(&current) {
        Ok(())
    } else {
        Err(ContractError::Invalid(format!(
            "generator requires qcg_version `{requirement}`, runtime is `{}`",
            env!("CARGO_PKG_VERSION")
        )))
    }
}

impl ContractError {
    fn with_line_hint(self, source: &str) -> Self {
        match self {
            ContractError::Invalid(message) => {
                ContractError::Invalid(with_line_hint(source, &message))
            }
            ContractError::Graph(message) => ContractError::Graph(with_line_hint(source, &message)),
            other => other,
        }
    }
}

fn with_line_hint(source: &str, message: &str) -> String {
    let unknown_field = message
        .split_once("unknown field `")
        .and_then(|(_, rest)| rest.split_once('`'))
        .map(|(field, _)| field);
    let needle = unknown_field.unwrap_or_else(|| {
        message
            .split('`')
            .enumerate()
            .filter(|(index, token)| index % 2 == 1 && !token.is_empty())
            .map(|(_, token)| token)
            .filter(|token| source.contains(token))
            .last()
            .unwrap_or_default()
    });
    if !needle.is_empty()
        && let Some(offset) = source.find(needle)
    {
        let line = source[..offset]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        return format!("line {line}: {message}");
    }
    format!("line 1: {message}")
}

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
        GeneratorMetadataRule.validate(self)?;
        InputRule.validate(self)?;
        ResourceRule.validate(self)?;
        FlowNodeRule.validate(self)?;
        CommandPermissionRule.validate(self)?;
        ToolRule.validate(self)?;
        OutputArtifactRule.validate(self)?;
        AssetRule.validate(self)
    }
}

fn validate_asset_files(root: &Utf8Path, assets: &AssetSpec) -> Result<(), ContractError> {
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

fn validate_resource_files(
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
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
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
    let command_limit = u64::try_from(runtime.command_output_limit_bytes).map_err(|_| {
        ContractError::Invalid("runtime.command_output_limit_bytes is too large".into())
    })?;
    if let Some(max_bytes) = resource.params.get("max_bytes").and_then(Value::as_u64)
        && max_bytes > command_limit
    {
        return Err(ContractError::Invalid(format!(
            "resource `{name}` type `exec` max_bytes ({max_bytes}) exceeds runtime.command_output_limit_bytes ({command_limit})"
        )));
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
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
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

pub fn validate_bounded_json_schema(schema: &Value) -> Result<(), String> {
    let encoded = serde_json::to_vec(schema)
        .map_err(|error| format!("failed to encode JSON Schema: {error}"))?;
    if encoded.len() > MAX_JSON_SCHEMA_BYTES {
        return Err(format!("schema exceeds {MAX_JSON_SCHEMA_BYTES} bytes"));
    }

    let mut stack = vec![(schema, 0_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_JSON_SCHEMA_NODES {
            return Err(format!(
                "schema exceeds {MAX_JSON_SCHEMA_NODES} JSON values"
            ));
        }
        if depth > MAX_JSON_SCHEMA_DEPTH {
            return Err(format!(
                "schema nesting exceeds {MAX_JSON_SCHEMA_DEPTH} levels"
            ));
        }
        match value {
            Value::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth.saturating_add(1))));
            }
            Value::Object(values) => {
                if values.len() > MAX_JSON_SCHEMA_OBJECT_MEMBERS {
                    return Err(format!(
                        "schema object exceeds {MAX_JSON_SCHEMA_OBJECT_MEMBERS} members"
                    ));
                }
                for (name, value) in values {
                    if name.len() > MAX_JSON_SCHEMA_STRING_BYTES {
                        return Err(format!(
                            "schema property name exceeds {MAX_JSON_SCHEMA_STRING_BYTES} bytes"
                        ));
                    }
                    if matches!(name.as_str(), "$ref" | "$dynamicRef" | "$recursiveRef")
                        && value
                            .as_str()
                            .is_some_and(|reference| !reference.starts_with('#'))
                    {
                        return Err("schema contains an external reference".into());
                    }
                    stack.push((value, depth.saturating_add(1)));
                }
            }
            Value::String(value) if value.len() > MAX_JSON_SCHEMA_STRING_BYTES => {
                return Err(format!(
                    "schema string exceeds {MAX_JSON_SCHEMA_STRING_BYTES} bytes"
                ));
            }
            _ => {}
        }
    }

    jsonschema::validator_for(schema)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

struct GeneratorMetadataRule;

impl GeneratorMetadataRule {
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        if manifest.generator.id.trim().is_empty() {
            return Err(ContractError::Invalid("generator.id is required".into()));
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
        if manifest.budget.max_steps > MAX_BUDGET_STEPS {
            return Err(ContractError::Invalid(format!(
                "budget.max_steps must not exceed {MAX_BUDGET_STEPS}"
            )));
        }
        if manifest.budget.max_tokens == Some(0) {
            return Err(ContractError::Invalid(
                "budget.max_tokens must be greater than zero".into(),
            ));
        }
        if manifest
            .budget
            .max_tokens
            .is_some_and(|value| value > MAX_BUDGET_TOKENS)
        {
            return Err(ContractError::Invalid(format!(
                "budget.max_tokens must not exceed {MAX_BUDGET_TOKENS}"
            )));
        }
        if manifest.budget.max_elapsed_seconds == Some(0) {
            return Err(ContractError::Invalid(
                "budget.max_elapsed_seconds must be greater than zero".into(),
            ));
        }
        if manifest
            .budget
            .max_elapsed_seconds
            .is_some_and(|value| value > MAX_BUDGET_ELAPSED_SECONDS)
        {
            return Err(ContractError::Invalid(format!(
                "budget.max_elapsed_seconds must not exceed {MAX_BUDGET_ELAPSED_SECONDS}"
            )));
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
            if value > MAX_RUNTIME_TIMEOUT_SECONDS {
                return Err(ContractError::Invalid(format!(
                    "{name} must not exceed {MAX_RUNTIME_TIMEOUT_SECONDS}"
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
            if value == 0 {
                return Err(ContractError::Invalid(format!(
                    "{name} must be greater than zero"
                )));
            }
            if value > MAX_RUNTIME_LIMIT_BYTES {
                return Err(ContractError::Invalid(format!(
                    "{name} must not exceed {MAX_RUNTIME_LIMIT_BYTES}"
                )));
            }
        }
        for (name, value) in [
            (
                "runtime.file_count_limit",
                manifest.runtime.file_count_limit,
            ),
            (
                "runtime.output_artifact_limit",
                manifest.runtime.output_artifact_limit,
            ),
            (
                "runtime.journal_event_count_limit",
                manifest.runtime.journal_event_count_limit,
            ),
        ] {
            if value > MAX_RUNTIME_COUNT_LIMIT {
                return Err(ContractError::Invalid(format!(
                    "{name} must not exceed {MAX_RUNTIME_COUNT_LIMIT}"
                )));
            }
        }
        if manifest.runtime.http_redirect_limit > MAX_RUNTIME_HTTP_REDIRECTS {
            return Err(ContractError::Invalid(format!(
                "runtime.http_redirect_limit must not exceed {MAX_RUNTIME_HTTP_REDIRECTS}"
            )));
        }
        if manifest.runtime.template_fuel == 0 {
            return Err(ContractError::Invalid(
                "runtime.template_fuel must be greater than zero".into(),
            ));
        }
        if manifest.runtime.template_fuel > MAX_RUNTIME_TEMPLATE_FUEL {
            return Err(ContractError::Invalid(format!(
                "runtime.template_fuel must not exceed {MAX_RUNTIME_TEMPLATE_FUEL}"
            )));
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
            if llm
                .max_context_bytes
                .is_some_and(|value| value > MAX_RUNTIME_LIMIT_BYTES)
            {
                return Err(ContractError::Invalid(format!(
                    "[llm].max_context_bytes must not exceed {MAX_RUNTIME_LIMIT_BYTES}"
                )));
            }
            if llm.max_context_tokens == Some(0) {
                return Err(ContractError::Invalid(
                    "[llm].max_context_tokens must be greater than zero".into(),
                ));
            }
            if llm
                .max_context_tokens
                .is_some_and(|value| value > MAX_RUNTIME_LIMIT_BYTES / 4)
            {
                return Err(ContractError::Invalid(format!(
                    "[llm].max_context_tokens must not exceed {}",
                    MAX_RUNTIME_LIMIT_BYTES / 4
                )));
            }
            if llm.max_media_bytes == Some(0) {
                return Err(ContractError::Invalid(
                    "[llm].max_media_bytes must be greater than zero".into(),
                ));
            }
            if llm
                .max_media_bytes
                .is_some_and(|value| value > MAX_RUNTIME_LIMIT_BYTES)
            {
                return Err(ContractError::Invalid(format!(
                    "[llm].max_media_bytes must not exceed {MAX_RUNTIME_LIMIT_BYTES}"
                )));
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

struct FlowNodeRule;

struct CommandPermissionRule;

impl CommandPermissionRule {
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
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
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        let mut ids = BTreeSet::new();
        for node in &manifest.flow {
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
                let params: CheckToolValidationParams =
                    node.deserialize_params().map_err(|error| {
                        ContractError::Invalid(format!(
                            "check.tool node `{}` has invalid params: {error}",
                            node.id
                        ))
                    })?;
                let tool_name = params.tool.as_deref().ok_or_else(|| {
                    ContractError::Invalid(format!(
                        "check.tool node `{}` must declare tool",
                        node.id
                    ))
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

struct ToolRule;

impl ToolRule {
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for (name, tool) in &manifest.tools {
            validate_tool(name, tool, &manifest.permissions)?;
        }
        Ok(())
    }
}

struct OutputArtifactRule;

impl OutputArtifactRule {
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
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

struct AssetRule;

impl AssetRule {
    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
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

fn validate_artifact_pattern(pattern: &str, kind: &str) -> Result<(), ContractError> {
    if !is_safe_relative_path(pattern) {
        return Err(ContractError::Invalid(format!(
            "{kind} `{pattern}` is not a safe relative path"
        )));
    }
    Ok(())
}

fn validate_field_value(
    field: &InputField,
    value: &Value,
    runtime: &RuntimeLimits,
) -> Result<(), ContractError> {
    match field.kind {
        FieldType::Json | FieldType::Custom(_) => {}
        FieldType::File => {
            let file = FileValue::from_value_with_limit(value, runtime.file_input_limit_bytes)
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

fn validate_input_sizes<'a>(
    values: impl IntoIterator<Item = (&'a String, &'a Value)>,
    runtime: &RuntimeLimits,
) -> Result<(), ContractError> {
    let mut total = 0_usize;
    for (field, value) in values {
        let bytes =
            serialized_value_size(value, runtime.file_input_limit_bytes).map_err(|error| {
                ContractError::PayloadTooLarge {
                    field: field.clone(),
                    actual_bytes: error,
                    limit_bytes: runtime.file_input_limit_bytes,
                }
            })?;
        total = total
            .checked_add(bytes)
            .ok_or_else(|| ContractError::PayloadTooLarge {
                field: "inputs".into(),
                actual_bytes: usize::MAX,
                limit_bytes: runtime.input_total_limit_bytes,
            })?;
        if total > runtime.input_total_limit_bytes {
            return Err(ContractError::PayloadTooLarge {
                field: "inputs".into(),
                actual_bytes: total,
                limit_bytes: runtime.input_total_limit_bytes,
            });
        }
    }
    Ok(())
}

fn serialized_value_size(value: &Value, limit: usize) -> Result<usize, usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }

    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = self.bytes.saturating_add(bytes.len());
            self.bytes = next;
            if next > self.limit {
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
        Err(_error) if counter.bytes > limit => Err(counter.bytes),
        Err(_) => Err(counter.bytes),
    }
}

fn validate_tool(
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmConfig {
    #[serde(default)]
    pub model: Option<ModelRef>,
    #[serde(default)]
    pub models: Vec<ModelRef>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stop_sequences: Vec<String>,
    #[serde(default)]
    pub max_context_bytes: Option<usize>,
    #[serde(default)]
    pub max_context_tokens: Option<usize>,
    #[serde(default)]
    pub max_media_bytes: Option<usize>,
    #[serde(default)]
    pub context_overflow: ContextOverflowPolicy,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub retry_prompt: Option<String>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub reasoning_effort: Option<qcg_types::ReasoningEffort>,
    #[serde(default)]
    pub structured_output: StructuredOutputMode,
    #[serde(default)]
    pub tool_choice: Option<ToolChoice>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub verbosity: Option<ResponseVerbosity>,
}

/// Per-invocation LLM policy layered over the generator-wide `[llm]` defaults.
///
/// The provider registry advertises transport capabilities. This value only
/// selects behavior for one node or specialist invocation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmRequestPolicy {
    #[serde(default)]
    pub clear: Vec<LlmRequestControl>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub reasoning_effort: Option<qcg_types::ReasoningEffort>,
    #[serde(default)]
    pub structured_output: Option<StructuredOutputMode>,
    #[serde(default)]
    pub tool_choice: Option<ToolChoice>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub verbosity: Option<ResponseVerbosity>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub max_context_bytes: Option<usize>,
    #[serde(default)]
    pub max_context_tokens: Option<usize>,
    #[serde(default)]
    pub max_media_bytes: Option<usize>,
    #[serde(default)]
    pub context_overflow: Option<ContextOverflowPolicy>,
    #[serde(default)]
    pub retry_prompt: Option<String>,
}

/// Optional inherited request controls that an inner policy layer may omit.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum LlmRequestControl {
    Temperature,
    TopP,
    StopSequences,
    Seed,
    ReasoningEffort,
    ToolChoice,
    ParallelToolCalls,
    Verbosity,
}

impl LlmRequestControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Temperature => "temperature",
            Self::TopP => "top_p",
            Self::StopSequences => "stop_sequences",
            Self::Seed => "seed",
            Self::ReasoningEffort => "reasoning_effort",
            Self::ToolChoice => "tool_choice",
            Self::ParallelToolCalls => "parallel_tool_calls",
            Self::Verbosity => "verbosity",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContextOverflowPolicy {
    #[default]
    Error,
    TruncateHead,
    TruncateTail,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRef {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub input_cost_per_million_usd: Option<f64>,
    #[serde(default)]
    pub output_cost_per_million_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLimits {
    #[serde(default = "default_timeout_seconds")]
    pub command_timeout_seconds: u64,
    #[serde(default = "default_command_input_limit_bytes")]
    pub command_input_limit_bytes: usize,
    #[serde(default = "default_output_limit_bytes")]
    pub command_output_limit_bytes: usize,
    #[serde(default = "default_timeout_seconds")]
    pub http_timeout_seconds: u64,
    #[serde(default = "default_output_limit_bytes")]
    pub http_body_limit_bytes: usize,
    #[serde(default = "default_http_redirect_limit")]
    pub http_redirect_limit: usize,
    #[serde(default = "default_file_input_limit_bytes")]
    pub file_input_limit_bytes: usize,
    #[serde(default = "default_file_count_limit")]
    pub file_count_limit: usize,
    #[serde(default = "default_input_total_limit_bytes")]
    pub input_total_limit_bytes: usize,
    #[serde(default = "default_output_file_limit_bytes")]
    pub output_file_limit_bytes: usize,
    #[serde(default = "default_output_total_limit_bytes")]
    pub output_total_limit_bytes: usize,
    #[serde(default = "default_output_artifact_limit")]
    pub output_artifact_limit: usize,
    #[serde(default = "default_template_source_limit_bytes")]
    pub template_source_limit_bytes: usize,
    #[serde(default = "default_template_context_limit_bytes")]
    pub template_context_limit_bytes: usize,
    #[serde(default = "default_journal_event_limit_bytes")]
    pub journal_event_limit_bytes: usize,
    #[serde(default = "default_journal_total_limit_bytes")]
    pub journal_total_limit_bytes: usize,
    #[serde(default = "default_journal_event_count_limit")]
    pub journal_event_count_limit: usize,
    #[serde(default = "default_state_limit_bytes")]
    pub state_limit_bytes: usize,
    #[serde(default = "default_template_output_limit_bytes")]
    pub template_output_limit_bytes: usize,
    #[serde(default = "default_template_fuel")]
    pub template_fuel: u64,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            command_timeout_seconds: default_timeout_seconds(),
            command_input_limit_bytes: default_command_input_limit_bytes(),
            command_output_limit_bytes: default_output_limit_bytes(),
            http_timeout_seconds: default_timeout_seconds(),
            http_body_limit_bytes: default_output_limit_bytes(),
            http_redirect_limit: default_http_redirect_limit(),
            file_input_limit_bytes: default_file_input_limit_bytes(),
            file_count_limit: default_file_count_limit(),
            input_total_limit_bytes: default_input_total_limit_bytes(),
            output_file_limit_bytes: default_output_file_limit_bytes(),
            output_total_limit_bytes: default_output_total_limit_bytes(),
            output_artifact_limit: default_output_artifact_limit(),
            template_source_limit_bytes: default_template_source_limit_bytes(),
            template_context_limit_bytes: default_template_context_limit_bytes(),
            journal_event_limit_bytes: default_journal_event_limit_bytes(),
            journal_total_limit_bytes: default_journal_total_limit_bytes(),
            journal_event_count_limit: default_journal_event_count_limit(),
            state_limit_bytes: default_state_limit_bytes(),
            template_output_limit_bytes: default_template_output_limit_bytes(),
            template_fuel: default_template_fuel(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunBudget {
    #[serde(default = "default_max_total_steps")]
    pub max_steps: usize,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub max_cost_usd: Option<f64>,
    #[serde(default)]
    pub max_elapsed_seconds: Option<u64>,
}

impl Default for RunBudget {
    fn default() -> Self {
        Self {
            max_steps: default_max_total_steps(),
            max_tokens: None,
            max_cost_usd: None,
            max_elapsed_seconds: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceDef {
    #[serde(rename = "type")]
    pub kind: ResourceKind,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub pin_sha256: Option<String>,
    #[serde(default)]
    pub cache_ttl_seconds: Option<u64>,
    #[serde(default)]
    pub trust: Trust,
    #[serde(default)]
    pub llm_visible: bool,
    /// Type-specific bounded settings for built-in resource loaders.
    #[serde(default)]
    pub params: serde_json::Map<String, Value>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    File,
    Dir,
    Skill,
    Url,
    Openapi,
    Exec,
}

impl ResourceKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
            Self::Skill => "skill",
            Self::Url => "url",
            Self::Openapi => "openapi",
            Self::Exec => "exec",
        }
    }
}

impl std::fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Trust {
    Trusted,
    #[default]
    Untrusted,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Permissions {
    #[serde(default)]
    pub fs_read: Vec<String>,
    #[serde(default)]
    pub fs_write: Vec<String>,
    #[serde(default)]
    pub network: Vec<String>,
    #[serde(default)]
    pub commands: Vec<CommandPermission>,
    #[serde(default)]
    pub containers: ContainerPermission,
    #[serde(default)]
    pub side_effects: SideEffects,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommandPermission {
    pub bin: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub purpose: String,
    #[serde(default)]
    pub isolation: Option<CommandIsolation>,
    #[serde(default)]
    pub image: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CommandIsolation {
    Container,
    TrustedHost,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolDef {
    pub kind: String,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default = "default_tool_network")]
    pub network: ToolNetwork,
    #[serde(default = "default_tool_workspace")]
    pub workspace: ToolWorkspace,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_output_limit_bytes")]
    pub output_limit_bytes: usize,
    #[serde(default)]
    pub resolution: ToolResolution,
    #[serde(default)]
    pub backends: ToolBackends,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolBackends {
    #[serde(default)]
    pub host: Option<HostToolBackend>,
    #[serde(default)]
    pub bundled: Option<BundledToolBackend>,
    #[serde(default)]
    pub container: Option<ContainerToolBackend>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HostToolBackend {
    pub bin: String,
    #[serde(default)]
    pub version_command: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BundledToolBackend {
    pub bin: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContainerToolBackend {
    pub image: String,
    #[serde(default = "default_container_mount")]
    pub mount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolResolution {
    #[serde(default)]
    pub allowed_backends: Vec<ToolBackendKind>,
    #[serde(default)]
    pub preferred_backends: Vec<ToolBackendKind>,
    #[serde(default)]
    pub fallback: ToolFallback,
}

impl Default for ToolResolution {
    fn default() -> Self {
        Self {
            allowed_backends: Vec::new(),
            preferred_backends: Vec::new(),
            fallback: ToolFallback::Explicit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolBackendKind {
    Host,
    Bundled,
    Container,
}

impl std::fmt::Display for ToolBackendKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Host => "host",
            Self::Bundled => "bundled",
            Self::Container => "container",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolFallback {
    #[default]
    Explicit,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolNetwork {
    None,
    Permissioned,
}

fn default_tool_network() -> ToolNetwork {
    ToolNetwork::None
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolWorkspace {
    ReadOnly,
    Writable,
    None,
}

fn default_tool_workspace() -> ToolWorkspace {
    ToolWorkspace::ReadOnly
}

fn default_timeout_seconds() -> u64 {
    30
}

fn default_output_limit_bytes() -> usize {
    1024 * 1024
}

fn default_command_input_limit_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_http_redirect_limit() -> usize {
    5
}

fn default_file_input_limit_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_file_count_limit() -> usize {
    100_000
}

fn default_input_total_limit_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_output_file_limit_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_output_total_limit_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_output_artifact_limit() -> usize {
    10_000
}

fn default_template_source_limit_bytes() -> usize {
    1024 * 1024
}

fn default_template_context_limit_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_journal_event_limit_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_journal_total_limit_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_journal_event_count_limit() -> usize {
    100_000
}

fn default_state_limit_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_template_output_limit_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_template_fuel() -> u64 {
    1_000_000
}

fn default_max_total_steps() -> usize {
    10_000
}

fn default_container_mount() -> String {
    "/work".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContainerPermission {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub runtime: Option<ContainerRuntime>,
    #[serde(default)]
    pub images: Vec<String>,
    #[serde(default)]
    pub on_missing: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContainerRuntime {
    Docker,
    Podman,
    DockerRunsc,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SideEffects {
    #[default]
    None,
    Confirm,
    DryRunFirst,
    Allowed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub file_env: Option<String>,
}

impl SecretRef {
    pub fn source_env_name(&self) -> Option<&str> {
        self.env.as_deref().or(self.file_env.as_deref())
    }

    pub fn source_label(&self) -> Option<String> {
        self.env
            .as_deref()
            .map(|name| format!("env:{name}"))
            .or_else(|| {
                self.file_env
                    .as_deref()
                    .map(|name| format!("file_env:{name}"))
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeDef {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: StepType,
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(default)]
    pub when: Option<Expr>,
    #[serde(default)]
    pub on_deps: OnDeps,
    #[serde(default)]
    pub context: Vec<ContextRef>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub artifact: Option<NodeArtifactDef>,
    #[serde(default)]
    pub on_fail: Option<OnFail>,
    #[serde(default)]
    pub failure: Option<FailurePolicy>,
    #[serde(default)]
    #[schemars(skip)]
    pub params: toml::Table,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ContextRef {
    Short(String),
    Resource(ResourceContextRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceContextRef {
    pub resource: String,
    #[serde(default)]
    pub select: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
}

impl NodeDef {
    pub fn artifact_path_template(&self) -> Option<&str> {
        self.artifact.as_ref()?;
        self.param_str("output_file")
            .or_else(|| self.param_str("target"))
            .or_else(|| self.param_str("destination"))
    }

    pub fn param(&self, key: &str) -> Option<&toml::Value> {
        self.params.get(key)
    }

    pub fn param_str(&self, key: &str) -> Option<&str> {
        self.param(key).and_then(toml::Value::as_str)
    }

    pub fn param_array(&self, key: &str) -> Option<&toml::value::Array> {
        self.param(key).and_then(toml::Value::as_array)
    }

    pub fn param_table(&self, key: &str) -> Option<&toml::Table> {
        self.param(key).and_then(toml::Value::as_table)
    }

    pub fn params_json(&self) -> Value {
        let mut object = serde_json::Map::new();
        for (key, value) in &self.params {
            object.insert(key.clone(), toml_value_to_json(value));
        }

        if !self.context.is_empty() && self.kind.as_str().starts_with("llm.") {
            object.insert("context".into(), serde_json::json!(self.context));
        }

        Value::Object(object)
    }

    pub fn deserialize_params<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(self.params_json())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeArtifactDef {
    pub label: String,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub preview: ArtifactPreview,
}

fn default_true() -> bool {
    true
}

fn toml_value_to_json(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(value) => Value::String(value.clone()),
        toml::Value::Integer(value) => Value::Number((*value).into()),
        toml::Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        toml::Value::Boolean(value) => Value::Bool(*value),
        toml::Value::Datetime(value) => Value::String(value.to_string()),
        toml::Value::Array(values) => Value::Array(values.iter().map(toml_value_to_json).collect()),
        toml::Value::Table(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), toml_value_to_json(value)))
                .collect(),
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
#[schemars(transparent)]
pub struct StepType(String);

impl StepType {
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        debug_assert!(
            validate_step_type(&value).is_ok(),
            "invalid step type `{value}`"
        );
        Self(value)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_step_type(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_llm(&self) -> bool {
        self.0.starts_with("llm.")
    }
}

impl Serialize for StepType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for StepType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        StepType::parse(value).map_err(D::Error::custom)
    }
}

impl From<&str> for StepType {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for StepType {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for StepType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate_step_type(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("step type must not be empty".into());
    }
    if value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
    }) {
        Ok(())
    } else {
        Err(format!(
            "step type `{value}` must use only lowercase ASCII letters, digits, `_`, and `.`"
        ))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnDeps {
    /// Run only after every dependency succeeds; skip if any dependency skips or fails.
    #[default]
    AllSucceeded,
    /// Run after at least one dependency succeeds; skip only when all dependencies are terminal and none succeeded.
    AnySucceeded,
    /// Run once every dependency reached a terminal state and none failed;
    /// dependencies skipped by `when` satisfy this policy. Use it to keep
    /// conditional (`when`) branches from cascading skips onto the rest of
    /// the flow.
    NoneFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpectDef {
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub exit_code_in: Vec<i32>,
    #[serde(default)]
    pub stdout_contains: Option<String>,
    #[serde(default)]
    pub stderr_contains: Option<String>,
    #[serde(default)]
    pub stdout_matches: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MountDef {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum OnFail {
    Repair {
        repair: String,
        recheck: String,
        max_attempts: u32,
        #[serde(default)]
        on_exhausted: ExhaustedAction,
    },
    Regenerate {
        max_attempts: u32,
        #[serde(default)]
        on_exhausted: ExhaustedAction,
    },
    AskUser,
    Route {
        to: String,
    },
    Fail,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExhaustedAction {
    #[default]
    Fail,
    Route {
        to: String,
    },
    AskUser {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        fields: Vec<InputField>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ToolDecl {
    #[serde(rename = "fs.write")]
    FsWrite {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        path_prefix: String,
    },
    #[serde(rename = "command")]
    Command {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        command: Vec<String>,
    },
    #[serde(rename = "http")]
    Http {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        methods: Vec<String>,
        hosts: Vec<String>,
    },
    #[serde(rename = "ask_user")]
    AskUser {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
    },
    #[serde(rename = "web.search")]
    WebSearch {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        provider: Option<String>,
        #[serde(default = "default_search_max_results")]
        max_results: usize,
        #[serde(default = "default_search_max_calls")]
        max_calls: usize,
    },
    #[serde(rename = "mcp")]
    Mcp {
        name: String,
        #[serde(default)]
        description: Option<String>,
        server: String,
        tool: String,
        #[serde(default = "default_mcp_max_calls")]
        max_calls: usize,
        #[serde(default = "default_true")]
        side_effects: bool,
    },
    #[serde(rename = "agent")]
    Agent {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        #[serde(default)]
        output_schema: Option<String>,
        instructions: String,
        #[serde(default)]
        tools: Vec<String>,
        #[serde(default = "default_agent_tool_max_calls")]
        max_calls: usize,
        #[serde(default = "default_agent_tool_max_iterations")]
        max_iterations: usize,
        #[serde(default = "default_agent_tool_max_tokens_total")]
        max_tokens_total: u64,
        max_tool_calls_total: usize,
        #[serde(default)]
        model: Option<ModelRef>,
        #[serde(default)]
        fallback_models: Vec<ModelRef>,
        #[serde(default)]
        request: Box<LlmRequestPolicy>,
        #[serde(default)]
        on_failure: Box<AgentFailurePolicy>,
        #[serde(default)]
        handoff: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentFailurePolicy {
    #[serde(default)]
    pub default: AgentFailureAction,
    #[serde(default)]
    pub by_code: BTreeMap<RecoverableAgentFailureCode, AgentFailureAction>,
}

impl Default for AgentFailurePolicy {
    fn default() -> Self {
        Self {
            default: AgentFailureAction::ReturnError,
            by_code: BTreeMap::new(),
        }
    }
}

impl AgentFailurePolicy {
    pub fn action(&self, code: AgentFailureCode) -> AgentFailureAction {
        code.policy_code()
            .and_then(|code| self.by_code.get(&code).copied())
            .unwrap_or_else(|| {
                if code.is_recoverable() {
                    self.default
                } else {
                    AgentFailureAction::Fail
                }
            })
    }
}

impl ToolDecl {
    pub fn name(&self) -> &str {
        match self {
            Self::FsWrite { name, .. }
            | Self::Command { name, .. }
            | Self::Http { name, .. }
            | Self::AskUser { name, .. }
            | Self::WebSearch { name, .. }
            | Self::Mcp { name, .. }
            | Self::Agent { name, .. } => name,
        }
    }

    pub fn description(&self) -> Option<&str> {
        match self {
            Self::FsWrite { description, .. }
            | Self::Command { description, .. }
            | Self::Http { description, .. }
            | Self::AskUser { description, .. }
            | Self::WebSearch { description, .. }
            | Self::Mcp { description, .. }
            | Self::Agent { description, .. } => description.as_deref(),
        }
    }

    pub fn input_schema(&self) -> Option<&Value> {
        match self {
            Self::FsWrite { input_schema, .. }
            | Self::Command { input_schema, .. }
            | Self::Http { input_schema, .. }
            | Self::AskUser { input_schema, .. }
            | Self::Agent { input_schema, .. } => input_schema.as_ref(),
            Self::WebSearch { .. } | Self::Mcp { .. } => None,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::FsWrite { .. } => "fs.write",
            Self::Command { .. } => "command",
            Self::Http { .. } => "http",
            Self::AskUser { .. } => "ask_user",
            Self::WebSearch { .. } => "web.search",
            Self::Mcp { .. } => "mcp",
            Self::Agent { .. } => "agent",
        }
    }
}

fn default_search_max_results() -> usize {
    5
}

fn default_search_max_calls() -> usize {
    3
}

fn default_mcp_max_calls() -> usize {
    3
}

fn default_agent_tool_max_iterations() -> usize {
    6
}

fn default_agent_tool_max_calls() -> usize {
    3
}

fn default_agent_tool_max_tokens_total() -> u64 {
    32_768
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    #[serde(default)]
    pub extras: Vec<OutputExtraDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputExtraDef {
    pub glob: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub preview: ArtifactPreview,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FailurePolicy {
    #[serde(default)]
    pub default: FailureAction,
    #[serde(default)]
    pub by_kind: BTreeMap<FailureKind, FailureAction>,
}

impl FailurePolicy {
    pub fn action(&self, kind: FailureKind) -> FailureAction {
        self.by_kind.get(&kind).copied().unwrap_or(self.default)
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FailureAction {
    Reject,
    Clarify,
    Clamp,
    #[default]
    Fail,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Schema,
    Range,
    Permission,
    OutOfContract,
    Execution,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JournalPolicy {
    #[serde(default)]
    pub retain_days: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_with_llm_settings(settings: &str) -> Manifest {
        let max_tokens = if settings
            .lines()
            .any(|line| line.trim_start().starts_with("max_tokens"))
        {
            ""
        } else {
            "max_tokens = 2048"
        };
        toml::from_str(&format!(
            r#"
[generator]
id = "llm-settings"
name = "LLM Settings"
version = "0.1.0"
qcg_version = "^0.1"

[llm]
{max_tokens}
{settings}
"#
        ))
        .expect("manifest should parse")
    }

    fn manifest_with_field(field: InputField) -> Manifest {
        Manifest {
            generator: GeneratorMeta {
                id: "test".into(),
                name: "Test".into(),
                version: "0.1.0".into(),
                description: String::new(),
                authors: vec![],
                qcg_version: String::new(),
            },
            llm: None,
            inputs: InputSpec {
                stages: vec![InputStage {
                    id: "basic".into(),
                    when: None,
                    fields: vec![field],
                }],
            },
            resources: BTreeMap::new(),
            tools: BTreeMap::new(),
            permissions: Permissions::default(),
            secrets: BTreeMap::new(),
            runtime: RuntimeLimits::default(),
            budget: RunBudget::default(),
            flow: vec![],
            parallel: vec![],
            blocks: BTreeMap::new(),
            outputs: OutputSpec::default(),
            failure: FailurePolicy::default(),
            journal: JournalPolicy::default(),
            assets: AssetSpec::default(),
        }
    }

    fn input_field_defaults() -> InputField {
        InputField {
            id: String::new(),
            label: None,
            label_i18n: BTreeMap::new(),
            description: None,
            description_i18n: BTreeMap::new(),
            placeholder: None,
            placeholder_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: Vec::new(),
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            schema: None,
            ui: Default::default(),
        }
    }

    #[test]
    fn agent_failure_policy_returns_recoverable_errors_and_propagates_run_boundaries() {
        let policy = AgentFailurePolicy::default();
        assert_eq!(
            policy.action(AgentFailureCode::TokenBudgetExceeded),
            AgentFailureAction::ReturnError
        );
        assert_eq!(
            policy.action(AgentFailureCode::ProviderFailed),
            AgentFailureAction::ReturnError
        );
        assert_eq!(
            policy.action(AgentFailureCode::RunBudgetExceeded),
            AgentFailureAction::Fail
        );
        assert_eq!(
            policy.action(AgentFailureCode::Cancelled),
            AgentFailureAction::Fail
        );

        let policy = AgentFailurePolicy {
            default: AgentFailureAction::ReturnError,
            by_code: BTreeMap::from([(
                RecoverableAgentFailureCode::ProviderFailed,
                AgentFailureAction::Fail,
            )]),
        };
        assert_eq!(
            policy.action(AgentFailureCode::ProviderFailed),
            AgentFailureAction::Fail
        );
        assert_eq!(
            policy.action(AgentFailureCode::ValidationFailed),
            AgentFailureAction::ReturnError
        );

        let error = toml::from_str::<AgentFailurePolicy>(
            "default = \"return_error\"\n[by_code]\ncancelled = \"return_error\"",
        )
        .expect_err("cancellation must not be exposed as a recoverable policy key");
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn runtime_template_limits_have_positive_defaults_and_reject_zero() {
        let defaults = RuntimeLimits::default();
        assert_eq!(defaults.output_file_limit_bytes, 64 * 1024 * 1024);
        assert_eq!(defaults.output_total_limit_bytes, 256 * 1024 * 1024);
        assert_eq!(defaults.output_artifact_limit, 10_000);
        assert_eq!(defaults.template_source_limit_bytes, 1024 * 1024);
        assert_eq!(defaults.template_context_limit_bytes, 16 * 1024 * 1024);
        assert_eq!(defaults.template_output_limit_bytes, 16 * 1024 * 1024);
        assert_eq!(defaults.template_fuel, 1_000_000);

        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            ..input_field_defaults()
        });
        manifest.generator.qcg_version = "^0.1".into();
        manifest
            .validate()
            .expect("positive template limits should validate");

        manifest.runtime.template_output_limit_bytes = 0;
        let error = manifest
            .validate()
            .expect_err("zero template output limit must be rejected");
        assert!(error.to_string().contains("template_output_limit_bytes"));

        manifest.runtime.template_output_limit_bytes = defaults.template_output_limit_bytes;
        manifest.runtime.template_fuel = 0;
        let error = manifest
            .validate()
            .expect_err("zero template fuel must be rejected");
        assert!(error.to_string().contains("template_fuel"));

        manifest.runtime.template_fuel = defaults.template_fuel;
        manifest.runtime.template_source_limit_bytes = 0;
        let error = manifest
            .validate()
            .expect_err("zero template source limit must be rejected");
        assert!(error.to_string().contains("template_source_limit_bytes"));

        manifest.runtime.template_source_limit_bytes = defaults.template_source_limit_bytes;
        manifest.runtime.template_context_limit_bytes = 0;
        let error = manifest
            .validate()
            .expect_err("zero template context limit must be rejected");
        assert!(error.to_string().contains("template_context_limit_bytes"));

        manifest.runtime.template_context_limit_bytes = defaults.template_context_limit_bytes;
        manifest.runtime.output_file_limit_bytes = 0;
        let error = manifest
            .validate()
            .expect_err("zero output file limit must be rejected");
        assert!(error.to_string().contains("output_file_limit_bytes"));

        manifest.runtime.output_file_limit_bytes = defaults.output_file_limit_bytes;
        manifest.runtime.output_total_limit_bytes = 0;
        let error = manifest
            .validate()
            .expect_err("zero output total limit must be rejected");
        assert!(error.to_string().contains("output_total_limit_bytes"));

        manifest.runtime.output_total_limit_bytes = defaults.output_total_limit_bytes;
        manifest.runtime.output_artifact_limit = 0;
        let error = manifest
            .validate()
            .expect_err("zero output artifact limit must be rejected");
        assert!(error.to_string().contains("output_artifact_limit"));
    }

    #[test]
    fn runtime_and_budget_limits_reject_values_above_hard_ceiling() {
        let base = manifest_with_field(InputField {
            id: "name".into(),
            ..input_field_defaults()
        });
        for (field, mutate) in [
            (
                "command_timeout_seconds",
                (|manifest: &mut Manifest| {
                    manifest.runtime.command_timeout_seconds = MAX_RUNTIME_TIMEOUT_SECONDS + 1;
                }) as fn(&mut Manifest),
            ),
            ("command_output_limit_bytes", |manifest: &mut Manifest| {
                manifest.runtime.command_output_limit_bytes = MAX_RUNTIME_LIMIT_BYTES + 1;
            }),
            ("file_count_limit", |manifest: &mut Manifest| {
                manifest.runtime.file_count_limit = MAX_RUNTIME_COUNT_LIMIT + 1;
            }),
            ("http_redirect_limit", |manifest: &mut Manifest| {
                manifest.runtime.http_redirect_limit = MAX_RUNTIME_HTTP_REDIRECTS + 1;
            }),
            ("template_fuel", |manifest: &mut Manifest| {
                manifest.runtime.template_fuel = MAX_RUNTIME_TEMPLATE_FUEL + 1;
            }),
            ("max_steps", |manifest: &mut Manifest| {
                manifest.budget.max_steps = MAX_BUDGET_STEPS + 1;
            }),
            ("max_tokens", |manifest: &mut Manifest| {
                manifest.budget.max_tokens = Some(MAX_BUDGET_TOKENS + 1);
            }),
            ("max_elapsed_seconds", |manifest: &mut Manifest| {
                manifest.budget.max_elapsed_seconds = Some(MAX_BUDGET_ELAPSED_SECONDS + 1);
            }),
        ] {
            let mut manifest = base.clone();
            mutate(&mut manifest);
            let error = manifest
                .validate()
                .expect_err("hard ceiling must be enforced");
            assert!(error.to_string().contains(field), "{error}");
        }
    }

    #[test]
    fn resolve_inputs_applies_defaults_and_patterns() {
        let manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            description: None,
            description_i18n: BTreeMap::new(),
            placeholder: None,
            placeholder_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: true,
            default: Some(Value::String("alpha".into())),
            pattern: Some("^[a-z]+$".into()),
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            schema: None,
            ui: Default::default(),
        });
        let resolved = manifest.resolve_inputs(BTreeMap::new()).unwrap();
        assert_eq!(resolved.get("name"), Some(&Value::String("alpha".into())));
    }

    #[test]
    fn input_schema_validates_defaults_and_resolved_values() {
        let manifest = manifest_with_field(InputField {
            id: "count".into(),
            kind: FieldType::Number,
            required: true,
            schema: Some(serde_json::json!({
                "type": "number",
                "minimum": 2,
                "maximum": 4
            })),
            ..input_field_defaults()
        });
        manifest
            .validate()
            .expect("valid field schema should compile");
        let error = manifest
            .resolve_inputs(BTreeMap::from([("count".into(), serde_json::json!(1))]))
            .expect_err("value outside field schema should fail");
        assert!(error.to_string().contains("JSON Schema"));

        let invalid_default = manifest_with_field(InputField {
            id: "count".into(),
            kind: FieldType::Number,
            default: Some(serde_json::json!(5)),
            schema: Some(serde_json::json!({ "type": "number", "maximum": 4 })),
            ..input_field_defaults()
        });
        invalid_default
            .validate()
            .expect_err("invalid default should fail contract validation");

        let custom = manifest_with_field(InputField {
            id: "coordinates".into(),
            kind: FieldType::Custom("geo.point".into()),
            required: true,
            schema: Some(serde_json::json!({
                "type": "object",
                "required": ["lat", "lon"],
                "properties": {
                    "lat": { "type": "number" },
                    "lon": { "type": "number" }
                }
            })),
            ..input_field_defaults()
        });
        custom
            .resolve_inputs(BTreeMap::from([(
                "coordinates".into(),
                serde_json::json!({ "lat": 35.0, "lon": 139.0 }),
            )]))
            .expect("custom fields should derive their value shape from schema");

        for kind in ["geo.point", "Geo Point"] {
            let manifest = manifest_with_field(InputField {
                id: "coordinates".into(),
                kind: FieldType::Custom(kind.into()),
                required: true,
                schema: None,
                ..input_field_defaults()
            });
            let error = manifest
                .validate()
                .expect_err("custom fields without a valid kind and schema must fail");
            assert!(
                error.to_string().contains("requires schema")
                    || error.to_string().contains("invalid custom field type")
            );
        }
    }

    #[test]
    fn validates_on_exhausted_forms_inside_reusable_blocks() {
        let mut manifest = manifest_with_field(InputField {
            id: "request".into(),
            ..input_field_defaults()
        });
        manifest.blocks.insert(
            "retry".into(),
            vec![NodeDef {
                id: "generate".into(),
                kind: StepType::new("render"),
                needs: Vec::new(),
                when: None,
                on_deps: OnDeps::default(),
                context: Vec::new(),
                output: None,
                artifact: None,
                on_fail: Some(OnFail::Regenerate {
                    max_attempts: 1,
                    on_exhausted: ExhaustedAction::AskUser {
                        title: None,
                        fields: vec![input_field_defaults()],
                    },
                }),
                failure: None,
                params: toml::Table::new(),
            }],
        );

        let error = manifest
            .validate()
            .expect_err("an empty block form field id should be rejected");
        assert!(error.to_string().contains("on_exhausted input field id"));
    }

    #[test]
    fn llm_reasoning_effort_accepts_every_supported_level() {
        for effort in ["none", "minimal", "low", "medium", "high", "xhigh", "max"] {
            manifest_with_llm_settings(&format!("reasoning_effort = \"{effort}\""))
                .validate()
                .expect("reasoning effort should validate");
        }
    }

    #[test]
    fn llm_reasoning_effort_rejects_sampling_controls() {
        for field in ["temperature = 0.5", "seed = 42"] {
            let manifest =
                manifest_with_llm_settings(&format!("reasoning_effort = \"high\"\n{field}"));
            let error = manifest
                .validate()
                .expect_err("reasoning and sampling controls must not be mixed");
            assert!(error.to_string().contains("must be omitted"));
        }
    }

    #[test]
    fn llm_limits_reject_invalid_values() {
        for settings in [
            "max_tokens = 0",
            "temperature = -0.1",
            "temperature = 2.1",
            "max_context_bytes = 0",
            "max_context_tokens = 0",
            "max_context_bytes = 1073741825",
            "max_context_tokens = 268435457",
            "max_media_bytes = 1073741825",
        ] {
            let error = manifest_with_llm_settings(settings)
                .validate()
                .expect_err("invalid LLM limits must be rejected");
            assert!(error.to_string().contains("[llm]."));
        }
    }

    #[test]
    fn llm_max_tokens_is_required() {
        let mut manifest = manifest_with_llm_settings("");
        manifest.llm.as_mut().expect("LLM config").max_tokens = None;
        let error = manifest
            .validate()
            .expect_err("max_tokens must be explicit");
        assert!(error.to_string().contains("max_tokens is required"));
    }

    #[test]
    fn llm_requires_rejects_unknown_and_duplicate_capabilities() {
        for settings in [
            "requires = [\"unknown\"]",
            "requires = [\"json_schema\", \"json_schema\"]",
        ] {
            let error = manifest_with_llm_settings(settings)
                .validate()
                .expect_err("invalid requires entries must fail");
            assert!(error.to_string().contains("[llm].requires"));
        }
        manifest_with_llm_settings("requires = [\"structured_output_with_tools\"]")
            .validate()
            .expect("structured output with tools is a declared provider capability");
    }

    #[test]
    fn llm_model_names_must_not_be_empty() {
        for model in [
            "model = { provider = \"\", model = \"gpt\" }",
            "model = { provider = \"openai\", model = \"\" }",
        ] {
            let error = manifest_with_llm_settings(model)
                .validate()
                .expect_err("empty model identifiers must fail");
            assert!(error.to_string().contains("must not be empty"));
        }
    }

    #[test]
    fn resolve_inputs_validates_file_values_as_canonical_objects() {
        let manifest = manifest_with_field(InputField {
            id: "attachment".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::File,
            required: true,
            default: None,
            pattern: Some(r"^[a-z]+\.txt$".into()),
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        let file = FileValue::from_text("note.txt", "hello").expect("file should be valid");
        let resolved = manifest
            .resolve_inputs(BTreeMap::from([(
                "attachment".into(),
                serde_json::to_value(file).expect("file should encode"),
            )]))
            .expect("valid file input should resolve");
        assert!(resolved.contains_key("attachment"));

        let error = manifest
            .resolve_inputs(BTreeMap::from([(
                "attachment".into(),
                serde_json::json!({"name": "../note.txt", "text": "hello"}),
            )]))
            .expect_err("unsafe file names must be rejected");
        assert!(error.to_string().contains("invalid file value"));
    }

    #[test]
    fn assets_must_be_declared_safe_package_paths() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.assets.files = vec!["ui/index.html".into(), "ui/app.js".into()];
        manifest
            .validate()
            .expect("declared assets should validate");

        manifest.assets.files[1] = "../app.js".into();
        let error = manifest
            .validate()
            .expect_err("path traversal must be rejected");
        assert!(error.to_string().contains("safe relative path"));
    }

    #[test]
    fn assets_accept_arbitrary_extensions_and_extensionless_files() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.assets.files = vec!["bundle.wasm".into(), "NOTICE".into()];
        manifest
            .validate()
            .expect("declared assets with arbitrary names should validate");
    }

    #[test]
    fn contract_load_allows_unbuilt_asset_directories_but_not_missing_asset_files() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must follow the Unix epoch")
            .as_nanos();
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-contract-unbuilt-assets-{}-{unique}",
            std::process::id()
        )))
        .expect("temporary path must be UTF-8");
        fs::create_dir(&root).expect("temporary contract directory must be created");
        fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "unbuilt-assets"
version = "0.1.0"
qcg_version = "^0.1"

[assets]
dirs = ["ui"]
meta = { entry = "ui/index.html" }
"#,
        )
        .expect("manifest must be written");

        Contract::load(&root).expect("an unbuilt declared asset directory must remain loadable");

        fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "missing-file-asset"
version = "0.1.0"
qcg_version = "^0.1"

[assets]
files = ["ui/index.html"]
"#,
        )
        .expect("manifest must be replaced");
        let error = Contract::load(&root)
            .expect_err("an explicitly declared missing asset file must be rejected");
        assert!(error.to_string().contains("asset file `ui/index.html`"));

        fs::remove_dir_all(&root).expect("temporary contract directory must be removed");
    }

    #[test]
    fn output_artifact_mime_must_be_a_valid_media_type() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.outputs.extras.push(OutputExtraDef {
            glob: "reports/*.json".into(),
            label: "Reports".into(),
            required: false,
            mime: Some("not a media type".into()),
            description: String::new(),
            preview: ArtifactPreview::Auto,
        });
        let error = manifest
            .validate()
            .expect_err("invalid artifact MIME type must be rejected");
        assert!(error.to_string().contains("invalid MIME type"));
    }

    #[test]
    fn resolve_inputs_rejects_missing_required_values() {
        let manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        assert!(manifest.resolve_inputs(BTreeMap::new()).is_err());
    }

    #[test]
    fn resolve_inputs_rejects_short_lists() {
        let manifest = manifest_with_field(InputField {
            id: "items".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::List,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: Some(2),
            item_type: Some(FieldType::String),
            ..input_field_defaults()
        });
        let mut input = BTreeMap::new();
        input.insert(
            "items".into(),
            Value::Array(vec![Value::String("one".into())]),
        );
        assert!(manifest.resolve_inputs(input).is_err());
    }

    #[test]
    fn contract_load_rejects_a_future_qcg_runtime_requirement() {
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-contract-version-{}", std::process::id())),
        )
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary contract directory should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "version-check"
version = "0.1.0"
qcg_version = ">=99"
"#,
        )
        .expect("manifest should be written");
        let error = Contract::load(&root).expect_err("future runtime requirement must fail");
        assert!(error.to_string().contains("requires qcg_version"));
    }

    #[test]
    fn contract_load_rejects_resource_paths_before_loader_dispatch() {
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-contract-resource-path-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        )))
        .expect("temporary path should be UTF-8");
        std::fs::create_dir_all(&root).expect("temporary contract directory should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "resource-path-check"
version = "0.1.0"
qcg_version = "^0.1"

[resources.escape]
type = "file"
path = "../outside.txt"
"#,
        )
        .expect("manifest should be written");

        let error = Contract::load(&root).expect_err("unsafe resource path must fail at load time");
        assert!(
            error.to_string().contains("resource `escape` path"),
            "{error}"
        );
        assert!(error.to_string().contains("safe relative path"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary contract directory should be removed");
    }

    #[test]
    fn resource_sources_are_exclusive_and_builtin_sources_are_required() {
        let cases = [
            (
                r#"
[resources.ambiguous]
type = "file"
path = "resource.txt"
url = "https://example.test/resource.txt"
"#,
                "requires path and forbids url",
            ),
            (
                r#"
[resources.missing]
type = "file"
"#,
                "type `file` requires path",
            ),
            (
                r#"
[resources.missing]
type = "url"
"#,
                "type `url` requires url",
            ),
            (
                r#"
[resources.missing]
type = "openapi"
"#,
                "type `openapi` requires exactly one of path or url",
            ),
        ];
        for (resource, expected) in cases {
            let manifest: Manifest = toml::from_str(&format!(
                r#"
[generator]
id = "resource-validation"
version = "0.1.0"
qcg_version = "^0.1"
{resource}
"#
            ))
            .expect("manifest should parse");
            let error = manifest
                .validate()
                .expect_err("invalid resource source declaration must fail");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn exec_resource_requires_a_declared_bounded_command() {
        let valid: Manifest = toml::from_str(
            r#"
[generator]
id = "exec-resource"
version = "0.1.0"
qcg_version = "^0.1"

[resources.generated]
type = "exec"
llm_visible = true

[resources.generated.params]
command = ["printf", "hello"]
max_bytes = 1024

[[permissions.commands]]
bin = "printf"
args = ["hello"]
purpose = "load deterministic resource"
isolation = "trusted_host"
"#,
        )
        .expect("manifest should parse");
        valid.validate().expect("exec resource should validate");

        let mut oversized = valid.clone();
        oversized
            .resources
            .get_mut("generated")
            .expect("resource must exist")
            .params
            .insert("max_bytes".into(), Value::from(5 * 1024 * 1024));
        let error = oversized
            .validate()
            .expect_err("resource limit above the command capture limit must fail");
        assert!(error.to_string().contains("command_output_limit_bytes"));

        for (manifest, expected) in [
            (
                r#"
[generator]
id = "exec-resource"
version = "0.1.0"
qcg_version = "^0.1"
[resources.generated]
type = "exec"
[resources.generated.params]
command = ["printf", "hello"]
"#,
                "not declared in permissions.commands",
            ),
            (
                r#"
[generator]
id = "exec-resource"
version = "0.1.0"
qcg_version = "^0.1"
[resources.generated]
type = "exec"
path = "resource.txt"
[resources.generated.params]
command = ["printf", "hello"]
"#,
                "forbids path and url",
            ),
        ] {
            let manifest: Manifest = toml::from_str(manifest).expect("manifest should parse");
            let error = manifest
                .validate()
                .expect_err("invalid exec resource must fail");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn contract_load_rejects_resource_paths_with_wrong_types() {
        let cases = [
            ("file", true, "must be a file"),
            ("dir", false, "must be a directory"),
            ("openapi", true, "must be a file"),
        ];
        for (index, (kind, create_directory, expected)) in cases.into_iter().enumerate() {
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
                "qcg-contract-resource-type-{}-{index}",
                std::process::id()
            )))
            .expect("temporary path should be UTF-8");
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).expect("temporary contract directory should be created");
            let resource_path = root.join("resource");
            if create_directory {
                std::fs::create_dir_all(&resource_path)
                    .expect("resource directory should be created");
            } else {
                std::fs::write(&resource_path, "resource")
                    .expect("resource file should be written");
            }
            std::fs::write(
                root.join("qcg.toml"),
                format!(
                    r#"
[generator]
id = "resource-type-validation"
version = "0.1.0"
qcg_version = "^0.1"

[resources.test]
type = "{kind}"
path = "resource"
"#
                ),
            )
            .expect("manifest should be written");

            let error = Contract::load(&root)
                .expect_err("resource path with the wrong type must fail at load time");
            assert!(error.to_string().contains(expected), "{error}");
            std::fs::remove_dir_all(root).expect("temporary contract directory should be removed");
        }
    }

    #[test]
    fn step_type_accepts_namespaced_lowercase_ids() {
        let step_type = StepType::parse("llm.generate").expect("step type should be valid");
        assert_eq!(step_type.as_str(), "llm.generate");
    }

    #[test]
    fn step_type_rejects_uppercase_and_separators() {
        assert!(StepType::parse("Llm.Generate").is_err());
        assert!(StepType::parse("llm-generate").is_err());
        assert!(StepType::parse("").is_err());
    }

    #[test]
    fn resource_kind_accepts_builtins() {
        let resource: ResourceDef =
            toml::from_str("type = \"openapi\"\nurl = \"https://example.test/openapi.json\"")
                .expect("built-in resource kind should deserialize");
        assert_eq!(resource.kind, ResourceKind::Openapi);
    }

    #[test]
    fn node_def_rejects_step_params_outside_params_table() {
        let source = r#"
id = "echo"
type = "demo.echo"
message = "hello"
destination = "result.txt"
"#;
        let error = toml::from_str::<NodeDef>(source)
            .expect_err("step params outside params must be rejected");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn node_def_params_json_contains_only_closed_params_table() {
        let source = r#"
id = "write"
type = "write"
context = ["inputs.*"]

[params]
output_file = "result.txt"
content = "hello"
"#;
        let node: NodeDef = toml::from_str(source).expect("node should parse");
        let params = node.params_json();
        assert_eq!(params["output_file"], Value::String("result.txt".into()));
        assert_eq!(params["content"], Value::String("hello".into()));
        assert!(params.get("context").is_none());
        assert_eq!(node.context, vec![ContextRef::Short("inputs.*".into())]);
    }

    #[test]
    fn node_def_parses_closed_resource_context_selector() {
        let source = r#"
id = "draft"
type = "llm.generate"
context = [
  { resource = "todo_api", select = "operations", tag = "todos", path = "openapi.json" },
]

[params]
prompt = "prompts/draft.j2"
output_file = "draft.md"
"#;
        let node: NodeDef = toml::from_str(source).expect("resource selector should parse");
        assert_eq!(
            node.context,
            vec![ContextRef::Resource(ResourceContextRef {
                resource: "todo_api".into(),
                select: Some("operations".into()),
                tag: Some("todos".into()),
                path: Some("openapi.json".into()),
            })]
        );
        assert_eq!(node.params_json()["context"][0]["resource"], "todo_api");
    }

    #[test]
    fn node_def_deserializes_open_params() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct EchoParams {
            message: String,
            destination: String,
        }

        let source = r#"
id = "echo"
type = "demo.echo"

[params]
message = "hello"
destination = "result.txt"
"#;
        let node: NodeDef = toml::from_str(source).expect("node should parse");
        let params: EchoParams = node
            .deserialize_params()
            .expect("params should deserialize");
        assert_eq!(
            params,
            EchoParams {
                message: "hello".into(),
                destination: "result.txt".into(),
            }
        );
    }

    #[test]
    fn failure_policy_resolves_kind_and_node_override() {
        let global: FailurePolicy = toml::from_str(
            r#"
default = "fail"

[by_kind]
permission = "reject"
range = "clamp"
"#,
        )
        .expect("global failure policy should parse");
        assert_eq!(
            global.action(FailureKind::Permission),
            FailureAction::Reject
        );
        assert_eq!(global.action(FailureKind::Range), FailureAction::Clamp);
        assert_eq!(global.action(FailureKind::Schema), FailureAction::Fail);

        let node: NodeDef = toml::from_str(
            r#"
id = "generate"
type = "llm.generate"
failure = { default = "clarify", by_kind = { out_of_contract = "reject" } }
"#,
        )
        .expect("node failure override should parse");
        let override_policy = node.failure.expect("node override should exist");
        assert_eq!(
            override_policy.action(FailureKind::OutOfContract),
            FailureAction::Reject
        );
        assert_eq!(
            override_policy.action(FailureKind::Schema),
            FailureAction::Clarify
        );
    }

    #[test]
    fn command_permissions_require_explicit_isolation() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.permissions.commands.push(CommandPermission {
            bin: "tool".into(),
            args: vec![],
            purpose: "test".into(),
            isolation: None,
            image: None,
        });
        let error = manifest
            .validate()
            .expect_err("implicit host execution must be rejected");
        assert!(error.to_string().contains("must declare isolation"));
    }

    #[test]
    fn container_commands_require_digest_pinned_allowlisted_images() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.permissions.commands.push(CommandPermission {
            bin: "tool".into(),
            args: vec![],
            purpose: "test".into(),
            isolation: Some(CommandIsolation::Container),
            image: Some("example/tool:latest".into()),
        });
        let error = manifest
            .validate()
            .expect_err("mutable image tags must be rejected");
        assert!(error.to_string().contains("pinned by digest"));
    }

    #[test]
    fn unimplemented_tool_backends_are_not_in_the_contract_type() {
        for source in [
            "[wasm]\nmodule = \"validator.wasm\"\nsha256 = \"abc\"\n",
            "[remote]\nurl = \"https://validator.example/check\"\n",
        ] {
            let error = toml::from_str::<ToolBackends>(source)
                .expect_err("unimplemented backend must be rejected during deserialization");
            assert!(error.to_string().contains("unknown field"));
        }
    }

    #[test]
    fn resource_kind_rejects_unknown_values() {
        for kind in ["OpenApi", "open-api", "acme.custom", ""] {
            let error = toml::from_str::<ResourceDef>(&format!("type = {kind:?}"))
                .expect_err("unknown resource kind must be rejected");
            assert!(error.to_string().contains("unknown variant"), "{error}");
        }
    }

    #[test]
    fn bounded_json_schema_accepts_internal_refs_and_rejects_external_refs() {
        validate_bounded_json_schema(&serde_json::json!({
            "$defs": { "value": { "type": "string" } },
            "$ref": "#/$defs/value"
        }))
        .expect("internal references must remain available");
        let error = validate_bounded_json_schema(&serde_json::json!({
            "$ref": "https://example.invalid/schema.json"
        }))
        .expect_err("external schema retrieval must not occur during validation");
        assert!(error.contains("external reference"));
    }

    #[test]
    fn bounded_json_schema_rejects_excessive_depth_and_size() {
        let mut deep = serde_json::json!({ "type": "string" });
        for _ in 0..=MAX_JSON_SCHEMA_DEPTH {
            deep = serde_json::json!({ "not": deep });
        }
        assert!(
            validate_bounded_json_schema(&deep)
                .expect_err("excessive nesting must be rejected")
                .contains("nesting")
        );

        let oversized = serde_json::json!({
            "description": "x".repeat(MAX_JSON_SCHEMA_STRING_BYTES + 1)
        });
        assert!(
            validate_bounded_json_schema(&oversized)
                .expect_err("oversized strings must be rejected")
                .contains("string")
        );
    }

    #[test]
    fn manifest_reader_stops_at_the_configured_byte_limit() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must follow the Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "qcg-contract-manifest-limit-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).expect("temporary directory must be created");
        let path = directory.join("qcg.toml");
        fs::write(&path, "0123456789").expect("temporary manifest must be written");
        let path = Utf8PathBuf::from_path_buf(path).expect("temporary path must be UTF-8");

        let error = read_manifest_with_limit(&path, 8)
            .expect_err("a manifest larger than the read limit must fail");
        assert!(error.to_string().contains("exceeds 8 bytes"));

        fs::remove_dir_all(&directory).expect("temporary directory must be removed");
    }
}
