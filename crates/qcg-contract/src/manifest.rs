use crate::graph::Graph;
use camino::{Utf8Path, Utf8PathBuf};
pub use qcg_types::{
    AssetSpec, Expr, FieldType, FileValue, FileValueError, GeneratorMeta, InputField, InputSpec,
    InputStage, is_safe_relative_path,
};
use schemars::JsonSchema;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned, de::Error as DeError,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

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
        let source = fs::read_to_string(&manifest_path).map_err(|source| ContractError::Read {
            path: manifest_path.clone(),
            source,
        })?;
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
        let sha256 = hex::encode(Sha256::digest(source.as_bytes()));
        Ok(Self {
            root,
            manifest,
            graph,
            sha256,
        })
    }

    pub fn line_hint(&self, message: &str) -> String {
        let source = fs::read_to_string(self.root.join("qcg.toml")).unwrap_or_default();
        with_line_hint(&source, message)
    }
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
                validate_field_value(field, value)?;
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
        Ok(values)
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractValidator::default().validate(self)
    }

    pub fn validate_with(&self, validator: &ContractValidator) -> Result<(), ContractError> {
        validator.validate(self)
    }
}

fn validate_asset_files(root: &Utf8Path, assets: &AssetSpec) -> Result<(), ContractError> {
    let root = fs::canonicalize(root)
        .map_err(|_| ContractError::Invalid("generator root cannot be canonicalized".into()))?;
    for path in &assets.files {
        let file = fs::canonicalize(root.join(path)).map_err(|error| {
            ContractError::Invalid(format!("asset file `{path}` cannot be read: {error}"))
        })?;
        if !file.starts_with(&root) || !file.is_file() {
            return Err(ContractError::Invalid(format!(
                "asset file `{path}` is outside the generator package"
            )));
        }
    }
    Ok(())
}

pub fn validate_form_values(fields: &[InputField], values: &Value) -> Result<(), ContractError> {
    let object = values
        .as_object()
        .ok_or_else(|| ContractError::Invalid("form values must be a JSON object".into()))?;
    for field in fields {
        match object.get(&field.id) {
            Some(value) => validate_field_value(field, value)?,
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

pub trait ContractValidationRule: Send + Sync {
    fn name(&self) -> &'static str;

    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError>;
}

pub struct ContractValidator {
    rules: Vec<Box<dyn ContractValidationRule>>,
}

impl ContractValidator {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn with_builtin_rules() -> Self {
        let mut validator = Self::new();
        validator.register(GeneratorMetadataRule);
        validator.register(FlowNodeRule);
        validator.register(ToolRule);
        validator.register(OutputArtifactRule);
        validator.register(AssetRule);
        validator
    }

    pub fn register<R>(&mut self, rule: R)
    where
        R: ContractValidationRule + 'static,
    {
        self.rules.push(Box::new(rule));
    }

    pub fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for rule in &self.rules {
            rule.validate(manifest)?;
        }
        Ok(())
    }

    pub fn rule_names(&self) -> Vec<&'static str> {
        self.rules.iter().map(|rule| rule.name()).collect()
    }
}

impl Default for ContractValidator {
    fn default() -> Self {
        Self::with_builtin_rules()
    }
}

struct GeneratorMetadataRule;

impl ContractValidationRule for GeneratorMetadataRule {
    fn name(&self) -> &'static str {
        "generator_metadata"
    }

    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        if manifest.generator.id.trim().is_empty() {
            return Err(ContractError::Invalid("generator.id is required".into()));
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
        for (name, value) in [
            (
                "runtime.command_output_limit_bytes",
                manifest.runtime.command_output_limit_bytes,
            ),
            (
                "runtime.http_body_limit_bytes",
                manifest.runtime.http_body_limit_bytes,
            ),
        ] {
            if value == 0 {
                return Err(ContractError::Invalid(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        if let Some(llm) = &manifest.llm {
            if let Some(model) = &llm.model {
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
        }
        Ok(())
    }
}

struct FlowNodeRule;

#[derive(Debug, Deserialize)]
struct ForeachValidationParams {
    max_iterations: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CheckToolValidationParams {
    tool: Option<String>,
}

impl ContractValidationRule for FlowNodeRule {
    fn name(&self) -> &'static str {
        "flow_nodes"
    }

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
        for (block_id, nodes) in &manifest.blocks {
            if let Some(node) = nodes.iter().find(|node| node.kind.as_str() == "foreach") {
                return Err(ContractError::Invalid(format!(
                    "block `{block_id}` contains nested foreach node `{}`",
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
        _ if select.is_none() && reference.tag.is_none() && reference.path.is_none() => Ok(()),
        _ => Err(ContractError::Invalid(format!(
            "node `{node_id}` resource `{}` does not support selectors",
            reference.resource
        ))),
    }
}

struct ToolRule;

impl ContractValidationRule for ToolRule {
    fn name(&self) -> &'static str {
        "tools"
    }

    fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for (name, tool) in &manifest.tools {
            validate_tool(name, tool, &manifest.permissions)?;
        }
        Ok(())
    }
}

struct OutputArtifactRule;

impl ContractValidationRule for OutputArtifactRule {
    fn name(&self) -> &'static str {
        "output_artifacts"
    }

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

impl ContractValidationRule for AssetRule {
    fn name(&self) -> &'static str {
        "assets"
    }

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

fn validate_field_value(field: &InputField, value: &Value) -> Result<(), ContractError> {
    match field.kind {
        FieldType::Json => {}
        FieldType::File => {
            let file = FileValue::from_value(value).map_err(|error| match error {
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
        FieldType::String | FieldType::Text | FieldType::NaturalLanguage | FieldType::Custom(_) => {
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
    Ok(())
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
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub max_context_bytes: Option<usize>,
    #[serde(default)]
    pub max_context_tokens: Option<usize>,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub retry_prompt: Option<String>,
    #[serde(default)]
    pub seed: Option<u64>,
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
    #[serde(default = "default_output_limit_bytes")]
    pub command_output_limit_bytes: usize,
    #[serde(default = "default_timeout_seconds")]
    pub http_timeout_seconds: u64,
    #[serde(default = "default_output_limit_bytes")]
    pub http_body_limit_bytes: usize,
    #[serde(default = "default_http_redirect_limit")]
    pub http_redirect_limit: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            command_timeout_seconds: default_timeout_seconds(),
            command_output_limit_bytes: default_output_limit_bytes(),
            http_timeout_seconds: default_timeout_seconds(),
            http_body_limit_bytes: default_output_limit_bytes(),
            http_redirect_limit: default_http_redirect_limit(),
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
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
#[schemars(transparent)]
pub struct ResourceKind(String);

impl ResourceKind {
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        debug_assert!(
            validate_resource_kind(&value).is_ok(),
            "invalid resource kind `{value}`"
        );
        Self(value)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_resource_kind(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for ResourceKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ResourceKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        ResourceKind::parse(value).map_err(D::Error::custom)
    }
}

impl From<&str> for ResourceKind {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ResourceKind {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate_resource_kind(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("resource kind must not be empty".into());
    }
    if value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
    }) {
        Ok(())
    } else {
        Err(format!(
            "resource kind `{value}` must use only lowercase ASCII letters, digits, `_`, and `.`"
        ))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Trust {
    Trusted,
    #[default]
    Untrusted,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Permissions {
    #[serde(default = "default_workspace_permission")]
    pub fs_read: Vec<String>,
    #[serde(default = "default_workspace_permission")]
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

fn default_workspace_permission() -> Vec<String> {
    vec!["workspace".into()]
}

impl Default for Permissions {
    fn default() -> Self {
        Self {
            fs_read: default_workspace_permission(),
            fs_write: default_workspace_permission(),
            network: Vec::new(),
            commands: Vec::new(),
            containers: ContainerPermission::default(),
            side_effects: SideEffects::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommandPermission {
    pub bin: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub purpose: String,
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
    Automatic,
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

fn default_http_redirect_limit() -> usize {
    5
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
    pub images: Vec<String>,
    #[serde(default)]
    pub on_missing: Option<String>,
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
    pub env: String,
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
        on_exhausted: Option<RepairExhausted>,
    },
    Regenerate {
        max_attempts: u32,
    },
    AskUser,
    Route {
        to: String,
    },
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum RepairExhausted {
    Fail,
    Route { to: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolDecl {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub input_schema: Option<Value>,
    #[serde(default)]
    pub resource: Option<String>,
    #[serde(default)]
    pub methods: Vec<String>,
    #[serde(default)]
    pub hosts: Vec<String>,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub command: Vec<String>,
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

    #[test]
    fn resolve_inputs_applies_defaults_and_patterns() {
        let manifest = manifest_with_field(InputField {
            id: "name".into(),
            kind: FieldType::String,
            required: true,
            default: Some(Value::String("alpha".into())),
            pattern: Some("^[a-z]+$".into()),
            options: vec![],
            min_items: None,
            item_type: None,
        });
        let resolved = manifest.resolve_inputs(BTreeMap::new()).unwrap();
        assert_eq!(resolved.get("name"), Some(&Value::String("alpha".into())));
    }

    #[test]
    fn resolve_inputs_validates_file_values_as_canonical_objects() {
        let manifest = manifest_with_field(InputField {
            id: "attachment".into(),
            kind: FieldType::File,
            required: true,
            default: None,
            pattern: Some(r"^[a-z]+\.txt$".into()),
            options: vec![],
            min_items: None,
            item_type: None,
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
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            min_items: None,
            item_type: None,
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
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            min_items: None,
            item_type: None,
        });
        manifest.assets.files = vec!["bundle.wasm".into(), "NOTICE".into()];
        manifest
            .validate()
            .expect("declared assets with arbitrary names should validate");
    }

    #[test]
    fn output_artifact_mime_must_be_a_valid_media_type() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            min_items: None,
            item_type: None,
        });
        manifest.outputs.extras.push(OutputExtraDef {
            glob: "reports/*.json".into(),
            label: "Reports".into(),
            required: false,
            mime: Some("not a media type".into()),
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
            kind: FieldType::String,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            min_items: None,
            item_type: None,
        });
        assert!(manifest.resolve_inputs(BTreeMap::new()).is_err());
    }

    #[test]
    fn resolve_inputs_rejects_short_lists() {
        let manifest = manifest_with_field(InputField {
            id: "items".into(),
            kind: FieldType::List,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            min_items: Some(2),
            item_type: Some(FieldType::String),
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
    fn resource_kind_accepts_open_resource_ids() {
        let kind = ResourceKind::parse("openapi").expect("resource kind should be valid");
        assert_eq!(kind.as_str(), "openapi");
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
    fn contract_validator_exposes_builtin_rule_names() {
        let validator = ContractValidator::default();
        assert_eq!(
            validator.rule_names(),
            vec![
                "generator_metadata",
                "flow_nodes",
                "tools",
                "output_artifacts",
                "assets"
            ]
        );
    }

    struct RequireDescriptionRule;

    impl ContractValidationRule for RequireDescriptionRule {
        fn name(&self) -> &'static str {
            "require_description"
        }

        fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
            if manifest.generator.description.trim().is_empty() {
                return Err(ContractError::Invalid(
                    "generator.description is required by custom rule".into(),
                ));
            }
            Ok(())
        }
    }

    #[test]
    fn contract_validator_accepts_registered_extension_rules() {
        let manifest = manifest_with_field(InputField {
            id: "name".into(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            min_items: None,
            item_type: None,
        });
        let mut validator = ContractValidator::default();
        validator.register(RequireDescriptionRule);
        let error = manifest.validate_with(&validator).unwrap_err();
        assert!(error.to_string().contains("generator.description"));
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
    fn resource_kind_rejects_uppercase_and_separators() {
        assert!(ResourceKind::parse("OpenApi").is_err());
        assert!(ResourceKind::parse("open-api").is_err());
        assert!(ResourceKind::parse("").is_err());
    }
}
