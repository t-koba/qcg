use crate::InputField;
use crate::expr::Expr;
use qcg_types::ArtifactPreview;
use schemars::JsonSchema;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned, de::Error as DeError,
};
use serde_json::Value;

use super::outputs::FailurePolicy;
use super::resources::RetryPolicy;

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
    /// Execution retry policy. Omitted means a single attempt with no timeout.
    /// Only execution failures are retried; contract, budget, and
    /// cancellation errors fail fast.
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
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

pub(crate) fn default_true() -> bool {
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
