use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

mod run_event;

pub use run_event::*;

/// Provider-neutral reasoning effort requested for a model invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

/// Provider-neutral policy for whether a model may call an exposed tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(ToolChoiceMode),
    Tool { tool: String },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoiceMode {
    None,
    #[default]
    Auto,
    Required,
}

impl ToolChoice {
    pub const fn none() -> Self {
        Self::Mode(ToolChoiceMode::None)
    }

    pub const fn auto() -> Self {
        Self::Mode(ToolChoiceMode::Auto)
    }

    pub const fn required() -> Self {
        Self::Mode(ToolChoiceMode::Required)
    }
}

impl Default for ToolChoice {
    fn default() -> Self {
        Self::auto()
    }
}

/// Provider-neutral control for the detail of a model response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResponseVerbosity {
    Low,
    Medium,
    High,
}

impl ReasoningEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Maximum decoded size of an inline file input.
pub const MAX_FILE_INPUT_BYTES: usize = 16 * 1024 * 1024;

/// Returns whether `path` is a safe, non-empty, relative slash-separated path.
///
/// The check is intentionally platform-independent because these paths are
/// persisted in contracts and journals and may be consumed on another host.
pub fn is_safe_relative_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    !path.is_empty()
        && !path.starts_with('/')
        && !(bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
        && !path.contains('\\')
        && !path.contains('\0')
        && !path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
}

/// Returns whether a configuration name conventionally denotes credential material.
///
/// Token boundaries avoid false positives such as `AUTHORITY` and `TOKENIZER`, while
/// also recognizing compact names such as `APIKEY` and numbered secret slots.
pub fn credential_like_name(name: &str) -> bool {
    name.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .any(|token| {
            [
                "APIKEY",
                "APITOKEN",
                "AUTH",
                "AUTHORIZATION",
                "BEARER",
                "COOKIE",
                "CREDENTIAL",
                "CREDENTIALS",
                "KEY",
                "PASSWORD",
                "PASSWD",
                "SECRET",
                "TOKEN",
            ]
            .iter()
            .any(|marker| {
                token == *marker
                    || token.strip_prefix(marker).is_some_and(|suffix| {
                        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
                    })
            })
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileValueError {
    InvalidShape(String),
    UnsafeName(String),
    MissingContent,
    MultipleContent,
    InvalidBase64(String),
    TooLarge {
        actual_bytes: usize,
        limit_bytes: usize,
    },
}

impl fmt::Display for FileValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape(message) => formatter.write_str(message),
            Self::UnsafeName(name) => {
                write!(
                    formatter,
                    "file name `{name}` is not a safe relative path component"
                )
            }
            Self::MissingContent | Self::MultipleContent => {
                formatter.write_str("file value must set exactly one of `text` or `content_base64`")
            }
            Self::InvalidBase64(message) => {
                write!(formatter, "invalid base64 file content: {message}")
            }
            Self::TooLarge {
                actual_bytes,
                limit_bytes,
            } => write!(
                formatter,
                "file content is too large: {actual_bytes} bytes exceeds {limit_bytes} bytes"
            ),
        }
    }
}

impl std::error::Error for FileValueError {}

/// Canonical inline file input exchanged by the CLI, HTTP API, and engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileValue {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_base64: Option<String>,
}

impl JsonSchema for FileValue {
    fn schema_name() -> Cow<'static, str> {
        "FileValue".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "title": "FileValue",
            "description": "Canonical inline file input exchanged by the CLI, HTTP API, and engine.",
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "name": {
                    "type": "string",
                    "pattern": r"^(?!\.\.?$)[^/\\\u0000]+$"
                },
                "text": { "type": "string" },
                "content_base64": { "type": "string" }
            },
            "required": ["name"],
            "oneOf": [
                {
                    "required": ["text"],
                    "not": { "required": ["content_base64"] }
                },
                {
                    "required": ["content_base64"],
                    "not": { "required": ["text"] }
                }
            ]
        })
    }
}

impl<'de> Deserialize<'de> for FileValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Self::from_value(&value).map_err(D::Error::custom)
    }
}

impl FileValue {
    pub fn from_text(
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Self, FileValueError> {
        Self::from_text_with_limit(name, text, MAX_FILE_INPUT_BYTES)
    }

    pub fn from_text_with_limit(
        name: impl Into<String>,
        text: impl Into<String>,
        max_bytes: usize,
    ) -> Result<Self, FileValueError> {
        Self {
            name: name.into(),
            text: Some(text.into()),
            content_base64: None,
        }
        .normalized_with_limit(max_bytes)
    }

    pub fn from_bytes(name: impl Into<String>, bytes: &[u8]) -> Result<Self, FileValueError> {
        Self::from_bytes_with_limit(name, bytes, MAX_FILE_INPUT_BYTES)
    }

    pub fn from_bytes_with_limit(
        name: impl Into<String>,
        bytes: &[u8],
        max_bytes: usize,
    ) -> Result<Self, FileValueError> {
        validate_decoded_size_with_limit(bytes.len(), max_bytes)?;
        Self {
            name: name.into(),
            text: None,
            content_base64: Some(encode_base64(bytes)),
        }
        .normalized_with_limit(max_bytes)
    }

    pub fn from_value(value: &Value) -> Result<Self, FileValueError> {
        Self::from_value_with_limit(value, MAX_FILE_INPUT_BYTES)
    }

    pub fn from_value_with_limit(value: &Value, max_bytes: usize) -> Result<Self, FileValueError> {
        let object = value
            .as_object()
            .ok_or_else(|| FileValueError::InvalidShape("file input must be an object".into()))?;
        for key in object.keys() {
            if !matches!(key.as_str(), "name" | "text" | "content_base64") {
                return Err(FileValueError::InvalidShape(format!(
                    "file input contains unknown field `{key}`"
                )));
            }
        }
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FileValueError::InvalidShape("file input `name` must be a string".into())
            })?
            .to_string();
        let text = object
            .get("text")
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    FileValueError::InvalidShape("file input `text` must be a string".into())
                })
            })
            .transpose()?;
        let content_base64 = object
            .get("content_base64")
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    FileValueError::InvalidShape(
                        "file input `content_base64` must be a string".into(),
                    )
                })
            })
            .transpose()?;
        Self {
            name,
            text,
            content_base64,
        }
        .normalized_with_limit(max_bytes)
    }

    pub fn validate(&self) -> Result<(), FileValueError> {
        self.validate_with_limit(MAX_FILE_INPUT_BYTES)
    }

    pub fn validate_with_limit(&self, max_bytes: usize) -> Result<(), FileValueError> {
        if !is_safe_relative_path(&self.name) || self.name.contains('/') {
            return Err(FileValueError::UnsafeName(self.name.clone()));
        }
        match (&self.text, &self.content_base64) {
            (Some(text), None) => validate_decoded_size_with_limit(text.len(), max_bytes),
            (None, Some(content_base64)) => {
                validate_base64_input_size(content_base64, max_bytes)?;
                let bytes = decode_base64(content_base64)?;
                validate_decoded_size_with_limit(bytes.len(), max_bytes)
            }
            (None, None) => Err(FileValueError::MissingContent),
            (Some(_), Some(_)) => Err(FileValueError::MultipleContent),
        }
    }

    pub fn decode(&self) -> Result<Vec<u8>, FileValueError> {
        self.decode_with_limit(MAX_FILE_INPUT_BYTES)
    }

    pub fn decode_with_limit(&self, max_bytes: usize) -> Result<Vec<u8>, FileValueError> {
        self.validate_with_limit(max_bytes)?;
        match (&self.text, &self.content_base64) {
            (Some(text), None) => Ok(text.as_bytes().to_vec()),
            (None, Some(content_base64)) => decode_base64(content_base64),
            _ => unreachable!("FileValue::validate checked content exclusivity"),
        }
    }

    fn normalized_with_limit(self, max_bytes: usize) -> Result<Self, FileValueError> {
        let bytes = match (&self.text, &self.content_base64) {
            (Some(text), None) => {
                validate_decoded_size_with_limit(text.len(), max_bytes)?;
                None
            }
            (None, Some(content_base64)) => {
                validate_base64_input_size(content_base64, max_bytes)?;
                let bytes = decode_base64(content_base64)?;
                validate_decoded_size_with_limit(bytes.len(), max_bytes)?;
                Some(bytes)
            }
            (None, None) => return Err(FileValueError::MissingContent),
            (Some(_), Some(_)) => return Err(FileValueError::MultipleContent),
        };
        if !is_safe_relative_path(&self.name) || self.name.contains('/') {
            return Err(FileValueError::UnsafeName(self.name));
        }
        Ok(match bytes {
            Some(bytes) => Self {
                name: self.name,
                text: None,
                content_base64: Some(encode_base64(&bytes)),
            },
            None => self,
        })
    }
}

fn validate_decoded_size_with_limit(bytes: usize, max_bytes: usize) -> Result<(), FileValueError> {
    if bytes > max_bytes {
        return Err(FileValueError::TooLarge {
            actual_bytes: bytes,
            limit_bytes: max_bytes,
        });
    }
    Ok(())
}

fn validate_base64_input_size(input: &str, max_bytes: usize) -> Result<(), FileValueError> {
    let max_encoded = max_bytes.div_ceil(3).saturating_mul(4);
    if input.len() > max_encoded {
        return Err(FileValueError::TooLarge {
            actual_bytes: input.len().saturating_mul(3) / 4,
            limit_bytes: max_bytes,
        });
    }
    Ok(())
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        output.push(TABLE[(first >> 2) as usize] as char);
        let second = chunk.get(1).copied();
        output.push(
            TABLE[(((first & 0x03) << 4) | second.map_or(0, |value| value >> 4)) as usize] as char,
        );
        match second {
            Some(second) => {
                let third = chunk.get(2).copied();
                output.push(
                    TABLE[(((second & 0x0f) << 2) | third.map_or(0, |value| value >> 6)) as usize]
                        as char,
                );
                output.push(third.map_or('=', |value| TABLE[(value & 0x3f) as usize] as char));
            }
            None => output.push('='),
        }
    }
    output
}

fn decode_base64(input: &str) -> Result<Vec<u8>, FileValueError> {
    if input.len() % 4 == 1 {
        return Err(FileValueError::InvalidBase64(
            "length must not leave a single trailing sextet".into(),
        ));
    }
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(input.len().div_ceil(4) * 3);
    let mut index = 0;
    while index < bytes.len() {
        let remaining = bytes.len() - index;
        let chunk_len = remaining.min(4);
        if chunk_len < 4 {
            let first = base64_value(bytes[index])?;
            let second = base64_value(bytes[index + 1])?;
            output.push((first << 2) | (second >> 4));
            if chunk_len == 3 {
                let third = base64_value(bytes[index + 2])?;
                output.push((second << 4) | (third >> 2));
            }
            break;
        }
        let first = base64_value(bytes[index])?;
        let second = base64_value(bytes[index + 1])?;
        output.push((first << 2) | (second >> 4));
        if bytes[index + 2] == b'=' {
            if bytes[index + 3] != b'=' || index + 4 != bytes.len() {
                return Err(FileValueError::InvalidBase64("invalid padding".into()));
            }
            break;
        }
        let third = base64_value(bytes[index + 2])?;
        output.push((second << 4) | (third >> 2));
        if bytes[index + 3] == b'=' {
            if index + 4 != bytes.len() {
                return Err(FileValueError::InvalidBase64("invalid padding".into()));
            }
            break;
        }
        let fourth = base64_value(bytes[index + 3])?;
        output.push((third << 6) | fourth);
        index += 4;
    }
    Ok(output)
}

fn base64_value(byte: u8) -> Result<u8, FileValueError> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(FileValueError::InvalidBase64(format!(
            "invalid character `{}`",
            char::from(byte)
        ))),
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct NodePath(String);

impl NodePath {
    pub fn root(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn foreach_child(&self, index: usize, child: &str) -> Self {
        Self(format!("{}[{index}]/{child}", self.0))
    }

    pub fn repair_child(&self, attempt: u32, child: &str) -> Self {
        Self(format!("{}@repair.{attempt}/{child}", self.0))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    WhenFalse,
    DependencyUnsatisfied,
    NoDependencySucceeded,
    CheckFailed,
    ExecutionFailed,
    RepairExhausted,
    SchedulerFailed,
    Canceled,
    BudgetExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DependencyStatus {
    Skipped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DependencyFailure {
    pub path: NodePath,
    pub status: DependencyStatus,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FailureDetail {
    pub code: FailureCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<DependencyFailure>,
}

impl FailureDetail {
    pub fn new(code: FailureCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            dependencies: Vec::new(),
        }
    }

    pub fn execution(message: impl Into<String>) -> Self {
        Self::new(FailureCode::ExecutionFailed, message)
    }
}

impl fmt::Display for FailureDetail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::ops::Deref for FailureDetail {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl From<String> for FailureDetail {
    fn from(message: String) -> Self {
        Self::execution(message)
    }
}

impl From<&str> for FailureDetail {
    fn from(message: &str) -> Self {
        Self::execution(message)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(transparent)]
pub struct Expr(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GeneratorMeta {
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub authors: Vec<String>,
    pub qcg_version: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    #[serde(default)]
    pub stages: Vec<InputStage>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AssetSpec {
    /// Safe relative paths served verbatim by the assets API.
    #[serde(default)]
    pub files: Vec<String>,
    /// Safe relative directory trees resolved when an asset is requested.
    #[serde(default)]
    pub dirs: Vec<String>,
    /// Free-form metadata forwarded to clients untouched; the backend assigns
    /// no semantics.
    #[serde(default)]
    pub meta: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputStage {
    pub id: String,
    #[serde(default)]
    pub when: Option<Expr>,
    #[serde(default)]
    pub fields: Vec<InputField>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputField {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub label_i18n: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub description_i18n: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub placeholder_i18n: BTreeMap<String, String>,
    #[serde(rename = "type")]
    #[schemars(with = "String")]
    pub kind: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub option_labels_i18n: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(default)]
    pub min_items: Option<usize>,
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub item_type: Option<FieldType>,
    /// Optional JSON Schema applied after the canonical field-type checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    /// Renderer-specific presentation metadata forwarded to clients unchanged.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub ui: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema)]
pub enum FieldType {
    String,
    Text,
    Number,
    Boolean,
    Select,
    Multiselect,
    List,
    File,
    Json,
    NaturalLanguage,
    Custom(String),
}

impl Serialize for FieldType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            FieldType::String => "string",
            FieldType::Text => "text",
            FieldType::Number => "number",
            FieldType::Boolean => "boolean",
            FieldType::Select => "select",
            FieldType::Multiselect => "multiselect",
            FieldType::List => "list",
            FieldType::File => "file",
            FieldType::Json => "json",
            FieldType::NaturalLanguage => "natural_language",
            FieldType::Custom(kind) => kind,
        })
    }
}

impl<'de> Deserialize<'de> for FieldType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "string" => FieldType::String,
            "text" => FieldType::Text,
            "number" => FieldType::Number,
            "boolean" => FieldType::Boolean,
            "select" => FieldType::Select,
            "multiselect" => FieldType::Multiselect,
            "list" => FieldType::List,
            "file" => FieldType::File,
            "json" => FieldType::Json,
            "natural_language" => FieldType::NaturalLanguage,
            "" => return Err(D::Error::custom("field type must not be empty")),
            _ => FieldType::Custom(value),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OutputManifest {
    pub artifacts: Vec<OutputArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OutputArtifact {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub label: String,
    pub required: bool,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub preview: ArtifactPreview,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPreview {
    #[default]
    Auto,
    Text,
    Json,
    Markdown,
    Image,
    Html,
    Pdf,
    Audio,
    Video,
    None,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuredOutputMode {
    #[default]
    Auto,
    NativeStrict,
    NativeCompatible,
    Prompt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub severity: Severity,
    pub message: String,
    pub location: Option<String>,
    pub raw_output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FormSpec {
    pub id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub title_i18n: BTreeMap<String, String>,
    pub fields: Vec<InputField>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConfirmSpec {
    pub id: String,
    pub title: String,
    pub kind: String,
    pub target: String,
    pub dry_run: bool,
    #[serde(default)]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct RunMetrics {
    #[serde(default)]
    pub steps_total: u64,
    #[serde(default)]
    pub steps_succeeded: u64,
    #[serde(default)]
    pub steps_failed: u64,
    #[serde(default)]
    pub steps_skipped: u64,
    #[serde(default)]
    pub steps_executed: u64,
    #[serde(default)]
    pub repair_attempts: u64,
    #[serde(default)]
    pub regenerate_attempts: u64,
    #[serde(default)]
    pub llm_calls: u64,
    #[serde(default)]
    pub tokens_input: u64,
    #[serde(default)]
    pub tokens_output: u64,
    #[serde(default)]
    pub tokens_total: u64,
    #[serde(default)]
    pub cost_microusd: u64,
    #[serde(default)]
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RunEvent {
    pub seq: u64,
    pub ts: String,
    pub run_id: String,
    pub trace_id: String,
    pub span_id: String,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub path: Option<NodePath>,
    pub kind: String,
    pub data: RunEventData,
}

impl<'de> Deserialize<'de> for RunEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawRunEvent {
            seq: u64,
            ts: String,
            run_id: String,
            trace_id: String,
            span_id: String,
            #[serde(default)]
            parent_span_id: Option<String>,
            #[serde(default)]
            path: Option<NodePath>,
            kind: String,
            data: Value,
        }

        let raw = RawRunEvent::deserialize(deserializer)?;
        let data = RunEventData::parse(&raw.kind, raw.data).map_err(D::Error::custom)?;
        Ok(Self {
            seq: raw.seq,
            ts: raw.ts,
            run_id: raw.run_id,
            trace_id: raw.trace_id,
            span_id: raw.span_id,
            parent_span_id: raw.parent_span_id,
            path: raw.path,
            kind: raw.kind,
            data,
        })
    }
}

impl RunEvent {
    pub fn from_flat(value: &Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "run event must be an object".to_string())?;
        let seq = object
            .get("seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| "run event seq is required".to_string())?;
        let ts = object
            .get("ts")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event ts is required".to_string())?
            .to_string();
        let kind = object
            .get("t")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event kind is required".to_string())?
            .to_string();
        let run_id = object
            .get("run_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event run_id is required".to_string())?
            .to_string();
        let trace_id = object
            .get("trace_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event trace_id is required".to_string())?
            .to_string();
        let span_id = object
            .get("span_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event span_id is required".to_string())?
            .to_string();
        let path = object
            .get("node")
            .and_then(Value::as_str)
            .map(NodePath::root);
        let mut data = object.clone();
        for common in [
            "seq",
            "ts",
            "t",
            "run_id",
            "trace_id",
            "span_id",
            "parent_span_id",
            "node",
        ] {
            data.remove(common);
        }
        let data = RunEventData::parse(&kind, Value::Object(data))?;
        Ok(Self {
            seq,
            ts,
            run_id,
            trace_id,
            span_id,
            parent_span_id: object
                .get("parent_span_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            path,
            kind,
            data,
        })
    }

    pub fn lagged(run_id: impl Into<String>, seq: u64) -> Self {
        let run_id = run_id.into();
        Self {
            seq,
            ts: chrono::Utc::now().to_rfc3339(),
            trace_id: trace_id_for_run(&run_id),
            span_id: span_id_for_seq(seq),
            parent_span_id: None,
            run_id,
            path: None,
            kind: "lagged".into(),
            data: RunEventData::Lagged(LaggedEventData {
                action: LaggedAction::ResyncSnapshot,
            }),
        }
    }
}

pub fn trace_id_for_run(run_id: &str) -> String {
    let compact = run_id
        .bytes()
        .filter(|byte| byte.is_ascii_hexdigit())
        .map(|byte| (byte as char).to_ascii_lowercase())
        .collect::<String>();
    if compact.len() == 32 && compact.bytes().any(|byte| byte != b'0') {
        return compact;
    }
    let mut state = [0xcbf29ce484222325_u64, 0x84222325cbf29ce4_u64];
    for (index, byte) in run_id.bytes().enumerate() {
        let slot = index & 1;
        state[slot] ^= u64::from(byte);
        state[slot] = state[slot].wrapping_mul(0x100000001b3);
    }
    format!("{:016x}{:016x}", state[0], state[1])
}

pub fn span_id_for_seq(seq: u64) -> String {
    format!("{:016x}", seq.max(1))
}

pub fn span_id_for_scope(run_id: &str, scope: &str) -> String {
    let mut state = 0xcbf29ce484222325_u64;
    for byte in run_id.bytes().chain(*b":").chain(scope.bytes()) {
        state ^= u64::from(byte);
        state = state.wrapping_mul(0x100000001b3);
    }
    if state == 0 {
        state = 1;
    }
    format!("{state:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn run_event_decodes_flat_step_finished_into_closed_data() {
        let value = json!({
            "t": "step_finished",
            "seq": 7,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-7",
            "node": "write_config",
            "status": "success",
            "files": [],
            "output": { "ok": true },
            "output_name": "write_config"
        });
        let event = RunEvent::from_flat(&value).expect("event should decode");

        assert_eq!(
            event.path.as_ref().map(NodePath::as_str),
            Some("write_config")
        );
        match event.data {
            RunEventData::StepFinished(data) => {
                assert_eq!(data.status, StepStatus::Success);
                assert_eq!(data.output, Some(json!({ "ok": true })));
            }
            other => panic!("expected typed step_finished, got {other:?}"),
        }
    }

    #[test]
    fn known_event_data_rejects_unknown_fields() {
        let value = json!({
            "t": "step_skipped",
            "seq": 1,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-1",
            "node": "skip",
            "reason": {
                "code": "when_false",
                "message": "condition was false"
            },
            "unexpected": true
        });
        let error = RunEvent::from_flat(&value).expect_err("known event data must be closed");
        assert!(error.contains("unknown field"));
    }

    #[test]
    fn command_resource_events_decode_as_a_typed_source() {
        let value = json!({
            "t": "resource",
            "seq": 2,
            "ts": "2026-09-01T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-2",
            "name": "generated",
            "type": "exec",
            "source": { "kind": "command", "command": ["printf", "hello"] },
            "sha256": "00",
            "bytes": 5,
            "cache": "not_applicable",
            "trust": "untrusted",
            "llm_visible": true
        });
        let event = RunEvent::from_flat(&value).expect("command resource event should decode");
        assert!(matches!(
            event.data,
            RunEventData::Resource(ResourceEventData {
                source: ResourceSource::Command { command },
                ..
            }) if command == ["printf", "hello"]
        ));
    }

    #[test]
    fn specialist_events_use_the_node_envelope_and_closed_typed_data() {
        let delegated = json!({
            "t": "agent_delegated",
            "seq": 1,
            "ts": "2026-08-31T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-1",
            "node": "research",
            "agent": "parallel_researcher",
            "tool_call_id": "call-delegate-1",
            "tools": ["parallel_search"],
            "max_calls": 2,
            "max_iterations": 4,
            "max_tokens_total": 10000,
            "max_tool_calls_total": 3
        });
        let event = RunEvent::from_flat(&delegated).expect("delegated event should decode");
        assert_eq!(event.path.as_ref().map(NodePath::as_str), Some("research"));
        assert!(matches!(event.data, RunEventData::AgentDelegated(_)));

        let llm_call = json!({
            "t": "llm_call",
            "seq": 2,
            "ts": "2026-08-31T00:00:01Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-2",
            "node": "research",
            "provider": "fake",
            "model": "fake",
            "max_tokens": 128,
            "agent": "parallel_researcher",
            "tokens": { "input": 10, "output": 2 },
            "cost_microusd": 0
        });
        let event = RunEvent::from_flat(&llm_call).expect("specialist LLM event should decode");
        match event.data {
            RunEventData::LlmCall(data) => {
                assert_eq!(data.agent.as_deref(), Some("parallel_researcher"));
            }
            other => panic!("expected typed llm_call, got {other:?}"),
        }
    }

    #[test]
    fn failed_tool_events_decode_typed_phase_status_and_error() {
        let value = json!({
            "t": "tool_call",
            "seq": 3,
            "ts": "2026-08-31T00:00:02Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-3",
            "node": "research",
            "tool": "parallel_search",
            "id": "call-1",
            "status": "failed",
            "phase": "execution",
            "agent": "parallel_researcher",
            "error": { "code": "execution_failed", "message": "transport failed" },
            "duration_ms": 7,
            "arguments": { "objective": "research" },
            "result": null,
            "sources": [],
            "truncated": false
        });
        let event = RunEvent::from_flat(&value).expect("typed tool failure should decode");
        match event.data {
            RunEventData::ToolCall(data) => {
                assert_eq!(data.status, ToolCallStatus::Failed);
                assert_eq!(data.phase, ToolCallPhase::Execution);
                assert_eq!(
                    data.error.expect("error details").code,
                    ToolCallErrorCode::ExecutionFailed
                );
            }
            other => panic!("expected typed tool_call, got {other:?}"),
        }
    }

    #[test]
    fn context_compaction_events_decode_prompt_and_transcript_shapes() {
        let prompt = json!({
            "t": "context_compacted",
            "seq": 4,
            "ts": "2026-08-31T00:00:03Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-4",
            "node": "generate",
            "policy": "truncate_tail",
            "original_bytes": 4096,
            "final_bytes": 1024,
            "limit_bytes": 1024
        });
        let event = RunEvent::from_flat(&prompt).expect("prompt compaction should decode");
        assert!(matches!(
            event.data,
            RunEventData::ContextCompacted(ContextCompactedEventData::Prompt(data))
                if data.policy == ContextCompactionPolicy::TruncateTail
                    && data.original_bytes == 4096
        ));

        let transcript = json!({
            "t": "context_compacted",
            "seq": 5,
            "ts": "2026-08-31T00:00:04Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-5",
            "node": "research",
            "scope": "agent_transcript",
            "policy": "truncate_head",
            "original_bytes": 8192,
            "final_bytes": 2048,
            "limit_bytes": 2048,
            "compacted_tool_results": 3,
            "compacted_messages": 1
        });
        let event = RunEvent::from_flat(&transcript).expect("transcript compaction should decode");
        assert!(matches!(
            event.data,
            RunEventData::ContextCompacted(ContextCompactedEventData::RequestOrTranscript(data))
                if data.scope == ContextCompactionScope::AgentTranscript
                    && data.compacted_tool_results == 3
                    && data.compacted_messages == 1
        ));

        let request = json!({
            "t": "context_compacted",
            "seq": 6,
            "ts": "2026-08-31T00:00:05Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-6",
            "node": "research",
            "scope": "request",
            "policy": "truncate_tail",
            "original_bytes": 16384,
            "final_bytes": 4096,
            "limit_bytes": 4096,
            "compacted_tool_results": 2,
            "compacted_messages": 0
        });
        let event = RunEvent::from_flat(&request).expect("request compaction should decode");
        assert!(matches!(
            event.data,
            RunEventData::ContextCompacted(ContextCompactedEventData::RequestOrTranscript(data))
                if data.scope == ContextCompactionScope::Request
                    && data.compacted_tool_results == 2
        ));

        let mut invalid = transcript;
        invalid["unexpected"] = json!(true);
        let error = RunEvent::from_flat(&invalid)
            .expect_err("closed context compaction payload should reject unknown fields");
        assert!(!error.is_empty(), "{error}");
    }

    #[test]
    fn agent_handoff_and_llm_route_failure_events_decode_closed_payloads() {
        let handoff = json!({
            "t": "agent_handoff",
            "seq": 6,
            "ts": "2026-08-31T00:00:05Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-6",
            "node": "research",
            "agent": "parallel_researcher",
            "tool_call_id": "call-handoff-1"
        });
        let event = RunEvent::from_flat(&handoff).expect("agent handoff should decode");
        assert!(matches!(
            event.data,
            RunEventData::AgentHandoff(AgentHandoffEventData { agent, tool_call_id })
                if agent == "parallel_researcher" && tool_call_id == "call-handoff-1"
        ));

        let route_failed = json!({
            "t": "llm_route_failed",
            "seq": 7,
            "ts": "2026-08-31T00:00:06Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-7",
            "node": "research",
            "provider": "openai",
            "model": "gpt-5",
            "attempt": 1,
            "kind": { "http_status": 503 }
        });
        let event = RunEvent::from_flat(&route_failed).expect("route failure should decode");
        assert!(matches!(
            event.data,
            RunEventData::LlmRouteFailed(LlmRouteFailedEventData {
                kind: LlmRouteFailureKind::HttpStatus(503),
                ..
            })
        ));

        let mut invalid = route_failed;
        invalid["unexpected"] = json!(true);
        let error = RunEvent::from_flat(&invalid)
            .expect_err("closed route failure payload should reject unknown fields");
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn unknown_event_kind_preserves_data() {
        let value = json!({
            "t": "third_party.progress",
            "seq": 3,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-3",
            "percent": 50
        });
        let event = RunEvent::from_flat(&value).expect("unknown event should decode");
        match event.data {
            RunEventData::Unknown(data) => assert_eq!(data, json!({ "percent": 50 })),
            other => panic!("expected unknown event data, got {other:?}"),
        }
    }

    #[test]
    fn flat_event_requires_complete_trace_identity() {
        let base = json!({
            "t": "third_party.progress",
            "seq": 1,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-1"
        });
        for field in ["run_id", "trace_id", "span_id"] {
            let mut value = base.clone();
            value.as_object_mut().expect("object").remove(field);
            let error = RunEvent::from_flat(&value).expect_err("identity field must be required");
            assert!(error.contains(field), "{error}");
        }
    }

    #[test]
    fn field_type_preserves_unknown_kinds_as_custom() {
        let kind: FieldType =
            serde_json::from_value(json!("color_picker")).expect("custom field type should decode");
        assert_eq!(kind, FieldType::Custom("color_picker".into()));
        assert_eq!(
            serde_json::to_value(kind).expect("custom field type should encode"),
            json!("color_picker")
        );
    }

    #[test]
    fn safe_relative_paths_reject_traversal_and_absolute_forms() {
        for path in ["index.html", "assets/logo.svg"] {
            assert!(is_safe_relative_path(path), "{path} should be safe");
        }
        for path in [
            "",
            "/index.html",
            "../index.html",
            "assets/../index.html",
            "assets//index.html",
            "assets/./index.html",
            "assets\\index.html",
            "C:/index.html",
            "C:index.html",
            ".",
            "..",
            "assets\0/index.html",
        ] {
            assert!(!is_safe_relative_path(path), "{path:?} should be rejected");
        }
    }

    #[test]
    fn credential_names_use_token_boundaries() {
        for name in ["APIKEY", "QCG_AUTH", "X-Credential", "ACCESS_KEY", "TOKEN2"] {
            assert!(credential_like_name(name), "{name} should be sensitive");
        }
        for name in ["AUTHORITY", "KEYBOARD", "PASSWORDLESS", "TOKENIZER"] {
            assert!(
                !credential_like_name(name),
                "{name} should not be sensitive"
            );
        }
    }

    #[test]
    fn file_value_normalizes_text_and_base64_content() {
        let text = FileValue::from_text("note.txt", "hello").expect("text file should decode");
        assert_eq!(text.decode().expect("text should decode"), b"hello");
        assert_eq!(
            serde_json::to_value(&text).expect("text file should encode"),
            json!({"name": "note.txt", "text": "hello"})
        );

        let bytes = FileValue::from_bytes("bytes.bin", &[0, 255]).expect("bytes should encode");
        assert_eq!(bytes.content_base64.as_deref(), Some("AP8="));
        assert_eq!(bytes.decode().expect("base64 should decode"), [0, 255]);

        let unpadded: FileValue = serde_json::from_value(json!({
            "name": "note.txt",
            "content_base64": "aGk"
        }))
        .expect("unpadded base64 should be accepted and normalized");
        assert_eq!(unpadded.content_base64.as_deref(), Some("aGk="));
    }

    #[test]
    fn file_value_requires_exclusive_content_and_safe_name() {
        for value in [
            json!({"name": "note.txt"}),
            json!({"name": "note.txt", "text": "a", "content_base64": "Yg=="}),
            json!({"name": "../note.txt", "text": "a"}),
            json!({"name": "note.txt", "text": "a", "unexpected": true}),
            json!({"name": "note.txt", "content_base64": "not base64"}),
        ] {
            assert!(
                serde_json::from_value::<FileValue>(value).is_err(),
                "invalid file value should be rejected"
            );
        }
    }

    #[test]
    fn file_value_enforces_decoded_size_limit() {
        let at_limit = FileValue {
            name: "limit.bin".into(),
            text: Some("a".repeat(MAX_FILE_INPUT_BYTES)),
            content_base64: None,
        };
        at_limit
            .validate()
            .expect("exactly the limit should be accepted");

        let over_limit = FileValue {
            name: "limit.bin".into(),
            text: Some("a".repeat(MAX_FILE_INPUT_BYTES + 1)),
            content_base64: None,
        };
        assert!(matches!(
            over_limit.validate(),
            Err(FileValueError::TooLarge {
                actual_bytes,
                limit_bytes,
            }) if actual_bytes == MAX_FILE_INPUT_BYTES + 1 && limit_bytes == MAX_FILE_INPUT_BYTES
        ));
    }
}
