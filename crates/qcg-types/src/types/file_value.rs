use qcg_policy::is_safe_relative_path;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};
use serde_json::Value;
use std::borrow::Cow;
use std::fmt;

use super::encoding::{
    decode_base64, encode_base64, validate_base64_input_size_optional,
    validate_decoded_size_optional_limit,
};

/// Maximum decoded size of an inline file input.
///
/// This is only a fallback for callers that have no explicitly configured
/// limit. Bounds set in `qcg.toml` `[runtime]`, CLI flags, or server flags
/// always take precedence. `None` means no size check.
pub const MAX_FILE_INPUT_BYTES: usize = 16 * 1024 * 1024;

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

    pub fn from_text_optional_limit(
        name: impl Into<String>,
        text: impl Into<String>,
        max_bytes: Option<usize>,
    ) -> Result<Self, FileValueError> {
        Self {
            name: name.into(),
            text: Some(text.into()),
            content_base64: None,
        }
        .normalized_optional_limit(max_bytes)
    }

    pub fn from_text_with_limit(
        name: impl Into<String>,
        text: impl Into<String>,
        max_bytes: usize,
    ) -> Result<Self, FileValueError> {
        Self::from_text_optional_limit(name, text, Some(max_bytes))
    }

    pub fn from_bytes(name: impl Into<String>, bytes: &[u8]) -> Result<Self, FileValueError> {
        Self::from_bytes_with_limit(name, bytes, MAX_FILE_INPUT_BYTES)
    }

    pub fn from_bytes_optional_limit(
        name: impl Into<String>,
        bytes: &[u8],
        max_bytes: Option<usize>,
    ) -> Result<Self, FileValueError> {
        validate_decoded_size_optional_limit(bytes.len(), max_bytes)?;
        Self {
            name: name.into(),
            text: None,
            content_base64: Some(encode_base64(bytes)),
        }
        .normalized_optional_limit(max_bytes)
    }

    pub fn from_bytes_with_limit(
        name: impl Into<String>,
        bytes: &[u8],
        max_bytes: usize,
    ) -> Result<Self, FileValueError> {
        Self::from_bytes_optional_limit(name, bytes, Some(max_bytes))
    }

    pub fn from_value(value: &Value) -> Result<Self, FileValueError> {
        Self::from_value_with_limit(value, MAX_FILE_INPUT_BYTES)
    }

    pub fn from_value_optional_limit(
        value: &Value,
        max_bytes: Option<usize>,
    ) -> Result<Self, FileValueError> {
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
        .normalized_optional_limit(max_bytes)
    }

    pub fn from_value_with_limit(value: &Value, max_bytes: usize) -> Result<Self, FileValueError> {
        Self::from_value_optional_limit(value, Some(max_bytes))
    }

    pub fn validate(&self) -> Result<(), FileValueError> {
        self.validate_with_limit(MAX_FILE_INPUT_BYTES)
    }

    pub fn validate_optional_limit(&self, max_bytes: Option<usize>) -> Result<(), FileValueError> {
        if !is_safe_relative_path(&self.name) || self.name.contains('/') {
            return Err(FileValueError::UnsafeName(self.name.clone()));
        }
        match (&self.text, &self.content_base64) {
            (Some(text), None) => validate_decoded_size_optional_limit(text.len(), max_bytes),
            (None, Some(content_base64)) => {
                validate_base64_input_size_optional(content_base64, max_bytes)?;
                let bytes = decode_base64(content_base64)?;
                validate_decoded_size_optional_limit(bytes.len(), max_bytes)
            }
            (None, None) => Err(FileValueError::MissingContent),
            (Some(_), Some(_)) => Err(FileValueError::MultipleContent),
        }
    }

    pub fn validate_with_limit(&self, max_bytes: usize) -> Result<(), FileValueError> {
        self.validate_optional_limit(Some(max_bytes))
    }

    pub fn decode(&self) -> Result<Vec<u8>, FileValueError> {
        self.decode_with_limit(MAX_FILE_INPUT_BYTES)
    }

    pub fn decode_optional_limit(
        &self,
        max_bytes: Option<usize>,
    ) -> Result<Vec<u8>, FileValueError> {
        self.validate_optional_limit(max_bytes)?;
        match (&self.text, &self.content_base64) {
            (Some(text), None) => Ok(text.as_bytes().to_vec()),
            (None, Some(content_base64)) => decode_base64(content_base64),
            _ => unreachable!("FileValue::validate checked content exclusivity"),
        }
    }

    pub fn decode_with_limit(&self, max_bytes: usize) -> Result<Vec<u8>, FileValueError> {
        self.decode_optional_limit(Some(max_bytes))
    }

    fn normalized_optional_limit(self, max_bytes: Option<usize>) -> Result<Self, FileValueError> {
        let bytes = match (&self.text, &self.content_base64) {
            (Some(text), None) => {
                validate_decoded_size_optional_limit(text.len(), max_bytes)?;
                None
            }
            (None, Some(content_base64)) => {
                validate_base64_input_size_optional(content_base64, max_bytes)?;
                let bytes = decode_base64(content_base64)?;
                validate_decoded_size_optional_limit(bytes.len(), max_bytes)?;
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
