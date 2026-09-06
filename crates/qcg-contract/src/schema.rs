//! Manifest schema leaf types: generator metadata, inputs, and assets.
//! Validation lives in [`crate::manifest`]; wire interaction shapes live in
//! `qcg_api`.

use crate::expr::Expr;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use serde_json::Value;
use std::collections::BTreeMap;

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
