use qcg_types::{Finding, Severity};
use serde_json::Value;

use crate::StepError;

pub fn validate_json_schema_findings(
    schema: &Value,
    value: &Value,
    location: &str,
) -> Vec<Finding> {
    let mut findings = Vec::new();
    collect_schema_findings(schema, value, location, &mut findings);
    findings
}

pub fn validate_json_schema_step(
    node: &str,
    schema: &Value,
    value: &Value,
    label: &str,
) -> Result<(), StepError> {
    if schema.get("type").and_then(Value::as_str) == Some("object") && !value.is_object() {
        return Err(StepError::failed(
            node,
            format!("{label} must have JSON Schema type `object`"),
        ));
    }
    let findings = validate_json_schema_findings(schema, value, label);
    if findings.is_empty() {
        return Ok(());
    }
    // Surface every violation (with its location) so an LLM retry loop can
    // correct the response in one round instead of guessing.
    let report = findings
        .iter()
        .map(|finding| match &finding.location {
            Some(location) => format!("{location}: {}", finding.message),
            None => finding.message.clone(),
        })
        .collect::<Vec<_>>()
        .join("; ");
    Err(StepError::failed(
        node,
        format!("{} violations: {report}", findings.len()),
    ))
}

fn collect_schema_findings(
    schema: &Value,
    value: &Value,
    location: &str,
    findings: &mut Vec<Finding>,
) {
    if let Some(negated_schema) = schema.get("not") {
        let mut matches = Vec::new();
        collect_schema_findings(negated_schema, value, location, &mut matches);
        if matches.is_empty() {
            findings.push(Finding {
                severity: Severity::Error,
                message: "value matches a forbidden schema".into(),
                location: Some(location.to_string()),
                raw_output: None,
            });
        }
    }
    if let Some(expected_type) = schema.get("type").and_then(Value::as_str)
        && !json_type_matches(expected_type, value)
    {
        findings.push(Finding {
            severity: Severity::Error,
            message: format!("expected {expected_type}, got {}", json_type_name(value)),
            location: Some(location.to_string()),
            raw_output: None,
        });
        return;
    }
    if let Some(required) = schema.get("required").and_then(Value::as_array)
        && let Some(object) = value.as_object()
    {
        for field in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(field) {
                findings.push(Finding {
                    severity: Severity::Error,
                    message: format!("required property `{field}` is missing"),
                    location: Some(location.to_string()),
                    raw_output: None,
                });
            }
        }
    }
    if let (Some(pattern), Some(actual)) = (
        schema.get("pattern").and_then(Value::as_str),
        value.as_str(),
    ) && !regex::Regex::new(pattern)
        .map(|re| re.is_match(actual))
        .unwrap_or(false)
    {
        findings.push(Finding {
            severity: Severity::Error,
            message: format!("value does not match pattern `{pattern}`"),
            location: Some(location.to_string()),
            raw_output: None,
        });
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array) {
        let matches_enum = allowed.iter().any(|candidate| candidate == value);
        if !matches_enum {
            findings.push(Finding {
                severity: Severity::Error,
                message: format!(
                    "value {} is not one of the allowed values",
                    json_type_name(value)
                ),
                location: Some(location.to_string()),
                raw_output: None,
            });
        }
    }
    if let Some(min_items) = schema.get("minItems").and_then(Value::as_u64)
        && let Some(array) = value.as_array()
        && (array.len() as u64) < min_items
    {
        findings.push(Finding {
            severity: Severity::Error,
            message: format!("expected at least {min_items} items, got {}", array.len()),
            location: Some(location.to_string()),
            raw_output: None,
        });
    }
    if let Some(max_properties) = schema.get("maxProperties").and_then(Value::as_u64)
        && let Some(object) = value.as_object()
        && (object.len() as u64) > max_properties
    {
        findings.push(Finding {
            severity: Severity::Error,
            message: format!(
                "expected at most {max_properties} properties, got {}",
                object.len()
            ),
            location: Some(location.to_string()),
            raw_output: None,
        });
    }
    if schema.get("additionalProperties") == Some(&Value::Bool(false))
        && let (Some(properties), Some(object)) = (
            schema.get("properties").and_then(Value::as_object),
            value.as_object(),
        )
    {
        for key in object.keys() {
            if !properties.contains_key(key) {
                findings.push(Finding {
                    severity: Severity::Error,
                    message: format!("unknown property `{key}`"),
                    location: Some(location.to_string()),
                    raw_output: None,
                });
            }
        }
    }
    if let (Some(properties), Some(object)) = (
        schema.get("properties").and_then(Value::as_object),
        value.as_object(),
    ) {
        for (name, property_schema) in properties {
            if let Some(property_value) = object.get(name) {
                collect_schema_findings(
                    property_schema,
                    property_value,
                    &format!("{location}.{name}"),
                    findings,
                );
            }
        }
    }
    if let (Some(property_name_schema), Some(object)) =
        (schema.get("propertyNames"), value.as_object())
    {
        for name in object.keys() {
            collect_schema_findings(
                property_name_schema,
                &Value::String(name.clone()),
                &format!("{location}.{name}"),
                findings,
            );
        }
    }
    if let (Some(additional_schema), Some(object)) = (
        schema
            .get("additionalProperties")
            .filter(|schema| schema.is_object()),
        value.as_object(),
    ) {
        let properties = schema
            .get("properties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(name, _)| name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for (name, property_value) in object {
            if !properties.contains(name.as_str()) {
                collect_schema_findings(
                    additional_schema,
                    property_value,
                    &format!("{location}.{name}"),
                    findings,
                );
            }
        }
    }
    if let (Some(item_schema), Some(array)) = (schema.get("items"), value.as_array()) {
        for (index, item) in array.iter().enumerate() {
            collect_schema_findings(item_schema, item, &format!("{location}[{index}]"), findings);
        }
    }
}

fn json_type_matches(expected: &str, value: &Value) -> bool {
    match expected {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => true,
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn schema_validator_reports_missing_required_property() {
        let schema = json!({
            "type": "object",
            "required": ["name"],
            "properties": { "name": { "type": "string" } }
        });
        let value = json!({});
        let findings = validate_json_schema_findings(&schema, &value, "$");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].message, "required property `name` is missing");
    }

    #[test]
    fn schema_validator_recurses_into_properties() {
        let schema = json!({
            "type": "object",
            "properties": {
                "count": { "type": "integer" }
            }
        });
        let value = json!({ "count": "three" });
        let findings = validate_json_schema_findings(&schema, &value, "$");
        assert_eq!(findings[0].location.as_deref(), Some("$.count"));
    }
}

#[cfg(test)]
mod validation_rules_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pattern_enum_and_min_items_are_enforced() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": { "type": "string", "pattern": "^[a-z]+$" },
                "kind": { "enum": ["a", "b"] },
                "items": { "type": "array", "minItems": 2 }
            }
        });
        let bad = json!({ "id": "", "kind": "zzz", "items": ["one"] });
        let findings = validate_json_schema_findings(&schema, &bad, "v");
        let text: Vec<&str> = findings.iter().map(|f| f.message.as_str()).collect();
        assert!(text.iter().any(|m| m.contains("pattern")), "{text:?}");
        assert!(
            text.iter().any(|m| m.contains("allowed values")),
            "{text:?}"
        );
        assert!(
            text.iter().any(|m| m.contains("at least 2 items")),
            "{text:?}"
        );

        let good = json!({ "id": "abc", "kind": "a", "items": ["one", "two"] });
        assert!(validate_json_schema_findings(&schema, &good, "v").is_empty());
    }

    #[test]
    fn additional_properties_false_rejects_unknown_keys() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "id": { "type": "string" } }
        });
        let bad = json!({ "id": "x", "label": "extra" });
        let findings = validate_json_schema_findings(&schema, &bad, "v");
        assert!(
            findings
                .iter()
                .any(|f| f.message.contains("unknown property `label`"))
        );
        let good = json!({ "id": "x" });
        assert!(validate_json_schema_findings(&schema, &good, "v").is_empty());
    }

    #[test]
    fn object_keys_and_values_are_validated() {
        let schema = json!({
            "type": "object",
            "maxProperties": 1,
            "propertyNames": {
                "type": "string",
                "pattern": "^[a-z]+$",
                "not": { "pattern": "^reserved$" }
            },
            "additionalProperties": { "type": "string" }
        });
        let bad = json!({ "reserved": 1, "UPPER": "value", "extra": "value" });
        let findings = validate_json_schema_findings(&schema, &bad, "v");
        let text: Vec<&str> = findings
            .iter()
            .map(|finding| finding.message.as_str())
            .collect();
        assert!(
            text.iter()
                .any(|message| message.contains("forbidden schema")),
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|message| message.contains("does not match pattern")),
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|message| message.contains("expected string")),
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|message| message.contains("at most 1 properties")),
            "{text:?}"
        );

        let good = json!({ "source": "content" });
        assert!(validate_json_schema_findings(&schema, &good, "v").is_empty());
    }

    #[test]
    fn not_schema_accepts_values_that_do_not_match() {
        let schema = json!({ "type": "string", "not": { "pattern": "^qcg\\.toml$" } });
        assert!(validate_json_schema_findings(&schema, &json!("README.md"), "v").is_empty());
        assert!(!validate_json_schema_findings(&schema, &json!("qcg.toml"), "v").is_empty());
    }

    #[test]
    fn object_map_schema_is_closed_over_paths_and_content() {
        let schema = json!({
            "type": "object",
            "required": ["sources"],
            "properties": {
                "sources": {
                    "type": "object",
                    "maxProperties": 64,
                    "propertyNames": {
                        "type": "string",
                        "pattern": "^(?:[A-Za-z0-9_-]+|\\.[A-Za-z0-9_-]+|[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)+)(?:/(?:[A-Za-z0-9_-]+|\\.[A-Za-z0-9_-]+|[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)+))*$",
                        "not": { "pattern": "^qcg\\.toml$" }
                    },
                    "additionalProperties": { "type": "string" }
                }
            },
            "additionalProperties": false
        });
        let valid = json!({
            "sources": {
                "README.md": "readme",
                "templates/artifact.txt.j2": "artifact"
            }
        });
        assert!(validate_json_schema_findings(&schema, &valid, "$").is_empty());

        for path in [
            "../escape.txt",
            "/absolute.txt",
            "templates/../escape.txt",
            "qcg.toml",
        ] {
            let invalid = json!({
                "sources": { path: "content" }
            });
            assert!(
                !validate_json_schema_findings(&schema, &invalid, "$").is_empty(),
                "source path {path} must be rejected"
            );
        }

        let invalid_content = json!({
            "sources": { "README.md": 7 }
        });
        assert!(!validate_json_schema_findings(&schema, &invalid_content, "$").is_empty());
    }
}
