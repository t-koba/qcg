use qcg_types::{Finding, Severity};
use serde_json::Value;

use crate::StepError;

pub const MAX_SCHEMA_FINDINGS_COUNT: usize = 64;
pub const MAX_SCHEMA_FINDING_MESSAGE_BYTES: usize = 1024;
pub const MAX_SCHEMA_FINDINGS_BYTES: usize = 32 * 1024;
pub const MAX_SCHEMA_REPORT_BYTES: usize = 32 * 1024;

pub fn validate_json_schema_findings(
    schema: &Value,
    value: &Value,
    location: &str,
) -> Vec<Finding> {
    let validator = match jsonschema::validator_for(schema) {
        Ok(validator) => validator,
        Err(error) => {
            return vec![error_finding(
                format!("invalid JSON Schema: {error}"),
                location,
            )];
        }
    };
    let mut findings = Vec::new();
    let mut omitted = 0;
    for error in validator.iter_errors(value) {
        let finding = error_finding(
            format!("{}: {error}", error.schema_path()),
            &join_instance_location(location, &error.instance_path().to_string()),
        );
        if findings.len() >= MAX_SCHEMA_FINDINGS_COUNT.saturating_sub(1)
            || !findings_fit(&findings, &finding)
        {
            omitted = 1;
            break;
        }
        findings.push(finding);
    }
    if omitted > 0 {
        append_truncation_finding(&mut findings, location, omitted);
    }
    findings
}

pub fn validate_json_schema_step(
    node: &str,
    schema: &Value,
    value: &Value,
    label: &str,
) -> Result<(), StepError> {
    let findings = validate_json_schema_findings(schema, value, label);
    if findings.is_empty() {
        return Ok(());
    }
    // Surface bounded violations (with their locations) so an LLM retry loop
    // can correct the response without allowing untrusted schema errors to
    // expand the retry transcript indefinitely.
    let prefix = format!("{} violations: ", findings.len());
    let report = findings_report(
        &findings,
        MAX_SCHEMA_REPORT_BYTES.saturating_sub(prefix.len()),
    );
    Err(StepError::failed(node, format!("{prefix}{report}")))
}

fn error_finding(message: String, location: &str) -> Finding {
    Finding {
        severity: Severity::Error,
        message: truncate_utf8(&message, MAX_SCHEMA_FINDING_MESSAGE_BYTES),
        location: Some(truncate_utf8(location, MAX_SCHEMA_FINDING_MESSAGE_BYTES)),
        raw_output: None,
    }
}

fn findings_fit(findings: &[Finding], candidate: &Finding) -> bool {
    let mut combined = Vec::with_capacity(findings.len() + 1);
    combined.extend(findings.iter().cloned());
    combined.push(candidate.clone());
    serialized_findings_bytes(&combined) <= MAX_SCHEMA_FINDINGS_BYTES
}

fn serialized_findings_bytes(findings: &[Finding]) -> usize {
    serde_json::to_vec(findings)
        .expect("Finding values contain only JSON-serializable fields")
        .len()
}

fn append_truncation_finding(findings: &mut Vec<Finding>, location: &str, mut omitted: usize) {
    omitted = omitted.max(1);
    loop {
        let finding = error_finding(
            format!(
                "JSON Schema validation findings truncated: at least {omitted} additional violation(s) omitted; limits are {MAX_SCHEMA_FINDINGS_COUNT} findings, {MAX_SCHEMA_FINDINGS_BYTES} bytes total, and {MAX_SCHEMA_FINDING_MESSAGE_BYTES} bytes per message"
            ),
            location,
        );
        if findings_fit(findings, &finding) {
            findings.push(finding);
            return;
        }
        if findings.pop().is_some() {
            omitted = omitted.saturating_add(1);
        } else {
            // The truncation finding is itself bounded and must remain visible
            // even if the configured aggregate limit is later reduced.
            findings.push(finding);
            return;
        }
    }
}

fn findings_report(findings: &[Finding], limit: usize) -> String {
    let report = findings
        .iter()
        .map(|finding| match &finding.location {
            Some(location) => format!("{location}: {}", finding.message),
            None => finding.message.clone(),
        })
        .collect::<Vec<_>>()
        .join("; ");
    truncate_utf8_with_marker(&report, limit, " ... [validation report truncated]")
}

fn truncate_utf8(value: &str, limit: usize) -> String {
    truncate_utf8_with_marker(value, limit, "...")
}

fn truncate_utf8_with_marker(value: &str, limit: usize, marker: &str) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }
    if limit <= marker.len() {
        return marker[..limit].to_owned();
    }
    let mut end = limit - marker.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut bounded = String::with_capacity(limit);
    bounded.push_str(&value[..end]);
    bounded.push_str(marker);
    bounded
}

fn join_instance_location(base: &str, pointer: &str) -> String {
    if pointer.is_empty() {
        return base.to_string();
    }
    let mut location = base.to_string();
    for component in pointer.split('/').skip(1) {
        let component = component.replace("~1", "/").replace("~0", "~");
        if component.bytes().all(|byte| byte.is_ascii_digit()) {
            location.push('[');
            location.push_str(&component);
            location.push(']');
        } else {
            location.push('.');
            location.push_str(&component);
        }
    }
    location
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
        assert!(findings[0].message.contains("required"));
        assert!(findings[0].message.contains("name"));
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
        assert!(text.iter().any(|m| m.contains("enum")), "{text:?}");
        assert!(text.iter().any(|m| m.contains("minItems")), "{text:?}");

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
        assert!(findings.iter().any(|f| f.message.contains("label")));
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
                .any(|message| message.contains("/propertyNames/not")),
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|message| message.contains("/propertyNames/pattern")),
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|message| message.contains("/additionalProperties/type")),
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|message| message.contains("/maxProperties")),
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

    #[test]
    fn full_validator_reports_all_scalar_and_array_keyword_violations() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "minLength": 3 },
                "items": { "type": "array", "maxItems": 1 },
                "score": { "type": "number", "minimum": 10 }
            }
        });
        let value = json!({
            "name": "x",
            "items": [1, 2],
            "score": 2
        });
        let findings = validate_json_schema_findings(&schema, &value, "$");
        assert_eq!(findings.len(), 3, "{findings:?}");
        let messages = findings
            .iter()
            .map(|finding| finding.message.as_str())
            .collect::<Vec<_>>();
        assert!(messages.iter().any(|message| message.contains("minLength")));
        assert!(messages.iter().any(|message| message.contains("maxItems")));
        assert!(messages.iter().any(|message| message.contains("minimum")));
        assert!(
            findings
                .iter()
                .any(|finding| finding.location.as_deref() == Some("$.name"))
        );
        assert!(
            findings
                .iter()
                .any(|finding| finding.location.as_deref() == Some("$.items"))
        );
        assert!(
            findings
                .iter()
                .any(|finding| finding.location.as_deref() == Some("$.score"))
        );
    }

    #[test]
    fn full_validator_resolves_internal_refs_and_combinators() {
        let schema = json!({
            "$defs": {
                "positive": { "type": "integer", "minimum": 1 }
            },
            "type": "object",
            "properties": {
                "count": { "$ref": "#/$defs/positive" },
                "choice": {
                    "anyOf": [
                        { "type": "string", "minLength": 2 },
                        { "type": "integer", "minimum": 10 }
                    ]
                }
            }
        });
        let value = json!({ "count": 0, "choice": false });
        let findings = validate_json_schema_findings(&schema, &value, "response");
        assert!(
            findings.iter().any(
                |finding| finding.location.as_deref() == Some("response.count")
                    && finding.message.contains("minimum")
            ),
            "{findings:?}"
        );
        assert!(
            findings.iter().any(|finding| {
                finding.location.as_deref() == Some("response.choice")
                    && finding.message.contains("anyOf")
            }),
            "{findings:?}"
        );
    }

    #[test]
    fn findings_are_bounded_and_report_truncation() {
        let required = (0..(MAX_SCHEMA_FINDINGS_COUNT * 2))
            .map(|index| format!("required_{index}"))
            .collect::<Vec<_>>();
        let schema = json!({
            "type": "object",
            "required": required,
            "additionalProperties": false
        });
        let findings = validate_json_schema_findings(&schema, &json!({}), "response");

        assert!(findings.len() <= MAX_SCHEMA_FINDINGS_COUNT);
        assert!(findings.iter().any(|finding| {
            finding
                .message
                .contains("JSON Schema validation findings truncated")
        }));
        assert!(
            findings
                .iter()
                .all(|finding| finding.message.len() <= MAX_SCHEMA_FINDING_MESSAGE_BYTES)
        );
        assert!(serialized_findings_bytes(&findings) <= MAX_SCHEMA_FINDINGS_BYTES);

        let error = validate_json_schema_step("node", &schema, &json!({}), "response")
            .expect_err("missing required properties should fail validation");
        assert!(error.to_string().len() <= MAX_SCHEMA_REPORT_BYTES + 64);
        assert!(error.to_string().contains("truncated"));
    }

    #[test]
    fn individual_finding_messages_are_bounded() {
        let property = "property_".to_string() + &"x".repeat(MAX_SCHEMA_FINDING_MESSAGE_BYTES * 4);
        let schema = json!({
            "type": "object",
            "required": [property]
        });
        let findings = validate_json_schema_findings(&schema, &json!({}), "response");

        assert_eq!(findings.len(), 1);
        assert!(findings[0].message.len() <= MAX_SCHEMA_FINDING_MESSAGE_BYTES);
        assert!(serialized_findings_bytes(&findings) <= MAX_SCHEMA_FINDINGS_BYTES);
    }
}
