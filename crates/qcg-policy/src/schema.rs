//! Bounded JSON Schema validation: self-protection for the validator itself
//! against hostile schemas. These bounds guard traversal cost, not user data.

use serde_json::Value;

pub const MAX_JSON_SCHEMA_BYTES: usize = 256 * 1024;
pub const MAX_JSON_SCHEMA_DEPTH: usize = 64;
pub const MAX_JSON_SCHEMA_NODES: usize = 8_192;
pub const MAX_JSON_SCHEMA_OBJECT_MEMBERS: usize = 1_024;
pub const MAX_JSON_SCHEMA_STRING_BYTES: usize = 16 * 1024;

/// Rejects oversized, over-nested, or externally-referenced schemas, then
/// proves the schema compiles. Iterative so untrusted input cannot overflow
/// the call stack.
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

/// Bounds-checks then compiles a schema, returning the reusable validator.
/// All crates must compile untrusted schemas through this choke point instead
/// of calling `jsonschema::validator_for` directly, so traversal bounds and
/// the external-reference ban apply uniformly.
pub fn compile_bounded_validator(schema: &Value) -> Result<jsonschema::Validator, String> {
    validate_bounded_json_schema(schema)?;
    jsonschema::validator_for(schema).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_a_small_closed_schema() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["name"],
            "properties": { "name": { "type": "string" } },
        });
        validate_bounded_json_schema(&schema).expect("small schema should pass");
    }

    #[test]
    fn rejects_an_oversized_schema() {
        let padding = "x".repeat(MAX_JSON_SCHEMA_BYTES + 1);
        let schema = json!({ "type": "string", "title": padding });
        let error = validate_bounded_json_schema(&schema).expect_err("oversized schema must fail");
        assert!(error.contains("exceeds"), "{error}");
    }

    #[test]
    fn rejects_over_nested_schemas() {
        let mut schema = json!({ "type": "string" });
        for _ in 0..=MAX_JSON_SCHEMA_DEPTH {
            schema = json!({ "type": "object", "properties": { "nested": schema } });
        }
        let error = validate_bounded_json_schema(&schema).expect_err("deep schema must fail");
        assert!(!error.is_empty());
    }

    #[test]
    fn rejects_too_many_nodes() {
        let properties: serde_json::Map<String, Value> = (0..MAX_JSON_SCHEMA_OBJECT_MEMBERS + 1)
            .map(|index| (format!("p{index}"), json!({ "type": "string" })))
            .collect();
        let schema = json!({ "type": "object", "properties": Value::Object(properties) });
        let error = validate_bounded_json_schema(&schema).expect_err("wide schema must fail");
        assert!(!error.is_empty());
    }

    #[test]
    fn rejects_oversized_strings() {
        let schema =
            json!({ "type": "string", "title": "y".repeat(MAX_JSON_SCHEMA_STRING_BYTES + 1) });
        let error = validate_bounded_json_schema(&schema).expect_err("long string must fail");
        assert!(error.contains("exceeds"), "{error}");
    }

    #[test]
    fn rejects_external_references_but_allows_local_ones() {
        let external = json!({ "$ref": "https://example.test/schema.json" });
        let error = validate_bounded_json_schema(&external).expect_err("external ref must fail");
        assert!(error.contains("external"), "{error}");
        let local = json!({
            "$defs": { "name": { "type": "string" } },
            "$ref": "#/$defs/name",
        });
        validate_bounded_json_schema(&local).expect("local ref should pass");
    }

    #[test]
    fn rejects_schemas_that_do_not_compile() {
        let schema = json!({ "type": "nonsense" });
        validate_bounded_json_schema(&schema).expect_err("invalid schema must fail");
    }
}
