//! Closed JSON Schema fragments for step parameter declarations.

use serde_json::{Value, json};

/// Object schema with a closed property set and required keys.
pub fn params_schema(required: &[&str], properties: Value) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties,
    })
}

/// Plain string schema.
pub fn string_schema() -> Value {
    json!({ "type": "string" })
}

/// String array schema.
pub fn string_array_schema() -> Value {
    json!({ "type": "array", "items": { "type": "string" } })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_schema_is_closed_with_required_keys() {
        let schema = params_schema(&["prompt"], json!({ "prompt": { "type": "string" } }));
        assert_eq!(schema["type"], json!("object"));
        assert_eq!(schema["additionalProperties"], json!(false));
        assert_eq!(schema["required"], json!(["prompt"]));
        assert_eq!(schema["properties"]["prompt"]["type"], json!("string"));
    }

    #[test]
    fn string_schemas_have_the_expected_shapes() {
        assert_eq!(string_schema(), json!({ "type": "string" }));
        assert_eq!(
            string_array_schema(),
            json!({ "type": "array", "items": { "type": "string" } })
        );
    }
}
