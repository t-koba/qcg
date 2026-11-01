use contract::NodeDef;
use engine::{StepContext, StepError};
use serde::de::{DeserializeOwned, Error as _};
use serde_json::{Value, value::RawValue};

/// Preserve number tokens until syntax and the evaluator's numeric range are checked.
pub(crate) fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> serde_json::Result<T> {
    serde_json::from_slice::<&RawValue>(bytes)?;
    contract::parse_json_guarded(bytes).map_err(serde_json::Error::custom)
}

/// Recursively merges `overlay` into `base`. Objects merge key-by-key with
/// `overlay` winning; every other value replaces the base value.
pub(crate) fn merge_json_objects(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, overlay_value) in overlay_map {
                match base_map.get_mut(key) {
                    Some(base_value) if base_value.is_object() && overlay_value.is_object() => {
                        merge_json_objects(base_value, overlay_value);
                    }
                    _ => {
                        base_map.insert(key.clone(), overlay_value.clone());
                    }
                }
            }
        }
        (base, overlay) => *base = overlay.clone(),
    }
}

/// Recursively removes all `null` values from a JSON structure so the
/// result can be safely converted to TOML, which has no null type.
pub(crate) fn strip_null_values(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                strip_null_values(v);
            }
        }
        Value::Array(arr) => {
            arr.retain(|v| !v.is_null());
            for v in arr.iter_mut() {
                strip_null_values(v);
            }
        }
        _ => {}
    }
}

pub(crate) fn render_json_templates(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    value: &Value,
    limit: Option<usize>,
) -> Result<Value, StepError> {
    let mut rendered_bytes = 0_usize;
    render_json_templates_inner(ctx, node, value, limit, &mut rendered_bytes)
}

pub(crate) fn render_json_templates_inner(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    value: &Value,
    limit: Option<usize>,
    rendered_bytes: &mut usize,
) -> Result<Value, StepError> {
    match value {
        Value::String(value) => {
            let rendered = ctx.render_inline(node, value)?;
            *rendered_bytes = rendered_bytes.checked_add(rendered.len()).ok_or_else(|| {
                StepError::failed(&node.id, "rendered JSON input size overflowed")
            })?;
            if let Some(limit) = limit
                && *rendered_bytes > limit
            {
                return Err(StepError::failed(
                    &node.id,
                    format!("rendered JSON input exceeds {limit} bytes"),
                ));
            }
            Ok(Value::String(rendered))
        }
        Value::Array(values) => values
            .iter()
            .map(|value| render_json_templates_inner(ctx, node, value, limit, rendered_bytes))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    render_json_templates_inner(ctx, node, value, limit, rendered_bytes)?,
                ))
            })
            .collect::<Result<serde_json::Map<_, _>, StepError>>()
            .map(Value::Object),
        value => Ok(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_number_tokens_preserve_supported_values_and_reject_rounding() {
        let raw = br#"{"values":[-9223372036854775808,18446744073709551615,18446744073709551616.0,1.8446744073709552e19,-0.0],"18446744073709551616":"\\\"18446744073709551617"}"#;
        let value: Value = parse_json(raw).unwrap();
        assert_eq!(value, serde_json::from_slice::<Value>(raw).unwrap());
        assert_eq!(value["values"][0].as_i64(), Some(i64::MIN));
        assert_eq!(value["values"][1].as_u64(), Some(u64::MAX));
        for token in [
            "18446744073709551616",
            "18446744073709551617",
            "-9223372036854775809",
            "1e400",
        ] {
            for raw in [token.to_owned(), format!(r#"{{"nested":[{token}]}}"#)] {
                assert!(
                    parse_json::<Value>(raw.as_bytes())
                        .unwrap_err()
                        .to_string()
                        .contains("[-2^63, 2^64-1]")
                );
            }
        }
        for invalid in [
            b"[18446744073709551616,]".as_slice(),
            b"null null",
            b"01",
            b"\xff",
        ] {
            assert!(parse_json::<Value>(invalid).unwrap_err().is_syntax());
        }
    }
}
