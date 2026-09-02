use qcg_contract::ValueBag;
use qcg_contract::expr::MAX_EXPRESSION_BYTES;
use qcg_types::Expr;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

/// Maximum UTF-8 byte length accepted for the JSON evaluation context.
pub const MAX_CONTEXT_JSON_BYTES: usize = 1024 * 1024;
/// Maximum number of JSON values accepted in one evaluation context.
pub const MAX_CONTEXT_JSON_NODES: usize = 16 * 1024;
/// Maximum JSON object/array nesting accepted in one evaluation context.
pub const MAX_CONTEXT_JSON_DEPTH: usize = 128;

#[derive(Debug, Deserialize)]
struct EvalContext {
    #[serde(default)]
    inputs: BTreeMap<String, Value>,
    #[serde(default)]
    steps: BTreeMap<String, StepContext>,
    #[serde(default)]
    item: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct StepContext {
    #[serde(default)]
    output: Value,
}

#[wasm_bindgen]
pub fn eval_bool_json(expr: &str, context_json: &str) -> Result<bool, JsValue> {
    eval_bool_json_inner(expr, context_json).map_err(|error| JsValue::from_str(&error))
}

fn eval_bool_json_inner(expr: &str, context_json: &str) -> Result<bool, String> {
    if expr.len() > MAX_EXPRESSION_BYTES {
        return Err(format!(
            "expression input is {} bytes, exceeding the {}-byte limit",
            expr.len(),
            MAX_EXPRESSION_BYTES
        ));
    }
    if context_json.len() > MAX_CONTEXT_JSON_BYTES {
        return Err(format!(
            "expression context JSON is {} bytes, exceeding the {}-byte limit",
            context_json.len(),
            MAX_CONTEXT_JSON_BYTES
        ));
    }
    validate_json_depth(context_json)?;
    let value: Value = serde_json::from_str(context_json)
        .map_err(|error| format!("invalid expression context JSON: {error}"))?;
    validate_json_nodes(&value)?;
    let context: EvalContext = serde_json::from_value(value)
        .map_err(|error| format!("invalid expression context JSON: {error}"))?;
    let mut bag = ValueBag::with_inputs(context.inputs);
    for (id, step) in context.steps {
        bag.set_step_output(id, step.output);
    }
    bag.set_item(context.item);
    bag.eval_bool(Some(&Expr(expr.to_string())))
}

fn validate_json_depth(source: &str) -> Result<(), String> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in source.bytes() {
        if in_string {
            if escaped {
                escaped = false;
            } else {
                match byte {
                    b'\\' => escaped = true,
                    b'"' => in_string = false,
                    _ => {}
                }
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                if depth >= MAX_CONTEXT_JSON_DEPTH {
                    return Err(format!(
                        "expression context JSON nesting depth exceeds the {}-level limit",
                        MAX_CONTEXT_JSON_DEPTH
                    ));
                }
                depth = depth.checked_add(1).ok_or_else(|| {
                    format!(
                        "expression context JSON nesting depth exceeds the {}-level limit",
                        MAX_CONTEXT_JSON_DEPTH
                    )
                })?;
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

fn validate_json_nodes(value: &Value) -> Result<(), String> {
    let mut nodes = 0usize;
    let mut pending = vec![(value, 1usize)];
    while let Some((value, depth)) = pending.pop() {
        nodes = nodes.checked_add(1).ok_or_else(node_limit_error)?;
        if nodes > MAX_CONTEXT_JSON_NODES {
            return Err(node_limit_error());
        }
        if depth > MAX_CONTEXT_JSON_DEPTH {
            return Err(format!(
                "expression context JSON nesting depth exceeds the {}-level limit",
                MAX_CONTEXT_JSON_DEPTH
            ));
        }
        match value {
            Value::Array(values) => {
                ensure_pending_node_budget(nodes, pending.len(), values.len())?;
                let child_depth = next_json_depth(depth)?;
                for value in values {
                    pending.push((value, child_depth));
                }
            }
            Value::Object(values) => {
                ensure_pending_node_budget(nodes, pending.len(), values.len())?;
                let child_depth = next_json_depth(depth)?;
                for value in values.values() {
                    pending.push((value, child_depth));
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn node_limit_error() -> String {
    format!(
        "expression context JSON node count exceeds the {}-node limit",
        MAX_CONTEXT_JSON_NODES
    )
}

fn next_json_depth(depth: usize) -> Result<usize, String> {
    depth.checked_add(1).ok_or_else(depth_limit_error)
}

fn depth_limit_error() -> String {
    format!(
        "expression context JSON nesting depth exceeds the {}-level limit",
        MAX_CONTEXT_JSON_DEPTH
    )
}

fn ensure_pending_node_budget(
    processed_nodes: usize,
    pending_nodes: usize,
    new_nodes: usize,
) -> Result<(), String> {
    let scheduled_nodes = pending_nodes
        .checked_add(new_nodes)
        .ok_or_else(node_limit_error)?;
    let minimum_total = processed_nodes
        .checked_add(scheduled_nodes)
        .ok_or_else(node_limit_error)?;
    if minimum_total > MAX_CONTEXT_JSON_NODES {
        return Err(node_limit_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_bool_json_uses_contract_expression_semantics() {
        let context = r#"{
          "inputs": { "enabled": true, "name": "alpha" },
          "steps": { "render": { "output": { "ready": true } } },
          "item": { "priority": 3 }
        }"#;
        assert!(
            eval_bool_json_inner("inputs.enabled && steps.render.output.ready", context).unwrap()
        );
        assert!(
            eval_bool_json_inner("item.priority == 3 && inputs.name == 'alpha'", context).unwrap()
        );
        assert!(!eval_bool_json_inner("inputs.name == 'beta'", context).unwrap());
    }

    #[test]
    fn eval_bool_json_reports_invalid_context() {
        let error = eval_bool_json_inner("true", "{").expect_err("invalid JSON should fail");
        assert!(error.contains("invalid expression context JSON"));
    }

    #[test]
    fn eval_bool_json_rejects_context_over_byte_limit() {
        let context = " ".repeat(MAX_CONTEXT_JSON_BYTES + 1);
        let error = eval_bool_json_inner("true", &context).expect_err("oversized JSON should fail");
        assert!(error.contains("byte limit"));
    }

    #[test]
    fn eval_bool_json_rejects_context_over_depth_limit_before_parsing() {
        let context = format!(
            "{}true{}",
            "[".repeat(MAX_CONTEXT_JSON_DEPTH + 1),
            "]".repeat(MAX_CONTEXT_JSON_DEPTH + 1)
        );
        let error = eval_bool_json_inner("true", &context).expect_err("deep JSON should fail");
        assert!(error.contains("nesting depth"));
    }

    #[test]
    fn eval_bool_json_rejects_context_over_node_limit() {
        let context = format!(
            "[{}]",
            std::iter::repeat_n("null", MAX_CONTEXT_JSON_NODES + 1)
                .collect::<Vec<_>>()
                .join(",")
        );
        let error = eval_bool_json_inner("true", &context).expect_err("large JSON should fail");
        assert!(error.contains("node count"));
    }
}
