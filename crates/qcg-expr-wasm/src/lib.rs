use qcg_contract::ValueBag;
use qcg_types::Expr;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

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
    let context: EvalContext = serde_json::from_str(context_json)
        .map_err(|error| format!("invalid expression context JSON: {error}"))?;
    let mut bag = ValueBag::with_inputs(context.inputs);
    for (id, step) in context.steps {
        bag.set_step_output(id, step.output);
    }
    bag.set_item(context.item);
    bag.eval_bool(Some(&Expr(expr.to_string())))
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
}
