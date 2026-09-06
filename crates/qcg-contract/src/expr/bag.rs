use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use super::error::ExprError;
use super::lexer::{ensure_expression_bytes, eval_expression, tokenize};
use super::parser::{ExpressionNode, Parser};
use schemars::JsonSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(transparent)]
pub struct Expr(pub String);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValueBag {
    #[serde(default)]
    inputs: BTreeMap<String, Value>,
    #[serde(default)]
    steps: BTreeMap<String, Value>,
    #[serde(default)]
    statuses: BTreeMap<String, Value>,
    #[serde(default)]
    item: Option<Value>,
}

impl ValueBag {
    pub fn with_inputs(inputs: BTreeMap<String, Value>) -> Self {
        Self {
            inputs,
            steps: BTreeMap::new(),
            statuses: BTreeMap::new(),
            item: None,
        }
    }

    pub fn set_step_output(&mut self, id: impl Into<String>, value: Value) {
        self.steps.insert(id.into(), value);
    }

    pub fn set_inputs(&mut self, inputs: BTreeMap<String, Value>) {
        self.inputs = inputs;
    }

    pub fn set_step_status(&mut self, id: impl Into<String>, status: impl Into<String>) {
        self.statuses
            .insert(id.into(), Value::String(status.into()));
    }

    pub fn set_item(&mut self, value: Option<Value>) {
        self.item = value;
    }

    pub fn patch_inputs(&mut self, values: BTreeMap<String, Value>) {
        self.inputs.extend(values);
    }

    pub fn patch_step_outputs(&mut self, values: BTreeMap<String, Value>) {
        self.steps.extend(values);
    }

    pub fn patch_step_statuses(&mut self, values: BTreeMap<String, String>) {
        self.statuses.extend(
            values
                .into_iter()
                .map(|(key, value)| (key, Value::String(value))),
        );
    }

    pub fn item(&self) -> Option<&Value> {
        self.item.as_ref()
    }

    pub fn inputs(&self) -> &BTreeMap<String, Value> {
        &self.inputs
    }

    pub fn to_json(&self) -> Value {
        let mut ids = self
            .steps
            .keys()
            .chain(self.statuses.keys())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        let steps = ids
            .into_iter()
            .map(|key| {
                let mut value = serde_json::Map::new();
                if let Some(output) = self.steps.get(key) {
                    value.insert("output".into(), output.clone());
                }
                if let Some(status) = self.statuses.get(key) {
                    value.insert("status".into(), status.clone());
                }
                (key.clone(), Value::Object(value))
            })
            .collect::<BTreeMap<_, _>>();
        serde_json::json!({
            "inputs": self.inputs,
            "steps": steps,
            "item": self.item,
        })
    }

    pub fn get_path(&self, path: &str) -> Option<&Value> {
        let parts: Vec<&str> = path.split('.').collect();
        match parts.first().copied()? {
            "inputs" => Self::descend(self.inputs.get(parts.get(1).copied()?)?, &parts[2..]),
            "steps" => {
                let id = parts.get(1).copied()?;
                match parts.get(2).copied() {
                    Some("output") => Self::descend(self.steps.get(id)?, &parts[3..]),
                    Some("status") => Self::descend(self.statuses.get(id)?, &parts[3..]),
                    _ => None,
                }
            }
            "item" => {
                let item = self.item.as_ref()?;
                Self::descend(item, &parts[1..])
            }
            _ => None,
        }
    }

    fn descend<'a>(mut value: &'a Value, parts: &[&str]) -> Option<&'a Value> {
        for part in parts {
            match value {
                Value::Array(items) => {
                    let index: usize = part.parse().ok()?;
                    value = items.get(index)?;
                }
                _ => {
                    value = value.get(part)?;
                }
            }
        }
        Some(value)
    }

    pub fn eval_bool(&self, expr: Option<&Expr>) -> Result<bool, String> {
        self.eval_bool_typed(expr)
            .map_err(|error| error.to_string())
    }

    /// Evaluate a value-producing expression (paths, literals, and calls).
    /// Used for `foreach.items`, which also accepts plain dotted paths.
    pub fn eval_value(&self, src: &str) -> Result<Value, String> {
        ensure_expression_bytes(src).map_err(|error| error.to_string())?;
        let mut parser = Parser::new(tokenize(src).map_err(|error| error.to_string())?);
        let expression: ExpressionNode = parser
            .parse_expression(0)
            .map_err(|error| error.to_string())?;
        parser.expect_end().map_err(|error| error.to_string())?;
        expression.evaluate(self).map_err(|error| error.to_string())
    }

    pub fn eval_bool_typed(&self, expr: Option<&Expr>) -> Result<bool, ExprError> {
        let Some(expr) = expr else {
            return Ok(true);
        };
        eval_expression(&expr.0, self)
    }
}
