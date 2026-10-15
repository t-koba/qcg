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
    reasons: BTreeMap<String, Value>,
    #[serde(default)]
    item: Option<Value>,
}

impl ValueBag {
    pub fn with_inputs(inputs: BTreeMap<String, Value>) -> Self {
        Self {
            inputs,
            steps: BTreeMap::new(),
            statuses: BTreeMap::new(),
            reasons: BTreeMap::new(),
            item: None,
        }
    }

    pub fn set_step_output(&mut self, id: impl Into<String>, value: Value) {
        self.steps.insert(id.into(), value);
    }

    /// Publishes a step output under the node's output alias (or the node
    /// id when unset), optionally also under a second id for namespaced
    /// replays. One shared rule for the sequential, wave, and repair
    /// success paths so they cannot drift apart (E10).
    pub fn publish_step_output(
        &mut self,
        node_id: &str,
        output_alias: Option<&str>,
        output: &Option<Value>,
        extra_alias: Option<&str>,
    ) {
        let Some(value) = output.clone() else {
            return;
        };
        let name = output_alias.unwrap_or(node_id);
        self.set_step_output(name, value.clone());
        if let Some(extra) = extra_alias {
            self.set_step_output(extra, value);
        }
    }

    pub fn set_inputs(&mut self, inputs: BTreeMap<String, Value>) {
        self.inputs = inputs;
    }

    pub fn set_step_status(&mut self, id: impl Into<String>, status: impl Into<String>) {
        let id = id.into();
        self.statuses
            .insert(id.clone(), Value::String(status.into()));
        self.reasons.remove(&id);
    }

    /// Sets one folded outcome. Outcome metadata is keyed by canonical node
    /// path; output aliases belong only to `steps` output values.
    pub fn set_step_outcome(
        &mut self,
        id: impl Into<String>,
        status: impl Into<String>,
        reason: Option<Value>,
    ) {
        let id = id.into();
        self.statuses
            .insert(id.clone(), Value::String(status.into()));
        if let Some(reason) = reason {
            self.reasons.insert(id, reason);
        } else {
            self.reasons.remove(&id);
        }
    }

    /// Replaces only outcome metadata with the journal-fold projection.
    /// Outputs, inputs, and the foreach item remain live-scope data.
    pub fn sync_outcome_metadata(&mut self, folded: &ValueBag) {
        self.statuses.clone_from(&folded.statuses);
        self.reasons.clone_from(&folded.reasons);
    }

    /// Removes outcome metadata for a node path and its descendants. Path
    /// boundaries are the delimiters used by foreach and repair node paths.
    pub fn clear_step_outcome_subtree(&mut self, prefix: &str) {
        self.statuses
            .retain(|path, _| !Self::path_is_within(path, prefix));
        self.reasons
            .retain(|path, _| !Self::path_is_within(path, prefix));
    }

    /// Refreshes one node-path subtree from folded state without importing
    /// statuses from concurrent foreach siblings.
    pub fn sync_step_outcome_subtree(&mut self, folded: &ValueBag, prefix: &str) {
        self.clear_step_outcome_subtree(prefix);
        self.statuses.extend(
            folded
                .statuses
                .iter()
                .filter(|(path, _)| Self::path_is_within(path, prefix))
                .map(|(path, status)| (path.clone(), status.clone())),
        );
        self.reasons.extend(
            folded
                .reasons
                .iter()
                .filter(|(path, _)| Self::path_is_within(path, prefix))
                .map(|(path, reason)| (path.clone(), reason.clone())),
        );
    }

    fn path_is_within(path: &str, prefix: &str) -> bool {
        path == prefix
            || ["/", "[", "@repair."]
                .iter()
                .any(|separator| path.starts_with(&format!("{prefix}{separator}")))
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
        for (key, value) in values {
            self.set_step_status(key, value);
        }
    }

    /// Merges step outputs and statuses produced by a parallel iteration.
    /// Inputs and the loop item are per-iteration context and stay local;
    /// children address their own node ids, so keys are disjoint.
    pub fn absorb(&mut self, other: &ValueBag) {
        self.steps
            .extend(other.steps.iter().map(|(k, v)| (k.clone(), v.clone())));
        self.statuses
            .extend(other.statuses.iter().map(|(k, v)| (k.clone(), v.clone())));
        self.reasons
            .extend(other.reasons.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    /// Merges only outputs from an isolated iteration into its parent.
    /// Outcome metadata is projected from the journal after the join instead
    /// of copying potentially stale cloned status maps.
    pub fn absorb_outputs(&mut self, other: &ValueBag) {
        self.steps
            .extend(other.steps.iter().map(|(k, v)| (k.clone(), v.clone())));
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
            .chain(self.reasons.keys())
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
                if let Some(reason) = self.reasons.get(key) {
                    value.insert("reason".into(), reason.clone());
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
            "steps" => self.get_step_path(path.strip_prefix("steps.")?),
            "item" => {
                let item = self.item.as_ref()?;
                Self::descend(item, &parts[1..])
            }
            _ => None,
        }
    }

    /// Resolves the longest known canonical node path before the outcome
    /// field. Repair paths contain dots (`@repair.N`), so splitting the full
    /// expression path on dots would confuse part of the node id with a
    /// field name.
    fn get_step_path(&self, path: &str) -> Option<&Value> {
        let id = self
            .steps
            .keys()
            .chain(self.statuses.keys())
            .chain(self.reasons.keys())
            .filter(|id| {
                path.strip_prefix(id.as_str())
                    .is_some_and(|suffix| suffix.starts_with('.'))
            })
            .max_by_key(|id| id.len())?;
        let fields = path.strip_prefix(id)?.strip_prefix('.')?;
        let mut parts = fields.split('.');
        let value = match parts.next()? {
            "output" => self.steps.get(id)?,
            "status" => self.statuses.get(id)?,
            "reason" => self.reasons.get(id)?,
            _ => return None,
        };
        Self::descend(value, &parts.collect::<Vec<_>>())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_outcomes_resolve_repair_paths_and_patch_clears_reason() {
        let mut bag = ValueBag::default();
        let reason = serde_json::json!({ "code": "check_failed", "message": "failed" });
        bag.set_step_outcome("build@repair.1/repair", "failed", Some(reason.clone()));
        assert_eq!(
            bag.get_path("steps.build@repair.1/repair.status"),
            Some(&Value::String("failed".into()))
        );
        assert_eq!(
            bag.get_path("steps.build@repair.1/repair.reason.code"),
            Some(&Value::String("check_failed".into()))
        );
        assert!(
            bag.eval_bool_typed(Some(&Expr(
                "steps.build@repair.1/repair.status == \"failed\"".into()
            )))
            .expect("repair status expression should resolve")
        );
        assert!(
            bag.eval_bool_typed(Some(&Expr(
                "steps.build@repair.1/repair.reason.code == \"check_failed\"".into()
            )))
            .expect("repair reason expression should resolve")
        );

        bag.patch_step_statuses(BTreeMap::from([(
            "build@repair.1/repair".into(),
            "success".into(),
        )]));
        assert_eq!(
            bag.get_path("steps.build@repair.1/repair.status"),
            Some(&Value::String("success".into()))
        );
        assert_eq!(bag.get_path("steps.build@repair.1/repair.reason"), None);
    }

    #[test]
    fn scoped_outcome_sync_preserves_live_values_and_hides_sibling_outcomes() {
        let mut live =
            ValueBag::with_inputs(BTreeMap::from([("name".into(), Value::String("a".into()))]));
        live.set_step_output("live", serde_json::json!({ "value": 1 }));
        live.set_item(Some(Value::String("current".into())));

        live.set_step_outcome(
            "each[0]/stale",
            "failed",
            Some(serde_json::json!({ "code": "execution_failed", "message": "stale" })),
        );
        live.set_step_outcome(
            "each[1]/first",
            "failed",
            Some(serde_json::json!({ "code": "execution_failed", "message": "sibling" })),
        );

        let mut folded = ValueBag::default();
        folded.set_step_outcome("each[0]/first", "success", None);
        folded.set_step_outcome(
            "each[1]/first",
            "failed",
            Some(serde_json::json!({ "code": "execution_failed", "message": "other" })),
        );
        live.clear_step_outcome_subtree("each");
        live.sync_step_outcome_subtree(&folded, "each[0]");

        assert_eq!(
            live.get_path("steps.each[0]/first.status"),
            Some(&Value::String("success".into()))
        );
        assert_eq!(live.get_path("steps.each[1]/first.status"), None);
        assert_eq!(live.get_path("steps.each[0]/stale.status"), None);
        assert_eq!(
            live.get_path("steps.live.output.value"),
            Some(&Value::from(1))
        );
        assert_eq!(live.inputs().get("name").and_then(Value::as_str), Some("a"));
        assert_eq!(live.item().and_then(Value::as_str), Some("current"));
    }
}
