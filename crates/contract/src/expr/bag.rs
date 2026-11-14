use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use super::error::ExprError;
use super::lexer::{ensure_expression_bytes, eval_expression, tokenize};
use super::parser::{ExpressionNode, Parser};
use super::read::{ReadIdentity, ReadKind, ReadOwner, ReadRecorder, ReadTrace};
use schemars::JsonSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(transparent)]
pub struct Expr(pub String);

#[derive(Debug, Default, Serialize, Deserialize)]
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
    #[serde(skip)]
    reads: ReadRecorder,
}

// Parallel/nested bags inherit the budget, never their parent's read history.
impl Clone for ValueBag {
    fn clone(&self) -> Self {
        Self {
            inputs: self.inputs.clone(),
            steps: self.steps.clone(),
            statuses: self.statuses.clone(),
            reasons: self.reasons.clone(),
            item: self.item.clone(),
            reads: self.reads.fork(),
        }
    }
}

/// Borrowed attribution scope. Drop restores the previous owner, not history.
pub struct ValueBagReadScope<'a> {
    bag: &'a mut ValueBag,
    previous: ReadRecorder,
}
impl std::ops::Deref for ValueBagReadScope<'_> {
    type Target = ValueBag;
    fn deref(&self) -> &ValueBag {
        self.bag
    }
}
impl std::ops::DerefMut for ValueBagReadScope<'_> {
    fn deref_mut(&mut self) -> &mut ValueBag {
        self.bag
    }
}
impl Drop for ValueBagReadScope<'_> {
    fn drop(&mut self) {
        self.bag.reads = self.previous.clone();
    }
}

impl ValueBag {
    pub fn read_scope(&mut self, owner: ReadOwner) -> ValueBagReadScope<'_> {
        let previous = self.reads.clone();
        self.reads = self.reads.for_owner(owner);
        ValueBagReadScope {
            bag: self,
            previous,
        }
    }

    pub fn eval_bool_scoped(
        &mut self,
        owner: ReadOwner,
        expr: Option<&Expr>,
    ) -> Result<bool, String> {
        self.read_scope(owner).eval_bool(expr)
    }

    pub fn eval_value_scoped(&mut self, owner: ReadOwner, source: &str) -> Result<Value, String> {
        self.read_scope(owner).eval_value(source)
    }

    pub fn read_recorder(&self) -> ReadRecorder {
        self.reads.clone()
    }

    pub fn read_trace(&self) -> ReadTrace {
        self.reads.snapshot()
    }

    pub fn with_inputs(inputs: BTreeMap<String, Value>) -> Self {
        Self {
            inputs,
            steps: BTreeMap::new(),
            statuses: BTreeMap::new(),
            reasons: BTreeMap::new(),
            item: None,
            reads: ReadRecorder::default(),
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
        self.reads.merge(&other.reads);
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
        self.reads.merge(&other.reads);
        self.steps
            .extend(other.steps.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    pub fn item(&self) -> Option<&Value> {
        self.reads.record(
            &["item".into()],
            ReadKind::Collection,
            ReadIdentity::of(self.item.as_ref()),
        );
        self.item.as_ref()
    }

    pub fn inputs(&self) -> &BTreeMap<String, Value> {
        self.reads.record(
            &["inputs".into()],
            ReadKind::Collection,
            ReadIdentity::of(Some(&serde_json::json!(self.inputs))),
        );
        &self.inputs
    }

    pub fn to_json(&self) -> Value {
        let value = self.template_snapshot();
        self.reads
            .record(&[], ReadKind::Collection, ReadIdentity::of(Some(&value)));
        value
    }

    /// Internal bridge for the tracked template adapter only. Any other caller
    /// must use `to_json` so detached reads retain a conservative dependency.
    #[doc(hidden)]
    pub fn template_snapshot(&self) -> Value {
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
        let step_id = path.strip_prefix("steps.").and_then(|p| self.step_id(p));
        let value = self.resolve_path(path, step_id);
        let segments = match step_id {
            Some(id) => std::iter::once("steps")
                .chain(std::iter::once(id))
                .chain(path["steps.".len() + id.len() + 1..].split('.'))
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            None => path.split('.').map(str::to_owned).collect::<Vec<_>>(),
        };
        if path.starts_with("steps.") && step_id.is_none() {
            // With no known canonical id, dots may belong to the id rather
            // than to fields. Keep the attempted spelling and cover all
            // future additions instead of claiming precise normalization.
            self.reads.record(
                &["steps".into()],
                ReadKind::Collection,
                ReadIdentity::of(self.template_snapshot().get("steps")),
            );
        }
        self.reads
            .record(&segments, ReadKind::Lookup, ReadIdentity::of(value));
        if matches!(value, Some(Value::Array(_) | Value::Object(_))) {
            self.reads
                .record(&segments, ReadKind::Collection, ReadIdentity::of(value));
        }
        value
    }

    fn resolve_path(&self, path: &str, step_id: Option<&str>) -> Option<&Value> {
        let parts: Vec<&str> = path.split('.').collect();
        match parts.first().copied()? {
            "inputs" => Self::descend(self.inputs.get(parts.get(1).copied()?)?, &parts[2..]),
            "steps" => self.get_step_path(path.strip_prefix("steps.")?, step_id?),
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
    fn get_step_path(&self, path: &str, id: &str) -> Option<&Value> {
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

    fn step_id<'a>(&'a self, path: &str) -> Option<&'a str> {
        self.steps
            .keys()
            .chain(self.statuses.keys())
            .chain(self.reasons.keys())
            .filter(|id| {
                path.strip_prefix(id.as_str())
                    .is_some_and(|suffix| suffix.starts_with('.'))
            })
            .max_by_key(|id| id.len())
            .map(String::as_str)
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

    fn has_read(bag: &ValueBag, path: &[&str], kind: ReadKind, missing: bool) -> bool {
        bag.read_trace().reads.iter().any(|r| {
            r.path == path && r.kind == kind && (r.identity == ReadIdentity::Missing) == missing
        })
    }

    #[test]
    fn expression_reads_are_evaluated_paths_including_missing_and_collections() {
        let mut bag = ValueBag::with_inputs(BTreeMap::from([(
            "rows".into(),
            serde_json::json!([{"name":"a"}]),
        )]));
        let mut scope = bag.read_scope(ReadOwner::Node("node".into()));
        let bag = &mut *scope;
        assert_eq!(bag.eval_value("inputs.rows.0.name").unwrap(), "a");
        assert_eq!(
            bag.eval_value("steps.future.output.name").unwrap(),
            Value::Null
        );
        assert!(
            bag.eval_bool(Some(&Expr("true || inputs.unread".into())))
                .unwrap()
        );
        assert_eq!(
            bag.eval_value("inputs.rows").unwrap(),
            serde_json::json!([{"name":"a"}])
        );
        assert!(has_read(
            bag,
            &["inputs", "rows", "0", "name"],
            ReadKind::Lookup,
            false
        ));
        assert!(has_read(
            bag,
            &["steps", "future", "output", "name"],
            ReadKind::Lookup,
            true
        ));
        assert!(has_read(
            bag,
            &["inputs", "rows"],
            ReadKind::Collection,
            false
        ));
        assert!(
            !bag.read_trace()
                .reads
                .iter()
                .any(|read| read.path.iter().any(|p| p == "unread"))
        );
        bag.set_step_output("future", serde_json::json!({"name":"ready"}));
        assert_eq!(bag.eval_value("steps.future.output.name").unwrap(), "ready");
        bag.set_step_outcome("a@repair.1/child", "failed", None);
        assert_eq!(
            bag.get_path("steps.a@repair.1/child.status"),
            Some(&Value::from("failed"))
        );
        assert!(has_read(
            bag,
            &["steps", "a@repair.1/child", "status"],
            ReadKind::Lookup,
            false
        ));
        assert!(!bag.read_trace().incomplete);
    }

    #[test]
    fn owners_values_parallel_joins_and_direct_reads_are_isolated() {
        let mut parent = ValueBag::with_inputs(BTreeMap::from([("x".into(), Value::from(1))]));
        let public = parent.template_snapshot();
        let serialized = serde_json::to_value(&parent).unwrap();
        let a = ReadOwner::Node("each[0]/child".into());
        let b = ReadOwner::Node("each[1]/child".into());
        parent.eval_value_scoped(a.clone(), "inputs.x").unwrap();
        let mut child = parent.clone();
        assert!(child.read_trace().reads.is_empty());
        assert!(!child.read_trace().complete_for(&a));
        child.patch_inputs(BTreeMap::from([("x".into(), Value::from(2))]));
        let child = std::thread::spawn(move || {
            child.eval_value_scoped(b, "inputs.x").unwrap();
            child
        })
        .join()
        .unwrap();
        assert_eq!(parent.read_trace().reads.len(), 1);
        parent.absorb(&child);
        parent.absorb_outputs(&child); // repeated joins deduplicate
        let trace = parent.read_trace();
        assert_eq!(trace.reads.len(), 2);
        let identities = trace
            .reads
            .iter()
            .map(|r| &r.identity)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(identities.len(), 2);
        assert!(trace.reads.iter().any(|r| r.owner == a));
        assert!(
            trace
                .reads
                .iter()
                .any(|r| r.owner == ReadOwner::Node("each[1]/child".into()))
        );
        assert_eq!(child.read_trace().reads.len(), 1);
        // Same owner and path at two moments retain both value identities.
        parent.patch_inputs(BTreeMap::from([("x".into(), Value::from(3))]));
        parent.eval_value_scoped(a.clone(), "inputs.x").unwrap();
        assert_eq!(parent.read_trace().reads.len(), 3);
        parent.patch_inputs(BTreeMap::from([("x".into(), Value::from(1))]));
        {
            let scope = parent.read_scope(ReadOwner::Node("direct".into()));
            assert_eq!(scope.inputs()["x"], 1);
            assert!(scope.item().is_none());
            assert_eq!(scope.to_json(), public);
        }
        let trace = parent.read_trace();
        for path in [vec!["inputs"], vec!["item"], vec![]] {
            assert!(
                trace
                    .reads
                    .iter()
                    .any(|r| r.owner == ReadOwner::Node("direct".into())
                        && r.path == path
                        && r.kind == ReadKind::Collection)
            );
        }
        assert!(!trace.incomplete);
        // Scope restoration, including panic unwinding, must not attribute to a stale node.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _scope = parent.read_scope(ReadOwner::Node("panicked".into()));
            panic!("unwind scope");
        }));
        parent.get_path("inputs.x");
        assert!(
            parent
                .read_trace()
                .incomplete_owners
                .contains(&ReadOwner::Unscoped)
        );
        assert!(!parent.read_trace().complete_for(&a));
        assert_eq!(parent.to_json(), public);
        assert_eq!(serde_json::to_value(&parent).unwrap(), serialized);
        let restored: ValueBag = serde_json::from_value(serialized).unwrap();
        assert!(restored.read_trace().reads.is_empty());
        parent.read_recorder().set_byte_limit(0);
        assert_eq!(parent.eval_value("inputs.x").unwrap(), Value::from(1));
        assert!(parent.read_trace().incomplete && parent.read_trace().overflow);
        assert!(!parent.read_trace().complete_for(&a));
    }

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
