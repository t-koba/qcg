mod bag;
mod error;
mod eval;
mod lexer;
mod parser;

use serde_json::{Value, json};

pub use bag::*;
pub use error::*;

/// The shared expression corpus, parsed with the same TOML reader the native
/// evaluator's own test uses, so the frontend corpus test and the Rust corpus
/// test can never disagree about what the fixture says.
///
/// Returns the evaluation context and the cases as JSON-ready values.
pub fn corpus_fixture() -> Result<(Value, Vec<(String, bool)>), String> {
    let fixture = include_str!("../../../fixtures/expr-corpus.toml");
    let parsed = toml::from_str::<toml::Value>(fixture).map_err(|error| error.to_string())?;
    let context = parsed
        .get("context")
        .ok_or_else(|| "expression corpus fixture must contain `context`".to_string())?;
    let cases = parsed
        .get("cases")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| "expression corpus fixture must contain `cases`".to_string())?;
    let cases = cases
        .iter()
        .map(|case| {
            let expr = case
                .get("expr")
                .and_then(toml::Value::as_str)
                .ok_or_else(|| "each corpus case must carry `expr`".to_string())?;
            let expected = case
                .get("expected")
                .and_then(toml::Value::as_bool)
                .ok_or_else(|| "each corpus case must carry `expected`".to_string())?;
            Ok((expr.to_string(), expected))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((toml_to_json(context), cases))
}

/// TOML to JSON with the one fixture-specific rule: the string `null` is the
/// corpus' null value, matching how the native fixture test reads it.
fn toml_to_json(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(value) if value == "null" => Value::Null,
        toml::Value::String(value) => Value::String(value.clone()),
        toml::Value::Integer(value) => json!(value),
        toml::Value::Float(value) => json!(value),
        toml::Value::Boolean(value) => Value::Bool(*value),
        toml::Value::Datetime(value) => Value::String(value.to_string()),
        toml::Value::Array(values) => Value::Array(values.iter().map(toml_to_json).collect()),
        toml::Value::Table(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), toml_to_json(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    fn value_bag() -> ValueBag {
        let mut inputs = BTreeMap::new();
        inputs.insert("name".into(), Value::String("  Mixed Case  ".into()));
        inputs.insert(
            "tags".into(),
            Value::Array(vec![
                Value::String("b".into()),
                Value::String("a".into()),
                Value::String("b".into()),
            ]),
        );
        inputs.insert(
            "scores".into(),
            Value::Array(vec![json!(3), json!(1), json!(2)]),
        );
        inputs.insert("nested".into(), json!({"rows": [{"v": 1}, {"v": 2}]}));
        ValueBag::with_inputs(inputs)
    }

    fn eval_value(case: &str) -> Value {
        value_bag()
            .eval_value(case)
            .unwrap_or_else(|error| panic!("expression `{case}` should evaluate: {error}"))
    }

    #[test]
    fn expression_paths_support_array_indices() {
        assert_eq!(eval_value("inputs.tags.0"), json!("b"));
        assert_eq!(eval_value("inputs.tags.2"), json!("b"));
        assert_eq!(eval_value("inputs.nested.rows.1.v"), json!(2));
        assert_eq!(eval_value("inputs.tags.9"), Value::Null);
        assert_eq!(eval_value("inputs.name.0"), Value::Null);
    }

    #[test]
    fn expression_string_functions_evaluate() {
        assert_eq!(eval_value("upper(inputs.name)"), json!("  MIXED CASE  "));
        assert_eq!(eval_value("lower(inputs.name)"), json!("  mixed case  "));
        assert_eq!(eval_value("trim(inputs.name)"), json!("Mixed Case"));
        assert_eq!(
            eval_value("starts_with(inputs.name, '  Mix')"),
            Value::Bool(true)
        );
        assert_eq!(
            eval_value("ends_with(inputs.name, 'Case  ')"),
            Value::Bool(true)
        );
        assert_eq!(
            eval_value("replace(inputs.name, 'Mixed', 'Plain')"),
            json!("  Plain Case  ")
        );
        assert_eq!(eval_value("split('a,b,c', ',')"), json!(["a", "b", "c"]));
        assert_eq!(eval_value("join(split('a,b', ','), '-')"), json!("a-b"));
        assert_eq!(eval_value("join(inputs.tags, ',')"), json!("b,a,b"));
    }

    #[test]
    fn expression_collection_functions_evaluate() {
        assert_eq!(eval_value("first(inputs.tags)"), json!("b"));
        assert_eq!(eval_value("last(inputs.tags)"), json!("b"));
        assert_eq!(eval_value("first(split('', ','))"), json!(""));
        assert_eq!(eval_value("keys(inputs.nested)"), json!(["rows"]));
        assert_eq!(eval_value("reverse(inputs.tags)"), json!(["b", "a", "b"]));
        assert_eq!(eval_value("unique(inputs.tags)"), json!(["b", "a"]));
        assert_eq!(eval_value("sort(inputs.tags)"), json!(["a", "b", "b"]));
        assert_eq!(eval_value("sort(inputs.scores)"), json!([1, 2, 3]));
        assert_eq!(
            eval_value("flatten([inputs.tags, ['c']])"),
            json!(["b", "a", "b", "c"])
        );
        assert_eq!(
            eval_value("values(inputs.nested)"),
            json!([[{"v": 1}, {"v": 2}]])
        );
    }

    #[test]
    fn expression_scalar_helpers_evaluate() {
        assert_eq!(eval_value("empty('')"), Value::Bool(true));
        assert_eq!(eval_value("empty(inputs.tags)"), Value::Bool(false));
        assert_eq!(eval_value("empty(inputs.missing)"), Value::Bool(true));
        assert_eq!(
            eval_value("default(inputs.missing, 'fallback')"),
            json!("fallback")
        );
        assert_eq!(
            eval_value("default(inputs.name, 'fallback')"),
            json!("  Mixed Case  ")
        );
        assert_eq!(eval_value("sum(inputs.scores)"), json!(6.0));
        assert_eq!(eval_value("min(inputs.scores)"), json!(1.0));
        assert_eq!(eval_value("max(inputs.scores)"), json!(3.0));
        assert_eq!(eval_value("min([])"), Value::Null);
    }

    #[test]
    fn expression_function_arity_and_type_errors_are_explicit() {
        for case in [
            "upper(1)",
            "first(inputs.name)",
            "keys(inputs.tags)",
            "sort([1, 'a'])",
            "sum(['a'])",
            "join([1, 2], ',')",
            "unknown_fn(1)",
            "len()",
        ] {
            assert!(
                value_bag().eval_value(case).is_err(),
                "expression `{case}` should fail"
            );
        }
    }

    #[test]
    fn expression_input_byte_limit_is_typed_and_explicit() {
        let expression = Expr("x".repeat(MAX_EXPRESSION_BYTES + 1));
        let error = ValueBag::default()
            .eval_bool_typed(Some(&expression))
            .expect_err("oversized expression should fail");
        assert_eq!(
            error,
            ExprError::InputTooLarge {
                bytes: MAX_EXPRESSION_BYTES + 1,
                limit: MAX_EXPRESSION_BYTES,
            }
        );
    }

    #[test]
    fn expression_token_limit_is_typed_and_explicit() {
        let expression = (0..MAX_EXPRESSION_TOKENS)
            .map(|_| "true")
            .collect::<Vec<_>>()
            .join(" ");
        let error = ValueBag::default()
            .eval_bool_typed(Some(&Expr(expression)))
            .expect_err("oversized token stream should fail");
        assert!(matches!(error, ExprError::TooManyTokens { .. }));
    }

    #[test]
    fn expression_parenthesis_and_unary_depth_limits_are_typed() {
        let nested = format!(
            "{}true{}",
            "(".repeat(MAX_EXPRESSION_DEPTH + 1),
            ")".repeat(MAX_EXPRESSION_DEPTH + 1)
        );
        let error = ValueBag::default()
            .eval_bool_typed(Some(&Expr(nested)))
            .expect_err("deep parenthesized expression should fail");
        assert!(matches!(error, ExprError::TooDeep { .. }));

        let unary = format!("{}true", "!".repeat(MAX_EXPRESSION_DEPTH + 1));
        let error = ValueBag::default()
            .eval_bool_typed(Some(&Expr(unary)))
            .expect_err("deep unary expression should fail");
        assert!(matches!(error, ExprError::TooDeep { .. }));
    }

    #[test]
    fn expression_node_limit_is_typed_and_explicit() {
        let terms = MAX_EXPRESSION_NODES / 4 + 2;
        let expression = (0..terms)
            .map(|_| "true == true")
            .collect::<Vec<_>>()
            .join(" && ");
        let error = ValueBag::default()
            .eval_bool_typed(Some(&Expr(expression)))
            .expect_err("oversized AST should fail");
        assert!(matches!(error, ExprError::TooManyNodes { .. }));
    }

    fn corpus_bag() -> ValueBag {
        let mut inputs = BTreeMap::new();
        inputs.insert("enabled".into(), json!(true));
        inputs.insert("disabled".into(), json!(false));
        inputs.insert("name".into(), json!("alpha"));
        inputs.insert("other".into(), json!("beta"));
        inputs.insert("count".into(), json!(3));
        inputs.insert("limit".into(), json!(5));
        inputs.insert("zero".into(), json!(0));
        inputs.insert("nullish".into(), Value::Null);
        inputs.insert("object".into(), json!({ "ready": true, "rank": 7 }));
        let mut bag = ValueBag::with_inputs(inputs);
        bag.set_step_output(
            "render",
            json!({
                "ready": true,
                "status": "ok",
                "count": 2,
                "nested": { "flag": false }
            }),
        );
        bag.set_step_output("empty", Value::Null);
        bag.set_step_status("render", "succeeded");
        bag.set_item(Some(json!({
            "name": "site-a",
            "enabled": true,
            "priority": 10,
            "meta": { "tier": "gold" }
        })));
        bag
    }

    #[test]
    fn shared_expr_corpus_fixture_matches_expected_results() {
        let fixture = include_str!("../../../fixtures/expr-corpus.toml");
        let parsed =
            toml::from_str::<toml::Value>(fixture).expect("expression corpus fixture should parse");
        let context = parsed
            .get("context")
            .expect("fixture should contain context");
        let inputs = json_from_toml(
            context
                .get("inputs")
                .expect("fixture should contain context.inputs"),
        );
        let mut bag = ValueBag::with_inputs(
            inputs
                .as_object()
                .expect("context.inputs should be an object")
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        let steps = context
            .get("steps")
            .expect("fixture should contain context.steps")
            .as_table()
            .expect("context.steps should be a table");
        for (id, value) in steps {
            if let Some(output) = value.get("output") {
                bag.set_step_output(id, json_from_toml(output));
            }
        }
        bag.set_item(Some(json_from_toml(
            context
                .get("item")
                .expect("fixture should contain context.item"),
        )));
        let cases = parsed
            .get("cases")
            .and_then(toml::Value::as_array)
            .expect("fixture should contain cases");
        assert!(cases.len() >= 50, "expression corpus must have 50+ cases");
        // Every case runs: mismatches collect and report together, so one
        // failure never masks another.
        let mut diverged = Vec::new();
        for case in cases {
            let expr = case
                .get("expr")
                .and_then(toml::Value::as_str)
                .expect("case should contain expr");
            let expected = case
                .get("expected")
                .and_then(toml::Value::as_bool)
                .expect("case should contain expected");
            if bag.eval_bool(Some(&Expr(expr.into()))).unwrap() != expected {
                diverged.push(expr.to_string());
            }
        }
        assert!(
            diverged.is_empty(),
            "expression corpus diverged: {}",
            diverged.join(", ")
        );
    }

    fn json_from_toml(value: &toml::Value) -> Value {
        toml_to_json(value)
    }

    #[test]
    fn expression_corpus_bool_cases() {
        let bag = corpus_bag();
        let cases: &[(&str, &str, bool)] = &[
            ("expr_corpus_absent_expression_is_true", "", false),
            (
                "expr_corpus_or_false_true",
                "inputs.disabled || inputs.enabled",
                true,
            ),
            (
                "expr_corpus_and_precedence_left_split",
                "inputs.enabled && inputs.count == 3",
                true,
            ),
            (
                "expr_corpus_or_precedence_left_split",
                "inputs.disabled || inputs.count == 3",
                true,
            ),
            (
                "expr_corpus_string_order_ge_equal",
                "inputs.name >= 'alpha'",
                true,
            ),
            (
                "expr_corpus_number_decimal_equal",
                "inputs.count == 3.0",
                true,
            ),
            (
                "expr_corpus_bool_not_equal_false",
                "inputs.enabled != false",
                true,
            ),
            (
                "expr_corpus_path_to_path_number_equal",
                "inputs.count != inputs.limit",
                true,
            ),
            (
                "expr_corpus_right_literal_whitespace_trimmed",
                "inputs.name ==   'alpha'  ",
                true,
            ),
            (
                "expr_corpus_left_path_whitespace_trimmed",
                "  inputs.count   >= 3",
                true,
            ),
            (
                "expr_corpus_neq_with_type_mismatch",
                "inputs.name != inputs.count",
                true,
            ),
            (
                "expr_corpus_non_empty_string_is_truthy",
                "inputs.name",
                true,
            ),
            (
                "expr_corpus_non_zero_number_is_truthy",
                "inputs.count",
                true,
            ),
            (
                "expr_corpus_unknown_left_path_is_null",
                "inputs.missing == true",
                false,
            ),
            (
                "expr_parentheses_override_precedence",
                "inputs.disabled && (inputs.count == 3 || inputs.enabled)",
                false,
            ),
            (
                "expr_nested_parentheses",
                "(inputs.disabled || inputs.enabled) && (3 < inputs.limit)",
                true,
            ),
            (
                "expr_prefix_not_binds_tighter_than_equality",
                "!inputs.disabled == true",
                true,
            ),
            (
                "expr_literal_can_be_comparison_left_operand",
                "3 < inputs.limit",
                true,
            ),
            (
                "expr_arithmetic_obeys_precedence",
                "inputs.count + 2 * 2 == 7",
                true,
            ),
            (
                "expr_contains_and_len_functions",
                "contains(inputs.name, 'ph') && len(inputs.name) == 5",
                true,
            ),
            (
                "expr_escaped_quote_in_string",
                "contains(\"it\\\'s alpha\", inputs.name)",
                true,
            ),
            (
                "expr_step_status_is_available",
                "steps.render.status == 'succeeded'",
                true,
            ),
        ];
        let mut diverged = Vec::new();
        for (name, expr, expected) in cases {
            if bag.eval_bool(Some(&Expr((*expr).into()))).unwrap() != *expected {
                diverged.push(*name);
            }
        }
        assert!(
            diverged.is_empty(),
            "bool cases diverged: {}",
            diverged.join(", ")
        );
    }

    #[test]
    fn expression_corpus_error_cases() {
        let bag = corpus_bag();
        let cases: &[(&str, &str, &str)] = &[
            (
                "expr_corpus_unsupported_literal_errors",
                "inputs.name == alpha",
                "unsupported literal",
            ),
            (
                "expr_corpus_ordering_boolean_errors",
                "inputs.enabled > false",
                "not valid for booleans",
            ),
            (
                "expr_corpus_ordering_null_errors",
                "inputs.nullish > null",
                "not valid for null",
            ),
            (
                "expr_corpus_ordering_type_mismatch_errors",
                "inputs.name > 3",
                "type mismatch",
            ),
            (
                "expr_corpus_unexpected_character_errors",
                "inputs.name == {1}",
                "unexpected character",
            ),
        ];
        let mut diverged = Vec::new();
        for (name, expr, needle) in cases {
            match bag.eval_bool(Some(&Expr((*expr).into()))) {
                Err(error) if error.contains(*needle) => {}
                Err(error) => diverged.push(format!("{name}: {error}")),
                Ok(value) => diverged.push(format!("{name}: unexpectedly ok ({value:?})")),
            }
        }
        assert!(
            diverged.is_empty(),
            "error cases diverged: {}",
            diverged.join(", ")
        );
    }
}
