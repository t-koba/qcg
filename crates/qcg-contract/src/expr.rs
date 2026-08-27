use qcg_types::Expr;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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
            value = value.get(part)?;
        }
        Some(value)
    }

    pub fn eval_bool(&self, expr: Option<&Expr>) -> Result<bool, String> {
        let Some(expr) = expr else {
            return Ok(true);
        };
        eval_expression(&expr.0, self)
    }
}

fn eval_expression(src: &str, bag: &ValueBag) -> Result<bool, String> {
    if src.trim().is_empty() {
        return Ok(false);
    }
    let mut parser = Parser::new(tokenize(src)?);
    let expression = parser.parse_expression(0)?;
    parser.expect_end()?;
    Ok(truthy(&expression.evaluate(bag)?))
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Identifier(String),
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Operator(&'static str),
    LeftParen,
    RightParen,
    Comma,
    End,
}

fn tokenize(source: &str) -> Result<Vec<Token>, String> {
    let chars = source.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }
        if matches!(ch, '\'' | '"') {
            let quote = ch;
            index += 1;
            let mut value = String::new();
            let mut closed = false;
            while index < chars.len() {
                match chars[index] {
                    current if current == quote => {
                        index += 1;
                        closed = true;
                        break;
                    }
                    '\\' => {
                        index += 1;
                        let escaped = chars
                            .get(index)
                            .copied()
                            .ok_or_else(|| "unterminated string escape".to_string())?;
                        value.push(match escaped {
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            '\\' => '\\',
                            '\'' => '\'',
                            '"' => '"',
                            other => {
                                return Err(format!("unsupported string escape `\\{other}`"));
                            }
                        });
                        index += 1;
                    }
                    current => {
                        value.push(current);
                        index += 1;
                    }
                }
            }
            if !closed {
                return Err("unterminated string literal".into());
            }
            tokens.push(Token::String(value));
            continue;
        }
        if ch.is_ascii_digit()
            || (ch == '.' && chars.get(index + 1).is_some_and(char::is_ascii_digit))
        {
            let start = index;
            index += 1;
            while index < chars.len()
                && (chars[index].is_ascii_digit()
                    || matches!(chars[index], '.' | 'e' | 'E' | '+' | '-'))
            {
                if matches!(chars[index], '+' | '-')
                    && !chars
                        .get(index.wrapping_sub(1))
                        .is_some_and(|previous| matches!(*previous, 'e' | 'E'))
                {
                    break;
                }
                index += 1;
            }
            let literal = chars[start..index].iter().collect::<String>();
            let value = literal
                .parse::<f64>()
                .map_err(|_| format!("invalid number literal `{literal}`"))?;
            if !value.is_finite() {
                return Err("number literal is not finite".into());
            }
            tokens.push(Token::Number(value));
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = index;
            index += 1;
            while index < chars.len()
                && (chars[index].is_ascii_alphanumeric() || matches!(chars[index], '_' | '.' | '-'))
            {
                index += 1;
            }
            let identifier = chars[start..index].iter().collect::<String>();
            tokens.push(match identifier.as_str() {
                "true" => Token::Bool(true),
                "false" => Token::Bool(false),
                "null" => Token::Null,
                _ => Token::Identifier(identifier),
            });
            continue;
        }
        let pair = chars
            .get(index + 1)
            .map(|next| [ch, *next].iter().collect::<String>());
        if let Some(operator) = pair.as_deref().and_then(|pair| match pair {
            "||" => Some("||"),
            "&&" => Some("&&"),
            "==" => Some("=="),
            "!=" => Some("!="),
            ">=" => Some(">="),
            "<=" => Some("<="),
            _ => None,
        }) {
            tokens.push(Token::Operator(operator));
            index += 2;
            continue;
        }
        tokens.push(match ch {
            '!' => Token::Operator("!"),
            '>' => Token::Operator(">"),
            '<' => Token::Operator("<"),
            '+' => Token::Operator("+"),
            '-' => Token::Operator("-"),
            '*' => Token::Operator("*"),
            '/' => Token::Operator("/"),
            '%' => Token::Operator("%"),
            '(' => Token::LeftParen,
            ')' => Token::RightParen,
            ',' => Token::Comma,
            _ => return Err(format!("unexpected character `{ch}` at position {index}")),
        });
        index += 1;
    }
    tokens.push(Token::End);
    Ok(tokens)
}

#[derive(Debug, Clone)]
enum ExpressionNode {
    Literal(Value),
    Path(String),
    Unary {
        operator: &'static str,
        value: Box<Self>,
    },
    Binary {
        operator: &'static str,
        left: Box<Self>,
        right: Box<Self>,
    },
    Call {
        name: String,
        arguments: Vec<Self>,
    },
}

impl ExpressionNode {
    fn evaluate(&self, bag: &ValueBag) -> Result<Value, String> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Path(path) => Ok(bag.get_path(path).cloned().unwrap_or(Value::Null)),
            Self::Unary { operator, value } => {
                let value = value.evaluate(bag)?;
                match *operator {
                    "!" => Ok(Value::Bool(!truthy(&value))),
                    "-" => number_value(-as_number(&value, "unary -")?),
                    _ => Err(format!("unknown unary operator `{operator}`")),
                }
            }
            Self::Binary {
                operator,
                left,
                right,
            } => {
                let left = left.evaluate(bag)?;
                match *operator {
                    "||" if truthy(&left) => return Ok(Value::Bool(true)),
                    "&&" if !truthy(&left) => return Ok(Value::Bool(false)),
                    _ => {}
                }
                let right = right.evaluate(bag)?;
                evaluate_binary(operator, &left, &right)
            }
            Self::Call { name, arguments } => {
                let values = arguments
                    .iter()
                    .map(|argument| argument.evaluate(bag))
                    .collect::<Result<Vec<_>, _>>()?;
                evaluate_call(name, &values)
            }
        }
    }
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, cursor: 0 }
    }

    fn parse_expression(&mut self, min_binding_power: u8) -> Result<ExpressionNode, String> {
        let mut left = self.parse_prefix()?;
        while let Token::Operator(operator) = self.peek() {
            let Some((left_power, right_power)) = infix_binding_power(operator) else {
                break;
            };
            if left_power < min_binding_power {
                break;
            }
            let operator = *operator;
            self.cursor += 1;
            let right = self.parse_expression(right_power)?;
            left = ExpressionNode::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_prefix(&mut self) -> Result<ExpressionNode, String> {
        let token = self.next().clone();
        match token {
            Token::Bool(value) => Ok(ExpressionNode::Literal(Value::Bool(value))),
            Token::Null => Ok(ExpressionNode::Literal(Value::Null)),
            Token::String(value) => Ok(ExpressionNode::Literal(Value::String(value))),
            Token::Number(value) => number_value(value).map(ExpressionNode::Literal),
            Token::Identifier(identifier) => {
                if matches!(self.peek(), Token::LeftParen) {
                    self.cursor += 1;
                    self.parse_call(identifier)
                } else if identifier.starts_with("inputs.")
                    || identifier.starts_with("steps.")
                    || identifier == "item"
                    || identifier.starts_with("item.")
                {
                    Ok(ExpressionNode::Path(identifier))
                } else {
                    Err(format!("unsupported literal `{identifier}`"))
                }
            }
            Token::Operator(operator @ ("!" | "-")) => Ok(ExpressionNode::Unary {
                operator,
                value: Box::new(self.parse_expression(13)?),
            }),
            Token::LeftParen => {
                let expression = self.parse_expression(0)?;
                match self.next() {
                    Token::RightParen => Ok(expression),
                    token => Err(format!("expected `)`, found {token:?}")),
                }
            }
            token => Err(format!("expected expression, found {token:?}")),
        }
    }

    fn parse_call(&mut self, name: String) -> Result<ExpressionNode, String> {
        let mut arguments = Vec::new();
        if matches!(self.peek(), Token::RightParen) {
            self.cursor += 1;
            return Ok(ExpressionNode::Call { name, arguments });
        }
        loop {
            arguments.push(self.parse_expression(0)?);
            match self.next() {
                Token::Comma => {}
                Token::RightParen => break,
                token => return Err(format!("expected `,` or `)`, found {token:?}")),
            }
        }
        Ok(ExpressionNode::Call { name, arguments })
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.cursor).unwrap_or(&Token::End)
    }

    fn next(&mut self) -> &Token {
        let index = self.cursor;
        self.cursor = self.cursor.saturating_add(1);
        self.tokens.get(index).unwrap_or(&Token::End)
    }

    fn expect_end(&self) -> Result<(), String> {
        match self.peek() {
            Token::End => Ok(()),
            token => Err(format!("unexpected trailing token {token:?}")),
        }
    }
}

fn infix_binding_power(operator: &str) -> Option<(u8, u8)> {
    Some(match operator {
        "||" => (1, 2),
        "&&" => (3, 4),
        "==" | "!=" => (5, 6),
        ">" | "<" | ">=" | "<=" => (7, 8),
        "+" | "-" => (9, 10),
        "*" | "/" | "%" => (11, 12),
        _ => return None,
    })
}

fn evaluate_binary(operator: &str, left: &Value, right: &Value) -> Result<Value, String> {
    match operator {
        "||" => Ok(Value::Bool(truthy(left) || truthy(right))),
        "&&" => Ok(Value::Bool(truthy(left) && truthy(right))),
        "==" => Ok(Value::Bool(values_equal(left, right))),
        "!=" => Ok(Value::Bool(!values_equal(left, right))),
        ">" | "<" | ">=" | "<=" => compare_order(left, operator, right).map(Value::Bool),
        "+" | "-" | "*" | "/" | "%" => {
            let left = as_number(left, operator)?;
            let right = as_number(right, operator)?;
            let value = match operator {
                "+" => left + right,
                "-" => left - right,
                "*" => left * right,
                "/" if right == 0.0 => return Err("division by zero".into()),
                "/" => left / right,
                "%" if right == 0.0 => return Err("remainder by zero".into()),
                "%" => left % right,
                _ => return Err(format!("unknown arithmetic operator `{operator}`")),
            };
            number_value(value)
        }
        _ => Err(format!("unknown operator `{operator}`")),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        _ => left == right,
    }
}

fn compare_order(left: &Value, operator: &str, right: &Value) -> Result<bool, String> {
    match (left, right) {
        (Value::String(left), Value::String(right)) => compare_ordered(left, operator, right),
        (Value::Number(left), Value::Number(right)) => compare_ordered(
            left.as_f64()
                .ok_or_else(|| "invalid left number".to_string())?,
            operator,
            right
                .as_f64()
                .ok_or_else(|| "invalid right number".to_string())?,
        ),
        (Value::Bool(_), Value::Bool(_)) => {
            Err(format!("operator `{operator}` is not valid for booleans"))
        }
        (Value::Null, Value::Null) => Err(format!("operator `{operator}` is not valid for null")),
        _ => Err(format!(
            "type mismatch in expression with operator `{operator}`"
        )),
    }
}

fn compare_ordered<T: PartialOrd>(left: T, operator: &str, right: T) -> Result<bool, String> {
    Ok(match operator {
        ">" => left > right,
        "<" => left < right,
        ">=" => left >= right,
        "<=" => left <= right,
        _ => return Err(format!("unknown ordering operator `{operator}`")),
    })
}

fn evaluate_call(name: &str, arguments: &[Value]) -> Result<Value, String> {
    match (name, arguments) {
        ("len", [value]) => {
            let length = match value {
                Value::String(value) => value.chars().count(),
                Value::Array(value) => value.len(),
                Value::Object(value) => value.len(),
                _ => return Err("len() requires a string, array, or object".into()),
            };
            Ok(Value::Number((length as u64).into()))
        }
        ("contains", [container, needle]) => Ok(Value::Bool(match (container, needle) {
            (Value::String(container), Value::String(needle)) => container.contains(needle),
            (Value::Array(container), needle) => container.contains(needle),
            (Value::Object(container), Value::String(needle)) => container.contains_key(needle),
            _ => return Err("contains() received incompatible arguments".into()),
        })),
        ("len", _) => Err("len() requires exactly one argument".into()),
        ("contains", _) => Err("contains() requires exactly two arguments".into()),
        _ => Err(format!("unknown expression function `{name}`")),
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn as_number(value: &Value, operator: &str) -> Result<f64, String> {
    value
        .as_f64()
        .ok_or_else(|| format!("operator `{operator}` requires numbers"))
}

fn number_value(value: f64) -> Result<Value, String> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| "numeric expression result is not finite".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn evaluates_contract_expression_subset() {
        let mut inputs = BTreeMap::new();
        inputs.insert("tls".into(), Value::Bool(true));
        inputs.insert("name".into(), Value::String("example.com".into()));
        let bag = ValueBag::with_inputs(inputs);
        assert!(
            bag.eval_bool(Some(&Expr("inputs.tls == true".into())))
                .unwrap()
        );
        assert!(
            bag.eval_bool(Some(&Expr(
                "inputs.name == 'example.com' && inputs.tls".into()
            )))
            .unwrap()
        );
        assert!(
            !bag.eval_bool(Some(&Expr("inputs.name == 'other'".into())))
                .unwrap()
        );
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
        for case in cases {
            let expr = case
                .get("expr")
                .and_then(toml::Value::as_str)
                .expect("case should contain expr");
            let expected = case
                .get("expected")
                .and_then(toml::Value::as_bool)
                .expect("case should contain expected");
            assert_eq!(
                bag.eval_bool(Some(&Expr(expr.into()))).unwrap(),
                expected,
                "expression `{expr}` should evaluate as expected"
            );
        }
    }

    fn json_from_toml(value: &toml::Value) -> Value {
        match value {
            toml::Value::String(value) if value == "null" => Value::Null,
            toml::Value::String(value) => Value::String(value.clone()),
            toml::Value::Integer(value) => json!(value),
            toml::Value::Float(value) => json!(value),
            toml::Value::Boolean(value) => Value::Bool(*value),
            toml::Value::Datetime(value) => Value::String(value.to_string()),
            toml::Value::Array(values) => Value::Array(values.iter().map(json_from_toml).collect()),
            toml::Value::Table(values) => Value::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), json_from_toml(value)))
                    .collect(),
            ),
        }
    }

    macro_rules! bool_case {
        ($name:ident, $expr:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let bag = corpus_bag();
                assert_eq!(bag.eval_bool(Some(&Expr($expr.into()))).unwrap(), $expected);
            }
        };
    }

    macro_rules! error_case {
        ($name:ident, $expr:expr, $needle:expr) => {
            #[test]
            fn $name() {
                let bag = corpus_bag();
                let error = bag
                    .eval_bool(Some(&Expr($expr.into())))
                    .expect_err("expression should fail");
                assert!(
                    error.contains($needle),
                    "expected `{error}` to contain `{}`",
                    $needle
                );
            }
        };
    }

    bool_case!(expr_corpus_absent_expression_is_true, "", false);
    bool_case!(expr_corpus_true_literal, "true", true);
    bool_case!(expr_corpus_false_literal, "false", false);
    bool_case!(expr_corpus_input_bool_true_path, "inputs.enabled", true);
    bool_case!(expr_corpus_input_bool_false_path, "inputs.disabled", false);
    bool_case!(expr_corpus_missing_path_is_false, "inputs.missing", false);
    bool_case!(expr_corpus_null_path_is_false, "inputs.nullish", false);
    bool_case!(
        expr_corpus_nested_input_bool_path,
        "inputs.object.ready",
        true
    );
    bool_case!(
        expr_corpus_step_bool_path,
        "steps.render.output.ready",
        true
    );
    bool_case!(
        expr_corpus_step_nested_bool_path,
        "steps.render.output.nested.flag",
        false
    );
    bool_case!(expr_corpus_item_bool_path, "item.enabled", true);
    bool_case!(expr_corpus_not_true_literal, "!true", false);
    bool_case!(expr_corpus_not_false_literal, "!false", true);
    bool_case!(expr_corpus_not_input_bool, "!inputs.disabled", true);
    bool_case!(expr_corpus_double_not_input_bool, "!!inputs.enabled", true);
    bool_case!(
        expr_corpus_and_true_true,
        "inputs.enabled && item.enabled",
        true
    );
    bool_case!(
        expr_corpus_and_true_false,
        "inputs.enabled && inputs.disabled",
        false
    );
    bool_case!(
        expr_corpus_or_false_true,
        "inputs.disabled || inputs.enabled",
        true
    );
    bool_case!(
        expr_corpus_or_false_false,
        "inputs.disabled || false",
        false
    );
    bool_case!(
        expr_corpus_and_precedence_left_split,
        "inputs.enabled && inputs.count == 3",
        true
    );
    bool_case!(
        expr_corpus_or_precedence_left_split,
        "inputs.disabled || inputs.count == 3",
        true
    );
    bool_case!(
        expr_corpus_string_single_quote_equal,
        "inputs.name == 'alpha'",
        true
    );
    bool_case!(
        expr_corpus_string_single_quote_not_equal,
        "inputs.name != 'beta'",
        true
    );
    bool_case!(
        expr_corpus_string_double_quote_equal,
        "inputs.name == \"alpha\"",
        true
    );
    bool_case!(
        expr_corpus_string_double_quote_not_equal_false,
        "inputs.name != \"alpha\"",
        false
    );
    bool_case!(expr_corpus_string_order_gt, "inputs.other > 'alpha'", true);
    bool_case!(expr_corpus_string_order_lt, "inputs.name < 'beta'", true);
    bool_case!(
        expr_corpus_string_order_ge_equal,
        "inputs.name >= 'alpha'",
        true
    );
    bool_case!(
        expr_corpus_string_order_le_equal,
        "inputs.name <= 'alpha'",
        true
    );
    bool_case!(expr_corpus_number_equal_int, "inputs.count == 3", true);
    bool_case!(expr_corpus_number_not_equal, "inputs.count != 4", true);
    bool_case!(expr_corpus_number_gt, "inputs.limit > 3", true);
    bool_case!(expr_corpus_number_lt, "inputs.count < 5", true);
    bool_case!(expr_corpus_number_ge_equal, "inputs.count >= 3", true);
    bool_case!(expr_corpus_number_le_equal, "inputs.count <= 3", true);
    bool_case!(expr_corpus_number_zero_equal, "inputs.zero == 0", true);
    bool_case!(
        expr_corpus_number_decimal_equal,
        "inputs.count == 3.0",
        true
    );
    bool_case!(expr_corpus_bool_equal_true, "inputs.enabled == true", true);
    bool_case!(
        expr_corpus_bool_not_equal_false,
        "inputs.enabled != false",
        true
    );
    bool_case!(
        expr_corpus_bool_equal_false,
        "inputs.disabled == false",
        true
    );
    bool_case!(expr_corpus_null_equal, "inputs.nullish == null", true);
    bool_case!(expr_corpus_null_not_equal, "inputs.nullish != true", true);
    bool_case!(
        expr_corpus_path_to_path_string_equal,
        "inputs.name != inputs.other",
        true
    );
    bool_case!(
        expr_corpus_path_to_path_number_equal,
        "inputs.count != inputs.limit",
        true
    );
    bool_case!(
        expr_corpus_path_to_path_bool_equal,
        "inputs.enabled == item.enabled",
        true
    );
    bool_case!(
        expr_corpus_step_string_equal,
        "steps.render.output.status == 'ok'",
        true
    );
    bool_case!(
        expr_corpus_step_number_equal,
        "steps.render.output.count == 2",
        true
    );
    bool_case!(
        expr_corpus_step_nested_bool_equal,
        "steps.render.output.nested.flag == false",
        true
    );
    bool_case!(expr_corpus_step_null_is_false, "steps.empty.output", false);
    bool_case!(
        expr_corpus_step_null_equal,
        "steps.empty.output == null",
        true
    );
    bool_case!(expr_corpus_item_string_equal, "item.name == 'site-a'", true);
    bool_case!(expr_corpus_item_number_gt, "item.priority > 5", true);
    bool_case!(
        expr_corpus_item_nested_string_equal,
        "item.meta.tier == 'gold'",
        true
    );
    bool_case!(
        expr_corpus_operator_inside_single_quoted_string,
        "inputs.name == 'a||b' || inputs.enabled",
        true
    );
    bool_case!(
        expr_corpus_operator_inside_double_quoted_string,
        "inputs.name == \"a&&b\" || inputs.enabled",
        true
    );
    bool_case!(
        expr_corpus_not_comparison,
        "!inputs.disabled && inputs.count == 3",
        true
    );
    bool_case!(
        expr_corpus_long_and_chain,
        "inputs.enabled && item.enabled && steps.render.output.ready",
        true
    );
    bool_case!(
        expr_corpus_long_or_chain,
        "inputs.disabled || false || steps.render.output.ready",
        true
    );
    bool_case!(
        expr_corpus_whitespace_trimmed,
        "  inputs.name == 'alpha'  ",
        true
    );
    bool_case!(
        expr_corpus_right_literal_whitespace_trimmed,
        "inputs.name ==   'alpha'  ",
        true
    );
    bool_case!(
        expr_corpus_left_path_whitespace_trimmed,
        "  inputs.count   >= 3",
        true
    );
    bool_case!(
        expr_corpus_false_comparison,
        "steps.render.output.status == 'failed'",
        false
    );
    bool_case!(
        expr_corpus_false_number_comparison,
        "item.priority < 5",
        false
    );
    bool_case!(
        expr_corpus_false_bool_path_to_path,
        "inputs.disabled == item.enabled",
        false
    );
    bool_case!(
        expr_corpus_neq_with_type_mismatch,
        "inputs.name != inputs.count",
        true
    );
    bool_case!(
        expr_corpus_eq_with_type_mismatch,
        "inputs.name == inputs.count",
        false
    );
    bool_case!(
        expr_corpus_missing_step_output_is_false,
        "steps.missing.output.ready",
        false
    );
    bool_case!(
        expr_corpus_missing_item_path_is_false,
        "item.missing",
        false
    );
    bool_case!(
        expr_corpus_string_case_sensitive,
        "inputs.name == 'Alpha'",
        false
    );
    bool_case!(
        expr_corpus_number_negative_literal,
        "inputs.zero > -1",
        true
    );
    bool_case!(expr_corpus_number_negative_false, "inputs.zero < -1", false);

    bool_case!(expr_corpus_non_empty_string_is_truthy, "inputs.name", true);
    bool_case!(expr_corpus_non_zero_number_is_truthy, "inputs.count", true);
    bool_case!(
        expr_corpus_unknown_left_path_is_null,
        "inputs.missing == true",
        false
    );
    error_case!(
        expr_corpus_unsupported_literal_errors,
        "inputs.name == alpha",
        "unsupported literal"
    );
    error_case!(
        expr_corpus_ordering_boolean_errors,
        "inputs.enabled > false",
        "not valid for booleans"
    );
    error_case!(
        expr_corpus_ordering_null_errors,
        "inputs.nullish > null",
        "not valid for null"
    );
    error_case!(
        expr_corpus_ordering_type_mismatch_errors,
        "inputs.name > 3",
        "type mismatch"
    );
    error_case!(
        expr_corpus_unknown_operator_literal_errors,
        "inputs.name == [1]",
        "unexpected character"
    );

    bool_case!(
        expr_parentheses_override_precedence,
        "inputs.disabled && (inputs.count == 3 || inputs.enabled)",
        false
    );
    bool_case!(
        expr_nested_parentheses,
        "(inputs.disabled || inputs.enabled) && (3 < inputs.limit)",
        true
    );
    bool_case!(
        expr_prefix_not_binds_tighter_than_equality,
        "!inputs.disabled == true",
        true
    );
    bool_case!(
        expr_literal_can_be_comparison_left_operand,
        "3 < inputs.limit",
        true
    );
    bool_case!(
        expr_arithmetic_obeys_precedence,
        "inputs.count + 2 * 2 == 7",
        true
    );
    bool_case!(
        expr_contains_and_len_functions,
        "contains(inputs.name, 'ph') && len(inputs.name) == 5",
        true
    );
    bool_case!(
        expr_escaped_quote_in_string,
        "contains(\"it\\\'s alpha\", inputs.name)",
        true
    );
    bool_case!(
        expr_step_status_is_available,
        "steps.render.status == 'succeeded'",
        true
    );
}
