use serde_json::Value;

pub(crate) fn evaluate_binary(
    operator: &str,
    left: &Value,
    right: &Value,
) -> Result<Value, String> {
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

pub(crate) fn evaluate_call(name: &str, arguments: &[Value]) -> Result<Value, String> {
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
        ("empty", [value]) => Ok(Value::Bool(!truthy(value))),
        ("default", [value, fallback]) => Ok(if matches!(value, Value::Null) {
            fallback.clone()
        } else {
            value.clone()
        }),
        ("upper", [value]) => match value {
            Value::String(value) => Ok(Value::String(value.to_uppercase())),
            _ => Err("upper() requires a string".into()),
        },
        ("lower", [value]) => match value {
            Value::String(value) => Ok(Value::String(value.to_lowercase())),
            _ => Err("lower() requires a string".into()),
        },
        ("trim", [value]) => match value {
            Value::String(value) => Ok(Value::String(value.trim().to_string())),
            _ => Err("trim() requires a string".into()),
        },
        ("starts_with", [value, prefix]) => match (value, prefix) {
            (Value::String(value), Value::String(prefix)) => {
                Ok(Value::Bool(value.starts_with(prefix.as_str())))
            }
            _ => Err("starts_with() requires two strings".into()),
        },
        ("ends_with", [value, suffix]) => match (value, suffix) {
            (Value::String(value), Value::String(suffix)) => {
                Ok(Value::Bool(value.ends_with(suffix.as_str())))
            }
            _ => Err("ends_with() requires two strings".into()),
        },
        ("replace", [value, from, to]) => match (value, from, to) {
            (Value::String(value), Value::String(from), Value::String(to)) => {
                Ok(Value::String(value.replace(from.as_str(), to.as_str())))
            }
            _ => Err("replace() requires three strings".into()),
        },
        ("split", [value, separator]) => match (value, separator) {
            (Value::String(value), Value::String(separator)) => Ok(Value::Array(
                value
                    .split(separator.as_str())
                    .map(|part| Value::String(part.to_string()))
                    .collect(),
            )),
            _ => Err("split() requires two strings".into()),
        },
        ("join", [value]) => join_values(value, ""),
        ("join", [value, separator]) => match separator {
            Value::String(separator) => join_values(value, separator),
            _ => Err("join() separator must be a string".into()),
        },
        ("first", [value]) => match value {
            Value::Array(items) => Ok(items.first().cloned().unwrap_or(Value::Null)),
            _ => Err("first() requires an array".into()),
        },
        ("last", [value]) => match value {
            Value::Array(items) => Ok(items.last().cloned().unwrap_or(Value::Null)),
            _ => Err("last() requires an array".into()),
        },
        ("keys", [value]) => match value {
            Value::Object(map) => Ok(Value::Array(
                map.keys().map(|key| Value::String(key.clone())).collect(),
            )),
            _ => Err("keys() requires an object".into()),
        },
        ("values", [value]) => match value {
            Value::Object(map) => Ok(Value::Array(map.values().cloned().collect())),
            _ => Err("values() requires an object".into()),
        },
        ("reverse", [value]) => match value {
            Value::Array(items) => Ok(Value::Array(items.iter().rev().cloned().collect())),
            _ => Err("reverse() requires an array".into()),
        },
        ("flatten", [value]) => match value {
            Value::Array(items) => {
                let mut flat = Vec::new();
                for item in items {
                    match item {
                        Value::Array(nested) => flat.extend(nested.iter().cloned()),
                        _ => flat.push(item.clone()),
                    }
                }
                Ok(Value::Array(flat))
            }
            _ => Err("flatten() requires an array".into()),
        },
        ("unique", [value]) => match value {
            Value::Array(items) => {
                let mut unique = Vec::new();
                for item in items {
                    if !unique.contains(item) {
                        unique.push(item.clone());
                    }
                }
                Ok(Value::Array(unique))
            }
            _ => Err("unique() requires an array".into()),
        },
        ("sort", [value]) => match value {
            Value::Array(items) => {
                let mut sorted = items.clone();
                for window in sorted.windows(2) {
                    if compare_json_scalar(&window[0], &window[1]).is_none() {
                        return Err(
                            "sort() requires an array of numbers, strings, or booleans".into()
                        );
                    }
                }
                sorted.sort_by(|left, right| {
                    compare_json_scalar(left, right).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(Value::Array(sorted))
            }
            _ => Err("sort() requires an array".into()),
        },
        ("sum", [value]) => match value {
            Value::Array(items) => {
                let mut total = 0.0;
                for item in items {
                    total += as_number(item, "sum()")?;
                }
                number_value(total)
            }
            _ => Err("sum() requires an array".into()),
        },
        ("min", [value]) => match value {
            Value::Array(items) => items
                .iter()
                .map(|item| as_number(item, "min()"))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .reduce(f64::min)
                .map_or(Ok(Value::Null), number_value),
            _ => Err("min() requires an array".into()),
        },
        ("max", [value]) => match value {
            Value::Array(items) => items
                .iter()
                .map(|item| as_number(item, "max()"))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .reduce(f64::max)
                .map_or(Ok(Value::Null), number_value),
            _ => Err("max() requires an array".into()),
        },
        ("len", _) => Err("len() requires exactly one argument".into()),
        ("contains", _) => Err("contains() requires exactly two arguments".into()),
        ("empty", _) => Err("empty() requires exactly one argument".into()),
        ("default", _) => Err("default() requires exactly two arguments".into()),
        ("upper", _) => Err("upper() requires exactly one string".into()),
        ("lower", _) => Err("lower() requires exactly one string".into()),
        ("trim", _) => Err("trim() requires exactly one string".into()),
        ("starts_with", _) => Err("starts_with() requires exactly two strings".into()),
        ("ends_with", _) => Err("ends_with() requires exactly two strings".into()),
        ("replace", _) => Err("replace() requires exactly three strings".into()),
        ("split", _) => Err("split() requires exactly two strings".into()),
        ("join", _) => Err("join() requires an array and an optional separator".into()),
        ("first", _) => Err("first() requires exactly one array".into()),
        ("last", _) => Err("last() requires exactly one array".into()),
        ("keys", _) => Err("keys() requires exactly one object".into()),
        ("values", _) => Err("values() requires exactly one object".into()),
        ("reverse", _) => Err("reverse() requires exactly one array".into()),
        ("flatten", _) => Err("flatten() requires exactly one array".into()),
        ("unique", _) => Err("unique() requires exactly one array".into()),
        ("sort", _) => Err("sort() requires exactly one array".into()),
        ("sum", _) => Err("sum() requires exactly one array".into()),
        ("min", _) => Err("min() requires exactly one array".into()),
        ("max", _) => Err("max() requires exactly one array".into()),
        _ => Err(format!("unknown expression function `{name}`")),
    }
}

fn join_values(value: &Value, separator: &str) -> Result<Value, String> {
    match value {
        Value::Array(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(part) => parts.push(part.clone()),
                    _ => return Err("join() requires an array of strings".into()),
                }
            }
            Ok(Value::String(parts.join(separator)))
        }
        _ => Err("join() requires an array".into()),
    }
}

fn compare_json_scalar(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64()?.partial_cmp(&right.as_f64()?),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

pub(crate) fn as_number(value: &Value, operator: &str) -> Result<f64, String> {
    value
        .as_f64()
        .ok_or_else(|| format!("operator `{operator}` requires numbers"))
}

pub(crate) fn number_value(value: f64) -> Result<Value, String> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| "numeric expression result is not finite".to_string())
}
