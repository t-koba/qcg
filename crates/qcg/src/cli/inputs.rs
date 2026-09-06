use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn load_inputs(
    pairs: Vec<String>,
    file: Option<Utf8PathBuf>,
    file_pairs: Vec<String>,
    runtime: &qcg_contract::RuntimeLimits,
    max_inputs_file_bytes: Option<usize>,
) -> Result<BTreeMap<String, Value>> {
    let mut values = if let Some(file) = file {
        let bytes = qcg_policy::read_bounded(&file, max_inputs_file_bytes)?;
        serde_json::from_slice::<BTreeMap<String, Value>>(&bytes)?
    } else {
        BTreeMap::new()
    };
    for pair in pairs {
        let (key, value) = pair
            .split_once('=')
            .with_context(|| format!("input `{pair}` must be k=v"))?;
        values.insert(key.to_string(), parse_value(value));
    }
    for pair in file_pairs {
        let (field, path) = pair
            .split_once('=')
            .with_context(|| format!("file input `{pair}` must be field=path"))?;
        if field.is_empty() {
            anyhow::bail!("file input field must not be empty");
        }
        let path = Utf8Path::new(path);
        let name = path
            .file_name()
            .with_context(|| format!("file input path `{path}` has no file name"))?;
        let bytes = qcg_policy::read_bounded(path, runtime.file_input_limit_bytes)?;
        let value = qcg_types::FileValue::from_bytes_optional_limit(
            name,
            &bytes,
            runtime.file_input_limit_bytes,
        )
        .with_context(|| format!("invalid file input `{field}`"))?;
        values.insert(field.to_string(), serde_json::to_value(value)?);
    }
    Ok(values)
}

pub(crate) fn load_answers(pairs: Vec<String>) -> Result<BTreeMap<String, Value>> {
    let mut values = BTreeMap::new();
    for pair in pairs {
        let (key, value) = pair
            .split_once('=')
            .with_context(|| format!("answer `{pair}` must be k=v"))?;
        values.insert(key.to_string(), parse_value(value));
    }
    Ok(values)
}

pub(crate) fn load_confirmations(
    pairs: Vec<String>,
    file: Option<Utf8PathBuf>,
) -> Result<BTreeMap<String, bool>> {
    let mut values = if let Some(file) = file {
        let bytes = qcg_policy::read_bounded(&file, None)?;
        serde_json::from_slice::<BTreeMap<String, bool>>(&bytes).with_context(|| {
            format!("failed to parse confirmations file `{file}` as a JSON id-to-boolean map")
        })?
    } else {
        BTreeMap::new()
    };
    for pair in pairs {
        let (key, decision) = pair
            .split_once('=')
            .with_context(|| format!("confirm `{pair}` must be ID=approve|deny"))?;
        let approved = match decision.to_ascii_lowercase().as_str() {
            "approve" | "approved" | "true" | "yes" | "1" => true,
            "deny" | "denied" | "false" | "no" | "0" => false,
            _ => anyhow::bail!("confirm `{pair}` must be ID=approve|deny"),
        };
        values.insert(key.to_string(), approved);
    }
    Ok(values)
}

fn parse_value(value: &str) -> Value {
    if let Ok(json) = serde_json::from_str(value) {
        return json;
    }
    if value == "true" {
        return Value::Bool(true);
    }
    if value == "false" {
        return Value::Bool(false);
    }
    if let Ok(number) = value.parse::<i64>() {
        return Value::Number(number.into());
    }
    Value::String(value.to_string())
}
