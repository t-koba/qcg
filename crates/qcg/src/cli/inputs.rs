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
        let bytes = qcg_fs::read_bounded(&file, max_inputs_file_bytes)?;
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
        let bytes = qcg_fs::read_bounded(path, runtime.file_input_limit_bytes)?;
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
        let bytes = qcg_fs::read_bounded(&file, None)?;
        let parsed: BTreeMap<String, bool> = serde_json::from_slice(&bytes).with_context(|| {
            format!("failed to parse confirmations file `{file}` as a JSON id-to-boolean map")
        })?;
        // File-loaded ids are validated exactly like CLI pairs: a
        // shortened or legacy id in a file must not alias another scope
        // (Q1).
        for key in parsed.keys() {
            validate_confirmation_id(key).with_context(|| {
                format!("confirmations file `{file}` has an invalid confirmation id `{key}`")
            })?;
        }
        parsed
    } else {
        BTreeMap::new()
    };
    for pair in pairs {
        let (key, decision) = pair
            .split_once('=')
            .with_context(|| format!("confirm `{pair}` must be ID=approve|deny"))?;
        // Fail closed on malformed confirmation ids: accept only
        // `node:kind:64hex[:scope]` so a shortened or legacy 2-part id
        // cannot alias another approval scope (Q1).
        validate_confirmation_id(key)
            .with_context(|| format!("confirm `{pair}` has an invalid confirmation id"))?;
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

/// Validates a confirmation id as `node:kind:64hex-digest[:invocation-hash]`.
/// The 3-part form is content scope; the 4-part form is invocation scope and
/// its fourth element is the 64-hex invocation hash, never an arbitrary
/// scope word. Legacy 2-part ids are refused fail-closed (Q1).
/// Scope-vs-manifest correspondence is enforced server-side by exact id
/// match against the pending confirmation: an id of the wrong scope never
/// matches, so a mismatched pre-approval is refused on use, never
/// misapplied.
fn validate_confirmation_id(id: &str) -> anyhow::Result<()> {
    let parts: Vec<&str> = id.split(':').collect();
    if parts.len() != 3 && parts.len() != 4 {
        // Defense in depth: node and kind segments must never contain `:`.
        // Any extra colon means a colon-containing segment is attempting to
        // alias another scope, so refuse with an explicit colon message
        // rather than a generic shape error (Q1). Contract validation (G3)
        // already rejects colon-containing node ids; this CLI check mirrors
        // it so a crafted confirmation id fails here too.
        if id.matches(':').count() > 3 {
            anyhow::bail!("confirmation id node and kind must not contain ':'");
        }
        anyhow::bail!("confirmation id must be node:kind:digest[:invocation-hash]");
    }
    if parts[0].is_empty() || parts[1].is_empty() {
        anyhow::bail!("confirmation id node and kind must not be empty");
    }
    // Explicit colon rejection for defense in depth: after splitting no
    // segment should contain `:`, and an empty check above already covers
    // `::`. A crafted id with an embedded colon would have split into
    // extra parts and been refused above, but this pin keeps the invariant
    // explicit even if the split logic changes (Q1).
    if parts[0].contains(':') || parts[1].contains(':') {
        anyhow::bail!("confirmation id node and kind must not contain ':'");
    }
    for (position, part) in [(2, "digest"), (3, "invocation-hash")]
        .into_iter()
        .filter(|(index, _)| parts.len() > *index)
    {
        let value = parts[position];
        if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            anyhow::bail!("confirmation id {part} must be 64 hex characters");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_confirmation_id;
    #[test]
    fn confirmation_ids_reject_legacy_two_part_forms() {
        // Q1: only 3-part content and 4-part invocation ids are accepted,
        // and the fourth element is a 64-hex invocation hash, never a
        // scope word or a shortened value. Colon-containing node segments
        // are refused explicitly (defense in depth alongside contract G3).
        let digest = "a".repeat(64);
        let invocation = "b".repeat(64);
        assert!(validate_confirmation_id(&format!("node:kind:{digest}")).is_ok());
        assert!(validate_confirmation_id(&format!("node:kind:{digest}:{invocation}")).is_ok());
        assert!(
            validate_confirmation_id(&format!("node:kind:{digest}:inv")).is_err(),
            "a non-hex fourth element must be refused"
        );
        assert!(
            validate_confirmation_id("node:kind").is_err(),
            "legacy 2-part must be refused"
        );
        assert!(validate_confirmation_id("node:kind:short").is_err());
        assert!(validate_confirmation_id("node:kind:ZZZZ").is_err());
        // Colon-containing node segments alias scopes and must be refused
        // with an explicit colon message (Q1 defense in depth). Extra-colon
        // ids split into 5+ parts and hit the explicit branch; a 4-part id
        // with an embedded colon still fails closed via the digest check.
        let error = validate_confirmation_id(&format!("a:b:kind:{digest}:{invocation}"))
            .expect_err("colon-containing node segment must be refused");
        assert!(
            format!("{error:#}").contains("must not contain ':'"),
            "colon rejection must be explicit, got: {error:#}"
        );
        let error = validate_confirmation_id(&format!("node:kind:{digest}:{invocation}:extra"))
            .expect_err("extra colon segment must be refused");
        assert!(
            format!("{error:#}").contains("must not contain ':'"),
            "colon rejection must be explicit, got: {error:#}"
        );
        assert!(
            validate_confirmation_id(&format!("a:b:kind:{digest}")).is_err(),
            "colon-containing 4-part alias must still fail closed"
        );
        let error = validate_confirmation_id(
            "node::kind:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .expect_err("empty segment must be refused");
        assert!(
            format!("{error:#}").contains("must not"),
            "empty segment must be refused, got: {error:#}"
        );
    }

    #[test]
    fn confirmations_file_rejects_malformed_ids() {
        // Q1: file-loaded ids are validated exactly like CLI pairs.
        let dir = std::env::temp_dir().join(format!("qcg-confirm-file-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("conf.json")).expect("utf8 path");
        std::fs::write(path.as_std_path(), r#"{"node:kind": true}"#)
            .expect("file should be written");
        let error = super::load_confirmations(Vec::new(), Some(path.clone()))
            .expect_err("legacy id in file must be refused");
        assert!(
            format!("{error:#}").contains("invalid confirmation id"),
            "{error:#}"
        );
        let digest = "b".repeat(64);
        std::fs::write(
            path.as_std_path(),
            format!(r#"{{"node:kind:{digest}": true}}"#),
        )
        .expect("file should be written");
        let values =
            super::load_confirmations(Vec::new(), Some(path)).expect("valid file should load");
        assert_eq!(values.len(), 1);
    }
}
