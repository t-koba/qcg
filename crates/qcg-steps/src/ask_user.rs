use async_trait::async_trait;
use qcg_api::FormSpec;
use qcg_contract::{Contract, FieldType, InputField, NodeDef, validate_form_values};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{params_schema, string_array_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead as _, Read as _, Write as _};

use super::common::require;
use qcg_policy::MAX_INTERACTIVE_INPUT_BYTES;

pub(crate) struct AskUserStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AskUserParams {
    content: String,
    #[serde(default)]
    content_i18n: BTreeMap<String, String>,
    #[serde(default)]
    options: Vec<String>,
    #[serde(default)]
    option_labels_i18n: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(default, rename = "default")]
    default_answer: Option<String>,
    #[serde(default)]
    fields: Vec<InputField>,
    /// Dotted path (for example `steps.design.output.input_fields`) whose
    /// array value supplies the form fields at run time. Mutually exclusive
    /// with static `fields`.
    #[serde(default)]
    fields_from: Option<String>,
}

#[async_trait]
impl StepExecutor for AskUserStep {
    fn type_id(&self) -> &'static str {
        "ask_user"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["content"],
            json!({
                "content": string_schema(),
                "content_i18n": { "type": "object", "additionalProperties": { "type": "string" } },
                "options": string_array_schema(),
                "option_labels_i18n": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    }
                },
                "default": string_schema(),
                "fields": { "type": "array", "items": { "type": "object" } },
                "fields_from": string_schema(),
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = ask_user_params(node)?;
        if !params.fields.is_empty() && params.fields_from.is_some() {
            return Err(StepError::failed(
                &node.id,
                "ask_user cannot combine `fields` with `fields_from`",
            ));
        }
        if let Some(default) = params.default_answer.as_deref() {
            if !params.fields.is_empty() || params.fields_from.is_some() {
                return Err(StepError::failed(
                    &node.id,
                    "ask_user `default` is only valid for scalar `options`",
                ));
            }
            if params.options.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "ask_user `default` requires non-empty `options`",
                ));
            }
            validate_answer(node, &params.options, default)?;
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let mut params = ask_user_params(node)?;
        if let Some(path) = &params.fields_from {
            let fields = ctx
                .vars
                .get_path(path)
                .ok_or_else(|| StepError::failed(&node.id, format!("`{path}` was not found")))?;
            let Value::Array(items) = fields else {
                return Err(StepError::failed(
                    &node.id,
                    format!("`{path}` must be an array of input fields"),
                ));
            };
            params.fields = items
                .iter()
                .map(|item| {
                    serde_json::from_value::<InputField>(item.clone()).map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("invalid input field in `{path}`: {error}"),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
        }
        let title = ctx.render_inline(node, &params.content)?;
        let title_i18n = params
            .content_i18n
            .iter()
            .map(|(language, content)| {
                ctx.render_inline(node, content)
                    .map(|rendered| (language.clone(), rendered))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if let Some(answer) = ctx.run.answers.get(&node.id) {
            if !params.fields.is_empty() {
                validate_form_values(&params.fields, answer, &ctx.run.contract.manifest.runtime)
                    .map_err(|error| {
                        StepError::failed(&node.id, format!("invalid form answer: {error}"))
                    })?;
                ctx.journal
                    .event(
                        "user_interaction",
                        json!({ "node": node.id, "source": "answer" }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                return Ok(StepOutcome::Success {
                    output: Some(answer.clone()),
                    files: vec![],
                });
            }
            let answer = answer
                .as_object()
                .and_then(|values| (values.len() == 1).then(|| values.get("answer")).flatten())
                .unwrap_or(answer);
            let answer = answer_to_string(node, answer)?;
            validate_answer(node, &params.options, &answer)?;
            ctx.journal
                .event(
                    "user_interaction",
                    json!({ "node": node.id, "source": "answer" }),
                )
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            return Ok(StepOutcome::Success {
                output: Some(Value::String(answer)),
                files: vec![],
            });
        }
        if !ctx.run.interactive {
            return Ok(StepOutcome::NeedsUser {
                question: FormSpec {
                    id: node.id.clone(),
                    title,
                    title_i18n,
                    fields: ask_user_fields(&params),
                },
            });
        }

        if !params.fields.is_empty() {
            return Err(StepError::failed(
                &node.id,
                "multi-field ask_user requires a form-capable client",
            ));
        }

        let answer = prompt_for_answer(
            node,
            &params.options,
            params.default_answer.as_deref(),
            &title,
        )?;
        ctx.journal
            .event("user_interaction", json!({ "node": node.id }))
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        Ok(StepOutcome::Success {
            output: Some(Value::String(answer)),
            files: vec![],
        })
    }
}

fn ask_user_params(node: &NodeDef) -> Result<AskUserParams, StepError> {
    let params: AskUserParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid ask_user params: {error}"))
    })?;
    require(node, Some(&params.content), "content")?;
    for field in &params.fields {
        if field.id.trim().is_empty() {
            return Err(StepError::failed(
                &node.id,
                "form field id must not be empty",
            ));
        }
        if matches!(field.kind, FieldType::Custom(_)) {
            return Err(StepError::failed(
                &node.id,
                format!("form field `{}` uses an unsupported custom type", field.id),
            ));
        }
    }
    Ok(params)
}

fn ask_user_fields(params: &AskUserParams) -> Vec<InputField> {
    if !params.fields.is_empty() {
        return params.fields.clone();
    }
    if params.options.is_empty() {
        vec![answer_field(FieldType::String, vec![])]
    } else {
        let mut field = answer_field(FieldType::Select, params.options.clone());
        field.option_labels_i18n = params.option_labels_i18n.clone();
        field.default = params.default_answer.clone().map(Value::String);
        vec![field]
    }
}

fn answer_field(kind: FieldType, options: Vec<String>) -> InputField {
    InputField {
        id: "answer".into(),
        label: None,
        label_i18n: BTreeMap::new(),
        description: None,
        description_i18n: BTreeMap::new(),
        placeholder: None,
        placeholder_i18n: BTreeMap::new(),
        kind,
        required: true,
        default: None,
        pattern: None,
        options,
        option_labels_i18n: BTreeMap::new(),
        min_items: None,
        item_type: None,
        schema: None,
        ui: Default::default(),
    }
}

fn prompt_for_answer(
    node: &NodeDef,
    options: &[String],
    default: Option<&str>,
    title: &str,
) -> Result<String, StepError> {
    eprintln!("{title}");
    if !options.is_empty() {
        eprintln!("Options:");
        for option in options {
            eprintln!("  - {option}");
        }
    }
    if let Some(default) = default {
        eprintln!("Default: {default}");
    }
    eprint!("> ");
    std::io::stderr()
        .flush()
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let answer = read_bounded_line(&mut std::io::stdin().lock(), MAX_INTERACTIVE_INPUT_BYTES)
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let answer = match (answer.trim(), default) {
        ("", Some(default)) => default.to_string(),
        (answer, _) => answer.to_string(),
    };
    validate_answer(node, options, &answer)?;
    Ok(answer)
}

fn read_bounded_line<R: std::io::BufRead>(
    reader: &mut R,
    max_bytes: usize,
) -> std::io::Result<String> {
    let read_limit = u64::try_from(max_bytes)
        .map_err(|_| std::io::Error::other("interactive input limit is too large"))?
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("interactive input limit is too large"))?;
    let mut bytes = Vec::with_capacity(max_bytes.min(8 * 1024));
    reader.take(read_limit).read_until(b'\n', &mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("interactive input exceeds {max_bytes} bytes"),
        ));
    }
    String::from_utf8(bytes).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "interactive input must be valid UTF-8",
        )
    })
}

fn answer_to_string(node: &NodeDef, value: &Value) -> Result<String, StepError> {
    match value {
        Value::String(answer) => Ok(answer.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(StepError::failed(
            &node.id,
            "answer must be a scalar JSON value",
        )),
    }
}

fn validate_answer(node: &NodeDef, options: &[String], answer: &str) -> Result<(), StepError> {
    if answer.is_empty() {
        return Err(StepError::failed(&node.id, "answer must not be empty"));
    }
    if !options.is_empty() && !options.iter().any(|option| option == answer) {
        return Err(StepError::failed(
            &node.id,
            format!("answer `{answer}` is outside declared options"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interactive_input_reader_accepts_exact_limit_and_rejects_larger_line() {
        let mut exact = std::io::Cursor::new(format!("{}\n", "x".repeat(8)));
        assert_eq!(
            read_bounded_line(&mut exact, 8).expect("exact limit should pass"),
            "xxxxxxxx"
        );
        let mut excessive = std::io::Cursor::new(format!("{}\n", "x".repeat(9)));
        let error = read_bounded_line(&mut excessive, 8)
            .expect_err("line above the limit must be rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn ask_user_preserves_localized_option_labels_without_changing_values() {
        let node: NodeDef = toml::from_str(
            r#"
id = "choose"
type = "ask_user"
[params]
content = "Choose a mode."
content_i18n = { ja = "モードを選択してください。" }
options = ["automatic", "manual"]
default = "automatic"
option_labels_i18n = { ja = { automatic = "自動", manual = "手動" } }
"#,
        )
        .unwrap();
        let params = ask_user_params(&node).unwrap();
        assert_eq!(
            params.content_i18n.get("ja").map(String::as_str),
            Some("モードを選択してください。")
        );
        let fields = ask_user_fields(&params);
        assert_eq!(fields[0].options, ["automatic", "manual"]);
        assert_eq!(fields[0].default, Some(json!("automatic")));
        assert_eq!(fields[0].option_labels_i18n["ja"]["automatic"], "自動");
    }

    #[test]
    fn ask_user_rejects_a_default_outside_options() {
        let node: NodeDef = toml::from_str(
            r#"
id = "choose"
type = "ask_user"
[params]
content = "Choose a mode."
options = ["automatic", "manual"]
default = "invalid"
"#,
        )
        .unwrap();
        let params = ask_user_params(&node).unwrap();
        let error = validate_answer(
            &node,
            &params.options,
            params.default_answer.as_deref().unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside declared options"));
    }
}
