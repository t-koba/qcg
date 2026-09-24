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
    /// Dotted run-variable path supplying scalar options at run time. Mutually
    /// exclusive with static `options`.
    #[serde(default)]
    options_from: Option<String>,
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
                "options_from": string_schema(),
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
        if params.options_from.is_some() && !params.options.is_empty() {
            return Err(StepError::failed(
                &node.id,
                "ask_user cannot combine `options` with `options_from`",
            ));
        }
        if params.options_from.is_some() && params.default_answer.is_some() {
            return Err(StepError::failed(
                &node.id,
                "ask_user `default` cannot be validated against dynamic `options_from`",
            ));
        }
        if params
            .options_from
            .as_deref()
            .is_some_and(|path| path.trim().is_empty())
        {
            return Err(StepError::failed(
                &node.id,
                "ask_user `options_from` must not be empty",
            ));
        }
        for field in &params.fields {
            if field.options_from.is_some() && !field.options.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "ask_user field `{}` cannot combine `options` with `options_from`",
                        field.id
                    ),
                ));
            }
            if field
                .options_from
                .as_deref()
                .is_some_and(|path| path.trim().is_empty())
            {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "ask_user field `{}` options_from must not be empty",
                        field.id
                    ),
                ));
            }
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
        // Dynamic options resolve from durable run variables before any
        // answer check or form rendering, so validation, interactive
        // prompts, and the frozen FormSpec all observe the same choices.
        resolve_dynamic_options(ctx.vars, node, &mut params)?;
        let title = ctx.render_inline(node, &params.content)?;
        let title_i18n = params
            .content_i18n
            .iter()
            .map(|(language, content)| {
                ctx.render_inline(node, content)
                    .map(|rendered| (language.clone(), rendered))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        // E08f: the question identity binds the full resolved content
        // (content, options, fields). A bare node id would let an answer
        // for one question satisfy a different question after a contract
        // edit; only the full content-bound id is honored, never the bare
        // node id. Resolved options are folded in so dynamic `options_from`
        // changes separate identities.
        let fields_for_id = ask_user_fields(&params);
        let question_id = ask_user_step_question_id(&node.id, &title, &title_i18n, &fields_for_id)?;
        // E09d: the displayed title keeps the full question, but any
        // journaled copy must not carry raw secrets. Titles are redacted
        // for credential assignments and URLs before display truncation,
        // and over-long titles journal as hash plus shape only.
        let title = redacted_ask_user_title(&title);
        let title_i18n = title_i18n
            .into_iter()
            .map(|(language, content)| (language, redacted_ask_user_title(&content)))
            .collect::<BTreeMap<_, _>>();
        if let Some(answer) = ctx.run.answers.get(&question_id) {
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
                    id: question_id,
                    title,
                    title_i18n,
                    fields: fields_for_id,
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

fn resolve_dynamic_options(
    vars: &qcg_contract::ValueBag,
    node: &NodeDef,
    params: &mut AskUserParams,
) -> Result<(), StepError> {
    if let Some(path) = params.options_from.clone() {
        let (options, labels, _) = read_option_source(vars, node, &path, "answer")?;
        params.options = options;
        params.option_labels_i18n = labels;
    }
    for field in &mut params.fields {
        let Some(path) = field.options_from.clone() else {
            continue;
        };
        let (options, labels, plain_labels) = read_option_source(vars, node, &path, &field.id)?;
        field.options = options;
        field.option_labels_i18n = labels;
        if !plain_labels.is_empty() {
            let labels = plain_labels
                .into_iter()
                .map(|(value, label)| (value, Value::String(label)))
                .collect::<serde_json::Map<_, _>>();
            field
                .ui
                .insert("option_labels".to_string(), Value::Object(labels));
        }
    }
    Ok(())
}

/// Values, per-language labels, and plain labels read from an option source.
type ResolvedOptionSource = (
    Vec<String>,
    BTreeMap<String, BTreeMap<String, String>>,
    BTreeMap<String, String>,
);

/// Reads a dotted run-variable path holding an option array. Accepted entry
/// shapes are plain strings and `{ value, label?, label_i18n? }` objects.
/// Empty results fail closed: a selection field with nothing to select is a
/// configuration error, not an unconstrained text box.
fn read_option_source(
    vars: &qcg_contract::ValueBag,
    node: &NodeDef,
    path: &str,
    field_id: &str,
) -> Result<ResolvedOptionSource, StepError> {
    let value = vars.get_path(path).ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!("options_from `{path}` was not found for field `{field_id}`"),
        )
    })?;
    let items = value.as_array().ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!("options_from `{path}` must be an array for field `{field_id}`"),
        )
    })?;
    let mut options = Vec::new();
    let mut labels = BTreeMap::new();
    let mut plain_labels = BTreeMap::new();
    for item in items {
        match item {
            Value::String(text) => options.push(text.clone()),
            Value::Object(object) => {
                let Some(text) = object.get("value").and_then(Value::as_str) else {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "options_from `{path}` entry for field `{field_id}` must contain a string `value`"
                        ),
                    ));
                };
                options.push(text.to_string());
                if let Some(label) = object.get("label").and_then(Value::as_str) {
                    plain_labels.insert(text.to_string(), label.to_string());
                }
                if let Some(map) = object.get("label_i18n").and_then(Value::as_object) {
                    for (language, label) in map {
                        if let Some(label) = label.as_str() {
                            labels
                                .entry(language.clone())
                                .or_insert_with(BTreeMap::new)
                                .insert(text.to_string(), label.to_string());
                        }
                    }
                }
            }
            _ => {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "options_from `{path}` entries for field `{field_id}` must be strings or objects"
                    ),
                ));
            }
        }
    }
    if options.is_empty() {
        return Err(StepError::failed(
            &node.id,
            format!("options_from `{path}` resolved no options for field `{field_id}`"),
        ));
    }
    Ok((options, labels, plain_labels))
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
        options_from: None,
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

/// Content-bound question identity for single-shot steps (E08f). The id
/// folds the rendered content, localized contents, and the fully resolved
/// fields (which already embed resolved options, defaults, and labels), so
/// a contract edit or a different `options_from` resolution yields a
/// different id and the old answer never satisfies the new question. The
/// id is always `node:ask_user:<hex>`; the bare node id is never honored.
pub(crate) fn ask_user_step_question_id(
    node_id: &str,
    content: &str,
    content_i18n: &BTreeMap<String, String>,
    fields: &[InputField],
) -> Result<String, StepError> {
    use sha2::Digest as _;
    let canonical = serde_json::to_vec(&serde_json::json!({
        "content": content,
        "content_i18n": content_i18n,
        "fields": fields,
    }))
    .map_err(|error| {
        StepError::failed(
            node_id,
            format!("ask_user question is not serializable: {error}"),
        )
    })?;
    let mut hasher = sha2::Sha256::new();
    hasher.update(node_id.as_bytes());
    hasher.update([0]);
    hasher.update(canonical);
    Ok(format!(
        "{node_id}:ask_user:{}",
        hex::encode(hasher.finalize())
    ))
}

/// Redacts a question title for display and journaling (E09d). Credential
/// assignments and URL secrets are stripped first; over-long titles are
/// truncated to 256 characters plus a hash of the full text so the journal
/// carries shape plus hash, never raw secrets.
pub(crate) fn redacted_ask_user_title(title: &str) -> String {
    let redacted =
        qcg_policy::redact_credential_assignments_in_text(&qcg_policy::redact_urls_in_text(title));
    const LIMIT: usize = 256;
    if redacted.len() <= LIMIT {
        return redacted;
    }
    use sha2::Digest as _;
    let digest = hex::encode(sha2::Sha256::digest(redacted.as_bytes()));
    let mut end = LIMIT.min(redacted.len());
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...[sha256:{digest}]", &redacted[..end])
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
content_i18n = { fr = "Choose a mode (fr)." }
options = ["automatic", "manual"]
default = "automatic"
option_labels_i18n = { fr = { automatic = "Auto (fr)", manual = "Manual (fr)" } }
"#,
        )
        .unwrap();
        let params = ask_user_params(&node).unwrap();
        assert_eq!(
            params.content_i18n.get("fr").map(String::as_str),
            Some("Choose a mode (fr).")
        );
        let fields = ask_user_fields(&params);
        assert_eq!(fields[0].options, ["automatic", "manual"]);
        assert_eq!(fields[0].default, Some(json!("automatic")));
        assert_eq!(fields[0].option_labels_i18n["fr"]["automatic"], "Auto (fr)");
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

    #[test]
    fn options_from_resolves_scalar_and_field_options() {
        let scalar: NodeDef = toml::from_str(
            r#"
id = "select"
type = "ask_user"
[params]
content = "Select a provider."
options_from = "steps.list.output.options"
"#,
        )
        .unwrap();
        let mut params = ask_user_params(&scalar).unwrap();
        let mut vars = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
        vars.set_step_output("list", json!({ "options": ["openai", "anthropic"] }));
        resolve_dynamic_options(&vars, &scalar, &mut params).expect("scalar options resolve");
        assert_eq!(params.options, ["openai", "anthropic"]);

        let fields: NodeDef = toml::from_str(
            r#"
id = "select"
type = "ask_user"
[params]
content = "Select a model."
fields = [{ id = "model", type = "select", options_from = "steps.list.output.options" }]
"#,
        )
        .unwrap();
        let mut params = ask_user_params(&fields).unwrap();
        let mut vars = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
        vars.set_step_output(
            "list",
            json!({
                "options": [
                    { "value": "gpt-5", "label": "GPT-5", "label_i18n": { "ja": "GPT-5 (ja)" } },
                    "gpt-5-mini"
                ]
            }),
        );
        resolve_dynamic_options(&vars, &fields, &mut params).expect("field options resolve");
        assert_eq!(params.fields[0].options, ["gpt-5", "gpt-5-mini"]);
        assert_eq!(
            params.fields[0].option_labels_i18n["ja"]["gpt-5"],
            "GPT-5 (ja)"
        );
        assert_eq!(params.fields[0].ui["option_labels"]["gpt-5"], "GPT-5");
    }

    #[test]
    fn options_from_fails_closed_on_missing_or_empty_sources() {
        let node: NodeDef = toml::from_str(
            r#"
id = "select"
type = "ask_user"
[params]
content = "Select a model."
options_from = "steps.list.output.options"
"#,
        )
        .unwrap();
        let mut params = ask_user_params(&node).unwrap();
        let empty = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
        let error = resolve_dynamic_options(&empty, &node, &mut params)
            .expect_err("missing path must fail");
        assert!(error.to_string().contains("was not found"), "{error}");

        let mut vars = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
        vars.set_step_output("list", json!({ "options": [] }));
        let mut params = ask_user_params(&node).unwrap();
        let error = resolve_dynamic_options(&vars, &node, &mut params)
            .expect_err("empty options must fail");
        assert!(error.to_string().contains("resolved no options"), "{error}");
    }

    #[test]
    fn step_question_id_binds_content_and_options() {
        // E08f: the single-shot id folds content and resolved options;
        // different content or options separate, restarts recompute stably,
        // and the bare node id is never the identity.
        let fields_a = vec![answer_field(FieldType::String, vec![])];
        let fields_b = vec![answer_field(
            FieldType::Select,
            vec!["a".into(), "b".into()],
        )];
        let first = ask_user_step_question_id("node", "Content A?", &BTreeMap::new(), &fields_a)
            .expect("id should build");
        let same = ask_user_step_question_id("node", "Content A?", &BTreeMap::new(), &fields_a)
            .expect("restart must recompute the same id");
        assert_eq!(first, same, "restart must be stable");
        assert_ne!(first, "node", "bare node id must never be the identity");
        let different_content =
            ask_user_step_question_id("node", "Content B?", &BTreeMap::new(), &fields_a)
                .expect("id should build");
        assert_ne!(first, different_content, "content change must separate");
        let different_options =
            ask_user_step_question_id("node", "Content A?", &BTreeMap::new(), &fields_b)
                .expect("id should build");
        assert_ne!(first, different_options, "option change must separate");
    }

    #[test]
    fn step_title_redaction_removes_secrets_and_bounds_length() {
        // E09d: titles strip credential assignments and URL secrets, and
        // over-long titles journal as truncated hash plus shape only.
        let redacted = redacted_ask_user_title(
            "Deploy with api_key=s3cret at https://example.test/?token=abc",
        );
        assert!(!redacted.contains("s3cret"), "{redacted}");
        assert!(!redacted.contains("abc"), "{redacted}");
        assert!(redacted.contains("api_key="), "{redacted}");
        let long = "x".repeat(400);
        let redacted = redacted_ask_user_title(&long);
        assert!(redacted.len() < long.len(), "long titles must truncate");
        assert!(redacted.contains("sha256:"), "{redacted}");
    }

    struct TempDir(std::path::PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn test_journal(run_id: &str) -> (TempDir, qcg_engine::JournalWriter, camino::Utf8PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "qcg-ask-user-{run_id}-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("test directory should be creatable");
        let root =
            camino::Utf8PathBuf::from_path_buf(path.clone()).expect("temporary path must be UTF-8");
        let journal_path = root.join("journal.jsonl");
        let journal = qcg_engine::JournalWriter::create(&journal_path, run_id, false, None)
            .expect("test journal should open");
        (TempDir(path), journal, journal_path)
    }

    fn journal_event_count(path: &camino::Utf8PathBuf) -> usize {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
    }

    #[test]
    fn invalid_ask_user_form_writes_zero_journal_events() {
        // Gap 3 (plain step): `AskUserStep::execute` validates params and
        // fields before any `user_interaction` journal write. This drives
        // the exact validation prefix (`ask_user_params`, which checks
        // empty ids and custom types) with a real journal open, proving an
        // invalid form errors with zero new journal events. Real objects,
        // no mocks.
        let (_dir, journal, journal_path) = test_journal("invalid-form");
        assert_eq!(journal_event_count(&journal_path), 0);
        // Empty field id fails before any journal write.
        let empty_id: NodeDef = toml::from_str(
            r#"
id = "choose"
type = "ask_user"
[params]
content = "Choose."
fields = [{ id = "", type = "string" }]
"#,
        )
        .unwrap();
        let error = ask_user_params(&empty_id).expect_err("empty field id must fail");
        assert!(error.to_string().contains("must not be empty"), "{error}");
        // Custom field type fails closed.
        let custom: NodeDef = toml::from_str(
            r#"
id = "choose"
type = "ask_user"
[params]
content = "Choose."
fields = [{ id = "a", type = "custom-thing" }]
"#,
        )
        .unwrap();
        let error = ask_user_params(&custom).expect_err("custom type must fail");
        assert!(
            error.to_string().contains("unsupported custom type"),
            "{error}"
        );
        // Conflicting fields/fields_from fails before journaling.
        let conflict: NodeDef = toml::from_str(
            r#"
id = "choose"
type = "ask_user"
[params]
content = "Choose."
fields = [{ id = "a", type = "string" }]
fields_from = "steps.x.output.fields"
"#,
        )
        .unwrap();
        let step = AskUserStep;
        let contract = empty_contract();
        let error = step
            .validate(&conflict, &contract)
            .expect_err("fields conflict must fail");
        assert!(error.to_string().contains("cannot combine"), "{error}");
        assert_eq!(
            journal_event_count(&journal_path),
            0,
            "invalid forms must write zero journal events"
        );
        assert_eq!(
            journal.state().operation_records.len(),
            0,
            "validation must not create operation records"
        );
        let _ = journal;
    }

    fn empty_contract() -> Contract {
        use qcg_contract::{
            AssetSpec, FailurePolicy, GeneratorMeta, Graph, InputSpec, Manifest, OutputSpec,
            RetentionPolicy,
        };
        let manifest = Manifest {
            generator: GeneratorMeta {
                id: "test".into(),
                name: "Test".into(),
                version: "0.1.0".into(),
                description: String::new(),
                authors: vec![],
                qcg_version: String::new(),
            },
            permissions: Default::default(),
            llm: None,
            inputs: InputSpec::default(),
            resources: Default::default(),
            tools: Default::default(),
            secrets: Default::default(),
            runtime: Default::default(),
            budget: Default::default(),
            flow: vec![],
            parallel: vec![],
            blocks: Default::default(),
            outputs: OutputSpec { extras: vec![] },
            failure: FailurePolicy::default(),
            retention: RetentionPolicy::default(),
            audit: qcg_policy::AuditConfig::default(),
            hooks: Default::default(),
            assets: AssetSpec::default(),
            dependencies: Default::default(),
        };
        Contract {
            root: camino::Utf8PathBuf::from("test"),
            graph: Graph::build(&manifest).expect("empty graph should build"),
            manifest,
            sha256: "test".into(),
        }
    }
}
