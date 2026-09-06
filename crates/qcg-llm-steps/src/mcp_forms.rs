use qcg_api::FormSpec;
use qcg_contract::{FieldType, InputField};
use qcg_engine::StepError;
use qcg_mcp::McpInputRequired;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(crate) fn mcp_question_id(node_id: &str, alias: &str, required: &McpInputRequired) -> String {
    let requests = stable_mcp_input_requests(required)
        .into_iter()
        .map(|(_request_id, request)| serde_json::to_vec(request).unwrap_or_default())
        .collect::<Vec<_>>();
    let encoded = serde_json::to_vec(&json!({
        "alias": alias,
        "requests": requests,
    }))
    .unwrap_or_default();
    let digest = hex::encode(Sha256::digest(encoded));
    format!("{node_id}:mcp:{alias}:{}", &digest[..16])
}

pub(crate) fn mcp_form_spec(
    question_id: String,
    alias: &str,
    required: &McpInputRequired,
) -> Result<FormSpec, StepError> {
    let mut fields = Vec::with_capacity(required.input_requests.len());
    for (index, (request_id, request)) in
        stable_mcp_input_requests(required).into_iter().enumerate()
    {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method != "elicitation/create" {
            return Err(StepError::failed(
                alias,
                format!("MCP input request `{request_id}` uses unsupported method `{method}`"),
            ));
        }
        let params = request.get("params").unwrap_or(request);
        let message = params
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("MCP tool requested structured input");
        let label = params
            .get("url")
            .and_then(Value::as_str)
            .map_or_else(|| message.to_string(), |url| format!("{message} ({url})"));
        fields.push(InputField {
            id: format!("response_{index}"),
            label: Some(label),
            label_i18n: Default::default(),
            description: Some(message.to_string()),
            description_i18n: Default::default(),
            placeholder: None,
            placeholder_i18n: Default::default(),
            kind: FieldType::Json,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: Default::default(),
            min_items: None,
            item_type: None,
            schema: params.get("requestedSchema").cloned(),
            ui: Default::default(),
        });
    }
    Ok(FormSpec {
        id: question_id,
        title: format!("MCP tool `{alias}` requires input"),
        title_i18n: Default::default(),
        fields,
    })
}

pub(crate) fn mcp_input_responses(
    required: &McpInputRequired,
    answer: &Value,
) -> Result<BTreeMap<String, Value>, StepError> {
    let answers = answer
        .as_object()
        .ok_or_else(|| StepError::failed("mcp", "MCP input-required answer must be an object"))?;
    stable_mcp_input_requests(required)
        .into_iter()
        .enumerate()
        .map(|(index, (request_id, _request))| {
            let field = format!("response_{index}");
            let content = answers
                .get(&field)
                .cloned()
                .ok_or_else(|| StepError::failed("mcp", format!("MCP answer omitted `{field}`")))?;
            Ok((
                request_id.clone(),
                json!({ "action": "accept", "content": content }),
            ))
        })
        .collect()
}

pub(crate) fn stable_mcp_input_requests(required: &McpInputRequired) -> Vec<(&String, &Value)> {
    let mut requests = required.input_requests.iter().collect::<Vec<_>>();
    requests.sort_by_key(|(_request_id, request)| serde_json::to_vec(request).unwrap_or_default());
    requests
}

pub(crate) fn mcp_argument_summary(arguments: &Value) -> Value {
    let mut names = arguments
        .as_object()
        .map(|values| values.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    names.sort();
    json!({
        "argument_names": names,
        "encoded_bytes": serde_json::to_vec(arguments).map_or(0, |value| value.len()),
    })
}
