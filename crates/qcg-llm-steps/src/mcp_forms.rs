use qcg_api::FormSpec;
use qcg_contract::{FieldType, InputField};
use qcg_engine::StepError;
use qcg_mcp::McpInputRequired;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(crate) fn mcp_question_id(
    node_id: &str,
    alias: &str,
    call_id: &str,
    args: &Value,
    required: &McpInputRequired,
) -> Result<String, StepError> {
    // Canonicalize before hashing, matching the continuation key: suspend
    // stores the redacted copy and resume recomputes from it, so raw args
    // here would fork the identity across the restart (E07/E08).
    // Identity scope (E08): unlike builtin AskUser (FULL unredacted hash),
    // MCP binds the REDACTED canonical form by design — a checkpointed
    // redacted copy must recompute the identical id without the original
    // secrets. Distinct secrets that redact to the same shape therefore
    // alias under one call id; callers must issue distinct call ids for
    // distinct secrets. Cross-run correlation of identical redacted shapes
    // is accepted on the same terms.
    // `serde_json::to_vec` on in-memory `Value` is infallible in practice;
    // on theoretical failure fail closed instead of hashing a shared
    // marker that would alias distinct failures onto one identity (E07).
    let args = crate::tool_events::canonical_mcp_key_args(args);
    let mut requests = Vec::with_capacity(required.input_requests.len());
    for (_request_id, request) in stable_mcp_input_requests(required) {
        requests.push(serde_json::to_vec(request).map_err(|error| {
            StepError::failed(
                node_id,
                format!("failed to serialize MCP request identity: {error}"),
            )
        })?);
    }
    let encoded = serde_json::to_vec(&json!({
        "node": node_id,
        "alias": alias,
        "call_id": call_id,
        "arguments": args,
        "requests": requests,
    }))
    .map_err(|error| {
        StepError::failed(
            node_id,
            format!("failed to serialize MCP question identity: {error}"),
        )
    })?;
    let digest = hex::encode(Sha256::digest(encoded));
    Ok(format!("{node_id}:mcp:{alias}:{digest}"))
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
            options_from: None,
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
    // Deterministic content order with an explicit request-id tiebreak
    // (E08): same-byte requests keep key order instead of an implicit
    // stable-sort tie. Serialization of `Value` is infallible in practice;
    // the failure marker below matches the identity-hash paths so a
    // theoretical failure sorts identically everywhere instead of aliasing
    // onto an empty key (E07/E08).
    requests.sort_by_key(|(request_id, request)| {
        (
            serde_json::to_vec(request)
                .unwrap_or_else(|_| Vec::from(b"{\"sort_key_serialization_failed\":true}")),
            (*request_id).clone(),
        )
    });
    requests
}

pub(crate) fn mcp_argument_summary(arguments: &Value) -> Value {
    let mut names = arguments
        .as_object()
        .map(|values| values.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    names.sort();
    // The approval binds the full argument value through its digest, not
    // just names and size: approving one call must never authorize a
    // regenerated different payload. This summary hash is display-only
    // (unsalted, for stable UI rendering); the binding digest in the
    // operation record is separately salted per operation (E09).
    // Serialization of `Value` is infallible in practice; on theoretical
    // failure the marker below is display-only (never an identity key), so
    // aliasing there cannot complete a foreign question or resume a foreign
    // continuation (E07). Identity paths fail closed via `Result` instead.
    let bytes = serde_json::to_vec(arguments)
        .unwrap_or_else(|_| Vec::from(b"{\"serialization_failed\":true}"));
    json!({
        "argument_names": names,
        "encoded_bytes": bytes.len(),
        "arguments_sha256": hex::encode(Sha256::digest(&bytes)),
    })
}
