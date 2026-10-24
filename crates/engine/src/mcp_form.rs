use api::FormSpec;
use contract::FieldType;
use serde_json::Value;

use crate::{StepError, required_input_field};

/// Builds a fresh empty form for resuming an MCP authorization prerequisite.
pub fn mcp_authorization_form_spec(title: String) -> FormSpec {
    FormSpec {
        id: format!("mcp-auth:{}", uuid::Uuid::now_v7()),
        title: format!(
            "Authorize {title} in Connections, then resume; authorization is rechecked."
        ),
        ..Default::default()
    }
}

/// Builds the user-facing form for an ordered set of MCP elicitation requests.
/// Request identity and ordering remain the caller's responsibility.
pub fn mcp_elicitation_form_spec(
    question_id: String,
    alias: &str,
    requests: &[(&String, &Value)],
) -> Result<FormSpec, StepError> {
    let mut fields = Vec::with_capacity(requests.len());
    for (index, (request_id, request)) in requests.iter().copied().enumerate() {
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
        // Only form-mode elicitation is supported. URL (out-of-band) mode
        // must not be coerced into an in-band form: its payload must never
        // pass through the client, and qcg has no external-navigation
        // approval path, so fail closed. Legacy requests without a mode are
        // treated as form, matching rmcp's LegacyForm wire handling.
        if let Some(mode) = params.get("mode").and_then(Value::as_str)
            && mode != "form"
        {
            return Err(StepError::failed(
                alias,
                format!(
                    "MCP input request `{request_id}` uses unsupported elicitation mode `{mode}`"
                ),
            ));
        }
        if params.get("url").is_some()
            || params.get("elicitationId").is_some()
            || params.get("elicitation_id").is_some()
        {
            return Err(StepError::failed(
                alias,
                format!(
                    "MCP input request `{request_id}` uses unsupported URL-mode elicitation; only form mode is supported"
                ),
            ));
        }
        let message = params
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("MCP tool requested structured input");
        fields.push(required_input_field(
            format!("response_{index}"),
            FieldType::Json,
            Some(message.to_string()),
            Some(message.to_string()),
            params.get("requestedSchema").cloned(),
        ));
    }
    Ok(FormSpec {
        id: question_id,
        title: format!("MCP tool `{alias}` requires input"),
        title_i18n: Default::default(),
        fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn elicitation_form_preserves_caller_order_and_schema() {
        let second_id = "request-z".to_string();
        let first_id = "request-a".to_string();
        let second = json!({
            "method": "elicitation/create",
            "params": { "message": "Second", "requestedSchema": { "type": "string" } }
        });
        let first = json!({
            "method": "elicitation/create",
            "params": { "message": "First", "requestedSchema": { "type": "object" } }
        });
        let requests = vec![(&second_id, &second), (&first_id, &first)];

        let form = mcp_elicitation_form_spec("question".into(), "server/tool", &requests)
            .expect("supported elicitation requests build a form");

        assert_eq!(form.id, "question");
        assert_eq!(form.title, "MCP tool `server/tool` requires input");
        assert_eq!(form.fields.len(), 2);
        assert_eq!(form.fields[0].id, "response_0");
        assert_eq!(form.fields[0].label.as_deref(), Some("Second"));
        assert_eq!(form.fields[0].schema, Some(json!({ "type": "string" })));
        assert!(form.fields[0].required);
        assert!(form.fields[1].required);
        assert_eq!(form.fields[1].id, "response_1");
        assert_eq!(form.fields[1].label.as_deref(), Some("First"));
        assert_eq!(form.fields[1].schema, Some(json!({ "type": "object" })));
    }

    #[test]
    fn elicitation_form_rejects_unsupported_shapes_with_alias_diagnostics() {
        for (request, message) in [
            (
                json!({ "method": "sampling/createMessage" }),
                "MCP input request `request-1` uses unsupported method `sampling/createMessage`",
            ),
            (
                json!({ "method": "elicitation/create", "params": { "mode": "url" } }),
                "MCP input request `request-1` uses unsupported elicitation mode `url`",
            ),
            (
                json!({ "method": "elicitation/create", "params": { "url": null } }),
                "MCP input request `request-1` uses unsupported URL-mode elicitation; only form mode is supported",
            ),
        ] {
            let id = "request-1".to_string();
            let requests = vec![(&id, &request)];
            let error = mcp_elicitation_form_spec("question".into(), "alias", &requests)
                .expect_err("unsupported elicitation shapes fail closed");
            assert_eq!(error.to_string(), format!("step `alias` failed: {message}"));
        }
    }
}
