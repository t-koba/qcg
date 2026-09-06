use qcg_api::FormSpec;
use qcg_contract::{FailureAction, FailureKind, NodeDef};
use qcg_contract::{FieldType, InputField};
use qcg_engine::{ResultExt, StepContext, StepError};
use serde_json::{Value, json};

pub(crate) enum OutOfContractDecision {
    Continue(Value),
    NeedsUser { question: FormSpec },
}

pub(crate) fn enforce_out_of_contract_policy(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    mut value: Value,
) -> Result<OutOfContractDecision, StepError> {
    let Some(object) = value.as_object_mut() else {
        return Ok(OutOfContractDecision::Continue(value));
    };
    if object.get("out_of_contract").and_then(Value::as_bool) != Some(true) {
        return Ok(OutOfContractDecision::Continue(value));
    }
    let reason = object
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("LLM response was marked out of contract")
        .to_string();
    let policy = node
        .failure
        .as_ref()
        .unwrap_or(&ctx.run.contract.manifest.failure)
        .action(FailureKind::OutOfContract);
    ctx.journal
        .event(
            "out_of_contract",
            json!({ "node": node.id, "policy": format!("{policy:?}").to_lowercase(), "reason": reason }),
        )
        .step_err(&node.id)?;
    match policy {
        FailureAction::Reject => Err(StepError::failed(
            &node.id,
            format!("LLM response was rejected as out of contract: {reason}"),
        )),
        FailureAction::Fail => Err(StepError::failed(
            &node.id,
            format!("LLM response was out of contract: {reason}"),
        )),
        FailureAction::Clarify => Ok(OutOfContractDecision::NeedsUser {
            question: FormSpec {
                id: node.id.clone(),
                title: "Clarify out-of-contract LLM response".into(),
                title_i18n: Default::default(),
                fields: vec![InputField {
                    id: "answer".into(),
                    label: None,
                    label_i18n: Default::default(),
                    description: None,
                    description_i18n: Default::default(),
                    placeholder: None,
                    placeholder_i18n: Default::default(),
                    kind: FieldType::Text,
                    required: true,
                    default: Some(Value::String(reason)),
                    pattern: None,
                    options: vec![],
                    option_labels_i18n: Default::default(),
                    min_items: None,
                    item_type: None,
                    schema: None,
                    ui: Default::default(),
                }],
            },
        }),
        FailureAction::Clamp => {
            object.remove("out_of_contract");
            object.remove("reason");
            Ok(OutOfContractDecision::Continue(value))
        }
    }
}
