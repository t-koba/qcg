use qcg_api::FormSpec;
use qcg_contract::NodeDef;
use qcg_contract::{FieldType, InputField};
use qcg_types::Finding;
use qcg_types::{FailureCode, FailureDetail};
use serde_json::Value;

pub(crate) enum RepairCycleOutcome {
    Repaired { output: Option<Value> },
    Routed { to: String, output: Value },
    Answered { output: Value },
    Failed { reason: FailureDetail },
}

pub(crate) fn exhausted_question(
    node: &NodeDef,
    phase: &str,
    title: Option<&str>,
    fields: &[InputField],
) -> FormSpec {
    let fields = if fields.is_empty() {
        vec![InputField {
            id: "answer".into(),
            label: Some("Resolution".into()),
            label_i18n: Default::default(),
            description: Some(format!(
                "Provide the resolution after {phase} attempts were exhausted"
            )),
            description_i18n: Default::default(),
            placeholder: None,
            placeholder_i18n: Default::default(),
            kind: FieldType::Text,
            required: true,
            default: None,
            pattern: None,
            options: Vec::new(),
            option_labels_i18n: Default::default(),
            min_items: None,
            item_type: None,
            schema: None,
            ui: Default::default(),
        }]
    } else {
        fields.to_vec()
    };
    FormSpec {
        id: format!("{}:{phase}_exhausted", node.id),
        title: title
            .map(str::to_string)
            .unwrap_or_else(|| format!("Resolve exhausted {phase} for node `{}`", node.id)),
        title_i18n: Default::default(),
        fields,
    }
}

pub(crate) fn failure_from_findings(findings: &[Finding], code: FailureCode) -> FailureDetail {
    let message = findings
        .iter()
        .map(|finding| finding.message.as_str())
        .filter(|message| !message.is_empty())
        .collect::<Vec<_>>()
        .join("; ");
    FailureDetail::new(
        code,
        if message.is_empty() {
            "check failed".into()
        } else {
            message
        },
    )
}
