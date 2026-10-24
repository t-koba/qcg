use crate::required_input_field;
use api::FormSpec;
use contract::NodeDef;
use contract::{FieldType, InputField};
use model::Finding;
use model::{FailureCode, FailureDetail};
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
        vec![required_input_field(
            "answer",
            FieldType::Text,
            Some("Resolution".into()),
            Some(format!(
                "Provide the resolution after {phase} attempts were exhausted"
            )),
            None,
        )]
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
