use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::provider::{DecisionInputWrapper, DecisionResponseWrapper};
use crate::{LlmError, TokenUsage};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionRequest {
    pub provider: String,
    pub model: String,
    pub state: Value,
    pub questions: BTreeMap<String, DecisionQuestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionQuestion {
    Noul {
        instructions: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        criteria: Option<BTreeMap<String, String>>,
    },
    Choice {
        instructions: Value,
        criteria: BTreeMap<String, Option<String>>,
    },
    Score {
        instructions: Value,
        criteria: Vec<String>,
    },
}

impl DecisionRequest {
    pub fn payload(&self) -> Value {
        self.payload_for(true, DecisionInputWrapper::None)
    }

    /// Mechanism mapping from the logical decision onto the provider's HTTP
    /// JSON body. `model_in_body` and `input_wrapper` come from the provider
    /// registry row, never from the generator contract: direct TypeSafe
    /// gateways use `(true, None)`, Cloudflare `POST .../ai/run` uses
    /// `(true, Input)`, and Cloudflare `POST .../ai/run/{model}` uses
    /// `(false, None)`.
    pub fn payload_for(&self, model_in_body: bool, input_wrapper: DecisionInputWrapper) -> Value {
        let inner = json!({"state": self.state, "questions": self.questions});
        match input_wrapper {
            DecisionInputWrapper::None => {
                let mut body = json!({"state": self.state, "questions": self.questions});
                if model_in_body {
                    body["model"] = json!(self.model);
                }
                body
            }
            DecisionInputWrapper::Input => {
                let mut body = json!({"input": inner});
                if model_in_body {
                    body["model"] = json!(self.model);
                }
                body
            }
        }
    }

    pub fn validate(&self) -> Result<(), LlmError> {
        if self.provider.trim().is_empty()
            || self.model.trim().is_empty()
            || self.questions.is_empty()
        {
            return Err(LlmError::new(
                "Decision provider, model and questions must not be empty",
            ));
        }
        if !valid_content(&self.state) {
            return Err(LlmError::new(
                "Decision state must be a string, object or array",
            ));
        }
        for question in self.questions.values() {
            let instructions = match question {
                DecisionQuestion::Noul {
                    instructions,
                    criteria,
                } => {
                    if criteria.as_ref().is_some_and(|criteria| {
                        criteria.keys().any(|key| key != "true" && key != "false")
                    }) {
                        return Err(LlmError::new(
                            "Decision noul criteria keys must be true or false",
                        ));
                    }
                    instructions
                }
                DecisionQuestion::Choice {
                    instructions,
                    criteria,
                } => {
                    if !(2..=255).contains(&criteria.len()) {
                        return Err(LlmError::new(
                            "Decision choice requires between 2 and 255 options",
                        ));
                    }
                    instructions
                }
                DecisionQuestion::Score {
                    instructions,
                    criteria,
                } => {
                    if !(2..=255).contains(&criteria.len()) {
                        return Err(LlmError::new(
                            "Decision score requires between 2 and 255 levels",
                        ));
                    }
                    instructions
                }
            };
            if !valid_content(instructions) {
                return Err(LlmError::new(
                    "Decision instructions must be a string, object or array",
                ));
            }
        }
        Ok(())
    }
}

fn valid_content(value: &Value) -> bool {
    matches!(value, Value::String(_) | Value::Object(_) | Value::Array(_))
}

/// Mechanism mapping from the provider's HTTP JSON body back onto the typed
/// decision. `wrapper` comes from the registry row: `Direct` accepts only
/// the top-level TypeSafe shape, `Result` requires the Cloudflare
/// `{"result":{...}}` envelope (honoring `"success": false` as an upstream
/// failure without echoing the body), and `Auto` accepts both so one row
/// works against direct and Cloudflare-compatible gateways.
pub fn parse_decision_response(
    value: Value,
    wrapper: DecisionResponseWrapper,
) -> Result<DecisionResponse, LlmError> {
    match wrapper {
        DecisionResponseWrapper::Direct => serde_json::from_value(value)
            .map_err(|_| LlmError::invalid_response("invalid System One response shape")),
        DecisionResponseWrapper::Result => parse_result_envelope(value),
        DecisionResponseWrapper::Auto => {
            // Prefer the direct shape so a top-level answer never hides
            // behind a coincidental `result` field; fall back to the
            // Cloudflare envelope when direct parsing fails.
            match serde_json::from_value::<DecisionResponse>(value.clone()) {
                Ok(response) => Ok(response),
                Err(_) => parse_result_envelope(value),
            }
        }
    }
}

fn parse_result_envelope(value: Value) -> Result<DecisionResponse, LlmError> {
    let result = value
        .get("result")
        .cloned()
        .ok_or_else(|| LlmError::invalid_response("invalid System One response shape"))?;
    if value.get("success").and_then(Value::as_bool) == Some(false) {
        let detail = value
            .get("errors")
            .and_then(Value::as_array)
            .map(|errors| {
                errors
                    .iter()
                    .take(3)
                    .filter_map(|error| {
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .or_else(|| error.as_str())
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .filter(|detail| !detail.is_empty())
            .map(|detail| {
                const LIMIT: usize = 500;
                if detail.len() > LIMIT {
                    format!("{}…", &detail[..LIMIT])
                } else {
                    detail
                }
            })
            .map(|detail| format!(": {detail}"))
            .unwrap_or_default();
        return Err(LlmError::new(format!(
            "decision provider reported failure{detail}"
        )));
    }
    serde_json::from_value(result)
        .map_err(|_| LlmError::invalid_response("invalid System One response shape"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionResponse {
    pub model: String,
    pub answers: BTreeMap<String, DecisionAnswer>,
    pub usage: DecisionUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnswer {
    Noul {
        noul: f64,
    },
    Choice {
        choice: String,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
    Score {
        score: f64,
        legend: BTreeMap<String, String>,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
}

impl DecisionResponse {
    pub fn validate(&self, request: &DecisionRequest) -> Result<(), LlmError> {
        request.validate()?;
        if self.model.trim().is_empty() || !self.answers.keys().eq(request.questions.keys()) {
            return Err(LlmError::invalid_response(
                "Decision model must not be empty and answer IDs must match question IDs",
            ));
        }
        for (id, question) in &request.questions {
            match (question, &self.answers[id]) {
                (DecisionQuestion::Noul { .. }, DecisionAnswer::Noul { noul }) => {
                    if !unit_interval(*noul) {
                        return Err(LlmError::invalid_response(
                            "Decision noul must be finite and between 0 and 1",
                        ));
                    }
                }
                (
                    DecisionQuestion::Choice { criteria, .. },
                    DecisionAnswer::Choice {
                        choice,
                        probabilities,
                        confidence,
                    },
                ) => {
                    validate_probabilities(probabilities, *confidence)?;
                    if !probabilities.keys().eq(criteria.keys())
                        || probabilities.get(choice).is_none_or(|selected| {
                            probabilities.values().any(|value| value > selected)
                        })
                    {
                        return Err(LlmError::invalid_response(
                            "Decision choice must select a maximum probability option with exactly the declared criteria",
                        ));
                    }
                }
                (
                    DecisionQuestion::Score { criteria, .. },
                    DecisionAnswer::Score {
                        score,
                        legend,
                        probabilities,
                        confidence,
                    },
                ) => {
                    validate_probabilities(probabilities, *confidence)?;
                    if legend.len() != criteria.len()
                        || criteria
                            .iter()
                            .enumerate()
                            .any(|(index, label)| legend.get(&index.to_string()) != Some(label))
                        || !probabilities.keys().eq(legend.keys())
                        || !score.is_finite()
                        || !(0.0..=(criteria.len() - 1) as f64).contains(score)
                    {
                        return Err(LlmError::invalid_response(
                            "Decision score and legend must match the declared levels",
                        ));
                    }
                }
                _ => {
                    return Err(LlmError::invalid_response(
                        "Decision answer kind must match question kind",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn token_usage(&self) -> TokenUsage {
        TokenUsage {
            input: self.usage.input_tokens,
            output: self.usage.output_tokens,
            ..TokenUsage::default()
        }
    }
}

fn unit_interval(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

fn validate_probabilities(
    probabilities: &BTreeMap<String, f64>,
    confidence: f64,
) -> Result<(), LlmError> {
    if !unit_interval(confidence)
        || probabilities.values().any(|value| !unit_interval(*value))
        || (probabilities.values().sum::<f64>() - 1.0).abs() > 1e-5
    {
        return Err(LlmError::invalid_response(
            "Decision probabilities must be finite, between 0 and 1 and sum to 1; confidence must be between 0 and 1",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> DecisionRequest {
        serde_json::from_value(json!({
            "provider": "typesafe", "model": "jev-latest", "state": {"text": "Help"},
            "questions": {
                "urgent": {"type": "noul", "instructions": "Is this urgent?"},
                "team": {"type": "choice", "instructions": "Choose a team", "criteria": {"billing": null, "support": "Support"}},
                "rating": {"type": "score", "instructions": ["Rate urgency"], "criteria": ["Low", "High"]}
            }
        })).unwrap()
    }

    fn response() -> Value {
        json!({"model": "jev-1.13.0", "usage": {"input_tokens": 312, "output_tokens": 48}, "answers": {
            "urgent": {"type": "noul", "noul": 0.92},
            "team": {"type": "choice", "choice": "support", "probabilities": {"billing": 0.1, "support": 0.9}, "confidence": 0.8},
            "rating": {"type": "score", "score": 0.7, "legend": {"0": "Low", "1": "High"}, "probabilities": {"0": 0.3, "1": 0.7}, "confidence": 0.6}
        }})
    }

    #[test]
    fn wire_format_preserves_typed_answers_and_usage() {
        let request = request();
        request.validate().unwrap();
        assert!(request.payload().get("provider").is_none());
        assert!(request.payload().get("max_tokens").is_none());
        let value = response();
        let response: DecisionResponse = serde_json::from_value(value.clone()).unwrap();
        response.validate(&request).unwrap();
        assert_eq!(serde_json::to_value(&response).unwrap(), value);
        assert_eq!(response.token_usage().input, 312);
        assert_eq!(response.token_usage().output, 48);
    }

    #[test]
    fn payload_envelopes_cover_direct_and_workers_ai_transports() {
        use crate::provider::{DecisionInputWrapper, DecisionResponseWrapper};
        let request = request();
        // Direct TypeSafe shape (default).
        let direct = request.payload_for(true, DecisionInputWrapper::None);
        assert_eq!(direct["model"], json!("jev-latest"));
        assert_eq!(direct["state"], json!({"text": "Help"}));
        assert!(direct["questions"].is_object());
        assert!(direct.get("input").is_none());
        // Cloudflare `POST .../ai/run`: model in body, inputs under `input`.
        let wrapped = request.payload_for(true, DecisionInputWrapper::Input);
        assert_eq!(wrapped["model"], json!("jev-latest"));
        assert_eq!(wrapped["input"]["state"], json!({"text": "Help"}));
        assert!(wrapped["input"]["questions"].is_object());
        assert!(wrapped.get("state").is_none());
        // Cloudflare `POST .../ai/run/{model}`: model in path only.
        let bare = request.payload_for(false, DecisionInputWrapper::None);
        assert!(bare.get("model").is_none());
        assert!(bare["state"].is_object());
        let bare_wrapped = request.payload_for(false, DecisionInputWrapper::Input);
        assert!(bare_wrapped.get("model").is_none());
        assert!(bare_wrapped["input"]["state"].is_object());
        // Response envelopes: direct, Cloudflare `result`, and auto.
        let direct_response = response();
        let parsed =
            parse_decision_response(direct_response.clone(), DecisionResponseWrapper::Direct)
                .expect("direct response should parse");
        assert_eq!(parsed.model, "jev-1.13.0");
        let enveloped =
            json!({"result": direct_response, "success": true, "errors": [], "messages": []});
        let parsed = parse_decision_response(enveloped, DecisionResponseWrapper::Result)
            .expect("result envelope should parse");
        assert_eq!(parsed.model, "jev-1.13.0");
        // Auto accepts both shapes.
        let parsed = parse_decision_response(direct_response, DecisionResponseWrapper::Auto)
            .expect("auto should accept direct");
        assert_eq!(parsed.model, "jev-1.13.0");
        let enveloped =
            json!({"result": response(), "success": true, "errors": [], "messages": []});
        let parsed = parse_decision_response(enveloped, DecisionResponseWrapper::Auto)
            .expect("auto should accept result envelope");
        assert_eq!(parsed.model, "jev-1.13.0");
        // Strict wrappers reject the other shape.
        assert!(
            parse_decision_response(response(), DecisionResponseWrapper::Result).is_err(),
            "result wrapper must reject a direct response"
        );
        assert!(
            parse_decision_response(
                json!({"result": response(), "success": true}),
                DecisionResponseWrapper::Direct
            )
            .is_err(),
            "direct wrapper must reject a result envelope"
        );
        // `success: false` fails without echoing the upstream body.
        let failed = parse_decision_response(
            json!({"result": response(), "success": false, "errors": [{"message": "overloaded"}]}),
            DecisionResponseWrapper::Auto,
        )
        .expect_err("failed envelope must not parse");
        assert!(failed.to_string().contains("reported failure"));
    }

    #[test]
    fn rejects_invalid_requests() {
        for (pointer, replacement) in [
            ("/state", json!(true)),
            ("/model", json!("")),
            ("/provider", json!("")),
            ("/questions", json!({})),
            ("/questions/urgent/instructions", json!(null)),
            ("/questions/urgent/criteria", json!({"yes": "True"})),
            ("/questions/team/criteria", json!({"only": null})),
            ("/questions/rating/criteria", json!(["only"])),
        ] {
            let mut value = serde_json::to_value(request()).unwrap();
            if pointer.ends_with("/criteria") && value.pointer(pointer).is_none() {
                value["questions"]["urgent"]["criteria"] = replacement;
            } else {
                *value.pointer_mut(pointer).unwrap() = replacement;
            }
            let parsed: DecisionRequest = serde_json::from_value(value).unwrap();
            assert!(
                parsed.validate().is_err(),
                "invalid request accepted: {pointer}"
            );
        }
    }

    #[test]
    fn rejects_invalid_answers_and_usage() {
        for (pointer, replacement) in [
            ("/model", json!("")),
            ("/answers", json!({})),
            ("/answers/urgent/noul", json!(1.1)),
            ("/answers/urgent", json!({"noul": 0.1})),
            ("/answers/team/choice", json!("billing")),
            ("/answers/team/confidence", json!(-0.1)),
            (
                "/answers/team/probabilities",
                json!({"billing": 0.2, "support": 0.9}),
            ),
            (
                "/answers/team/probabilities",
                json!({"billing": 0.1, "other": 0.9}),
            ),
            ("/answers/rating/score", json!(2)),
            ("/answers/rating/legend/0", json!("Wrong")),
            ("/answers/rating/probabilities/0", json!(-0.1)),
            ("/usage", json!({"input_tokens": 10})),
            ("/usage/input_tokens", json!(-1)),
        ] {
            let mut value = response();
            *value.pointer_mut(pointer).unwrap() = replacement;
            if let Ok(parsed) = serde_json::from_value::<DecisionResponse>(value) {
                assert!(
                    parsed.validate(&request()).is_err(),
                    "invalid response accepted: {pointer}"
                );
            }
        }
    }

    #[test]
    fn rejects_nonfinite_numbers_and_accepts_ties() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut parsed: DecisionResponse = serde_json::from_value(response()).unwrap();
            parsed
                .answers
                .insert("urgent".into(), DecisionAnswer::Noul { noul: value });
            assert!(parsed.validate(&request()).is_err());
        }
        let mut value = response();
        value["answers"]["team"]["probabilities"] = json!({"billing": 0.5, "support": 0.5});
        let parsed: DecisionResponse = serde_json::from_value(value).unwrap();
        parsed.validate(&request()).unwrap();
    }
}
