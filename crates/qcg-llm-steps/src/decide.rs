use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use qcg_contract::{Contract, ModelRef, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_llm::{DecisionQuestion, DecisionRequest, LlmRuntime};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DecideParams {
    model: ModelRef,
    state: Option<Value>,
    state_from: Option<String>,
    questions: BTreeMap<String, DecisionQuestion>,
    max_tokens: u32,
}

pub(crate) struct LlmDecideStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}

impl LlmDecideStep {
    fn validated_params(
        &self,
        node: &NodeDef,
        contract: &Contract,
    ) -> Result<DecideParams, StepError> {
        if !node.context.is_empty() {
            return Err(StepError::failed(
                &node.id,
                "llm.decide does not support node.context",
            ));
        }
        let value = node.params_json();
        if contains_template(&value) {
            return Err(StepError::failed(
                &node.id,
                "llm.decide does not support templates",
            ));
        }
        for field in ["input_cost_per_million_usd", "output_cost_per_million_usd"] {
            if let Some(price) = value.get("model").and_then(|model| model.get(field))
                && price
                    .as_f64()
                    .is_none_or(|price| !price.is_finite() || price < 0.0)
            {
                return Err(StepError::failed(
                    &node.id,
                    format!("model.{field} must be finite and non-negative"),
                ));
            }
        }
        let params: DecideParams = serde_json::from_value(value).map_err(|error| {
            StepError::failed(&node.id, format!("invalid llm.decide params: {error}"))
        })?;
        if params.max_tokens == 0 {
            return Err(StepError::failed(
                &node.id,
                "max_tokens must be greater than zero",
            ));
        }
        let state = match (&params.state, &params.state_from) {
            (Some(state), None) => state.clone(),
            (None, Some(path)) if valid_state_path(path) => json!({}),
            (None, Some(_)) => {
                return Err(StepError::failed(
                    &node.id,
                    "state_from must be a dotted ValueBag path",
                ));
            }
            _ => {
                return Err(StepError::failed(
                    &node.id,
                    "exactly one of state or state_from is required",
                ));
            }
        };
        params.request(node, state)?;
        if contract.manifest.budget.max_cost_usd.is_some()
            && (params.model.input_cost_per_million_usd.is_none()
                || params.model.output_cost_per_million_usd.is_none())
        {
            return Err(StepError::failed(
                &node.id,
                "llm.decide model must declare input and output pricing when budget.max_cost_usd is set",
            ));
        }
        let provider = &params.model.provider;
        if self.runtime.provider.capabilities_for(provider).is_none() {
            return Err(StepError::failed(
                &node.id,
                format!("LLM provider `{provider}` is not registered"),
            ));
        }
        if let Some(error) = self.runtime.provider.configuration_error_for(provider) {
            return Err(StepError::failed(&node.id, error));
        }
        if !self.runtime.provider.supports_decisions_for(provider) {
            return Err(StepError::failed(
                &node.id,
                format!("LLM provider `{provider}` does not support decisions"),
            ));
        }
        Ok(params)
    }
}

impl DecideParams {
    fn request(&self, node: &NodeDef, state: Value) -> Result<DecisionRequest, StepError> {
        let request = DecisionRequest {
            provider: self.model.provider.clone(),
            model: self.model.model.clone(),
            state,
            questions: self.questions.clone(),
        };
        request
            .validate()
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        Ok(request)
    }
}

fn contains_template(value: &Value) -> bool {
    match value {
        Value::String(text) => template_text(text),
        Value::Array(values) => values.iter().any(contains_template),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| template_text(key) || contains_template(value)),
        _ => false,
    }
}

fn template_text(text: &str) -> bool {
    ["{{", "{%", "{#"]
        .iter()
        .any(|marker| text.contains(marker))
}

fn valid_state_path(path: &str) -> bool {
    if path
        .split('.')
        .any(|part| part.is_empty() || part.chars().any(char::is_whitespace))
    {
        return false;
    }
    let mut parts = path.split('.');
    match parts.next() {
        Some("inputs") => parts.next().is_some(),
        Some("steps") => {
            parts.next().is_some() && matches!(parts.next(), Some("output" | "status"))
        }
        Some("item") => true,
        _ => false,
    }
}

fn question_schema(kind: &str, criteria: Value, required_criteria: bool) -> Value {
    let required = if required_criteria {
        vec!["type", "instructions", "criteria"]
    } else {
        vec!["type", "instructions"]
    };
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": {
            "type": {"const": kind},
            "instructions": {"$ref": "#/$defs/content"},
            "criteria": criteria
        }
    })
}

#[async_trait]
impl StepExecutor for LlmDecideStep {
    fn type_id(&self) -> &'static str {
        "llm.decide"
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn params_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["model", "questions", "max_tokens"],
            "oneOf": [{"required": ["state"]}, {"required": ["state_from"]}],
            "$defs": {
                "text": {"type": "string", "not": {"pattern": "\\{\\{|\\{%|\\{#"}},
                "literal": {
                    "anyOf": [
                        {"$ref": "#/$defs/text"},
                        {"type": ["null", "boolean", "number"]},
                        {"type": "array", "items": {"$ref": "#/$defs/literal"}},
                        {
                            "type": "object",
                            "propertyNames": {"$ref": "#/$defs/text"},
                            "additionalProperties": {"$ref": "#/$defs/literal"}
                        }
                    ]
                },
                "content": {
                    "type": ["string", "object", "array"],
                    "allOf": [{"$ref": "#/$defs/literal"}]
                }
            },
            "properties": {
                "model": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["provider", "model"],
                    "properties": {
                        "provider": {"$ref": "#/$defs/text", "pattern": "\\S"},
                        "model": {"$ref": "#/$defs/text", "pattern": "\\S"},
                        "input_cost_per_million_usd": {"type": "number", "minimum": 0},
                        "output_cost_per_million_usd": {"type": "number", "minimum": 0}
                    }
                },
                "state": {"$ref": "#/$defs/content"},
                "state_from": {
                    "$ref": "#/$defs/text",
                    "pattern": "^(inputs\\.[^.\\s]+(\\.[^.\\s]+)*|steps\\.[^.\\s]+\\.(output|status)(\\.[^.\\s]+)*|item(\\.[^.\\s]+)*)$",
                    "description": "ValueBag path resolved at execution without template rendering."
                },
                "questions": {
                    "type": "object",
                    "minProperties": 1,
                    "propertyNames": {"$ref": "#/$defs/text"},
                    "additionalProperties": {
                        "oneOf": [
                            question_schema("noul", json!({
                                "type": ["object", "null"],
                                "additionalProperties": false,
                                "properties": {
                                    "true": {"$ref": "#/$defs/text"},
                                    "false": {"$ref": "#/$defs/text"}
                                }
                            }), false),
                            question_schema("choice", json!({
                                "type": "object",
                                "minProperties": 2,
                                "maxProperties": 255,
                                "propertyNames": {"$ref": "#/$defs/text"},
                                "additionalProperties": {"anyOf": [{"$ref": "#/$defs/text"}, {"type": "null"}]}
                            }), true),
                            question_schema("score", json!({
                                "type": "array",
                                "minItems": 2,
                                "maxItems": 255,
                                "items": {"$ref": "#/$defs/text"}
                            }), true)
                        ]
                    }
                },
                "max_tokens": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": u32::MAX,
                    "description": "Local total input plus output usage ceiling, not sent upstream. Independent of [llm].max_tokens, which limits chat output. No [llm] chat controls are inherited; run-wide budgets remain enforced."
                }
            }
        }))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        self.validated_params(node, contract).map(|_| ())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = self.validated_params(node, &ctx.run.contract)?;
        let state = match (&params.state, &params.state_from) {
            (Some(state), None) => state.clone(),
            (None, Some(path)) => ctx.vars.get_path(path).cloned().ok_or_else(|| {
                StepError::failed(&node.id, "state_from path does not resolve to a value")
            })?,
            _ => {
                return Err(StepError::failed(
                    &node.id,
                    "exactly one of state or state_from is required",
                ));
            }
        };
        let request = params.request(node, state)?;
        let gateway = ctx
            .llm
            .as_ref()
            .ok_or_else(|| StepError::failed(&node.id, "LLM gateway is not configured"))?;
        let response = gateway
            .decide(node, request, &params.model, params.max_tokens)
            .await?;
        Ok(StepOutcome::Success {
            output: Some(serde_json::to_value(response)?),
            files: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcg_contract::{ContextRef, StepType, ValueBag};
    use qcg_engine::StepRegistry;
    use qcg_llm::LlmRouter;

    struct Fixture {
        root: std::path::PathBuf,
        contract: Contract,
        step: LlmDecideStep,
    }

    impl Fixture {
        fn new(llm: &str) -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "qcg-decide-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ));
            std::fs::create_dir(&root).expect("fixture directory should be created");
            std::fs::write(
                root.join("qcg.toml"),
                format!(
                    r#"
[generator]
id = "decide-test"
name = "Decision Test"
version = "0.1.0"
qcg_version = "^0.1"
{llm}
[[flow]]
id = "decide"
type = "llm.decide"
[flow.params]
model = {{ provider = "decisions", model = "decision-model" }}
state = {{ ready = true }}
max_tokens = 100
questions = {{ accept = {{ type = "noul", instructions = "Accept?" }} }}
"#
                ),
            )
            .expect("fixture manifest should be written");
            let contract =
                Contract::load(camino::Utf8PathBuf::from_path_buf(root.clone()).unwrap())
                    .expect("decision contract should load");
            let runtime = LlmRouter::parse_text(
                r#"
[default]
model = { provider = "chat", model = "not-a-decision-model" }
[[provider]]
id = "decisions"
api = "system_one"
base_url = "https://example.invalid/v1"
[[provider]]
id = "chat"
api = "chat_completions"
base_url = "https://example.invalid/v1"
"#,
            )
            .expect("real provider registry should parse")
            .into_runtime();
            Self {
                root,
                contract,
                step: LlmDecideStep {
                    runtime: Arc::new(runtime),
                },
            }
        }

        fn node(&self, params: Value) -> NodeDef {
            let mut node = self.contract.manifest.flow[0].clone();
            node.params = serde_json::from_value(params).expect("params should convert to TOML");
            node
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).expect("fixture directory should be removed");
        }
    }

    fn params() -> Value {
        json!({
            "model": {"provider": "decisions", "model": "decision-model"},
            "state": {"ready": true},
            "questions": {"accept": {"type": "noul", "instructions": "Accept?"}},
            "max_tokens": 100
        })
    }

    #[test]
    fn registers_and_validates_without_chat_configuration_or_network() {
        let fixture = Fixture::new("");
        assert!(fixture.contract.manifest.llm.is_none());
        let mut registry = StepRegistry::new();
        crate::register_llm_steps(&mut registry, Arc::clone(&fixture.step.runtime));
        assert!(registry.get(&StepType::from("llm.decide")).is_some());
        registry
            .validate_contract(&fixture.contract)
            .expect("decision should validate");
    }

    #[test]
    fn decision_total_ceiling_is_independent_of_global_chat_output_controls() {
        let fixture = Fixture::new(
            r#"
[llm]
model = { provider = "chat", model = "chat-model" }
max_tokens = 1
temperature = 0.5
seed = 7
system = "Chat only"
"#,
        );
        fixture
            .step
            .validate(&fixture.node(params()), &fixture.contract)
            .expect("decision should not inherit chat controls or chat output ceiling");
    }

    #[test]
    fn rejects_missing_required_fields_and_chat_params() {
        let fixture = Fixture::new("");
        for field in ["model", "questions", "max_tokens"] {
            let mut value = params();
            value.as_object_mut().unwrap().remove(field);
            assert!(
                fixture
                    .step
                    .validate(&fixture.node(value), &fixture.contract)
                    .is_err(),
                "missing {field} must fail"
            );
        }
        for field in [
            "prompt",
            "context",
            "request",
            "temperature",
            "seed",
            "system",
            "tools",
            "media",
            "fallback_models",
            "schema",
            "stream",
            "max_tokens_total",
            "unknown",
        ] {
            let mut value = params();
            value[field] = json!({});
            assert!(
                fixture
                    .step
                    .validate(&fixture.node(value), &fixture.contract)
                    .is_err(),
                "chat field {field} must fail"
            );
        }
        let mut node = fixture.node(params());
        node.context.push(ContextRef::Short("inputs.*".into()));
        let error = fixture.step.validate(&node, &fixture.contract).unwrap_err();
        assert!(error.to_string().contains("node.context"));
    }

    #[test]
    fn rejects_invalid_params_and_unsupported_or_missing_providers() {
        let fixture = Fixture::new("");
        for (path, replacement) in [
            ("/max_tokens", json!(0)),
            ("/max_tokens", json!(-1)),
            ("/max_tokens", json!(1.5)),
            ("/max_tokens", json!(u64::from(u32::MAX) + 1)),
            ("/max_tokens", json!("100")),
            ("/model/provider", json!("missing")),
            ("/model/provider", json!("chat")),
            ("/model/provider", json!(" ")),
            ("/model/model", json!("")),
            ("/state", json!(true)),
            ("/questions", json!({})),
            ("/questions/accept/type", json!("unknown")),
            ("/questions/accept/instructions", json!(10)),
        ] {
            let mut value = params();
            *value.pointer_mut(path).unwrap() = replacement;
            assert!(
                fixture
                    .step
                    .validate(&fixture.node(value), &fixture.contract)
                    .is_err(),
                "invalid {path} must fail"
            );
        }
        for (provider, expected) in [
            ("missing", "not registered"),
            ("chat", "does not support decisions"),
        ] {
            let mut value = params();
            value["model"]["provider"] = json!(provider);
            let error = fixture
                .step
                .validate(&fixture.node(value), &fixture.contract)
                .unwrap_err();
            assert!(error.to_string().contains(expected));
        }
    }

    #[test]
    fn rejects_templates_in_routing_state_paths_questions_and_nested_content() {
        let fixture = Fixture::new("");
        for marker in ["{{ value }}", "{% if value %}", "{# value #}"] {
            for path in [
                "/model/provider",
                "/model/model",
                "/state",
                "/questions/accept/instructions",
            ] {
                let mut value = params();
                *value.pointer_mut(path).unwrap() = json!(marker);
                assert!(
                    fixture
                        .step
                        .validate(&fixture.node(value), &fixture.contract)
                        .is_err()
                );
            }
            let mut value = params();
            value["state"] = json!({"nested": [{marker: "value"}]});
            assert!(
                fixture
                    .step
                    .validate(&fixture.node(value), &fixture.contract)
                    .is_err()
            );
            let mut value = params();
            value.as_object_mut().unwrap().remove("state");
            value["state_from"] = json!(marker);
            assert!(
                fixture
                    .step
                    .validate(&fixture.node(value), &fixture.contract)
                    .is_err()
            );
        }
    }

    #[test]
    fn requires_exactly_one_state_source_and_validates_resolved_state() {
        let fixture = Fixture::new("");
        let mut value = params();
        value["state_from"] = json!("inputs.data");
        assert!(
            fixture
                .step
                .validate(&fixture.node(value.clone()), &fixture.contract)
                .is_err()
        );
        value.as_object_mut().unwrap().remove("state");
        let node = fixture.node(value.clone());
        let parsed = fixture
            .step
            .validated_params(&node, &fixture.contract)
            .expect("path should validate without data");
        let bag = ValueBag::with_inputs(BTreeMap::from([(
            "data".into(),
            json!({"nested": ["ready"]}),
        )]));
        let state = bag
            .get_path(parsed.state_from.as_deref().unwrap())
            .unwrap()
            .clone();
        assert_eq!(parsed.request(&node, state.clone()).unwrap().state, state);
        assert!(parsed.request(&node, json!(null)).is_err());
        assert!(parsed.request(&node, json!(false)).is_err());
        for path in [
            "inputs.data.nested.0",
            "steps.previous.output.value",
            "item",
            "item.value",
        ] {
            value["state_from"] = json!(path);
            fixture
                .step
                .validate(&fixture.node(value.clone()), &fixture.contract)
                .expect("valid path should pass preflight");
        }
        for path in [
            "",
            "inputs",
            "inputs..data",
            "inputs.data.",
            "steps.previous",
            "steps.previous.value",
            "unknown.data",
            "inputs. data",
        ] {
            value["state_from"] = json!(path);
            assert!(
                fixture
                    .step
                    .validate(&fixture.node(value.clone()), &fixture.contract)
                    .is_err()
            );
        }
        value.as_object_mut().unwrap().remove("state_from");
        assert!(
            fixture
                .step
                .validate(&fixture.node(value), &fixture.contract)
                .is_err()
        );
    }

    #[test]
    fn rejects_invalid_pricing_and_requires_explicit_prices_for_cost_budget() {
        let mut fixture = Fixture::new("");
        for field in ["input_cost_per_million_usd", "output_cost_per_million_usd"] {
            for price in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                let mut node = fixture.node(params());
                node.params
                    .get_mut("model")
                    .unwrap()
                    .as_table_mut()
                    .unwrap()
                    .insert(field.into(), price.into());
                assert!(
                    fixture.step.validate(&node, &fixture.contract).is_err(),
                    "invalid {field} must fail"
                );
            }
        }
        fixture.contract.manifest.budget.max_cost_usd = Some(1.0);
        assert!(
            fixture
                .step
                .validate(&fixture.node(params()), &fixture.contract)
                .is_err()
        );
        let mut value = params();
        value["model"]["input_cost_per_million_usd"] = json!(0.0);
        assert!(
            fixture
                .step
                .validate(&fixture.node(value.clone()), &fixture.contract)
                .is_err()
        );
        value["model"]["output_cost_per_million_usd"] = json!(2.0);
        fixture
            .step
            .validate(&fixture.node(value), &fixture.contract)
            .expect("explicit pricing should validate");
    }

    #[test]
    fn schema_and_question_validation_accept_only_primitive_question_shapes() {
        let fixture = Fixture::new("");
        let schema = fixture.step.params_schema().unwrap();
        let validator = jsonschema::validator_for(&schema).expect("decision schema should compile");
        for question in [
            json!({"type": "noul", "instructions": "Accept?"}),
            json!({"type": "noul", "instructions": {}, "criteria": {"true": "Yes", "false": "No"}}),
            json!({"type": "choice", "instructions": [], "criteria": {"a": "A", "b": null}}),
            json!({"type": "score", "instructions": ["Rate"], "criteria": ["Low", "High"]}),
        ] {
            let mut value = params();
            value["questions"]["accept"] = question;
            assert!(validator.is_valid(&value));
            let parsed: DecideParams = serde_json::from_value(value).unwrap();
            parsed
                .request(&fixture.contract.manifest.flow[0], json!({}))
                .expect("primitive question should validate");
        }
        for question in [
            json!({"type": "noul", "instructions": "Accept?", "criteria": {"yes": "Yes"}}),
            json!({"type": "noul", "instructions": "Accept?", "criteria": {"true": null}}),
            json!({"type": "noul", "instructions": "Accept?", "extra": true}),
            json!({"type": "choice", "instructions": "Pick", "criteria": {"a": "A"}}),
            json!({"type": "score", "instructions": "Rate", "criteria": ["Only"]}),
            json!({"type": "score", "instructions": false, "criteria": ["Low", "High"]}),
        ] {
            let mut value = params();
            value["questions"]["accept"] = question;
            assert!(!validator.is_valid(&value));
            if let Ok(parsed) = serde_json::from_value::<DecideParams>(value) {
                assert!(
                    parsed
                        .request(&fixture.contract.manifest.flow[0], json!({}))
                        .is_err()
                );
            }
        }
        for count in [2, 255, 256] {
            for kind in ["choice", "score"] {
                let criteria = if kind == "choice" {
                    json!(
                        (0..count)
                            .map(|index| (index.to_string(), Value::Null))
                            .collect::<BTreeMap<_, _>>()
                    )
                } else {
                    json!(
                        (0..count)
                            .map(|index| index.to_string())
                            .collect::<Vec<_>>()
                    )
                };
                let mut value = params();
                value["questions"]["accept"] =
                    json!({"type": kind, "instructions": "Decide", "criteria": criteria});
                assert_eq!(validator.is_valid(&value), count <= 255);
                let parsed: DecideParams = serde_json::from_value(value).unwrap();
                assert_eq!(
                    parsed
                        .request(&fixture.contract.manifest.flow[0], json!({}))
                        .is_ok(),
                    count <= 255
                );
            }
        }
    }
}
