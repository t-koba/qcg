use async_trait::async_trait;
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_llm::LlmRuntime;
use qcg_policy::{params_schema, string_array_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

use crate::policy::validate_capability_names;
use crate::validation::has_capability;

/// `llm.catalog` exposes the selectable provider/model/effort catalog as run
/// data. Contracts bind the emitted `options` (or `entries`) through
/// `ask_user.options_from`, so form choices stay data-driven without any
/// catalog knowledge in the form engine.
pub(crate) struct LlmCatalogStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CatalogSelect {
    Providers,
    Models,
    Efforts,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogParams {
    select: CatalogSelect,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    model: Option<String>,
    /// Hide entries declared with `enabled = false`.
    #[serde(default = "default_enabled_only")]
    enabled_only: bool,
    /// Optional capability filter (for example `tool_use`).
    #[serde(default)]
    require: Vec<String>,
}

fn default_enabled_only() -> bool {
    true
}

#[async_trait]
impl StepExecutor for LlmCatalogStep {
    fn type_id(&self) -> &'static str {
        "llm.catalog"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["select"],
            json!({
                "select": { "enum": ["providers", "models", "efforts"] },
                "provider": string_schema(),
                "model": string_schema(),
                "enabled_only": { "type": "boolean" },
                "require": string_array_schema(),
            }),
        ))
    }

    fn validate(
        &self,
        node: &qcg_contract::NodeDef,
        _contract: &qcg_contract::Contract,
    ) -> Result<(), StepError> {
        let params = catalog_params(node)?;
        match params.select {
            CatalogSelect::Providers => {
                if params.provider.is_some() || params.model.is_some() {
                    return Err(StepError::failed(
                        &node.id,
                        "llm.catalog select=providers does not take provider or model",
                    ));
                }
            }
            CatalogSelect::Models => {
                require_param(node, params.provider.as_deref(), "provider")?;
                if params.model.is_some() {
                    return Err(StepError::failed(
                        &node.id,
                        "llm.catalog select=models does not take model",
                    ));
                }
            }
            CatalogSelect::Efforts => {
                require_param(node, params.provider.as_deref(), "provider")?;
                require_param(node, params.model.as_deref(), "model")?;
            }
        }
        validate_capability_names(node, &params.require, "require")
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &qcg_contract::NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = catalog_params(node)?;
        let provider_id = params
            .provider
            .as_deref()
            .map(|value| ctx.render_inline(node, value))
            .transpose()?;
        let model_id = params
            .model
            .as_deref()
            .map(|value| ctx.render_inline(node, value))
            .transpose()?;
        let view = self.runtime.catalog.view(false).await;
        let output = match params.select {
            CatalogSelect::Providers => {
                let mut entries = Vec::new();
                let mut catalog = Vec::new();
                for provider in &view.providers {
                    entries.push(json!({
                        "value": provider.id,
                        "label": provider.label.clone().unwrap_or_else(|| provider.id.clone()),
                        "metadata": {
                            "available": provider.available,
                            "discovery": provider.discovery,
                            "enabled": true,
                            "error": provider.error,
                        },
                    }));
                    catalog.push(json!({
                        "id": provider.id,
                        "label": provider.label,
                        "available": provider.available,
                        "discovery": provider.discovery,
                        "error": provider.error,
                        "models": provider.models.len(),
                    }));
                }
                let options = entries
                    .iter()
                    .filter_map(|entry| entry["value"].as_str().map(str::to_string))
                    .collect::<Vec<_>>();
                json!({
                    "select": "providers",
                    "options": options,
                    "entries": entries,
                    "count": catalog.len(),
                    "catalog": { "providers": catalog },
                })
            }
            CatalogSelect::Models | CatalogSelect::Efforts => {
                let provider_id = provider_id.ok_or_else(|| {
                    StepError::failed(&node.id, "llm.catalog provider is required for this select")
                })?;
                let selected = view
                    .providers
                    .iter()
                    .find(|provider| provider.id == provider_id)
                    .ok_or_else(|| {
                        StepError::failed(
                            &node.id,
                            format!(
                                "provider `{provider_id}` is not registered; known providers: {}",
                                view.providers
                                    .iter()
                                    .map(|provider| provider.id.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ),
                        )
                    })?;
                let mut models = selected
                    .models
                    .iter()
                    .filter(|model| !params.enabled_only || model.enabled)
                    .filter(|model| {
                        params
                            .require
                            .iter()
                            .all(|capability| has_capability(&model.capabilities, capability))
                    })
                    .collect::<Vec<_>>();
                if params.select == CatalogSelect::Efforts {
                    let model_id = model_id.ok_or_else(|| {
                        StepError::failed(&node.id, "llm.catalog model is required for this select")
                    })?;
                    models.retain(|model| model.id == model_id);
                    // A provider that declares no models at all keeps the
                    // provider-level effort list; a catalog-backed provider
                    // must contain the requested model or fail explicitly.
                    let provider_spec = self.runtime.catalog.provider(&provider_id);
                    let declared = provider_spec.is_some_and(|spec| {
                        !spec.models.is_empty() || spec.models_discovery.is_some()
                    });
                    let efforts = match models.first() {
                        Some(entry) => entry.reasoning_effort.clone(),
                        None if declared => {
                            return Err(no_model_error(node, &provider_id, &model_id, selected));
                        }
                        None => provider_spec
                            .map(|spec| spec.capabilities.reasoning_effort.clone())
                            .unwrap_or_default(),
                    };
                    let options = efforts
                        .iter()
                        .map(|effort| effort.as_str().to_string())
                        .collect::<Vec<_>>();
                    let entries = options
                        .iter()
                        .map(|option| json!({ "value": option, "label": option }))
                        .collect::<Vec<_>>();
                    json!({
                        "select": "efforts",
                        "provider": provider_id,
                        "model": model_id,
                        "options": options,
                        "entries": entries,
                        "count": entries.len(),
                        "catalog": {
                            "provider": provider_id,
                            "model": model_id,
                            "reasoning_effort": options,
                        },
                    })
                } else {
                    let entries = models
                        .iter()
                        .map(|model| {
                            json!({
                                "value": model.id,
                                "label": model.label.clone().unwrap_or_else(|| model.id.clone()),
                                "metadata": {
                                    "source": model.source,
                                    "enabled": model.enabled,
                                    "reasoning_effort": model.reasoning_effort
                                        .iter()
                                        .map(|effort| effort.as_str())
                                        .collect::<Vec<_>>(),
                                    "capabilities": serde_json::to_value(&model.capabilities)
                                        .unwrap_or(Value::Null),
                                    "input_cost_per_million_usd": model.input_cost_per_million_usd,
                                    "output_cost_per_million_usd": model.output_cost_per_million_usd,
                                    "context_tokens": model.context_tokens,
                                    "max_output_tokens": model.max_output_tokens,
                                },
                            })
                        })
                        .collect::<Vec<_>>();
                    let options = entries
                        .iter()
                        .filter_map(|entry| entry["value"].as_str().map(str::to_string))
                        .collect::<Vec<_>>();
                    json!({
                        "select": "models",
                        "provider": provider_id,
                        "options": options,
                        "entries": entries,
                        "count": entries.len(),
                        "catalog": {
                            "provider": provider_id,
                            "available": selected.available,
                            "discovery": selected.discovery,
                            "error": selected.error,
                            "models": models.iter().map(|model| json!({
                                "id": model.id,
                                "label": model.label,
                                "enabled": model.enabled,
                                "source": model.source,
                                "reasoning_effort": model.reasoning_effort
                                    .iter()
                                    .map(|effort| effort.as_str())
                                    .collect::<Vec<_>>(),
                                "context_tokens": model.context_tokens,
                                "max_output_tokens": model.max_output_tokens,
                            })).collect::<Vec<_>>(),
                        },
                    })
                }
            }
        };
        Ok(StepOutcome::Success {
            output: Some(output),
            files: vec![],
        })
    }
}

fn no_model_error(
    node: &qcg_contract::NodeDef,
    provider: &str,
    model: &str,
    selected: &qcg_llm::CatalogProviderView,
) -> StepError {
    StepError::failed(
        &node.id,
        format!(
            "model `{model}` is not declared for provider `{provider}`; known models: {}",
            if selected.models.is_empty() {
                "<none declared>".to_string()
            } else {
                selected
                    .models
                    .iter()
                    .map(|model| model.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ),
    )
}

fn catalog_params(node: &qcg_contract::NodeDef) -> Result<CatalogParams, StepError> {
    node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid llm.catalog params: {error}"))
    })
}

fn require_param(
    node: &qcg_contract::NodeDef,
    value: Option<&str>,
    field: &str,
) -> Result<(), StepError> {
    if value.unwrap_or_default().trim().is_empty() {
        return Err(StepError::failed(
            &node.id,
            format!("llm.catalog `{field}` is required for this select"),
        ));
    }
    Ok(())
}
