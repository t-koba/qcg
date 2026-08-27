use async_trait::async_trait;
use camino::Utf8PathBuf;
use minijinja::{Environment, ErrorKind};
use qcg_contract::{Contract, NodeDef, StepType};
use qcg_types::{ConfirmSpec, Finding, FormSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;

use crate::{JournalWriter, LlmGateway, RunContext};

#[derive(Debug, thiserror::Error)]
pub enum StepError {
    #[error("step `{node}` failed: {message}")]
    Failed { node: String, message: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("step execution was canceled")]
    Cancelled,
    #[error("run budget `{resource}` exceeded: {used} > {limit}")]
    BudgetExceeded {
        resource: &'static str,
        used: u64,
        limit: u64,
    },
}

impl StepError {
    pub fn failed(node: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            node: node.into(),
            message: message.into(),
        }
    }

    pub fn from_gateway(node: impl Into<String>, error: crate::GatewayError) -> Self {
        if matches!(error, crate::GatewayError::Canceled) {
            Self::Cancelled
        } else {
            Self::failed(node, error.to_string())
        }
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

pub trait ResultExt<T> {
    fn step_err(self, node: impl Into<String>) -> Result<T, StepError>;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: std::fmt::Display,
{
    fn step_err(self, node: impl Into<String>) -> Result<T, StepError> {
        let node = node.into();
        self.map_err(|error| StepError::failed(node, error.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StepOutcome {
    Success {
        #[serde(default)]
        output: Option<Value>,
        #[serde(default)]
        files: Vec<Utf8PathBuf>,
    },
    CheckFailed {
        findings: Vec<Finding>,
    },
    NeedsUser {
        question: FormSpec,
    },
    NeedsConfirm {
        confirm: ConfirmSpec,
    },
}

#[async_trait]
pub trait StepExecutor: Send + Sync {
    fn type_id(&self) -> &'static str;

    fn traits(&self) -> StepTraits {
        StepTraits::default()
    }

    fn params_schema(&self) -> Option<Value> {
        None
    }

    fn validate(&self, _node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StepTraits {
    pub parallel_safe: bool,
    pub control_flow: StepControlFlow,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StepControlFlow {
    #[default]
    Plain,
    Foreach,
}

#[derive(Default, Clone)]
pub struct StepRegistry {
    executors: BTreeMap<String, Arc<dyn StepExecutor>>,
    reserved_secret_env_names: BTreeSet<String>,
}

impl StepRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<E: StepExecutor + 'static>(&mut self, executor: E) {
        self.executors
            .insert(executor.type_id().to_string(), Arc::new(executor));
    }

    pub fn reserve_secret_env_names<I>(&mut self, names: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.reserved_secret_env_names.extend(names);
    }

    pub fn get(&self, kind: &StepType) -> Option<Arc<dyn StepExecutor>> {
        self.executors.get(kind.as_str()).cloned()
    }

    pub fn traits(&self, kind: &StepType) -> Option<StepTraits> {
        self.executors
            .get(kind.as_str())
            .map(|executor| executor.traits())
    }

    pub fn params_schemas(&self) -> BTreeMap<String, Value> {
        self.executors
            .iter()
            .filter_map(|(kind, executor)| {
                executor
                    .params_schema()
                    .map(|schema| (kind.clone(), schema))
            })
            .collect()
    }

    pub fn validate_contract(&self, contract: &Contract) -> Result<(), StepError> {
        for (secret_name, secret) in &contract.manifest.secrets {
            if self.reserved_secret_env_names.contains(&secret.env) {
                return Err(StepError::failed(
                    "contract",
                    format!(
                        "generator secret `{secret_name}` targets reserved LLM provider credential environment variable `{}`",
                        secret.env
                    ),
                ));
            }
        }
        for node in contract
            .manifest
            .flow
            .iter()
            .chain(contract.manifest.blocks.values().flatten())
        {
            let Some(executor) = self.get(&node.kind) else {
                return Err(StepError::failed(
                    &node.id,
                    contract.line_hint(&format!("step type `{}` is not registered", node.kind)),
                ));
            };
            executor.validate(node, contract).map_err(|error| {
                StepError::failed(&node.id, contract.line_hint(&error.to_string()))
            })?;
        }
        Ok(())
    }
}

pub struct StepContext<'a> {
    pub run: &'a RunContext,
    pub journal: &'a JournalWriter,
    pub vars: &'a mut qcg_contract::ValueBag,
    pub llm: Option<LlmGateway<'a>>,
}

impl StepContext<'_> {
    pub async fn checkpoint(&self) -> Result<(), StepError> {
        tokio::task::yield_now().await;
        if self.run.cancellation.is_cancelled() {
            return Err(StepError::Cancelled);
        }
        let budget = &self.run.contract.manifest.budget;
        let state = self.journal.state();
        let tokens = state
            .budget
            .tokens_input
            .saturating_add(state.budget.tokens_output);
        if let Some(limit) = budget.max_tokens
            && tokens > limit
        {
            return Err(StepError::BudgetExceeded {
                resource: "tokens",
                used: tokens,
                limit,
            });
        }
        if let Some(limit) = budget.max_cost_usd {
            let limit = usd_to_microusd(limit);
            if state.budget.cost_microusd > limit {
                return Err(StepError::BudgetExceeded {
                    resource: "cost_microusd",
                    used: state.budget.cost_microusd,
                    limit,
                });
            }
        }
        if let (Some(limit_seconds), Some(started_at)) =
            (budget.max_elapsed_seconds, state.budget.started_at)
        {
            let started_at = chrono::DateTime::parse_from_rfc3339(&started_at)
                .map_err(|error| StepError::failed("runtime", error.to_string()))?;
            let elapsed = chrono::Utc::now()
                .signed_duration_since(started_at)
                .num_seconds()
                .max(0) as u64;
            if elapsed > limit_seconds {
                return Err(StepError::BudgetExceeded {
                    resource: "elapsed_seconds",
                    used: elapsed,
                    limit: limit_seconds,
                });
            }
        }
        Ok(())
    }

    pub async fn spawn_process(
        &self,
        node: &NodeDef,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: usize,
    ) -> Result<crate::CommandOutput, StepError> {
        self.run
            .cmd
            .run_trusted_process(argv, timeout_seconds, output_limit_bytes)
            .await
            .map_err(|error| StepError::from_gateway(&node.id, error))
    }

    pub async fn kill_container(&self, runtime: &str, container_id: &str) {
        self.run.cmd.kill_container(runtime, container_id).await;
    }

    pub fn render_inline(&self, node: &NodeDef, source: &str) -> Result<String, StepError> {
        self.run
            .templates
            .render_inline(source, self.vars.to_json())
            .step_err(&node.id)
    }

    pub fn assert_secret_absent(&self, node: &NodeDef, text: &str) -> Result<(), StepError> {
        self.run.secrets.assert_absent(text).step_err(&node.id)
    }
}

pub(crate) fn usd_to_microusd(value: f64) -> u64 {
    (value * 1_000_000.0).round().clamp(0.0, u64::MAX as f64) as u64
}

#[derive(Clone)]
pub struct TemplateService {
    inner: Arc<Mutex<TemplateCache>>,
}

impl Default for TemplateService {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TemplateCache {
                env: Environment::new(),
                names_by_source_hash: BTreeMap::new(),
            })),
        }
    }
}

impl TemplateService {
    pub fn render_inline(&self, source: &str, context: Value) -> Result<String, minijinja::Error> {
        let mut cache = self.inner.lock().map_err(|_| {
            minijinja::Error::new(
                ErrorKind::InvalidOperation,
                "template cache mutex was poisoned",
            )
        })?;
        let name = cache.template_name(source)?;
        cache.env.get_template(&name)?.render(context)
    }
}

struct TemplateCache {
    env: Environment<'static>,
    names_by_source_hash: BTreeMap<String, String>,
}

impl TemplateCache {
    fn template_name(&mut self, source: &str) -> Result<String, minijinja::Error> {
        let hash = hex::encode(Sha256::digest(source.as_bytes()));
        if let Some(name) = self.names_by_source_hash.get(&hash) {
            return Ok(name.clone());
        }
        let name = format!("inline/{hash}");
        self.env
            .add_template_owned(name.clone(), source.to_string())?;
        self.names_by_source_hash.insert(hash, name.clone());
        Ok(name)
    }
}

#[cfg(test)]
mod tests {
    use super::TemplateService;
    use serde_json::json;

    #[test]
    fn template_service_reuses_compiled_inline_template() {
        let service = TemplateService::default();
        let first = service
            .render_inline("hello {{ name }}", json!({ "name": "one" }))
            .expect("first render should succeed");
        let second = service
            .render_inline("hello {{ name }}", json!({ "name": "two" }))
            .expect("second render should succeed");
        assert_eq!(first, "hello one");
        assert_eq!(second, "hello two");
        let cache = service.inner.lock().expect("cache should lock");
        assert_eq!(cache.names_by_source_hash.len(), 1);
    }
}
