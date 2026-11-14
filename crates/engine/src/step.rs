mod read;

use api::{ConfirmSpec, FormSpec};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use contract::{Contract, FieldType, InputField, NodeDef, RuntimeLimits, StepReuse, StepType};
use minijinja::{Environment, ErrorKind};
use model::Finding;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

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
    #[error("step `{node}` failed: global step budget exceeded: {used} >= {allowed}")]
    GlobalStepBudgetExceeded {
        node: String,
        used: u64,
        allowed: u64,
    },
    /// Provider terminal without reported capacity; partial output is not completion.
    #[error("step `{node}` exhausted the provider context window (model_context_window_exceeded)")]
    ProviderContextExhausted { node: String },
    /// Agent-local limit (not a run budget): settles like `Failed`, retaining validation errors.
    #[error("step `{node}` failed: {message}")]
    AgentIterationsExhausted {
        node: String,
        message: String,
        resource: &'static str,
        used: u64,
        allowed: u64,
        validation: Option<String>,
    },
    /// Step-local resource limit (not a run budget): settles like `Failed`.
    #[error("step `{node}` failed: {message}")]
    LocalResourceExhausted {
        node: String,
        message: String,
        resource: &'static str,
        used: u64,
        allowed: u64,
    },
    /// The operation guard refused execution: resend of changed content,
    /// uncacheable prior success, or indeterminate outcome without an
    /// at-least-once opt-in. Never retried automatically; manual recovery
    /// or a policy decision is required.
    #[error("step `{node}` refused: {message}")]
    Refused { node: String, message: String },
    #[error("step `{node}` requires MCP authorization for `{server}`")]
    McpAuthorizationRequired { node: String, server: String },
    /// Per-attempt timeout, distinct from ordinary failures so retry policy
    /// separates unknown outcomes from proven-clean errors (B08/B11).
    #[error("step `{node}` timed out after {timeout_secs}s")]
    TimedOut { node: String, timeout_secs: u64 },
    /// The run-wide elapsed budget stopped this node. Distinct from a node
    /// timeout and from cancellation so hosts can report the actual cause
    /// (E11).
    #[error("step `{node}` exceeded the run elapsed limit of {limit_secs}s")]
    ElapsedExceeded {
        node: String,
        limit_secs: u64,
        used: std::time::Duration,
    },
    /// Run elapsed budget expired before the next step, unlike `ElapsedExceeded`,
    /// which stops a step already running (E11).
    #[error("run elapsed limit of {limit_secs}s expired before the next step could start")]
    ElapsedExpiredBeforeStart {
        limit_secs: u64,
        used: std::time::Duration,
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
            // Gateway-internal bounds (including `CommandTimedOut`) surface
            // as retryable Failed: the run-cancel / node-timeout / elapsed
            // distinction of E11 applies to engine enforcement (node
            // `timeout_secs`, run `max_elapsed_seconds`, cancellation), not
            // to transport-internal limits, which carry no node budget to
            // report against.
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

/// Deserialize a step's params with the shared `invalid <label> params` diagnostic.
pub fn step_params<T: DeserializeOwned>(node: &NodeDef, label: &str) -> Result<T, StepError> {
    node.deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid {label} params: {error}")))
}

/// Validate a required string field from a step's decoded parameters.
pub fn require_field(node: &NodeDef, value: Option<&str>, field: &str) -> Result<(), StepError> {
    if value.unwrap_or_default().is_empty() {
        Err(StepError::failed(&node.id, format!("{field} is required")))
    } else {
        Ok(())
    }
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
    // No `#[serde(default)]` shims (E07): `output` and `files` are required
    // on read so partial records fail closed instead of silently degrading
    // to empty outputs. `output` uses presence-required deserialization
    // because serde implicitly defaults missing `Option` fields to `None`.
    Success {
        #[serde(deserialize_with = "crate::required_presence")]
        output: Option<Value>,
        files: Vec<Utf8PathBuf>,
    },
    CheckFailed {
        findings: Vec<Finding>,
        #[serde(deserialize_with = "crate::required_presence")]
        output: Option<Value>,
        files: Vec<Utf8PathBuf>,
    },
    NeedsUser {
        question: FormSpec,
        /// The files this attempt produced before it suspended. The
        /// suspension keeps them: the journal records what the attempt
        /// wrote, so a resume restores those writes instead of losing
        /// them with the attempt's outcome (E06).
        files: Vec<Utf8PathBuf>,
    },
    NeedsConfirm {
        confirm: ConfirmSpec,
        /// The files this attempt produced before it asked for
        /// confirmation, with the same rule as the suspension above.
        files: Vec<Utf8PathBuf>,
    },
}

impl StepOutcome {
    /// Construct a successful outcome that emitted no files.
    pub fn success(output: Option<Value>) -> Self {
        Self::Success {
            output,
            files: Vec::new(),
        }
    }

    /// Construct a check failure without a separate output or emitted files.
    pub fn check_failed(findings: Vec<Finding>) -> Self {
        Self::CheckFailed {
            findings,
            output: None,
            files: Vec::new(),
        }
    }
}

/// Required-field defaults shared by engine form builders.
pub(crate) fn required_input_field(
    id: impl Into<String>,
    kind: FieldType,
    label: Option<String>,
    description: Option<String>,
    schema: Option<Value>,
) -> InputField {
    InputField {
        id: id.into(),
        label,
        label_i18n: Default::default(),
        description,
        description_i18n: Default::default(),
        placeholder: None,
        placeholder_i18n: Default::default(),
        kind,
        required: true,
        default: None,
        pattern: None,
        options: Vec::new(),
        option_labels_i18n: Default::default(),
        min_items: None,
        item_type: None,
        schema,
        options_from: None,
        ui: Default::default(),
    }
}

/// Effect safety for per-node continuation, independent of model-response reuse
/// and parallel scheduling. This declaration alone never authorizes reuse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ContinuationSafety {
    #[default]
    Unsupported,
    /// No side effects beyond captured reads and the outcome. Reuse may only be
    /// considered once an eligibility evaluator proves all dependencies unchanged.
    ReadOnly,
}

#[async_trait]
pub trait StepExecutor: Send + Sync {
    fn type_id(&self) -> &'static str;

    fn traits(&self) -> StepTraits {
        StepTraits::default()
    }

    /// Undeclared executors are ineligible for per-node continuation.
    fn continuation_safety(&self, _node: &NodeDef) -> ContinuationSafety {
        ContinuationSafety::Unsupported
    }

    /// Safe/guarded re-entry after an unreported direct node execution; parallel safety is not enough.
    fn replay_safe_after_interruption(&self) -> bool {
        false
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
    pub reuse: ReuseSupport,
}

/// Whether a step type's recorded model responses may serve a continuation.
/// Fail closed: an executor that declares nothing is never reused.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReuseSupport {
    #[default]
    None,
    /// Reused only when the contract sets `reuse = "matching"`.
    OptIn,
    /// Reused unless the contract sets `reuse = "never"`.
    Default,
}

impl StepTraits {
    /// The effective reuse decision for `node` under the contract policy.
    pub fn reuses(&self, node: &NodeDef) -> bool {
        match (node.reuse, self.reuse) {
            (Some(StepReuse::Never), _) | (_, ReuseSupport::None) => false,
            (Some(StepReuse::Matching), _) => true,
            (None, support) => support == ReuseSupport::Default,
        }
    }

    /// Traits for executors that are safe to run in parallel waves.
    pub fn parallel() -> Self {
        Self {
            parallel_safe: true,
            ..Self::default()
        }
    }
}

/// Implement the standard parallel-safe traits for a `StepExecutor`.
#[macro_export]
macro_rules! parallel_step_traits {
    () => {
        fn traits(&self) -> $crate::StepTraits {
            $crate::StepTraits::parallel()
        }
    };
    ($reuse:ident) => {
        fn traits(&self) -> $crate::StepTraits {
            $crate::StepTraits {
                reuse: $crate::ReuseSupport::$reuse,
                ..$crate::StepTraits::parallel()
            }
        }
    };
}

/// Implement a `StepExecutor`'s stable contract type identifier.
#[macro_export]
macro_rules! step_type_id {
    ($type_id:literal) => {
        fn type_id(&self) -> &'static str {
            $type_id
        }
    };
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

    pub fn reserve_secret_env_names<I: IntoIterator<Item = String>>(&mut self, names: I) {
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

    /// Unknown step types fail closed, just like undeclared executors.
    pub fn continuation_safety(&self, node: &NodeDef) -> ContinuationSafety {
        self.executors
            .get(node.kind.as_str())
            .map(|executor| executor.continuation_safety(node))
            .unwrap_or_default()
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
        let mut errors: Vec<String> = Vec::new();
        for (secret_name, secret) in &contract.manifest.secrets {
            let Some(source) = secret.source_env_name() else {
                continue;
            };
            if self.reserved_secret_env_names.contains(source) {
                errors.push(format!(
                    "generator secret `{secret_name}` targets reserved provider credential environment variable `{source}`"
                ));
            }
        }
        for node in contract
            .manifest
            .flow
            .iter()
            .chain(contract.manifest.blocks.values().flatten())
        {
            errors.extend(self.validation_error(node, contract, "node", &node.id));
        }
        // Hooks share registry preflight with flow and block nodes.
        for (event, hook) in contract.manifest.hooks.entries() {
            let node = hook.to_node(event);
            errors.extend(self.validation_error(&node, contract, "hook", event));
        }
        match errors.as_slice() {
            [] => Ok(()),
            [message] => Err(StepError::failed("contract", message)),
            _ => Err(StepError::failed(
                "contract",
                format!(
                    "invalid contract ({} problems):\n{}",
                    errors.len(),
                    errors
                        .iter()
                        .enumerate()
                        .map(|(index, error)| format!("{}. {error}", index + 1))
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
            )),
        }
    }

    /// Foreach admits control-flow children independently of wave scheduling traits.
    pub(crate) fn foreach_block<'a>(
        &self,
        node: &NodeDef,
        contract: &'a Contract,
        params: &crate::engine::ForeachControlParams,
    ) -> Result<&'a [NodeDef], StepError> {
        let subflow = params.subflow.as_str();
        let block =
            contract.manifest.blocks.get(subflow).ok_or_else(|| {
                StepError::failed(&node.id, format!("unknown subflow `{subflow}`"))
            })?;
        if params.parallel > 1 {
            for child in block.iter() {
                let unsafe_leaf = self.traits(&child.kind).is_none_or(|traits| {
                    !traits.parallel_safe && traits.control_flow == StepControlFlow::Plain
                });
                let problem = if unsafe_leaf {
                    "is not parallel-safe"
                } else if child.on_fail.is_some() {
                    "declares on_fail routing"
                } else {
                    continue;
                };
                let id = &child.id;
                return Err(StepError::failed(
                    &node.id,
                    format!("foreach block child `{id}` {problem}; use parallel = 1"),
                ));
            }
        }
        Ok(block)
    }

    fn validation_error(
        &self,
        node: &NodeDef,
        contract: &Contract,
        label: &str,
        name: &str,
    ) -> Option<String> {
        let message = match self.get(&node.kind) {
            Some(executor) => executor
                .validate(node, contract)
                .and_then(|()| {
                    if node.reuse == Some(StepReuse::Matching)
                        && executor.traits().reuse == ReuseSupport::None
                    {
                        return Err(StepError::failed(
                            &node.id,
                            "reuse = \"matching\" needs a step type that records model responses",
                        ));
                    }
                    if executor.traits().control_flow == StepControlFlow::Foreach {
                        self.foreach_block(node, contract, &step_params(node, "foreach")?)?;
                    }
                    Ok(())
                })
                .err()?
                .to_string(),
            None => format!("step type `{}` is not registered", node.kind),
        };
        let message = contract.line_hint(&message);
        Some(format!("{label} `{name}`: {message}"))
    }
}

pub struct StepContext<'a> {
    pub run: &'a RunContext,
    pub journal: &'a JournalWriter,
    pub vars: &'a mut contract::ValueBag,
    pub llm: Option<LlmGateway<'a>>,
}

fn checked_settled_tokens(input: u64, output: u64) -> Result<u64, StepError> {
    input.checked_add(output).ok_or_else(|| {
        StepError::failed(
            "budget",
            "settled token usage overflowed; refusing further spend",
        )
    })
}

impl StepContext<'_> {
    /// Node-scoped shared checkpoint; only scope and error mapping differ,
    /// never elapsed enforcement (E11).
    pub async fn step_checkpoint(&self, node: &NodeDef) -> Result<(), StepError> {
        tokio::task::yield_now().await;
        // The monotonic deadline stops in-flight work; cancellation maps to
        // this step's Cancelled variant.
        crate::engine::checkpoint_scope(
            &self.run.cancellation,
            self.run
                .elapsed_deadline
                .zip(self.run.contract.manifest.budget.max_elapsed_seconds),
            crate::engine::CheckpointScope::Node(&node.id),
        )?;
        let budget = &self.run.contract.manifest.budget;
        let state = self.journal.state();
        let tokens = checked_settled_tokens(state.budget.tokens_input, state.budget.tokens_output)?;
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
        // The monotonic deadline is authoritative; durable wall time is diagnostic
        // only. Using both would drift at truncation/NTP; resume reconstructs the
        // same deadline from durable elapsed (E11).
        Ok(())
    }

    /// Record a side-effecting gateway failure and map it to a step error.
    /// Cancellation deliberately has no operation completion record.
    pub fn side_effecting_gateway_error(
        &self,
        node: &NodeDef,
        operation_id: &str,
        error: crate::GatewayError,
    ) -> StepError {
        if !matches!(error, crate::GatewayError::Canceled) {
            self.run.finish_external_operation_with_warn(
                self.journal,
                node,
                operation_id,
                crate::OperationOutcome::gateway_error(&error, false),
            );
        }
        StepError::from_gateway(&node.id, error)
    }

    pub async fn spawn_process(
        &self,
        node: &NodeDef,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: Option<usize>,
    ) -> Result<crate::CommandOutput, StepError> {
        self.run
            .cmd
            .run_trusted_process(argv, timeout_seconds, output_limit_bytes)
            .await
            .map_err(|error| StepError::from_gateway(&node.id, error))
    }

    pub fn render_inline(&self, node: &NodeDef, source: &str) -> Result<String, StepError> {
        self.run
            .templates
            .render_bag_scoped(
                contract::ReadOwner::Node(node.id.clone()),
                source,
                self.vars,
                &self.run.contract.manifest.runtime,
            )
            .step_err(&node.id)
    }
}

pub(crate) fn usd_to_microusd(value: f64) -> u64 {
    (value * 1_000_000.0).round().clamp(0.0, u64::MAX as f64) as u64
}

#[derive(Clone, Default)]
pub struct TemplateService;

impl TemplateService {
    pub fn render_inline(
        &self,
        source: &str,
        context: Value,
        limits: &RuntimeLimits,
    ) -> Result<String, minijinja::Error> {
        self.render_context(source, context, limits, None)
    }

    pub fn render_bag_scoped(
        &self,
        owner: contract::ReadOwner,
        source: &str,
        bag: &contract::ValueBag,
        limits: &RuntimeLimits,
    ) -> Result<String, minijinja::Error> {
        self.render_context_scoped(
            owner,
            source,
            bag.template_snapshot(),
            limits,
            bag.read_recorder(),
        )
    }

    /// Renders a detached JSON context while retaining actual reads in the
    /// owning execution trace. Use for runtime-derived contexts such as host
    /// metadata or retry-local values that are not part of ValueBag.
    pub fn render_context_scoped(
        &self,
        owner: contract::ReadOwner,
        source: &str,
        context: Value,
        limits: &RuntimeLimits,
        recorder: contract::ReadRecorder,
    ) -> Result<String, minijinja::Error> {
        let reads = recorder.for_owner(owner);
        reads.set_byte_limit(
            limits
                .journal_event_limit_bytes
                .unwrap_or(policy::DEFAULT_MAX_JOURNAL_EVENT_BYTES),
        );
        self.render_context(source, context, limits, Some(reads))
    }

    fn render_context(
        &self,
        source: &str,
        context: Value,
        limits: &RuntimeLimits,
        reads: Option<contract::ReadRecorder>,
    ) -> Result<String, minijinja::Error> {
        validate_template_limits(limits)?;
        if let Some(limit) = limits.template_source_limit_bytes
            && source.len() > limit
        {
            return Err(minijinja::Error::new(
                ErrorKind::InvalidOperation,
                format!("template source exceeds {limit} bytes"),
            ));
        }
        validate_template_context(&context, limits.template_context_limit_bytes)?;
        let mut env = Environment::new();
        env.set_fuel(Some(limits.template_fuel));
        env.add_template_owned("inline".to_owned(), source.to_owned())?;
        let template = env.get_template("inline")?;
        let mut writer = BoundedTemplateWriter {
            bytes: Vec::new(),
            limit: limits.template_output_limit_bytes,
        };
        let context = minijinja::Value::from_serialize(context);
        let context = match reads {
            Some(reads) => read::context(context, reads),
            None => context,
        };
        let captured = template
            .render_captured_to(context, &mut writer)
            .map_err(|error| {
                let Some(source) = std::error::Error::source(&error) else {
                    return error;
                };
                minijinja::Error::new(error.kind(), format!("{error}: {source}"))
            })?;
        drop(captured);
        String::from_utf8(writer.into_inner()).map_err(|error| {
            minijinja::Error::new(
                ErrorKind::InvalidOperation,
                format!("template output is not valid UTF-8: {error}"),
            )
        })
    }
}

fn validate_template_limits(limits: &RuntimeLimits) -> Result<(), minijinja::Error> {
    for (name, value) in [
        (
            "template_source_limit_bytes",
            limits.template_source_limit_bytes,
        ),
        (
            "template_context_limit_bytes",
            limits.template_context_limit_bytes,
        ),
        (
            "template_output_limit_bytes",
            limits.template_output_limit_bytes,
        ),
    ] {
        if value == Some(0) {
            return Err(minijinja::Error::new(
                ErrorKind::InvalidOperation,
                format!("runtime.{name} must be greater than zero"),
            ));
        }
    }
    if limits.template_fuel == 0 {
        return Err(minijinja::Error::new(
            ErrorKind::InvalidOperation,
            "runtime.template_fuel must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_template_context(
    context: &Value,
    limit: Option<usize>,
) -> Result<(), minijinja::Error> {
    let Some(limit) = limit else {
        // No mechanistic limit: still ensure serializability.
        serde_json::to_value(context).map_err(|error| {
            minijinja::Error::new(
                ErrorKind::InvalidOperation,
                format!("template context serialization failed: {error}"),
            )
        })?;
        return Ok(());
    };
    let mut writer = CountingTemplateWriter {
        bytes: 0,
        limit,
        exceeded: false,
    };
    serde_json::to_writer(&mut writer, context).map_err(|error| {
        if writer.exceeded {
            minijinja::Error::new(
                ErrorKind::InvalidOperation,
                format!("template context exceeds {limit} bytes"),
            )
        } else {
            minijinja::Error::new(
                ErrorKind::InvalidOperation,
                format!("template context serialization failed: {error}"),
            )
        }
    })
}

struct CountingTemplateWriter {
    bytes: usize,
    limit: usize,
    exceeded: bool,
}

impl io::Write for CountingTemplateWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("template context size overflowed"))?;
        if next > self.limit {
            self.exceeded = true;
            return Err(io::Error::other(format!(
                "template context exceeds {} bytes",
                self.limit
            )));
        }
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct BoundedTemplateWriter {
    bytes: Vec<u8>,
    limit: Option<usize>,
}

impl BoundedTemplateWriter {
    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedTemplateWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("template output size overflowed"))?;
        if let Some(limit) = self.limit
            && next > limit
        {
            return Err(io::Error::other(format!(
                "template output exceeds {limit} bytes"
            )));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeLimits, TemplateService};
    use serde_json::json;

    #[test]
    fn settled_token_total_rejects_overflow() {
        assert_eq!(
            super::checked_settled_tokens(u64::MAX, 0).unwrap(),
            u64::MAX
        );
        assert!(super::checked_settled_tokens(u64::MAX, 1).is_err());
    }

    #[test]
    fn continuation_safety_defaults_fail_closed() {
        use super::*;

        struct CustomStep;

        #[async_trait]
        impl StepExecutor for CustomStep {
            fn type_id(&self) -> &'static str {
                "custom"
            }

            async fn execute(
                &self,
                _ctx: &mut StepContext<'_>,
                _node: &NodeDef,
            ) -> Result<StepOutcome, StepError> {
                unreachable!("classification does not execute the step")
            }
        }

        let node = |kind: &str| -> NodeDef {
            serde_json::from_value(json!({"id": kind, "type": kind})).unwrap()
        };
        let custom = node("custom");
        let unknown = node("unregistered");
        let mut registry = StepRegistry::new();
        assert_eq!(
            ContinuationSafety::default(),
            ContinuationSafety::Unsupported
        );
        assert_eq!(
            registry.continuation_safety(&unknown),
            ContinuationSafety::Unsupported
        );
        assert_eq!(
            CustomStep.continuation_safety(&custom),
            ContinuationSafety::Unsupported
        );
        registry.register(CustomStep);
        assert_eq!(
            registry.continuation_safety(&custom),
            ContinuationSafety::Unsupported
        );
        assert_eq!(
            registry.continuation_safety(&unknown),
            ContinuationSafety::Unsupported
        );
    }

    #[test]
    fn required_input_field_serializes_required_defaults() {
        let field = super::required_input_field(
            "answer",
            contract::FieldType::Text,
            Some("Resolution".to_string()),
            Some("Describe it".to_string()),
            Some(json!({ "type": "string" })),
        );
        let value = serde_json::to_value(&field).expect("field should serialize");
        assert_eq!(
            value,
            json!({
                "id": "answer",
                "label": "Resolution",
                "description": "Describe it",
                "type": "text",
                "required": true,
                "default": null,
                "pattern": null,
                "options": [],
                "min_items": null,
                "item_type": null,
                "schema": { "type": "string" }
            })
        );
        let bare =
            super::required_input_field("answer", contract::FieldType::String, None, None, None);
        let bare_value = serde_json::to_value(&bare).expect("bare field should serialize");
        assert_eq!(
            bare_value,
            json!({
                "id": "answer",
                "type": "string",
                "required": true,
                "default": null,
                "pattern": null,
                "options": [],
                "min_items": null,
                "item_type": null
            })
        );
    }

    #[test]
    fn template_service_renders_repeated_sources_without_retaining_cache_state() {
        let service = TemplateService;
        let limits = RuntimeLimits::default();
        let first = service
            .render_inline("hello {{ name }}", json!({ "name": "one" }), &limits)
            .expect("first render should succeed");
        let second = service
            .render_inline("hello {{ name }}", json!({ "name": "two" }), &limits)
            .expect("second render should succeed");
        assert_eq!(first, "hello one");
        assert_eq!(second, "hello two");
    }

    #[test]
    fn template_service_rejects_source_larger_than_runtime_limit() {
        let service = TemplateService;
        let limits = RuntimeLimits {
            template_source_limit_bytes: Some(8),
            ..RuntimeLimits::default()
        };
        let error = service
            .render_inline("0123456789", json!({}), &limits)
            .expect_err("template source should be bounded");
        assert!(
            error
                .to_string()
                .contains("template source exceeds 8 bytes")
        );
    }

    #[test]
    fn template_service_rejects_context_larger_than_runtime_limit() {
        let service = TemplateService;
        let limits = RuntimeLimits {
            template_context_limit_bytes: Some(8),
            ..RuntimeLimits::default()
        };
        let error = service
            .render_inline("{{ value }}", json!({ "value": "0123456789" }), &limits)
            .expect_err("template context should be bounded");
        assert!(
            error
                .to_string()
                .contains("template context exceeds 8 bytes")
        );
    }

    #[test]
    fn template_service_rejects_output_larger_than_runtime_limit() {
        let service = TemplateService;
        let limits = RuntimeLimits {
            template_output_limit_bytes: Some(8),
            ..RuntimeLimits::default()
        };
        let error = service
            .render_inline("{{ value }}", json!({ "value": "0123456789" }), &limits)
            .expect_err("template output should be bounded");
        assert!(
            error
                .to_string()
                .contains("template output exceeds 8 bytes")
        );
    }

    #[test]
    fn template_service_stops_when_fuel_is_exhausted() {
        let service = TemplateService;
        let limits = RuntimeLimits {
            template_fuel: 1,
            ..RuntimeLimits::default()
        };
        let error = service
            .render_inline(
                "{% for value in values %}{{ value }}{% endfor %}",
                json!({ "values": [1, 2, 3, 4, 5] }),
                &limits,
            )
            .expect_err("template fuel should be bounded");
        assert!(error.to_string().contains("fuel"));
    }
}

#[cfg(test)]
mod reuse_tests {
    use super::*;

    fn node(reuse: Option<StepReuse>) -> NodeDef {
        let mut node: NodeDef = toml::from_str("id = \"n\"\ntype = \"x\"").unwrap();
        node.reuse = reuse;
        node
    }

    #[test]
    fn contract_policy_cannot_widen_what_the_step_type_declares() {
        use ReuseSupport::{Default as ByDefault, None as Undeclared, OptIn};
        use StepReuse::{Matching, Never};
        for (support, policy, expected) in [
            (Undeclared, None, false),
            (Undeclared, Some(Matching), false),
            (Undeclared, Some(Never), false),
            (OptIn, None, false),
            (OptIn, Some(Matching), true),
            (OptIn, Some(Never), false),
            (ByDefault, None, true),
            (ByDefault, Some(Matching), true),
            (ByDefault, Some(Never), false),
        ] {
            let traits = StepTraits {
                reuse: support,
                ..StepTraits::default()
            };
            assert_eq!(
                traits.reuses(&node(policy)),
                expected,
                "{support:?} {policy:?}"
            );
        }
    }
}
