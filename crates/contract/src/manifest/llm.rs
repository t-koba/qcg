use model::{ReasoningEffort, ResponseVerbosity, StructuredOutputMode, ToolChoice};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::resources::{default_max_total_steps, default_template_fuel, default_timeout_seconds};

/// A reasoning effort that is either pinned by the contract or rendered from
/// run variables immediately before the request. Node-level `model.provider`
/// and `model.model` accept templates too; this is the effort counterpart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ReasoningEffortSpec {
    Value(ReasoningEffort),
    Template(String),
}

impl ReasoningEffortSpec {
    pub fn static_value(&self) -> Option<ReasoningEffort> {
        match self {
            Self::Value(value) => Some(*value),
            Self::Template(_) => None,
        }
    }

    pub fn template(&self) -> Option<&str> {
        match self {
            Self::Value(_) => None,
            Self::Template(template) => Some(template),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LlmConfig {
    pub model: Option<ModelRef>,
    pub models: Vec<ModelRef>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_tokens: Option<u32>,
    pub stop_sequences: Vec<String>,
    pub max_context_bytes: Option<usize>,
    pub max_context_tokens: Option<usize>,
    pub max_media_bytes: Option<usize>,
    pub context_overflow: ContextOverflowPolicy,
    /// Prompt-cache policy. Unset and `off` send no cache instructions;
    /// `auto` sends the selected provider's cache instructions.
    /// `off` only omits qcg cache instructions; it does not disable
    /// provider-side caching. See `cache_write_policy` for a disable.
    pub cache: PromptCachePolicy,
    /// Prompt-cache write policy. `allow` (default) leaves provider
    /// caching alone; `forbid` requires qcg/provider caching to be
    /// disabled for the request and fails before network dispatch when
    /// the selected route cannot honor the disable.
    pub cache_write_policy: CacheWritePolicy,
    /// Optional prompt-cache retention tier. Only `extended` is defined;
    /// absent means the provider default lifetime. Valid only with
    /// `cache = "auto"` and `cache_write_policy = "allow"`, and only
    /// when the selected route explicitly declares the tier mechanism.
    pub cache_retention: Option<CacheRetention>,
    pub requires: Vec<String>,
    pub system: Option<String>,
    pub retry_prompt: Option<String>,
    pub seed: Option<u64>,
    pub reasoning_effort: Option<ReasoningEffortSpec>,
    pub structured_output: StructuredOutputMode,
    pub tool_choice: Option<ToolChoice>,
    pub parallel_tool_calls: Option<bool>,
    pub verbosity: Option<ResponseVerbosity>,
}

/// Per-invocation LLM policy layered over the generator-wide `[llm]` defaults.
///
/// The provider registry advertises transport capabilities. This value only
/// selects behavior for one node or specialist invocation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LlmRequestPolicy {
    pub clear: Vec<LlmRequestControl>,
    pub system: Option<String>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_tokens: Option<u32>,
    pub stop_sequences: Option<Vec<String>>,
    pub seed: Option<u64>,
    pub reasoning_effort: Option<ReasoningEffortSpec>,
    pub structured_output: Option<StructuredOutputMode>,
    pub tool_choice: Option<ToolChoice>,
    pub parallel_tool_calls: Option<bool>,
    pub verbosity: Option<ResponseVerbosity>,
    pub stream: Option<bool>,
    pub requires: Vec<String>,
    pub max_context_bytes: Option<usize>,
    pub max_context_tokens: Option<usize>,
    pub max_media_bytes: Option<usize>,
    pub context_overflow: Option<ContextOverflowPolicy>,
    pub retry_prompt: Option<String>,
}

/// Optional inherited request controls that an inner policy layer may omit.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum LlmRequestControl {
    Temperature,
    TopP,
    StopSequences,
    Seed,
    ReasoningEffort,
    ToolChoice,
    ParallelToolCalls,
    Verbosity,
}

impl LlmRequestControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Temperature => "temperature",
            Self::TopP => "top_p",
            Self::StopSequences => "stop_sequences",
            Self::Seed => "seed",
            Self::ReasoningEffort => "reasoning_effort",
            Self::ToolChoice => "tool_choice",
            Self::ParallelToolCalls => "parallel_tool_calls",
            Self::Verbosity => "verbosity",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContextOverflowPolicy {
    #[default]
    Error,
    TruncateHead,
    TruncateTail,
}

/// Declared prompt-cache policy for this contract's LLM requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PromptCachePolicy {
    /// Send the provider's cache instructions when the selected provider
    /// advertises prompt-cache support.
    Auto,
    /// Never send cache instructions.
    #[default]
    Off,
}

/// Declared prompt-cache write policy for this contract's LLM requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CacheWritePolicy {
    /// Leave provider caching alone (default).
    #[default]
    Allow,
    /// Require qcg/provider caching to be disabled for the request.
    Forbid,
}

/// Declared prompt-cache retention tier. Only `extended` is defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CacheRetention {
    /// The route's explicitly declared extended-retention mechanism.
    Extended,
}

impl LlmConfig {
    /// Fail-closed combination rule for the advanced cache controls.
    /// Retention needs `cache = "auto"` with write `allow`; `forbid`
    /// itself is validated against the selected route at dispatch.
    pub fn validate_cache_policy(&self) -> Result<(), String> {
        match (self.cache_retention, self.cache, self.cache_write_policy) {
            (None, _, _) | (Some(_), PromptCachePolicy::Auto, CacheWritePolicy::Allow) => Ok(()),
            _ => Err(
                "cache_retention requires cache = \"auto\" and cache_write_policy = \"allow\""
                    .to_string(),
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRef {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub input_cost_per_million_usd: Option<f64>,
    /// Cache creation input USD/1M; absent falls back to ordinary input.
    #[serde(default)]
    pub cache_creation_input_cost_per_million_usd: Option<f64>,
    /// Cache read input USD/1M; absent falls back to ordinary input.
    #[serde(default)]
    pub cache_read_input_cost_per_million_usd: Option<f64>,
    #[serde(default)]
    pub output_cost_per_million_usd: Option<f64>,
    /// Hosted web search USD per call. Absent means the search charge is
    /// unmetered, never free.
    #[serde(default)]
    pub web_search_call_cost_usd: Option<f64>,
    /// Context tiers: the rates that apply to a whole request once its
    /// prompt input reaches a tier's threshold. Absent keeps the flat
    /// rates above for every request.
    #[serde(default)]
    pub cost_tiers: Vec<policy::CostTier>,
}

impl ModelRef {
    /// The first non-finite or negative base-row/web-search rate, named by field.
    /// `None` when valid; tier schedules use [`policy::validate_cost_tiers`].
    pub fn invalid_price(&self) -> Option<&'static str> {
        self.rates().invalid_rate().or_else(|| {
            self.web_search_call_cost_usd
                .filter(|value| !value.is_finite() || *value < 0.0)
                .map(|_| "web_search_call_cost_usd")
        })
    }

    /// The cache-read rate: an absent rate bills at the ordinary input rate.
    pub fn cache_read_rate(&self) -> Option<f64> {
        self.cache_read_input_cost_per_million_usd
            .or(self.input_cost_per_million_usd)
    }

    /// The cache-creation rate: an absent rate bills at the ordinary input rate.
    pub fn cache_creation_rate(&self) -> Option<f64> {
        self.cache_creation_input_cost_per_million_usd
            .or(self.input_cost_per_million_usd)
    }

    /// The base row's rates, before any context tier applies.
    pub fn rates(&self) -> policy::CostRates {
        policy::CostRates {
            input: self.input_cost_per_million_usd,
            cache_read: self.cache_read_input_cost_per_million_usd,
            cache_creation: self.cache_creation_input_cost_per_million_usd,
            output: self.output_cost_per_million_usd,
        }
    }

    // Missing prices are allowed: catalog prices may fill the gaps at dispatch.
    pub(super) fn validate_pricing(&self) -> Result<(), super::ContractError> {
        let invalid = |message| {
            super::ContractError::Invalid(format!(
                "model `{}/{}` {message}",
                self.provider, self.model
            ))
        };
        if let Some(name) = self.invalid_price() {
            return Err(invalid(format!("{name} must be finite and non-negative")));
        }
        policy::validate_cost_tiers(&self.cost_tiers).map_err(|error| invalid(error.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLimits {
    #[serde(default = "default_timeout_seconds")]
    pub command_timeout_seconds: u64,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub command_input_limit_bytes: Option<usize>,
    /// Omission uses the 16 MiB combined stdout/stderr policy default.
    #[serde(default)]
    pub command_output_limit_bytes: Option<usize>,
    #[serde(default = "default_timeout_seconds")]
    pub http_timeout_seconds: u64,
    /// Explicit max only. Omitted (`None`) uses the 16 MiB policy default
    /// for one HTTP request or response body.
    #[serde(default)]
    pub http_body_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub http_redirect_limit: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub file_input_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub file_count_limit: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub input_total_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub output_file_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub output_total_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub output_artifact_limit: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub template_source_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub template_context_limit_bytes: Option<usize>,
    /// Omitted (`None`) uses the 8 MiB engine default per journal event.
    #[serde(default)]
    pub journal_event_limit_bytes: Option<usize>,
    /// In-memory scan window for journal tail repair and seq recovery.
    #[serde(default)]
    pub journal_scan_window_bytes: Option<usize>,
    /// Omitted (`None`) uses the 64 MiB engine default for the journal.
    #[serde(default)]
    pub journal_total_limit_bytes: Option<usize>,
    /// Omitted (`None`) uses the 1,000,000-event engine default.
    #[serde(default)]
    pub journal_event_count_limit: Option<usize>,
    /// Omitted (`None`) uses the 16 MiB engine default for state.
    #[serde(default)]
    pub state_limit_bytes: Option<usize>,
    /// Explicit max only. `None` means no mechanistic limit.
    #[serde(default)]
    pub template_output_limit_bytes: Option<usize>,
    /// Explicit max only for hash-anchored patch edits per call.
    /// `None` falls back to the policy default.
    #[serde(default)]
    pub patch_hunks_limit: Option<usize>,
    /// Explicit max only for hash-anchored patch payload bytes.
    /// `None` falls back to the policy default.
    #[serde(default)]
    pub patch_bytes_limit: Option<usize>,
    #[serde(default = "default_template_fuel")]
    pub template_fuel: u64,
}

impl RuntimeLimits {
    /// Resolve an optional command-specific override, then the runtime limit.
    pub fn command_output_limit(&self, override_limit: Option<usize>) -> Option<usize> {
        Some(
            override_limit
                .or(self.command_output_limit_bytes)
                .unwrap_or(policy::limits::DEFAULT_COMMAND_OUTPUT_LIMIT_BYTES),
        )
    }

    /// The effective HTTP body limit: the contract's explicit value when
    /// set, otherwise `policy::DEFAULT_HTTP_BODY_LIMIT_BYTES`. Omitted
    /// never means an unbounded body.
    pub fn effective_http_body_limit_bytes(&self) -> usize {
        self.http_body_limit_bytes
            .unwrap_or(policy::DEFAULT_HTTP_BODY_LIMIT_BYTES)
    }
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            command_timeout_seconds: default_timeout_seconds(),
            command_input_limit_bytes: None,
            command_output_limit_bytes: None,
            http_timeout_seconds: default_timeout_seconds(),
            http_body_limit_bytes: None,
            http_redirect_limit: None,
            file_input_limit_bytes: None,
            file_count_limit: None,
            input_total_limit_bytes: None,
            output_file_limit_bytes: None,
            output_total_limit_bytes: None,
            output_artifact_limit: None,
            template_source_limit_bytes: None,
            template_context_limit_bytes: None,
            journal_event_limit_bytes: None,
            journal_scan_window_bytes: None,
            journal_total_limit_bytes: None,
            journal_event_count_limit: None,
            state_limit_bytes: None,
            template_output_limit_bytes: None,
            patch_hunks_limit: None,
            patch_bytes_limit: None,
            template_fuel: default_template_fuel(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunBudget {
    #[serde(default = "default_max_total_steps")]
    pub max_steps: usize,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub max_cost_usd: Option<f64>,
    #[serde(default)]
    pub max_elapsed_seconds: Option<u64>,
}

impl Default for RunBudget {
    fn default() -> Self {
        Self {
            max_steps: default_max_total_steps(),
            max_tokens: None,
            max_cost_usd: None,
            max_elapsed_seconds: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_output_limit_resolves_default_and_overrides() {
        let omitted: RuntimeLimits = toml::from_str("").unwrap();
        let default = policy::limits::DEFAULT_COMMAND_OUTPUT_LIMIT_BYTES;
        assert_eq!(default, 16 * 1024 * 1024);
        assert_eq!(omitted.command_output_limit_bytes, None);
        assert_eq!(omitted.command_output_limit(None), Some(default));
        assert_eq!(
            omitted.command_output_limit(Some(default + 1)),
            Some(default + 1)
        );
        let explicit: RuntimeLimits = toml::from_str("command_output_limit_bytes = 42").unwrap();
        assert_eq!(explicit.command_output_limit(None), Some(42));
        assert_eq!(explicit.command_output_limit(Some(12)), Some(12));
        assert_eq!(omitted.command_input_limit_bytes, None);
    }

    #[test]
    fn the_http_body_limit_has_a_default() {
        // The body is read into memory, so an omitted limit must not mean
        // an unbounded read: the policy default bounds one exchange.
        let unset = RuntimeLimits::default();
        assert_eq!(
            unset.effective_http_body_limit_bytes(),
            policy::DEFAULT_HTTP_BODY_LIMIT_BYTES
        );
        let explicit = RuntimeLimits {
            http_body_limit_bytes: Some(1024),
            ..RuntimeLimits::default()
        };
        assert_eq!(explicit.effective_http_body_limit_bytes(), 1024);
    }

    #[test]
    fn prompt_cache_policy_requires_an_explicit_value() {
        let unset: LlmConfig =
            toml::from_str("max_tokens = 128").expect("unset cache should parse");
        assert_eq!(unset.cache, PromptCachePolicy::Off);

        let off: LlmConfig =
            toml::from_str("max_tokens = 128\ncache = \"off\"").expect("off should parse");
        assert_eq!(off.cache, PromptCachePolicy::Off);

        let auto: LlmConfig =
            toml::from_str("max_tokens = 128\ncache = \"auto\"").expect("auto should parse");
        assert_eq!(auto.cache, PromptCachePolicy::Auto);

        let error = toml::from_str::<LlmConfig>("max_tokens = 128\ncache = \"always\"")
            .expect_err("unknown cache value must fail");
        assert!(error.to_string().contains("unknown variant"), "{error}");
    }

    #[test]
    fn llm_container_defaults_preserve_missing_and_null_fields() {
        let config: LlmConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(config.cache, PromptCachePolicy::Off);
        assert_eq!(config.cache_write_policy, CacheWritePolicy::Allow);
        assert_eq!(config.cache_retention, None);
        assert!(config.stop_sequences.is_empty());
        assert!(config.requires.is_empty());
        for field in [
            "cache",
            "cache_write_policy",
            "stop_sequences",
            "requires",
            "structured_output",
        ] {
            assert!(serde_json::from_value::<LlmConfig>(serde_json::json!({field: null})).is_err());
        }
        let nullable: LlmConfig = serde_json::from_value(serde_json::json!({
            "model": null, "cache_retention": null
        }))
        .unwrap();
        assert_eq!(nullable.model.map(|model| model.model), None);
        assert_eq!(nullable.cache_retention, None);
        let policy: LlmRequestPolicy = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(policy.clear.is_empty());
        assert!(policy.requires.is_empty());
        for field in ["clear", "requires"] {
            assert!(
                serde_json::from_value::<LlmRequestPolicy>(serde_json::json!({field: null}))
                    .is_err()
            );
        }
    }

    #[test]
    fn cache_control_defaults_and_combination_rule() {
        let unset: LlmConfig =
            toml::from_str("max_tokens = 128").expect("unset cache should parse");
        assert_eq!(unset.cache, PromptCachePolicy::Off);
        assert_eq!(unset.cache_write_policy, CacheWritePolicy::Allow);
        assert_eq!(unset.cache_retention, None);
        unset
            .validate_cache_policy()
            .expect("defaults must validate");

        let extended: LlmConfig =
            toml::from_str("max_tokens = 128\ncache = \"auto\"\ncache_retention = \"extended\"")
                .expect("extended retention should parse");
        assert_eq!(extended.cache_retention, Some(CacheRetention::Extended));
        extended
            .validate_cache_policy()
            .expect("auto+allow+extended must validate");

        let off_extended: LlmConfig =
            toml::from_str("max_tokens = 128\ncache_retention = \"extended\"")
                .expect("off+extended should parse");
        assert!(
            off_extended.validate_cache_policy().is_err(),
            "retention requires cache=auto"
        );

        let forbid_extended: LlmConfig = toml::from_str(
            "max_tokens = 128\ncache = \"auto\"\ncache_write_policy = \"forbid\"\ncache_retention = \"extended\"",
        )
        .expect("forbid+extended should parse");
        assert!(
            forbid_extended.validate_cache_policy().is_err(),
            "retention requires write allow"
        );
    }
}
