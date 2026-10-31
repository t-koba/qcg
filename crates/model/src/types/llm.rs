use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Provider-neutral reasoning effort requested for a model invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

/// Provider-neutral policy for whether a model may call an exposed tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(ToolChoiceMode),
    Tool { tool: String },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoiceMode {
    None,
    #[default]
    Auto,
    Required,
}

impl ToolChoice {
    pub const fn none() -> Self {
        Self::Mode(ToolChoiceMode::None)
    }

    pub const fn auto() -> Self {
        Self::Mode(ToolChoiceMode::Auto)
    }

    pub const fn required() -> Self {
        Self::Mode(ToolChoiceMode::Required)
    }
}

impl Default for ToolChoice {
    fn default() -> Self {
        Self::auto()
    }
}

/// Provider-neutral control for the detail of a model response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResponseVerbosity {
    Low,
    Medium,
    High,
}

impl ReasoningEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }

    /// Parses the provider-neutral name. Returns `None` for unknown values so
    /// callers can distinguish a typo from an unsupported dynamic value.
    pub fn from_snake_case(value: &str) -> Option<Self> {
        Some(match value {
            "none" => Self::None,
            "minimal" => Self::Minimal,
            "low" => Self::Low,
            "medium" => Self::Medium,
            "high" => Self::High,
            "xhigh" => Self::Xhigh,
            "max" => Self::Max,
            _ => return None,
        })
    }
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The provider's own hosted web search. It is a separate mechanism from the
/// local `web.search` tool, from MCP tools and from the external search
/// adapter: usage follows the configured provider-billing unit, and the
/// fetch and its network egress happen on the provider's side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderWebSearch {
    /// Domains the hosted search may visit. Mutually exclusive with
    /// `blocked_domains`.
    pub allowed_domains: Vec<String>,
    /// Domains the hosted search must not visit. Mutually exclusive with
    /// `allowed_domains`.
    pub blocked_domains: Vec<String>,
    /// How much context the provider should retrieve per call. Absent leaves
    /// the provider's own default in place.
    pub search_context_size: Option<SearchContextSize>,
    /// Approximate location used to localize the hosted search.
    pub user_location: Option<UserLocation>,
    /// Upper bound on hosted search calls per request, never local function calls.
    pub max_calls: u32,
}

/// Default hosted-search cap; local `web.search` uses the same numeric default independently.
pub const DEFAULT_PROVIDER_WEB_SEARCH_MAX_CALLS: u32 = 3;

impl Default for ProviderWebSearch {
    fn default() -> Self {
        Self {
            allowed_domains: Vec::new(),
            blocked_domains: Vec::new(),
            search_context_size: None,
            user_location: None,
            max_calls: DEFAULT_PROVIDER_WEB_SEARCH_MAX_CALLS,
        }
    }
}

/// How much context the provider should retrieve for one hosted search.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchContextSize {
    Low,
    #[default]
    Medium,
    High,
}

/// An approximate location, used by the provider to localize a hosted search.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct UserLocation {
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,
    pub timezone: Option<String>,
}

/// What one hosted search call did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProviderSearchAction {
    #[default]
    Search,
    OpenPage,
    FindInPage,
}

/// One hosted search call reported by the provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderSearchEvent {
    /// The provider's identifier for the call, when it reports one.
    pub id: Option<String>,
    pub action: ProviderSearchAction,
    /// The queries a search action ran. Empty for page actions.
    pub queries: Vec<String>,
    /// The page a page action opened. Absent for a search action.
    pub url: Option<String>,
}

/// One citation the provider attached to its answer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct UrlCitation {
    pub url: String,
    pub title: Option<String>,
    /// The cited range inside the answer text, when the provider reports one.
    pub start_index: Option<u64>,
    pub end_index: Option<u64>,
}

/// What one response did with hosted search. `calls` counts provider-billable units,
/// mapped by the configured billing mode, not action events. `None` means unknown,
/// never zero; events and citations remain available independently of metering.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderWebSearchOutcome {
    pub calls: Option<u64>,
    pub events: Vec<ProviderSearchEvent>,
    pub citations: Vec<UrlCitation>,
}
