//! Structured, credential-free transport policy evidence shared by runtime and events.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LlmErrorKind {
    HttpStatus(u16),
    TimedOut,
    Network,
    EmptyResponse,
    InvalidResponse,
    PartialStream,
    Canceled,
    CircuitOpen,
    #[default]
    Other,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransportPhase {
    Connect,
    Response,
    #[default]
    Unknown,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequestDispatch {
    NotSent,
    Sent,
    #[default]
    Unknown,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransportFacts {
    pub phase: TransportPhase,
    pub dispatch: RequestDispatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetryAction {
    FailFast,
    Retry,
    NextRoute,
}

/// One failed provider attempt, including the effective bounded decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryDecision {
    pub kind: LlmErrorKind,
    pub facts: TransportFacts,
    /// One-based attempt within this route (not the fallback index). Null
    /// when gateway cancellation cannot know which router attempt is active.
    pub attempt: Option<usize>,
    pub stream: bool,
    pub requested: RetryAction,
    pub action: RetryAction,
    /// `default`, `rule:<zero-based index>`, or `gateway` for terminal guards.
    pub source: String,
    /// A mechanism that overrode policy, if any.
    pub guard: Option<RetryGuard>,
    /// Sanitized provider `error.code`: 1..=64 ASCII `A-Z a-z 0-9 . _ -`.
    /// Absent if missing, malformed, oversized, or unreadable; inert unless a rule matches it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    /// Raw representable `Retry-After` seconds observed on this attempt,
    /// before provider policy clipping; absent when missing, invalid, or past.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_observed_secs: Option<u64>,
    /// Candidate: observed seconds clipped to provider max_retry_after_secs (default 60), even without opt-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<u64>,
    /// Whether this decision uses the policy-bounded candidate as a backoff floor.
    /// True only for matching rule opt-in, an observation, and an effective retry;
    /// terminal guards and route fallbacks never wait.
    /// Does not imply extra wait if backoff already exceeds the candidate.
    #[serde(default)]
    pub retry_after_applied: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetryGuard {
    AttemptLimit,
    StreamEmitted,
    TerminalError,
    CircuitOpen,
}
