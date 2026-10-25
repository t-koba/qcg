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
    /// Sanitized provider `error.code` observed on this attempt, when any.
    /// Bounded to 1..=64 ASCII `A-Z a-z 0-9 . _ -`; absent when no
    /// code was present, malformed, oversized, or the body was unreadable.
    /// Capture alone changes nothing: it is inert unless a provider rule
    /// explicitly matches it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    /// Bounded `Retry-After` delay observed on this attempt, in seconds.
    /// Capped at 60; absent when missing, invalid, or in the past.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<u64>,
    /// Whether this decision will apply the observed `Retry-After` delay.
    /// True only when a matching rule opts in and the effective action still
    /// retries; terminal guards and route fallbacks never wait.
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
