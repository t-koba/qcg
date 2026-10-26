use crate::{LlmError, LlmErrorKind};
pub use model::{
    RequestDispatch, RetryAction, RetryDecision, RetryGuard, TransportFacts, TransportPhase,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type RetryObserver = Arc<dyn Fn(&RetryDecision) + Send + Sync>;

/// Longest provider `error.code` retained for policy matching. Raw bodies
/// are never stored; longer or malformed codes are dropped.
pub const MAX_PROVIDER_ERROR_CODE_LEN: usize = 64;

/// Longest observed `Retry-After` delay retained or applied, in seconds.
/// Existing backoff is unchanged; header delays require explicit rule opt-in.
pub(crate) const MAX_RETRY_AFTER_SECS: u64 = 60;

/// Returns the sanitized provider error code, or `None` when the raw value
/// is missing, empty, oversized, or contains characters outside
/// `A-Z a-z 0-9 . _ -`.
pub(crate) fn sanitize_provider_error_code(raw: &str) -> Option<String> {
    let code = raw.trim();
    if code.is_empty() || code.len() > MAX_PROVIDER_ERROR_CODE_LEN {
        return None;
    }
    if !code
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        return None;
    }
    Some(code.to_string())
}

/// First matching rule wins; omitted match fields are wildcards. Empty rules
/// use the documented default. Bounds and terminal guards cannot be bypassed.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    #[serde(default)]
    pub rules: Vec<RetryRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryRule {
    pub kind: Option<crate::LlmErrorKind>,
    pub phase: Option<TransportPhase>,
    pub dispatch: Option<RequestDispatch>,
    pub stream: Option<bool>,
    pub attempt_from: Option<usize>,
    pub attempt_through: Option<usize>,
    /// Optional exact provider `error.code` to match. `None` (the default)
    /// is a wildcard; `Some` matches only when the failure carries the same
    /// sanitized code. Captured codes are inert unless a rule names them.
    #[serde(default)]
    pub provider_code: Option<String>,
    /// Opt in to using the bounded `Retry-After` delay when this rule causes
    /// a same-route retry. `false` (the default) preserves current backoff;
    /// terminal guards and route fallbacks never wait.
    #[serde(default)]
    pub use_retry_after: bool,
    pub action: RetryAction,
    /// What a bounded retry does when its attempt allowance is exhausted.
    #[serde(default = "fail_fast")]
    pub on_exhausted: RetryAction,
}

fn fail_fast() -> RetryAction {
    RetryAction::FailFast
}

fn is_terminal_error_kind(kind: LlmErrorKind) -> bool {
    matches!(
        kind,
        LlmErrorKind::Canceled | LlmErrorKind::PartialStream | LlmErrorKind::Other
    )
}

impl RetryPolicy {
    pub(crate) fn validate(&self) -> Result<(), String> {
        for rule in &self.rules {
            if rule.attempt_from == Some(0)
                || rule.attempt_through == Some(0)
                || rule
                    .attempt_from
                    .zip(rule.attempt_through)
                    .is_some_and(|(a, b)| a > b)
                || rule.on_exhausted == RetryAction::Retry
            {
                return Err("retry_policy requires positive ordered attempt bounds and on_exhausted = fail_fast or next_route".into());
            }
            if let Some(code) = rule.provider_code.as_deref()
                && sanitize_provider_error_code(code).is_none_or(|sanitized| sanitized != code)
            {
                return Err(
                    "retry_policy provider_code must be 1..=64 ASCII alphanumeric, '.', '_' or '-'"
                        .into(),
                );
            }
            if rule.use_retry_after && rule.action != RetryAction::Retry {
                return Err("retry_policy use_retry_after requires action = retry".into());
            }
        }
        Ok(())
    }

    pub fn decide(
        &self,
        error: &LlmError,
        attempt: usize,
        max_attempts: usize,
        stream: bool,
        emitted: bool,
    ) -> RetryDecision {
        // Public error facts may come from custom providers, not just HTTP.
        // Sanitize before matching as well as before recording event facts.
        let provider_code = error
            .provider_code
            .as_deref()
            .and_then(sanitize_provider_error_code);
        let retry_after_secs = error
            .retry_after_secs
            .map(|secs| secs.min(MAX_RETRY_AFTER_SECS));
        let matched = self.rules.iter().enumerate().find(|(_, rule)| {
            rule.kind.is_none_or(|v| v == error.kind)
                && rule.phase.is_none_or(|v| v == error.facts.phase)
                && rule.dispatch.is_none_or(|v| v == error.facts.dispatch)
                && rule.stream.is_none_or(|v| v == stream)
                && rule.attempt_from.is_none_or(|v| attempt >= v)
                && rule.attempt_through.is_none_or(|v| attempt <= v)
                && rule
                    .provider_code
                    .as_ref()
                    .is_none_or(|want| provider_code.as_deref() == Some(want.as_str()))
        });
        let (requested, exhausted, source) = match matched {
            Some((index, rule)) => (rule.action, rule.on_exhausted, format!("rule:{index}")),
            // Preserve the legacy retryable set. Network stays fail-fast,
            // including connect failures unless an operator opts in.
            None if error.is_retryable() => {
                (RetryAction::Retry, RetryAction::NextRoute, "default".into())
            }
            None => (
                RetryAction::FailFast,
                RetryAction::FailFast,
                "default".into(),
            ),
        };
        // Even programmatically supplied (unvalidated) policy cannot turn
        // exhaustion into another retry. Registry validation also rejects it.
        let exhausted = if exhausted == RetryAction::Retry {
            RetryAction::FailFast
        } else {
            exhausted
        };
        let mut decision = RetryDecision {
            kind: error.kind,
            facts: error.facts,
            attempt: Some(attempt),
            stream,
            requested,
            action: requested,
            source,
            guard: None,
            provider_code,
            retry_after_secs,
            retry_after_applied: false,
        };
        if emitted {
            decision.action = RetryAction::FailFast;
            decision.guard = Some(RetryGuard::StreamEmitted);
        } else if is_terminal_error_kind(error.kind) {
            // Configuration/validation errors are not transport policy inputs.
            decision.action = RetryAction::FailFast;
            decision.guard = Some(RetryGuard::TerminalError);
        } else if error.kind == LlmErrorKind::CircuitOpen && requested == RetryAction::Retry {
            decision.action = exhausted;
            decision.guard = Some(RetryGuard::CircuitOpen);
        } else if requested == RetryAction::Retry && attempt >= max_attempts {
            decision.action = exhausted;
            decision.guard = Some(RetryGuard::AttemptLimit);
        }
        // Record an applied delay only when this failure will actually retry;
        // attempt limits, terminal guards, and route fallback do not sleep.
        decision.retry_after_applied = decision.action == RetryAction::Retry
            && matched.is_some_and(|(_, rule)| rule.use_retry_after)
            && decision.retry_after_secs.is_some();
        decision
    }
}

/// Effective wait for one retry attempt: the computed backoff, raised to
/// at least the observed `Retry-After` delay only when the matching rule
/// opted in (`Some` delay). Without opt-in (`None`) the base delay is
/// unchanged.
pub(crate) fn effective_retry_delay(
    base: std::time::Duration,
    applied_after_secs: Option<u64>,
) -> std::time::Duration {
    if let Some(secs) = applied_after_secs {
        return base.max(std::time::Duration::from_secs(
            secs.min(MAX_RETRY_AFTER_SECS),
        ));
    }
    base
}

impl RetryPolicy {
    pub(crate) fn observe_failure(
        &self,
        error: &mut LlmError,
        attempt: usize,
        max_attempts: usize,
        stream: bool,
        emitted: bool,
        observer: &RetryObserver,
    ) -> RetryAction {
        let decision = self.decide(error, attempt, max_attempts, stream, emitted);
        let action = decision.action;
        observer(&decision);
        error.decisions.push(decision);
        action
    }
}

impl LlmError {
    /// Router decisions take precedence; direct/custom providers retain the
    /// same default without creating undeclared routes.
    pub fn allows_next_route(&self) -> bool {
        if is_terminal_error_kind(self.kind) {
            return false;
        }
        self.decisions.last().map_or_else(
            || {
                RetryPolicy::default()
                    .decide(self, 1, 1, false, false)
                    .action
                    == RetryAction::NextRoute
            },
            |decision| decision.action == RetryAction::NextRoute,
        )
    }
}
