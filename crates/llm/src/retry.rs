use crate::LlmError;
pub use model::{
    RequestDispatch, RetryAction, RetryDecision, RetryGuard, TransportFacts, TransportPhase,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type RetryObserver = Arc<dyn Fn(&RetryDecision) + Send + Sync>;

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
    pub action: RetryAction,
    /// What a bounded retry does when its attempt allowance is exhausted.
    #[serde(default = "fail_fast")]
    pub on_exhausted: RetryAction,
}

fn fail_fast() -> RetryAction {
    RetryAction::FailFast
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
        use crate::LlmErrorKind;
        let matched = self.rules.iter().enumerate().find(|(_, rule)| {
            rule.kind.is_none_or(|v| v == error.kind)
                && rule.phase.is_none_or(|v| v == error.facts.phase)
                && rule.dispatch.is_none_or(|v| v == error.facts.dispatch)
                && rule.stream.is_none_or(|v| v == stream)
                && rule.attempt_from.is_none_or(|v| attempt >= v)
                && rule.attempt_through.is_none_or(|v| attempt <= v)
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
        };
        if emitted {
            decision.action = RetryAction::FailFast;
            decision.guard = Some(RetryGuard::StreamEmitted);
        } else if matches!(
            error.kind,
            LlmErrorKind::Canceled | LlmErrorKind::PartialStream | LlmErrorKind::Other
        ) {
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
        decision
    }
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
        if matches!(
            self.kind,
            crate::LlmErrorKind::Canceled
                | crate::LlmErrorKind::PartialStream
                | crate::LlmErrorKind::Other
        ) {
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
