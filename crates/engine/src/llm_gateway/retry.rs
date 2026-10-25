use llm::{LlmError, LlmProvider, RetryDecision, RetryObserver};
use std::sync::{Arc, Mutex};

/// Per-call, bounded by the provider attempt limits; never stores messages,
/// request data, or credentials. Shared only with the synchronous observer.
#[derive(Default)]
pub(super) struct RetryTrace(Arc<Mutex<Vec<RetryDecision>>>);

impl RetryTrace {
    pub fn observer(&self) -> RetryObserver {
        let trace = Arc::clone(&self.0);
        Arc::new(move |decision| {
            trace
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(decision.clone())
        })
    }

    pub fn decisions(&self) -> Vec<RetryDecision> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn finish_error(
        &self,
        provider: &dyn LlmProvider,
        mut error: LlmError,
        stream: bool,
    ) -> LlmError {
        let decisions = self.decisions();
        if !decisions.is_empty() {
            error.decisions = decisions;
        }
        // Direct/custom providers need a terminal decision too. Cancellation
        // or the gateway's partial-stream guard may replace a router error.
        if error
            .decisions
            .last()
            .is_none_or(|decision| decision.kind != error.kind)
        {
            let mut decision = provider.retry_policy().decide(
                &error,
                error.decisions.len() + 1,
                error.decisions.len() + 1,
                stream,
                error.kind == llm::LlmErrorKind::PartialStream,
            );
            if error.kind == llm::LlmErrorKind::Canceled {
                // Cancellation can arrive during backoff OR an active next
                // attempt. The gateway must not invent an exact count.
                decision.attempt = None;
                decision.source = "gateway".into();
            } else if error.kind == llm::LlmErrorKind::PartialStream {
                // This is the router error wrapped by the publication guard,
                // not another provider attempt.
                decision.attempt = error.decisions.last().and_then(|d| d.attempt);
                decision.source = "gateway".into();
                decision.facts = llm::TransportFacts {
                    phase: llm::TransportPhase::Response,
                    dispatch: llm::RequestDispatch::Sent,
                };
            }
            error.decisions.push(decision);
        }
        error
    }
}
