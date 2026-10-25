use super::*;
use llm::{RequestDispatch, RetryPolicy, TransportFacts, TransportPhase};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};
use tokio::time::Duration;

struct TransportProvider {
    id: &'static str,
    failures: usize,
    calls: AtomicUsize,
    policy: RetryPolicy,
    capabilities: Capabilities,
    emit: bool,
    backoff: Duration,
    failed: Notify,
}

impl TransportProvider {
    fn new(id: &'static str, failures: usize, action: &str) -> Self {
        Self {
            id,
            failures,
            calls: AtomicUsize::new(0),
            emit: false,
            capabilities: Capabilities {
                seed: true,
                streaming: true,
                ..Capabilities::default()
            },
            policy: serde_json::from_value(json!({"rules": [{
                "kind": "network", "phase": "connect", "dispatch": "not_sent",
                "action": action, "on_exhausted": "next_route"
            }]}))
            .unwrap(),
            backoff: Duration::ZERO,
            failed: Notify::new(),
        }
    }
    fn fail(&self) -> Result<(), LlmError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.failures {
            self.failed.notify_one();
            Err(LlmError {
                message: "transport unavailable".into(),
                kind: LlmErrorKind::Network,
                facts: TransportFacts {
                    phase: TransportPhase::Connect,
                    dispatch: RequestDispatch::NotSent,
                },
                ..Default::default()
            })
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl LlmProvider for TransportProvider {
    fn id(&self) -> &str {
        self.id
    }
    fn capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }
    fn retry_policy(&self) -> RetryPolicy {
        self.policy.clone()
    }
    fn retry_attempts(&self) -> usize {
        3
    }
    fn stream_retry_attempts(&self) -> usize {
        2
    }
    fn retry_base_backoff(&self) -> Duration {
        self.backoff
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.fail()?;
        Ok(ChatResponse {
            content: vec![ChatContent::Text("safe".into())],
            usage: TokenUsage {
                input: 3,
                output: 2,
                ..Default::default()
            },
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
    async fn stream(
        &self,
        _: ChatRequest,
        tx: mpsc::Sender<llm::ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        if self.emit {
            tx.send(llm::ChatStreamEvent::TextDelta {
                text: "visible".into(),
            })
            .await
            .unwrap();
        }
        self.fail()
    }
    async fn decide(&self, req: llm::DecisionRequest) -> Result<llm::DecisionResponse, LlmError> {
        self.fail()?;
        Ok(llm::DecisionResponse {
            model: req.model,
            answers: BTreeMap::from([("accept".into(), llm::DecisionAnswer::Noul { noul: 0.8 })]),
            usage: llm::DecisionUsage {
                input_tokens: 3,
                output_tokens: 2,
            },
        })
    }
}

fn router(
    primary: Arc<TransportProvider>,
    fallback: Arc<TransportProvider>,
) -> Arc<dyn LlmProvider> {
    let mut router = llm::LlmRouter::parse_text("").unwrap();
    router.register(primary);
    router.register(fallback);
    Arc::new(router)
}

fn events(dir: &Utf8PathBuf) -> Vec<serde_json::Value> {
    persisted_text(dir)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn gateway_distinguishes_internal_retries_from_declared_fallback_and_records_success() {
    for (action, failures, primary_calls, fallback_calls) in [
        ("retry", 1, 2, 0),
        ("retry", 10, 3, 1),
        ("next_route", 10, 1, 1),
        ("fail_fast", 10, 1, 0),
    ] {
        let primary = Arc::new(TransportProvider::new("primary", failures, action));
        let fallback = Arc::new(TransportProvider::new("fallback", 0, "fail_fast"));
        let dir = test_dir("transport-policy");
        let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
        let secrets = SecretStore::default();
        let gateway = test_gateway(
            router(primary.clone(), fallback.clone()),
            &secrets,
            &journal,
        );
        let mut request = test_request();
        request.provider = "primary".into();
        let result = gateway
            .complete(
                &test_node(),
                request,
                &test_routes(&[("primary", "test"), ("fallback", "safe")]),
                |_| json!({"retry_decisions": "cannot replace provenance"}),
            )
            .await;
        assert_eq!(result.is_ok(), action != "fail_fast");
        assert_eq!(primary.calls.load(Ordering::SeqCst), primary_calls);
        assert_eq!(fallback.calls.load(Ordering::SeqCst), fallback_calls);
        let recorded = events(&dir);
        for event in &recorded {
            api::RunEvent::from_flat(event)
                .expect("new evidence must decode through the typed public event");
        }
        let failed = recorded
            .iter()
            .find(|event| event["t"] == "llm_route_failed");
        if failures == 1 {
            assert!(failed.is_none());
            let call = recorded
                .iter()
                .find(|event| event["t"] == "llm_call")
                .unwrap();
            assert_eq!(call["retry_decisions"][0]["action"], "retry");
            assert_eq!(call["retry_decisions"][0]["source"], "rule:0");
            assert_eq!(call["retry_decisions"][0]["facts"]["dispatch"], "not_sent");
        } else {
            let failed = failed.unwrap();
            assert_eq!(
                failed["retry_decisions"].as_array().unwrap().len(),
                primary_calls
            );
            assert_eq!(failed["fallback_taken"], fallback_calls > 0);
        }
        assert_eq!(
            journal.state().budget.tokens_input,
            if result.is_ok() { 3 } else { 0 }
        );
        assert_eq!(
            journal.state().budget.tokens_output,
            if result.is_ok() { 2 } else { 0 }
        );
    }
}

#[tokio::test]
async fn gateway_revalidates_fallback_model_without_stripping_seed_or_effort() {
    for stream in [false, true] {
        for effort in [false, true] {
            let mut first = TransportProvider::new("primary", 10, "next_route");
            first.capabilities.reasoning_effort = vec![model::ReasoningEffort::High];
            let primary = Arc::new(first);
            let mut second = TransportProvider::new("fallback", 0, "fail_fast");
            second.capabilities.seed = false;
            let fallback = Arc::new(second);
            let dir = test_dir("fallback-revalidation");
            let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
            let secrets = SecretStore::default();
            let gateway = test_gateway(
                router(primary.clone(), fallback.clone()),
                &secrets,
                &journal,
            );
            let mut request = test_request();
            request.provider = "primary".into();
            request.stream = stream;
            if effort {
                request.reasoning_effort = Some(model::ReasoningEffort::High);
            } else {
                request.seed = Some(42);
            }
            let error = gateway
                .complete(
                    &test_node(),
                    request,
                    &test_routes(&[("primary", "test"), ("fallback", "safe")]),
                    |_| json!({}),
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(if effort { "reasoning_effort" } else { "seed" })
            );
            assert_eq!(primary.calls.load(Ordering::SeqCst), 1);
            assert_eq!(fallback.calls.load(Ordering::SeqCst), 0);
            let failures = route_failure_events(&dir);
            assert_eq!(failures.len(), 2);
            assert_eq!(failures[1]["retry_decisions"][0]["guard"], "terminal_error");
            assert_eq!(failures[1]["fallback_taken"], false);
        }
    }
}

#[tokio::test]
async fn emitted_stream_never_falls_back_even_when_policy_requests_it() {
    let mut first = TransportProvider::new("primary", 10, "next_route");
    first.emit = true;
    let primary = Arc::new(first);
    let fallback = Arc::new(TransportProvider::new("fallback", 0, "fail_fast"));
    let dir = test_dir("stream-policy-guard");
    let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
    let secrets = SecretStore::default();
    let gateway = test_gateway(
        router(primary.clone(), fallback.clone()),
        &secrets,
        &journal,
    );
    let mut request = test_request();
    request.provider = "primary".into();
    request.stream = true;
    assert!(
        gateway
            .complete(
                &test_node(),
                request,
                &test_routes(&[("primary", "test"), ("fallback", "safe")]),
                |_| json!({})
            )
            .await
            .is_err()
    );
    assert_eq!(primary.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fallback.calls.load(Ordering::SeqCst), 0);
    let failures = route_failure_events(&dir);
    assert_eq!(failures[0]["fallback_taken"], false);
    assert_eq!(failures[0]["retry_decisions"][0]["guard"], "stream_emitted");
}

#[tokio::test]
async fn cancellation_during_backoff_does_not_retry_or_fallback_and_keeps_trace() {
    let mut first = TransportProvider::new("primary", 10, "retry");
    first.backoff = Duration::from_secs(60);
    let primary = Arc::new(first);
    let fallback = Arc::new(TransportProvider::new("fallback", 0, "fail_fast"));
    let dir = test_dir("retry-cancel");
    let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
    let secrets = SecretStore::default();
    let token = CancellationToken::new();
    let gateway = LlmGateway::new(
        router(primary.clone(), fallback.clone()),
        &secrets,
        &journal,
        token.clone(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: None,
            require_pricing: false,
        },
        vec![],
    );
    let canceler = tokio::spawn({
        let primary = primary.clone();
        async move {
            primary.failed.notified().await;
            token.cancel();
        }
    });
    let mut request = test_request();
    request.provider = "primary".into();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        gateway.complete(
            &test_node(),
            request,
            &test_routes(&[("primary", "test"), ("fallback", "safe")]),
            |_| json!({}),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(StepError::Cancelled)));
    canceler.await.unwrap();
    assert_eq!(primary.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fallback.calls.load(Ordering::SeqCst), 0);
    let failures = route_failure_events(&dir);
    assert_eq!(failures[0]["retry_decisions"][0]["action"], "retry");
    assert_eq!(failures[0]["retry_decisions"][1]["kind"], "canceled");
    assert!(failures[0]["retry_decisions"][1]["attempt"].is_null());
    assert_eq!(failures[0]["retry_decisions"][1]["source"], "gateway");
    assert_eq!(failures[0]["fallback_taken"], false);
}

#[tokio::test]
async fn decision_retry_success_is_journaled_and_accounted_once() {
    let primary = Arc::new(TransportProvider::new("primary", 1, "retry"));
    let fallback = Arc::new(TransportProvider::new("fallback", 0, "fail_fast"));
    let dir = test_dir("decision-policy");
    let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
    let secrets = SecretStore::default();
    let gateway = test_gateway(router(primary.clone(), fallback), &secrets, &journal);
    let request = serde_json::from_value(json!({"provider": "primary", "model": "test", "state": {}, "questions": {"accept": {"type": "noul", "instructions": "Accept?"}}})).unwrap();
    gateway
        .decide(
            &test_node(),
            request,
            &test_routes(&[("primary", "test")])[0],
            8,
        )
        .await
        .unwrap();
    let recorded = events(&dir);
    let call = recorded
        .iter()
        .find(|event| event["t"] == "llm_call")
        .unwrap();
    assert_eq!(call["retry_decisions"][0]["source"], "rule:0");
    assert_eq!(primary.calls.load(Ordering::SeqCst), 2);
    assert_eq!(journal.state().budget.tokens_input, 3);
    assert_eq!(journal.state().budget.tokens_output, 2);
    api::RunEvent::from_flat(call).unwrap();
}
