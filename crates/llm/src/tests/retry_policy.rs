use super::*;
use std::sync::Mutex;

fn error(kind: LlmErrorKind, phase: TransportPhase, dispatch: RequestDispatch) -> LlmError {
    LlmError {
        kind,
        facts: TransportFacts { phase, dispatch },
        ..Default::default()
    }
}

fn policy(action: &str, exhausted: &str) -> RetryPolicy {
    serde_json::from_value(json!({"rules": [{
        "kind": "network", "phase": "connect", "dispatch": "not_sent",
        "action": action, "on_exhausted": exhausted
    }]}))
    .unwrap()
}

#[test]
fn policy_matches_structured_facts_in_order_and_reports_bounds() {
    let policy: RetryPolicy = serde_json::from_value(json!({"rules": [
        {"kind": "network", "phase": "connect", "dispatch": "not_sent", "attempt_through": 1, "action": "retry"},
        {"kind": "network", "phase": "connect", "dispatch": "not_sent", "attempt_from": 2, "action": "next_route"},
        {"kind": "network", "stream": true, "action": "fail_fast"}
    ]})).unwrap();
    let connect = error(
        LlmErrorKind::Network,
        TransportPhase::Connect,
        RequestDispatch::NotSent,
    );
    let first = policy.decide(&connect, 1, 3, false, false);
    assert_eq!(
        (first.source.as_str(), first.action),
        ("rule:0", RetryAction::Retry)
    );
    assert_eq!(policy.decide(&connect, 2, 3, false, false).source, "rule:1");
    assert_eq!(
        policy.decide(&connect, 2, 3, false, false).action,
        RetryAction::NextRoute
    );
    let bounded = policy.decide(&connect, 1, 1, false, false);
    assert_eq!(
        (bounded.action, bounded.guard),
        (RetryAction::FailFast, Some(RetryGuard::AttemptLimit))
    );
    let ambiguous = error(
        LlmErrorKind::Network,
        TransportPhase::Unknown,
        RequestDispatch::Unknown,
    );
    assert_eq!(
        policy.decide(&ambiguous, 1, 3, false, false).source,
        "default"
    );
    assert_eq!(
        policy.decide(&ambiguous, 1, 3, false, false).action,
        RetryAction::FailFast
    );
    assert_eq!(
        policy.decide(&ambiguous, 1, 3, true, false).source,
        "rule:2"
    );
    let post_dispatch = error(
        LlmErrorKind::Network,
        TransportPhase::Response,
        RequestDispatch::Sent,
    );
    assert_eq!(
        policy.decide(&post_dispatch, 1, 3, false, false).action,
        RetryAction::FailFast
    );
}

#[test]
fn defaults_and_mechanism_guards_are_explicit() {
    let default = RetryPolicy::default();
    for kind in [
        LlmErrorKind::TimedOut,
        LlmErrorKind::EmptyResponse,
        LlmErrorKind::HttpStatus(429),
        LlmErrorKind::HttpStatus(503),
    ] {
        let error = LlmError {
            kind,
            ..Default::default()
        };
        assert_eq!(
            default.decide(&error, 1, 3, false, false).action,
            RetryAction::Retry
        );
        assert_eq!(
            default.decide(&error, 3, 3, false, false).action,
            RetryAction::NextRoute
        );
        assert_eq!(
            default.decide(&error, 1, 1, true, false).guard,
            Some(RetryGuard::AttemptLimit)
        );
    }
    let any: RetryPolicy = serde_json::from_value(
        json!({"rules": [{"action": "retry", "on_exhausted": "next_route"}]}),
    )
    .unwrap();
    for kind in [
        LlmErrorKind::Canceled,
        LlmErrorKind::PartialStream,
        LlmErrorKind::Other,
    ] {
        let decision = any.decide(
            &LlmError {
                kind,
                ..Default::default()
            },
            1,
            3,
            false,
            false,
        );
        assert_eq!(
            (decision.action, decision.guard),
            (RetryAction::FailFast, Some(RetryGuard::TerminalError))
        );
    }
    let circuit = default.decide(
        &LlmError {
            kind: LlmErrorKind::CircuitOpen,
            ..Default::default()
        },
        1,
        3,
        false,
        false,
    );
    assert_eq!(
        (circuit.action, circuit.guard),
        (RetryAction::NextRoute, Some(RetryGuard::CircuitOpen))
    );
    let emitted = any.decide(&LlmError::default(), 1, 4, true, true);
    assert_eq!(
        (emitted.action, emitted.guard),
        (RetryAction::FailFast, Some(RetryGuard::StreamEmitted))
    );
    assert_eq!(
        default
            .decide(
                &error(
                    LlmErrorKind::Network,
                    TransportPhase::Connect,
                    RequestDispatch::NotSent
                ),
                1,
                3,
                false,
                false
            )
            .action,
        RetryAction::FailFast
    );
}

#[test]
fn registry_owns_policy_and_rejects_invalid_rules() {
    let text = format!(
        "{REGISTRY_FIXTURE}\n[[provider.retry_policy.rules]]\nkind = 'network'\nphase = 'connect'\ndispatch = 'not_sent'\naction = 'next_route'\n"
    );
    let router = LlmRouter::parse_text(&text).unwrap();
    assert_eq!(router.providers["local"].retry_policy().rules.len(), 1);
    let http = format!(
        "{text}\n[[provider.retry_policy.rules]]\nkind = {{ http_status = 503 }}\naction = 'retry'\non_exhausted = 'next_route'\n"
    );
    assert_eq!(
        LlmRouter::parse_text(&http).unwrap().providers["local"]
            .retry_policy()
            .rules[1]
            .kind,
        Some(LlmErrorKind::HttpStatus(503))
    );

    for invalid in [
        "attempt_from = 0",
        "attempt_from = 3\nattempt_through = 2",
        "on_exhausted = 'retry'",
        "typo = true",
    ] {
        assert!(
            LlmRouter::parse_text(&format!("{text}\n{invalid}\n")).is_err(),
            "{invalid}"
        );
    }
}

#[tokio::test]
async fn reqwest_connect_and_body_facts_are_not_inferred_from_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let raw = client
        .get(format!("http://{address}"))
        .send()
        .await
        .unwrap_err();
    assert!(raw.is_connect());
    let mapped = llm_http_error(raw);
    assert_eq!(mapped.kind, LlmErrorKind::Network);
    assert_eq!(
        mapped.facts,
        TransportFacts {
            phase: TransportPhase::Connect,
            dispatch: RequestDispatch::NotSent
        }
    );

    // A request can time out after dispatch while waiting for response headers.
    // Keep a loopback connection open until reqwest times out, and synchronize
    // on the server reading the request rather than relying on a sleep.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (received_tx, received_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let received = stream.read(&mut request).is_ok_and(|count| count > 0);
        received_tx.send(received).unwrap();
        let _ = release_rx.recv_timeout(Duration::from_secs(5));
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let raw = client
        .get(format!("http://{address}"))
        .send()
        .await
        .unwrap_err();
    assert!(raw.is_timeout());
    assert!(!raw.is_connect());
    assert!(received_rx.recv_timeout(Duration::from_secs(5)).unwrap());
    let mapped = llm_http_error(raw);
    assert_eq!(mapped.kind, LlmErrorKind::TimedOut);
    assert_eq!(mapped.facts, TransportFacts::default());
    release_tx.send(()).unwrap();
    server.join().unwrap();

    let (url, server) = spawn_http_response(200, "short".into(), "".into());
    let response = client.get(url).send().await.unwrap();
    // Decoding a text body as JSON is a response-side failure.
    let raw = response.json::<Value>().await.unwrap_err();
    let mapped = llm_response_error(raw);
    assert_eq!(mapped.kind, LlmErrorKind::InvalidResponse);
    assert_eq!(
        mapped.facts,
        TransportFacts {
            phase: TransportPhase::Response,
            dispatch: RequestDispatch::Sent
        }
    );
    server.join().unwrap();
}

struct PolicyProvider {
    calls: AtomicUsize,
    fail_count: usize,
    policy: RetryPolicy,
    kind: LlmErrorKind,
    emit: bool,
}

impl PolicyProvider {
    fn fail(&self) -> Result<(), LlmError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.fail_count {
            Err(error(
                self.kind,
                TransportPhase::Connect,
                RequestDispatch::NotSent,
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl LlmProvider for PolicyProvider {
    fn id(&self) -> &str {
        "policy"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
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
        Duration::ZERO
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.fail()?;
        Ok(ChatResponse {
            content: vec![ChatContent::Text("ok".into())],
            usage: TokenUsage::default(),
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
    async fn decide(&self, req: DecisionRequest) -> Result<DecisionResponse, LlmError> {
        self.fail()?;
        Ok(serde_json::from_value(decision_fixture_answer(&req.model)).unwrap())
    }
    async fn stream(
        &self,
        _: ChatRequest,
        tx: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        if self.emit {
            tx.send(ChatStreamEvent::TextDelta {
                text: "visible".into(),
            })
            .await
            .unwrap();
        }
        self.fail()
    }
}

fn setup(
    policy: RetryPolicy,
    fail_count: usize,
    kind: LlmErrorKind,
    emit: bool,
) -> (
    LlmRouter,
    Arc<PolicyProvider>,
    Arc<Mutex<Vec<RetryDecision>>>,
    RetryObserver,
) {
    let provider = Arc::new(PolicyProvider {
        policy,
        fail_count,
        kind,
        emit,
        calls: AtomicUsize::new(0),
    });
    let mut router = LlmRouter::parse_text("").unwrap();
    router.register(provider.clone());
    let decisions = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&decisions);
    let observer: RetryObserver =
        Arc::new(move |decision| capture.lock().unwrap().push(decision.clone()));
    (router, provider, decisions, observer)
}

#[tokio::test]
async fn retry_success_and_exhaustion_keep_provenance_for_complete_and_decide() {
    for decide in [false, true] {
        for failures in [1, 10] {
            let (router, provider, decisions, observer) = setup(
                policy("retry", "next_route"),
                failures,
                LlmErrorKind::Network,
                false,
            );
            let result = if decide {
                router
                    .decide_observed(decision_fixture_request("policy", "test"), observer)
                    .await
                    .map(|_| ())
            } else {
                let mut request = sample_request();
                request.provider = "policy".into();
                router
                    .complete_observed(request, observer)
                    .await
                    .map(|_| ())
            };
            assert_eq!(
                provider.calls.load(Ordering::SeqCst),
                if failures == 1 { 2 } else { 3 }
            );
            let trace = decisions.lock().unwrap();
            assert_eq!(trace.len(), if failures == 1 { 1 } else { 3 });
            assert!(trace.iter().all(|d| d.source == "rule:0"));
            if failures == 1 {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.decisions, *trace);
                assert_eq!(trace.last().unwrap().action, RetryAction::NextRoute);
                assert!(error.allows_next_route());
            }
        }
    }
}

#[tokio::test]
async fn next_route_and_fail_fast_do_not_spend_internal_attempts() {
    for action in ["fail_fast", "next_route"] {
        let (router, provider, decisions, observer) = setup(
            policy(action, "fail_fast"),
            10,
            LlmErrorKind::Network,
            false,
        );
        let mut request = sample_request();
        request.provider = "policy".into();
        let error = router
            .complete_observed(request, observer)
            .await
            .unwrap_err();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(decisions.lock().unwrap().len(), 1);
        assert_eq!(error.allows_next_route(), action == "next_route");
    }
}

#[tokio::test]
async fn stream_uses_policy_bounds_and_never_retries_or_falls_back_after_emission() {
    for emit in [false, true] {
        let (router, provider, decisions, observer) = setup(
            policy("retry", "next_route"),
            10,
            LlmErrorKind::Network,
            emit,
        );
        let mut request = stream_request();
        request.provider = "policy".into();
        let (tx, mut rx) = mpsc::channel(8);
        let error = router
            .stream_observed(request, tx, observer)
            .await
            .unwrap_err();
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            if emit { 1 } else { 3 }
        );
        let trace = decisions.lock().unwrap();
        assert_eq!(
            trace.last().unwrap().guard,
            Some(if emit {
                RetryGuard::StreamEmitted
            } else {
                RetryGuard::AttemptLimit
            })
        );
        assert_eq!(error.allows_next_route(), !emit);
        assert_eq!(rx.try_recv().is_ok(), emit);
    }
    let (router, provider, _, observer) =
        setup(RetryPolicy::default(), 10, LlmErrorKind::Other, false);
    let mut request = stream_request();
    request.provider = "policy".into();
    let (tx, _) = mpsc::channel(8);
    assert!(router.stream_observed(request, tx, observer).await.is_err());
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        1,
        "untyped errors no longer get blanket stream retries"
    );
}

#[tokio::test]
async fn successful_stream_retry_reports_the_recovered_attempt() {
    let (router, provider, decisions, observer) = setup(
        policy("retry", "fail_fast"),
        1,
        LlmErrorKind::Network,
        false,
    );
    let mut request = stream_request();
    request.provider = "policy".into();
    let (tx, _) = mpsc::channel(8);
    router.stream_observed(request, tx, observer).await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    let trace = decisions.lock().unwrap();
    assert_eq!(trace.len(), 1);
    assert_eq!(trace[0].source, "rule:0");
    assert_eq!(trace[0].action, RetryAction::Retry);
    assert!(trace[0].stream);
}
