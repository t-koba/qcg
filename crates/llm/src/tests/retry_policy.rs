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
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    // Port zero is never a listener port, avoiding a race where another
    // parallel test rebinds a just-released ephemeral port.
    let raw = client.get("http://127.0.0.1:0").send().await.unwrap_err();
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

// --- Bounded provider error-code and Retry-After facts (wi_bb6819) ---

fn coded_error(code: &str) -> LlmError {
    LlmError {
        message: "rate limited".into(),
        kind: LlmErrorKind::HttpStatus(429),
        facts: TransportFacts {
            phase: TransportPhase::Response,
            dispatch: RequestDispatch::Sent,
        },
        provider_code: Some(code.into()),
        ..Default::default()
    }
}

#[test]
fn provider_code_match_distinguishes_identical_429() {
    // One rule names only the transient code; the quota code keeps defaults.
    let policy: RetryPolicy = serde_json::from_value(json!({"rules": [{
        "kind": {"http_status": 429},
        "provider_code": "slow_down",
        "action": "retry",
        "on_exhausted": "next_route"
    }]}))
    .unwrap();
    let slow = coded_error("slow_down");
    let quota = LlmError {
        message: "quota".into(),
        kind: LlmErrorKind::HttpStatus(429),
        facts: TransportFacts {
            phase: TransportPhase::Response,
            dispatch: RequestDispatch::Sent,
        },
        provider_code: Some("insufficient_quota".into()),
        ..Default::default()
    };
    let slow_decision = policy.decide(&slow, 1, 3, false, false);
    assert_eq!(slow_decision.source, "rule:0");
    assert_eq!(slow_decision.action, RetryAction::Retry);
    assert_eq!(slow_decision.provider_code.as_deref(), Some("slow_down"));
    let quota_decision = policy.decide(&quota, 1, 3, false, false);
    assert_eq!(quota_decision.source, "default");
    assert_eq!(quota_decision.action, RetryAction::Retry);
    assert_eq!(
        quota_decision.provider_code.as_deref(),
        Some("insufficient_quota")
    );

    // A quota-specific rule selects fallback while the transient code keeps
    // the default retry: no OpenAI disposition is hardcoded.
    let billing: RetryPolicy = serde_json::from_value(json!({"rules": [{
        "kind": {"http_status": 429},
        "provider_code": "insufficient_quota",
        "action": "next_route"
    }]}))
    .unwrap();
    let quota_route = billing.decide(&quota, 1, 3, false, false);
    assert_eq!(
        (quota_route.source.as_str(), quota_route.action),
        ("rule:0", RetryAction::NextRoute)
    );
    let slow_default = billing.decide(&slow, 1, 3, false, false);
    assert_eq!(slow_default.source, "default");
    assert_eq!(slow_default.action, RetryAction::Retry);
}

#[test]
fn generic_code_capture_is_inert_without_a_matching_rule() {
    let default = RetryPolicy::default();
    let plain = error(
        LlmErrorKind::HttpStatus(429),
        TransportPhase::Response,
        RequestDispatch::Sent,
    );
    let coded = coded_error("slow_down");
    // Same status, same defaults: the captured code alone changes nothing.
    assert_eq!(
        default.decide(&plain, 1, 3, false, false).action,
        default.decide(&coded, 1, 3, false, false).action
    );
    assert_eq!(
        default.decide(&plain, 3, 3, false, false).action,
        RetryAction::NextRoute
    );
    assert_eq!(
        default.decide(&coded, 3, 3, false, false).action,
        RetryAction::NextRoute
    );
    // A rule without a code still matches coded failures (wildcard).
    let any: RetryPolicy = serde_json::from_value(
        json!({"rules": [{"kind": {"http_status": 429}, "action": "fail_fast"}]}),
    )
    .unwrap();
    assert_eq!(
        any.decide(&coded, 1, 3, false, false).action,
        RetryAction::FailFast
    );
}

#[test]
fn error_code_extraction_accepts_only_the_supported_shape() {
    use crate::http_provider::extract_provider_error_code;
    assert_eq!(
        extract_provider_error_code(&json!({"error": {"code": "slow_down"}})).as_deref(),
        Some("slow_down")
    );
    // Non-string, missing, or misplaced codes are dropped.
    assert!(extract_provider_error_code(&json!({"error": {"code": 42}})).is_none());
    assert!(extract_provider_error_code(&json!({"error": {}})).is_none());
    assert!(extract_provider_error_code(&json!({"code": "slow_down"})).is_none());
    assert!(extract_provider_error_code(&json!({"error": {"code": ""}})).is_none());
    assert!(extract_provider_error_code(&json!({"error": {"code": "has space"}})).is_none());
    assert!(extract_provider_error_code(&json!({"error": {"code": "semi;colon"}})).is_none());
    assert!(
        extract_provider_error_code(&json!({"error": {"code": "a".repeat(65).as_str()}})).is_none()
    );
    assert!(
        extract_provider_error_code(&json!({"error": {"code": "a".repeat(64).as_str()}})).is_some()
    );
}

#[test]
fn retry_after_parsing_is_bounded_and_conservative() {
    use crate::http_provider::parse_retry_after;
    use reqwest::header::{HeaderMap, HeaderValue};
    fn headers(value: &str) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_str(value).unwrap(),
        );
        map
    }
    assert_eq!(parse_retry_after(&headers("2")), Some(2));
    assert_eq!(parse_retry_after(&headers("0")), Some(0));
    assert_eq!(parse_retry_after(&headers("120")), Some(60));
    assert_eq!(parse_retry_after(&headers("  7  ")), Some(7));
    assert!(parse_retry_after(&headers("abc")).is_none());
    assert!(parse_retry_after(&headers("1.5")).is_none());
    assert!(parse_retry_after(&headers("-5")).is_none());
    assert!(parse_retry_after(&HeaderMap::new()).is_none());
    // Far-future HTTP-date is capped; the past yields no delay.
    let future = (chrono::Utc::now() + chrono::Duration::seconds(3600)).to_rfc2822();
    assert_eq!(parse_retry_after(&headers(&future)), Some(60));
    let past = (chrono::Utc::now() - chrono::Duration::seconds(3600)).to_rfc2822();
    assert!(parse_retry_after(&headers(&past)).is_none());
    // IMF-fixdate GMT spellings are bounded the same way.
    let gmt_future = (chrono::Utc::now() + chrono::Duration::seconds(3600))
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    assert_eq!(parse_retry_after(&headers(&gmt_future)), Some(60));
    assert!(parse_retry_after(&headers("Wed, 21 Oct 2015 07:28:00 GMT")).is_none());
}

#[test]
fn retry_after_opt_in_controls_delay_provenance() {
    let mut base = coded_error("slow_down");
    base.retry_after_secs = Some(7);
    let plain_default = RetryPolicy::default();
    let observed = plain_default.decide(&base, 1, 3, false, false);
    assert_eq!(observed.retry_after_secs, Some(7));
    assert!(
        !observed.retry_after_applied,
        "default must not opt into Retry-After"
    );

    let opt_in: RetryPolicy = serde_json::from_value(json!({"rules": [{
        "kind": {"http_status": 429},
        "provider_code": "slow_down",
        "use_retry_after": true,
        "action": "retry",
        "on_exhausted": "next_route"
    }]}))
    .unwrap();
    let applied = opt_in.decide(&base, 1, 3, false, false);
    assert_eq!(applied.source, "rule:0");
    assert!(applied.retry_after_applied);
    assert_eq!(applied.retry_after_secs, Some(7));

    // The header is not reported as applied after an attempt-limit fallback
    // or a stream-emission guard prevents the retry.
    let exhausted = opt_in.decide(&base, 3, 3, false, false);
    assert_eq!(exhausted.action, RetryAction::NextRoute);
    assert!(!exhausted.retry_after_applied);
    let emitted = opt_in.decide(&base, 1, 3, true, true);
    assert_eq!(emitted.action, RetryAction::FailFast);
    assert!(!emitted.retry_after_applied);

    let invalid_opt_in: RetryPolicy = serde_json::from_value(json!({"rules": [{
        "use_retry_after": true,
        "action": "next_route"
    }]}))
    .unwrap();
    assert!(invalid_opt_in.validate().is_err());

    let explicit_off: RetryPolicy = serde_json::from_value(json!({"rules": [{
        "kind": {"http_status": 429},
        "use_retry_after": false,
        "action": "retry"
    }]}))
    .unwrap();
    assert!(
        !explicit_off
            .decide(&base, 1, 3, false, false)
            .retry_after_applied
    );

    // Opt-in without an observed delay applies nothing.
    let mut no_header = coded_error("slow_down");
    no_header.retry_after_secs = None;
    assert!(
        !opt_in
            .decide(&no_header, 1, 3, false, false)
            .retry_after_applied
    );
}

#[test]
fn policy_drops_invalid_custom_provider_codes_before_matching_and_recording() {
    for raw in [
        "".to_string(),
        "a".repeat(65),
        "slow_down\ncredential".to_string(),
        "slow down".to_string(),
        "é".to_string(),
        "secret/token".to_string(),
    ] {
        // Even an unvalidated rule cannot match a rejected raw code.
        let policy: RetryPolicy = serde_json::from_value(json!({"rules": [{
            "provider_code": raw,
            "action": "fail_fast"
        }]}))
        .unwrap();
        let decision = policy.decide(&coded_error(&raw), 1, 3, false, false);
        assert_eq!(decision.provider_code, None);
        assert_eq!(decision.source, "default");
        assert_eq!(decision.action, RetryAction::Retry);
    }

    let valid = "a".repeat(64);
    let decision = RetryPolicy::default().decide(&coded_error(&valid), 1, 3, false, false);
    assert_eq!(decision.provider_code.as_deref(), Some(valid.as_str()));
}

#[test]
fn policy_bounds_custom_provider_retry_after_before_recording_and_waiting() {
    use crate::retry::effective_retry_delay;
    let mut error = coded_error("slow_down");
    error.retry_after_secs = Some(u64::MAX);
    let default = RetryPolicy::default().decide(&error, 1, 3, false, false);
    assert_eq!(default.retry_after_secs, Some(60));
    assert!(!default.retry_after_applied);
    assert_eq!(default.source, "default");
    let base = Duration::from_millis(200);
    assert_eq!(effective_retry_delay(base, None), base);

    let policy: RetryPolicy = serde_json::from_value(json!({"rules": [{
        "provider_code": "slow_down",
        "use_retry_after": true,
        "action": "retry",
        "on_exhausted": "next_route"
    }]}))
    .unwrap();
    let decision = policy.decide(&error, 1, 3, false, false);
    assert_eq!(decision.provider_code.as_deref(), Some("slow_down"));
    assert_eq!(decision.retry_after_secs, Some(60));
    assert_eq!(decision.source, "rule:0");
    assert!(decision.retry_after_applied);
    assert_eq!(
        effective_retry_delay(base, decision.retry_after_secs),
        Duration::from_secs(60)
    );
    // Defense in depth at Duration construction, even without a decision.
    assert_eq!(
        effective_retry_delay(base, Some(u64::MAX)),
        Duration::from_secs(60)
    );
    for (attempt, emitted, action) in [
        (3, false, RetryAction::NextRoute),
        (1, true, RetryAction::FailFast),
    ] {
        let guarded = policy.decide(&error, attempt, 3, emitted, emitted);
        assert_eq!(guarded.action, action);
        assert_eq!(guarded.retry_after_secs, Some(60));
        assert!(!guarded.retry_after_applied);
    }
}

#[test]
fn registry_rejects_invalid_provider_codes() {
    let valid = format!(
        "{REGISTRY_FIXTURE}\n[[provider.retry_policy.rules]]\nkind = {{ http_status = 429 }}\nprovider_code = 'slow_down'\naction = 'retry'\n"
    );
    assert!(LlmRouter::parse_text(&valid).is_ok());
    for bad in ["", "has space", "semi;colon", &"a".repeat(65)] {
        let text = format!(
            "{REGISTRY_FIXTURE}\n[[provider.retry_policy.rules]]\nkind = {{ http_status = 429 }}\nprovider_code = '{bad}'\naction = 'retry'\n"
        );
        assert!(
            LlmRouter::parse_text(&text).is_err(),
            "code `{bad}` must fail"
        );
    }
    // Unknown rule fields still fail closed.
    let typo = format!(
        "{REGISTRY_FIXTURE}\n[[provider.retry_policy.rules]]\nkind = {{ http_status = 429 }}\nprovidercode = 'slow_down'\naction = 'retry'\n"
    );
    assert!(LlmRouter::parse_text(&typo).is_err());
}

#[tokio::test]
async fn loopback_error_code_and_retry_after_roundtrip() {
    let body = serde_json::json!({"error": {
        "code": "slow_down",
        "message": "sensitive upstream detail",
        "type": "rate_limit_error"
    }})
    .to_string();
    let (base_url, server) = spawn_http_response(429, body, "Retry-After: 2\r\n".into());
    let provider = HttpProvider::from_spec(spec_with_base_url("loopback", &base_url));
    let mut request = sample_request();
    request.seed = None;
    let error = provider.complete(request).await.expect_err("429 must fail");
    assert_eq!(error.kind, LlmErrorKind::HttpStatus(429));
    assert_eq!(error.provider_code.as_deref(), Some("slow_down"));
    assert_eq!(error.retry_after_secs, Some(2));
    assert!(!error.message.contains("sensitive upstream detail"));
    assert!(!error.message.contains("slow_down"));
    server.join().expect("test server should stop");

    // Identical status, distinct quota code.
    let quota_body = serde_json::json!({"error": {"code": "insufficient_quota"}}).to_string();
    let (quota_url, quota_server) = spawn_http_response(429, quota_body, "".into());
    let quota_provider = HttpProvider::from_spec(spec_with_base_url("loopback", &quota_url));
    let mut quota_request = sample_request();
    quota_request.seed = None;
    let quota_error = quota_provider
        .complete(quota_request)
        .await
        .expect_err("429 must fail");
    assert_eq!(
        quota_error.provider_code.as_deref(),
        Some("insufficient_quota")
    );
    assert_eq!(quota_error.retry_after_secs, None);
    quota_server.join().expect("test server should stop");

    // Missing code shape preserves current behavior.
    let (plain_url, plain_server) = spawn_http_response(429, r#"{"oops": true}"#.into(), "".into());
    let plain_provider = HttpProvider::from_spec(spec_with_base_url("loopback", &plain_url));
    let mut plain_request = sample_request();
    plain_request.seed = None;
    let plain_error = plain_provider
        .complete(plain_request)
        .await
        .expect_err("429 must fail");
    assert_eq!(plain_error.provider_code, None);
    assert_eq!(plain_error.retry_after_secs, None);
    plain_server.join().expect("test server should stop");
}

#[tokio::test]
async fn loopback_malformed_and_oversized_bodies_yield_no_code() {
    // Malformed JSON keeps the status without a code.
    let (bad_url, bad_server) =
        spawn_http_response(429, "not-json{{{".into(), "Retry-After: 3\r\n".into());
    let bad_provider = HttpProvider::from_spec(spec_with_base_url("loopback", &bad_url));
    let mut bad_request = sample_request();
    bad_request.seed = None;
    let bad_error = bad_provider
        .complete(bad_request)
        .await
        .expect_err("429 must fail");
    assert_eq!(bad_error.provider_code, None);
    assert_eq!(
        bad_error.retry_after_secs,
        Some(3),
        "header survives a bad body"
    );
    bad_server.join().expect("test server should stop");

    // Oversized bodies are not inspected, but the header still applies.
    let big_code = "a".repeat(64);
    let big_body = format!(
        r#"{{"error": {{"code": "{big_code}"}}, "pad": "{}"}}"#,
        "x".repeat(16 * 1024)
    );
    let (big_url, big_server) = spawn_http_response(429, big_body, "Retry-After: 4\r\n".into());
    let big_provider = HttpProvider::from_spec(spec_with_base_url("loopback", &big_url));
    let mut big_request = sample_request();
    big_request.seed = None;
    let big_error = big_provider
        .complete(big_request)
        .await
        .expect_err("429 must fail");
    assert_eq!(
        big_error.provider_code, None,
        "oversized body must not yield a code"
    );
    assert_eq!(big_error.retry_after_secs, Some(4));
    big_server.join().expect("test server should stop");

    // Invalid Retry-After values preserve current behavior.
    let (invalid_url, invalid_server) = spawn_http_response(
        429,
        serde_json::json!({"error": {"code": "slow_down"}}).to_string(),
        "Retry-After: soon\r\n".into(),
    );
    let invalid_provider = HttpProvider::from_spec(spec_with_base_url("loopback", &invalid_url));
    let mut invalid_request = sample_request();
    invalid_request.seed = None;
    let invalid_error = invalid_provider
        .complete(invalid_request)
        .await
        .expect_err("429 must fail");
    assert_eq!(invalid_error.provider_code.as_deref(), Some("slow_down"));
    assert_eq!(invalid_error.retry_after_secs, None);
    invalid_server.join().expect("test server should stop");
}

#[tokio::test]
async fn loopback_reflected_credential_never_becomes_a_code() {
    if env_process::isolated() {
        return;
    }
    let key = "loopback-code-credential-xyz-12345";
    let _guard = super::ENV_LOCK.lock().await;
    unsafe { std::env::set_var("LLM_CODE_CRED_XYZ", key) };
    // Raw reflection fails closed without retaining the secret.
    let (raw_url, raw_server) = spawn_http_response(
        429,
        format!(r#"{{"error": {{"code": "slow_down"}}, "detail": "{key}"}}"#),
        "".into(),
    );
    let mut raw_spec = spec_with_base_url("loopback", &raw_url);
    raw_spec.api_key_env = Some("LLM_CODE_CRED_XYZ".into());
    let raw_provider = HttpProvider::from_spec(raw_spec);
    let mut raw_request = sample_request();
    raw_request.seed = None;
    let raw_error = raw_provider
        .complete(raw_request)
        .await
        .expect_err("reflected credential must fail closed");
    assert!(raw_error.message.contains("configured credential"));
    assert!(!raw_error.message.contains(key));
    assert_eq!(raw_error.provider_code, None);
    raw_server.join().expect("test server should stop");

    // A decoded JSON fragment containing the secret fails the same way.
    let fragmented = serde_json::json!({"error": {"code": "slow_down"}, "detail": key}).to_string();
    let (frag_url, frag_server) = spawn_http_response(429, fragmented, "".into());
    let mut frag_spec = spec_with_base_url("loopback", &frag_url);
    frag_spec.api_key_env = Some("LLM_CODE_CRED_XYZ".into());
    let frag_provider = HttpProvider::from_spec(frag_spec);
    let mut frag_request = sample_request();
    frag_request.seed = None;
    let frag_error = frag_provider
        .complete(frag_request)
        .await
        .expect_err("fragmented credential must fail closed");
    assert!(!frag_error.message.contains(key));
    assert_eq!(frag_error.provider_code, None);
    frag_server.join().expect("test server should stop");
    unsafe { std::env::remove_var("LLM_CODE_CRED_XYZ") };
}

#[test]
fn effective_retry_delay_never_shortens_backoff() {
    use crate::retry::effective_retry_delay;
    let base = Duration::from_millis(5000);
    assert_eq!(effective_retry_delay(base, None), base);
    assert_eq!(
        effective_retry_delay(base, Some(2)),
        base,
        "a smaller Retry-After must not shorten the floor"
    );
    assert_eq!(
        effective_retry_delay(base, Some(30)),
        Duration::from_secs(30)
    );
    assert_eq!(
        effective_retry_delay(Duration::from_millis(200), Some(60)),
        Duration::from_secs(60)
    );
}
