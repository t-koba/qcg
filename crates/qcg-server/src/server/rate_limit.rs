//! HTTP rate limiting: token buckets resolved once at boot.
//!
//! Deployment policy enters through [`super::serve::ResolvedServerPolicy`];
//! requests never re-read the environment. Buckets key only on verified
//! identity (F10): the configured bearer digest when authentication is on,
//! otherwise a single shared anonymous/instance bucket. Presented but
//! unverified `Authorization` strings never mint new buckets, so rotating
//! fake bearers cannot escape the anonymous budget. `/healthz` stays exempt
//! so liveness probes never consume budget.

use axum::extract::{Request, State};
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::error::ApiHttpError;
use super::middleware::sha256_bytes;

/// Upper bound on tracked credential buckets. A fully refilled bucket is
/// indistinguishable from an absent one, so the least recently used entry is
/// evicted when a new credential arrives at capacity; without this bound an
/// attacker rotating `Authorization` headers would grow the map forever.
const MAX_TRACKED_BUCKETS: usize = 4_096;

/// Boot-frozen rate limit policy. Both values are validated in
/// [`resolve_rate_limit_policy`]: a rejected knob refuses boot instead of
/// silently degrading to an unthrottled or mis-throttled server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitPolicy {
    /// Sustained refill rate in tokens per second.
    pub rps: u32,
    /// Bucket capacity: the largest instantaneous burst admitted.
    pub burst: u32,
    /// Optional trusted identity header (e.g. set by an outer proxy that
    /// already verified the caller). When set, its value hashes to the
    /// bucket key. Must only be configured when the outer layer strips or
    /// overwrites it; otherwise callers could self-assert identity (F10).
    pub trusted_identity_header: Option<String>,
}

/// Resolves the deployment rate limit from the environment. Unset
/// `QCG_RATE_LIMIT_RPS` disables limiting; `QCG_RATE_LIMIT_BURST` defaults
/// to the rps value. Every other combination is refused: only integers of at
/// least 1 are accepted, and a lone burst setting is an incomplete
/// configuration rather than a silent no-op (E04).
pub(crate) fn resolve_rate_limit_policy() -> Result<Option<RateLimitPolicy>, String> {
    let rps = match std::env::var("QCG_RATE_LIMIT_RPS") {
        Err(_) => {
            if std::env::var("QCG_RATE_LIMIT_BURST").is_ok() {
                return Err(
                    "QCG_RATE_LIMIT_BURST is set without QCG_RATE_LIMIT_RPS; set both or neither"
                        .to_string(),
                );
            }
            return Ok(None);
        }
        Ok(value) => parse_positive("QCG_RATE_LIMIT_RPS", &value)?,
    };
    let burst = match std::env::var("QCG_RATE_LIMIT_BURST") {
        Err(_) => rps,
        Ok(value) => parse_positive("QCG_RATE_LIMIT_BURST", &value)?,
    };
    let trusted_identity_header = match std::env::var("QCG_RATE_LIMIT_IDENTITY_HEADER") {
        Err(_) => None,
        Ok(value) => {
            let name = value.trim().to_ascii_lowercase();
            if name.is_empty()
                || name == "authorization"
                || name.contains(' ')
                || name.contains(':')
            {
                return Err(format!(
                    "invalid QCG_RATE_LIMIT_IDENTITY_HEADER `{value}`: must be a non-authorization header name"
                ));
            }
            Some(name)
        }
    };
    Ok(Some(RateLimitPolicy {
        rps,
        burst,
        trusted_identity_header,
    }))
}

/// Strict positive integer parse: zero, negative, fractional, and empty
/// values all refuse boot with an error naming the variable (E04).
fn parse_positive(name: &str, value: &str) -> Result<u32, String> {
    match value.parse::<u32>() {
        Ok(parsed) if parsed >= 1 => Ok(parsed),
        _ => Err(format!(
            "invalid {name} `{value}`: must be an integer of at least 1"
        )),
    }
}

/// Lazily refilled token bucket: tokens accrue at `rps` up to `burst`.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

/// Per-identity token buckets behind one lock. The lock is taken for the
/// arithmetic only and never held across an await, so it cannot serialize
/// request handling beyond the bucket fold.
pub(crate) struct RateLimiter {
    policy: RateLimitPolicy,
    /// Expected bearer digest when authentication is configured. `None`
    /// means the instance is unauthenticated: every request shares the
    /// anonymous bucket regardless of any presented header (F10).
    expected_digest: Option<[u8; 32]>,
    buckets: Mutex<HashMap<Option<[u8; 32]>, TokenBucket>>,
}

impl RateLimiter {
    pub(crate) fn with_expected_digest(
        policy: RateLimitPolicy,
        expected_digest: Option<[u8; 32]>,
    ) -> Self {
        Self {
            policy,
            expected_digest,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Derives the bucket key for a request (F10). Verified bearers get
    /// their own bucket; everything unverified shares the anonymous bucket
    /// (`None`). A configured trusted identity header (outer proxy) takes
    /// precedence when present.
    fn rate_key(&self, request: &Request) -> Option<[u8; 32]> {
        if let Some(header_name) = self.policy.trusted_identity_header.as_deref()
            && let Some(value) = request
                .headers()
                .get(header_name)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|v| !v.is_empty())
        {
            return Some(sha256_bytes(value));
        }
        let presented = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(sha256_bytes);
        match (presented, self.expected_digest) {
            (Some(supplied), Some(expected)) if constant_time_eq(&supplied, &expected) => {
                Some(expected)
            }
            // Unauthenticated instance or unverified/missing credential:
            // one shared bucket so rotating fake strings gains nothing.
            _ => None,
        }
    }

    /// Takes one token for `key` (`None` is the anonymous bucket). `Err`
    /// carries the wait until the next token becomes available.
    fn try_acquire(&self, key: Option<[u8; 32]>, now: Instant) -> Result<(), Duration> {
        // A poisoned lock only means a previous holder panicked mid-fold;
        // the bucket arithmetic is independent per entry, so recovering the
        // guard keeps serving instead of failing every later request.
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // At capacity, fold new identities into the shared anonymous bucket
        // instead of evicting (F10-03): evicting an exhausted bucket and
        // recreating it full would hand out fresh bursts on rotation.
        let mut key = key;
        if !buckets.contains_key(&key) && buckets.len() >= MAX_TRACKED_BUCKETS {
            if key.is_some() {
                key = None;
            } else {
                // Even the anonymous bucket is at capacity pressure: evict
                // the fullest bucket so an exhausted bucket never refills
                // via eviction.
                let fullest = buckets
                    .iter()
                    .max_by(|a, b| {
                        a.1.tokens
                            .partial_cmp(&b.1.tokens)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(key, _)| *key);
                if let Some(fullest) = fullest {
                    buckets.remove(&fullest);
                }
            }
        }
        let rps = f64::from(self.policy.rps);
        let burst = f64::from(self.policy.burst);
        let bucket = buckets.entry(key).or_insert(TokenBucket {
            tokens: burst,
            last_refill: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last_refill);
        if elapsed > Duration::ZERO {
            bucket.tokens = (bucket.tokens + elapsed.as_secs_f64() * rps).min(burst);
            bucket.last_refill = now;
        }
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - bucket.tokens) / rps))
        }
    }
}

/// Axum middleware: admits the request when a token is available and answers
/// 429 with an integer `Retry-After` otherwise. `/healthz` is exempt so a
/// saturated limiter can never make the process look unhealthy. Only
/// verified identity splits buckets (F10); unverified bearers share the
/// anonymous budget.
pub(crate) async fn enforce_rate_limit(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path() == "/healthz" {
        return next.run(request).await;
    }
    let key = limiter.rate_key(&request);
    match limiter.try_acquire(key, Instant::now()) {
        Ok(()) => next.run(request).await,
        Err(wait) => {
            let mut response =
                ApiHttpError::too_many_requests("request rate limit exceeded").into_response();
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from(retry_after_seconds(wait)),
            );
            response
        }
    }
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Whole seconds for `Retry-After`, rounded up and never zero: a sub-second
/// wait still tells the client to wait one second.
fn retry_after_seconds(wait: Duration) -> u64 {
    let seconds = wait.as_secs_f64().ceil();
    if seconds < 1.0 { 1 } else { seconds as u64 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use axum::{http::Request as HttpRequest, middleware as axum_middleware};
    use tower::ServiceExt as _;

    fn test_router(limiter: Arc<RateLimiter>) -> Router {
        Router::new()
            .route("/healthz", get(|| async { "ok" }))
            .route("/work", get(|| async { "ok" }))
            .layer(axum_middleware::from_fn_with_state(
                limiter,
                enforce_rate_limit,
            ))
    }

    #[test]
    fn bucket_admits_the_burst_then_recovers_with_time() {
        let limiter = RateLimiter::with_expected_digest(
            RateLimitPolicy {
                rps: 2,
                burst: 3,
                trusted_identity_header: None,
            },
            None,
        );
        let start = Instant::now();
        for _ in 0..3 {
            limiter
                .try_acquire(None, start)
                .expect("the burst must be admitted");
        }
        let wait = limiter
            .try_acquire(None, start)
            .expect_err("the burst must be exhausted");
        assert!(wait > Duration::ZERO, "a denial must report a retry delay");
        // At 2 rps, 500 ms refill exactly one token.
        limiter
            .try_acquire(None, start + Duration::from_millis(500))
            .expect("elapsed time must refill one token");
        limiter
            .try_acquire(None, start + Duration::from_millis(500))
            .expect_err("only one token was refilled");
        // Long idle time refills no further than the burst capacity.
        limiter
            .try_acquire(None, start + Duration::from_secs(60))
            .expect("an idle bucket must refill one token");
    }

    #[test]
    fn verified_identity_is_isolated_but_unverified_shares_anonymous() {
        // F10-01/F10-02: verified identity splits; unverified strings do
        // not mint buckets at the try_acquire level, and rate_key folds
        // them to anonymous.
        let expected = sha256_bytes("valid-token");
        let limiter = RateLimiter::with_expected_digest(
            RateLimitPolicy {
                rps: 1,
                burst: 1,
                trusted_identity_header: None,
            },
            Some(expected),
        );
        let now = Instant::now();
        limiter
            .try_acquire(Some(expected), now)
            .expect("verified identity is admitted");
        assert!(
            limiter.try_acquire(Some(expected), now).is_err(),
            "verified identity must be exhausted"
        );
        limiter
            .try_acquire(None, now)
            .expect("anonymous gets its own bucket");
        assert!(
            limiter.try_acquire(None, now).is_err(),
            "anonymous must be exhausted"
        );
        // rate_key: rotating fake bearers on an unauthenticated instance
        // all fold to the shared bucket.
        let open = RateLimiter::with_expected_digest(
            RateLimitPolicy {
                rps: 1,
                burst: 1,
                trusted_identity_header: None,
            },
            None,
        );
        for fake in ["fake-1", "fake-2", "fake-3"] {
            let request = HttpRequest::builder()
                .uri("/work")
                .header(header::AUTHORIZATION, format!("Bearer {fake}"))
                .body(Body::empty())
                .expect("request should build");
            assert_eq!(
                open.rate_key(&request),
                None,
                "unverified bearer must share the anonymous bucket"
            );
        }
        let verified_request = HttpRequest::builder()
            .uri("/work")
            .header(header::AUTHORIZATION, "Bearer valid-token")
            .body(Body::empty())
            .expect("request should build");
        assert_eq!(
            limiter.rate_key(&verified_request),
            Some(expected),
            "verified bearer must split"
        );
        let forged_request = HttpRequest::builder()
            .uri("/work")
            .header(header::AUTHORIZATION, "Bearer forged-token")
            .body(Body::empty())
            .expect("request should build");
        assert_eq!(
            limiter.rate_key(&forged_request),
            None,
            "forged bearer must share the anonymous bucket"
        );
    }

    #[test]
    fn retry_after_rounds_up_to_whole_seconds() {
        assert_eq!(retry_after_seconds(Duration::from_millis(1)), 1);
        assert_eq!(retry_after_seconds(Duration::from_millis(1_500)), 2);
        assert_eq!(retry_after_seconds(Duration::from_secs(3)), 3);
    }

    #[tokio::test]
    async fn middleware_answers_429_with_retry_after_and_exempts_healthz() {
        let app = test_router(Arc::new(RateLimiter::with_expected_digest(
            RateLimitPolicy {
                rps: 1,
                burst: 1,
                trusted_identity_header: None,
            },
            None,
        )));
        for _ in 0..5 {
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .uri("/healthz")
                        .body(Body::empty())
                        .expect("request should build"),
                )
                .await
                .expect("health request should respond");
            assert_eq!(
                response.status(),
                axum::http::StatusCode::OK,
                "health checks must never consume rate limit budget"
            );
        }
        let work = || {
            HttpRequest::builder()
                .uri("/work")
                .body(Body::empty())
                .expect("request should build")
        };
        let response = app
            .clone()
            .oneshot(work())
            .await
            .expect("first work request should respond");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let response = app
            .oneshot(work())
            .await
            .expect("second work request should respond");
        assert_eq!(response.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from(1_u64)),
            "a 429 must carry an integer Retry-After"
        );
    }

    #[tokio::test]
    async fn rotating_fake_bearers_share_the_anonymous_budget() {
        // F10-01: on an unauthenticated instance every presented bearer is
        // unverified, so rotating strings must not mint fresh bursts.
        let app = test_router(Arc::new(RateLimiter::with_expected_digest(
            RateLimitPolicy {
                rps: 1,
                burst: 1,
                trusted_identity_header: None,
            },
            None,
        )));
        let attempt = |token: &str| {
            HttpRequest::builder()
                .uri("/work")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("request should build")
        };
        let response = app
            .clone()
            .oneshot(attempt("fake-1"))
            .await
            .expect("first request should respond");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        for fake in ["fake-2", "fake-3", "fake-4"] {
            let response = app
                .clone()
                .oneshot(attempt(fake))
                .await
                .expect("rotated request should respond");
            assert_eq!(
                response.status(),
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "a rotated fake bearer must share the exhausted anonymous bucket"
            );
        }
    }

    #[tokio::test]
    async fn verified_identity_keeps_its_own_bucket() {
        // F10-02: the configured bearer splits from anonymous traffic, so a
        // saturated anonymous bucket never blocks the verified caller (and
        // forged strings never escape into a fresh bucket).
        let expected = sha256_bytes("valid-token");
        let app = test_router(Arc::new(RateLimiter::with_expected_digest(
            RateLimitPolicy {
                rps: 1,
                burst: 1,
                trusted_identity_header: None,
            },
            Some(expected),
        )));
        let attempt = |token: Option<&str>| {
            let mut builder = HttpRequest::builder().uri("/work");
            if let Some(token) = token {
                builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            builder.body(Body::empty()).expect("request should build")
        };
        // Exhaust the anonymous bucket with a forged bearer.
        assert_eq!(
            app.clone()
                .oneshot(attempt(Some("forged")))
                .await
                .expect("forged request should respond")
                .status(),
            axum::http::StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(attempt(Some("forged-2")))
                .await
                .expect("rotated forgery should respond")
                .status(),
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "forgeries share the anonymous bucket"
        );
        // The verified credential still has its own budget.
        assert_eq!(
            app.clone()
                .oneshot(attempt(Some("valid-token")))
                .await
                .expect("verified request should respond")
                .status(),
            axum::http::StatusCode::OK,
            "verified identity must split from anonymous traffic"
        );
        assert_eq!(
            app.oneshot(attempt(Some("valid-token")))
                .await
                .expect("second verified request should respond")
                .status(),
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "the verified bucket enforces its own burst"
        );
    }

    #[test]
    fn overflow_identities_fold_into_anonymous_without_refill() {
        // F10-03: at capacity no exhausted bucket is evicted into a full
        // one; memory stays bounded by MAX_TRACKED_BUCKETS plus the shared
        // overflow bucket.
        let limiter = RateLimiter::with_expected_digest(
            RateLimitPolicy {
                rps: 1,
                burst: 1,
                trusted_identity_header: None,
            },
            None,
        );
        let now = Instant::now();
        // Fill every slot with an exhausted bucket via distinct keys.
        for i in 0..MAX_TRACKED_BUCKETS {
            let key = Some(sha256_bytes(&format!("user-{i}")));
            limiter.try_acquire(key, now).expect("fresh bucket admits");
            assert!(
                limiter.try_acquire(key, now).is_err(),
                "each bucket must be exhausted"
            );
        }
        assert_eq!(
            limiter
                .buckets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            MAX_TRACKED_BUCKETS,
            "memory must stay bounded"
        );
        // A new identity folds into anonymous instead of evicting an
        // exhausted bucket back to full.
        let first = Some(sha256_bytes("user-0"));
        let fresh = Some(sha256_bytes("brand-new-user"));
        let _ = limiter.try_acquire(fresh, now);
        assert!(
            limiter.try_acquire(first, now).is_err(),
            "no exhausted bucket may be evicted into a refill"
        );
        assert!(
            limiter
                .buckets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                <= MAX_TRACKED_BUCKETS + 1,
            "memory must stay bounded"
        );
    }
}
