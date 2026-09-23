//! HTTP rate limiting: a per-credential token bucket resolved once at boot.
//!
//! Deployment policy enters through [`super::serve::ResolvedServerPolicy`];
//! requests never re-read the environment. The limiter keys each bucket by
//! the SHA-256 of the presented bearer credential (the same digest the auth
//! layer validates), so every credential gets its own budget while requests
//! without a credential share one anonymous bucket. `/healthz` stays exempt
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitPolicy {
    /// Sustained refill rate in tokens per second.
    pub rps: u32,
    /// Bucket capacity: the largest instantaneous burst admitted.
    pub burst: u32,
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
    Ok(Some(RateLimitPolicy { rps, burst }))
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

/// Per-credential token buckets behind one lock. The lock is taken for the
/// arithmetic only and never held across an await, so it cannot serialize
/// request handling beyond the bucket fold.
pub(crate) struct RateLimiter {
    policy: RateLimitPolicy,
    buckets: Mutex<HashMap<Option<[u8; 32]>, TokenBucket>>,
}

impl RateLimiter {
    pub(crate) fn new(policy: RateLimitPolicy) -> Self {
        Self {
            policy,
            buckets: Mutex::new(HashMap::new()),
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
        if !buckets.contains_key(&key) && buckets.len() >= MAX_TRACKED_BUCKETS {
            let oldest = buckets
                .iter()
                .min_by_key(|(_, bucket)| bucket.last_refill)
                .map(|(key, _)| *key);
            if let Some(oldest) = oldest {
                buckets.remove(&oldest);
            }
        }
        let policy = self.policy;
        let bucket = buckets.entry(key).or_insert(TokenBucket {
            tokens: f64::from(policy.burst),
            last_refill: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last_refill);
        if elapsed > Duration::ZERO {
            bucket.tokens = (bucket.tokens + elapsed.as_secs_f64() * f64::from(policy.rps))
                .min(f64::from(policy.burst));
            bucket.last_refill = now;
        }
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64(
                (1.0 - bucket.tokens) / f64::from(policy.rps),
            ))
        }
    }
}

/// Axum middleware: admits the request when a token is available and answers
/// 429 with an integer `Retry-After` otherwise. `/healthz` is exempt so a
/// saturated limiter can never make the process look unhealthy.
pub(crate) async fn enforce_rate_limit(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path() == "/healthz" {
        return next.run(request).await;
    }
    let key = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(sha256_bytes);
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
        let limiter = RateLimiter::new(RateLimitPolicy { rps: 2, burst: 3 });
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
    fn buckets_are_isolated_per_credential_and_anonymous() {
        let limiter = RateLimiter::new(RateLimitPolicy { rps: 1, burst: 1 });
        let now = Instant::now();
        let first = Some(sha256_bytes("first"));
        let second = Some(sha256_bytes("second"));
        limiter
            .try_acquire(first, now)
            .expect("the first credential is admitted");
        assert!(
            limiter.try_acquire(first, now).is_err(),
            "the first credential must be exhausted"
        );
        limiter
            .try_acquire(second, now)
            .expect("a second credential gets its own bucket");
        limiter
            .try_acquire(None, now)
            .expect("anonymous requests get their own bucket");
        assert!(
            limiter.try_acquire(None, now).is_err(),
            "the anonymous bucket must be exhausted"
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
        let app = test_router(Arc::new(RateLimiter::new(RateLimitPolicy {
            rps: 1,
            burst: 1,
        })));
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
}
