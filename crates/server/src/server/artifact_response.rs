use super::error::ApiHttpError;
use axum::http::StatusCode;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Four default-sized (16 MiB) artifact and JSON run-view response bodies.
pub(crate) const DEFAULT_MAX_ARTIFACT_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct ArtifactResponseBudget {
    limit: u64,
    used: AtomicU64,
}

impl ArtifactResponseBudget {
    pub(crate) fn new(limit: Option<u64>) -> Result<Arc<Self>, String> {
        let limit = limit.unwrap_or(DEFAULT_MAX_ARTIFACT_RESPONSE_BYTES);
        if limit == 0 {
            return Err("max_artifact_response_bytes must be greater than zero".into());
        }
        Ok(Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
        }))
    }

    pub(crate) fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation, ApiHttpError> {
        if bytes > self.limit {
            return Err(ApiHttpError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Payload too large",
                "payload_too_large",
                format!(
                    "artifact response requires {bytes} bytes; limit is {} bytes",
                    self.limit
                ),
                "",
                Vec::new(),
            ));
        }
        // Only the counter is shared; no memory is published through it.
        self.used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                (bytes <= self.limit - used).then(|| used + bytes)
            })
            .map_err(|_| {
                ApiHttpError::service_unavailable("artifact response byte budget exhausted")
            })?;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
}

/// Owned by response bytes or stream, including while its body is unpolled.
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<ArtifactResponseBudget>,
    bytes: u64,
}

impl Reservation {
    pub(crate) fn into_body(self, bytes: Vec<u8>) -> axum::body::Body {
        struct ReservedBytes {
            bytes: Vec<u8>,
            _reservation: Reservation,
        }
        impl AsRef<[u8]> for ReservedBytes {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        axum::body::Body::from(axum::body::Bytes::from_owner(ReservedBytes {
            bytes,
            _reservation: self,
        }))
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Count first without retaining JSON, then reserve the exact serialized size.
/// The value is an immutable run view: both passes serialize the same shape.
pub(crate) fn serialize_json(
    value: &impl serde::Serialize,
    limit: u64,
    budget: &Arc<ArtifactResponseBudget>,
) -> Result<(Vec<u8>, Reservation), ApiHttpError> {
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("JSON size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(ApiHttpError::internal)?;
    if counter.0 > limit {
        return Err(ApiHttpError::from_api(api::ApiError::TooLarge {
            actual_bytes: counter.0 as usize,
            limit_bytes: limit as usize,
        }));
    }
    let reservation = budget.reserve(counter.0)?;
    let body = serde_json::to_vec(value).map_err(ApiHttpError::internal)?;
    Ok((body, reservation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_reserves_exact_bytes_before_buffering_and_releases_on_errors() {
        struct Value<'a> {
            budget: &'a Arc<ArtifactResponseBudget>,
            calls: std::cell::Cell<usize>,
            fail_on: usize,
        }
        impl serde::Serialize for Value<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let pass = self.calls.get();
                self.calls.set(pass + 1);
                // Four bytes for the JSON string "ok"; no buffer exists on pass zero.
                assert_eq!(
                    self.budget.used.load(Ordering::Relaxed),
                    if pass == 0 { 0 } else { 4 }
                );
                if pass == self.fail_on {
                    return Err(serde::ser::Error::custom("injected serialization failure"));
                }
                serializer.serialize_str("ok")
            }
        }
        let budget = ArtifactResponseBudget::new(Some(4)).unwrap();
        for fail_on in [0, 1, usize::MAX] {
            let value = Value {
                budget: &budget,
                calls: std::cell::Cell::new(0),
                fail_on,
            };
            let result = serialize_json(&value, 4, &budget);
            if fail_on == usize::MAX {
                let (body, reservation) = result.unwrap();
                assert_eq!(body, br#""ok""#);
                assert_eq!(reservation.bytes, body.len() as u64);
                drop(reservation);
            } else {
                assert_eq!(result.unwrap_err().problem.status, 500);
            }
            assert_eq!(budget.used.load(Ordering::Relaxed), 0);
        }
        let value = serde_json::json!({"escaped": "\n\"é"});
        let expected = serde_json::to_vec(&value).unwrap();
        let budget = ArtifactResponseBudget::new(Some(expected.len() as u64)).unwrap();
        assert_eq!(
            serialize_json(&value, expected.len() as u64 - 1, &budget)
                .unwrap_err()
                .problem
                .status,
            413
        );
        let (body, reservation) = serialize_json(&value, expected.len() as u64, &budget).unwrap();
        assert_eq!(body, expected);
        assert_eq!(reservation.bytes, expected.len() as u64);
        drop(reservation);
        let occupied = budget.reserve(1).unwrap();
        assert_eq!(
            serialize_json(&value, u64::MAX, &budget)
                .unwrap_err()
                .problem
                .status,
            503
        );
        assert_eq!(budget.used.load(Ordering::Relaxed), 1);
        drop(occupied);
        let too_small = ArtifactResponseBudget::new(Some(expected.len() as u64 - 1)).unwrap();
        assert_eq!(
            serialize_json(&value, u64::MAX, &too_small)
                .unwrap_err()
                .problem
                .status,
            413
        );
        assert_eq!(too_small.used.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_reservations_cannot_overadmit() {
        let budget = ArtifactResponseBudget::new(Some(4)).unwrap();
        let barrier = std::sync::Barrier::new(8);
        let admitted = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    barrier.wait();
                    let reservation = budget.reserve(1);
                    if reservation.is_ok() {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                    barrier.wait();
                    drop(reservation);
                });
            }
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 4);
        assert_eq!(budget.used.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn exact_admission_exhaustion_and_drop() {
        assert!(ArtifactResponseBudget::new(Some(0)).is_err());
        assert_eq!(
            ArtifactResponseBudget::new(None).unwrap().limit,
            64 * 1024 * 1024
        );
        let budget = ArtifactResponseBudget::new(Some(10)).unwrap();
        let first = budget.reserve(4).unwrap();
        let second = budget.reserve(6).unwrap();
        assert_eq!(budget.reserve(1).unwrap_err().problem.status, 503);
        let too_large = budget.reserve(11).unwrap_err();
        assert_eq!(too_large.problem.status, 413);
        assert_eq!(too_large.problem.code, "payload_too_large");
        drop(budget.reserve(0).unwrap());
        drop(first);
        let replacement = budget.reserve(4).unwrap();
        drop(second);
        drop(replacement);
        assert_eq!(budget.used.load(Ordering::Relaxed), 0);
        drop(budget.reserve(10).unwrap());
        // Checked subtraction admits the entire u64 domain without overflow.
        let huge = ArtifactResponseBudget::new(Some(u64::MAX)).unwrap();
        let all = huge.reserve(u64::MAX).unwrap();
        assert!(huge.reserve(1).is_err());
        drop(all);
        assert!(huge.reserve(u64::MAX).is_ok());
    }
}
