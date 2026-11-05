use super::error::ApiHttpError;
use axum::http::StatusCode;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Four default-sized (16 MiB) single-artifact response snapshots.
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

/// Owned by the response stream, including while its body is unpolled.
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<ArtifactResponseBudget>,
    bytes: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
