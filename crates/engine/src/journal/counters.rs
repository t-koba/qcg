//! Process-lifetime run counters bumped at journal appends: metrics read
//! these instead of scanning run history.

use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

type Counts<K> = BTreeMap<K, u64>;
static COUNTERS: Mutex<(Counts<String>, Counts<&'static str>)> =
    Mutex::new((BTreeMap::new(), BTreeMap::new()));

/// Counts one appended event: the first `run_queued` of a run is an
/// admission for its generator, a terminal event settles one run.
pub(crate) fn observe(kind: &str, event: &Value, first_queued: bool) {
    let mut counters = COUNTERS.lock().unwrap_or_else(PoisonError::into_inner);
    let generator = event.get("generator").and_then(Value::as_str);
    if let (true, Some(generator)) = (first_queued, generator) {
        *counters
            .0
            .entry(generator.split('@').next().unwrap_or("").into())
            .or_default() += 1;
    }
    let status = match (kind, event.get("status").and_then(Value::as_str)) {
        ("run_finished", Some("success")) => "success",
        ("run_finished" | "run_error", _) => "failed",
        ("run_canceled", _) => "canceled",
        ("run_interrupted", _) => "interrupted",
        _ => return,
    };
    *counters.1.entry(status).or_default() += 1;
}

/// Runs admitted per generator id and runs settled per terminal status by
/// this process since start.
pub fn run_counters() -> (Counts<String>, Counts<&'static str>) {
    COUNTERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}
