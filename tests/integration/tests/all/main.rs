mod examples;
mod self_hosting;

use std::collections::BTreeMap;

/// Upper bound on discovery replays for one generator run. Each replay
/// consumes one newly discovered question, so the bound only guards against
/// a non-deterministic question id looping forever.
pub(crate) const MAX_QUESTION_DISCOVERY_ATTEMPTS: usize = 32;

/// Resolves a pre-provisioned answer keyed by node id against the concrete
/// question id a run reports. `ask_user` identities bind the resolved
/// question content (`<node>:ask_user:<sha256>`), so callers that provision
/// answers by node id must re-key them to the id the engine mints at run
/// time; questions with stable ids (on_fail, exhausted) match directly.
pub(crate) fn answer_for_question<'a>(
    answers: &'a BTreeMap<String, serde_json::Value>,
    question_id: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(value) = answers.get(question_id) {
        return Some(value);
    }
    let (node, digest) = question_id.split_once(":ask_user:")?;
    (digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| answers.get(node))
        .flatten()
}

/// Test-only service construction through the policy constructor (E04).
/// Integration tests are external crates, so the unit-test-only
/// `LocalQcgService::new` is unavailable; every test service is built here
/// with explicit defaults instead of a legacy constructor.
pub(crate) fn test_service(
    generators_dir: camino::Utf8PathBuf,
    runs_dir: camino::Utf8PathBuf,
    providers_path: Option<camino::Utf8PathBuf>,
) -> Result<qcg_service::LocalQcgService, qcg_service::ServiceError> {
    qcg_service::LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        providers_path,
        qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
        qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
        qcg_service::RunStoreMode::Exclusive,
        qcg_service::ServiceDeploymentPolicy::default(),
    )
}
