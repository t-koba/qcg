//! Continuation reuse: a repeated, identical model request is answered from
//! the source run's recorded response instead of the provider.
use super::types::LlmGateway;
use crate::StepError;
use camino::{Utf8Path, Utf8PathBuf};
use contract::NodeDef;
use llm::{ChatRequest, ChatResponse};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

const MEMO_DIR: &str = "llm-memo";

/// Recorded responses of a source run, by node and request digest, consumed
/// once each in journal order.
pub struct ReuseSource {
    run_id: String,
    metadata: Utf8PathBuf,
    calls: Mutex<BTreeMap<(String, String), VecDeque<(u64, String)>>>,
}

impl ReuseSource {
    /// Indexes the `llm_memo` records of nodes that finished successfully.
    pub fn from_events(run_id: &str, metadata: &Utf8Path, events: &[Value]) -> Self {
        let succeeded: BTreeSet<&str> = events
            .iter()
            .filter(|event| event["t"] == "step_finished" && event["status"] == "success")
            .filter_map(|event| event["node"].as_str())
            .collect();
        let mut calls = BTreeMap::<_, VecDeque<_>>::new();
        for event in events.iter().filter(|event| event["t"] == "llm_memo") {
            if let (Some(node), Some(digest), Some(name), Some(seq)) = (
                event["node"]
                    .as_str()
                    .filter(|node| succeeded.contains(node)),
                event["request_digest"].as_str(),
                event["memo_ref"].as_str(),
                event["seq"].as_u64(),
            ) {
                calls
                    .entry((node.to_string(), digest.to_string()))
                    .or_default()
                    .push_back((seq, name.to_string()));
            }
        }
        Self {
            run_id: run_id.to_string(),
            metadata: metadata.to_owned(),
            calls: Mutex::new(calls),
        }
    }
}

/// Reuse configuration of one step: recording is on, a source is optional.
#[derive(Clone)]
pub(crate) struct ReuseCtx {
    pub(crate) source: Option<Arc<ReuseSource>>,
    pub(crate) metadata: Utf8PathBuf,
}

/// Digest of everything the provider is asked: the node and the full request.
pub(super) fn request_digest(node: &NodeDef, request: &ChatRequest) -> Result<String, StepError> {
    let bytes =
        serde_json::to_vec(&json!({ "node": node.id, "type": node.kind, "request": request }))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

impl LlmGateway<'_> {
    /// Records responses and, with a source, reuses them when `enabled`.
    pub fn with_reuse(
        mut self,
        enabled: bool,
        source: Option<Arc<ReuseSource>>,
        metadata: &Utf8Path,
    ) -> Self {
        self.reuse = enabled.then(|| ReuseCtx {
            source,
            metadata: metadata.to_owned(),
        });
        self
    }

    /// Digest for a reuse-enabled gateway, `None` when reuse is off.
    pub(crate) fn reuse_digest(
        &self,
        node: &NodeDef,
        request: &ChatRequest,
    ) -> Result<Option<String>, StepError> {
        self.reuse
            .as_ref()
            .map(|_| request_digest(node, request))
            .transpose()
    }

    /// The source's verified response for this request, journaled as reused.
    /// Anything missing, altered or invalid falls through to a live call.
    pub(crate) fn reuse_response(&self, node: &NodeDef, digest: &str) -> Option<ChatResponse> {
        let source = self.reuse.as_ref()?.source.as_ref()?;
        let (seq, name) = source
            .calls
            .lock()
            .ok()?
            .get_mut(&(node.id.clone(), digest.to_string()))?
            .pop_front()?;
        let path = source.metadata.join(MEMO_DIR).join(format!("{name}.json"));
        let bytes =
            files::read_nofollow_bounded(&path, Some(policy::DEFAULT_MAX_STATE_BYTES)).ok()?;
        if hex::encode(Sha256::digest(&bytes)) != name {
            return None;
        }
        let response: ChatResponse = serde_json::from_slice(&bytes).ok()?;
        self.scan_response(node, &response).ok()?;
        self.record_response(node, digest, &response, Some((&source.run_id, seq)))
            .ok()?;
        Some(response)
    }

    /// Persists a response beside the journal and records its digest.
    pub(crate) fn record_response(
        &self,
        node: &NodeDef,
        digest: &str,
        response: &ChatResponse,
        reused_from: Option<(&str, u64)>,
    ) -> Result<(), StepError> {
        let Some(reuse) = &self.reuse else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(response)?;
        if bytes.len() > policy::DEFAULT_MAX_STATE_BYTES {
            return Ok(());
        }
        let name = hex::encode(Sha256::digest(&bytes));
        let dir = reuse.metadata.join(MEMO_DIR);
        std::fs::create_dir_all(&dir)?;
        files::write_sidecar_atomic(&dir.join(format!("{name}.json")), &bytes)?;
        let mut event = json!({ "node": node.id, "request_digest": digest, "memo_ref": name });
        if let Some((run_id, seq)) = reused_from {
            event["reused_from"] = json!({ "run_id": run_id, "seq": seq });
        }
        self.journal
            .event("llm_memo", event)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))
    }
}
