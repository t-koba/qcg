//! Durable, private child projection. No continuation entry point lives here.
use super::{ResetNode, ResetPlanCandidate};
use crate::{JournalError, RunState};
use contract::NodeState;
use model::NodePath;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const KIND: &str = "child_reset_overlay";

fn invalid() -> JournalError {
    JournalError::InvalidEvent("invalid child reset overlay evidence".into())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    binding_version: u8,
    plan_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reset_candidate_sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Overlay {
    version: u8,
    #[serde(deserialize_with = "decode_candidate")]
    candidate: ResetPlanCandidate,
    binding: Binding,
}

// Private decoding does not implement Deserialize on the opaque public candidate.
fn decode_candidate<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<ResetPlanCandidate, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Wire {
        identity: super::DiagnosticIdentity,
        source_run_id: String,
        source_contract_sha256: String,
        source_last_seq: u64,
        retry_roots: Vec<String>,
        stale_outcomes: Vec<String>,
        preserved_outcomes: Vec<String>,
        inherited_budget: crate::BudgetState,
        reset_nodes: BTreeMap<String, ResetNode>,
        expected_executor_attestations: BTreeMap<String, [u8; 32]>,
    }
    let w = Wire::deserialize(d)?;
    Ok(ResetPlanCandidate {
        identity: w.identity,
        required_files: None,
        source_run_id: w.source_run_id,
        source_contract_sha256: w.source_contract_sha256,
        source_last_seq: w.source_last_seq,
        retry_roots: w.retry_roots,
        stale_outcomes: w.stale_outcomes,
        preserved_outcomes: w.preserved_outcomes,
        inherited_budget: w.inherited_budget,
        reset_nodes: w.reset_nodes,
        expected_executor_attestations: w.expected_executor_attestations,
    })
}

pub(crate) fn digest(value: &Value) -> Result<String, JournalError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn candidate_digest(candidate: &ResetPlanCandidate) -> Result<String, JournalError> {
    // Hash the canonical wire object so service and engine validate exactly
    // the same bytes, independent of Rust struct field order.
    let value = serde_json::to_value(candidate)?;
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&value)?)))
}

impl ResetPlanCandidate {
    /// Service-only evidence serialization; accepts no replacement reset payload.
    #[doc(hidden)]
    pub fn fork_overlay_evidence(&self, binding: Value) -> Result<Value, JournalError> {
        let overlay = Overlay {
            version: 1,
            candidate: self.clone(),
            binding: serde_json::from_value(binding)?,
        };
        overlay.validate_binding()?;
        Ok(serde_json::to_value(overlay)?)
    }
}

impl Overlay {
    fn validate_binding(&self) -> Result<(), JournalError> {
        if self.version != 1
            || self.binding.binding_version != 1
            || self.binding.plan_digest.len() != 64
            || !self
                .binding
                .plan_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.binding.reset_candidate_sha256.as_deref()
                != Some(candidate_digest(&self.candidate)?.as_str())
        {
            return Err(invalid());
        }
        Ok(())
    }
}

pub(crate) fn validate(state: &RunState, event: &Value) -> Result<(), JournalError> {
    let kind = event["t"].as_str().ok_or_else(invalid)?;
    if state.reset_patch_forbidden && kind == "state_patched" {
        return Err(invalid());
    }
    if state.reset_queue_required
        && (kind != "run_queued"
            || event["run_id"].as_str() != state.run_id.as_deref()
            || event["contract_sha256"].as_str() != state.contract_sha256.as_deref())
    {
        return Err(invalid());
    }
    if let Some(fork) = state.pending_reset.as_ref() {
        if kind != KIND {
            return Err(invalid());
        }
        checked_overlay(state, event, fork)?;
        return Ok(());
    }
    if kind == KIND {
        return Err(invalid());
    }
    if !state.reset_gates.is_empty()
        && matches!(kind, "run_queued" | "run_started")
        && event["contract_sha256"].as_str() != state.contract_sha256.as_deref()
    {
        return Err(invalid());
    }
    if kind == "run_forked" {
        let candidate_hash = event
            .get("binding")
            .and_then(|binding| binding.get("reset_candidate_sha256"));
        let overlay_hash = event.get("reset_overlay_sha256");
        match (candidate_hash, overlay_hash) {
            (Some(Value::String(candidate_hash)), Some(Value::String(overlay_hash)))
                if valid_hash(candidate_hash) && valid_hash(overlay_hash) =>
            {
                let binding: Binding = serde_json::from_value(event["binding"].clone())?;
                if binding.binding_version != 1
                    || !valid_hash(&binding.plan_digest)
                    || binding.reset_candidate_sha256.as_deref() != Some(candidate_hash)
                {
                    return Err(invalid());
                }
            }
            (None, None) => {}
            _ => return Err(invalid()),
        }
    }
    Ok(())
}

/// Commit the reset-only effects after the ordinary event transition succeeds.
pub(crate) fn observe(state: &mut RunState, event: &Value) -> Result<(), JournalError> {
    let kind = event["t"].as_str().ok_or_else(invalid)?;
    if state.reset_patch_forbidden && kind == "run_forked" {
        state.reset_patch_forbidden = false;
    }
    if state.reset_queue_required && kind == "run_queued" {
        state.reset_queue_required = false;
        state.reset_patch_forbidden = true;
    }
    if state.pending_reset.is_some() && kind == KIND {
        let fork = state.pending_reset.take().ok_or_else(invalid)?;
        let overlay = checked_overlay(state, event, &fork)?;
        apply_overlay(state, overlay);
        state.reset_queue_required = true;
        return Ok(());
    }
    if kind == "run_forked" {
        let candidate_hash = event
            .get("binding")
            .and_then(|binding| binding.get("reset_candidate_sha256"));
        let overlay_hash = event.get("reset_overlay_sha256");
        if matches!(
            (candidate_hash, overlay_hash),
            (Some(Value::String(_)), Some(Value::String(_)))
        ) {
            state.pending_reset = Some(event.clone());
        }
    }
    Ok(())
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A full fold cannot authorize a reset admission that has not reached its queue.
pub(crate) fn complete(state: &RunState) -> Result<(), JournalError> {
    if state.pending_reset.is_some() || state.reset_queue_required {
        Err(invalid())
    } else {
        Ok(())
    }
}

fn checked_overlay(state: &RunState, event: &Value, fork: &Value) -> Result<Overlay, JournalError> {
    if event.as_object().is_none_or(|object| {
        object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "t" | "ts"
                    | "seq"
                    | "run_id"
                    | "trace_id"
                    | "span_id"
                    | "parent_span_id"
                    | "overlay"
            )
        })
    }) {
        return Err(invalid());
    }
    let value = event.get("overlay").ok_or_else(invalid)?;
    let overlay: Overlay = serde_json::from_value(value.clone())?;
    overlay.validate_binding()?;
    if serde_json::to_value(&overlay)? != *value {
        return Err(invalid());
    }
    let candidate = &overlay.candidate;
    if event["run_id"] != fork["run_id"]
        || fork["source_run_id"] != candidate.source_run_id
        || fork["source_seq"] != candidate.source_last_seq
        || state.contract_sha256.as_deref() != Some(candidate.source_contract_sha256.as_str())
        || fork["source_contract_sha256"] != candidate.source_contract_sha256
        || fork["binding"] != serde_json::to_value(&overlay.binding)?
        || fork["reset_overlay_sha256"] != digest(value)?
        || serde_json::to_value(&candidate.inherited_budget)?
            != serde_json::to_value(&state.budget)?
    {
        return Err(invalid());
    }
    let reset = candidate
        .retry_roots
        .iter()
        .chain(&candidate.stale_outcomes)
        .cloned()
        .collect::<BTreeSet<_>>();
    let preserved = candidate
        .preserved_outcomes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if candidate.retry_roots.is_empty()
        || reset.len() != candidate.retry_roots.len() + candidate.stale_outcomes.len()
        || preserved.len() != candidate.preserved_outcomes.len()
        || !reset.is_disjoint(&preserved)
        || candidate
            .expected_executor_attestations
            .keys()
            .any(String::is_empty)
        || candidate
            .expected_executor_attestations
            .keys()
            .collect::<BTreeSet<_>>()
            != reset.union(&preserved).collect::<BTreeSet<_>>()
        || reset != candidate.reset_nodes.keys().cloned().collect()
        || reset
            .union(&preserved)
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            != state.nodes.keys().map(NodePath::as_str).collect()
    {
        return Err(invalid());
    }
    if candidate.retry_roots.iter().any(|id| {
        !matches!(
            state.nodes.get(&NodePath::root(id)),
            Some(crate::NodeOutcome::Failed { .. })
        )
    }) || candidate
        .stale_outcomes
        .iter()
        .chain(&candidate.preserved_outcomes)
        .any(|id| {
            !matches!(
                state.nodes.get(&NodePath::root(id)),
                Some(crate::NodeOutcome::Success { .. })
            )
        })
    {
        return Err(invalid());
    }
    for (owner, node) in &candidate.reset_nodes {
        if owner.is_empty()
            || node.output_alias.is_empty()
            || !node.predecessors.is_subset(&reset)
            || node.predecessors.contains(owner)
        {
            return Err(invalid());
        }
    }
    Ok(overlay)
}

fn apply_overlay(state: &mut RunState, overlay: Overlay) {
    // A newer reset plan supersedes transient gates from earlier reset episodes.
    state.reset_gates.clear();
    state.reset_expected_attestations = overlay.candidate.expected_executor_attestations;
    // Nothing outside the current outcome/value projection is touched.
    for (
        owner,
        ResetNode {
            output_alias,
            predecessors,
        },
    ) in overlay.candidate.reset_nodes
    {
        state.nodes.remove(&NodePath::root(&owner));
        state.vars.clear_step_projection(&owner, &output_alias);
        state.reset_gates.insert(owner, predecessors);
    }
}

/// Unknown/missing targets never count as settled. Consult before dependency
/// skips, needs gates, or expressions can observe reset values/statuses.
pub(crate) fn settled(
    gates: &BTreeMap<String, BTreeSet<String>>,
    owner: &str,
    states: &BTreeMap<String, NodeState>,
) -> bool {
    gates.get(owner).is_none_or(|targets| {
        targets.iter().all(|target| {
            matches!(
                states.get(target),
                Some(NodeState::Success | NodeState::Skipped(_) | NodeState::Failed(_))
            )
        })
    })
}
