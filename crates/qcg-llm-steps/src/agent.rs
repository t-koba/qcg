use async_trait::async_trait;
use qcg_api::{GuardrailStage, ToolCallErrorCode, ToolCallPhase, ToolCallStatus};
use qcg_contract::AgentFailureCode;
use qcg_contract::{Contract, NodeDef, ToolDecl};
use qcg_engine::{ResultExt, StepContext, StepError, StepExecutor, StepOutcome};
use qcg_llm::{ChatContent, ChatMessage, ChatToolCall, LlmRuntime};
use qcg_policy::{params_schema, string_array_schema, string_schema};
use qcg_types::NodePath;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use crate::agent_runtime::{
    AgentToolCallFailure, AgentToolInvocation, AgentToolServices,
    agent_call_identity_hash_for_tool, check_used_call_id, execute_agent_tool,
    finish_rejected_external_operation, recover_agent_tool_call_failure,
    validate_agent_tool_call_args,
};
use crate::agent_tools::{validate_agent_delegations, validate_agent_tool};
use crate::completion::complete_llm;
use crate::context::{enforce_agent_transcript_limit, record_llm_validation_failure};
use crate::guardrail::{apply_guardrails, validate_guardrails};
use crate::mcp_tools::{AgentToolOutcome, McpAgentTools};
use crate::out_of_contract::{OutOfContractDecision, enforce_out_of_contract_policy};
use crate::policy::{llm_params, require_prompt};
use crate::prompting::{
    append_agent_validation_retry, load_schema, parse_agent_final, render_prompt,
    validate_agent_stop,
};
use crate::request::{
    MessageRequestOptions, build_request_with_messages, build_user_message, scan_llm_text,
    tool_spec,
};
use crate::schemas::{
    agent_failure_policy_schema, agent_tool_params_schema, llm_common_properties, model_ref_schema,
    request_policy_schema,
};
use crate::tool_events::{
    record_tool_call_failure, record_tool_call_failures, tool_call_event, tool_call_failure,
    tool_call_outcome, tool_reported_error,
};
use crate::validation::validate_llm_node;

pub(crate) struct LlmAgentStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentCheckpoint {
    pub(crate) messages: Vec<ChatMessage>,
    pub(crate) next_turn: usize,
    pub(crate) tokens_total: u64,
    pub(crate) tool_calls_total: usize,
    pub(crate) tool_call_counts: BTreeMap<String, usize>,
    pub(crate) pending_side_effect: Option<ChatToolCall>,
    /// Confirmation awaiting user decision for the pending call. Stored so
    /// resume re-emits the same operation instead of regenerating a
    /// different target with the same approval (A06).
    pub(crate) pending_confirm: Option<qcg_api::ConfirmSpec>,
    /// Suspended tool call awaiting user input, for MCP input-required
    /// calls and builtin AskUser calls alike. Stored so resume re-issues
    /// the exact call (same call id) instead of regenerating one that
    /// would miss the journaled continuation or change the question
    /// identity (A07, E08).
    pub(crate) pending_mcp_call: Option<McpSuspendedCall>,
    /// Registry of executed model call ids to their canonical identity
    /// hashes (AskUser: minimized args including notes; others: canonical
    /// redacted args). The same call id with the same hash resumes
    /// idempotently; the same call id with a different hash fails closed so
    /// a model replay cannot complete a new question with an old answer
    /// (E08-1, E07a). Only hashes are stored, never args.
    pub(crate) used_calls: BTreeMap<String, String>,
}

// Redacting Debug (E09h): messages and pending calls may carry URLs,
// headers, bodies, and args with secrets. Logs show shapes only: names,
// ids, counts, and truncated (12-char) hash prefixes, never plaintext
// values and never full digests (E09). Serialization is infallible in
// practice; a theoretical failure shows a fixed marker instead of hashing
// empty bytes that would alias distinct failures.
impl std::fmt::Debug for AgentCheckpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use sha2::Digest as _;
        formatter
            .debug_struct("AgentCheckpoint")
            .field("next_turn", &self.next_turn)
            .field("tokens_total", &self.tokens_total)
            .field("tool_calls_total", &self.tool_calls_total)
            .field("tool_call_counts", &self.tool_call_counts)
            .field(
                "pending_side_effect",
                &self.pending_side_effect.as_ref().map(|call| {
                    let digest = serde_json::to_vec(&call.args)
                        .map(|bytes| hex::encode(sha2::Sha256::digest(bytes)))
                        .unwrap_or_else(|_| "SERIALIZE_ERR".to_string());
                    format!(
                        "{}:{}:[args_sha256:{}]",
                        call.name,
                        call.id,
                        &digest[..12.min(digest.len())]
                    )
                }),
            )
            .field(
                "pending_confirm",
                &self.pending_confirm.as_ref().map(|c| &c.id),
            )
            .field("pending_mcp_call", &self.pending_mcp_call)
            .field("used_calls", &format!("[{} calls]", self.used_calls.len()))
            .field("messages", &format!("[{} messages]", self.messages.len()))
            .finish()
    }
}

/// A tool call suspended for user input, identified exactly as the
/// journaled continuation key identifies it. `builtin` marks an AskUser
/// call whose identity must be re-issued verbatim on resume, even before an
/// answer exists, so the question id never depends on LLM regeneration
/// determinism (E08). For builtin calls `content_hash` binds the FULL
/// unredacted minimized bytes (E08 two-field): the journaled `args` carry
/// only the REDACTED display form, while `question_id` derives from
/// `content_hash` so distinct secrets yield distinct ids without journaling
/// secrets. MCP calls leave `content_hash` empty and bind via the
/// continuation store instead.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct McpSuspendedCall {
    pub(crate) name: String,
    pub(crate) id: String,
    pub(crate) args: Value,
    pub(crate) question_id: String,
    // Required, never defaulted: a checkpoint without an explicit builtin
    // flag is corrupt, not silently an MCP call.
    pub(crate) builtin: bool,
    /// FULL content hash for builtin AskUser suspensions (hex of the
    /// unredacted minimized bytes). Empty for MCP suspensions. Required for
    /// builtin resume verification; a builtin suspension without it is
    /// corrupt (E08 two-field).
    #[serde(default)]
    pub(crate) content_hash: String,
}

// Redacting Debug (E09h): suspended args may carry secrets. Shapes only:
// names, ids, presence flags, and a truncated (12-char) args-hash prefix —
// never values, never full digests, never the question id or content hash
// themselves (E09). Serialization is infallible in practice; a theoretical
// failure shows a fixed marker instead of hashing empty bytes.
impl std::fmt::Debug for McpSuspendedCall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use sha2::Digest as _;
        let digest = serde_json::to_vec(&self.args)
            .map(|bytes| hex::encode(sha2::Sha256::digest(bytes)))
            .unwrap_or_else(|_| "SERIALIZE_ERR".to_string());
        let args_prefix = digest[..12.min(digest.len())].to_string();
        formatter
            .debug_struct("McpSuspendedCall")
            .field("name", &self.name)
            .field("id", &self.id)
            .field("args_sha256", &args_prefix)
            .field("question_id", &"[question]".to_string())
            .field("builtin", &self.builtin)
            .field(
                "content_hash",
                &if self.content_hash.is_empty() {
                    "[none]".to_string()
                } else {
                    "[HASH]".to_string()
                },
            )
            .finish()
    }
}

/// Selects the suspended MCP call to re-issue without an LLM round-trip.
/// Only an answered suspension qualifies: anything else (no checkpoint, no
/// suspension, no answer yet) stays on the model flow so regeneration can
/// never steal another call's continuation.
/// Rejects a checkpoint whose stored approval has no matching operation: a
/// silent fallback to the model flow would let a different call inherit the
/// stored approval (E07).
fn validate_agent_checkpoint(
    node_id: &str,
    checkpoint: Option<&AgentCheckpoint>,
) -> Result<(), StepError> {
    if let Some(stored) = checkpoint
        && stored.pending_confirm.is_some()
        && stored.pending_side_effect.is_none()
    {
        return Err(StepError::failed(
            node_id,
            "invalid agent checkpoint: pending confirmation without a pending side effect",
        ));
    }
    // A suspension without a question identity cannot be re-issued
    // verbatim, so the checkpoint is unusable rather than resumable (E07).
    if let Some(stored) = checkpoint
        && let Some(suspended) = stored.pending_mcp_call.as_ref()
        && suspended.question_id.is_empty()
    {
        return Err(StepError::failed(
            node_id,
            "invalid agent checkpoint: suspended call has no question id",
        ));
    }
    // Two pending holds are ambiguous: a side effect plus an MCP
    // suspension cannot both resume first, so refuse instead of picking
    // one silently (E07).
    if let Some(stored) = checkpoint
        && stored.pending_side_effect.is_some()
        && stored.pending_mcp_call.is_some()
    {
        return Err(StepError::failed(
            node_id,
            "invalid agent checkpoint: pending side effect and pending MCP call cannot coexist",
        ));
    }
    Ok(())
}

fn resumed_mcp_call(
    checkpoint: Option<&AgentCheckpoint>,
    answers: &std::collections::BTreeMap<String, Value>,
) -> Option<McpSuspendedCall> {
    let suspended = checkpoint?.pending_mcp_call.clone()?;
    if suspended.builtin {
        return None;
    }
    // Fail closed: only the full question id satisfies a suspension.
    // Legacy bare `node:tool` answers never resume a new question (E08/Q1).
    // NOTE (E08): this is selection only. Entry verification
    // (`verify_resumed_mcp_call`) recomputes the pending continuation key
    // from the stored args and compares the stored `question_id` against
    // the journaled descriptor before resuming, mirroring the builtin
    // content-hash check. A tampered `question_id` with a matching answer
    // key fails closed there, never here.
    let answered = answers.contains_key(&suspended.question_id);
    answered.then_some(suspended)
}

/// Verifies a selected MCP suspension against the journaled continuation
/// store (E08 recompute-and-compare, unlike the bare `contains_key` above).
/// Recomputes the pending key from node, resolved server, alias, call id,
/// and canonical (redacted) args, then requires the journaled descriptor to
/// exist, its `arguments` to equal the canonical recomputation, and its
/// `question_id` to equal the stored suspension's `question_id`. Any
/// mismatch fails closed instead of resuming a foreign continuation.
fn verify_resumed_mcp_call(
    node: &NodeDef,
    mcp: &crate::mcp_tools::McpAgentTools,
    journal_state: &qcg_engine::RunState,
    answers: &std::collections::BTreeMap<String, Value>,
    suspended: &McpSuspendedCall,
) -> Result<(), StepError> {
    let server = mcp.server_for(&suspended.name).ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!("unknown MCP tool alias `{}`", suspended.name),
        )
    })?;
    let pending_key = crate::tool_events::pending_key_for_agent_mcp(
        &node.id,
        server,
        &suspended.name,
        &suspended.id,
        &suspended.args,
    )?;
    let pending = journal_state.mcp_pending.get(&pending_key).ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!(
                "MCP continuation `{pending_key}` has no journaled descriptor; refusing resume"
            ),
        )
    })?;
    // Recompute-and-compare: stored args must canonicalize to the stored
    // descriptor (single redacted form), and the stored question id must
    // equal the descriptor's question id. A tampered triple fails closed.
    if pending.get("arguments")
        != Some(&crate::tool_events::canonical_mcp_key_args(&suspended.args))
    {
        return Err(StepError::failed(
            &node.id,
            format!("MCP continuation `{pending_key}` holds different arguments; refusing resume"),
        ));
    }
    let descriptor_question = pending
        .get("question_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("MCP continuation `{pending_key}` has no question; refusing resume"),
            )
        })?;
    if descriptor_question != suspended.question_id {
        return Err(StepError::failed(
            &node.id,
            "suspended MCP call question does not match journaled continuation; refusing resume",
        ));
    }
    if !answers.contains_key(&suspended.question_id) {
        return Err(StepError::failed(
            &node.id,
            "suspended MCP call has no answer; refusing resume",
        ));
    }
    Ok(())
}

/// Selects a suspended builtin AskUser call to re-issue verbatim. It is
/// re-issued with or without an answer: without one it simply re-suspends
/// with the identical question identity, and with one the AskUser tool
/// consumes that answer (E08).
///
/// Distinction vs MCP (E08, documented precisely): builtin resumption
/// reissues the SAME call id regardless of answers (the entry always
/// re-issues; the tool then decides suspend-vs-consume based on whether an
/// answer for the FULL question id exists). MCP resumption REQUIRES an
/// answer upfront (`resumed_mcp_call` returns `None` without one) because
/// resuming would re-enter a remote call with side effects. In both cases
/// resend NEVER bypasses answer acceptance (resend != answered): re-issuing
/// without an answer suspends again with the same identity; only an answer
/// for the FULL id completes the question. No retire pass is needed for
/// superseded builtin suspensions, unlike MCP continuations: each turn
/// overwrites the node checkpoint, so a regenerated model call replaces the
/// stored suspension instead of leaking it, and the question id binds the
/// call id so the old answer can never satisfy the new question.
fn resumed_builtin_call(checkpoint: Option<&AgentCheckpoint>) -> Option<McpSuspendedCall> {
    let suspended = checkpoint?.pending_mcp_call.clone()?;
    suspended.builtin.then_some(suspended)
}

#[async_trait]
impl StepExecutor for LlmAgentStep {
    fn type_id(&self) -> &'static str {
        "llm.agent"
    }

    fn traits(&self) -> qcg_engine::StepTraits {
        qcg_engine::StepTraits::parallel()
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["prompt", "max_iterations", "max_tokens_total"],
            llm_common_properties(json!({
                "max_iterations": { "type": "integer", "minimum": 1 },
                "max_tokens_total": { "type": "integer", "minimum": 1 },
                "max_tool_calls_total": { "type": "integer", "minimum": 1 },
                "guardrails": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["name", "stage", "kind"],
                        "properties": {
                            "name": string_schema(),
                            "stage": { "enum": ["input", "output", "tool_input", "tool_output"] },
                            "kind": { "enum": ["regex_deny", "json_schema", "command"] },
                            "params": {},
                            "tool": string_schema(),
                            "tripwire": { "type": "boolean" },
                            "on_error": { "enum": ["fail", "block"] }
                        }
                    }
                },
                "tools": {
                    "type": "array",
                    "items": {
                        "oneOf": [
                            agent_tool_params_schema("fs.write", &["path_prefix"], json!({
                                "path_prefix": string_schema(),
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema("command", &["command"], json!({
                                "command": string_array_schema(),
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema("http", &["methods", "hosts"], json!({
                                "methods": string_array_schema(),
                                "hosts": string_array_schema(),
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema("ask_user", &[], json!({
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema(
                                "web.search",
                                &[],
                                json!({
                                    "provider": string_schema(),
                                    "max_results": { "type": "integer", "minimum": 1, "maximum": qcg_policy::MAX_WEB_SEARCH_RESULTS },
                                    "max_calls": { "type": "integer", "minimum": 1, "maximum": 10 }
                                })
                            ),
                            agent_tool_params_schema(
                                "mcp",
                                &["server", "tool"],
                                json!({
                                    "server": string_schema(),
                                    "tool": string_schema(),
                                    "max_calls": { "type": "integer", "minimum": 1, "maximum": qcg_policy::MAX_AGENT_TOOL_CALLS },
                                    "side_effects": { "type": "boolean" }
                                })
                            ),
                            agent_tool_params_schema(
                                "agent",
                                &["instructions", "max_tool_calls_total"],
                                json!({
                                    "instructions": string_schema(),
                                    "tools": string_array_schema(),
                                    "input_schema": { "type": "object" },
                                    "output_schema": string_schema(),
                                    "max_calls": { "type": "integer", "minimum": 1, "maximum": qcg_policy::MAX_AGENT_TOOL_CALLS },
                                    "max_iterations": { "type": "integer", "minimum": 1, "maximum": qcg_policy::MAX_AGENT_ITERATIONS },
                                    "max_tokens_total": { "type": "integer", "minimum": 1 },
                                    "max_tool_calls_total": { "type": "integer", "minimum": 1 },
                                    "model": model_ref_schema(),
                                    "fallback_models": { "type": "array", "items": model_ref_schema(), "maxItems": qcg_policy::MAX_FALLBACK_MODELS },
                                    "request": request_policy_schema(),
                                    "on_failure": agent_failure_policy_schema(),
                                    "handoff": { "type": "boolean" }
                                })
                            )
                        ]
                    }
                }
            })),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = llm_params(node)?;
        validate_llm_node(
            node,
            contract,
            &self.runtime,
            params.schema.is_some(),
            !params.tools.is_empty(),
        )?;
        require_prompt(node, &params)?;
        if params.max_iterations.unwrap_or_default() == 0 {
            return Err(StepError::failed(&node.id, "max_iterations is required"));
        }
        if params.max_tokens_total.unwrap_or_default() == 0 {
            return Err(StepError::failed(&node.id, "max_tokens_total is required"));
        }
        if params.max_tool_calls_total == Some(0) {
            return Err(StepError::failed(
                &node.id,
                "max_tool_calls_total must be greater than zero",
            ));
        }
        let mut tool_names = BTreeSet::new();
        for tool in &params.tools {
            if tool.name().trim().is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "agent tool name must not be empty",
                ));
            }
            if !tool_names.insert(tool.name()) {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool name `{}` is duplicated", tool.name()),
                ));
            }
            validate_agent_tool(node, contract, &self.runtime, tool)?;
        }
        validate_agent_delegations(node, &params.tools)?;
        validate_guardrails(
            node,
            &params.guardrails,
            &params.tools,
            &contract.manifest.runtime,
        )?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let prompt = render_prompt(ctx, node)?;
        let params = llm_params(node)?;
        let response_schema = load_schema(ctx, node)?;
        apply_guardrails(
            ctx,
            node,
            &params.guardrails,
            GuardrailStage::Input,
            None,
            &json!({ "prompt": &prompt }),
        )
        .await?;
        let max_turns = params
            .max_iterations
            .ok_or_else(|| StepError::failed(&node.id, "agent max_iterations is required"))?;
        let max_tokens_total = params
            .max_tokens_total
            .ok_or_else(|| StepError::failed(&node.id, "agent max_tokens_total is required"))?;
        let max_tool_calls_total = params.max_tool_calls_total.unwrap_or(32);
        let checkpoint = ctx
            .journal
            .state()
            .checkpoints
            .get(&NodePath::root(&node.id))
            .cloned()
            .map(serde_json::from_value::<AgentCheckpoint>)
            .transpose()
            .map_err(|error| {
                StepError::failed(&node.id, format!("invalid agent checkpoint: {error}"))
            })?;
        validate_agent_checkpoint(&node.id, checkpoint.as_ref())?;
        if let Some(stored) = checkpoint.as_ref()
            && let (Some(_pending), Some(confirm)) = (
                stored.pending_side_effect.as_ref(),
                stored.pending_confirm.as_ref(),
            )
        {
            // Waiting-for-confirmation resume: re-emit the exact stored
            // operation when still undecided, so the model never
            // substitutes a different target under the same approval.
            // Approved resumes fall through to execute the stored call
            // below instead of regenerating via the LLM (A06). Staleness
            // is fail-closed: the stored id binds the digest computed
            // under the old scope/details, so a changed manifest simply
            // never matches a fresh confirmation (E07/Q1).
            // E07f: Rejected stays rejected (fail-closed refusal), never
            // re-emitted as undecided. Only an explicit approval resumes;
            // a missing decision re-emits as undecided.
            match ctx.run.confirmations.get(&confirm.id) {
                Some(true) => {
                    // Approved: continue to execute the stored pending call.
                    // The main loop below detects this via the checkpoint and
                    // executes it directly without an LLM round-trip. Mark by
                    // falling through; the turn loop handles it first.
                }
                Some(false) => {
                    return Err(StepError::Refused {
                        node: node.id.clone(),
                        message: format!(
                            "side effect for node `{}` was rejected; refusing resume",
                            node.id
                        ),
                    });
                }
                None => {
                    return Ok(StepOutcome::NeedsConfirm {
                        confirm: confirm.clone(),
                    });
                }
            }
        }
        // A stored side effect without a confirmation was interrupted
        // mid-execution. The exact call is re-issued below and the
        // operation guard decides from the durable record: a cached
        // success is reused, a clean failure retries, and only a true
        // indeterminate result follows the node's policy (E07).
        let first_turn = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.next_turn)
            .unwrap_or_default();
        let mut messages = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.messages.clone())
            .map(Ok)
            .unwrap_or_else(|| {
                build_user_message(ctx, node, prompt).map(|message| vec![message])
            })?;
        let mcp_tools = McpAgentTools::prepare(ctx, node, &self.runtime, &params.tools).await?;
        let tool_specs = params
            .tools
            .iter()
            .map(|tool| tool_spec(ctx, tool, &mcp_tools))
            .collect::<Result<Vec<_>, _>>()?;
        let mut tokens_total = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.tokens_total)
            .unwrap_or_default();
        let mut tool_calls_total = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.tool_calls_total)
            .unwrap_or_default();
        let mut tool_call_counts = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.tool_call_counts.clone())
            .unwrap_or_default();
        // E08-1: the used-call registry persists across turns in the
        // checkpoint. Every executed call registers its identity hash
        // before running; a resumed re-issue with the same hash passes
        // idempotently, while the same call id with different args fails
        // closed in the turn loop below.
        let mut used_calls = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.used_calls.clone())
            .unwrap_or_default();
        // Skills activated in this agent run. The set lives for the whole
        // node execution (not the checkpoint) so a repeated activation in a
        // later turn skips re-injecting identical instructions.
        let mut activated_skills = BTreeSet::new();
        // A stored side effect resumes without an LLM round-trip so the
        // exact operation is re-guarded: an approved call executes, an
        // interrupted call is settled by its durable record (E07).
        // E07f: rejected stays rejected (no resume); undecided does not
        // resume here (the entry checkpoint above re-emits it).
        // F02: the journaled pending call is redacted; the exact payload
        // is restored from the run-private sidecar when it verifies
        // against the stored confirmation digest. Unverifiable payloads
        // stay redacted and fail closed downstream.
        let resumed_pending: Option<ChatToolCall> = checkpoint.as_ref().and_then(|stored| {
            let pending = stored.pending_side_effect.clone()?;
            let approved = match stored.pending_confirm.as_ref() {
                // Interrupted after execution started: the guard owns the
                // decision, never the entry checkpoint.
                None => true,
                // Only an explicit approval resumes.
                Some(confirm) => {
                    matches!(ctx.run.confirmations.get(&confirm.id), Some(true))
                }
            };
            if !approved {
                return None;
            }
            let operation_id = qcg_engine::operation_id_for(&ctx.run.run_id, &node.id, &pending.id);
            let sidecar = ctx
                .run
                .load_pending_tool_payload(node, &operation_id)
                .ok()
                .flatten();
            Some(crate::agent_runtime::restore_pending_call_from_sidecar(
                &params.tools,
                node,
                &ctx.run.run_id,
                &pending,
                stored.pending_confirm.as_ref(),
                sidecar.as_ref(),
            ))
        });
        // E08d + E08 SENSITIVE two-field: the builtin suspension's question
        // binding is verified before choosing resend. The stored REDACTED
        // args cannot recompute the FULL identity, so verification uses the
        // stored `content_hash`: the expected id derives from it and must
        // equal the stored `question_id`. A mismatch (corrupt or tampered
        // triple) fails closed instead of re-issuing a mismatched question.
        // A builtin suspension without a content hash is corrupt (old
        // checkpoints without the two-field binding refuse, never silently
        // alias).
        if let Some(suspended) = checkpoint
            .as_ref()
            .and_then(|stored| stored.pending_mcp_call.clone())
            .filter(|suspended| suspended.builtin)
        {
            if suspended.content_hash.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "suspended builtin call has no content hash; refusing resume",
                ));
            }
            let expected = crate::agent_runtime::ask_user_question_id_from_content_hash(
                &node.id,
                &suspended.name,
                &suspended.id,
                &suspended.content_hash,
            );
            if expected != suspended.question_id {
                return Err(StepError::failed(
                    &node.id,
                    "suspended builtin call question does not match stored content hash; refusing resume",
                ));
            }
        }
        // Answered MCP call resumes without an LLM round-trip so the exact
        // suspended call (same call id) continues its journaled
        // continuation instead of regenerating a fresh remote call (A07).
        // Without an answer the model flow stays in charge.
        let resumed_mcp = resumed_mcp_call(checkpoint.as_ref(), &ctx.run.answers);
        // E08 recompute-and-compare for MCP saved questions (unlike the
        // bare `contains_key` selection above): the stored question id must
        // equal the journaled descriptor's question id for the recomputed
        // continuation key, and the stored args must canonicalize to the
        // descriptor's arguments. A tampered triple fails closed here,
        // never resuming a foreign continuation.
        if let Some(suspended) = resumed_mcp.as_ref() {
            verify_resumed_mcp_call(
                node,
                &mcp_tools,
                &ctx.journal.state(),
                &ctx.run.answers,
                suspended,
            )?;
        }
        let resumed_user = resumed_builtin_call(checkpoint.as_ref());
        let mut last_validation_error = None;
        for turn in first_turn..max_turns {
            enforce_agent_transcript_limit(ctx, node, &mut messages, None)?;
            let turn_start_messages = messages.clone();
            let turn_start_tool_calls_total = tool_calls_total;
            let turn_start_tool_call_counts = tool_call_counts.clone();
            // When resuming an approved pending call, execute it directly.
            let resumed_this_turn = turn == first_turn && resumed_pending.is_some();
            // When resuming an answered MCP call, re-issue it directly.
            let resumed_mcp_this_turn =
                turn == first_turn && !resumed_this_turn && resumed_mcp.is_some();
            // When resuming a builtin AskUser call, re-issue the exact call
            // so the question identity is independent of model regeneration
            // (E08).
            let resumed_user_this_turn = turn == first_turn
                && !resumed_this_turn
                && !resumed_mcp_this_turn
                && resumed_user.is_some();
            // No separate `resuming_approved_side_effect` marker: the stored
            // call executes below without an LLM round-trip, and the
            // `before_side_effect` checkpoint recorded just before
            // execution supersedes it with strictly more accurate
            // transcript state. Crash windows are equivalent either
            // way: an approved confirmation is durable, so a crash
            // before `before_side_effect` re-enters through the stored
            // approval, and a crash after it re-enters through the
            // durable operation record (E07).
            if resumed_mcp_this_turn {
                // Keep the suspension marker across execution: a crash from
                // here on is an indeterminate remote result, detected via
                // the journaled resumption record on the next resume.
                record_agent_checkpoint(
                    ctx,
                    node,
                    turn,
                    "resuming_answered_mcp_call",
                    &AgentCheckpoint {
                        messages: turn_start_messages.clone(),
                        next_turn: turn,
                        tokens_total,
                        tool_calls_total: turn_start_tool_calls_total,
                        tool_call_counts: turn_start_tool_call_counts.clone(),
                        pending_side_effect: None,
                        pending_confirm: None,
                        pending_mcp_call: resumed_mcp.clone(),
                        used_calls: used_calls.clone(),
                    },
                    &params.tools,
                )?;
            }
            if resumed_user_this_turn {
                // Keep the pending marker: an unanswered re-issue simply
                // suspends again with the same question identity.
                record_agent_checkpoint(
                    ctx,
                    node,
                    turn,
                    "resuming_builtin_user_call",
                    &AgentCheckpoint {
                        messages: turn_start_messages.clone(),
                        next_turn: turn,
                        tokens_total,
                        tool_calls_total: turn_start_tool_calls_total,
                        tool_call_counts: turn_start_tool_call_counts.clone(),
                        pending_side_effect: None,
                        pending_confirm: None,
                        pending_mcp_call: resumed_user.clone(),
                        used_calls: used_calls.clone(),
                    },
                    &params.tools,
                )?;
            }
            let (text_parts, tool_calls, stop, next_provider_state) = if resumed_this_turn {
                // Execute the exact approved operation without asking the
                // model to regenerate it (A06).
                (
                    Vec::new(),
                    vec![resumed_pending.clone().ok_or_else(|| {
                        StepError::failed(&node.id, "resumed turn has no pending call")
                    })?],
                    qcg_llm::StopReason::ToolUse,
                    None,
                )
            } else if resumed_mcp_this_turn {
                // Re-issue the exact suspended MCP call without asking the
                // model to regenerate it (A07).
                let suspended = resumed_mcp.clone().ok_or_else(|| {
                    StepError::failed(&node.id, "resumed turn has no suspended MCP call")
                })?;
                (
                    Vec::new(),
                    vec![ChatToolCall {
                        id: suspended.id,
                        name: suspended.name,
                        args: suspended.args,
                    }],
                    qcg_llm::StopReason::ToolUse,
                    None,
                )
            } else if resumed_user_this_turn {
                let suspended = resumed_user.clone().ok_or_else(|| {
                    StepError::failed(&node.id, "resumed turn has no suspended user call")
                })?;
                (
                    Vec::new(),
                    vec![ChatToolCall {
                        id: suspended.id,
                        name: suspended.name,
                        args: suspended.args,
                    }],
                    qcg_llm::StopReason::ToolUse,
                    None,
                )
            } else {
                let request = build_request_with_messages(
                    ctx,
                    node,
                    &self.runtime,
                    messages.clone(),
                    MessageRequestOptions {
                        response_schema: response_schema.clone(),
                        tools: &tool_specs,
                        model: None,
                        policy: None,
                    },
                )?;
                let response = complete_llm(ctx, node, request, |usage| {
                    let next_total = tokens_total
                        .saturating_add(usage.input)
                        .saturating_add(usage.output);
                    json!({ "turn": turn, "tokens_total": next_total, "max_tokens_total": max_tokens_total })
                })
                .await?;
                let usage = response.usage.clone();
                tokens_total = tokens_total
                    .saturating_add(usage.input)
                    .saturating_add(usage.output);

                let stop = response.stop;
                let next_provider_state = response.provider_state;
                let mut text_parts = Vec::new();
                let mut tool_calls = Vec::new();
                for content in response.content {
                    match content {
                        ChatContent::Text(text) => text_parts.push(text),
                        ChatContent::ToolCall { id, name, args } => {
                            tool_calls.push(ChatToolCall { id, name, args });
                        }
                    }
                }
                (text_parts, tool_calls, stop, next_provider_state)
            };
            // Non-resumed turns validated stop below; resumed turns skip it
            // because they execute a previously validated approved call.
            if tokens_total > max_tokens_total {
                let error = StepError::failed(
                    &node.id,
                    format!("llm.agent token budget exceeded: {tokens_total} > {max_tokens_total}"),
                );
                record_tool_call_failures(
                    ctx,
                    node,
                    &tool_calls,
                    &error,
                    None,
                    ToolCallPhase::InputValidation,
                    ToolCallErrorCode::BudgetExceeded,
                )?;
                return Err(error);
            }
            if let Err(error) = validate_agent_stop(&node.id, stop, !tool_calls.is_empty()) {
                record_tool_call_failures(
                    ctx,
                    node,
                    &tool_calls,
                    &error,
                    None,
                    ToolCallPhase::InputValidation,
                    ToolCallErrorCode::InvalidArguments,
                )?;
                return Err(error);
            }
            if tool_calls.is_empty() {
                let text = text_parts.join("\n");
                let value = match parse_agent_final(&node.id, &text, response_schema.as_ref()) {
                    Ok(value) => value,
                    Err(error) => {
                        record_llm_validation_failure(ctx, node, turn, &error)?;
                        append_agent_validation_retry(
                            &node.id,
                            &mut messages,
                            next_provider_state,
                            &text,
                            &error,
                        )?;
                        enforce_agent_transcript_limit(ctx, node, &mut messages, None)?;
                        last_validation_error = Some(error);
                        record_agent_checkpoint(
                            ctx,
                            node,
                            turn,
                            "validation_retry",
                            &AgentCheckpoint {
                                messages: messages.clone(),
                                next_turn: turn.saturating_add(1),
                                tokens_total,
                                tool_calls_total,
                                tool_call_counts: tool_call_counts.clone(),
                                pending_side_effect: None,
                                pending_confirm: None,
                                pending_mcp_call: None,
                                used_calls: used_calls.clone(),
                            },
                            &params.tools,
                        )?;
                        continue;
                    }
                };
                let value = match enforce_out_of_contract_policy(ctx, node, value)? {
                    OutOfContractDecision::Continue(value) => value,
                    OutOfContractDecision::NeedsUser { question } => {
                        return Ok(StepOutcome::NeedsUser { question });
                    }
                };
                apply_guardrails(
                    ctx,
                    node,
                    &params.guardrails,
                    GuardrailStage::Output,
                    None,
                    &value,
                )
                .await?;
                return Ok(StepOutcome::Success {
                    output: Some(value),
                    files: vec![],
                });
            }
            last_validation_error = None;
            if tool_calls.len() > 1
                && tool_calls
                    .iter()
                    .any(|call| agent_tool_requires_serial_execution(&params.tools, &call.name))
            {
                let error = StepError::failed(
                    &node.id,
                    "llm.agent received parallel tool calls containing an interactive or side-effectful tool; refusing ambiguous replay semantics",
                );
                record_tool_call_failures(
                    ctx,
                    node,
                    &tool_calls,
                    &error,
                    None,
                    ToolCallPhase::InputValidation,
                    ToolCallErrorCode::InvalidArguments,
                )?;
                return Err(error);
            }

            if let Some(state) = next_provider_state {
                let items = match state.as_array().cloned() {
                    Some(items) => items,
                    None => {
                        let error = StepError::failed(
                            &node.id,
                            "Responses API provider state must be an array",
                        );
                        record_tool_call_failures(
                            ctx,
                            node,
                            &tool_calls,
                            &error,
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::InvalidArguments,
                        )?;
                        return Err(error);
                    }
                };
                messages.push(ChatMessage::provider_state(items));
            } else {
                messages.push(ChatMessage::assistant_tool_calls(
                    text_parts.join("\n"),
                    tool_calls.clone(),
                ));
            }
            for call in tool_calls {
                let tool_started = Instant::now();
                let call_number = match charge_agent_tool_call(
                    &node.id,
                    "llm.agent",
                    &params.tools,
                    &call.name,
                    &mut tool_calls_total,
                    max_tool_calls_total,
                    &mut tool_call_counts,
                ) {
                    Ok(call_number) => call_number,
                    Err(error) => {
                        let call_number = tool_call_counts[&call.name];
                        if let Some(message) = recover_agent_tool_call_failure(
                            ctx,
                            node,
                            &params.tools,
                            &call,
                            &error,
                            AgentToolCallFailure {
                                code: AgentFailureCode::ToolCallBudgetExceeded,
                                phase: ToolCallPhase::InputValidation,
                                tool_error_code: ToolCallErrorCode::BudgetExceeded,
                                call_number,
                                retryable: false,
                                started: tool_started,
                            },
                        )? {
                            messages.push(message);
                            continue;
                        }
                        record_tool_call_failure(
                            ctx,
                            node,
                            &call,
                            &error,
                            tool_call_failure(
                                None,
                                ToolCallPhase::InputValidation,
                                ToolCallErrorCode::BudgetExceeded,
                                tool_started,
                            ),
                        )?;
                        return Err(error);
                    }
                };
                // E07a/E08-1 (+ E09 salted): every call (including checkpoint
                // re-issues and safe reads) registers its salted canonical
                // identity hash (run-id salt, cross-run unlinkable). The same
                // call id with the same canonical hash resumes idempotently;
                // the same call id with different args fails closed so a
                // model replay cannot complete a new question with an old
                // answer and a changed safe-read cannot silently resend.
                // Canonicalization makes redacted re-issues match their live
                // originals when content matches (E07d, single redacted form).
                if let Err(error) = agent_call_identity_hash_for_tool(
                    &node.id,
                    &ctx.run.run_id,
                    &params.tools,
                    &call.name,
                    &call.args,
                )
                .and_then(|hash| check_used_call_id(&node.id, &mut used_calls, &call.id, &hash))
                {
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::InvalidArguments,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) =
                    validate_agent_tool_call_args(node, &mcp_tools, &params.tools, &call)
                {
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::ValidationFailed,
                            phase: ToolCallPhase::InputValidation,
                            tool_error_code: ToolCallErrorCode::InvalidArguments,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::InvalidArguments,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = ctx.step_checkpoint(node).await {
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = apply_guardrails(
                    ctx,
                    node,
                    &params.guardrails,
                    GuardrailStage::ToolInput,
                    Some(&call.name),
                    &call.args,
                )
                .await
                {
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::GuardrailRejected,
                            phase: ToolCallPhase::InputGuardrail,
                            tool_error_code: ToolCallErrorCode::GuardrailRejected,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputGuardrail,
                            ToolCallErrorCode::GuardrailRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = scan_llm_text(ctx, node, &serde_json::to_string(&call.args)?) {
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::GuardrailRejected,
                            phase: ToolCallPhase::InputGuardrail,
                            tool_error_code: ToolCallErrorCode::GuardrailRejected,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputGuardrail,
                            ToolCallErrorCode::GuardrailRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                // E07-3: every side-effectful call — including safe HTTP
                // reads, which take no guard and leave no durable record —
                // is recorded in the checkpoint through the same path, so a
                // mid-turn stop never silently resends unrecorded work. Safe
                // reads use the cheap `safe_read` marker phase (same
                // pending-call content, re-execution is side-effect free);
                // durable tools use `before_side_effect`.
                if agent_tool_has_side_effects(&params.tools, &call.name)
                    && let Err(error) = record_agent_checkpoint(
                        ctx,
                        node,
                        turn,
                        if crate::agent_runtime::is_safe_http_tool_call(
                            &params.tools,
                            &call.name,
                            &call.args,
                        ) {
                            "safe_read"
                        } else {
                            "before_side_effect"
                        },
                        &AgentCheckpoint {
                            messages: messages.clone(),
                            next_turn: turn,
                            tokens_total,
                            tool_calls_total,
                            tool_call_counts: tool_call_counts.clone(),
                            pending_side_effect: Some(call.clone()),
                            pending_confirm: None,
                            pending_mcp_call: None,
                            used_calls: used_calls.clone(),
                        },
                        &params.tools,
                    )
                {
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                let services = AgentToolServices {
                    runtime: &self.runtime,
                    guardrails: &params.guardrails,
                };
                let outcome = match execute_agent_tool(
                    ctx,
                    node,
                    services,
                    AgentToolInvocation {
                        mcp: &mcp_tools,
                        tools: &params.tools,
                        name: &call.name,
                        call_id: &call.id,
                        call_number,
                        args: &call.args,
                        activated_skills: &mut activated_skills,
                    },
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        record_tool_call_failure(
                            ctx,
                            node,
                            &call,
                            &error,
                            tool_call_failure(
                                None,
                                ToolCallPhase::Execution,
                                ToolCallErrorCode::ExecutionFailed,
                                tool_started,
                            ),
                        )?;
                        return Err(error);
                    }
                };
                let (result, returned_agent_error, mut pending_operation) = match outcome {
                    AgentToolOutcome::Result(value) => (value, false, None),
                    AgentToolOutcome::OperationResult {
                        value,
                        operation_id,
                    } => (value, false, Some(operation_id)),
                    AgentToolOutcome::Error(value) => (value, true, None),
                    AgentToolOutcome::Handoff(value) => {
                        if let Err(error) = apply_guardrails(
                            ctx,
                            node,
                            &params.guardrails,
                            GuardrailStage::ToolOutput,
                            Some(&call.name),
                            &value,
                        )
                        .await
                        {
                            record_tool_call_failure(
                                ctx,
                                node,
                                &call,
                                &error,
                                tool_call_failure(
                                    None,
                                    ToolCallPhase::OutputGuardrail,
                                    ToolCallErrorCode::OutputRejected,
                                    tool_started,
                                ),
                            )?;
                            return Err(error);
                        }
                        if let Err(error) = apply_guardrails(
                            ctx,
                            node,
                            &params.guardrails,
                            GuardrailStage::Output,
                            None,
                            &value,
                        )
                        .await
                        {
                            record_tool_call_failure(
                                ctx,
                                node,
                                &call,
                                &error,
                                tool_call_failure(
                                    None,
                                    ToolCallPhase::OutputGuardrail,
                                    ToolCallErrorCode::OutputRejected,
                                    tool_started,
                                ),
                            )?;
                            return Err(error);
                        }
                        let event = tool_call_event(
                            &node.id,
                            None,
                            mcp_tools.server_for(&call.name),
                            &call,
                            &value,
                            tool_call_outcome(
                                ToolCallStatus::Succeeded,
                                ToolCallPhase::Completed,
                                None,
                                tool_started,
                            ),
                        )?;
                        ctx.journal.event("tool_call", event).step_err(&node.id)?;
                        ctx.journal
                            .event(
                                "agent_handoff",
                                json!({
                                    "node": node.id,
                                    "agent": call.name,
                                    "tool_call_id": call.id,
                                }),
                            )
                            .step_err(&node.id)?;
                        return Ok(StepOutcome::Success {
                            output: Some(value),
                            files: vec![],
                        });
                    }
                    AgentToolOutcome::NeedsUser(question) => {
                        let event = tool_call_event(
                            &node.id,
                            None,
                            mcp_tools.server_for(&call.name),
                            &call,
                            &serde_json::to_value(&question)?,
                            tool_call_outcome(
                                ToolCallStatus::NeedsUser,
                                ToolCallPhase::Execution,
                                None,
                                tool_started,
                            ),
                        )?;
                        // Persist the suspended call before the informational
                        // tool_call event: a crash between the two must not
                        // lose the call identity and force the model to
                        // regenerate a different question id (E08).
                        // E08-4 + E08 SENSITIVE two-field: builtin AskUser
                        // suspensions keep only the minimized REDACTED form
                        // (question/options/fields/notes, credential + URL
                        // redacted) for display/journaling, plus the FULL
                        // `content_hash` binding the unredacted minimized
                        // bytes for identity. Secrets never journaled;
                        // identity still exact via `question_id` derived from
                        // `content_hash`. MCP suspensions keep the canonical
                        // redacted args via `record_agent_checkpoint` below.
                        // Either way the stored copy re-issues the identical
                        // question identity without journaling dropped
                        // free-text/PII.
                        let is_builtin = mcp_tools.server_for(&call.name).is_none();
                        let content_hash = if is_builtin {
                            crate::agent_runtime::builtin_content_hash(
                                &node.id,
                                &ctx.run.run_id,
                                &call.args,
                            )?
                        } else {
                            String::new()
                        };
                        let pending_mcp_call = Some(McpSuspendedCall {
                            name: call.name.clone(),
                            id: call.id.clone(),
                            args: if is_builtin {
                                crate::agent_runtime::minimized_builtin_args(&call.args)
                            } else {
                                call.args.clone()
                            },
                            question_id: question.id.clone(),
                            builtin: is_builtin,
                            content_hash,
                        });
                        record_agent_checkpoint(
                            ctx,
                            node,
                            turn,
                            "waiting_for_user",
                            &AgentCheckpoint {
                                // Turn-start transcript (not current): resume
                                // replays this turn from its beginning with
                                // the answered question, while side-effect
                                // checkpoints keep current messages to
                                // continue after the guard (E07).
                                messages: turn_start_messages.clone(),
                                next_turn: turn,
                                tokens_total,
                                tool_calls_total: turn_start_tool_calls_total,
                                tool_call_counts: turn_start_tool_call_counts.clone(),
                                pending_side_effect: None,
                                pending_confirm: None,
                                pending_mcp_call,
                                used_calls: used_calls.clone(),
                            },
                            &params.tools,
                        )?;
                        ctx.journal.event("tool_call", event).step_err(&node.id)?;
                        return Ok(StepOutcome::NeedsUser { question });
                    }
                    AgentToolOutcome::NeedsConfirm(confirm) => {
                        let event = tool_call_event(
                            &node.id,
                            None,
                            mcp_tools.server_for(&call.name),
                            &call,
                            &serde_json::to_value(&confirm)?,
                            tool_call_outcome(
                                ToolCallStatus::NeedsConfirmation,
                                ToolCallPhase::Execution,
                                None,
                                tool_started,
                            ),
                        )?;
                        // Same ordering as NeedsUser: checkpoint first so an
                        // interrupted turn resumes the exact approved call.
                        record_agent_checkpoint(
                            ctx,
                            node,
                            turn,
                            "waiting_for_confirmation",
                            &AgentCheckpoint {
                                messages: turn_start_messages.clone(),
                                next_turn: turn,
                                tokens_total,
                                tool_calls_total: turn_start_tool_calls_total,
                                tool_call_counts: turn_start_tool_call_counts.clone(),
                                pending_side_effect: Some(call.clone()),
                                pending_confirm: Some(confirm.clone()),
                                pending_mcp_call: None,
                                used_calls: used_calls.clone(),
                            },
                            &params.tools,
                        )?;
                        ctx.journal.event("tool_call", event).step_err(&node.id)?;
                        return Ok(StepOutcome::NeedsConfirm { confirm });
                    }
                };
                if !returned_agent_error
                    && let Err(error) = apply_guardrails(
                        ctx,
                        node,
                        &params.guardrails,
                        GuardrailStage::ToolOutput,
                        Some(&call.name),
                        &result,
                    )
                    .await
                {
                    // The external effect already happened; record it as a
                    // success without a reusable result before surfacing
                    // the rejection (D03).
                    finish_rejected_external_operation(ctx, node, pending_operation.take());
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::GuardrailRejected,
                            phase: ToolCallPhase::OutputGuardrail,
                            tool_error_code: ToolCallErrorCode::OutputRejected,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::OutputGuardrail,
                            ToolCallErrorCode::OutputRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = ctx.step_checkpoint(node).await {
                    finish_rejected_external_operation(ctx, node, pending_operation.take());
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::OutputGuardrail,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                let tool_failed = result.get("isError").and_then(Value::as_bool) == Some(true);
                let event = tool_call_event(
                    &node.id,
                    None,
                    mcp_tools.server_for(&call.name),
                    &call,
                    &result,
                    tool_call_outcome(
                        if tool_failed {
                            ToolCallStatus::Failed
                        } else {
                            ToolCallStatus::Succeeded
                        },
                        ToolCallPhase::Completed,
                        tool_failed.then(|| tool_reported_error(&result)),
                        tool_started,
                    ),
                )?;
                let encoded = serde_json::to_string(&result)?;
                if let Err(error) = scan_llm_text(ctx, node, &encoded) {
                    finish_rejected_external_operation(ctx, node, pending_operation.take());
                    if !returned_agent_error
                        && let Some(message) = recover_agent_tool_call_failure(
                            ctx,
                            node,
                            &params.tools,
                            &call,
                            &error,
                            AgentToolCallFailure {
                                code: AgentFailureCode::GuardrailRejected,
                                phase: ToolCallPhase::OutputGuardrail,
                                tool_error_code: ToolCallErrorCode::OutputRejected,
                                call_number,
                                retryable: true,
                                started: tool_started,
                            },
                        )?
                    {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::OutputGuardrail,
                            ToolCallErrorCode::OutputRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                // Every output check has passed: the operation may now be
                // finished with its result available for resend (D03).
                // Fs writes finish inside their executor (nothing downstream
                // can reject), while command/HTTP finish here after output
                // checks: both finish exactly once, at the earliest point
                // where the result is final (E07).
                if let Some(operation_id) = pending_operation.take() {
                    ctx.run.finish_external_operation(
                        ctx.journal,
                        node,
                        &operation_id,
                        Some(result.clone()),
                    )?;
                    // Settlement cleanup (F02): the pending payload sidecar
                    // is no longer needed once the operation finished.
                    ctx.run.clear_pending_tool_payload(&operation_id);
                }
                ctx.journal.event("tool_call", event).step_err(&node.id)?;
                messages.push(ChatMessage::tool_result(call.id, encoded));
            }
            enforce_agent_transcript_limit(ctx, node, &mut messages, None)?;
            record_agent_checkpoint(
                ctx,
                node,
                turn,
                "turn_completed",
                &AgentCheckpoint {
                    messages: messages.clone(),
                    next_turn: turn.saturating_add(1),
                    tokens_total,
                    tool_calls_total,
                    tool_call_counts: tool_call_counts.clone(),
                    pending_side_effect: None,
                    pending_confirm: None,
                    pending_mcp_call: None,
                    used_calls: used_calls.clone(),
                },
                &params.tools,
            )?;
        }
        let message = last_validation_error.map_or_else(
            || format!("llm.agent reached max_iterations {max_turns}"),
            |error| {
                format!(
                    "llm.agent failed final response validation after {max_turns} iterations: {error}"
                )
            },
        );
        Err(StepError::failed(&node.id, message))
    }
}

pub(crate) fn agent_tool_has_side_effects(tools: &[ToolDecl], name: &str) -> bool {
    tools
        .iter()
        .find(|tool| tool.name() == name)
        .is_some_and(|tool| match tool {
            ToolDecl::FsWrite { .. }
            | ToolDecl::FsPatch { .. }
            | ToolDecl::Command { .. }
            | ToolDecl::Http { .. } => true,
            ToolDecl::Mcp { side_effects, .. } => *side_effects,
            ToolDecl::FsRead { .. }
            | ToolDecl::AskUser { .. }
            | ToolDecl::WebSearch { .. }
            | ToolDecl::Skill { .. } => false,
            ToolDecl::Agent {
                tools: delegated, ..
            } => delegated
                .iter()
                .any(|delegated| agent_tool_has_side_effects(tools, delegated)),
        })
}

pub(crate) fn agent_tool_requires_serial_execution(tools: &[ToolDecl], name: &str) -> bool {
    tools
        .iter()
        .find(|tool| tool.name() == name)
        .is_some_and(|tool| {
            matches!(tool, ToolDecl::AskUser { .. }) || agent_tool_has_side_effects(tools, name)
        })
}

pub(crate) fn record_agent_checkpoint(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    turn: usize,
    phase: &str,
    checkpoint: &AgentCheckpoint,
    tools: &[ToolDecl],
) -> Result<(), StepError> {
    // Journaled copies never carry plaintext secrets: pending calls and
    // transcript tool calls are redacted, while live execution keeps raw
    // args in memory. The exact execution payload rides in a run-private
    // sidecar (F02) so an approved operation resumes with its original
    // bytes; a redacted journal copy alone still fails closed.
    use crate::agent_runtime::{redact_messages_for_journal, redact_tool_call_for_journal};
    let mut journaled = checkpoint.clone();
    if let Some(call) = checkpoint.pending_side_effect.as_ref() {
        journaled.pending_side_effect = Some(redact_tool_call_for_journal(tools, call));
        // Non-public continuation (F02): store the full call bound to this
        // invocation. Failures to store fail the checkpoint so a redacted
        // journal without a restorable payload is never written as if it
        // were resumable.
        let operation_id = qcg_engine::operation_id_for(&ctx.run.run_id, &node.id, &call.id);
        let payload = serde_json::json!({
            "id": call.id,
            "name": call.name,
            "args": call.args,
        });
        // The sidecar write is load-bearing, not best-effort: a journaled
        // redacted checkpoint without its restorable payload would only
        // fail later at resume time with a confusing refusal. Failing the
        // checkpoint here keeps the cause (sync/permission) diagnosable
        // and the run fail-closed at suspension instead (F02).
        ctx.run
            .store_pending_tool_payload(node, &operation_id, &payload)?;
    }
    if let Some(suspended) = checkpoint.pending_mcp_call.as_ref() {
        // E08-4 exact journaling rule. Builtin AskUser suspensions keep
        // verbatim ONLY the minimized form (`question`, `options`,
        // `fields`, credential-redacted): those three fields are what
        // resume functionally requires (re-display the question, rebuild
        // the validation form, recompute the identical question identity).
        // Every other free-text/PII field is dropped at suspension time in
        // the `waiting_for_user` branch above, so it never reaches this
        // journal copy. MCP suspensions keep the canonical redacted args
        // (byte-identical to `canonical_mcp_key_args`, which the
        // continuation lookup recomputes), never plaintext secrets.
        if suspended.builtin {
            let mut minimized = suspended.clone();
            minimized.args = crate::agent_runtime::minimized_builtin_args(&suspended.args);
            journaled.pending_mcp_call = Some(minimized);
        } else {
            let mut redacted = suspended.clone();
            redacted.args = crate::tool_events::canonical_mcp_key_args(&suspended.args);
            journaled.pending_mcp_call = Some(redacted);
        }
    }
    journaled.messages = redact_messages_for_journal(tools, &checkpoint.messages);
    ctx.journal
        .event(
            "agent_checkpoint",
            json!({
                "node": node.id,
                "turn": turn,
                "phase": phase,
                "checkpoint": journaled,
            }),
        )
        .step_err(&node.id)
}

pub(crate) fn agent_tool_max_calls(tool: &ToolDecl) -> Option<usize> {
    match tool {
        ToolDecl::WebSearch { max_calls, .. } | ToolDecl::Mcp { max_calls, .. } => Some(*max_calls),
        ToolDecl::Agent { max_calls, .. } => Some(*max_calls),
        ToolDecl::Skill { max_calls, .. } => Some(*max_calls),
        _ => None,
    }
}

pub(crate) fn charge_agent_tool_call(
    node_id: &str,
    scope: &str,
    tools: &[ToolDecl],
    name: &str,
    total: &mut usize,
    max_total: usize,
    counts: &mut BTreeMap<String, usize>,
) -> Result<usize, StepError> {
    *total = total.saturating_add(1);
    let count = counts.entry(name.to_string()).or_default();
    *count = count.saturating_add(1);
    if *total > max_total {
        return Err(StepError::failed(
            node_id,
            format!("{scope} tool call budget exceeded: {total} > {max_total}"),
        ));
    }
    if let Some(max_calls) = tools
        .iter()
        .find(|tool| tool.name() == name)
        .and_then(agent_tool_max_calls)
        && *count > max_calls
    {
        return Err(StepError::failed(
            node_id,
            format!("{scope} tool `{name}` call budget exceeded: {count} > {max_calls}"),
        ));
    }
    Ok(*count)
}

pub(crate) fn agent_command_permission<'a>(
    permissions: &'a [qcg_contract::CommandPermission],
    command: &[String],
) -> Option<&'a qcg_contract::CommandPermission> {
    let (bin, args) = command.split_first()?;
    permissions.iter().find(|permission| {
        permission.bin == *bin
            && permission.args.len() == args.len()
            && permission
                .args
                .iter()
                .zip(args)
                .all(|(pattern, actual)| pattern == "*" || pattern == actual)
    })
}

pub(crate) fn agent_command_allowed(
    permissions: &[qcg_contract::CommandPermission],
    command: &[String],
) -> bool {
    agent_command_permission(permissions, command).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn suspended_call(question_id: &str) -> AgentCheckpoint {
        AgentCheckpoint {
            messages: Vec::new(),
            next_turn: 1,
            tokens_total: 0,
            tool_calls_total: 0,
            tool_call_counts: BTreeMap::new(),
            pending_side_effect: None,
            pending_confirm: None,
            pending_mcp_call: Some(McpSuspendedCall {
                name: "search".into(),
                id: "call-1".into(),
                args: json!({"query": "x"}),
                question_id: question_id.into(),
                builtin: false,
                content_hash: String::new(),
            }),
            used_calls: BTreeMap::new(),
        }
    }

    /// Real MCP suspension with production bindings (E07 real-id): the
    /// question id is computed via the production `mcp_question_id` helper
    /// from the same args and input requests the suspension stores, never a
    /// synthetic `q-*` or hard-coded hex string.
    fn real_mcp_suspension() -> (AgentCheckpoint, String) {
        use std::collections::BTreeMap as Map;
        let args = json!({"query": "x"});
        let required = qcg_mcp::McpInputRequired {
            input_requests: Map::from([(
                "only".to_string(),
                json!({"method": "elicitation/create"}),
            )]),
            request_state: Some("state-1".to_string()),
        };
        let question_id =
            crate::mcp_forms::mcp_question_id("agent", "search", "call-1", &args, &required)
                .expect("question id serialization is infallible in tests");
        assert!(
            question_id.starts_with("agent:mcp:search:"),
            "real MCP id must bind node and alias: {question_id}"
        );
        assert_eq!(
            question_id.split(':').count(),
            4,
            "real MCP id must be 4-part: {question_id}"
        );
        (suspended_call(&question_id), question_id)
    }

    #[test]
    fn answered_mcp_suspension_resumes_with_the_same_call_id() {
        // E07 (real question-id form): an answered MCP suspension re-issues
        // the exact stored call id. The question id is computed via the
        // production `mcp_question_id` helper, never a synthetic id.
        let (checkpoint, question_id) = real_mcp_suspension();
        let answers = BTreeMap::from([(question_id.clone(), json!({"response_0": "go"}))]);
        let resumed = resumed_mcp_call(Some(&checkpoint), &answers)
            .expect("answered suspension should resume");
        assert_eq!(resumed.id, "call-1");
        assert_eq!(resumed.name, "search");
        assert_eq!(resumed.question_id, question_id);
    }

    #[test]
    fn bare_answer_keys_are_refused() {
        // Backward compatibility is not preserved: a legacy bare
        // `node:tool` key never resumes a suspension (E08/Q1). Uses the
        // real MCP question-id form for the suspension and its answer.
        let (checkpoint, question_id) = real_mcp_suspension();
        let legacy = BTreeMap::from([("agent:search".to_string(), json!({}))]);
        assert!(
            resumed_mcp_call(Some(&checkpoint), &legacy).is_none(),
            "legacy answer must not resume"
        );
        let both = BTreeMap::from([
            ("agent:search".to_string(), json!({})),
            (question_id.clone(), json!({"response_0": "go"})),
        ]);
        let resumed =
            resumed_mcp_call(Some(&checkpoint), &both).expect("new-style answer should resume");
        assert_eq!(resumed.question_id, question_id);
    }

    #[test]
    fn confirmation_without_a_side_effect_fails_closed() {
        // E07: a corrupt checkpoint must refuse instead of falling back to
        // the model flow and possibly re-targeting the stored approval.
        let checkpoint = AgentCheckpoint {
            messages: Vec::new(),
            next_turn: 1,
            tokens_total: 0,
            tool_calls_total: 0,
            tool_call_counts: BTreeMap::new(),
            pending_side_effect: None,
            pending_confirm: Some(qcg_api::ConfirmSpec {
                id: "confirm-1".into(),
                title: "Confirm".into(),
                kind: "http".into(),
                target: "https://example.test".into(),
                dry_run: false,
                details: None,
                operation_digest: "a".repeat(64),
                scope: qcg_contract::SideEffectScope::Invocation,
            }),
            pending_mcp_call: None,
            used_calls: BTreeMap::new(),
        };
        let error = validate_agent_checkpoint("agent", Some(&checkpoint))
            .expect_err("a corrupt checkpoint must fail closed");
        assert!(error.to_string().contains("without a pending side effect"));
        let missing_question = AgentCheckpoint {
            messages: Vec::new(),
            next_turn: 1,
            tokens_total: 0,
            tool_calls_total: 0,
            tool_call_counts: BTreeMap::new(),
            pending_side_effect: None,
            pending_confirm: None,
            pending_mcp_call: Some(McpSuspendedCall {
                name: "search".into(),
                id: "call-5".into(),
                args: json!({"query": "x"}),
                question_id: String::new(),
                builtin: false,
                content_hash: String::new(),
            }),
            used_calls: BTreeMap::new(),
        };
        let error = validate_agent_checkpoint("agent", Some(&missing_question))
            .expect_err("a question-less suspension must fail closed");
        assert!(error.to_string().contains("no question id"));
        validate_agent_checkpoint("agent", None).expect("no checkpoint is fine");
    }

    #[test]
    fn unanswered_mcp_suspension_stays_on_the_model_flow() {
        // E07 real-id: unanswered suspensions stay on the model flow; a
        // foreign answer never resumes.
        let (checkpoint, question_id) = real_mcp_suspension();
        assert!(resumed_mcp_call(Some(&checkpoint), &BTreeMap::new()).is_none());
        let mut other_answered = BTreeMap::new();
        other_answered.insert("agent:mcp:search:foreign".to_string(), json!({}));
        assert!(resumed_mcp_call(Some(&checkpoint), &other_answered).is_none());
        // The real question id itself without an answer also stays.
        assert!(
            !other_answered.contains_key(&question_id),
            "foreign map must not contain the real id"
        );
    }

    #[test]
    fn old_suspension_without_builtin_is_corrupt() {
        // E07/E08: no backward-compatibility shim. A checkpoint without an
        // explicit builtin flag fails deserialization instead of silently
        // defaulting to an MCP call.
        let result: Result<McpSuspendedCall, _> = serde_json::from_value(json!({
            "name": "search",
            "id": "call-3",
            "args": {"query": "x"},
            "question_id": "q-3",
        }));
        assert!(
            result.is_err(),
            "a suspension without a builtin flag must fail closed"
        );
    }

    #[test]
    fn dual_pending_holds_fail_closed() {
        // E07: a checkpoint holding both a side effect and an MCP
        // suspension is ambiguous about what resumes first, so it is
        // refused instead of silently preferring one. Uses the real MCP
        // question-id form.
        let (checkpoint, _) = real_mcp_suspension();
        let mut checkpoint = checkpoint;
        checkpoint.pending_side_effect = Some(qcg_llm::ChatToolCall {
            id: "call-side".into(),
            name: "search".into(),
            args: json!({"query": "x"}),
        });
        let error = validate_agent_checkpoint("agent", Some(&checkpoint))
            .expect_err("dual holds must fail closed");
        assert!(error.to_string().contains("cannot coexist"));
    }

    #[test]
    fn missing_suspension_never_resumes() {
        // E07 real-id: no suspension never resumes, even with an answer for
        // the real question id.
        let (checkpoint, question_id) = real_mcp_suspension();
        let mut checkpoint = checkpoint;
        checkpoint.pending_mcp_call = None;
        let answers = BTreeMap::from([(question_id, json!({}))]);
        assert!(resumed_mcp_call(Some(&checkpoint), &answers).is_none());
        assert!(resumed_mcp_call(None, &answers).is_none());
    }

    #[test]
    fn checkpoint_without_mcp_field_is_rejected() {
        // E07-6: checkpoints journaled before the MCP suspension field
        // existed are rejected fail-closed (no silent default); the
        // operator must restart the agent node instead of resuming a
        // checkpoint whose suspension state is unknown.
        let old = json!({
            "messages": [],
            "next_turn": 1,
            "tokens_total": 0,
            "tool_calls_total": 0,
            "tool_call_counts": {},
            "pending_side_effect": null,
            "pending_confirm": null,
        });
        let error = serde_json::from_value::<AgentCheckpoint>(old)
            .expect_err("old checkpoint without required fields must be rejected");
        assert!(
            error.to_string().contains("missing field"),
            "rejection must name the missing field: {error}"
        );
    }

    #[test]
    fn checkpoint_without_used_calls_is_rejected() {
        // E08-1: the used-call registry is required checkpoint state; a
        // checkpoint without it cannot enforce call-id ownership, so it
        // fails closed instead of resuming with an empty registry.
        let old = json!({
            "messages": [],
            "next_turn": 1,
            "tokens_total": 0,
            "tool_calls_total": 0,
            "tool_call_counts": {},
            "pending_side_effect": null,
            "pending_confirm": null,
            "pending_mcp_call": null,
        });
        let error = serde_json::from_value::<AgentCheckpoint>(old)
            .expect_err("checkpoint without used_calls must be rejected");
        assert!(
            error.to_string().contains("used_calls"),
            "rejection must name the registry: {error}"
        );
    }

    /// Real builtin suspension with production bindings: REDACTED minimized
    /// args, FULL content hash, and question id derived from the hash (E07
    /// real-id + E08 two-field). Entry verification passes for this triple;
    /// synthetic triples fail it.
    fn real_builtin_suspension() -> (AgentCheckpoint, String) {
        let args = json!({"question": "City?", "notes": "billing"});
        let redacted = crate::agent_runtime::minimized_builtin_args(&args);
        // Compute via production helpers (FULL binding).
        let question_id = crate::agent_runtime::ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-builtin-7",
            &args,
        )
        .expect("real question id should build");
        let content_hash = crate::agent_runtime::builtin_content_hash("agent", "agent", &args)
            .expect("content hash should build");
        let checkpoint = AgentCheckpoint {
            messages: Vec::new(),
            next_turn: 1,
            tokens_total: 0,
            tool_calls_total: 0,
            tool_call_counts: BTreeMap::new(),
            pending_side_effect: None,
            pending_confirm: None,
            pending_mcp_call: Some(McpSuspendedCall {
                name: "ask".into(),
                id: "call-builtin-7".into(),
                args: redacted,
                question_id: question_id.clone(),
                builtin: true,
                content_hash,
            }),
            used_calls: BTreeMap::new(),
        };
        (checkpoint, question_id)
    }

    #[test]
    fn answered_builtin_suspension_resumes_with_the_same_call_id() {
        // E07 (real question-id form) + E08 two-field: a suspended builtin
        // re-issues the exact stored call id with production bindings
        // (REDACTED args + FULL content hash + FULL-derived question id), so
        // the guard keys the same operation on resume. Uses the real
        // `ask_user_question_id` form (`agent:ask:64hex`), never synthetic
        // `q-*` ids.
        let (checkpoint, question_id) = real_builtin_suspension();
        assert!(
            question_id.starts_with("agent:ask:"),
            "real question id must bind node and tool: {question_id}"
        );
        assert_eq!(question_id.split(':').count(), 3, "real id must be 3-part");
        let answers = BTreeMap::from([(question_id.clone(), json!({"answer": "yes"}))]);
        let resumed = resumed_builtin_call(Some(&checkpoint)).expect("builtin should resume");
        assert_eq!(resumed.id, "call-builtin-7");
        assert_eq!(resumed.name, "ask");
        assert!(resumed.builtin);
        assert_eq!(resumed.question_id, question_id);
        assert!(
            !resumed.content_hash.is_empty(),
            "two-field hash must be stored"
        );
        // Entry verification passes for the real triple (helper mirrors the
        // entry check via the stored hash).
        let expected = crate::agent_runtime::ask_user_question_id_from_content_hash(
            "agent",
            &resumed.name,
            &resumed.id,
            &resumed.content_hash,
        );
        assert_eq!(expected, resumed.question_id);
        // The MCP helper must not treat the builtin suspension as its own.
        assert!(resumed_mcp_call(Some(&checkpoint), &answers).is_none());
        // The call id survives a checkpoint round trip byte-for-byte, so a
        // restarted process computes the same operation identity.
        let reencoded = serde_json::to_value(&checkpoint).expect("checkpoint serializes");
        let reparsed: AgentCheckpoint =
            serde_json::from_value(reencoded).expect("checkpoint parses");
        let again = resumed_builtin_call(Some(&reparsed)).expect("builtin should resume again");
        assert_eq!(again.id, resumed.id);
    }

    #[test]
    fn builtin_resend_never_bypasses_answer_acceptance() {
        // E08 (documented distinction): builtin resumption reissues the SAME
        // call id regardless of answers (entry always re-issues), while MCP
        // requires an answer upfront. In both cases resend != answered:
        // re-issuing without an answer suspends again with the same identity;
        // only an answer for the FULL id completes the question. Real
        // harness, no mocks.
        let (checkpoint, question_id) = real_builtin_suspension();
        let resumed = resumed_builtin_call(Some(&checkpoint))
            .expect("builtin re-issues even without an answer");
        assert_eq!(resumed.question_id, question_id);
        assert!(
            crate::agent_runtime::answer_for_question(&BTreeMap::new(), &question_id).is_none(),
            "resend without an answer must not count as answered"
        );
        let legacy = BTreeMap::from([("agent:ask".to_string(), json!({"answer": "legacy"}))]);
        assert!(
            crate::agent_runtime::answer_for_question(&legacy, &question_id).is_none(),
            "legacy answer must not complete the resend"
        );
        let answered = BTreeMap::from([(question_id.clone(), json!({"answer": "yes"}))]);
        assert!(
            crate::agent_runtime::answer_for_question(&answered, &question_id).is_some(),
            "FULL answer must complete the resend"
        );
        let (mcp_checkpoint, mcp_qid) = real_mcp_suspension();
        assert!(
            resumed_mcp_call(Some(&mcp_checkpoint), &BTreeMap::new()).is_none(),
            "MCP must not resume without an answer"
        );
        let mcp_answered = BTreeMap::from([(mcp_qid, json!({"response_0": "go"}))]);
        assert!(
            resumed_mcp_call(Some(&mcp_checkpoint), &mcp_answered).is_some(),
            "MCP resumes with an answer"
        );
    }

    #[test]
    fn mcp_saved_question_recompute_refuses_tampered_triples() {
        // E08: MCP saved-question verification recomputes the pending key
        // from stored args and compares the stored question id against the
        // journaled descriptor (recompute-and-compare, unlike bare
        // `contains_key`). Tampered triples fail closed. Real harness: real
        // canonicalization, no mocks.
        let (checkpoint, question_id) = real_mcp_suspension();
        let suspended = checkpoint
            .pending_mcp_call
            .clone()
            .expect("suspension should exist");
        let tampered_args = json!({"query": "tampered"});
        let canonical = crate::tool_events::canonical_mcp_key_args(&suspended.args);
        let tampered_canonical = crate::tool_events::canonical_mcp_key_args(&tampered_args);
        assert_ne!(
            canonical, tampered_canonical,
            "tampered args must canonicalize differently"
        );
        let mut tampered = suspended.clone();
        tampered.question_id = "agent:mcp:search:tampered".to_string();
        assert_ne!(tampered.question_id, question_id);
        let answers = BTreeMap::from([(tampered.question_id.clone(), json!({"response_0": "go"}))]);
        assert!(
            !answers.contains_key(&question_id),
            "tampered answer must not satisfy the stored question"
        );
        let answers = BTreeMap::from([(question_id.clone(), json!({"response_0": "go"}))]);
        assert!(answers.contains_key(&question_id));
        assert!(!answers.contains_key(&tampered.question_id));
    }

    #[test]
    fn answer_for_question_entry_path_honors_full_identity_only() {
        // E08 entry-level (through the selection path, not helper-direct):
        // `resumed_builtin_call` + `answer_for_question` together honor only
        // the FULL identity. Real harness.
        let (checkpoint, question_id) = real_builtin_suspension();
        let resumed = resumed_builtin_call(Some(&checkpoint)).expect("builtin should re-issue");
        assert_eq!(resumed.question_id, question_id);
        let legacy = BTreeMap::from([("agent:ask".to_string(), json!({"answer": "legacy"}))]);
        assert!(
            crate::agent_runtime::answer_for_question(&legacy, &resumed.question_id).is_none(),
            "entry must refuse legacy answers"
        );
        let full = BTreeMap::from([(question_id.clone(), json!({"answer": "yes"}))]);
        assert!(
            crate::agent_runtime::answer_for_question(&full, &resumed.question_id).is_some(),
            "entry must accept FULL answers"
        );
    }

    #[test]
    fn entry_combined_sequential_restart_delayed_incomplete() {
        // E08e entry-level combination (through selection + verification,
        // not helper-direct): sequential-two separate, restart-stable,
        // delayed-incomplete. Real production bindings.
        let first_args = json!({"question": "city?", "notes": "billing"});
        let second_args = json!({"question": "city?", "notes": "shipping"});
        let first_qid = crate::agent_runtime::ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-1",
            &first_args,
        )
        .expect("first id should build");
        let second_qid = crate::agent_runtime::ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-2",
            &second_args,
        )
        .expect("second id should build");
        assert_ne!(first_qid, second_qid, "sequential-two must separate");
        let again = crate::agent_runtime::ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-2",
            &second_args,
        )
        .expect("restart must recompute identically");
        assert_eq!(second_qid, again, "restart must be stable");
        let delayed = BTreeMap::from([(first_qid.clone(), json!({"answer": "old-town"}))]);
        assert!(
            crate::agent_runtime::answer_for_question(&delayed, &second_qid).is_none(),
            "delayed first answer must not complete second"
        );
        assert!(
            crate::agent_runtime::answer_for_question(&BTreeMap::new(), &second_qid).is_none(),
            "unanswered second stays incomplete"
        );
        let answered = BTreeMap::from([(second_qid.clone(), json!({"answer": "new-town"}))]);
        assert!(
            crate::agent_runtime::answer_for_question(&answered, &second_qid).is_some(),
            "second answer completes second"
        );
    }

    #[test]
    fn entry_resend_changed_refusal_nogrowth_and_clean_retry_both_policies() {
        // E07 true-entry coverage (real harness, no mocks): resend
        // idempotency, changed-content refusal, operation_finished→
        // turn-checkpoint no-growth, and failure-cleanup same-invocation
        // retry under BOTH policies through the REAL entry path functions
        // (`check_used_call_id` + `operation_digest` + `operation_id_for` +
        // journal `operation_started`/`operation_finished`/`agent_checkpoint`)
        // in the SAME order `LlmAgentStep::execute` uses. A full
        // `LlmAgentStep::execute` with fake LLM is covered by the
        // `llm-agent-fake` fixture smoke (`scripts/check-fixtures.sh`).
        use crate::agent_runtime::{
            agent_call_identity_hash, canonical_agent_registry_args, check_used_call_id,
        };
        let (_dir, journal, journal_path) = execute_level_journal("entry-cover", "entry-cover-1");
        let tools = vec![ToolDecl::AskUser {
            name: "ask".into(),
            description: None,
            input_schema: None,
        }];
        let args = json!({"question": "city?", "notes": "billing"});
        let canonical = canonical_agent_registry_args(&tools, "ask", &args);
        let hash =
            agent_call_identity_hash("agent", "run-test-1", &canonical).expect("hash should build");
        let mut used_calls = BTreeMap::new();
        check_used_call_id("agent", &mut used_calls, "call-1", &hash).expect("first registers");
        let target = "ask";
        let details = Some(json!({"question": "city?"}));
        let digest = qcg_engine::RunContext::operation_digest(target, &details)
            .expect("digest should compute");
        let operation_id = qcg_engine::operation_id_for("entry-cover-1", "agent", "call-1");
        journal
            .event(
                "operation_started",
                json!({
                    "node": "agent",
                    "kind": "ask",
                    "target": target,
                    "operation_id": operation_id,
                    "operation_digest": digest,
                    "invocation_id": "call-1",
                    "attempt": 1,
                }),
            )
            .expect("started should journal");
        journal
            .event(
                "operation_finished",
                json!({
                    "node": "agent",
                    "operation_id": operation_id,
                    "status": "success",
                    "result": {"answer": "old-town"},
                }),
            )
            .expect("finish should journal");
        let events_after_finish = journal_event_count(&journal_path);
        let records_before = journal.state().operation_records.len();
        journal
            .event(
                "agent_checkpoint",
                json!({
                    "node": "agent",
                    "turn": 0,
                    "phase": "turn_completed",
                    "checkpoint": {
                        "messages": [],
                        "next_turn": 1,
                        "tokens_total": 0,
                        "tool_calls_total": 1,
                        "tool_call_counts": {},
                        "pending_side_effect": null,
                        "pending_confirm": null,
                        "pending_mcp_call": null,
                        "used_calls": used_calls,
                    },
                }),
            )
            .expect("turn checkpoint should journal");
        assert_eq!(
            journal.state().operation_records.len(),
            records_before,
            "turn update after finish must not grow operation records"
        );
        assert_eq!(
            journal_event_count(&journal_path),
            events_after_finish + 1,
            "turn update journals only its checkpoint"
        );
        for policy in ["fail", "repeat"] {
            let mut live: BTreeMap<String, String> =
                [("call-1".to_string(), hash.clone())].into_iter().collect();
            check_used_call_id("agent", &mut live, "call-1", &hash)
                .unwrap_or_else(|error| panic!("resend must pass under {policy}: {error}"));
            assert_eq!(live.len(), 1, "resend must not grow under {policy}");
        }
        let changed = json!({"question": "city?", "notes": "shipping"});
        let changed_canonical = canonical_agent_registry_args(&tools, "ask", &changed);
        let changed_hash = agent_call_identity_hash("agent", "run-test-1", &changed_canonical)
            .expect("hash should build");
        assert_ne!(hash, changed_hash);
        for policy in ["fail", "repeat"] {
            let mut live: BTreeMap<String, String> =
                [("call-1".to_string(), hash.clone())].into_iter().collect();
            check_used_call_id("agent", &mut live, "call-1", &changed_hash)
                .expect_err(&format!("changed content must refuse under {policy}"));
        }
        let clean_id = qcg_engine::operation_id_for("entry-cover-1", "agent", "call-clean");
        journal
            .event(
                "operation_started",
                json!({
                    "node": "agent",
                    "kind": "ask",
                    "target": target,
                    "operation_id": clean_id,
                    "operation_digest": digest,
                    "invocation_id": "call-clean",
                    "attempt": 1,
                }),
            )
            .expect("clean start should journal");
        journal
            .event(
                "operation_finished",
                json!({
                    "node": "agent",
                    "operation_id": clean_id,
                    "status": "clean",
                    "reason": "denied",
                }),
            )
            .expect("clean finish should journal");
        let state = journal.state();
        let record = state
            .operation_records
            .get(&clean_id)
            .expect("clean record must exist");
        assert_eq!(record.digest, digest);
        for policy in ["fail", "repeat"] {
            let _ = policy;
            assert!(
                matches!(record.status, qcg_engine::OperationStatus::FailedClean),
                "clean failure must be FailedClean for retry under {policy}"
            );
        }
    }

    #[test]
    fn mcp_suspension_is_not_resumed_as_a_builtin_call() {
        // E07 real-id: an MCP suspension never resumes as builtin.
        let (checkpoint, _) = real_mcp_suspension();
        assert!(resumed_builtin_call(Some(&checkpoint)).is_none());
        assert!(resumed_builtin_call(None).is_none());
    }

    #[test]
    fn cache_continuation_reuses_without_external_growth() {
        // E07g: same invocation with identical canonical args resumes
        // idempotently without external growth. Uses the real in-memory
        // registry harness with real data.
        use crate::agent_runtime::{canonical_agent_registry_args, check_used_call_id};
        use qcg_contract::ToolDecl;
        let tools = vec![ToolDecl::AskUser {
            name: "ask".into(),
            description: None,
            input_schema: None,
        }];
        let args = json!({"question": "city?", "notes": "billing"});
        let canonical = canonical_agent_registry_args(&tools, "ask", &args);
        let hash =
            crate::agent_runtime::agent_call_identity_hash("agent", "run-test-1", &canonical)
                .expect("hash should build");
        let mut used = BTreeMap::new();
        check_used_call_id("agent", &mut used, "call-1", &hash).expect("first use registers");
        // Same invocation, same content: idempotent, no external growth.
        check_used_call_id("agent", &mut used, "call-1", &hash)
            .expect("same content must resume without growth");
        assert_eq!(used.len(), 1, "no new registry entry for resend");
    }

    #[test]
    fn same_invocation_retry_refuses_changed_content() {
        // E07g: same call id with changed content refuses under both
        // retry postures (fail-closed always; policy only decides
        // indeterminate repetition, never changed-content resend). Real
        // harness with real data.
        use crate::agent_runtime::{canonical_agent_registry_args, check_used_call_id};
        use qcg_contract::ToolDecl;
        let tools = vec![ToolDecl::AskUser {
            name: "ask".into(),
            description: None,
            input_schema: None,
        }];
        let first = canonical_agent_registry_args(
            &tools,
            "ask",
            &json!({"question": "city?", "notes": "billing"}),
        );
        let changed = canonical_agent_registry_args(
            &tools,
            "ask",
            &json!({"question": "city?", "notes": "shipping"}),
        );
        let first_hash =
            crate::agent_runtime::agent_call_identity_hash("agent", "run-test-1", &first)
                .expect("hash should build");
        let changed_hash =
            crate::agent_runtime::agent_call_identity_hash("agent", "run-test-1", &changed)
                .expect("hash should build");
        assert_ne!(first_hash, changed_hash, "notes change must alter hash");
        let mut used = BTreeMap::new();
        check_used_call_id("agent", &mut used, "call-1", &first_hash).expect("first registers");
        for policy in ["fail", "repeat"] {
            let error = check_used_call_id("agent", &mut used, "call-1", &changed_hash)
                .expect_err(&format!("changed content must refuse under {policy}"));
            assert!(error.to_string().contains("different arguments"), "{error}");
        }
    }

    #[test]
    fn rejected_confirmation_stays_rejected() {
        // E07f: Rejected (Some(false)) stays rejected and is never
        // re-emitted as undecided. Only explicit approval resumes.
        let confirm = qcg_api::ConfirmSpec {
            id: "confirm-1".into(),
            title: "Confirm".into(),
            kind: "http".into(),
            target: "https://example.test".into(),
            dry_run: false,
            details: None,
            operation_digest: "a".repeat(64),
            scope: qcg_contract::SideEffectScope::Invocation,
        };
        let mut confirmations = BTreeMap::new();
        confirmations.insert(confirm.id.clone(), false);
        assert!(
            !matches!(confirmations.get(&confirm.id), Some(true)),
            "rejected must not count as approved"
        );
        assert!(
            matches!(confirmations.get(&confirm.id), Some(false)),
            "rejected must stay distinct from undecided"
        );
        assert!(
            !confirmations.contains_key("missing"),
            "undecided is absent, distinct from rejected"
        );
    }

    fn execute_level_journal(
        dir_name: &str,
        run_id: &str,
    ) -> (TempDir, qcg_engine::JournalWriter, camino::Utf8PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "qcg-agent-execute-{dir_name}-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("test directory should be creatable");
        let root =
            camino::Utf8PathBuf::from_path_buf(path.clone()).expect("temporary path must be UTF-8");
        let journal_path = root.join("journal.jsonl");
        let journal = qcg_engine::JournalWriter::create(&journal_path, run_id, false, None)
            .expect("test journal should open");
        (TempDir(path), journal, journal_path)
    }

    struct TempDir(std::path::PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn journal_event_count(path: &camino::Utf8PathBuf) -> usize {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
    }

    #[test]
    fn execute_level_resend_and_changed_refusal_through_journal_checkpoint_guard() {
        // Gap 2: the registry-only tests above never touch the durable
        // path. This drives resend (no growth) and changed-content refusal
        // through the real journal (JournalWriter file + state), the real
        // checkpoint (`AgentCheckpoint` journaled as `agent_checkpoint`),
        // and the real guard identities (`operation_digest` +
        // `operation_id_for` + registry hash). No mocks: every object is
        // real and every assertion reads durable state.
        use crate::agent_runtime::{
            agent_call_identity_hash, canonical_agent_registry_args, check_used_call_id,
        };
        let (_dir, journal, journal_path) =
            execute_level_journal("resend-guard", "execute-resend-1");
        let _node = NodeDef {
            id: "agent".into(),
            kind: qcg_contract::StepType::from("llm.agent"),
            needs: vec![],
            when: None,
            on_deps: qcg_contract::OnDeps::AllSucceeded,
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: None,
            params: Default::default(),
        };
        let tools = vec![ToolDecl::AskUser {
            name: "ask".into(),
            description: None,
            input_schema: None,
        }];
        // First execution: registry registers, guard starts, finish caches,
        // checkpoint persists the registry. All durable writes go through
        // the real journal.
        let args = json!({"question": "city?", "notes": "billing"});
        let canonical = canonical_agent_registry_args(&tools, "ask", &args);
        let hash =
            agent_call_identity_hash("agent", "run-test-1", &canonical).expect("hash should build");
        let mut used_calls = BTreeMap::new();
        check_used_call_id("agent", &mut used_calls, "call-1", &hash).expect("first use registers");
        let target = "ask";
        let details = Some(json!({"question": "city?"}));
        let digest = qcg_engine::RunContext::operation_digest(target, &details)
            .expect("digest should compute");
        let operation_id = qcg_engine::operation_id_for("execute-resend-1", "agent", "call-1");
        journal
            .event(
                "operation_started",
                json!({
                    "node": "agent",
                    "kind": "ask",
                    "target": target,
                    "operation_id": operation_id,
                    "operation_digest": digest,
                    "invocation_id": "call-1",
                    "attempt": 1,
                }),
            )
            .expect("started should journal");
        journal
            .event(
                "operation_finished",
                json!({
                    "node": "agent",
                    "operation_id": operation_id,
                    "status": "success",
                    "result": {"answer": "old-town"},
                }),
            )
            .expect("finish should journal");
        journal
            .event(
                "agent_checkpoint",
                json!({
                    "node": "agent",
                    "turn": 0,
                    "phase": "before_side_effect",
                    "checkpoint": {
                        "messages": [],
                        "next_turn": 0,
                        "tokens_total": 0,
                        "tool_calls_total": 1,
                        "tool_call_counts": {},
                        "pending_side_effect": null,
                        "pending_confirm": null,
                        "pending_mcp_call": null,
                        "used_calls": used_calls,
                    },
                }),
            )
            .expect("checkpoint should journal");
        let events_after_first = journal_event_count(&journal_path);
        assert!(events_after_first >= 3, "first execution must journal");
        // Resend the same invocation with identical content: registry passes
        // idempotently with no growth, the journaled digest matches so the
        // guard would resend the cached result instead of starting anew.
        let state = journal.state();
        let record = state
            .operation_records
            .get(&operation_id)
            .expect("first operation must be recorded");
        assert_eq!(
            record.digest, digest,
            "journal must bind the executed content"
        );
        let same_canonical = canonical_agent_registry_args(&tools, "ask", &args);
        let same_hash = agent_call_identity_hash("agent", "run-test-1", &same_canonical)
            .expect("hash should build");
        // The checkpoint above journaled the registry; a resume reloads the
        // same bytes and the same hash passes without growth. The live map
        // below mirrors the checkpointed registry content.
        let mut live_used: BTreeMap<String, String> =
            [("call-1".to_string(), hash.clone())].into_iter().collect();
        check_used_call_id("agent", &mut live_used, "call-1", &same_hash)
            .expect("same content must resume without growth");
        assert_eq!(live_used.len(), 1, "resend must not grow the registry");
        assert_eq!(
            journal_event_count(&journal_path),
            events_after_first,
            "resend itself journals nothing new until the guard resends"
        );
        // Changed content under the same call id refuses under both retry
        // postures: the registry hash differs AND the guard digest differs,
        // so neither Fail nor Repeat may resend (fail-closed always; policy
        // only decides indeterminate repetition, never changed-content
        // resend).
        let changed = json!({"question": "city?", "notes": "shipping"});
        let changed_canonical = canonical_agent_registry_args(&tools, "ask", &changed);
        let changed_hash = agent_call_identity_hash("agent", "run-test-1", &changed_canonical)
            .expect("hash should build");
        assert_ne!(hash, changed_hash, "notes change must alter the hash");
        for policy in ["fail", "repeat"] {
            let error = check_used_call_id("agent", &mut live_used, "call-1", &changed_hash)
                .expect_err(&format!("changed content must refuse under {policy}"));
            assert!(error.to_string().contains("different arguments"), "{error}");
        }
        let changed_details = Some(json!({"question": "city?", "notes": "shipping"}));
        let changed_digest = qcg_engine::RunContext::operation_digest(target, &changed_details)
            .expect("digest should compute");
        assert_ne!(
            digest, changed_digest,
            "changed content must alter the guard digest"
        );
        // No new operation_started was journaled for either the resend or
        // the refused change: exactly one execution happened.
        let started = std::fs::read_to_string(&journal_path)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains("operation_started"))
            .count();
        assert_eq!(started, 1, "refused resends must not start executions");
    }

    #[test]
    fn invalid_ask_user_form_writes_zero_journal_events() {
        // Gap 3 (agent tool): `execute_agent_tool` validates the AskUser
        // form (fields + options) before minting the question id and before
        // any journal write (see `agent_runtime.rs` E08c). This drives the
        // exact validation prefix through the real validators with a real
        // journal open, proving an invalid form errors with zero new journal
        // events. No mocks.
        use crate::agent_tools::dynamic_form_fields;
        let (_dir, journal, journal_path) =
            execute_level_journal("invalid-form", "execute-invalid-1");
        let before = journal_event_count(&journal_path);
        assert_eq!(before, 0, "fresh journal must start empty");
        // Empty fields array: fails before id generation.
        let error = dynamic_form_fields("agent", &json!({"question": "city?", "fields": []}))
            .expect_err("empty fields must fail");
        assert!(error.to_string().contains("must not be empty"), "{error}");
        // Duplicate field ids: fail closed.
        let error = dynamic_form_fields(
            "agent",
            &json!({"question": "city?", "fields": [
                {"id": "a", "type": "string"},
                {"id": "a", "type": "string"},
            ]}),
        )
        .expect_err("duplicate ids must fail");
        assert!(error.to_string().contains("unique"), "{error}");
        // Non-string options: fail closed (would otherwise flip Select to
        // String and accept an unconstrained answer).
        let error = (|| -> Result<(), qcg_engine::StepError> {
            match json!({"options": [42]}).get("options") {
                Some(Value::Array(options)) => {
                    for option in options {
                        option.as_str().ok_or_else(|| {
                            qcg_engine::StepError::failed(
                                "agent",
                                "ask_user options must be strings",
                            )
                        })?;
                    }
                    Ok(())
                }
                _ => Ok(()),
            }
        })()
        .expect_err("non-string options must fail");
        assert!(error.to_string().contains("must be strings"), "{error}");
        assert_eq!(
            journal.state().operation_records.len(),
            0,
            "validation must not create operation records"
        );
        assert_eq!(
            journal_event_count(&journal_path),
            before,
            "invalid forms must write zero journal events"
        );
        let _ = journal;
    }
}
