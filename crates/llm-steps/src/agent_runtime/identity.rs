use super::*;

/// Extracts the stable `AskUser` question identity from node, tool, call
/// id, and arguments (E08 SENSITIVE two-field). The identity binds the
/// FULL unredacted minimized form via [`builtin_content_hash`] (call id +
/// content hash of the full canonical bytes), while display/journaling uses
/// the REDACTED minimized form ([`minimized_builtin_args`]). Distinct
/// secrets therefore yield distinct ids even though their displays alias,
/// and secrets never reach the journal. The full digest (never truncated)
/// keeps distinct calls separate, and the same identity recomputed from the
/// same FULL inputs lets a resume reuse only its own answer (E08).
/// Verification on resume uses [`ask_user_question_id_from_content_hash`]
/// with the stored `content_hash` so a checkpointed redacted copy never
/// needs the original secrets to prove its identity.
pub(crate) fn ask_user_question_id(
    node_id: &str,
    salt: &str,
    tool_name: &str,
    call_id: &str,
    args: &Value,
) -> Result<String, StepError> {
    let content_hash = builtin_content_hash(node_id, salt, args)?;
    Ok(ask_user_question_id_from_content_hash(
        node_id,
        tool_name,
        call_id,
        &content_hash,
    ))
}

/// Derives the question identity from an already-computed FULL
/// `content_hash` (E08 two-field). Stored checkpoints verify their
/// `question_id` through this function using the stored hash, never by
/// recomputing from the redacted display args (which would fork the
/// identity across the restart).
pub(crate) fn ask_user_question_id_from_content_hash(
    node_id: &str,
    tool_name: &str,
    call_id: &str,
    content_hash: &str,
) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(call_id.as_bytes());
    hasher.update([0]);
    hasher.update(content_hash.as_bytes());
    format!("{node_id}:{tool_name}:{}", hex::encode(hasher.finalize()))
}

/// Looks up the answer for one agent question (E08). Only the full
/// `node:tool:hex` identity is honored; bare `node:tool` keys are refused
/// fail-closed and never complete a question (E08/Q1).
pub(crate) fn answer_for_question<'a>(
    answers: &'a BTreeMap<String, Value>,
    question_id: &str,
) -> Option<&'a Value> {
    answers.get(question_id)
}

/// Canonical registry args for the used-call registry (E07a/E08b). One
/// canonical REDACTED form everywhere (single form, same fix as the MCP
/// guard): AskUser uses the minimized REDACTED display form, MCP uses the
/// canonical redacted key args, HTTP safe reads use the journal-redacted
/// form (never raw, so a checkpointed redacted copy recomputes identically),
/// and every other tool uses the journal-redacted form. Fresh raw args and
/// checkpointed redacted copies canonicalize to the same bytes when content
/// matches, so a resume re-issuing the stored copy passes while genuinely
/// different args refuse. Only hashes are checkpointed, never args. AskUser
/// identity itself binds the FULL hash separately via `builtin_content_hash`
/// / `ask_user_question_id`; the registry binds the redacted display so
/// redacted resumes match, while distinct secrets still separate via their
/// distinct question ids (E08 two-field).
pub(crate) fn canonical_agent_registry_args(tools: &[ToolDecl], name: &str, args: &Value) -> Value {
    let kind = tools
        .iter()
        .find(|tool| tool.name() == name)
        .map(|tool| tool.kind());
    match kind {
        Some("ask_user") => minimized_builtin_args(args),
        Some("mcp") => crate::tool_events::canonical_mcp_key_args(args),
        // E07 safe-read single-form: bind the CANONICAL REDACTED form, never
        // raw, so a resume re-issuing the checkpointed redacted copy
        // recomputes identically when content matches and refuses when
        // query/headers differ. Execution still uses raw args; only the
        // registry and journal use this form.
        Some("http") | Some("fs.write") | Some("fs.patch") => {
            redact_agent_tool_args_for_journal(tools, name, args)
        }
        _ => engine::redact_credential_values(args),
    }
}

/// Stable per-call identity hash for the used-call registry (E08-1, E09
/// salted). The hash covers the canonical REDACTED registry bytes (see
/// [`canonical_agent_registry_args`]) salted by the run binding
/// (`agent-call-v1` domain + run id, same scheme as the salted policy), so
/// identical content in different runs yields different hashes
/// (cross-run unlinkability) while identical content in the same run
/// resumes idempotently. Only the hash is checkpointed, never the args.
/// The registry is consulted for every call including checkpoint re-issues
/// (E07a): the same call id with the same canonical hash resumes
/// idempotently, while the same call id with a different hash fails closed.
pub(crate) fn agent_call_identity_hash(
    node_id: &str,
    salt: &str,
    args: &Value,
) -> Result<String, StepError> {
    let bytes = serde_json::to_vec(args).map_err(|error| {
        StepError::failed(
            node_id,
            format!("tool arguments are not serializable: {error}"),
        )
    })?;
    Ok(engine::salted_binding_digest("agent-call-v1", salt, &bytes))
}

/// Registry hash bound to the canonical REDACTED form for one tool call
/// (E07a/E08b, salted E09). Fresh and resumed copies hash identically when
/// content matches (single redacted form); any material change (including
/// `notes`, headers, or query) changes the hash and refuses. The salt is
/// the run id for cross-run divergence.
pub(crate) fn agent_call_identity_hash_for_tool(
    node_id: &str,
    salt: &str,
    tools: &[ToolDecl],
    name: &str,
    args: &Value,
) -> Result<String, StepError> {
    let canonical = canonical_agent_registry_args(tools, name, args);
    agent_call_identity_hash(node_id, salt, &canonical)
}
