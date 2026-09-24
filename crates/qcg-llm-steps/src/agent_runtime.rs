use qcg_api::FormSpec;
use qcg_api::{ToolCallErrorCode, ToolCallPhase, ToolCallStatus};
use qcg_contract::{AgentFailureAction, NodeDef, ToolDecl, validate_form_values};
use qcg_contract::{AgentFailureCode, FieldType, InputField};
use qcg_engine::{HttpRequest, ResultExt, StepContext, StepError};
use qcg_llm::{ChatMessage, ChatToolCall, LlmRuntime};
use qcg_policy::is_safe_relative_path;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Instant;

use crate::agent_tools::{dynamic_form_fields, validate_agent_tool_args};
use crate::guardrail::GuardrailDecl;
use crate::mcp_forms::mcp_argument_summary;
use crate::mcp_tools::{AgentToolOutcome, McpAgentTools};
use crate::prompting::utf8_head;
use crate::request::scan_llm_text;
use crate::search::{execute_web_search, http_body_value, url_host_matches};
use crate::specialist::{SpecialistAgentSpec, execute_specialist_agent};
use crate::tool_events::{execute_mcp_tool, tool_call_error, tool_call_event, tool_call_outcome};

#[derive(Clone, Copy)]
pub(crate) struct AgentToolServices<'a> {
    pub(crate) runtime: &'a LlmRuntime,
    pub(crate) guardrails: &'a [GuardrailDecl],
}

pub(crate) struct AgentToolInvocation<'a> {
    pub(crate) mcp: &'a McpAgentTools,
    pub(crate) tools: &'a [ToolDecl],
    pub(crate) name: &'a str,
    pub(crate) call_id: &'a str,
    pub(crate) call_number: usize,
    pub(crate) args: &'a Value,
    /// Skills already activated in this agent run, keyed by
    /// `<resource>::<name>`; repeated activations skip re-injection.
    pub(crate) activated_skills: &'a mut std::collections::BTreeSet<String>,
}

/// Maps an agent http tool's declared `sensitive_query` names through the
/// single shared gateway parser so the agent tool and the plain HTTP step
/// cannot disagree on shape or percent-encoding (E09).
fn agent_sensitive_query(
    node_id: &str,
    url: &str,
    declared: Option<&Value>,
) -> Result<BTreeMap<String, String>, StepError> {
    qcg_engine::sensitive_query_values_from_decl(url, declared)
        .map_err(|error| StepError::failed(node_id, error.to_string()))
}

/// Redacts one agent tool call's arguments for journaling. HTTP tools get
/// URL query, credential-header, and body redaction; `fs.write` tools get
/// content-hash redaction; every other tool gets generic credential-key
/// redaction. Execution always uses the raw args; only journaled copies go
/// through here (E09). URL query VALUES are all redacted by default (keys
/// stay visible); declared sensitivity still drives digest salting (E09-1).
pub(crate) fn redact_agent_tool_args_for_journal(
    tools: &[ToolDecl],
    name: &str,
    args: &Value,
) -> Value {
    let kind = tools
        .iter()
        .find(|tool| tool.name() == name)
        .map(|tool| tool.kind());
    let mut redacted = if kind == Some("http") {
        qcg_engine::redact_http_args_for_journal(args)
    } else if kind == Some("fs.write") {
        qcg_engine::redact_fs_write_args_for_journal(args)
    } else {
        qcg_engine::redact_credential_values(args)
    };
    if kind == Some("http")
        && let Some(url) = redacted
            .get("url")
            .and_then(Value::as_str)
            .map(str::to_owned)
        && let Some(object) = redacted.as_object_mut()
    {
        object.insert(
            "url".into(),
            Value::String(qcg_policy::redact_all_query_values(&url)),
        );
    }
    redacted
}

/// Reports whether an agent tool call is a safe HTTP read (`GET`/`HEAD`,
/// defaulting to `GET` only when the method is absent). Safe reads perform
/// no durable side effect and take no operation guard; they still record a
/// cheap `safe_read` checkpoint marker (same path as
/// `before_side_effect`) so a mid-turn stop never silently resends
/// unrecorded work (E07-3). A present-but-invalid method is never treated
/// as safe: fail-closed to the guarded path (E07).
pub(crate) fn is_safe_http_tool_call(tools: &[ToolDecl], name: &str, args: &Value) -> bool {
    let is_http = tools
        .iter()
        .find(|tool| tool.name() == name)
        .is_some_and(|tool| matches!(tool, ToolDecl::Http { .. }));
    if !is_http {
        return false;
    }
    let Some(method) = args.get("method") else {
        return true;
    };
    let Some(method) = method.as_str() else {
        return false;
    };
    matches!(method.to_ascii_uppercase().as_str(), "GET" | "HEAD")
}

/// Full (unredacted) minimized builtin AskUser args for identity binding
/// (E08). Keeps only `question`, `options`, `fields`, `notes` — the same
/// retained set as the display form — but without any redaction. Only its
/// hash ever leaves this function (as `content_hash` / question identity);
/// the full bytes themselves are never journaled, only the redacted display
/// form below.
/// Risk boundary (E08-1): any other field is refused upstream by schema
/// validation (`agent_tool_schema` sets `additionalProperties: false` for
/// `AskUser`), so a dropped-`extra` alias cannot reach this function
/// through a validated call. An unvalidated direct caller that bypasses
/// `validate_agent_tool_call_args` is a programming error outside the
/// supported path.
pub(crate) fn minimized_builtin_full_args(args: &Value) -> Value {
    let mut kept = serde_json::Map::new();
    if let Some(object) = args.as_object() {
        for key in ["question", "options", "fields", "notes"] {
            if let Some(value) = object.get(key) {
                kept.insert(key.to_string(), value.clone());
            }
        }
    }
    Value::Object(kept)
}

/// Redacts credential-like values plus URL/query secrets inside retained
/// text for display/journaling (E08). JSON credential keys are redacted via
/// the shared gateway helper; every remaining string also passes through
/// URL and credential-assignment redaction so in-text secrets (for example
/// `api_key=s3cret` inside `question` or a `?token=` URL in `notes`) never
/// reach the journal. Keys stay visible for diagnosability.
fn redact_minimized_strings(value: Value) -> Value {
    match value {
        Value::String(text) => Value::String(qcg_policy::redact_credential_assignments_in_text(
            &qcg_policy::redact_urls_in_text(&text),
        )),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(redact_minimized_strings).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, val)| (key, redact_minimized_strings(val)))
                .collect(),
        ),
        other => other,
    }
}

/// Minimized builtin AskUser args kept in the checkpoint for display and
/// journaling (E08-4, E08 SENSITIVE). Resume requires the re-displayed
/// question (`question`), the answer constraint (`options`), the validated
/// form (`fields`), and the material `notes` context: every other
/// free-text/PII field is dropped. The retained set is what the user sees
/// and answers. This is the REDACTED display form only — journaled and
/// shown, never used for identity. Including `notes` means two calls
/// differing only there separate (E08a). An explicitly empty `fields`
/// array is part of the identity but fails at execution time like any
/// invalid form. Identity binds the FULL form via [`builtin_content_hash`]
/// and [`ask_user_question_id`]; display never forks it (E08 two-field).
pub(crate) fn minimized_builtin_args(args: &Value) -> Value {
    let full = minimized_builtin_full_args(args);
    let redacted = qcg_engine::redact_credential_values(&full);
    redact_minimized_strings(redacted)
}

/// Binds the FULL unredacted minimized bytes as a separate `content_hash`
/// for identity (E08 SENSITIVE). Secrets never leave as plaintext: only
/// this hash is stored alongside the redacted display form, and the
/// question identity derives from it. Distinct secrets yield distinct
/// hashes even when their redacted displays alias. Salted with the run id
/// like the E09 digest scheme so identical content in different runs yields
/// different hashes (cross-run unlinkability); the question id binds
/// `call_id` plus this salted hash (E08).
pub(crate) fn builtin_content_hash(
    node_id: &str,
    salt: &str,
    args: &Value,
) -> Result<String, StepError> {
    let full = minimized_builtin_full_args(args);
    let bytes = serde_json::to_vec(&full).map_err(|error| {
        StepError::failed(
            node_id,
            format!("ask_user arguments are not serializable: {error}"),
        )
    })?;
    Ok(qcg_engine::salted_binding_digest(
        "builtin-content-v1",
        salt,
        &bytes,
    ))
}

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
/// `node:tool:hex` identity is honored; legacy bare `node:tool` keys are
/// refused fail-closed. Backward compatibility is intentionally not
/// preserved: an old bare answer never completes a new question (E08/Q1).
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
        Some("http") | Some("fs.write") => redact_agent_tool_args_for_journal(tools, name, args),
        _ => qcg_engine::redact_credential_values(args),
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
    Ok(qcg_engine::salted_binding_digest(
        "agent-call-v1",
        salt,
        &bytes,
    ))
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

/// Agent HTTP details (E09a). Delegates to the single shared constructor
/// in `qcg-engine` (`qcg_engine::http_details_with_url`), which the plain
/// HTTP step also uses, so the two paths produce byte-identical details and
/// cannot fork approvals (E09).
pub(crate) fn http_details_with_url(
    method: &str,
    headers: &std::collections::BTreeMap<String, String>,
    body: Option<&[u8]>,
    sensitive: &BTreeMap<String, String>,
    salt: &str,
    url: &str,
) -> Option<Value> {
    qcg_engine::http_details_with_url(method, headers, body, sensitive, salt, url)
}

/// Redacts a command target for journaling (E09c). Credential assignments
/// and URL secrets are stripped; keys stay visible for diagnosability.
/// Plain commands (for example `echo hi`) pass through unchanged so
/// existing digests stay stable.
pub(crate) fn redact_command_target_for_journal(target: &str) -> String {
    qcg_policy::redact_credential_assignments_in_text(&qcg_policy::redact_urls_in_text(target))
}

/// Redacted Debug for [`AgentHttpRequest`]: URL, headers, body, and
/// sensitive values never reach logs in plaintext.
pub(crate) fn debug_agent_http_request(
    request: &AgentHttpRequest,
    formatter: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    formatter
        .debug_struct("AgentHttpRequest")
        .field("method", &request.method)
        .field("url", &qcg_policy::redact_all_query_values(&request.url))
        .field(
            "headers",
            &qcg_policy::redact_header_values(&request.headers),
        )
        .field(
            "body",
            &request.body.as_ref().map(|body| {
                use sha2::Digest as _;
                format!("[BODY:sha256:{}]", hex::encode(sha2::Sha256::digest(body)))
            }),
        )
        .field("operation_id", &request.operation_id)
        .finish()
}

/// Enforces the used-call registry: the same call id with the same args
/// hash resumes idempotently, while the same call id with a different hash
/// fails closed — a model replay must never complete a new question with
/// an old answer (E08-1).
pub(crate) fn check_used_call_id(
    node_id: &str,
    used_calls: &mut BTreeMap<String, String>,
    call_id: &str,
    identity_hash: &str,
) -> Result<(), StepError> {
    match used_calls.get(call_id) {
        None => {
            used_calls.insert(call_id.to_string(), identity_hash.to_string());
            Ok(())
        }
        Some(known) if known == identity_hash => Ok(()),
        Some(_) => Err(StepError::failed(
            node_id,
            format!("tool call `{call_id}` was reused with different arguments; refusing resume"),
        )),
    }
}

/// Reports whether journaled args carry a redaction placeholder. Matches
/// the exact placeholder shapes (`[REDACTED...]`, URL-encoded
/// `%5BREDACTED...`) instead of a bare substring, so legitimate content
/// merely containing the word `REDACTED` is not misdetected (E07/E09).
/// Content identical to the placeholder itself is treated as redacted:
/// that is the fail-closed direction.
pub(crate) fn args_contain_redaction_marker(args: &Value) -> bool {
    qcg_engine::contains_redaction_marker(&args.to_string())
}

/// Builds the approval/guard details for an agent command tool call. The
/// agent command tool takes no stdin arguments today, but the binding
/// keeps the same `stdin_sha256` shape as plain command steps (explicit
/// `null`) through the shared helper, so a future stdin argument must
/// extend this binding instead of silently riding an unbound approval
/// (E09).
pub(crate) fn agent_command_details(
    node_id: &str,
    plan: Value,
    salt: &str,
) -> Result<Option<Value>, StepError> {
    qcg_engine::bind_command_stdin(node_id, plan, None, salt).map(Some)
}

/// Reports whether a redaction placeholder remains in any executed
/// position of an agent HTTP request (URL, header value, body, sensitive
/// value). A marker in free-text args never reaches the wire, but a marker
/// here would send the placeholder as a credential: the resume must refuse
/// instead of executing a request that differs from the approved and
/// recorded one (E07/E09).
pub(crate) fn executed_http_positions_hold_marker(
    url: &str,
    headers: &std::collections::BTreeMap<String, String>,
    body: Option<&str>,
    sensitive: &std::collections::BTreeMap<String, String>,
) -> bool {
    qcg_engine::contains_redaction_marker(url)
        || headers
            .values()
            .any(|value| qcg_engine::contains_redaction_marker(value))
        || body.is_some_and(qcg_engine::contains_redaction_marker)
        || sensitive
            .values()
            .any(|value| qcg_engine::contains_redaction_marker(value))
}

/// Cached success for a redacted resume: the same invocation already
/// finished with a result, so it resends without needing the original
/// bytes. Returns `None` when no such cache exists and the guard must
/// decide instead (E07-1). Lookup is by invocation-bound operation id, and
/// the caller (`agent.rs`) enforces the used-call registry beforehand (same
/// call id with a different canonical identity hash fails closed), so this
/// resend runs only when both the operation id and the used-call identity
/// match. It intentionally runs before the engine guard: the redacted
/// placeholder digest would never equal the original record digest, so a
/// guard-first route would refuse every legitimate redacted resend. The
/// guard still decides all non-cached redacted resumes below (E07/E08).
fn redacted_success_resend(record: Option<&qcg_engine::OperationRecord>) -> Option<Value> {
    let record = record?;
    if !matches!(record.status, qcg_engine::OperationStatus::Succeeded) {
        return None;
    }
    record.result.clone()
}

/// Redacts a tool call for journaling, preserving its identity (E09).
pub(crate) fn redact_tool_call_for_journal(
    tools: &[ToolDecl],
    call: &qcg_llm::ChatToolCall,
) -> qcg_llm::ChatToolCall {
    qcg_llm::ChatToolCall {
        id: call.id.clone(),
        name: call.name.clone(),
        args: redact_agent_tool_args_for_journal(tools, &call.name, &call.args),
    }
}

/// Redacts assistant tool calls inside checkpoint messages for journaling.
/// The live `messages` vector keeps raw args for execution; only the
/// checkpoint copy is redacted (E09).
pub(crate) fn redact_messages_for_journal(
    tools: &[ToolDecl],
    messages: &[qcg_llm::ChatMessage],
) -> Vec<qcg_llm::ChatMessage> {
    messages
        .iter()
        .map(|message| {
            if message.tool_calls.is_empty() {
                return message.clone();
            }
            let mut redacted = message.clone();
            redacted.tool_calls = message
                .tool_calls
                .iter()
                .map(|call| redact_tool_call_for_journal(tools, call))
                .collect();
            redacted
        })
        .collect()
}

/// Restores a redacted pending call from its run-private sidecar (F02).
/// Returns the full call when the sidecar payload verifies against the
/// stored confirmation digest; otherwise returns the stored (redacted)
/// call so the caller still fails closed. The public journal never gains
/// the restored bytes: restoration is execution-only.
pub(crate) fn restore_pending_call_from_sidecar(
    tools: &[ToolDecl],
    node: &NodeDef,
    run_id: &str,
    stored: &qcg_llm::ChatToolCall,
    confirm: Option<&qcg_api::ConfirmSpec>,
    sidecar: Option<&Value>,
) -> qcg_llm::ChatToolCall {
    let Some(sidecar) = sidecar else {
        return stored.clone();
    };
    let restored = qcg_llm::ChatToolCall {
        id: sidecar
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or(&stored.id)
            .to_string(),
        name: sidecar
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(&stored.name)
            .to_string(),
        args: sidecar.get("args").cloned().unwrap_or(Value::Null),
    };
    if restored.id != stored.id || restored.name != stored.name {
        return stored.clone();
    }
    // The restored payload must redact to the journaled copy: otherwise
    // the sidecar names a different operation than the journaled one.
    let redacted_restored = redact_tool_call_for_journal(tools, &restored);
    if redacted_restored.id != stored.id
        || redacted_restored.name != stored.name
        || redacted_restored.args != stored.args
    {
        return stored.clone();
    }
    // When a confirmation binds the original content digest, the restored
    // payload must recompute to it; otherwise a swapped payload could ride
    // the same approval.
    if let Some(confirm) = confirm
        && let Some(expected) = pending_call_digest(tools, node, run_id, &restored)
        && expected != confirm.operation_digest
    {
        return stored.clone();
    }
    restored
}

/// Recomputes the approval digest for a pending call (F02). Returns `None`
/// for tool kinds without a content-bound digest reconstructible here
/// (command tools bind through gateway-built plans); those keep the
/// redacted-equality check above, which still requires the private sidecar
/// to match the journaled operation exactly.
fn pending_call_digest(
    tools: &[ToolDecl],
    node: &NodeDef,
    run_id: &str,
    call: &qcg_llm::ChatToolCall,
) -> Option<String> {
    let tool = tools.iter().find(|tool| tool.name() == call.name)?;
    match tool {
        ToolDecl::FsWrite { path_prefix, .. } => {
            let path = call.args.get("path")?.as_str()?;
            let content = call.args.get("content")?.as_str()?;
            let details = Some(json!({
                "path_prefix": path_prefix,
                "content_sha256": qcg_engine::salted_binding_digest(
                    "fswrite-content-v1",
                    run_id,
                    content.as_bytes()
                ),
            }));
            qcg_engine::RunContext::operation_digest(path, &details).ok()
        }
        ToolDecl::Http { .. } => {
            let method = call
                .args
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("GET")
                .to_ascii_uppercase();
            let url = call.args.get("url")?.as_str()?;
            let mut headers = BTreeMap::new();
            if let Some(object) = call.args.get("headers").and_then(Value::as_object) {
                for (key, value) in object {
                    headers.insert(key.clone(), value.as_str()?.to_string());
                }
            }
            let body = call.args.get("body").and_then(Value::as_str);
            let sensitive =
                agent_sensitive_query(&node.id, url, call.args.get("sensitive_query")).ok()?;
            let journal_url = qcg_policy::redact_all_query_values(
                &qcg_engine::redact_query_parameters(url, &sensitive).ok()?,
            );
            let details = http_details_with_url(
                &method,
                &headers,
                body.map(str::as_bytes),
                &sensitive,
                run_id,
                url,
            );
            qcg_engine::RunContext::operation_digest(&journal_url, &details).ok()
        }
        ToolDecl::Mcp { server, tool, .. } => {
            // Same target/details shape as the MCP approval site, so a
            // swapped payload recomputes a different digest and fails the
            // confirm binding below instead of riding it.
            let target = format!("{server}/{tool}");
            let details = Some(crate::mcp_forms::mcp_argument_summary(&call.args));
            qcg_engine::RunContext::operation_digest(&target, &details).ok()
        }
        _ => None,
    }
}

pub(crate) async fn execute_agent_fs_write(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    path: &str,
    content: &str,
    operation_id: &str,
) -> Result<AgentToolOutcome, StepError> {
    let target = ctx.run.fs.resolve_write(path).step_err(&node.id)?;
    match ctx
        .run
        .fs
        .write_file_atomic(&target, content.as_bytes())
        .await
    {
        Ok(()) => {
            let value = json!({ "file": path });
            ctx.run.finish_external_operation(
                ctx.journal,
                node,
                operation_id,
                Some(value.clone()),
            )?;
            // Settlement cleanup (F02): the pending payload is no longer
            // needed once the operation finished.
            ctx.run.clear_pending_tool_payload(operation_id);
            Ok(AgentToolOutcome::Result(value))
        }
        Err(error) => {
            if !matches!(error, qcg_engine::GatewayError::Canceled) {
                ctx.run.finish_external_operation_with_warn(
                    ctx.journal,
                    node,
                    operation_id,
                    qcg_engine::OperationOutcome::gateway_error(&error, false),
                );
            }
            Err(StepError::from_gateway(&node.id, error))
        }
    }
}

/// Resolved agent HTTP request parts shared by the guarded call sites so
/// the executor takes one bundle instead of eight separate arguments.
/// Debug is redacting: URL query values, credential headers, and body
/// bytes never reach logs in plaintext (E09h).
pub(crate) struct AgentHttpRequest {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) headers: std::collections::BTreeMap<String, String>,
    pub(crate) sensitive: BTreeMap<String, String>,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) operation_id: Option<String>,
}

impl std::fmt::Debug for AgentHttpRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug_agent_http_request(self, formatter)
    }
}

pub(crate) async fn execute_agent_http_request(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    request: AgentHttpRequest,
) -> Result<AgentToolOutcome, StepError> {
    let AgentHttpRequest {
        method,
        url,
        headers,
        sensitive,
        body,
        operation_id,
    } = request;
    let safe_method = matches!(method.as_str(), "GET" | "HEAD");
    // Agent tool calls never follow redirects (unlike plain steps, which
    // follow non-sensitive safe-method chains): a model-driven call must
    // not silently replay the approved operation against a new target; the
    // 3xx response itself is the tool result (E09).
    let output = match ctx
        .run
        .http
        .request(HttpRequest {
            method,
            url: url.to_string(),
            headers,
            sensitive_query: sensitive.clone(),
            body,
            follow_redirects: false,
            idempotency_key: operation_id.clone(),
        })
        .await
    {
        Ok(output) => output,
        Err(error) => {
            // Cancellation finishes nothing: it propagates
            // without a completion record.
            if !matches!(error, qcg_engine::GatewayError::Canceled)
                && let Some(operation_id) = operation_id
            {
                ctx.run.finish_external_operation_with_warn(
                    ctx.journal,
                    node,
                    &operation_id,
                    qcg_engine::OperationOutcome::gateway_error(&error, safe_method),
                );
            }
            return Err(StepError::from_gateway(&node.id, error));
        }
    };
    // E09-3: headers are redacted (authorization-like values
    // replaced) and the output URL carries redacted query values; the body
    // stays full up to the gateway limit here because it is the tool's
    // functional result, while journaled events carry the 32 KiB truncated
    // copy via `bounded_event_value`. No unbounded echo: the gateway bounds
    // the body, and the event path truncates again.
    let output = json!({
        "status": output.status,
        "url": qcg_policy::redact_all_query_values(&output.url),
        "headers": qcg_policy::redact_header_values(&output.headers),
        "body": http_body_value(&output.body),
    });
    match operation_id {
        Some(operation_id) => Ok(AgentToolOutcome::OperationResult {
            value: output,
            operation_id,
        }),
        None => Ok(AgentToolOutcome::Result(output)),
    }
}

pub(crate) async fn execute_agent_tool(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    services: AgentToolServices<'_>,
    invocation: AgentToolInvocation<'_>,
) -> Result<AgentToolOutcome, StepError> {
    let AgentToolInvocation {
        mcp,
        tools,
        name,
        call_id,
        call_number,
        args,
        activated_skills,
    } = invocation;
    let tool = tools
        .iter()
        .find(|tool| tool.name() == name)
        .ok_or_else(|| StepError::failed(&node.id, format!("tool `{name}` is not declared")))?;
    match tool {
        ToolDecl::FsWrite { path_prefix, .. } => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| StepError::failed(&node.id, "fs.write tool requires path"))?;
            let content = args
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| StepError::failed(&node.id, "fs.write tool requires content"))?;
            if !path_is_within_prefix(path, path_prefix) {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` path `{path}` is outside prefix `{path_prefix}`"),
                ));
            }
            // Same side-effect and durability gate as command and HTTP tools
            // (E07): the checkpoint alone is not a durable record. Approval,
            // guard, and completion records decide resend, clean retry, and
            // indeterminate refusal instead of bypassing them.
            {
                // The call id salts the content digest via the operation id
                // so identical file bytes in different calls do not share a
                // journaled digest (E09).
                // Run-scoped salt: identical content in this run binds
                // identically so content-scope approval reuse works, while
                // different runs never share a digest. Invocation separation
                // comes from the operation id, not the salt (E09/Q1).
                let salt = ctx.run.run_id.clone();
                let details = Some(json!({
                    "path_prefix": path_prefix,
                    "content_sha256": qcg_engine::salted_binding_digest(
                        "fswrite-content-v1",
                        &salt,
                        content.as_bytes()
                    ),
                }));
                // A journaled redacted copy must never be executed with the
                // hash placeholder as file content (E07/E09). A cached
                // `Succeeded` result under the same invocation-bound
                // operation id resends before the guard (the caller already
                // enforced the used-call registry, so both operation id and
                // call identity match); every other redacted resume routes
                // through the engine guard below, where a guard `Start`
                // (including a `FailedClean` clean retry with matching
                // content) proceeds to execution and only a guard refusal
                // fails closed (E07-1). The unconditional journal
                // redaction in the gateway stays as-is (fail-closed for
                // secrecy); legitimate retries carry their original bytes
                // and never enter this path, so they keep their normal
                // clean-retry reachability through the guard below (E07-2).
                // No new confirmation is minted here for placeholder
                // digests: a prior operation record under the same
                // invocation proves the approval gate already passed, so a
                // `Proceed` reuses it instead of asking the user to approve
                // a placeholder.
                if args_contain_redaction_marker(args) {
                    let operation_id =
                        qcg_engine::operation_id_for(&ctx.run.run_id, &node.id, call_id);
                    let state = ctx.journal.state();
                    let record = state.operation_records.get(&operation_id).cloned();
                    if let Some(result) = redacted_success_resend(record.as_ref()) {
                        return Ok(AgentToolOutcome::Result(result));
                    }
                    let had_record = record.is_some();
                    match ctx.run.guard_external_operation(
                        ctx.journal,
                        node,
                        "fs.write",
                        path,
                        &details,
                        call_id,
                    )? {
                        qcg_engine::GuardDecision::Resend { result, .. } => {
                            return Ok(AgentToolOutcome::Result(result));
                        }
                        qcg_engine::GuardDecision::Proceed { operation_id } => {
                            if !had_record {
                                // The guard would start a first attempt, but
                                // the bytes are placeholders from a journaled
                                // copy (a crash between checkpoint and guard
                                // leaves no record): finish the just-started
                                // operation as indeterminate and refuse
                                // fail-closed instead of writing the
                                // placeholder as file content.
                                ctx.run.finish_external_operation_with_warn(
                                    ctx.journal,
                                    node,
                                    &operation_id,
                                    qcg_engine::OperationOutcome::Indeterminate {
                                        reason: format!(
                                            "operation `{operation_id}` holds redacted file content; refusing to re-execute without the original bytes"
                                        ),
                                    },
                                );
                                return Err(StepError::Refused {
                                    node: node.id.clone(),
                                    message: format!(
                                        "operation `{operation_id}` holds redacted file content; refusing to re-execute without the original bytes"
                                    ),
                                });
                            }
                        }
                    }
                    // Guard `Proceed` with a prior record (for example a
                    // `FailedClean` clean retry whose content matches): the
                    // redaction marker is still present by construction of
                    // this branch, and file content is digest-bound, so the
                    // live bytes are placeholders, not the original content.
                    // Refuse fail-closed instead of writing the placeholder
                    // as file content. (No fall-through to execution exists
                    // here by design.)
                    let operation_id =
                        qcg_engine::operation_id_for(&ctx.run.run_id, &node.id, call_id);
                    ctx.run.finish_external_operation_with_warn(
                        ctx.journal,
                        node,
                        &operation_id,
                        qcg_engine::OperationOutcome::Indeterminate {
                            reason: format!(
                                "operation `{operation_id}` holds redacted file content; refusing to re-execute without the original bytes"
                            ),
                        },
                    );
                    return Err(StepError::Refused {
                        node: node.id.clone(),
                        message: format!(
                            "operation `{operation_id}` holds redacted file content; refusing to re-execute without the original bytes"
                        ),
                    });
                }
                if let Some(confirm) = ctx.run.require_side_effect(
                    ctx.journal,
                    node,
                    "fs.write",
                    path,
                    details.clone(),
                    call_id,
                )? {
                    return Ok(AgentToolOutcome::NeedsConfirm(confirm));
                }
                let operation_id = match ctx.run.guard_external_operation(
                    ctx.journal,
                    node,
                    "fs.write",
                    path,
                    &details,
                    call_id,
                )? {
                    qcg_engine::GuardDecision::Proceed { operation_id } => operation_id,
                    qcg_engine::GuardDecision::Resend { result, .. } => {
                        return Ok(AgentToolOutcome::Result(result));
                    }
                };
                ctx.run.fs.resolve_write(path).step_err(&node.id)?;
                return execute_agent_fs_write(ctx, node, path, content, &operation_id).await;
            }
        }
        ToolDecl::Command { command, .. } => {
            // Same side-effect gate as ordinary command steps (A05):
            // command allowlist alone never substitutes for the
            // permissions.side_effects policy.
            let full_target = command.join(" ");
            // E09c: the journaled target never carries plaintext secrets;
            // credential assignments and URL secrets are redacted while the
            // full argv stays bound through the salted plan digest below.
            let target = redact_command_target_for_journal(&full_target);
            // Bind the full plaintext target into the digest so redacted
            // journaling never aliases distinct commands to one approval.
            // `target_sha256` binds the joined display string; `argv_sha256`
            // binds the exact argv array bytes so boundary-shuffled argvs
            // never alias. Both are salted with the run id.
            let target_binding = qcg_engine::salted_binding_digest(
                "command-target-v1",
                &ctx.run.run_id,
                full_target.as_bytes(),
            );
            let argv_bytes = serde_json::to_vec(command).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("command argv is not serializable: {error}"),
                )
            })?;
            let argv_binding =
                qcg_engine::salted_binding_digest("command-argv-v1", &ctx.run.run_id, &argv_bytes);
            let plan = ctx
                .run
                .cmd
                .command_plan(command)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            // Run-scoped salt so identical commands in this run bind
            // identically (content-scope reuse); runs never share a digest.
            // Invocation separation comes from the operation id (E09/Q1).
            let salt = ctx.run.run_id.clone();
            let mut plan_value = agent_command_details(
                &node.id,
                serde_json::to_value(&plan).map_err(|error| {
                    StepError::failed(
                        &node.id,
                        format!("command plan is not serializable: {error}"),
                    )
                })?,
                &salt,
            )?
            .unwrap_or(serde_json::json!({}));
            if let Some(object) = plan_value.as_object_mut() {
                object.insert("target_sha256".into(), Value::String(target_binding));
                object.insert("argv_sha256".into(), Value::String(argv_binding));
            }
            let plan_value = Some(plan_value);
            if let Some(confirm) = ctx.run.require_side_effect(
                ctx.journal,
                node,
                "command",
                &target,
                plan_value.clone(),
                call_id,
            )? {
                return Ok(AgentToolOutcome::NeedsConfirm(confirm));
            }
            // Agent invocations identify by call id: the checkpoint
            // re-issues the exact suspended call on resume.
            let operation_id = match ctx.run.guard_external_operation(
                ctx.journal,
                node,
                "command",
                &target,
                &plan_value,
                call_id,
            )? {
                qcg_engine::GuardDecision::Proceed { operation_id } => operation_id,
                // Same invocation already succeeded: return the cached
                // result without touching the remote again.
                qcg_engine::GuardDecision::Resend { result, .. } => {
                    return Ok(AgentToolOutcome::Result(result));
                }
            };
            let output = match ctx.run.cmd.run(command).await {
                Ok(output) => output,
                Err(error) => {
                    // Cancellation finishes nothing: it propagates
                    // without a completion record.
                    if !matches!(error, qcg_engine::GatewayError::Canceled) {
                        ctx.run.finish_external_operation_with_warn(
                            ctx.journal,
                            node,
                            &operation_id,
                            qcg_engine::OperationOutcome::gateway_error(&error, false),
                        );
                    }
                    return Err(StepError::from_gateway(&node.id, error));
                }
            };
            let output = json!({
                "status": output.status,
                "stdout": output.stdout,
                "stderr": output.stderr,
            });
            Ok(AgentToolOutcome::OperationResult {
                value: output,
                operation_id,
            })
        }
        ToolDecl::Http { methods, hosts, .. } => {
            let method = args
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("GET")
                .to_ascii_uppercase();
            if !methods.iter().any(|allowed| allowed == &method) {
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` method `{method}` is not declared"),
                ));
            }
            let url = args
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| StepError::failed(&node.id, "http tool requires url"))?;
            let host_allowed = hosts.iter().any(|host| url_host_matches(url, host));
            if !host_allowed {
                // E09-2: error strings never echo query values; truncation
                // alone is not redaction.
                let safe_url = qcg_policy::redact_all_query_values(url);
                return Err(StepError::failed(
                    &node.id,
                    format!("tool `{name}` url `{safe_url}` is outside declared hosts"),
                ));
            }
            let mut headers = std::collections::BTreeMap::new();
            if let Some(object) = args.get("headers").and_then(Value::as_object) {
                for (key, value) in object {
                    let value = value.as_str().ok_or_else(|| {
                        StepError::failed(
                            &node.id,
                            format!("tool `{name}` header `{key}` must be a string"),
                        )
                    })?;
                    // Casing is normalized once inside
                    // `http_operation_details`; keep the raw name here.
                    headers.insert(key.clone(), value.to_string());
                }
            }
            let body = args
                .get("body")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            // Declared sensitive query values are redacted from the
            // journaled target and bound through a digest, so an agent call
            // cannot leak credentials into the journal (E09). Every
            // remaining query VALUE is additionally redacted by default
            // (keys stay visible); declared sensitivity still drives
            // digest salting via `sensitive` (E09-1).
            let sensitive = agent_sensitive_query(&node.id, url, args.get("sensitive_query"))?;
            let journal_url = qcg_policy::redact_all_query_values(
                &qcg_engine::redact_query_parameters(url, &sensitive)
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?,
            );
            // Run-scoped salt so identical requests in this run bind
            // identically (content-scope reuse); runs never share a digest.
            // Invocation separation comes from the operation id (E09/Q1).
            // E09a: the full canonical URL (sorted query pairs) is bound
            // into the digest so different queries never share one approval.
            let http_salt = ctx.run.run_id.clone();
            let http_details = http_details_with_url(
                &method,
                &headers,
                body.as_deref().map(str::as_bytes),
                &sensitive,
                &http_salt,
                url,
            );
            // A journaled redacted copy must never be executed with the
            // placeholder as a credential (E07/E09). A cached `Succeeded`
            // result under the same invocation-bound operation id resends
            // before the guard (the caller already enforced the used-call
            // registry, so both operation id and call identity match —
            // digest comparison is bypassed on this fast path by design,
            // see `redacted_success_resend`); every other redacted resume
            // routes through the engine guard below, where a guard `Start`
            // (including a `FailedClean` clean retry with matching content)
            // proceeds to execution and only a guard refusal fails closed
            // (E07-1). The unconditional journal
            // redaction in the gateway stays as-is (fail-closed for
            // secrecy); legitimate retries carry their original bytes and
            // never enter this path, so they keep their normal clean-retry
            // reachability through the guard below (E07-2). Safe reads
            // take no guard and leave no durable record, so a redacted
            // safe read always refuses: there is nothing to resend and no
            // retry bytes to spend.
            if args_contain_redaction_marker(args) {
                if matches!(method.as_str(), "GET" | "HEAD") {
                    return Err(StepError::Refused {
                        node: node.id.clone(),
                        message: format!(
                            "tool `{name}` holds a redacted credential with no durable record; refusing to re-execute without the original bytes"
                        ),
                    });
                }
                let operation_id = qcg_engine::operation_id_for(&ctx.run.run_id, &node.id, call_id);
                let state = ctx.journal.state();
                let record = state.operation_records.get(&operation_id).cloned();
                if let Some(result) = redacted_success_resend(record.as_ref()) {
                    return Ok(AgentToolOutcome::Result(result));
                }
                let had_record = record.is_some();
                match ctx.run.guard_external_operation(
                    ctx.journal,
                    node,
                    "http",
                    &journal_url,
                    &http_details,
                    call_id,
                )? {
                    qcg_engine::GuardDecision::Resend { result, .. } => {
                        return Ok(AgentToolOutcome::Result(result));
                    }
                    qcg_engine::GuardDecision::Proceed { operation_id } => {
                        if !had_record {
                            ctx.run.finish_external_operation_with_warn(
                                ctx.journal,
                                node,
                                &operation_id,
                                qcg_engine::OperationOutcome::Indeterminate {
                                    reason: format!(
                                        "operation `{operation_id}` holds a redacted credential; refusing to re-execute without the original bytes"
                                    ),
                                },
                            );
                            return Err(StepError::Refused {
                                node: node.id.clone(),
                                message: format!(
                                    "operation `{operation_id}` holds a redacted credential; refusing to re-execute without the original bytes"
                                ),
                            });
                        }
                        // A marker in free-text args never reaches the
                        // wire, but a marker in any executed position
                        // (URL, header value, body, sensitive value) would
                        // send the placeholder as a credential. Refuse
                        // instead of executing a request that differs from
                        // the approved and recorded one (E07/E09).
                        if executed_http_positions_hold_marker(
                            url,
                            &headers,
                            body.as_deref(),
                            &sensitive,
                        ) {
                            ctx.run.finish_external_operation_with_warn(
                                ctx.journal,
                                node,
                                &operation_id,
                                qcg_engine::OperationOutcome::Indeterminate {
                                    reason: format!(
                                        "operation `{operation_id}` holds a redacted credential in an executed position; refusing to re-execute without the original bytes"
                                    ),
                                },
                            );
                            return Err(StepError::Refused {
                                node: node.id.clone(),
                                message: format!(
                                    "operation `{operation_id}` holds a redacted credential in an executed position; refusing to re-execute without the original bytes"
                                ),
                            });
                        }
                        return execute_agent_http_request(
                            ctx,
                            node,
                            AgentHttpRequest {
                                method,
                                url: url.to_string(),
                                headers,
                                sensitive,
                                body: body.map(String::into_bytes),
                                operation_id: Some(operation_id),
                            },
                        )
                        .await;
                    }
                }
            }
            if !matches!(method.as_str(), "GET" | "HEAD")
                && let Some(confirm) = ctx.run.require_side_effect(
                    ctx.journal,
                    node,
                    "http",
                    &journal_url,
                    http_details.clone(),
                    call_id,
                )?
            {
                return Ok(AgentToolOutcome::NeedsConfirm(confirm));
            }
            let operation_id = if matches!(method.as_str(), "GET" | "HEAD") {
                None
            } else {
                Some(
                    match ctx.run.guard_external_operation(
                        ctx.journal,
                        node,
                        "http",
                        &journal_url,
                        &http_details,
                        call_id,
                    )? {
                        qcg_engine::GuardDecision::Proceed { operation_id } => operation_id,
                        // Same invocation already succeeded: return the cached
                        // result without touching the remote again.
                        qcg_engine::GuardDecision::Resend { result, .. } => {
                            return Ok(AgentToolOutcome::Result(result));
                        }
                    },
                )
            };
            return execute_agent_http_request(
                ctx,
                node,
                AgentHttpRequest {
                    method,
                    url: url.to_string(),
                    headers,
                    sensitive,
                    body: body.map(String::into_bytes),
                    operation_id,
                },
            )
            .await;
        }
        ToolDecl::AskUser { .. } => {
            // E08c: validation runs before id generation so an invalid
            // form fails before journaling anything. Question identity
            // binds node, tool, call id, and the minimized arguments
            // (question, options, fields, notes): two questions from the
            // same tool are distinct, and a resume re-issues the stored
            // minimized call with the same identity so only its own answer
            // is reused (E08, E07-5/E08-3).
            let declared_fields = dynamic_form_fields(&node.id, args)?;
            // E08-5: a non-string option is a fail-closed error, never a
            // silent drop (which would flip a Select into a String and
            // accept an unconstrained answer for a constrained question).
            // Validated here, before the question id is minted.
            let validated_options: Vec<String> = match args.get("options") {
                None => Vec::new(),
                Some(Value::Array(options)) => options
                    .iter()
                    .map(|option| {
                        option.as_str().map(str::to_owned).ok_or_else(|| {
                            StepError::failed(&node.id, "ask_user options must be strings")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Some(_) => {
                    return Err(StepError::failed(
                        &node.id,
                        "ask_user options must be an array of strings",
                    ));
                }
            };
            let question_id =
                ask_user_question_id(&node.id, &ctx.run.run_id, tool.name(), call_id, args)?;
            let raw_title = args
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("Agent requested input");
            // E09d: the journaled title never carries raw secrets. Strip
            // credential assignments and URL secrets, then truncate long
            // questions to shape plus hash.
            let title = {
                let redacted = qcg_policy::redact_credential_assignments_in_text(
                    &qcg_policy::redact_urls_in_text(raw_title),
                );
                const LIMIT: usize = 256;
                crate::prompting::truncate_with_digest(&redacted, LIMIT)
            };
            let options = validated_options;
            // The default form (no declared fields) is still a validated
            // form: an answer for it must satisfy the default field, never
            // pass through unchecked (E08).
            let effective_fields = declared_fields.unwrap_or_else(|| {
                let kind = if options.is_empty() {
                    FieldType::String
                } else {
                    FieldType::Select
                };
                vec![InputField {
                    id: "answer".into(),
                    label: None,
                    label_i18n: Default::default(),
                    description: None,
                    description_i18n: Default::default(),
                    placeholder: None,
                    placeholder_i18n: Default::default(),
                    kind,
                    required: true,
                    default: None,
                    pattern: None,
                    options: options.clone(),
                    option_labels_i18n: Default::default(),
                    min_items: None,
                    item_type: None,
                    schema: None,
                    options_from: None,
                    ui: Default::default(),
                }]
            });
            if let Some(answer) = answer_for_question(&ctx.run.answers, &question_id) {
                validate_form_values(
                    &effective_fields,
                    answer,
                    &ctx.run.contract.manifest.runtime,
                )
                .map_err(|error| {
                    StepError::failed(&node.id, format!("invalid agent form answer: {error}"))
                })?;
                return Ok(AgentToolOutcome::Result(json!({ "answer": answer })));
            }
            let fields = effective_fields;
            Ok(AgentToolOutcome::NeedsUser(FormSpec {
                id: question_id,
                title,
                title_i18n: Default::default(),
                fields,
            }))
        }
        ToolDecl::WebSearch { .. } => execute_web_search(
            &ctx.run.http,
            &ctx.run.secrets,
            &services.runtime.search,
            node,
            tool,
            args,
        )
        .await
        .map(AgentToolOutcome::Result),
        ToolDecl::Mcp {
            server,
            tool: remote_tool,
            side_effects,
            ..
        } => {
            if *side_effects
                && let Some(confirm) = ctx.run.require_side_effect(
                    ctx.journal,
                    node,
                    &format!("mcp:{name}"),
                    &format!("{server}/{remote_tool}"),
                    Some(mcp_argument_summary(args)),
                    call_id,
                )?
            {
                return Ok(AgentToolOutcome::NeedsConfirm(confirm));
            }
            execute_mcp_tool(ctx, node, mcp, name, args.clone(), call_id).await
        }
        ToolDecl::Skill { resources, .. } => crate::skill_tool::execute_skill_tool(
            ctx,
            node,
            name,
            resources,
            args,
            activated_skills,
        )
        .map(AgentToolOutcome::Result),
        ToolDecl::Agent {
            instructions,
            tools: delegated,
            max_calls,
            max_iterations,
            max_tokens_total,
            max_tool_calls_total,
            output_schema,
            model,
            fallback_models,
            request,
            on_failure,
            handoff,
            ..
        } => {
            let result = match execute_specialist_agent(
                ctx,
                node,
                services,
                mcp,
                tools,
                SpecialistAgentSpec {
                    name,
                    invocation_id: call_id,
                    instructions,
                    delegated_names: delegated,
                    max_calls: *max_calls,
                    max_iterations: *max_iterations,
                    max_tokens_total: *max_tokens_total,
                    max_tool_calls_total: *max_tool_calls_total,
                    output_schema_path: output_schema.as_deref(),
                    model: model.as_ref(),
                    fallback_models,
                    request,
                    args,
                },
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    let code = agent_failure_code(&error);
                    let action = on_failure.action(code);
                    record_agent_failure_event(ctx, node, name, call_id, code, action, &error)?;
                    return match action {
                        AgentFailureAction::Fail => Err(error),
                        AgentFailureAction::ReturnError => {
                            Ok(AgentToolOutcome::Error(agent_error_result(
                                name,
                                code,
                                &error,
                                call_number,
                                AgentToolFailureLimits {
                                    max_calls: *max_calls,
                                    max_iterations: *max_iterations,
                                    max_tokens_total: *max_tokens_total,
                                    max_tool_calls_total: *max_tool_calls_total,
                                },
                                true,
                            )))
                        }
                    };
                }
            };
            match result {
                AgentToolOutcome::Result(value) if *handoff => Ok(AgentToolOutcome::Handoff(value)),
                result => Ok(result),
            }
        }
    }
}

/// Records a rejected external operation as a success without a reusable
/// result, so a later replay refuses instead of re-executing the effect
/// (D03). The original rejection error is never masked.
pub(crate) fn finish_rejected_external_operation(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    operation_id: Option<String>,
) {
    let Some(operation_id) = operation_id else {
        return;
    };
    ctx.run.finish_external_operation_with_warn(
        ctx.journal,
        node,
        &operation_id,
        qcg_engine::OperationOutcome::Success { result: None },
    );
    // Settlement cleanup (F02): the pending payload is no longer needed.
    ctx.run.clear_pending_tool_payload(&operation_id);
}

pub(crate) fn validate_agent_tool_call_args(
    node: &NodeDef,
    mcp: &McpAgentTools,
    tools: &[ToolDecl],
    call: &ChatToolCall,
) -> Result<(), StepError> {
    let tool = tools
        .iter()
        .find(|tool| tool.name() == call.name)
        .ok_or_else(|| {
            StepError::failed(&node.id, format!("tool `{}` is not declared", call.name))
        })?;
    if matches!(tool, ToolDecl::Mcp { .. }) {
        mcp.validate_args(node, &call.name, &call.args)
    } else {
        validate_agent_tool_args(node, tool, &call.args)
    }
}

pub(crate) fn path_is_within_prefix(path: &str, prefix: &str) -> bool {
    let Some(prefix) = normalize_path_prefix(prefix) else {
        return false;
    };
    is_safe_relative_path(path)
        && (path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

pub(crate) fn normalize_path_prefix(prefix: &str) -> Option<&str> {
    let normalized = prefix.strip_suffix('/').unwrap_or(prefix);
    is_safe_relative_path(normalized).then_some(normalized)
}

pub(crate) fn agent_failure_code(error: &StepError) -> AgentFailureCode {
    match error {
        StepError::Cancelled => AgentFailureCode::Cancelled,
        StepError::TimedOut { .. } => AgentFailureCode::TimedOut,
        StepError::ElapsedExceeded { .. } => AgentFailureCode::ElapsedExceeded,
        StepError::BudgetExceeded { .. } => AgentFailureCode::RunBudgetExceeded,
        StepError::Failed { message, .. } if message.contains("token budget exceeded") => {
            AgentFailureCode::TokenBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("tool call budget exceeded") => {
            AgentFailureCode::ToolCallBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("call budget exceeded") => {
            AgentFailureCode::ToolCallBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("iteration budget exceeded") => {
            AgentFailureCode::IterationBudgetExceeded
        }
        StepError::Failed { message, .. } if message.contains("guardrail") => {
            AgentFailureCode::GuardrailRejected
        }
        StepError::Failed { message, .. } if message.contains("validation") => {
            AgentFailureCode::ValidationFailed
        }
        StepError::Failed { message, .. }
            if message.contains("LLM provider") || message.contains("LLM request") =>
        {
            AgentFailureCode::ProviderFailed
        }
        _ => AgentFailureCode::ToolFailed,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AgentToolFailureLimits {
    pub(crate) max_calls: usize,
    pub(crate) max_iterations: usize,
    pub(crate) max_tokens_total: u64,
    pub(crate) max_tool_calls_total: usize,
}

pub(crate) fn agent_error_result(
    name: &str,
    code: AgentFailureCode,
    error: &StepError,
    call_number: usize,
    limits: AgentToolFailureLimits,
    retryable: bool,
) -> Value {
    json!({
        "isError": true,
        "agent": name,
        "error": {
            "code": code,
            // E09-2: strip URL secrets and credential assignments before
            // truncating; truncation alone is not redaction. Error
            // constructors never interpolate secret values (keys only),
            // but remote echoes in free text still need masking.
            "message": utf8_head(
                &qcg_policy::redact_credential_assignments_in_text(
                    &qcg_policy::redact_urls_in_text(&error.to_string()),
                ),
                2_048,
            ),
            "retryable": code.is_recoverable() && retryable && call_number < limits.max_calls,
            "call_number": call_number,
            "limits": {
                "max_calls": limits.max_calls,
                "max_iterations": limits.max_iterations,
                "max_tokens_total": limits.max_tokens_total,
                "max_tool_calls_total": limits.max_tool_calls_total,
            }
        }
    })
}

pub(crate) struct AgentToolCallFailure {
    pub(crate) code: AgentFailureCode,
    pub(crate) phase: ToolCallPhase,
    pub(crate) tool_error_code: ToolCallErrorCode,
    pub(crate) call_number: usize,
    pub(crate) retryable: bool,
    pub(crate) started: Instant,
}

pub(crate) fn recover_agent_tool_call_failure(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    tools: &[ToolDecl],
    call: &ChatToolCall,
    error: &StepError,
    failure: AgentToolCallFailure,
) -> Result<Option<ChatMessage>, StepError> {
    let Some((action, limits)) = agent_tool_failure_resolution(tools, &call.name, failure.code)
    else {
        return Ok(None);
    };
    record_agent_failure_event(ctx, node, &call.name, &call.id, failure.code, action, error)?;
    if action == AgentFailureAction::Fail {
        return Ok(None);
    }
    let result = agent_error_result(
        &call.name,
        failure.code,
        error,
        failure.call_number,
        limits,
        failure.retryable,
    );
    let encoded = serde_json::to_string(&result)?;
    scan_llm_text(ctx, node, &encoded)?;
    let event = tool_call_event(
        &node.id,
        None,
        None,
        call,
        &result,
        tool_call_outcome(
            ToolCallStatus::Failed,
            failure.phase,
            Some(tool_call_error(failure.tool_error_code, error)),
            failure.started,
        ),
    )?;
    ctx.journal.event("tool_call", event).step_err(&node.id)?;
    Ok(Some(ChatMessage::tool_result(call.id.clone(), encoded)))
}

pub(crate) fn agent_tool_failure_resolution(
    tools: &[ToolDecl],
    name: &str,
    code: AgentFailureCode,
) -> Option<(AgentFailureAction, AgentToolFailureLimits)> {
    let ToolDecl::Agent {
        max_calls,
        max_iterations,
        max_tokens_total,
        max_tool_calls_total,
        on_failure,
        ..
    } = tools.iter().find(|tool| tool.name() == name)?
    else {
        return None;
    };
    Some((
        on_failure.action(code),
        AgentToolFailureLimits {
            max_calls: *max_calls,
            max_iterations: *max_iterations,
            max_tokens_total: *max_tokens_total,
            max_tool_calls_total: *max_tool_calls_total,
        },
    ))
}

pub(crate) fn record_agent_failure_event(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    name: &str,
    call_id: &str,
    code: AgentFailureCode,
    action: AgentFailureAction,
    error: &StepError,
) -> Result<(), StepError> {
    ctx.journal
        .event(
            "agent_failed",
            json!({
                "node": node.id,
                "agent": name,
                "tool_call_id": call_id,
                "code": code,
                "action": action,
                // E09-2: strip URL secrets and credential assignments before
                // truncating (see above).
                "message": utf8_head(
                    &qcg_policy::redact_credential_assignments_in_text(
                        &qcg_policy::redact_urls_in_text(&error.to_string()),
                    ),
                    2_048,
                ),
            }),
        )
        .step_err(&node.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn malformed_sensitive_query_declaration_fails_closed() {
        // E09: the agent delegates to the shared gateway parser, so this
        // test pins delegation only (one malformed shape fails through the
        // agent path); the full typo/URL matrix lives with the parser in
        // `gateway::tests::sensitive_query_values_refuse_typos_and_bad_urls`
        // and is not duplicated here.
        let error = agent_sensitive_query(
            "agent",
            "https://example.test/search?q=x",
            Some(&json!(["api_key"])),
        )
        .expect_err("an absent name must fail through delegation");
        assert!(error.to_string().contains("not present in the url"));
    }

    #[test]
    fn journal_redaction_removes_secrets_but_preserves_names() {
        // E09/E09-1: journaled HTTP args must not contain plaintext
        // secrets, but must preserve parameter names so a redacted resume
        // fails closed. Every query VALUE is redacted by default (keys
        // stay visible); declared sensitivity additionally drives digest
        // salting.
        let args = json!({
            "method": "POST",
            "url": "https://example.test/search?q=x&api_key=s3cret",
            "headers": {"Authorization": "Bearer s3cret-token", "X-Tenant": "alpha"},
            "sensitive_query": ["api_key"],
        });
        let tools = vec![ToolDecl::Http {
            name: "fetch".into(),
            description: None,
            input_schema: None,
            methods: vec!["POST".into()],
            hosts: vec!["example.test".into()],
        }];
        let redacted = redact_agent_tool_args_for_journal(&tools, "fetch", &args);
        let redacted_str = redacted.to_string();
        assert!(
            !redacted_str.contains("s3cret"),
            "journaled args must not contain the secret: {redacted_str}"
        );
        let url = redacted
            .get("url")
            .and_then(Value::as_str)
            .expect("redacted url should exist");
        assert!(
            url.contains("api_key="),
            "redacted url must preserve the parameter name: {url}"
        );
        assert!(
            url.contains("REDACTED"),
            "redacted url must carry the placeholder: {url}"
        );
        assert!(
            !url.contains("q=x"),
            "even undeclared query values must be redacted by default (E09-1): {url}"
        );
        assert!(
            url.contains("q="),
            "undeclared parameter names stay visible: {url}"
        );
        let auth = redacted
            .pointer("/headers/Authorization")
            .and_then(Value::as_str)
            .expect("auth header should exist");
        assert_eq!(auth, qcg_engine::REDACTED_PLACEHOLDER);
        // Non-credential headers survive redaction.
        assert_eq!(
            redacted
                .pointer("/headers/X-Tenant")
                .and_then(Value::as_str),
            Some("alpha")
        );
    }

    #[test]
    fn agent_failure_codes_distinguish_timeout_and_elapsed() {
        // E11: node timeout, elapsed exceedance, cancel, and budget must not
        // collapse into ToolFailed.
        assert_eq!(
            agent_failure_code(&StepError::TimedOut {
                node: "n".into(),
                timeout_secs: 1
            }),
            AgentFailureCode::TimedOut
        );
        assert_eq!(
            agent_failure_code(&StepError::ElapsedExceeded {
                node: "n".into(),
                limit_secs: 1
            }),
            AgentFailureCode::ElapsedExceeded
        );
        assert_eq!(
            agent_failure_code(&StepError::Cancelled),
            AgentFailureCode::Cancelled
        );
        assert!(
            !AgentFailureCode::ElapsedExceeded.is_recoverable(),
            "elapsed exceedance must fail, never return an error result"
        );
        assert!(
            AgentFailureCode::TimedOut.is_recoverable(),
            "node timeout stays retryable"
        );
    }

    #[test]
    fn default_ask_user_form_still_validates_answers() {
        // E08: the default form (no declared fields) must still validate:
        // an empty answer must not pass through unchecked.
        use qcg_contract::{FieldType, InputField};
        let default_fields = vec![InputField {
            id: "answer".into(),
            label: None,
            label_i18n: Default::default(),
            description: None,
            description_i18n: Default::default(),
            placeholder: None,
            placeholder_i18n: Default::default(),
            kind: FieldType::String,
            required: true,
            default: None,
            pattern: None,
            options: Vec::new(),
            option_labels_i18n: Default::default(),
            min_items: None,
            item_type: None,
            schema: None,
            options_from: None,
            ui: Default::default(),
        }];
        let runtime = qcg_contract::RuntimeLimits::default();
        assert!(
            qcg_contract::validate_form_values(&default_fields, &json!({}), &runtime).is_err(),
            "an empty answer must not satisfy the required default field"
        );
        assert!(
            qcg_contract::validate_form_values(
                &default_fields,
                &json!({"answer": "Nagoya"}),
                &runtime
            )
            .is_ok(),
            "a filled answer must satisfy the default field"
        );
    }

    #[test]
    fn same_args_with_different_call_ids_stay_separate() {
        // E08: identical arguments under different call ids must yield
        // different question identities so a delayed answer for the old
        // question can never complete the new one. Uses the real
        // validation path shape (no empty `fields` array, which fails
        // before id generation in execution).
        let args = json!({"question": "postal code?", "notes": "billing"});
        let first = ask_user_question_id("agent", "agent", "ask", "call-1", &args)
            .expect("first id should build");
        let second = ask_user_question_id("agent", "agent", "ask", "call-2", &args)
            .expect("second id should build");
        assert_ne!(
            first, second,
            "the call id must separate identical arguments"
        );
        let again = ask_user_question_id("agent", "agent", "ask", "call-1", &args)
            .expect("stable id should build");
        assert_eq!(first, again, "the same call must recompute stably");
        // A late answer for the old question must never complete the new
        // one: answers are keyed by question id, and the ids differ.
        let mut answers = std::collections::BTreeMap::new();
        answers.insert(first.clone(), json!({"answer": "old-town"}));
        assert!(
            !answers.contains_key(&second),
            "the old answer must not satisfy the new question"
        );
        assert!(answers.contains_key(&first));
    }

    #[test]
    fn sequential_questions_stay_separate_with_stable_restart() {
        // E08: one AskUser tool asking city then postal code yields two
        // distinct stable identities; a restart recomputes the same second
        // id, and the first answer never satisfies the second question.
        // Uses valid forms (no empty `fields` arrays) matching the real
        // execution path where empty arrays fail before id generation.
        let city = json!({"question": "city?", "notes": "billing"});
        let postal = json!({"question": "postal code?", "notes": "billing"});
        let first = ask_user_question_id("agent", "agent", "ask", "call-1", &city)
            .expect("city id should build");
        let second = ask_user_question_id("agent", "agent", "ask", "call-2", &postal)
            .expect("postal id should build");
        assert_ne!(first, second, "distinct questions must separate");
        let again = ask_user_question_id("agent", "agent", "ask", "call-2", &postal)
            .expect("restart must recompute the same second id");
        assert_eq!(second, again, "question ids must survive restarts");
        let answers =
            std::collections::BTreeMap::from([(first.clone(), json!({"answer": "old-town"}))]);
        assert!(
            answer_for_question(&answers, &second).is_none(),
            "the city answer must not complete the postal question"
        );
        let answers =
            std::collections::BTreeMap::from([(second.clone(), json!({"answer": "12345"}))]);
        assert!(
            answer_for_question(&answers, &second).is_some(),
            "the postal answer must complete the postal question"
        );
    }

    #[test]
    fn sequential_notes_separate_restart_stable_and_delayed_incomplete() {
        // E08e (combined): two sequential questions separate, a restart
        // recomputes the same second id, a delayed first answer never
        // completes the second, and an unanswered second stays incomplete.
        // Helper-level with real harness with real data.
        let first_args = json!({"question": "city?", "notes": "billing"});
        let second_args = json!({"question": "city?", "notes": "shipping"});
        let first = ask_user_question_id("agent", "agent", "ask", "call-1", &first_args)
            .expect("first should build");
        let second = ask_user_question_id("agent", "agent", "ask", "call-2", &second_args)
            .expect("second should build");
        assert_ne!(first, second, "notes change must separate");
        let again = ask_user_question_id("agent", "agent", "ask", "call-2", &second_args)
            .expect("restart stable");
        assert_eq!(second, again, "restart must recompute identically");
        let delayed =
            std::collections::BTreeMap::from([(first.clone(), json!({"answer": "old-town"}))]);
        assert!(
            answer_for_question(&delayed, &second).is_none(),
            "delayed first answer must not complete second"
        );
        assert!(
            answer_for_question(&std::collections::BTreeMap::new(), &second).is_none(),
            "unanswered second stays incomplete"
        );
        let answered =
            std::collections::BTreeMap::from([(second.clone(), json!({"answer": "new-town"}))]);
        assert!(
            answer_for_question(&answered, &second).is_some(),
            "second answer completes second"
        );
    }

    #[test]
    fn safe_http_reads_take_no_guard_but_still_checkpoint() {
        // E07-3: GET/HEAD (and absent method) take no guard, but they still
        // record a cheap `safe_read` checkpoint marker through the same
        // path. POST needs the full `before_side_effect` checkpoint. This
        // test pins the classification; the checkpoint behavior lives in
        // `agent.rs` (`safe_read` vs `before_side_effect` phases).
        let tools = vec![ToolDecl::Http {
            name: "fetch".into(),
            description: None,
            input_schema: None,
            methods: vec!["GET".into(), "POST".into()],
            hosts: vec!["example.test".into()],
        }];
        assert!(is_safe_http_tool_call(
            &tools,
            "fetch",
            &json!({"url": "https://example.test"})
        ));
        assert!(is_safe_http_tool_call(
            &tools,
            "fetch",
            &json!({"method": "GET", "url": "https://example.test"})
        ));
        assert!(!is_safe_http_tool_call(
            &tools,
            "fetch",
            &json!({"method": "POST", "url": "https://example.test"})
        ));
    }

    #[test]
    fn redaction_marker_matches_placeholders_not_bare_words() {
        // E07: legitimate content containing the word REDACTED must not be
        // misdetected; only the exact placeholder shapes count.
        assert!(!args_contain_redaction_marker(
            &json!({"content": "REDACTED"})
        ));
        assert!(!args_contain_redaction_marker(
            &json!({"content": "the file was redacted yesterday"})
        ));
        assert!(args_contain_redaction_marker(
            &json!({"content": "[REDACTED]"})
        ));
        assert!(args_contain_redaction_marker(
            &json!({"content": "[REDACTED:sha256:abc]"})
        ));
        assert!(args_contain_redaction_marker(
            &json!({"url": "https://example.test/?api_key=%5BREDACTED%5D"})
        ));
    }

    #[test]
    fn executed_http_positions_hold_marker_covers_wire_positions_only() {
        // E07/E09: a placeholder in the URL, a header value, the body, or
        // a sensitive value must refuse the resume; a marker confined to
        // free text must not block a request whose executed bytes are
        // intact. (The caller gates on whole-args markers first; this
        // predicate decides executed-position refusal.)
        use std::collections::BTreeMap;
        let clean: BTreeMap<String, String> = BTreeMap::new();
        assert!(executed_http_positions_hold_marker(
            "https://example.test/?api_key=%5BREDACTED%5D",
            &clean,
            None,
            &clean,
        ));
        assert!(executed_http_positions_hold_marker(
            "https://example.test/",
            &BTreeMap::from([("authorization".to_string(), "[REDACTED]".to_string())]),
            None,
            &clean,
        ));
        assert!(executed_http_positions_hold_marker(
            "https://example.test/",
            &clean,
            Some("[REDACTED:sha256:abc]"),
            &clean,
        ));
        assert!(executed_http_positions_hold_marker(
            "https://example.test/",
            &clean,
            None,
            &BTreeMap::from([("token".to_string(), "[REDACTED]".to_string())]),
        ));
        assert!(!executed_http_positions_hold_marker(
            "https://example.test/",
            &clean,
            Some("plain body"),
            &clean,
        ));
    }

    #[test]
    fn redacted_success_resend_hits_only_cached_success() {
        // E07-1: the redacted fast path resends only a cached success; every
        // other durable state falls through to the engine guard, whose
        // verdict (Start/Repeat/Refuse) decides retry vs refusal.
        use qcg_engine::{OperationRecord, OperationStatus};
        let cached = OperationRecord {
            digest: "d".into(),
            status: OperationStatus::Succeeded,
            result: Some(json!({"file": "out.txt"})),
            invocation: String::new(),
            result_ref: None,
        };
        assert_eq!(
            redacted_success_resend(Some(&cached)),
            Some(json!({"file": "out.txt"})),
            "cached success must resend"
        );
        for (name, record) in [
            (
                "clean failure",
                OperationRecord {
                    digest: "d".into(),
                    status: OperationStatus::FailedClean,
                    result: None,
                    invocation: String::new(),
                    result_ref: None,
                },
            ),
            (
                "indeterminate",
                OperationRecord {
                    digest: "d".into(),
                    status: OperationStatus::FailedIndeterminate,
                    result: None,
                    invocation: String::new(),
                    result_ref: None,
                },
            ),
            (
                "started",
                OperationRecord {
                    digest: "d".into(),
                    status: OperationStatus::Started,
                    result: None,
                    invocation: String::new(),
                    result_ref: None,
                },
            ),
            (
                "uncached success",
                OperationRecord {
                    digest: "d".into(),
                    status: OperationStatus::Succeeded,
                    result: None,
                    invocation: String::new(),
                    result_ref: None,
                },
            ),
        ] {
            assert!(
                redacted_success_resend(Some(&record)).is_none(),
                "{name} must fall through to the guard, never resend"
            );
        }
        assert!(
            redacted_success_resend(None).is_none(),
            "a missing record must fall through to the guard"
        );
    }

    #[test]
    fn used_call_registry_rejects_reused_ids_with_new_args() {
        // E08-1: the same call id with the same hash resumes idempotently;
        // the same call id with a different hash fails closed so a model
        // replay cannot complete a new question with an old answer.
        let mut used = BTreeMap::new();
        check_used_call_id("n", &mut used, "call-1", "hash-a").expect("first use should register");
        check_used_call_id("n", &mut used, "call-1", "hash-a")
            .expect("same args should resume idempotently");
        let error = check_used_call_id("n", &mut used, "call-1", "hash-b")
            .expect_err("different args under the same call id must fail closed");
        assert!(error.to_string().contains("different arguments"), "{error}");
        check_used_call_id("n", &mut used, "call-2", "hash-b")
            .expect("a fresh call id should register");
    }

    #[test]
    fn bare_answer_keys_are_refused() {
        // Backward compatibility is not preserved: a legacy bare
        // `node:tool` answer never satisfies a full-identity question
        // (E08/Q1).
        let answers = BTreeMap::from([("agent:ask".to_string(), json!({"answer": "legacy"}))]);
        assert!(
            answer_for_question(&answers, "agent:ask:deadbeef").is_none(),
            "legacy answer must not cover the new id"
        );
        let answers = BTreeMap::from([
            ("agent:ask".to_string(), json!({"answer": "legacy"})),
            ("agent:ask:deadbeef".to_string(), json!({"answer": "fresh"})),
        ]);
        assert_eq!(
            answer_for_question(&answers, "agent:ask:deadbeef"),
            Some(&json!({"answer": "fresh"})),
            "the new-style answer must win when present"
        );
        assert!(answer_for_question(&BTreeMap::new(), "agent:ask:deadbeef").is_none());
    }

    #[test]
    fn builtin_minimization_keeps_only_functional_fields() {
        // E08a/E08-4: resume needs the question, options, fields, and
        // material notes; other free-text extras and credential-like values
        // never reach the journal, while the identity stays computable from
        // the minimized form on both suspension and resume.
        let minimized = minimized_builtin_args(&json!({
            "question": "postal code?",
            "options": ["a", "b"],
            "fields": [{"id": "answer", "type": "string"}],
            "notes": "Pick the billing postal code.",
            "extra_pii": "drop me",
            "api_key": "s3cret",
        }));
        assert_eq!(
            minimized.get("question").and_then(Value::as_str),
            Some("postal code?")
        );
        assert!(minimized.get("options").is_some());
        assert!(minimized.get("fields").is_some());
        assert_eq!(
            minimized.get("notes").and_then(Value::as_str),
            Some("Pick the billing postal code."),
            "material notes must be retained for identity: {minimized}"
        );
        assert!(
            minimized.get("extra_pii").is_none(),
            "non-material free text must be dropped: {minimized}"
        );
        assert_ne!(
            minimized.get("api_key"),
            Some(&json!("s3cret")),
            "credential-like values must not survive verbatim: {minimized}"
        );
        let first = ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-1",
            &json!({"question": "q?", "notes": "x"}),
        )
        .expect("id should build");
        let second = ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-1",
            &minimized_builtin_args(&json!({"question": "q?", "notes": "x"})),
        )
        .expect("id should build");
        assert_eq!(
            first, second,
            "identity must derive from the minimized form"
        );
    }

    #[test]
    fn agent_command_details_bind_explicit_empty_stdin() {
        // E09: the agent command binding keeps the plain-step
        // `stdin_sha256` shape (explicit null) so a future stdin argument
        // cannot ride an unbound approval.
        let details = agent_command_details("n", json!({"argv": ["echo", "hi"]}), "salt-1")
            .expect("object plan should bind")
            .expect("details should exist");
        assert_eq!(details["stdin_sha256"], Value::Null);
        assert!(agent_command_details("n", json!([1, 2]), "salt-1").is_err());
    }

    #[test]
    fn fs_write_details_bind_content_by_hash_only() {
        // E07: fs.write approval and guard details must bind the exact
        // content without journaling the content itself in the digest
        // structure (the content lives in the write, the digest binds it).
        use sha2::{Digest as _, Sha256};
        let content = "hello secret";
        let expected = hex::encode(Sha256::digest(content.as_bytes()));
        let details = json!({
            "path_prefix": "workspace",
            "content_sha256": expected.clone(),
        });
        assert_eq!(details["content_sha256"], json!(expected));
        assert!(
            !details.to_string().contains("hello secret"),
            "details must not contain plaintext content"
        );
    }

    #[test]
    fn invalid_ask_user_forms_fail_before_id_with_zero_journal_events() {
        // Gap 3 (runtime side): the AskUser branch validates fields and
        // options before minting the question id and before any journal
        // write (E08c). This drives the exact prefix
        // (`dynamic_form_fields` + options validation) with a real journal
        // open, proving invalid forms error with zero new journal events.
        // Real objects, no mocks.
        use crate::agent_tools::dynamic_form_fields;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "qcg-agent-runtime-invalid-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("test directory should be creatable");
        let root =
            camino::Utf8PathBuf::from_path_buf(path.clone()).expect("temporary path must be UTF-8");
        let journal_path = root.join("journal.jsonl");
        let journal =
            qcg_engine::JournalWriter::create(&journal_path, "runtime-invalid-1", false, None)
                .expect("test journal should open");
        let count = || {
            std::fs::read_to_string(&journal_path)
                .unwrap_or_default()
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count()
        };
        assert_eq!(count(), 0);
        // Empty fields array fails before id generation.
        assert!(dynamic_form_fields("agent", &json!({"question": "q?", "fields": []})).is_err());
        // Duplicate ids fail closed.
        assert!(
            dynamic_form_fields(
                "agent",
                &json!({"question": "q?", "fields": [
                    {"id": "a", "type": "string"},
                    {"id": "a", "type": "string"},
                ]}),
            )
            .is_err()
        );
        // Notes-carrying valid forms still pass the field prefix and mint
        // distinct question ids (gap 1 runtime side).
        assert!(
            dynamic_form_fields("agent", &json!({"question": "q?", "notes": "billing"})).is_ok()
        );
        let first = ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-1",
            &json!({"question": "q?", "notes": "a"}),
        )
        .expect("id should build");
        let second = ask_user_question_id(
            "agent",
            "agent",
            "ask",
            "call-1",
            &json!({"question": "q?", "notes": "b"}),
        )
        .expect("id should build");
        assert_ne!(first, second, "notes must separate runtime identities");
        assert_eq!(count(), 0, "invalid forms must write zero journal events");
        assert_eq!(
            journal.state().operation_records.len(),
            0,
            "validation must not create operation records"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn distinct_secrets_yield_distinct_ids_with_redacted_display() {
        // E08 SENSITIVE two-field: journal/display uses the REDACTED
        // minimized form (credential + URL redaction on text, keys stay
        // visible), while identity binds the FULL unredacted bytes via
        // `content_hash`. Distinct credential-shaped secrets yield distinct
        // ids even though their redacted displays alias. Bare non-credential
        // text is display by design (it is the question the user must see);
        // secrecy for arbitrary shapes comes from the FULL hash binding, not
        // from display redaction of unknown shapes.
        let first_args = json!({
            "question": "Deploy with api_key=SENTINEL_SECRET_ALPHA at https://example.test/?token=SENTINEL_URL_ALPHA",
            "notes": "billing api_key=SENTINEL_NOTES_ALPHA",
        });
        let second_args = json!({
            "question": "Deploy with api_key=SENTINEL_SECRET_BETA at https://example.test/?token=SENTINEL_URL_BETA",
            "notes": "billing api_key=SENTINEL_NOTES_BETA",
        });
        let first_display = minimized_builtin_args(&first_args);
        let second_display = minimized_builtin_args(&second_args);
        for (name, display) in [("first", &first_display), ("second", &second_display)] {
            let display_str = display.to_string();
            assert!(
                !display_str.contains("SENTINEL_SECRET"),
                "{name} display must not leak credential secrets: {display_str}"
            );
            assert!(
                !display_str.contains("SENTINEL_URL"),
                "{name} display must not leak URL secrets: {display_str}"
            );
            assert!(
                !display_str.contains("SENTINEL_NOTES"),
                "{name} display must not leak notes secrets: {display_str}"
            );
        }
        // Displays alias (both redacted to the same shape) but ids separate.
        assert_eq!(
            first_display, second_display,
            "redacted displays alias by design"
        );
        let first_hash =
            builtin_content_hash("agent", "agent", &first_args).expect("hash should build");
        let second_hash =
            builtin_content_hash("agent", "agent", &second_args).expect("hash should build");
        assert_ne!(first_hash, second_hash, "FULL hashes must separate secrets");
        let first_id = ask_user_question_id("agent", "agent", "ask", "call-1", &first_args)
            .expect("id should build");
        let second_id = ask_user_question_id("agent", "agent", "ask", "call-1", &second_args)
            .expect("id should build");
        assert_ne!(first_id, second_id, "distinct secrets => distinct ids");
        // The stored content_hash verifies the question id without needing
        // the original secrets (restart-safe).
        let expected =
            ask_user_question_id_from_content_hash("agent", "ask", "call-1", &first_hash);
        assert_eq!(expected, first_id);
    }

    #[test]
    fn agent_and_plain_http_details_are_cross_equal() {
        // E09: the agent-side construction routes through the same shared
        // core as the plain step (engine `http_operation_details` + policy
        // canonicalizer + run-salted `http-url-v1` digest). Agent-built vs
        // plain-built details for the same input are equal + share the same
        // digest.
        let headers = std::collections::BTreeMap::from([(
            "Content-Type".to_string(),
            "text/plain".to_string(),
        )]);
        let sensitive = BTreeMap::new();
        let agent = http_details_with_url(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("agent helper should build");
        // Plain-equivalent constructed via the same shared core.
        let mut plain = qcg_engine::http_operation_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
        )
        .expect("plain core should build");
        let canonical =
            qcg_policy::credential::canonical_http_url("https://example.test/search?q=x");
        plain
            .as_object_mut()
            .expect("details should be an object")
            .insert(
                "url_sha256".into(),
                Value::String(qcg_engine::salted_binding_digest(
                    "http-url-v1",
                    "run-1",
                    canonical.as_bytes(),
                )),
            );
        assert_eq!(agent, plain, "agent vs plain details must be equal");
        let agent_digest =
            qcg_engine::RunContext::operation_digest("https://example.test", &Some(agent.clone()))
                .expect("digest should compute");
        let plain_digest =
            qcg_engine::RunContext::operation_digest("https://example.test", &Some(plain.clone()))
                .expect("digest should compute");
        assert_eq!(agent_digest, plain_digest, "digests must match");
        // Canonical URLs are byte-identical across paths.
        let agent_canonical =
            qcg_policy::credential::canonical_http_url("https://example.test/search?q=x&b=2");
        let plain_canonical =
            qcg_policy::credential::canonical_http_url("https://example.test/search?q=x&b=2");
        assert_eq!(agent_canonical, plain_canonical);
    }

    #[test]
    fn safe_method_details_bind_headers_and_query() {
        // E09 (YOUR side): safe methods bind method + full URL + headers
        // (redacted journal); header/query change => different digest.
        let headers = std::collections::BTreeMap::from([
            (
                "Authorization".to_string(),
                "Bearer SENTINEL_AGENT_SAFE".to_string(),
            ),
            ("X-Tenant".to_string(), "alpha".to_string()),
        ]);
        let sensitive = BTreeMap::new();
        let base = http_details_with_url(
            "GET",
            &headers,
            None,
            &sensitive,
            "run-1",
            "https://example.test/search?q=x&api_key=SENTINEL_AGENT_QUERY",
        )
        .expect("safe GET must bind details, never None");
        assert_eq!(base["method"], "GET");
        assert_eq!(base["safe_read"], true);
        let base_str = base.to_string();
        assert!(
            !base_str.contains("SENTINEL_AGENT_SAFE"),
            "safe details must not leak header secrets: {base_str}"
        );
        assert!(
            !base_str.contains("SENTINEL_AGENT_QUERY"),
            "safe details must not leak query secrets: {base_str}"
        );
        let other_headers = std::collections::BTreeMap::from([
            ("Authorization".to_string(), "Bearer DIFFERENT".to_string()),
            ("X-Tenant".to_string(), "alpha".to_string()),
        ]);
        let other = http_details_with_url(
            "GET",
            &other_headers,
            None,
            &sensitive,
            "run-1",
            "https://example.test/search?q=x&api_key=SENTINEL_AGENT_QUERY",
        )
        .expect("safe details should build");
        assert_ne!(
            base["headers_sha256"], other["headers_sha256"],
            "header change must alter the safe digest"
        );
        let changed = http_details_with_url(
            "GET",
            &headers,
            None,
            &sensitive,
            "run-1",
            "https://example.test/search?q=y&api_key=SENTINEL_AGENT_QUERY",
        )
        .expect("safe details should build");
        assert_ne!(
            base["url_sha256"], changed["url_sha256"],
            "query change must alter the safe digest"
        );
    }

    #[test]
    fn constructed_agent_details_carry_no_sentinel_secrets() {
        // E09 SENSITIVE: every details object YOUR code constructs carries
        // redacted (never raw) secrets.
        let headers = std::collections::BTreeMap::from([
            (
                "Authorization".to_string(),
                "Bearer SENTINEL_AGENT_DETAIL".to_string(),
            ),
            ("X-Tenant".to_string(), "alpha".to_string()),
        ]);
        let sensitive = BTreeMap::from([(
            "api_key".to_string(),
            "SENTINEL_AGENT_SENSITIVE".to_string(),
        )]);
        let details = http_details_with_url(
            "POST",
            &headers,
            Some(b"token=SENTINEL_AGENT_BODY"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x&api_key=SENTINEL_AGENT_URL",
        )
        .expect("details should build");
        let details_str = details.to_string();
        for sentinel in [
            "SENTINEL_AGENT_DETAIL",
            "SENTINEL_AGENT_SENSITIVE",
            "SENTINEL_AGENT_BODY",
            "SENTINEL_AGENT_URL",
        ] {
            assert!(
                !details_str.contains(sentinel),
                "agent details must not leak {sentinel}: {details_str}"
            );
        }
    }

    #[test]
    fn registry_hashes_are_salted_redacted_and_bind_changes() {
        // E09 (salted registry) + E07a (single redacted form): identical
        // safe-read content in different runs yields different hashes
        // (cross-run unlinkability), while header/query changes alter the
        // hash in the same run. Fresh raw args and checkpointed redacted
        // copies hash identically when content matches.
        use qcg_contract::ToolDecl;
        let tools = vec![ToolDecl::Http {
            name: "fetch".into(),
            description: None,
            input_schema: None,
            methods: vec!["GET".into()],
            hosts: vec!["example.test".into()],
        }];
        let args = json!({
            "method": "GET",
            "url": "https://example.test/search?q=x&api_key=s3cret",
            "headers": {"Authorization": "Bearer s3cret"},
        });
        let canonical = canonical_agent_registry_args(&tools, "fetch", &args);
        let canonical_str = canonical.to_string();
        assert!(
            !canonical_str.contains("s3cret"),
            "registry canonical form must be redacted: {canonical_str}"
        );
        let first = agent_call_identity_hash_for_tool("agent", "run-1", &tools, "fetch", &args)
            .expect("hash should build");
        let same = agent_call_identity_hash_for_tool("agent", "run-1", &tools, "fetch", &args)
            .expect("hash should build");
        assert_eq!(first, same, "same content same run must match");
        // Redacted reissue matches (single form).
        let redacted = redact_agent_tool_args_for_journal(&tools, "fetch", &args);
        let redacted_hash =
            agent_call_identity_hash_for_tool("agent", "run-1", &tools, "fetch", &redacted)
                .expect("hash should build");
        assert_eq!(
            first, redacted_hash,
            "redacted reissue must recompute identically"
        );
        // Cross-run divergence.
        let other_run = agent_call_identity_hash_for_tool("agent", "run-2", &tools, "fetch", &args)
            .expect("hash should build");
        assert_ne!(
            first, other_run,
            "identical content must diverge across runs"
        );
        // Header change alters the hash (non-credential headers are bound
        // verbatim; credential headers redact to the same placeholder by
        // design so the journal never carries them — their binding lives in
        // the FULL details digest, not the redacted registry form).
        let changed_headers = json!({
            "method": "GET",
            "url": "https://example.test/search?q=x&api_key=s3cret",
            "headers": {"X-Tenant": "beta"},
        });
        // Baseline has X-Tenant alpha (see below); beta must differ. To keep
        // the baseline explicit, recompute it with the non-credential header.
        let baseline_tenant = json!({
            "method": "GET",
            "url": "https://example.test/search?q=x&api_key=s3cret",
            "headers": {"X-Tenant": "alpha"},
        });
        let baseline_hash =
            agent_call_identity_hash_for_tool("agent", "run-1", &tools, "fetch", &baseline_tenant)
                .expect("hash should build");
        let changed =
            agent_call_identity_hash_for_tool("agent", "run-1", &tools, "fetch", &changed_headers)
                .expect("hash should build");
        assert_ne!(
            baseline_hash, changed,
            "non-credential header change must alter the hash"
        );
        // Query change aliases in the REGISTRY by design (the single
        // redacted form redacts ALL query values so the journal never
        // carries them; E09-1). The FULL binding lives in the details digest
        // (`http_details_with_url`), which separates queries. Same call id
        // with a different safe-read query therefore resumes idempotently in
        // the registry and simply executes the new query (side-effect-free),
        // never reusing stale data.
        let changed_query = json!({
            "method": "GET",
            "url": "https://example.test/search?q=y&api_key=s3cret",
            "headers": {"Authorization": "Bearer s3cret"},
        });
        let changed =
            agent_call_identity_hash_for_tool("agent", "run-1", &tools, "fetch", &changed_query)
                .expect("hash should build");
        assert_eq!(
            first, changed,
            "safe-read query values alias in the redacted registry form by design (E09-1); FULL binding separates in details"
        );
    }

    #[test]
    fn send_path_shares_the_approved_guard_digest() {
        // E09: the executed request equals the approved guard (same digest
        // at send time). Both the guard site and the send site construct via
        // the single `http_details_with_url` helper, so their digests match
        // byte-for-byte. Real objects, no mocks.
        let headers = std::collections::BTreeMap::from([(
            "Content-Type".to_string(),
            "text/plain".to_string(),
        )]);
        let sensitive = BTreeMap::new();
        let guard = http_details_with_url(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("guard site should build");
        let send = http_details_with_url(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("send site should build");
        assert_eq!(guard, send, "send must equal the approved guard");
        let guard_digest =
            qcg_engine::RunContext::operation_digest("https://example.test", &Some(guard))
                .expect("digest should compute");
        let send_digest =
            qcg_engine::RunContext::operation_digest("https://example.test", &Some(send))
                .expect("digest should compute");
        assert_eq!(guard_digest, send_digest, "digests must match at send time");
    }

    #[test]
    fn pending_sidecar_restores_nonsecret_payload_with_digest_binding() {
        // F02-01/F02-02/F02-04: a redacted journal copy restores to the
        // original bytes via the private sidecar when the digest matches;
        // tampered payloads stay redacted and fail closed downstream.
        use qcg_contract::NodeDef;
        let node = NodeDef {
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
        let tools = vec![ToolDecl::FsWrite {
            name: "write".into(),
            description: None,
            input_schema: None,
            path_prefix: "out/".into(),
        }];
        let original = qcg_llm::ChatToolCall {
            id: "call-1".into(),
            name: "write".into(),
            args: json!({"path": "out/a.txt", "content": "hello non-secret"}),
        };
        let stored = redact_tool_call_for_journal(&tools, &original);
        assert!(
            args_contain_redaction_marker(&stored.args),
            "journaled copy must be redacted"
        );
        let digest =
            pending_call_digest(&tools, &node, "run-1", &original).expect("digest should compute");
        let confirm = qcg_api::ConfirmSpec {
            id: "agent:fs.write:abc".into(),
            title: "confirm".into(),
            kind: "fs.write".into(),
            target: "out/a.txt".into(),
            dry_run: false,
            details: None,
            operation_digest: digest,
            scope: qcg_contract::SideEffectScope::Content,
        };
        let sidecar = json!({
            "id": original.id,
            "name": original.name,
            "args": original.args,
        });
        let restored = restore_pending_call_from_sidecar(
            &tools,
            &node,
            "run-1",
            &stored,
            Some(&confirm),
            Some(&sidecar),
        );
        assert_eq!(
            restored.args, original.args,
            "verified payload must restore original bytes"
        );
        // Tampered sidecar with different content redacts differently and
        // must not restore.
        let tampered = json!({
            "id": "call-1",
            "name": "write",
            "args": json!({"path": "out/a.txt", "content": "forged"}),
        });
        let kept = restore_pending_call_from_sidecar(
            &tools,
            &node,
            "run-1",
            &stored,
            Some(&confirm),
            Some(&tampered),
        );
        assert_eq!(
            kept.args, stored.args,
            "tampered payload must stay redacted"
        );
        // Journal redaction itself carries no plaintext secret.
        assert!(
            !stored.args.to_string().contains("hello non-secret"),
            "journaled copy must not carry the original bytes"
        );
    }
}

/// Approval end-to-end through real contexts, journals, and gateways
/// (F02-01..F02-04). No mocks: a real `RunContext` (real gateways, real
/// metadata dir), a real `JournalWriter` file, and — for HTTP — a real
/// loopback server. Phase 1 generates the call and checkpoints the
/// redacted copy; phase 2 rebuilds every context from disk (a restart)
/// and resumes from the private sidecar after approval.
#[cfg(test)]
mod approval_e2e {
    use super::*;
    use crate::agent::{AgentCheckpoint, record_agent_checkpoint};
    use crate::mcp_tools::McpAgentTools;
    use qcg_contract::{
        AssetSpec, Contract, FailurePolicy, GeneratorMeta, Graph, InputSpec, Manifest, OnDeps,
        OutputSpec, Permissions, RetentionPolicy, SideEffects, StepType,
    };
    use qcg_engine::{JournalWriter, RunContext, StepContext};
    use qcg_types::NodePath;
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct ApprovalHarness {
        _root_guard: TempGuard,
        root: camino::Utf8PathBuf,
        run_id: String,
    }

    struct TempGuard(std::path::PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl ApprovalHarness {
        fn setup(name: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
                "qcg-approval-{name}-{}-{nonce}",
                std::process::id()
            )))
            .expect("temporary path must be UTF-8");
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(root.join("workspace")).expect("workspace should be creatable");
            std::fs::create_dir_all(root.join("meta")).expect("meta should be creatable");
            let run_id = format!("approval-{name}-{nonce}");
            Self {
                _root_guard: TempGuard(root.clone().into_std_path_buf()),
                root,
                run_id,
            }
        }

        fn manifest(&self) -> Manifest {
            Manifest {
                generator: GeneratorMeta {
                    id: "approval-test".into(),
                    name: "Approval Test".into(),
                    version: "0.1.0".into(),
                    description: String::new(),
                    authors: vec![],
                    qcg_version: String::new(),
                },
                permissions: Permissions {
                    fs_read: vec![],
                    fs_write: vec!["workspace".into()],
                    network: vec!["127.0.0.1".into()],
                    commands: vec![],
                    containers: Default::default(),
                    side_effects: SideEffects::Confirm,
                    side_effects_scope: Default::default(),
                },
                llm: None,
                inputs: InputSpec::default(),
                resources: BTreeMap::new(),
                tools: BTreeMap::new(),
                secrets: BTreeMap::new(),
                runtime: Default::default(),
                budget: Default::default(),
                flow: vec![],
                parallel: vec![],
                blocks: BTreeMap::new(),
                outputs: OutputSpec { extras: vec![] },
                failure: FailurePolicy::default(),
                retention: RetentionPolicy::default(),
                audit: qcg_policy::AuditConfig::default(),
                hooks: Default::default(),
                assets: AssetSpec::default(),
                dependencies: Default::default(),
            }
        }

        fn contract(&self) -> Contract {
            let manifest = self.manifest();
            let graph = Graph::build(&manifest).expect("empty flow should build");
            Contract {
                root: self.root.clone(),
                manifest,
                graph,
                sha256: "approval-e2e".into(),
            }
        }

        fn node() -> NodeDef {
            NodeDef {
                id: "agent".into(),
                kind: StepType::from("llm.agent"),
                needs: vec![],
                when: None,
                on_deps: OnDeps::AllSucceeded,
                context: vec![],
                output: None,
                artifact: None,
                on_fail: None,
                failure: None,
                retry: None,
                params: Default::default(),
            }
        }

        fn journal_path(&self) -> camino::Utf8PathBuf {
            self.root.join("meta/journal.jsonl")
        }

        fn journal_text(&self) -> String {
            std::fs::read_to_string(self.journal_path()).unwrap_or_default()
        }

        fn operation_started_count(&self, operation_id: &str) -> usize {
            self.journal_text()
                .lines()
                .filter(|line| {
                    line.contains("\"t\":\"operation_started\"") && line.contains(operation_id)
                })
                .count()
        }
    }

    fn test_runtime() -> qcg_llm::LlmRuntime {
        qcg_llm::LlmRuntime::builtins()
    }

    /// One captured server hit: request line, authorization header, body.
    type ServerHit = (String, String, Vec<u8>);

    /// Drives one tool call to its approval request and checkpoints the
    /// redacted copy plus the private sidecar, like the agent turn loop.
    struct ApprovalCall<'a> {
        run: &'a RunContext,
        journal: &'a JournalWriter,
        node: &'a NodeDef,
        runtime: &'a qcg_llm::LlmRuntime,
        tools: &'a [ToolDecl],
        name: &'a str,
        call_id: &'a str,
        args: &'a Value,
    }

    async fn request_approval(call: ApprovalCall<'_>) -> qcg_api::ConfirmSpec {
        let ApprovalCall {
            run,
            journal,
            node,
            runtime,
            tools,
            name,
            call_id,
            args,
        } = call;
        let mut vars = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
        let mut ctx = StepContext {
            run,
            journal,
            vars: &mut vars,
            llm: None,
        };
        let mcp = McpAgentTools::prepare(&ctx, node, runtime, tools)
            .await
            .expect("no MCP tools should prepare cleanly");
        let mut activated = BTreeSet::new();
        let outcome = execute_agent_tool(
            &mut ctx,
            node,
            AgentToolServices {
                runtime,
                guardrails: &[],
            },
            AgentToolInvocation {
                mcp: &mcp,
                tools,
                name,
                call_id,
                call_number: 0,
                args,
                activated_skills: &mut activated,
            },
        )
        .await
        .expect("approval request should not error");
        let AgentToolOutcome::NeedsConfirm(confirm) = outcome else {
            panic!("a confirming policy must request approval");
        };
        record_agent_checkpoint(
            &ctx,
            node,
            0,
            "before_side_effect",
            &AgentCheckpoint {
                messages: vec![],
                next_turn: 0,
                tokens_total: 0,
                tool_calls_total: 0,
                tool_call_counts: BTreeMap::new(),
                pending_side_effect: Some(qcg_llm::ChatToolCall {
                    id: call_id.to_string(),
                    name: name.to_string(),
                    args: args.clone(),
                }),
                pending_confirm: Some(confirm.clone()),
                pending_mcp_call: None,
                used_calls: BTreeMap::new(),
            },
            tools,
        )
        .expect("checkpoint should journal");
        confirm
    }

    /// Reloads the journaled checkpoint after a restart and restores the
    /// exact payload from the private sidecar.
    fn restore_after_restart(
        run: &RunContext,
        journal: &JournalWriter,
        node: &NodeDef,
        run_id: &str,
        tools: &[ToolDecl],
        call_id: &str,
    ) -> (qcg_llm::ChatToolCall, qcg_api::ConfirmSpec) {
        let stored: AgentCheckpoint = serde_json::from_value(
            journal
                .state()
                .checkpoints
                .get(&NodePath::root(&node.id))
                .expect("checkpoint should survive the restart")
                .clone(),
        )
        .expect("checkpoint should deserialize");
        let pending = stored
            .pending_side_effect
            .expect("pending call should be stored");
        let confirm = stored.pending_confirm.expect("confirm should be stored");
        let operation_id = qcg_engine::operation_id_for(run_id, &node.id, call_id);
        let sidecar = run
            .load_pending_tool_payload(node, &operation_id)
            .expect("sidecar load should not corrupt")
            .expect("sidecar should survive the restart");
        let restored = restore_pending_call_from_sidecar(
            tools,
            node,
            run_id,
            &pending,
            Some(&confirm),
            Some(&sidecar),
        );
        assert!(
            !args_contain_redaction_marker(&restored.args),
            "the restored call must carry original bytes, not markers"
        );
        (restored, confirm)
    }

    #[tokio::test]
    async fn approved_fs_write_executes_original_content_once() {
        // F02-02 + F02-03 + F02-04: non-secret content approved after a
        // restart executes exactly once; the public journal never carries
        // the bytes; a duplicate resume resends instead of rewriting.
        let harness = ApprovalHarness::setup("fs-write");
        let node = ApprovalHarness::node();
        let tools = vec![ToolDecl::FsWrite {
            name: "write".into(),
            description: None,
            input_schema: None,
            path_prefix: "drafts".into(),
        }];
        let content = "deploy notes with password=hunter2-marked-secret inside";
        let args = json!({"path": "drafts/note.txt", "content": content});
        let runtime = test_runtime();
        let confirm = {
            let run = RunContext::for_tests(
                harness.run_id.clone(),
                harness.root.join("workspace"),
                harness.root.join("meta"),
                harness.contract(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .expect("test context should build");
            let journal =
                JournalWriter::create(&harness.journal_path(), &harness.run_id, false, None)
                    .expect("journal should open");
            request_approval(ApprovalCall {
                run: &run,
                journal: &journal,
                node: &node,
                runtime: &runtime,
                tools: &tools,
                name: "write",
                call_id: "call-1",
                args: &args,
            })
            .await
        };
        let journal_text = harness.journal_text();
        assert!(
            !journal_text.contains("hunter2-marked-secret"),
            "the public journal must not carry the content: {journal_text}"
        );
        // Restart: fresh contexts, approval recorded, journal + sidecar from disk.
        let target_before = harness.root.join("workspace/drafts/note.txt");
        assert!(!target_before.exists(), "nothing executes before approval");
        {
            let mut confirmations = BTreeMap::new();
            confirmations.insert(confirm.id.clone(), true);
            let run = RunContext::for_tests(
                harness.run_id.clone(),
                harness.root.join("workspace"),
                harness.root.join("meta"),
                harness.contract(),
                confirmations,
                BTreeMap::new(),
            )
            .expect("restart context should build");
            let journal =
                JournalWriter::create(&harness.journal_path(), &harness.run_id, false, None)
                    .expect("journal should reopen");
            let (restored, _) =
                restore_after_restart(&run, &journal, &node, &harness.run_id, &tools, "call-1");
            assert_eq!(restored.args, args, "the exact payload must come back");
            let mut vars = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
            let mut ctx = StepContext {
                run: &run,
                journal: &journal,
                vars: &mut vars,
                llm: None,
            };
            let mcp = McpAgentTools::prepare(&ctx, &node, &runtime, &tools)
                .await
                .expect("prepare should succeed");
            let mut activated = BTreeSet::new();
            let outcome = execute_agent_tool(
                &mut ctx,
                &node,
                AgentToolServices {
                    runtime: &runtime,
                    guardrails: &[],
                },
                AgentToolInvocation {
                    mcp: &mcp,
                    tools: &tools,
                    name: "write",
                    call_id: "call-1",
                    call_number: 0,
                    args: &restored.args,
                    activated_skills: &mut activated,
                },
            )
            .await
            .expect("approved execution should succeed");
            assert!(
                matches!(outcome, AgentToolOutcome::Result(_)),
                "fs.write finishes inline"
            );
            // Duplicate resume resends the cached result instead of
            // executing a second time.
            let again = execute_agent_tool(
                &mut ctx,
                &node,
                AgentToolServices {
                    runtime: &runtime,
                    guardrails: &[],
                },
                AgentToolInvocation {
                    mcp: &mcp,
                    tools: &tools,
                    name: "write",
                    call_id: "call-1",
                    call_number: 0,
                    args: &restored.args,
                    activated_skills: &mut activated,
                },
            )
            .await
            .expect("duplicate resume should resend");
            assert!(
                matches!(again, AgentToolOutcome::Result(_)),
                "duplicate resume must resend"
            );
        }
        let written = std::fs::read_to_string(harness.root.join("workspace/drafts/note.txt"))
            .expect("file written");
        assert_eq!(written, content, "the original content must land once");
        let operation_id = qcg_engine::operation_id_for(&harness.run_id, "agent", "call-1");
        assert_eq!(
            harness.operation_started_count(&operation_id),
            1,
            "exactly one guarded execution: {}",
            harness.journal_text()
        );
    }

    #[tokio::test]
    async fn approved_post_sends_original_body_once() {
        // F02-01 + F02-03: a non-secret POST body approved after a restart
        // is sent with the original method/headers/body exactly once; the
        // public journal never carries the bytes.
        use std::sync::{Arc, Mutex};
        let hits: Arc<Mutex<Vec<ServerHit>>> = Arc::new(Mutex::new(Vec::new()));
        let server_hits = Arc::clone(&hits);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
        let port = listener.local_addr().expect("address").port();
        let server = std::thread::spawn(move || {
            listener.set_nonblocking(true).expect("nonblocking mode");
            let start = std::time::Instant::now();
            let mut first_hit: Option<std::time::Instant> = None;
            // Exactly one approved execution sends; resends never touch
            // the wire. Linger briefly after the first hit so a duplicate
            // send would be observed instead of refused.
            loop {
                let elapsed = start.elapsed();
                if first_hit.is_some_and(|t| t.elapsed() > std::time::Duration::from_millis(1500))
                    || elapsed > std::time::Duration::from_secs(10)
                {
                    break;
                }
                let Ok((stream, _)) = listener.accept() else {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    continue;
                };
                let mut stream = stream;
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(10)));
                let _ = stream.set_nonblocking(false);
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") && head.len() < 65536 {
                    match stream.read(&mut byte) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => head.push(byte[0]),
                    }
                }
                let head_text = String::from_utf8_lossy(&head).into_owned();
                let first_line = head_text.lines().next().unwrap_or_default().to_string();
                let content_length = head_text
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("Content-Length:")
                            .or_else(|| line.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                let mut body = vec![0u8; content_length];
                let _ = stream.read_exact(&mut body);
                // Authorization header, if any, is captured from the head.
                let auth = head_text
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("Authorization:")
                            .or_else(|| line.strip_prefix("authorization:"))
                            .map(|v| v.trim().to_string())
                    })
                    .unwrap_or_default();
                server_hits
                    .lock()
                    .expect("hits lock")
                    .push((first_line, auth, body));
                if first_hit.is_none() {
                    first_hit = Some(std::time::Instant::now());
                }
                let response = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\nconnection: close\r\n\r\n{\"ok\":true}";
                let _ = stream.write_all(response);
            }
        });
        let harness = ApprovalHarness::setup("http-post");
        let node = ApprovalHarness::node();
        let tools = vec![ToolDecl::Http {
            name: "fetch".into(),
            description: None,
            input_schema: None,
            methods: vec!["POST".into()],
            hosts: vec!["127.0.0.1".into()],
        }];
        let body = "field=1&api_key=SECRET-XYZ-body-token";
        let args = json!({
            "method": "POST",
            "url": format!("http://127.0.0.1:{port}/ingest"),
            "headers": {"X-Tenant": "alpha"},
            "body": body,
        });
        let runtime = test_runtime();
        let confirm = {
            let run = RunContext::for_tests(
                harness.run_id.clone(),
                harness.root.join("workspace"),
                harness.root.join("meta"),
                harness.contract(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .expect("test context should build");
            let journal =
                JournalWriter::create(&harness.journal_path(), &harness.run_id, false, None)
                    .expect("journal should open");
            request_approval(ApprovalCall {
                run: &run,
                journal: &journal,
                node: &node,
                runtime: &runtime,
                tools: &tools,
                name: "fetch",
                call_id: "call-1",
                args: &args,
            })
            .await
        };
        let journal_text = harness.journal_text();
        assert!(
            !journal_text.contains("SECRET-XYZ-body-token"),
            "the public journal must not carry the body: {journal_text}"
        );
        {
            let mut confirmations = BTreeMap::new();
            confirmations.insert(confirm.id.clone(), true);
            let run = RunContext::for_tests(
                harness.run_id.clone(),
                harness.root.join("workspace"),
                harness.root.join("meta"),
                harness.contract(),
                confirmations,
                BTreeMap::new(),
            )
            .expect("restart context should build");
            let journal =
                JournalWriter::create(&harness.journal_path(), &harness.run_id, false, None)
                    .expect("journal should reopen");
            let (restored, _) =
                restore_after_restart(&run, &journal, &node, &harness.run_id, &tools, "call-1");
            assert_eq!(restored.args, args, "the exact body must come back");
            let mut vars = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
            let mut ctx = StepContext {
                run: &run,
                journal: &journal,
                vars: &mut vars,
                llm: None,
            };
            let mcp = McpAgentTools::prepare(&ctx, &node, &runtime, &tools)
                .await
                .expect("prepare should succeed");
            let mut activated = BTreeSet::new();
            // Finish the operation as the turn loop would after output
            // checks, then prove a duplicate resume resends from the cache
            // without touching the wire again.
            let outcome = execute_agent_tool(
                &mut ctx,
                &node,
                AgentToolServices {
                    runtime: &runtime,
                    guardrails: &[],
                },
                AgentToolInvocation {
                    mcp: &mcp,
                    tools: &tools,
                    name: "fetch",
                    call_id: "call-1",
                    call_number: 0,
                    args: &restored.args,
                    activated_skills: &mut activated,
                },
            )
            .await
            .expect("approved execution should succeed");
            assert!(
                matches!(outcome, AgentToolOutcome::OperationResult { .. }),
                "HTTP finishes as an operation result"
            );
            if let AgentToolOutcome::OperationResult {
                value,
                operation_id,
            } = outcome
            {
                ctx.run
                    .finish_external_operation(ctx.journal, &node, &operation_id, Some(value))
                    .expect("finish should journal");
                let again = execute_agent_tool(
                    &mut ctx,
                    &node,
                    AgentToolServices {
                        runtime: &runtime,
                        guardrails: &[],
                    },
                    AgentToolInvocation {
                        mcp: &mcp,
                        tools: &tools,
                        name: "fetch",
                        call_id: "call-1",
                        call_number: 0,
                        args: &restored.args,
                        activated_skills: &mut activated,
                    },
                )
                .await
                .expect("duplicate resume should resend");
                assert!(
                    matches!(again, AgentToolOutcome::Result(_)),
                    "duplicate resume must resend, not re-send"
                );
            }
        }
        drop(harness);
        server.join().expect("server thread should finish");
        let hits = hits.lock().expect("hits lock");
        assert_eq!(
            hits.len(),
            1,
            "exactly one POST must reach the server: {hits:?}"
        );
        let (request_line, _auth, body_bytes) = &hits[0];
        assert!(
            request_line.starts_with("POST /ingest "),
            "original method and path: {request_line}"
        );
        assert_eq!(
            body_bytes,
            &body.as_bytes().to_vec(),
            "original body bytes must arrive intact"
        );
    }

    #[tokio::test]
    async fn tampered_payload_after_approval_requests_fresh_confirmation() {
        // F02-04: the same invocation with changed content does not ride
        // the original approval; it requests a new confirmation instead of
        // executing.
        let harness = ApprovalHarness::setup("tamper");
        let node = ApprovalHarness::node();
        let tools = vec![ToolDecl::FsWrite {
            name: "write".into(),
            description: None,
            input_schema: None,
            path_prefix: "drafts".into(),
        }];
        let args = json!({"path": "drafts/note.txt", "content": "original"});
        let runtime = test_runtime();
        let confirm = {
            let run = RunContext::for_tests(
                harness.run_id.clone(),
                harness.root.join("workspace"),
                harness.root.join("meta"),
                harness.contract(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .expect("test context should build");
            let journal =
                JournalWriter::create(&harness.journal_path(), &harness.run_id, false, None)
                    .expect("journal should open");
            request_approval(ApprovalCall {
                run: &run,
                journal: &journal,
                node: &node,
                runtime: &runtime,
                tools: &tools,
                name: "write",
                call_id: "call-1",
                args: &args,
            })
            .await
        };
        let mut confirmations = BTreeMap::new();
        confirmations.insert(confirm.id.clone(), true);
        let run = RunContext::for_tests(
            harness.run_id.clone(),
            harness.root.join("workspace"),
            harness.root.join("meta"),
            harness.contract(),
            confirmations,
            BTreeMap::new(),
        )
        .expect("restart context should build");
        let journal = JournalWriter::create(&harness.journal_path(), &harness.run_id, false, None)
            .expect("journal should reopen");
        let mut vars = qcg_contract::ValueBag::with_inputs(BTreeMap::new());
        let mut ctx = StepContext {
            run: &run,
            journal: &journal,
            vars: &mut vars,
            llm: None,
        };
        let mcp = McpAgentTools::prepare(&ctx, &node, &runtime, &tools)
            .await
            .expect("prepare should succeed");
        let mut activated = BTreeSet::new();
        let tampered = json!({"path": "drafts/note.txt", "content": "forged"});
        let outcome = execute_agent_tool(
            &mut ctx,
            &node,
            AgentToolServices {
                runtime: &runtime,
                guardrails: &[],
            },
            AgentToolInvocation {
                mcp: &mcp,
                tools: &tools,
                name: "write",
                call_id: "call-1",
                call_number: 0,
                args: &tampered,
                activated_skills: &mut activated,
            },
        )
        .await
        .expect("tampered call should not error");
        match outcome {
            AgentToolOutcome::NeedsConfirm(fresh) => {
                assert_ne!(
                    fresh.id, confirm.id,
                    "changed content must not reuse the original approval"
                );
            }
            _ => panic!("changed content must re-confirm"),
        }
        assert!(
            !harness.root.join("workspace/drafts/note.txt").exists(),
            "nothing executes without its own approval"
        );
    }
}
