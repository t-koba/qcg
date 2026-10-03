use super::*;

pub(super) async fn execute_http(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    name: &str,
    call_id: &str,
    args: &Value,
    methods: &[String],
    hosts: &[String],
) -> Result<AgentToolOutcome, StepError> {
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
        let safe_url = policy::redact_all_query_values(url);
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
    let journal_url = policy::redact_all_query_values(
        &engine::redact_query_parameters(url, &sensitive)
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
        let operation_id = engine::operation_id_for(&ctx.run.run_id, &node.id, call_id);
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
            engine::GuardDecision::Resend { result, .. } => {
                return Ok(AgentToolOutcome::Result(result));
            }
            engine::GuardDecision::Proceed { operation_id } => {
                if !had_record {
                    ctx.run.finish_external_operation_with_warn(
                                ctx.journal,
                                node,
                                &operation_id,
                                engine::OperationOutcome::Indeterminate {
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
                if executed_http_positions_hold_marker(url, &headers, body.as_deref(), &sensitive) {
                    ctx.run.finish_external_operation_with_warn(
                                ctx.journal,
                                node,
                                &operation_id,
                                engine::OperationOutcome::Indeterminate {
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
                engine::GuardDecision::Proceed { operation_id } => operation_id,
                // Same invocation already succeeded: return the cached
                // result without touching the remote again.
                engine::GuardDecision::Resend { result, .. } => {
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
