use super::*;

pub(super) async fn execute_command(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    call_id: &str,
    command: &[String],
) -> Result<AgentToolOutcome, StepError> {
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
    let target_binding =
        engine::salted_binding_digest("command-target-v1", &ctx.run.run_id, full_target.as_bytes());
    let argv_bytes = serde_json::to_vec(command).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("command argv is not serializable: {error}"),
        )
    })?;
    let argv_binding =
        engine::salted_binding_digest("command-argv-v1", &ctx.run.run_id, &argv_bytes);
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
        engine::GuardDecision::Proceed { operation_id } => operation_id,
        // Same invocation already succeeded: return the cached
        // result without touching the remote again.
        engine::GuardDecision::Resend { result, .. } => {
            return Ok(AgentToolOutcome::Result(result));
        }
    };
    let output = match ctx.run.cmd.run(command).await {
        Ok(output) => output,
        Err(error) => {
            // Cancellation finishes nothing: it propagates
            // without a completion record.
            if !matches!(error, engine::GatewayError::Canceled) {
                ctx.run.finish_external_operation_with_warn(
                    ctx.journal,
                    node,
                    &operation_id,
                    engine::OperationOutcome::gateway_error(&error, false),
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
