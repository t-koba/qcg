use crate::{
    CmdGateway, CommandBounds, FsGateway, HttpGateway, JournalLimits, JournalWriter, NodeOutcome,
    SecretStore, StepError, StepOutcome, StepRegistry, TemplateService, collect_outputs,
    collect_resource_hashes, write_output_manifest_with_limits,
};
use camino::Utf8PathBuf;
use qcg_api::{FormSpec, RunNodeFailureEventData};
use qcg_contract::{Contract, NodeState, ValueBag};
use qcg_contract::{ExhaustedAction, HookErrorPolicy, OnFail};
use qcg_contract::{FieldType, InputField};
use qcg_types::{FailureCode, FailureDetail, NodePath, OutputManifest};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use super::checkpoint::{
    CheckpointAccounting, default_metadata_dir, pin_files, verify_resource_pins,
};
use super::repair_support::{RepairCycleOutcome, exhausted_question, failure_from_findings};
use super::replay::{BudgetTracker, ExecutionEnv, JournalReplay};

/// Resolves the contract audit configuration into a writer policy. Invalid
/// class keys (durable records) fail before any record is written.
fn audit_policies(
    contract: &Contract,
) -> Result<(qcg_policy::AuditPolicy, qcg_policy::AuditLimits), crate::EngineError> {
    let policy = qcg_policy::AuditPolicy::from_config(&contract.manifest.audit)
        .map_err(crate::EngineError::Failed)?;
    Ok((
        policy,
        qcg_policy::AuditLimits::from_config(&contract.manifest.audit),
    ))
}
use super::types::{
    Engine, EngineError, Progress, RunContext, RunFailure, RunOptions, RunSnapshotSource,
    canonical_file_inputs, failure_code_for_error, with_failed_evidence,
};

/// Staging filename fragment reaped at startup (E13): the single current
/// `.qcg-part-` prefix. Retired prefixes (`zip-source-*`, `contract-check-*`,
/// legacy `.fork-part-`) were removed with no compat retention (C-2): no
/// current writer emits them.
pub(crate) const STARTUP_SWEEP_FRAGMENTS: [&str; 1] = [".qcg-part-"];

impl Engine {
    pub fn new(registry: StepRegistry) -> Self {
        Self {
            registry,
            snapshot_source: None,
        }
    }

    /// Serve other runs' states to `await` nodes. The service sets this;
    /// direct runs leave it empty.
    pub fn with_snapshot_source(mut self, snapshot_source: Arc<dyn RunSnapshotSource>) -> Self {
        self.snapshot_source = Some(snapshot_source);
        self
    }

    pub async fn run(
        &self,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
    ) -> Result<OutputManifest, EngineError> {
        let run_id = Uuid::now_v7().to_string();
        let metadata_dir = default_metadata_dir(&options.output_dir, &run_id);
        let journal_limits = JournalLimits::from(&contract.manifest.runtime);
        let (audit_policy, audit_limits) = audit_policies(&contract)?;
        let cancellation = options.cancellation.clone();
        let event_sender = options.event_sender.clone();
        let shutdown = options.shutdown.clone();
        let result = self
            .run_inner(
                contract,
                inputs,
                options,
                run_id.clone(),
                metadata_dir.clone(),
            )
            .await;
        if result.as_ref().is_err_and(|error| error.is_canceled()) {
            let journal = JournalWriter::create_with_policies(
                &metadata_dir.join("journal.jsonl"),
                run_id,
                false,
                event_sender,
                journal_limits,
                audit_policy,
                audit_limits,
            )?;
            // Shutdown-aware settlement at settle time (E05): when the
            // service shutdown token is already cancelled, settle as
            // Interrupted, not Canceled. Direct runs (no shutdown token)
            // keep settling as Canceled.
            let shutting_down = shutdown.as_ref().is_some_and(|token| token.is_cancelled());
            if shutting_down {
                if journal.state().terminal.is_none() {
                    journal.event(
                        "run_interrupted",
                        json!({ "reason": FailureDetail::new(FailureCode::Interrupted, "service shutdown") }),
                    )?;
                }
            } else if !matches!(
                journal.state().terminal,
                Some(crate::TerminalState::Canceled)
            ) {
                journal.event(
                    "run_canceled",
                    json!({ "reason": FailureDetail::new(FailureCode::Canceled, "cancellation requested") }),
                )?;
            }
            cancellation.cancel();
        }
        result
    }

    pub async fn run_with_id(
        &self,
        run_id: String,
        metadata_dir: Utf8PathBuf,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
    ) -> Result<OutputManifest, EngineError> {
        let cancellation = options.cancellation.clone();
        let journal_limits = JournalLimits::from(&contract.manifest.runtime);
        let (audit_policy, audit_limits) = audit_policies(&contract)?;
        let event_sender = options.event_sender.clone();
        let shutdown = options.shutdown.clone();
        let result = self
            .run_inner(
                contract,
                inputs,
                options,
                run_id.clone(),
                metadata_dir.clone(),
            )
            .await;
        if result.as_ref().is_err_and(|error| error.is_canceled()) {
            let journal = JournalWriter::create_with_policies(
                &metadata_dir.join("journal.jsonl"),
                run_id,
                false,
                event_sender,
                journal_limits,
                audit_policy,
                audit_limits,
            )?;
            // Shutdown-aware settlement at settle time (E05): when the
            // service shutdown token is already cancelled, settle as
            // Interrupted, not Canceled. Direct runs (no shutdown token)
            // keep settling as Canceled.
            let shutting_down = shutdown.as_ref().is_some_and(|token| token.is_cancelled());
            if shutting_down {
                if journal.state().terminal.is_none() {
                    journal.event(
                        "run_interrupted",
                        json!({ "reason": FailureDetail::new(FailureCode::Interrupted, "service shutdown") }),
                    )?;
                }
            } else if !matches!(
                journal.state().terminal,
                Some(crate::TerminalState::Canceled)
            ) {
                journal.event(
                    "run_canceled",
                    json!({ "reason": FailureDetail::new(FailureCode::Canceled, "cancellation requested") }),
                )?;
            }
            cancellation.cancel();
        }
        result
    }

    pub async fn advance_with_id(
        &self,
        run_id: String,
        metadata_dir: Utf8PathBuf,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
    ) -> Progress {
        match self
            .run_with_id(run_id, metadata_dir, contract, inputs, options)
            .await
        {
            Ok(manifest) => Progress::Done(manifest),
            Err(EngineError::NeedsUser { question, .. }) => {
                Progress::Suspended(crate::Interaction::Question {
                    question: *question,
                })
            }
            Err(EngineError::NeedsConfirm { confirm, .. }) => {
                Progress::Suspended(crate::Interaction::Confirmation { confirm: *confirm })
            }
            Err(error) => Progress::Failed(RunFailure {
                kind: super::types::failure_kind_for_error(&error),
                code: failure_code_for_error(&error),
                message: error.to_string(),
            }),
        }
    }

    /// Executes the contract-declared lifecycle hooks for one event, in
    /// declaration order. Hooks are ordinary bounded steps (budget,
    /// permissions, journal, replay), but they may not suspend: a
    /// suspension attempt is a hook failure under its `on_error` policy.
    async fn run_hooks(
        &self,
        context: &RunContext,
        journal: &JournalWriter,
        vars: &mut ValueBag,
        budget: &mut BudgetTracker,
        event: &str,
    ) -> Result<(), EngineError> {
        let hooks = context.contract.manifest.hooks.for_event(event).to_vec();
        for hook in hooks {
            let node = hook.to_node(event);
            if let Some(replayed) = context.replayed_steps.get(&node.id) {
                if let Some(output) = replayed.output.clone() {
                    vars.set_step_output(&node.id, output);
                }
                journal.event(
                    "step_replayed",
                    json!({ "node": node.id, "status": replayed.status }),
                )?;
                continue;
            }
            // Warn/skip-settled hooks from a previous execution (F07-02):
            // resume must not re-execute them. Journal a replay marker so
            // the skip stays observable instead of silent.
            if journal.state().hooks_settled.contains(&node.id) {
                journal.event("hook_replayed", json!({ "hook": node.id, "event": event }))?;
                continue;
            }
            journal.event(
                "step_started",
                json!({ "node": node.id, "type": node.kind.to_string(), "attempt": 1 }),
            )?;
            let outcome = match self
                .execute_node_with_retry(context, journal, vars, budget, &node)
                .await
            {
                Ok(outcome) => outcome,
                // Cancellation always aborts the run: it is never a hook
                // outcome the contract can downgrade to a warning.
                Err(error) if error.is_canceled() => return Err(error),
                // An exhausted run budget is not a hook failure the
                // contract can act on: the run already spent its allowance.
                // The skip is recorded durably instead of silently dropping
                // the hook or failing an otherwise successful run.
                Err(error) if is_budget_exhaustion(&error) => {
                    journal.event(
                        "hook_skipped",
                        json!({
                            "hook": node.id,
                            "event": event,
                            "reason": "budget",
                            "error": error.to_string(),
                        }),
                    )?;
                    continue;
                }
                Err(error) => {
                    journal.event(
                        "hook_failed",
                        json!({
                            "hook": node.id,
                            "event": event,
                            "error": error.to_string(),
                            "policy": hook_policy_name(hook.on_error),
                        }),
                    )?;
                    match hook.on_error {
                        HookErrorPolicy::Warn => continue,
                        HookErrorPolicy::Fail => {
                            return Err(EngineError::Failed(format!(
                                "hook `{}` failed: {error}",
                                hook.id
                            )));
                        }
                    }
                }
            };
            let failure = match outcome {
                StepOutcome::Success { output, files } => {
                    let file_pins = pin_files(
                        &context.workspace,
                        &context.metadata,
                        &files,
                        &context.contract.manifest.runtime,
                        &context.checkpoint_accounting,
                    )?;
                    let output_name = super::types::output_name_for(&node);
                    vars.publish_step_output(&node.id, node.output.as_deref(), &output, None);
                    journal.event(
                        "step_finished",
                        json!({
                            "node": node.id,
                            "status": "success",
                            "files": file_pins,
                            "output": output,
                            "output_name": output_name,
                        }),
                    )?;
                    None
                }
                StepOutcome::CheckFailed {
                    findings,
                    output,
                    files,
                } => {
                    let reason = failure_from_findings(&findings, FailureCode::CheckFailed);
                    let file_pins = pin_files(
                        &context.workspace,
                        &context.metadata,
                        &files,
                        &context.contract.manifest.runtime,
                        &context.checkpoint_accounting,
                    )?;
                    journal.event(
                        "step_finished",
                        with_failed_evidence(
                            json!({
                                "node": node.id,
                                "status": "check_failed",
                                "findings": findings,
                                "reason": reason,
                            }),
                            &output,
                            &file_pins,
                        )?,
                    )?;
                    Some(reason.message)
                }
                StepOutcome::NeedsUser { .. } | StepOutcome::NeedsConfirm { .. } => {
                    Some("hook attempted to suspend the run; hooks must not suspend".to_string())
                }
            };
            if let Some(reason) = failure {
                journal.event(
                    "hook_failed",
                    json!({
                        "hook": node.id,
                        "event": event,
                        "error": reason,
                        "policy": hook_policy_name(hook.on_error),
                    }),
                )?;
                match hook.on_error {
                    HookErrorPolicy::Warn => continue,
                    HookErrorPolicy::Fail => {
                        return Err(EngineError::Failed(format!(
                            "hook `{}` failed: {reason}",
                            hook.id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_inner(
        &self,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
        requested_run_id: String,
        metadata_dir: Utf8PathBuf,
    ) -> Result<OutputManifest, EngineError> {
        let journal_limits = JournalLimits::from(&contract.manifest.runtime);
        let (audit_policy, audit_limits) = audit_policies(&contract)?;
        let mut validation_registry = self.registry.clone();
        if let Some(provider) = options.llm_provider.as_deref() {
            validation_registry.reserve_secret_env_names(provider.credential_env_names());
        }
        validation_registry.validate_contract(&contract)?;
        let inputs = contract.manifest.resolve_inputs(inputs)?;
        let inputs = canonical_file_inputs(&contract, inputs)?;
        std::fs::create_dir_all(&options.output_dir)?;
        let workspace = Utf8PathBuf::from_path_buf(dunce::canonicalize(&options.output_dir)?)
            .map_err(|path| {
                EngineError::Failed(format!(
                    "workspace path is not valid UTF-8: {}",
                    path.display()
                ))
            })?;
        std::fs::create_dir_all(&metadata_dir)?;
        // Reap staging orphans left by a killed process before doing any
        // work: callers hold the execution lease (or the direct-run lock),
        // so no live writer exists for these directories, and the age gate
        // additionally protects young files (E13b). Every known staging
        // prefix is swept (see STARTUP_SWEEP_FRAGMENTS).
        for fragment in STARTUP_SWEEP_FRAGMENTS {
            if let Err(error) = crate::FsGateway::sweep_orphaned_staging_files(
                &workspace,
                fragment,
                std::time::Duration::from_secs(3600),
            ) {
                return Err(EngineError::Failed(format!(
                    "startup staging sweep for `{fragment}` failed: {error}"
                )));
            }
        }
        if let Err(error) = crate::FsGateway::sweep_orphaned_staging_files(
            &metadata_dir.join("checkpoint-blobs"),
            ".tmp-",
            std::time::Duration::from_secs(3600),
        ) {
            return Err(EngineError::Failed(format!(
                "startup checkpoint-blob sweep failed: {error}"
            )));
        }
        // Fork blob staging uses the same `.qcg-part-` atomic-write prefix
        // as workspace writes (E06/E13); the blob sweep above only covers
        // `.tmp-`, so also reap `.qcg-part-` orphans here (C-2: the legacy
        // `.fork-part-` entry was removed with the workspace one above).
        for fragment in [".qcg-part-"] {
            if let Err(error) = crate::FsGateway::sweep_orphaned_staging_files(
                &metadata_dir.join("checkpoint-blobs"),
                fragment,
                std::time::Duration::from_secs(3600),
            ) {
                return Err(EngineError::Failed(format!(
                    "startup checkpoint-blob sweep for `{fragment}` failed: {error}"
                )));
            }
        }
        let journal = JournalWriter::create_with_policies(
            &metadata_dir.join("journal.jsonl"),
            &requested_run_id,
            options.json_events,
            options.event_sender,
            journal_limits,
            audit_policy,
            audit_limits,
        )?;
        let replay = JournalReplay::from_state(journal.state());
        let run_id = match &replay.state.run_id {
            Some(existing) if existing != &requested_run_id => {
                return Err(EngineError::Failed(format!(
                    "run id mismatch: journal has `{existing}`, driver requested `{requested_run_id}`"
                )));
            }
            Some(existing) => existing.clone(),
            None => requested_run_id,
        };
        if let Some(expected) = &replay.state.contract_sha256
            && expected != &contract.sha256
        {
            return Err(EngineError::Failed(format!(
                "contract changed while resuming run `{run_id}`: expected {expected}, got {}",
                contract.sha256
            )));
        }
        // Hard deadline for the run-wide elapsed budget: nodes stop at the
        // limit instead of only noticing on their next checkpoint (E11).
        let elapsed_deadline = contract
            .manifest
            .budget
            .max_elapsed_seconds
            .map(|limit_secs| {
                let remaining = remaining_elapsed_secs(
                    limit_secs,
                    replay.state.budget.started_at.as_deref(),
                    replay.state.execution_started,
                    chrono::Utc::now(),
                )?;
                Ok::<_, EngineError>(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(remaining),
                )
            })
            .transpose()?;
        let context = RunContext {
            run_id: run_id.clone(),
            elapsed_deadline,
            // Intentional per-gateway clones (C-4): each gateway owns its
            // config for its lifetime (`FsGateway` borrows only at
            // construction, `CmdGateway`/`HttpGateway` own `Permissions`),
            // so the clones are separate owners, not double counting. An
            // `Arc<Permissions>` redesign was evaluated and rejected: it
            // would touch every gateway constructor and call site for a
            // small config struct with zero behavioral gain.
            fs: FsGateway::new(workspace.clone(), &contract.manifest.permissions),
            cmd: CmdGateway::new(contract.manifest.permissions.clone(), workspace.clone())
                .with_bounds(CommandBounds {
                    timeout_seconds: contract.manifest.runtime.command_timeout_seconds,
                    input_limit_bytes: contract.manifest.runtime.command_input_limit_bytes,
                    output_limit_bytes: contract.manifest.runtime.command_output_limit_bytes,
                })
                // E13 snapshots live under the run metadata directory: allow
                // static wildcards to match those run-private absolutes while
                // all other absolutes still fail closed.
                .with_extra_allowed_root(metadata_dir.clone())
                .with_cancellation(options.cancellation.clone()),
            http: HttpGateway::new(
                contract.manifest.permissions.clone(),
                std::time::Duration::from_secs(contract.manifest.runtime.http_timeout_seconds),
                contract.manifest.runtime.http_body_limit_bytes,
                contract.manifest.runtime.http_redirect_limit,
            )?
            .with_cancellation(options.cancellation.clone()),
            secrets: SecretStore::try_from_env(&contract.manifest.secrets)
                .map_err(EngineError::Failed)?,
            interactive: options.interactive,
            answers: options.answers,
            confirmations: options.confirmations,
            llm_provider: options.llm_provider,
            llm_seed_override: options.llm_seed_override,
            templates: TemplateService,
            cancellation: options.cancellation.clone(),
            snapshot_source: self.snapshot_source.clone(),
            run_refs: Arc::new(options.run_refs),
            replayed_steps: Arc::new(replay.steps.clone()),
            checkpoint_accounting: Arc::new(Mutex::new(CheckpointAccounting::default())),
            contract,
            workspace,
            metadata: metadata_dir.clone(),
        };
        // Durable replay inputs win. Admission inputs are only a fallback
        // for fresh journals that recorded none; a resuming journal without
        // durable inputs is corrupt and fails closed instead of silently
        // substituting admission inputs (E06).
        let canonical_inputs = match replay.state.inputs.clone() {
            Some(inputs) => inputs,
            None => {
                if replay.state.execution_started || !replay.steps.is_empty() {
                    return Err(EngineError::Failed(format!(
                        "cannot safely resume run `{run_id}`: journal has no durable inputs"
                    )));
                }
                tracing::warn!(
                    run_id = %run_id,
                    "fresh journal has no durable inputs; using admission inputs for a new run"
                );
                inputs
            }
        };
        let resource_hashes = collect_resource_hashes(&context).await?;
        if !replay.state.execution_started {
            journal.event(
                "run_started",
                json!({
                    "run_id": run_id,
                    "generator": format!("{}@{}", context.contract.manifest.generator.id, context.contract.manifest.generator.version),
                    "generator_path": context.contract.root,
                    "contract_sha256": context.contract.sha256,
                    "inputs": &canonical_inputs,
                    "resource_hashes": resource_hashes,
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                    "retention_days": context.contract.manifest.retention.days,
                }),
            )?;
            for resource in &resource_hashes {
                journal.event("resource", resource)?;
            }
            journal.event(
                "graph_resolved",
                json!({ "nodes": context.contract.graph.order }),
            )?;
        }

        // Verify checkpoint pins and resource pins before placing file
        // inputs: a failed verification must not leave fresh writes behind.
        // The resume marker is appended only after both verification and
        // input placement succeed, so repeated failed resumes cannot grow
        // the journal (E06). Single-handle handoff: `verify_files` hashes
        // each workspace projection once FROM ITS FD; the returned digests
        // are threaded to input materialization so the same source is never
        // re-hashed (E06). FOREIGN `run_dirs` double-hashing stays outside;
        // this side eliminates its own duplication here.
        let preverified = replay.verify_files(
            &context.workspace,
            &context.metadata,
            &context.contract.manifest.runtime,
            &context.checkpoint_accounting,
        )?;
        verify_resource_pins(&replay.state, &resource_hashes)?;
        // `run_id` is already set on a never-executed `run_queued`
        // journal, so the resume decision must follow `execution_started`,
        // not the run id (E06).
        let resume = replay.state.execution_started;
        let materialized_inputs = super::types::materialize_file_inputs_with_preverified(
            &context.contract,
            &canonical_inputs,
            &context.fs,
            resume,
            &replay.state.latest_file_pins,
            &replay.state.historical_file_pins,
            &preverified,
        )
        .await?;
        if resume {
            journal.event("run_resumed", json!({ "run_id": run_id }))?;
        }

        let mut vars = if resume {
            let mut vars = replay.state.vars.clone();
            vars.set_inputs(materialized_inputs);
            vars
        } else {
            ValueBag::with_inputs(materialized_inputs)
        };
        let mut budget = BudgetTracker::new(
            options
                .max_total_steps
                .min(context.contract.manifest.budget.max_steps),
            // Durable budget seed (F13): journals with `budget_charged`
            // deltas resume from the same consumption live execution saw;
            // older journals without the event keep the legacy
            // `steps_executed` seed.
            if replay.state.budget.has_budget_charges {
                replay.state.budget.budget_charged
            } else {
                replay.state.budget.steps_executed
            },
        );
        let max_parallel_steps = options.max_parallel_steps.max(1);
        let mut states: BTreeMap<String, NodeState> = context
            .contract
            .graph
            .nodes
            .keys()
            .map(|id| (id.clone(), NodeState::Pending))
            .collect();
        for (path, outcome) in &replay.state.nodes {
            if !context.contract.graph.nodes.contains_key(path.as_str()) {
                continue;
            }
            let state = match outcome {
                NodeOutcome::Success { .. } => continue,
                NodeOutcome::Skipped { reason } => NodeState::Skipped(reason.clone()),
                NodeOutcome::Failed { reason } => NodeState::Failed(reason.clone()),
            };
            states.insert(path.as_str().to_string(), state);
        }
        self.run_hooks(&context, &journal, &mut vars, &mut budget, "run_started")
            .await?;

        // First distinct execution error (timeout/elapsed/budget) stashed
        // while failure settlement still runs hooks (F07-01 + E11): the
        // journal and hooks observe the failure, but the caller keeps the
        // distinct classification instead of a joined generic failure.
        let mut first_distinct_error: Option<EngineError> = None;
        loop {
            if context.cancellation.is_cancelled() {
                return Err(EngineError::Canceled);
            }
            let mut progressed = false;
            let mut ready = Vec::new();
            for id in context.contract.graph.order.clone() {
                if !matches!(states.get(&id), Some(NodeState::Pending)) {
                    continue;
                }
                let node = context.contract.graph.nodes.get(&id).ok_or_else(|| {
                    StepError::failed("scheduler", format!("node `{id}` disappeared"))
                })?;
                if let Some(reason) = context
                    .contract
                    .graph
                    .should_skip_by_dependencies(node, &states)
                {
                    states.insert(id.clone(), NodeState::Skipped(reason.clone()));
                    journal.event("step_skipped", json!({ "node": id, "reason": reason }))?;
                    progressed = true;
                    continue;
                }
                if !context.contract.graph.needs_satisfied(node, &states) {
                    continue;
                }
                let when =
                    vars.eval_bool(node.when.as_ref())
                        .map_err(|message| EngineError::Expr {
                            node: id.clone(),
                            message,
                        })?;
                if !when {
                    let reason = FailureDetail::new(
                        FailureCode::WhenFalse,
                        format!("when expression evaluated false: {:?}", node.when),
                    );
                    states.insert(id.clone(), NodeState::Skipped(reason.clone()));
                    journal.event("step_skipped", json!({ "node": id, "reason": reason }))?;
                    progressed = true;
                    continue;
                }
                if let Some(replayed) = replay.steps.get(&id) {
                    // Single shared output-name helper (E10): declared
                    // `output` alias wins, otherwise the node id.
                    vars.publish_step_output(
                        &node.id,
                        node.output.as_deref(),
                        &replayed.output,
                        None,
                    );
                    states.insert(id.clone(), NodeState::Success);
                    journal.event(
                        "step_replayed",
                        json!({ "node": id, "status": replayed.status }),
                    )?;
                    progressed = true;
                    continue;
                }
                ready.push(node.clone());
            }

            if ready.len() > 1
                && ready.len() <= max_parallel_steps
                && ready.iter().all(|node| self.is_parallel_safe_node(node))
            {
                // Like the sequential path (F07-01): ordinary wave errors
                // are already journaled per node inside the wave, so the
                // run continues to shared failure settlement for hooks.
                // HITL suspensions and cancellations still propagate.
                if let Err(error) = self
                    .execute_parallel_wave(
                        &context,
                        &journal,
                        &mut vars,
                        &mut states,
                        &mut budget,
                        ready,
                    )
                    .await
                {
                    if error.is_canceled()
                        || matches!(
                            error,
                            EngineError::NeedsUser { .. } | EngineError::NeedsConfirm { .. }
                        )
                    {
                        return Err(error);
                    }
                    // Per-node failures are already in `states`/journal;
                    // continue scheduling (dependents skip) toward
                    // settlement instead of bypassing failure hooks.
                    // Distinct classifications survive settlement (F07+E11).
                    if is_budget_exhaustion(&error) {
                        if first_distinct_error.is_none() {
                            first_distinct_error = Some(error);
                        }
                        break;
                    }
                    if is_distinct_execution_error(&error) && first_distinct_error.is_none() {
                        first_distinct_error = Some(error);
                    }
                }
                progressed = true;
            } else if let Some(node) = ready.first() {
                let id = node.id.clone();
                // Check the run-wide deadline before journaling start:
                // starting a node after the limit fired pollutes accounting
                // with work that can never commit (E11). Budget is charged
                // once per attempt inside the retry wrapper (unified rule,
                // E10), not here, so retries pay while the start marker
                // stays a single attempt-1 event.
                context.run_checkpoint()?;
                states.insert(id.clone(), NodeState::Running);
                // Record start before execution so long-running steps are
                // observable and durations reflect actual processing time,
                // matching the parallel wave path.
                journal.event(
                    "step_started",
                    json!({ "node": id, "type": node.kind.to_string(), "attempt": 1 }),
                )?;
                tracing::debug!(run_id = %context.run_id, node = %id, "step started");
                // Ordinary execution errors (command/HTTP/LLM failures,
                // timeouts) become node failures (F07-01) so the shared
                // failure settlement runs step_failed/run_failed hooks.
                // Cancellations and HITL suspensions still propagate.
                // Agent failures propagate too: the agent owns durable
                // continuations (checkpoints plus operation records) whose
                // replay-safety verdict only its operation guard can make
                // on re-entry. Settling one here as a terminal node failure
                // would report the stale error on resume instead of
                // re-entering the agent, stranding the continuation.
                let outcome = match self
                    .execute_node_with_retry(&context, &journal, &mut vars, &mut budget, node)
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(error)
                        if error.is_canceled()
                            || matches!(
                                error,
                                EngineError::NeedsUser { .. } | EngineError::NeedsConfirm { .. }
                            )
                            || node.kind.as_str() == "llm.agent" =>
                    {
                        return Err(error);
                    }
                    Err(error) => {
                        let reason =
                            FailureDetail::new(failure_code_for_error(&error), error.to_string());
                        states.insert(id.clone(), NodeState::Failed(reason.clone()));
                        journal.event(
                            "step_finished",
                            json!({ "node": id, "status": "failed", "reason": reason }),
                        )?;
                        // Budget exhaustion cannot progress further: break
                        // to settlement instead of failing every remaining
                        // node identically.
                        if is_budget_exhaustion(&error) {
                            if first_distinct_error.is_none() {
                                first_distinct_error = Some(error);
                            }
                            break;
                        }
                        if is_distinct_execution_error(&error) && first_distinct_error.is_none() {
                            first_distinct_error = Some(error);
                        }
                        // Continue scheduling: the failure is recorded and
                        // dependents skip on the next pass toward shared
                        // settlement. No `progressed` flag is needed here:
                        // `continue` restarts the loop (which recomputes
                        // readiness) instead of falling to the stall check.
                        continue;
                    }
                };
                if matches!(&outcome, StepOutcome::NeedsConfirm { .. }) {
                    // Confirmation suspends before real work; the start
                    // above overstates slightly but keeps seq monotonic.
                    // Keep it: removing would reintroduce the post-hoc gap.
                }
                match outcome {
                    StepOutcome::Success { output, files } => {
                        let file_pins = pin_files(
                            &context.workspace,
                            &context.metadata,
                            &files,
                            &context.contract.manifest.runtime,
                            &context.checkpoint_accounting,
                        )?;
                        let output_name = super::types::output_name_for(node);
                        vars.publish_step_output(&node.id, node.output.as_deref(), &output, None);
                        states.insert(id.clone(), NodeState::Success);
                        journal.event("step_finished", json!({ "node": id, "status": "success", "files": file_pins, "output": output, "output_name": output_name }))?;
                    }
                    StepOutcome::CheckFailed {
                        findings,
                        output,
                        files,
                    } => {
                        let reason = failure_from_findings(&findings, FailureCode::CheckFailed);
                        let failure_file_pins = pin_files(
                            &context.workspace,
                            &context.metadata,
                            &files,
                            &context.contract.manifest.runtime,
                            &context.checkpoint_accounting,
                        )?;
                        let failure_output = output.clone();
                        match &node.on_fail {
                            Some(OnFail::Repair { .. }) => {
                                let env = ExecutionEnv {
                                    context: &context,
                                    journal: &journal,
                                };
                                match self
                                    .execute_repair_cycle(
                                        env,
                                        &mut vars,
                                        &mut states,
                                        &mut budget,
                                        node,
                                        findings,
                                    )
                                    .await?
                                {
                                    RepairCycleOutcome::Repaired { output } => {
                                        // Single shared output-name helper
                                        // (E10): declared alias wins.
                                        let output_name = super::types::output_name_for(node);
                                        vars.publish_step_output(
                                            &node.id,
                                            node.output.as_deref(),
                                            &output,
                                            None,
                                        );
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            with_failed_evidence(json!({ "node": id, "status": "repaired", "output": output, "output_name": output_name }), &failure_output, &failure_file_pins)?,
                                        )?;
                                    }
                                    RepairCycleOutcome::Routed { to, output } => {
                                        let output_name = super::types::output_name_for(node);
                                        vars.publish_step_output(
                                            &node.id,
                                            node.output.as_deref(),
                                            &Some(output.clone()),
                                            None,
                                        );
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            with_failed_evidence(json!({ "node": id, "status": "routed", "to": to, "output": output, "output_name": output_name }), &failure_output, &failure_file_pins)?,
                                        )?;
                                    }
                                    RepairCycleOutcome::Answered { output } => {
                                        let output_name = super::types::output_name_for(node);
                                        vars.publish_step_output(
                                            &node.id,
                                            node.output.as_deref(),
                                            &Some(output.clone()),
                                            None,
                                        );
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            with_failed_evidence(json!({ "node": id, "status": "answered_on_fail", "output": output, "output_name": output_name }), &failure_output, &failure_file_pins)?,
                                        )?;
                                    }
                                    RepairCycleOutcome::Failed { reason } => {
                                        states
                                            .insert(id.clone(), NodeState::Failed(reason.clone()));
                                        journal.event(
                                            "step_finished",
                                            with_failed_evidence(json!({ "node": id, "status": "repair_exhausted", "reason": reason }), &failure_output, &failure_file_pins)?,
                                        )?;
                                    }
                                }
                            }
                            Some(OnFail::Route { to }) => {
                                let output = json!({ "routed_to": to, "findings": findings, "failed_output": failure_output, "failed_files": failure_file_pins });
                                let output_name = super::types::output_name_for(node);
                                vars.publish_step_output(
                                    &node.id,
                                    node.output.as_deref(),
                                    &Some(output.clone()),
                                    None,
                                );
                                states.insert(id.clone(), NodeState::Success);
                                journal.event(
                                    "step_finished",
                                    with_failed_evidence(json!({ "node": id, "status": "routed", "to": to, "findings": findings, "output": output, "output_name": output_name }), &failure_output, &failure_file_pins)?,
                                )?;
                            }
                            Some(OnFail::AskUser) => {
                                // One failure-escalation question per node by
                                // construction; the node-scoped id is stable
                                // across resume like single-shot ask_user
                                // (E08).
                                let question_id = format!("{}:on_fail", node.id);
                                if let Some(answer) = context.answers.get(&question_id) {
                                    let output = json!({ "answer": answer, "findings": findings });
                                    let output_name = super::types::output_name_for(node);
                                    vars.publish_step_output(
                                        &node.id,
                                        node.output.as_deref(),
                                        &Some(output.clone()),
                                        None,
                                    );
                                    states.insert(id.clone(), NodeState::Success);
                                    journal.event(
                                        "step_finished",
                                        with_failed_evidence(json!({ "node": id, "status": "answered_on_fail", "answer": answer, "output": output, "output_name": output_name }), &failure_output, &failure_file_pins)?,
                                    )?;
                                } else {
                                    let question = FormSpec {
                                        id: question_id,
                                        title: format!(
                                            "Resolve check failure for node `{}`",
                                            node.id
                                        ),
                                        title_i18n: Default::default(),
                                        fields: vec![InputField {
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
                                            options: vec![],
                                            option_labels_i18n: Default::default(),
                                            min_items: None,
                                            item_type: None,
                                            schema: None,
                                            options_from: None,
                                            ui: Default::default(),
                                        }],
                                    };
                                    journal.event(
                                        "step_finished",
                                        with_failed_evidence(json!({ "node": id, "status": "needs_user", "question": question, "findings": findings }), &failure_output, &failure_file_pins)?,
                                    )?;
                                    return Err(EngineError::NeedsUser {
                                        question_id: question.id.clone(),
                                        question: Box::new(question),
                                    });
                                }
                            }
                            Some(OnFail::Regenerate {
                                max_attempts,
                                on_exhausted,
                            }) => {
                                let env = ExecutionEnv {
                                    context: &context,
                                    journal: &journal,
                                };
                                match self
                                    .execute_regenerate(
                                        env,
                                        &mut vars,
                                        &mut budget,
                                        node,
                                        *max_attempts,
                                        findings,
                                    )
                                    .await?
                                {
                                    StepOutcome::Success { output, files } => {
                                        let file_pins = pin_files(
                                            &context.workspace,
                                            &context.metadata,
                                            &files,
                                            &context.contract.manifest.runtime,
                                            &context.checkpoint_accounting,
                                        )?;
                                        let output_name = super::types::output_name_for(node);
                                        // Single shared helper (E10): no
                                        // manual output-name branch.
                                        vars.publish_step_output(
                                            &node.id,
                                            node.output.as_deref(),
                                            &output,
                                            None,
                                        );
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            json!({ "node": id, "status": "regenerated", "files": file_pins, "output": output, "output_name": output_name }),
                                        )?;
                                    }
                                    StepOutcome::CheckFailed {
                                        findings,
                                        output: regen_output,
                                        files: regen_files,
                                    } => {
                                        // Pin the final failed revision so
                                        // exhaustion carries the same failed
                                        // evidence as the initial check
                                        // failure (E06): all exhaustion
                                        // paths include failed files via the
                                        // shared helper below.
                                        let regen_failed_pins = pin_files(
                                            &context.workspace,
                                            &context.metadata,
                                            &regen_files,
                                            &context.contract.manifest.runtime,
                                            &context.checkpoint_accounting,
                                        )?;
                                        match on_exhausted {
                                            ExhaustedAction::Fail => {
                                                let reason = failure_from_findings(
                                                    &findings,
                                                    FailureCode::CheckFailed,
                                                );
                                                states.insert(
                                                    id.clone(),
                                                    NodeState::Failed(reason.clone()),
                                                );
                                                journal.event(
                                                    "step_finished",
                                                    with_failed_evidence(json!({ "node": id, "status": "regenerate_exhausted", "findings": findings, "reason": reason }), &regen_output, &regen_failed_pins)?,
                                                )?;
                                            }
                                            ExhaustedAction::Route { to } => {
                                                let output = json!({
                                                    "status": "regenerate_exhausted",
                                                    "routed_to": to,
                                                    "findings": findings,
                                                });
                                                let output_name =
                                                    super::types::output_name_for(node);
                                                vars.publish_step_output(
                                                    &node.id,
                                                    node.output.as_deref(),
                                                    &Some(output.clone()),
                                                    None,
                                                );
                                                states.insert(id.clone(), NodeState::Success);
                                                journal.event(
                                                    "step_finished",
                                                    with_failed_evidence(json!({ "node": id, "status": "routed", "to": to, "output": output, "output_name": output_name }), &regen_output, &regen_failed_pins)?,
                                                )?;
                                            }
                                            ExhaustedAction::AskUser { title, fields } => {
                                                let question = exhausted_question(
                                                    node,
                                                    "regenerate",
                                                    title.as_deref(),
                                                    fields,
                                                );
                                                if let Some(answer) =
                                                    context.answers.get(&question.id)
                                                {
                                                    let output = json!({
                                                        "status": "regenerate_exhausted_answered",
                                                        "answer": answer,
                                                        "findings": findings,
                                                    });
                                                    let output_name =
                                                        super::types::output_name_for(node);
                                                    vars.publish_step_output(
                                                        &node.id,
                                                        node.output.as_deref(),
                                                        &Some(output.clone()),
                                                        None,
                                                    );
                                                    states.insert(id.clone(), NodeState::Success);
                                                    journal.event(
                                                        "step_finished",
                                                        with_failed_evidence(json!({ "node": id, "status": "answered_on_fail", "answer": answer, "output": output, "output_name": output_name }), &regen_output, &regen_failed_pins)?,
                                                    )?;
                                                } else {
                                                    journal.event(
                                                        "step_finished",
                                                        with_failed_evidence(json!({ "node": id, "status": "needs_user", "question": question, "findings": findings }), &regen_output, &regen_failed_pins)?,
                                                    )?;
                                                    return Err(EngineError::NeedsUser {
                                                        question_id: question.id.clone(),
                                                        question: Box::new(question),
                                                    });
                                                }
                                            }
                                        }
                                    }
                                    StepOutcome::NeedsUser { question } => {
                                        // Direct suspension without a failed attempt: still
                                        // journal the unified failed-evidence keys (null
                                        // plus an empty list) so every suspension shares
                                        // one notation with the failure paths (E06).
                                        const NO_OUTPUT: Option<serde_json::Value> = None;
                                        journal.event(
                                            "step_finished",
                                            with_failed_evidence(json!({ "node": id, "status": "needs_user", "question": question }), &NO_OUTPUT, &[])?,
                                        )?;
                                        return Err(EngineError::NeedsUser {
                                            question_id: question.id.clone(),
                                            question: Box::new(question),
                                        });
                                    }
                                    StepOutcome::NeedsConfirm { confirm } => {
                                        // `confirm_request` keeps its strict FOREIGN schema
                                        // (`qcg_api::ConfirmRequestEventData` allows only
                                        // `confirm`): failed-evidence keys would be rejected
                                        // at journal validation, so they stay off this event.
                                        journal.event(
                                            "confirm_request",
                                            json!({ "node": id, "confirm": confirm }),
                                        )?;
                                        return Err(EngineError::NeedsConfirm {
                                            confirm_id: confirm.id.clone(),
                                            confirm: Box::new(confirm),
                                        });
                                    }
                                }
                            }
                            Some(OnFail::Fail) | None => {
                                states.insert(id.clone(), NodeState::Failed(reason.clone()));
                                journal.event(
                                    "step_finished",
                                with_failed_evidence(json!({ "node": id, "status": "check_failed", "findings": findings, "reason": reason }), &failure_output, &failure_file_pins)?,
                                )?;
                            }
                        }
                    }
                    StepOutcome::NeedsUser { question } => {
                        // Direct suspension without a failed attempt: still
                        // journal the unified failed-evidence keys (null plus
                        // an empty list) so every suspension shares one
                        // notation with the failure paths (E06).
                        const NO_OUTPUT: Option<serde_json::Value> = None;
                        journal.event(
                            "step_finished",
                            with_failed_evidence(
                                json!({ "node": id, "status": "needs_user", "question": question }),
                                &NO_OUTPUT,
                                &[],
                            )?,
                        )?;
                        return Err(EngineError::NeedsUser {
                            question_id: question.id.clone(),
                            question: Box::new(question),
                        });
                    }
                    StepOutcome::NeedsConfirm { confirm } => {
                        // `confirm_request` keeps its strict FOREIGN schema
                        // (`qcg_api::ConfirmRequestEventData` allows only
                        // `confirm`): failed-evidence keys would be rejected
                        // at journal validation, so they stay off this event.
                        journal
                            .event("confirm_request", json!({ "node": id, "confirm": confirm }))?;
                        return Err(EngineError::NeedsConfirm {
                            confirm_id: confirm.id.clone(),
                            confirm: Box::new(confirm),
                        });
                    }
                }
                progressed = true;
            }
            if states
                .values()
                .all(|state| !matches!(state, NodeState::Pending | NodeState::Running))
            {
                break;
            }
            if !progressed {
                return Err(StepError::failed("scheduler", "no runnable node remains").into());
            }
        }

        let mut failed: Vec<_> = states
            .iter()
            .filter_map(|(id, state)| match state {
                NodeState::Failed(reason) => Some(RunNodeFailureEventData {
                    path: NodePath::root(id),
                    failure: reason.clone(),
                }),
                _ => None,
            })
            .collect();
        // The elapsed gate also precedes failure finalization: settling a
        // failure after the deadline must record the overrun instead of
        // journaling an on-budget failure. The proximate causes are kept;
        // the elapsed breach is appended so settlement shows both (E11).
        // Only append when the terminal outcome isn't already determined:
        // a canceled checkpoint never appends an elapsed breach (E11), and
        // only an `ElapsedExceeded` checkpoint appends (a `Canceled`
        // checkpoint means the run already settled as canceled).
        if !failed.is_empty() {
            match context.run_checkpoint() {
                Ok(()) => {}
                Err(EngineError::Step(crate::StepError::ElapsedExceeded {
                    limit_secs, ..
                })) => {
                    failed.push(RunNodeFailureEventData {
                        path: NodePath::root("runtime"),
                        failure: FailureDetail::new(
                            FailureCode::ElapsedExceeded,
                            format!(
                                "run elapsed budget exceeded before finalization (limit {limit_secs}s)"
                            ),
                        ),
                    });
                }
                Err(EngineError::Canceled)
                | Err(EngineError::Step(crate::StepError::Cancelled)) => {
                    // The run already settled as canceled; do not append an
                    // elapsed breach on top of it (E11).
                }
                Err(_) => {
                    // Any other checkpoint failure (for example a poisoned
                    // lock) does not prove an elapsed overrun; keep the
                    // proximate failures without appending.
                }
            }
        }
        if !failed.is_empty() {
            // step_failed hooks observe the failures once during failed
            // settlement, before the final run_failed report. The failure
            // list is published under the reserved `hook_failures` step so a
            // hook template can render it.
            vars.set_step_output("hook_failures", json!({ "failures": &failed }));
            if let Err(error) = self
                .run_hooks(&context, &journal, &mut vars, &mut budget, "step_failed")
                .await
            {
                failed.push(RunNodeFailureEventData {
                    path: NodePath::root("hook.step_failed"),
                    failure: FailureDetail::new(FailureCode::ExecutionFailed, error.to_string()),
                });
            }
            if let Err(error) = self
                .run_hooks(&context, &journal, &mut vars, &mut budget, "run_failed")
                .await
            {
                // The run is already failing; a run_failed hook failure is
                // recorded and appended to the failure list instead of
                // replacing the original cause.
                failed.push(RunNodeFailureEventData {
                    path: NodePath::root("hook.run_failed"),
                    failure: FailureDetail::new(FailureCode::ExecutionFailed, error.to_string()),
                });
            }
            journal.event(
                "run_finished",
                json!({ "status": "failed", "failures": failed }),
            )?;
            // Distinct execution errors keep their classification (E11)
            // after hooks observed the failure (F07-01): hooks ran, the
            // journal shows the failure, but the caller still sees the
            // timeout/elapsed/budget error, not a joined generic failure.
            if let Some(error) = first_distinct_error {
                return Err(error);
            }
            return Err(EngineError::Failed(
                failed
                    .iter()
                    .map(|failure| format!("{}: {}", failure.path, failure.failure))
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }

        // Final deadline gate: a long last step that overran the elapsed
        // budget without hitting a checkpoint must not settle success.
        // Elapsed is hard (Instant-based); token/cost are checkpoint-only
        // by design and enforced at attempt entry (E11). The original
        // error propagates (not folded into a generic failure) so the
        // run keeps its ElapsedExceeded/Canceled code for settlement and
        // observability (E11).
        context.run_checkpoint()?;
        self.run_hooks(&context, &journal, &mut vars, &mut budget, "run_succeeded")
            .await?;
        let outputs = collect_outputs(
            &context.workspace,
            &context.contract.manifest,
            &vars,
            &context.templates,
        )?;
        write_output_manifest_with_limits(
            &metadata_dir,
            &outputs,
            &context.contract.manifest.runtime,
        )?;
        for artifact in &outputs.artifacts {
            journal.event("artifact", artifact)?;
        }
        journal.event("run_finished", json!({ "status": "success" }))?;
        Ok(outputs)
    }
}

/// Whether a hook error is run-budget exhaustion rather than a hook failure.
fn is_budget_exhaustion(error: &EngineError) -> bool {
    match error {
        EngineError::Step(StepError::BudgetExceeded { .. }) => true,
        EngineError::Step(StepError::Failed { message, .. }) => {
            message.contains("global step budget exceeded")
        }
        _ => false,
    }
}

/// Execution errors with a distinct caller-visible classification (F07+E11):
/// timeouts, elapsed overruns, and budget exhaustion settle through failure
/// hooks like every other failure, but the caller keeps the original error
/// instead of a joined generic failure.
fn is_distinct_execution_error(error: &EngineError) -> bool {
    match error {
        EngineError::Step(
            StepError::TimedOut { .. }
            | StepError::ElapsedExceeded { .. }
            | StepError::BudgetExceeded { .. },
        ) => true,
        _ => is_budget_exhaustion(error),
    }
}

fn hook_policy_name(policy: HookErrorPolicy) -> &'static str {
    match policy {
        HookErrorPolicy::Fail => "fail",
        HookErrorPolicy::Warn => "warn",
    }
}

/// Remaining budget for the run-wide elapsed limit. Pure so the full
/// decision matrix is unit-testable without a journal (E11). Enforcement
/// after startup is monotonic (`Instant` deadline derived once here);
/// this resume-time seeding reads the durable wall-clock start once to
/// compute the remainder, then enforcement never touches wall time again,
/// so an NTP step after startup cannot stretch enforcement (E11).
/// `security.md` documents the same deadline; any wording that implies
/// wall-clock enforcement needs a doc fix there (FOREIGN, reported not
/// edited).
///
/// Defined elapsed semantics, pinned here and by the accounting test below:
/// - The durable start is queue-inclusive: it is stamped by the first
///   `run_queued`/`run_started` event, so time spent queued before execution
///   consumes the same budget as execution time.
/// - Suspension time is NOT deducted: HITL waits keep the clock running, so
///   a long human pause can exhaust the budget like any other elapsed time.
/// - A durable start in the future beyond clock-skew tolerance (5 s) is
///   corrupt: refusing beats silently widening the deadline.
/// - Small future skew (<= 5 s) clamps to zero elapsed, never negative.
/// - A journal that already started execution but records no durable start
///   is corrupt: resuming it with a fresh full budget would widen the
///   deadline, so resume is refused instead.
pub(crate) fn remaining_elapsed_secs(
    limit_secs: u64,
    started_at: Option<&str>,
    execution_started: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<u64, EngineError> {
    const FUTURE_TOLERANCE_SECS: i64 = 5;
    let elapsed_secs = match started_at {
        Some(started) => {
            let started = chrono::DateTime::parse_from_rfc3339(started).map_err(|error| {
                EngineError::Failed(format!(
                    "cannot parse durable budget start `{started}`: {error}"
                ))
            })?;
            // Exact whole-second elapsed (floor, no ceil): ceiling would
            // fire up to 1 s early by rounding 1001 ms to 2 s (E11).
            // Enforcement compares with `>` (not `>=`) in `checkpoint_scope`
            // for the same reason.
            let delta_ms = now.signed_duration_since(started).num_milliseconds();
            if delta_ms < -FUTURE_TOLERANCE_SECS * 1000 {
                return Err(EngineError::Failed(format!(
                    "durable budget start `{started}` is more than {FUTURE_TOLERANCE_SECS}s in the future; refusing resume"
                )));
            }
            (delta_ms.max(0) / 1000) as u64
        }
        None => {
            if execution_started {
                return Err(EngineError::Failed(
                    "cannot safely resume: journal started execution without a durable budget start"
                        .into(),
                ));
            }
            0
        }
    };
    Ok(limit_secs.saturating_sub(elapsed_secs))
}

#[cfg(test)]
mod elapsed_tests {
    use super::remaining_elapsed_secs;

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    #[test]
    fn fresh_run_gets_the_full_budget() {
        assert_eq!(
            remaining_elapsed_secs(60, None, false, now()).expect("fresh run"),
            60
        );
    }

    #[test]
    fn resumed_run_subtracts_durable_elapsed_time() {
        let started = (now() - chrono::Duration::seconds(25)).to_rfc3339();
        assert_eq!(
            remaining_elapsed_secs(60, Some(&started), true, now()).expect("resumed run"),
            35
        );
    }

    #[test]
    fn future_start_beyond_tolerance_is_refused_not_widened() {
        // E11: a durable start beyond the 5 s clock-skew tolerance must not
        // widen the deadline via `max(0)`.
        let started = (now() + chrono::Duration::seconds(3600)).to_rfc3339();
        assert!(
            remaining_elapsed_secs(60, Some(&started), true, now()).is_err(),
            "a future durable start must refuse resume"
        );
        let just_over = (now() + chrono::Duration::seconds(10)).to_rfc3339();
        assert!(
            remaining_elapsed_secs(60, Some(&just_over), true, now()).is_err(),
            "10 s in the future exceeds the 5 s tolerance and must refuse resume"
        );
    }

    #[test]
    fn small_future_skew_clamps_to_zero_elapsed() {
        // E11: clock skew within the 5 s tolerance clamps to zero elapsed
        // (full budget), never negative.
        let skewed = (now() + chrono::Duration::seconds(5)).to_rfc3339();
        assert_eq!(
            remaining_elapsed_secs(60, Some(&skewed), true, now()).expect("tolerated skew"),
            60
        );
    }

    #[test]
    fn queue_time_and_hitl_suspension_consume_the_elapsed_budget() {
        // E11: the durable start is queue-inclusive and suspension time is
        // not deducted. A run queued 50 s ago with a 60 s budget resumes
        // with 10 s left even if execution only just began and a HITL
        // prompt is still pending: neither queueing nor waiting pauses the
        // clock.
        let started = (now() - chrono::Duration::seconds(50)).to_rfc3339();
        assert_eq!(
            remaining_elapsed_secs(60, Some(&started), true, now()).expect("resumed run"),
            10
        );
        let queued_long_ago = (now() - chrono::Duration::seconds(3600)).to_rfc3339();
        assert_eq!(
            remaining_elapsed_secs(60, Some(&queued_long_ago), true, now())
                .expect("exhausted budget saturates"),
            0
        );
    }

    #[test]
    fn started_execution_without_a_start_is_refused() {
        // E11: resuming execution history with no durable start must not
        // restart with a fresh full budget.
        assert!(
            remaining_elapsed_secs(60, None, true, now()).is_err(),
            "execution history without a start must refuse resume"
        );
    }

    #[test]
    fn unparseable_start_is_refused() {
        assert!(remaining_elapsed_secs(60, Some("not-a-time"), true, now()).is_err());
    }
}
