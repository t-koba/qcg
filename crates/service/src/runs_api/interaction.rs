//! Interaction operations.
use super::*;

impl LocalService {
    pub async fn answer(
        &self,
        id: String,
        question_id: String,
        payload: AnswerPayload,
    ) -> Result<(), ApiError> {
        self.ensure_running()?;
        let answer = json!(payload.values);
        // Recovery omits terminal runs from memory. An idempotent retry may
        // still need to publish Ready after the exact answer already finished.
        let tracked = self.inner.runs.read().await.contains_key(&id);
        if !tracked && policy::is_safe_path_component(&id) {
            let run_dir = self.run_dir_for(&id).await?;
            let state = self.authoritative_state(&run_dir).await?;
            if state.terminal.is_some() {
                return match state.answers.get(&question_id) {
                    Some(existing) if existing == &answer => Ok(()),
                    Some(_) => Err(ApiError::Conflict {
                        detail: format!(
                            "question `{question_id}` was already answered with different values"
                        ),
                    }),
                    None => Err(ApiError::invalid(format!(
                        "run `{id}` is not waiting for user input"
                    ))),
                };
            }
        }
        // Memory fast paths first; durable acceptance is decided atomically
        // under the journal lock below, so racing peers serialize and exactly
        // one conflicting acceptance wins (A02). Durable acceptance precedes
        // the success report, so a restart before the engine consumes the
        // queue still resumes with the same values.
        //
        // The write guard never spans an await: rejection classification
        // takes run-map locks, so awaiting it under the guard would deadlock
        // the runs map against itself (B01). The guard returns a decision;
        // classification and spawning happen outside.
        enum AnswerDecision {
            Spawn(Box<crate::lifecycle::SpawnRun>),
            Reject(crate::types::ServiceError),
        }
        let outcome = {
            let mut runs = self.inner.runs.write().await;
            // Re-check under the admission lock: a shutdown that started
            // between the outer check and this block must not accept an
            // answer it can no longer execute (E05).
            self.ensure_running()?;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| ApiError::not_found(format!("run `{id}` was not found")))?;
            if let Some(existing) = record.answers.get(&question_id) {
                return if existing == &answer {
                    // Idempotent replay: the first call already journaled and
                    // scheduled the engine, so report success without duplicating.
                    Ok(())
                } else {
                    Err(ApiError::Conflict {
                        detail: format!(
                            "question `{question_id}` was already answered with different values"
                        ),
                    })
                };
            }
            // Resolve the prompt from memory, falling back to the durable
            // pending interaction when the record has none yet. An
            // adopting record whose engine has not re-issued its prompt
            // still carries the journaled question (same id, journaled
            // continuation): refusing the answer would force clients to
            // poll until the engine starts, and the journal precondition
            // below still verifies id and generation atomically (E03).
            let question = match (&record.state, record.question.clone()) {
                (RunStatus::Waiting, Some(question)) if question.id == question_id => question,
                _ => {
                    let observed = self.authoritative_state(&record.run_dir).await?;
                    match observed.pending {
                        Some(engine::Interaction::Question { question })
                            if question.id == question_id =>
                        {
                            question
                        }
                        _ => {
                            if record.state != RunStatus::Waiting {
                                return Err(ApiError::invalid(format!(
                                    "run `{id}` is not waiting for user input"
                                )));
                            }
                            let question = record.question.clone().ok_or_else(|| {
                                ApiError::invalid(format!("run `{id}` has no question"))
                            })?;
                            return Err(ApiError::invalid(format!(
                                "answer was for `{}`, but run is waiting for `{}`",
                                question_id, question.id
                            )));
                        }
                    }
                }
            };
            validate_form_values(&question.fields, &answer, &record.contract.manifest.runtime)
                .map_err(|error| {
                    ApiError::invalid_field("values", format!("invalid form answer: {error}"))
                })?;
            let persist = record.clone();
            let check_answer = answer.clone();
            let check_question_id = question_id.clone();
            // Bind the prompt generation as well as its id; the atomic append
            // rechecks both so a regenerated prompt cannot reuse acceptance.
            let observed = self.authoritative_state(&persist.run_dir).await?;
            // A stale observation still attempts the atomic write: the
            // lock-held precondition re-verifies, and classification below
            // reports the accurate outcome (terminal, answered, or gone)
            // instead of this spot guessing from possibly old data.
            let check_pending_seq = match &observed.pending {
                Some(engine::Interaction::Question { question }) if question.id == question_id => {
                    observed.pending_seq
                }
                _ => None,
            };
            let check_run_dir = persist.run_dir.clone();
            // One durable timestamp shared by the journal event and the
            // memory record so restarts observe the same requeue order.
            let queued_now = chrono::Utc::now();
            let accepted = crate::run_dirs::write_run_event_if(
                &persist,
                "user_answered",
                json!({
                    "question_id": check_question_id,
                    "values": check_answer,
                    "queued_at": queued_now.to_rfc3339(),
                }),
                move |state| {
                    use engine::JournalError;
                    if state.terminal.is_some() {
                        return Err(JournalError::PreconditionFailed(
                            "run is already terminal".into(),
                        ));
                    }
                    if state.cancel_requested
                        || !crate::run_dirs::list_pending_cancel_controls(&check_run_dir)
                            .map_err(|error| JournalError::InvalidEvent(error.to_string()))?
                            .is_empty()
                    {
                        return Err(JournalError::PreconditionFailed(
                            "a cancel was accepted".into(),
                        ));
                    }
                    match state.settled_pending() {
                        Some(engine::Interaction::Question { question })
                            if question.id == check_question_id => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "run is not waiting for this question".into(),
                            ));
                        }
                    }
                    match (state.pending_seq, check_pending_seq) {
                        (Some(current), Some(expected)) if current == expected => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "pending prompt generation changed; refresh and retry".into(),
                            ));
                        }
                    }
                    if state.answers.contains_key(&check_question_id) {
                        return Err(JournalError::PreconditionFailed(
                            "question was already answered".into(),
                        ));
                    }
                    Ok(())
                },
            );
            match accepted {
                // The rejection already happened atomically; re-fold outside
                // the guard only to classify which error to report.
                Err(error) => AnswerDecision::Reject(error),
                Ok(()) => {
                    // Continuations live in the typed journal store now, so only
                    // the user answer joins the memory map.
                    record.answers.insert(question_id.clone(), answer.clone());
                    record.state = RunStatus::Queued;
                    record.queued_at = Some(queued_now);
                    record.clear_prompt();
                    record.artifacts = None;
                    record.cancellation = CancellationToken::new();
                    record.scheduler_yield = CancellationToken::new();
                    record.preempted = false;
                    AnswerDecision::Spawn(Box::new(SpawnRun::from_record(id.clone(), record)))
                }
            }
        };
        match outcome {
            AnswerDecision::Spawn(request) => {
                self.clone().spawn_engine_run(*request).await;
                // Wake queue waiters: requeue changes the head and a freed ordering
                // slot must not wait for an unrelated notification (A11).
                self.inner.queue_notify.notify_waiters();
                Ok(())
            }
            AnswerDecision::Reject(error) => {
                self.classify_answer_rejection(&id, &question_id, &answer, error)
                    .await
            }
        }
    }

    pub async fn confirm(
        &self,
        id: String,
        confirmation_id: String,
        decision: ConfirmDecision,
    ) -> Result<(), ApiError> {
        self.ensure_running()?;
        let approved = decision.decision == ConfirmationDecision::Approve;
        let tracked = self.inner.runs.read().await.contains_key(&id);
        if !approved || (!tracked && policy::is_safe_path_component(&id)) {
            let run_dir = self.run_dir_for(&id).await?;
            let state = self.authoritative_state(&run_dir).await?;
            match state.confirmations.get(&confirmation_id) {
                Some(existing) if *existing != approved => {
                    return Err(ApiError::Conflict {
                        detail: format!(
                            "confirmation `{confirmation_id}` already has a different decision"
                        ),
                    });
                }
                Some(false) => {
                    return self.reconcile_denial(&id, &run_dir, &confirmation_id).await;
                }
                Some(true) if !tracked && state.terminal.is_some() => return Ok(()),
                _ if !tracked && state.terminal.is_some() => {
                    return Err(ApiError::invalid(format!(
                        "run `{id}` is not waiting for side-effect confirmation"
                    )));
                }
                _ => {}
            }
        }
        // Journal acceptance precedes memory changes. Schedule, reconcile and
        // classify outside the runs lock: these paths acquire it themselves.
        enum AfterLock {
            Deny(Utf8PathBuf),
            Spawn(Box<SpawnRun>),
            Reject(crate::types::ServiceError),
        }
        let after = {
            let mut runs = self.inner.runs.write().await;
            // Recheck shutdown under the admission lock.
            self.ensure_running()?;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| ApiError::not_found(format!("run `{id}` was not found")))?;
            if let Some(existing) = record.confirmations.get(&confirmation_id) {
                return if *existing == approved {
                    if approved {
                        Ok(())
                    } else {
                        let run_dir = record.run_dir.clone();
                        drop(runs);
                        self.reconcile_denial(&id, &run_dir, &confirmation_id).await
                    }
                } else {
                    Err(ApiError::Conflict {
                        detail: format!(
                            "confirmation `{confirmation_id}` already has a different decision"
                        ),
                    })
                };
            }
            if record.state != RunStatus::Confirming {
                return Err(ApiError::invalid(format!(
                    "run `{id}` is not waiting for side-effect confirmation"
                )));
            }
            let confirm = record
                .confirm
                .clone()
                .ok_or_else(|| ApiError::invalid(format!("run `{id}` has no confirmation")))?;
            // Exact identity rejects malformed or foreign confirmations.
            if confirm.id != confirmation_id {
                return Err(ApiError::Conflict {
                    detail: format!(
                        "confirmation was for `{confirmation_id}`, but run is waiting for `{}`",
                        confirm.id
                    ),
                });
            }
            let persist = record.clone();
            let check_confirmation_id = confirmation_id.clone();
            // Generation check: observe the durable pending generation just
            // before the atomic append. ID matching alone cannot tell a
            // regenerated prompt apart, so the lock-held precondition below
            // re-verifies this exact generation (A02).
            let observed = self.authoritative_state(&persist.run_dir).await?;
            // Classify stale observations only after the atomic check rejects.
            let check_pending_seq = match &observed.pending {
                Some(engine::Interaction::Confirmation { confirm })
                    if confirm.id == confirmation_id =>
                {
                    observed.pending_seq
                }
                _ => None,
            };
            let queued_now = chrono::Utc::now();
            let check_run_dir = persist.run_dir.clone();
            if let Err(error) = crate::run_dirs::write_run_event_if(
                &persist,
                "user_confirmed",
                json!({
                    "confirmation_id": check_confirmation_id,
                    "approved": approved,
                    "queued_at": queued_now.to_rfc3339(),
                }),
                move |state| {
                    use engine::JournalError;
                    if state.terminal.is_some() {
                        return Err(JournalError::PreconditionFailed(
                            "run is already terminal".into(),
                        ));
                    }
                    if state.cancel_requested
                        || !crate::run_dirs::list_pending_cancel_controls(&check_run_dir)
                            .map_err(|error| JournalError::InvalidEvent(error.to_string()))?
                            .is_empty()
                    {
                        return Err(JournalError::PreconditionFailed(
                            "a cancel was accepted".into(),
                        ));
                    }
                    match state.settled_pending() {
                        Some(engine::Interaction::Confirmation { confirm })
                            if confirm.id == check_confirmation_id => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "run is not waiting for this confirmation".into(),
                            ));
                        }
                    }
                    match (state.pending_seq, check_pending_seq) {
                        (Some(current), Some(expected)) if current == expected => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "pending prompt generation changed; refresh and retry".into(),
                            ));
                        }
                    }
                    if state.confirmations.contains_key(&check_confirmation_id) {
                        return Err(JournalError::PreconditionFailed(
                            "confirmation was already decided".into(),
                        ));
                    }
                    Ok(())
                },
            ) {
                // The rejection already happened atomically; re-fold outside
                // the guard only to classify which error to report.
                AfterLock::Reject(error)
            } else if !approved {
                record.confirmations.insert(confirm.id.clone(), false);
                record.state = RunStatus::Queued;
                record.clear_prompt();
                record.artifacts = None;
                AfterLock::Deny(record.run_dir.clone())
            } else {
                record.confirmations.insert(confirmation_id.clone(), true);
                record.state = RunStatus::Queued;
                record.queued_at = Some(queued_now);
                record.confirm = None;
                record.artifacts = None;
                record.cancellation = CancellationToken::new();
                record.scheduler_yield = CancellationToken::new();
                record.preempted = false;
                AfterLock::Spawn(Box::new(SpawnRun::from_record(id.clone(), record)))
            }
        };
        match after {
            AfterLock::Deny(run_dir) => {
                self.reconcile_denial(&id, &run_dir, &confirmation_id).await
            }
            AfterLock::Spawn(request) => {
                self.clone().spawn_engine_run(*request).await;
                self.inner.queue_notify.notify_waiters();
                Ok(())
            }
            AfterLock::Reject(error) => {
                self.classify_confirm_rejection(&id, &confirmation_id, approved, error)
                    .await
            }
        }
    }

    pub(crate) async fn reconcile_denial(
        &self,
        id: &str,
        run_dir: &Utf8Path,
        confirmation_id: &str,
    ) -> Result<(), ApiError> {
        let (sender, limits, package_sha256) = self
            .inner
            .runs
            .read()
            .await
            .get(id)
            .map(|record| {
                (
                    Some(record.events.clone()),
                    Some(engine::JournalLimits::from(
                        &record.contract.manifest.runtime,
                    )),
                    record.package_sha256.clone(),
                )
            })
            .unwrap_or((None, None, None));
        let directory = run_dir.to_owned();
        let confirmation = confirmation_id.to_owned();
        if let Some(record) = self.inner.runs.write().await.get_mut(id) {
            record
                .confirmations
                .insert(confirmation_id.to_string(), false);
            if !record.state.is_terminal() {
                record.state = RunStatus::Queued;
            }
            record.clear_prompt();
            record.artifacts = None;
        }
        self.blocking_work(move || {
            let limits = match limits {
                Some(limits) => {
                    if let Some(expected) = package_sha256 {
                        crate::run_package::verify(
                            &crate::run_package::package_dir(&directory),
                            &expected,
                        )?;
                    }
                    limits
                }
                None => {
                    let state = crate::summaries::fold_run_state(&directory)?;
                    let contract = crate::run_package::load(&directory, &state)?;
                    engine::JournalLimits::from(&contract.manifest.runtime)
                }
            };
            crate::run_dirs::settle_confirmed_denial(&directory, &confirmation, limits, sender)
        })
        .await
        .map_err(api_internal)?;
        if let Some(record) = self.inner.runs.write().await.get_mut(id)
            && !record.state.is_terminal()
        {
            record.state = RunStatus::Failed;
            record.clear_prompt();
            record.artifacts = None;
        }
        Ok(())
    }

    /// Re-fold to classify an atomic rejection; this cannot accept another
    /// winner because acceptance was already decided under the journal lock.
    pub(super) async fn classify_answer_rejection(
        &self,
        id: &str,
        question_id: &str,
        answer: &Value,
        error: crate::types::ServiceError,
    ) -> Result<(), ApiError> {
        if !matches!(error, crate::types::ServiceError::PreconditionFailed(_)) {
            return Err(api_internal(error));
        }
        let run_dir = self.run_dir_for(id).await?;
        let state = self.authoritative_state(&run_dir).await?;
        if state.terminal.is_some() {
            return Err(ApiError::Conflict {
                detail: format!("run `{id}` is already terminal; answer was rejected"),
            });
        }
        if state.cancel_requested {
            return Err(ApiError::invalid(format!(
                "run `{id}` is not waiting for user input; a cancel was accepted"
            )));
        }
        match state.answers.get(question_id) {
            Some(existing) if existing == answer => {
                // A peer accepted the identical answer first. Adopt it so
                // this process observes the same durable acceptance.
                let mut runs = self.inner.runs.write().await;
                if let Some(record) = runs.get_mut(id) {
                    record
                        .answers
                        .insert(question_id.to_string(), answer.clone());
                }
                Ok(())
            }
            Some(_) => Err(ApiError::Conflict {
                detail: format!(
                    "question `{question_id}` was already answered with different values"
                ),
            }),
            None => Err(ApiError::invalid(format!(
                "run `{id}` is not waiting for user input"
            ))),
        }
    }

    /// Classifies an atomically rejected confirmation the same way.
    pub(super) async fn classify_confirm_rejection(
        &self,
        id: &str,
        confirmation_id: &str,
        approved: bool,
        error: crate::types::ServiceError,
    ) -> Result<(), ApiError> {
        if !matches!(error, crate::types::ServiceError::PreconditionFailed(_)) {
            return Err(api_internal(error));
        }
        let run_dir = self.run_dir_for(id).await?;
        let state = self.authoritative_state(&run_dir).await?;
        if !approved && state.confirmations.get(confirmation_id) == Some(&false) {
            return self.reconcile_denial(id, &run_dir, confirmation_id).await;
        }
        if state.terminal.is_some() {
            return Err(ApiError::Conflict {
                detail: format!("run `{id}` is already terminal; confirm was rejected"),
            });
        }
        if state.cancel_requested {
            return Err(ApiError::invalid(format!(
                "run `{id}` is not waiting for side-effect confirmation; a cancel was accepted"
            )));
        }
        match state.confirmations.get(confirmation_id) {
            Some(existing) if *existing == approved => {
                // Adopt the peer's identical durable decision.
                let mut runs = self.inner.runs.write().await;
                if let Some(record) = runs.get_mut(id) {
                    record
                        .confirmations
                        .insert(confirmation_id.to_string(), approved);
                }
                Ok(())
            }
            Some(_) => Err(ApiError::Conflict {
                detail: format!(
                    "confirmation `{confirmation_id}` already has a different decision"
                ),
            }),
            None => Err(ApiError::invalid(format!(
                "run `{id}` is not waiting for side-effect confirmation"
            ))),
        }
    }
}
