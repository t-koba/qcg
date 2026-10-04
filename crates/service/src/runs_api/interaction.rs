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
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
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
                                return Err(api_bad_request(format!(
                                    "run `{id}` is not waiting for user input"
                                )));
                            }
                            let question = record.question.clone().ok_or_else(|| {
                                api_bad_request(format!("run `{id}` has no question"))
                            })?;
                            return Err(api_bad_request(format!(
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
            // Generation check: observe the durable pending generation just
            // before the atomic append. ID matching alone cannot tell a
            // regenerated prompt apart, so the lock-held precondition below
            // re-verifies this exact generation (A02).
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
                    if state.cancel_requested {
                        return Err(JournalError::PreconditionFailed(
                            "a cancel was accepted".into(),
                        ));
                    }
                    match &state.pending {
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
                    record.question = None;
                    record.confirm = None;
                    record.artifacts = None;
                    let cancellation = CancellationToken::new();
                    record.cancellation = cancellation.clone();
                    AnswerDecision::Spawn(Box::new(crate::lifecycle::SpawnRun {
                        run_id: id.clone(),
                        contract: record.contract.clone(),
                        inputs: record.inputs.clone(),
                        run_dir: record.run_dir.clone(),
                        events: record.events.clone(),
                        answers: record.answers.clone(),
                        confirmations: record.confirmations.clone(),
                        // The answer was just journaled above: no admission
                        // snapshot exists, so the spawn reads the journal
                        // once (E03).
                        journal_snapshot: None,
                        cancellation,
                        task: record.task.clone(),
                    }))
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
        // Validate, persist, and mutate under one write lock so concurrent
        // decisions cannot both journal conflicting values with last-wins.
        // Journal I/O is a short local append; engine scheduling and terminal
        // settlement stay outside the lock. Rejection classification also
        // stays outside: it takes run-map locks, so awaiting it under the
        // guard would deadlock the runs map against itself (B01).
        enum AfterLock {
            Deny {
                denied: Box<RunRecord>,
                confirm: Box<api::ConfirmSpec>,
            },
            Spawn(Box<SpawnRun>),
            Reject(crate::types::ServiceError),
        }
        let after = {
            let mut runs = self.inner.runs.write().await;
            // Re-check under the admission lock: a shutdown that started
            // between the outer check and this block must not accept a
            // decision it can no longer execute (E05).
            self.ensure_running()?;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if let Some(existing) = record.confirmations.get(&confirmation_id) {
                return if *existing == approved {
                    // Idempotent replay: the first call already journaled and
                    // settled or scheduled, so report success without duplicating.
                    Ok(())
                } else {
                    Err(ApiError::Conflict {
                        detail: format!(
                            "confirmation `{confirmation_id}` already has a different decision"
                        ),
                    })
                };
            }
            if record.state != RunStatus::Confirming {
                return Err(api_bad_request(format!(
                    "run `{id}` is not waiting for side-effect confirmation"
                )));
            }
            let confirm = record
                .confirm
                .clone()
                .ok_or_else(|| api_bad_request(format!("run `{id}` has no confirmation")))?;
            // Full-id string match is the corruption gate: a malformed or
            // foreign confirmation id never equals the pending id, so it
            // fails here as Conflict instead of aliasing another scope
            // (Q1). No separate corrupt-vs-mismatch branch is needed.
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
            // A stale observation still attempts the atomic write: the
            // lock-held precondition re-verifies, and classification below
            // reports the accurate outcome instead of this spot guessing
            // from possibly old data.
            let check_pending_seq = match &observed.pending {
                Some(engine::Interaction::Confirmation { confirm })
                    if confirm.id == confirmation_id =>
                {
                    observed.pending_seq
                }
                _ => None,
            };
            let queued_now = chrono::Utc::now();
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
                    if state.cancel_requested {
                        return Err(JournalError::PreconditionFailed(
                            "a cancel was accepted".into(),
                        ));
                    }
                    match &state.pending {
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
                record.state = RunStatus::Failed;
                record.confirm = None;
                AfterLock::Deny {
                    denied: Box::new(record.clone()),
                    confirm: Box::new(confirm),
                }
            } else {
                record.confirmations.insert(confirmation_id.clone(), true);
                record.state = RunStatus::Queued;
                record.queued_at = Some(queued_now);
                record.confirm = None;
                record.artifacts = None;
                let cancellation = CancellationToken::new();
                record.cancellation = cancellation.clone();
                AfterLock::Spawn(Box::new(SpawnRun {
                    run_id: id.clone(),
                    contract: record.contract.clone(),
                    inputs: record.inputs.clone(),
                    run_dir: record.run_dir.clone(),
                    events: record.events.clone(),
                    answers: record.answers.clone(),
                    confirmations: record.confirmations.clone(),
                    // The decision was just journaled above: no admission
                    // snapshot exists, so the spawn reads the journal once
                    // (E03).
                    journal_snapshot: None,
                    cancellation,
                    task: record.task.clone(),
                }))
            }
        };
        match after {
            AfterLock::Deny { denied, confirm } => {
                // The denial decision already won exclusively via the atomic
                // user_confirmed check above. Both settlement events append
                // under one journal-lock hold so no writer interleaves them.
                crate::run_dirs::write_run_events(
                    &denied,
                    vec![
                        (
                            "side_effect",
                            json!({
                                "kind": confirm.kind,
                                "target": confirm.target,
                                "decision": "denied_by_user",
                            }),
                        ),
                        (
                            "run_finished",
                            json!({
                                "status": "failed",
                                "metrics": {},
                                "reason": FailureDetail::new(
                                    FailureCode::ExecutionFailed,
                                    "side effect denied by user",
                                ),
                            }),
                        ),
                    ],
                )
                .map_err(api_internal)?;
                Ok(())
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

    /// Classifies an atomically rejected answer by re-folding the journal.
    /// The rejection itself already happened under the journal lock; this
    /// only decides which error to report, so a classification race cannot
    /// accept a second winner.
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
            return Err(api_bad_request(format!(
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
            None => Err(api_bad_request(format!(
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
        if state.terminal.is_some() {
            return Err(ApiError::Conflict {
                detail: format!("run `{id}` is already terminal; confirm was rejected"),
            });
        }
        if state.cancel_requested {
            return Err(api_bad_request(format!(
                "run `{id}` is not waiting for side-effect confirmation; a cancel was accepted"
            )));
        }
        match state.confirmations.get(confirmation_id) {
            Some(existing) if *existing == approved => {
                // A peer decided identically first. Adopt the durable
                // decision so this process observes the same acceptance
                // instead of a stale confirmation prompt.
                let mut runs = self.inner.runs.write().await;
                if let Some(record) = runs.get_mut(id) {
                    record
                        .confirmations
                        .insert(confirmation_id.to_string(), approved);
                    if !approved {
                        // A peer denied first: settle locally as failed so a
                        // stale confirmation prompt never requeues denied work.
                        record.state = RunStatus::Failed;
                        record.confirm = None;
                        record.artifacts = None;
                    }
                }
                Ok(())
            }
            Some(_) => Err(ApiError::Conflict {
                detail: format!(
                    "confirmation `{confirmation_id}` already has a different decision"
                ),
            }),
            None => Err(api_bad_request(format!(
                "run `{id}` is not waiting for side-effect confirmation"
            ))),
        }
    }
}
