//! Query operations.
use super::*;
use crate::ServiceError;
use crate::package::ArtifactZipLimits;

fn step_summaries_from_nodes(
    nodes: &std::collections::BTreeMap<model::NodePath, engine::NodeOutcome>,
) -> BTreeMap<String, api::StepSummary> {
    nodes
        .iter()
        .map(|(path, outcome)| {
            let (status, reason) = match outcome {
                engine::NodeOutcome::Success { .. } => (api::StepSummaryStatus::Success, None),
                engine::NodeOutcome::Failed { reason } => {
                    (api::StepSummaryStatus::Failed, Some(reason.clone()))
                }
                engine::NodeOutcome::Skipped { reason } => {
                    (api::StepSummaryStatus::Skipped, Some(reason.clone()))
                }
            };
            (path.to_string(), api::StepSummary { status, reason })
        })
        .collect()
}

impl LocalService {
    /// Delete a terminal run directory. Active runs are rejected so deletion
    /// never interrupts execution; cancel first. Missing runs are an error.
    /// A repeated delete after success reports not-found; list first when
    /// retrying unattended cleanup.
    pub async fn delete_run(&self, id: &str) -> Result<(), ApiError> {
        // Deletion during the shutdown drain could remove a directory that
        // shutdown settlement is still writing; refuse it like every other
        // mutating operation (E05).
        self.ensure_running()?;
        // Stable store lock precedes handoff and execution locks; re-check
        // disk even when the local map still contains a run deleted by a peer.
        let _maintenance = crate::run_dirs::try_lock_store_maintenance(&self.inner.runs_dir)
            .map_err(api_internal)?
            .ok_or_else(|| ApiError::Conflict {
                detail: "run store maintenance is busy; retry".into(),
            })?;
        let run_dir = self.run_dir_for(id).await?;
        let _admission = crate::run_dirs::try_lock_run_admission(&run_dir)
            .map_err(api_internal)?
            .ok_or_else(|| ApiError::Conflict {
                detail: format!("run `{id}` admission is busy; retry deletion"),
            })?;
        let _handoff = crate::run_dirs::try_lock_run_handoff(&run_dir)
            .map_err(api_internal)?
            .ok_or_else(|| ApiError::Conflict {
                detail: format!("run `{id}` is being requeued; retry deletion"),
            })?;
        crate::invalidation::require_run_directory(&run_dir)?;
        // A peer may own execution while this process sees no local task:
        // verify the execution lease and pending cancel mailbox in addition
        // to local terminal state (A02). Deletion before the owner ACKs the
        // stop would orphan a live writer.
        // This pre-check is advisory: deletion proceeds only for terminal
        // runs (checked below under the execution lease), and a cancel
        // arriving for an already-terminal run is a settled no-op, so a
        // publish racing this check loses no effect (E03).
        if crate::run_dirs::has_pending_cancel_control(&run_dir) {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{id}` has a pending cancel; wait for settlement before deletion"
                ),
            });
        }
        // Hold the execution lease until the directory is gone so no owner
        // can start appending between the check and the removal (TOCTOU).
        let _execution_lease =
            crate::run_dirs::try_lock_run_execution(&run_dir).map_err(api_internal)?;
        if _execution_lease.is_none() {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{id}` is executing elsewhere; cancel and wait for settlement before deletion"
                ),
            });
        }
        // Re-check under the execution lease: a shutdown that started after
        // the outer gate must not let the drain race a directory removal
        // (E05).
        self.ensure_running()?;
        crate::invalidation::require_run_directory(&run_dir)?;
        let live_task = self
            .inner
            .runs
            .read()
            .await
            .get(id)
            .is_some_and(|record| crate::types::task_slot_is_live(&record.task));
        let terminal = !live_task && self.authoritative_state(&run_dir).await?.terminal.is_some();
        if !terminal {
            return Err(ApiError::Conflict {
                detail: format!("run `{id}` is still active; cancel it before deletion"),
            });
        }
        self.inner.runs.write().await.remove(id);
        std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
        self.inner.queue_notify.notify_waiters();
        Ok(())
    }

    /// Bounded bundle preparation before headers: capped manifest/journal,
    /// pinned lengths, hashes, and serialized metadata.
    pub async fn run_bundle_parts(
        &self,
        id: &str,
        limits: &ArtifactZipLimits,
    ) -> Result<RunBundleParts, ApiError> {
        let memory = self.inner.runs.read().await.get(id).cloned();
        let run_dir = match &memory {
            Some(record) => record.run_dir.clone(),
            None => self.run_dir_for(id).await?,
        };
        let directory = run_dir.clone();
        let limits = *limits;
        use std::io::Read as _;
        let (manifest, manifest_len, journal_len, audit_len, disk_state, journal_events, verified) =
            self.blocking_work(move || {
                let meta = run_meta_dir(&directory);
                let j_path = meta.join("journal.jsonl"); // Read before outputs.json: terminal implies outputs are published.
                let j_len = std::fs::File::open(&j_path)
                    .map_err(ServiceError::Io)?
                    .metadata()
                    .map_err(ServiceError::Io)?
                    .len();
                check_bundle_limits(j_len, 0, &limits)?;
                let mut journal_limits = JournalLimits::default();
                if let Some(cap) = limits.max_bytes {
                    let cap = usize::try_from(cap)
                        .map_err(|_| ServiceError::Invalid("bundle limit overflow".into()))?;
                    journal_limits.max_total_bytes =
                        Some(journal_limits.max_total_bytes.map_or(cap, |l| l.min(cap)));
                }
                let scan = engine::read_journal_values(&j_path, journal_limits).map_err(|e| {
                    if let engine::JournalError::LimitExceeded {
                        resource: "total journal",
                        actual,
                        limit,
                    } = &e
                        && limits.max_bytes == Some(*limit as u64)
                    {
                        return ServiceError::TooLarge {
                            actual: *actual,
                            limit: *limit,
                        };
                    }
                    ServiceError::Invalid(e.to_string())
                })?;
                let j_len = scan.stats.bytes as u64;
                let vals = scan.events;
                let disk_state = RunState::fold_values(&vals)
                    .map_err(|e| ServiceError::Invalid(e.to_string()))?;
                let events = vals
                    .iter()
                    .map(|v| RunEvent::from_flat(v).map_err(ServiceError::Invalid))
                    .collect::<Result<Vec<_>, _>>()?;
                #[cfg(test)]
                crate::tests::terminal_manifest::after_first_read(&directory);
                let out_path = meta.join("outputs.json");
                let mut manifest_len = 0;
                let manifest: Option<OutputManifest> = match std::fs::File::open(&out_path) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(ServiceError::Io(e)),
                    Ok(mut f) => {
                        let len = f.metadata().map_err(ServiceError::Io)?.len();
                        check_bundle_limits(len, 0, &limits)?;
                        manifest_len = len;
                        let len = usize::try_from(len)
                            .map_err(|_| ServiceError::Invalid("bundle size overflow".into()))?;
                        let mut raw = vec![0; len];
                        f.read_exact(&mut raw)?;
                        Some(serde_json::from_slice(&raw).map_err(ServiceError::Json)?)
                    }
                };
                #[cfg(test)]
                crate::tests::terminal_manifest::after_first_read(&directory);
                let audit_len = match std::fs::File::open(meta.join("audit.jsonl")) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(ServiceError::Io(e)),
                    Ok(f) => Some(f.metadata().map_err(ServiceError::Io)?.len()),
                };
                let mut verified = Vec::new();
                if let Some(m) = &manifest {
                    check_bundle_limits(0, m.artifacts.len(), &limits)?;
                    let mut paths = std::collections::BTreeSet::new();
                    for artifact in &m.artifacts {
                        if !paths.insert(&artifact.path) {
                            return Err(ServiceError::Invalid(format!(
                                "output manifest contains duplicate artifact `{}`",
                                artifact.path
                            )));
                        }
                        let fp =
                            resolve_artifact_path(&run_workspace_dir(&directory), &artifact.path)?;
                        let len = std::fs::metadata(&fp)?.len();
                        if len != artifact.bytes {
                            return Err(ServiceError::Invalid(format!(
                                "artifact `{}` bytes mismatch: manifest={}, actual={len}",
                                artifact.path, artifact.bytes
                            )));
                        }
                        verified.push(crate::artifacts::VerifiedArtifact {
                            path: artifact.path.clone(),
                            fs_path: fp,
                            bytes: artifact.bytes,
                            sha256: artifact.sha256.clone(),
                        });
                    }
                }
                Ok((
                    manifest,
                    manifest_len,
                    j_len,
                    audit_len,
                    disk_state,
                    events,
                    verified,
                ))
            })
            .await
            .map_err(ApiError::from)?;
        let inputs = crate::summaries::run_identity_event(&run_dir, &journal_events)
            .map_err(api_internal)?
            .1
            .inputs
            .clone();
        let snapshot = self
            .assemble_snapshot_from_read(
                id.to_string(),
                memory,
                &run_dir,
                SnapshotRead {
                    events: &journal_events,
                    state: &disk_state,
                },
                manifest,
            )
            .await?;
        let outputs_len = snapshot
            .artifacts
            .as_ref()
            .map(json_size)
            .transpose()
            .map_err(ApiError::from)?
            .unwrap_or(0);
        let lengths = [
            json_size(&snapshot).map_err(ApiError::from)?,
            json_size(&inputs).map_err(ApiError::from)?,
            outputs_len,
            manifest_len,
            journal_len,
            audit_len.unwrap_or(0),
        ];
        let total = lengths
            .into_iter()
            .chain(verified.iter().map(|a| a.bytes))
            .try_fold(0_u64, |sum, len| sum.checked_add(len))
            .ok_or_else(|| api_internal("bundle size overflow"))?;
        check_bundle_limits(total, 0, &limits).map_err(ApiError::from)?;
        let base_entries = 3_usize
            .checked_add(usize::from(snapshot.artifacts.is_some()))
            .and_then(|n| n.checked_add(usize::from(audit_len.is_some())))
            .and_then(|n| n.checked_add(verified.len()))
            .ok_or_else(|| api_internal("bundle entries overflow"))?;
        let mut directories = std::collections::BTreeSet::from([Utf8PathBuf::from("artifacts")]);
        for artifact in &verified {
            let mut parent = Utf8Path::new(&artifact.path).parent();
            while let Some(dir) = parent.filter(|d| !d.as_str().is_empty()) {
                directories.insert(Utf8PathBuf::from("artifacts").join(dir));
                let entries = base_entries
                    .checked_add(directories.len())
                    .ok_or_else(|| api_internal("bundle entries overflow"))?;
                check_bundle_limits(total, entries, &limits).map_err(ApiError::from)?;
                parent = dir.parent();
            }
        }
        let entry_count = base_entries
            .checked_add(directories.len())
            .ok_or_else(|| api_internal("bundle entries overflow"))?;
        check_bundle_limits(total, entry_count, &limits).map_err(ApiError::from)?;
        // Only admitted sources may incur hash I/O; return the same private state.
        let verified = self
            .blocking_work(move || {
                check_verified_artifact_hashes(&verified)?;
                Ok(verified)
            })
            .await
            .map_err(ApiError::from)?;
        let snapshot_bytes = serde_json::to_vec_pretty(&snapshot).map_err(api_internal)?;
        let inputs_bytes = serde_json::to_vec_pretty(&inputs).map_err(api_internal)?;
        let outputs_bytes = snapshot
            .artifacts
            .as_ref()
            .map(serde_json::to_vec_pretty)
            .transpose()
            .map_err(api_internal)?;
        let journal = run_meta_dir(&run_dir).join("journal.jsonl");
        Ok(RunBundleParts {
            source_bytes: total,
            entry_count,
            snapshot_bytes,
            inputs_bytes,
            outputs_bytes,
            journal,
            journal_len,
            audit_len,
            verified,
            directories: directories.into_iter().collect(),
        })
    }

    pub async fn snapshot(&self, id: String) -> Result<RunSnapshot, ApiError> {
        let memory = self.inner.runs.read().await.get(&id).cloned();
        let run_dir = match &memory {
            Some(record) => record.run_dir.clone(),
            None => self.run_dir_for(&id).await?,
        };
        let view = self.read_view(run_dir.clone()).await?; // Read before outputs.json for terminal consistency.
        #[cfg(test)]
        crate::tests::terminal_manifest::after_first_read(&run_dir);
        let directory = run_dir.clone();
        let artifacts = match memory.as_ref().and_then(|record| record.artifacts.clone()) {
            Some(artifacts) => Some(artifacts),
            None => {
                self.blocking_read(move || read_optional_output_manifest(&directory))
                    .await?
            }
        };
        #[cfg(test)]
        crate::tests::terminal_manifest::after_first_read(&run_dir);
        let journal_events = &view.events;
        self.assemble_snapshot_from_read(
            id,
            memory,
            &run_dir,
            SnapshotRead {
                events: journal_events,
                state: &view.state,
            },
            artifacts,
        )
        .await
    }

    /// Snapshot assembly from one already-read journal snapshot. Every
    /// durable derivation (fold, identity, queue key, metrics) folds
    /// the same in-memory values, so callers never re-scan the journal
    /// per field and the bundle path reuses this single read (E16).
    pub(super) async fn assemble_snapshot_from_read(
        &self,
        id: String,
        memory: Option<RunRecord>,
        run_dir: &Utf8Path,
        read: SnapshotRead<'_>,
        artifacts: Option<OutputManifest>,
    ) -> Result<RunSnapshot, ApiError> {
        let SnapshotRead {
            events: journal_events,
            state: disk_state,
        } = read;
        // A journaled terminal outcome always wins over memory: acceptance
        // displays (such as `CancelRequested`) settle into their terminal
        // state as soon as the journal records it, on every process (A02).
        // An unsettled run without memory is queued, never terminal: only a
        // journaled outcome may report a terminal state, so an orphaned or
        // peer-owned run resumes through the lease instead of being
        // misreported as dead.
        let state = disk_state
            .terminal
            .as_ref()
            .map(crate::lifecycle::terminal_status)
            .or_else(|| memory.as_ref().map(|record| record.state))
            .unwrap_or(RunStatus::Queued);
        let contract_sha256 = match memory.as_ref() {
            Some(record) => Some(record.contract_sha256.clone()),
            None => Some(
                crate::summaries::run_identity_event(run_dir, journal_events)
                    .map(|(_, started)| started.contract_sha256.clone())
                    .map_err(api_internal)?,
            ),
        };
        let memory_priority = memory.as_ref().map(|record| record.priority);
        let memory_parent = memory
            .as_ref()
            .and_then(|record| record.parent_run_id.clone());
        // Prompt display falls back to the durable pending interaction
        // whenever memory holds none: for a run this process has not
        // rehydrated (a peer-owned or still-adopting run) and for an
        // adopting record whose engine has not re-issued its prompt yet.
        // The durable pending interaction is the same prompt the engine is
        // about to re-issue (same id, journaled continuation), answered
        // prompts clear it in the fold, so displaying it can neither show
        // stale work nor accept work the record cannot run: answers land
        // durably and the engine consumes them on start (E03).
        let durable_pending = || match disk_state.settled_pending().cloned() {
            Some(engine::Interaction::Question { question }) => (Some(question), None),
            Some(engine::Interaction::Confirmation { confirm }) => (None, Some(confirm)),
            None => (None, None),
        };
        let (question, confirm) = match memory.as_ref() {
            Some(record) => match (record.question.clone(), record.confirm.clone()) {
                (None, None) => durable_pending(),
                found => found,
            },
            None => durable_pending(),
        };
        let state = if state == RunStatus::Queued {
            match (&question, &confirm) {
                (Some(_), _) => RunStatus::Waiting,
                (_, Some(_)) => RunStatus::Confirming,
                _ => state,
            }
        } else {
            state
        };
        let (queued_at, queue_position, queue_position_quality) = if state == RunStatus::Queued {
            // Display the same durable instant used for ordering; memory is
            // only a fallback when the journal holds no instant (the read
            // above already succeeded, so an unreadable journal cannot
            // occur here).
            let displayed = disk_state.queued_at.clone().or_else(|| {
                memory
                    .as_ref()
                    .and_then(|record| record.queued_at)
                    .map(|at| at.to_rfc3339())
            });
            let (position, quality) = match self.queued_position_for_snapshot(&id, disk_state).await
            {
                Ok(position) => position,
                Err(error) => {
                    tracing::warn!(run_id = %id, %error, "queue position is unavailable");
                    (None, api::QueuePositionQuality::Unavailable)
                }
            };
            (displayed, position, quality)
        } else {
            (None, None, api::QueuePositionQuality::Unavailable)
        };
        // Disk-only snapshots (evicted or restarted runs) derive the
        // generator from the journal identity event, never from run_id
        // string parsing. An unresolvable owner fails the snapshot instead
        // of emitting an ownerless record for the client to guess about.
        let generator_id = memory
            .as_ref()
            .map(|record| record.contract.manifest.generator.id.clone())
            .or_else(|| disk_state.generator_id.clone())
            .ok_or_else(|| api_internal(format!("run `{id}` has no owning generator")))?;
        // Priority and parent derive from the same single read above, so
        // disk-only snapshots never trigger another scan of the journal.
        let (journal_priority, journal_parent) =
            (disk_state.priority, disk_state.parent_run_id.clone());
        let directory = run_dir.to_owned();
        let invalidation = self
            .blocking_work(move || crate::invalidation::read_invalidation(&directory))
            .await
            .map_err(ApiError::from)?;
        Ok(RunSnapshot {
            invalidation,
            run_id: id,
            state,
            seq: disk_state.last_seq,
            contract_sha256,
            generator_id,
            artifacts,
            question,
            confirm,
            queued_at,
            queue_position,
            queue_position_quality,
            priority: memory_priority.unwrap_or(journal_priority),
            parent_run_id: memory_parent.or(journal_parent),
            metrics: crate::summaries::read_run_metrics_from_view(journal_events, disk_state)
                .map_err(api_internal)?,
            labels: disk_state.labels.clone(),
            step_summaries: step_summaries_from_nodes(&disk_state.nodes),
        })
    }

    /// Queue position for a snapshot that is already known to be Queued.
    /// Memory and disk queued runs share one FIFO: the position merges the
    /// live map with a bounded disk scan, regardless of where the target
    /// itself lives. Computing memory targets from memory alone would
    /// ignore disk-queued runs ahead of them and pin a stale 304 (E16).
    /// The target's priority and FIFO key come from the caller's single
    /// journal read, never a second scan.
    /// The disk half is served from a 1 s process-wide cache shared across
    /// subscribers (E12): one store scan per second, not one per snapshot.
    /// Memory-tracked runs (every local admission) are always merged fresh,
    /// so local subscribers never observe stale self-positions; only
    /// disk-only peer runs can lag by at most the TTL, which delays
    /// position display without ever misordering execution (the scheduler
    /// never consults this cache).
    pub(super) async fn queued_position_for_snapshot(
        &self,
        id: &str,
        target: &RunState,
    ) -> Result<(Option<usize>, api::QueuePositionQuality), ApiError> {
        // One read lock serves both sets so an admission interleaving
        // between two locks cannot shift followers (E16).
        let (memory_queued, memory_all_ids) = {
            let runs = self.inner.runs.read().await;
            let queued = runs
                .iter()
                .filter(|(_, record)| record.state == RunStatus::Queued)
                .map(|(run_id, record)| (run_id.clone(), record.priority, record.queued_at))
                .collect::<Vec<_>>();
            let all: std::collections::BTreeSet<String> = runs.keys().cloned().collect();
            (queued, all)
        };
        // Exclude every memory-tracked run from the disk candidates, not
        // just queued ones: a Running record's stale disk journal still
        // shows `run_queued`, and counting it would duplicate the live run
        // as an extra queued rival and shift positions (E16).

        let (target_priority, target_queued_at) = (
            target.priority,
            target
                .queued_at
                .as_deref()
                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                .map(|at| at.with_timezone(&chrono::Utc)),
        );
        // The target may already be present in the memory set below; push
        // only when absent so followers are never shifted by a duplicate
        // entry. The completeness of the disk scan determines whether
        // the resulting position can be reported as exact.
        let mut order: Vec<QueueOrderEntry> = memory_queued;
        if !order.iter().any(|(run_id, _, _)| run_id == id) {
            order.push((id.to_string(), target_priority, target_queued_at));
        }
        // Disk candidates exclude every memory-tracked run plus the pushed
        // target (for disk-only snapshots): otherwise the target's own
        // directory would be counted twice (E16).
        let mut memory_ids: std::collections::BTreeSet<String> = memory_all_ids;
        for (run_id, _, _) in &order {
            memory_ids.insert(run_id.clone());
        }
        let (disk_entries, incomplete) = self.queued_disk_candidates(&memory_ids).await?;
        let estimated = incomplete
            || self.inner.run_store_mode == RunStoreMode::SharedFilesystem
            || !disk_entries.is_empty();
        order.extend(disk_entries);
        // Shared queue key with the scheduler, never a second inline
        // ordering that could drift from it (E12/E16).
        order.sort_by(|left, right| {
            crate::queue::queue_key(left.1, left.2, left.0.as_str()).cmp(&crate::queue::queue_key(
                right.1,
                right.2,
                right.0.as_str(),
            ))
        });
        Ok((
            order
                .iter()
                .position(|(run_id, _, _)| run_id == id)
                .map(|index| index + 1),
            if estimated {
                api::QueuePositionQuality::Estimated
            } else {
                api::QueuePositionQuality::Exact
            },
        ))
    }

    /// Disk half of the display-only queue merge, coalesced across subscribers
    /// through a one-second service cache: one store scan per second,
    /// not one per snapshot. The cache holds raw on-disk candidates keyed
    /// by runs directory; the caller-side exclusion set (memory-tracked
    /// runs plus the target) is applied fresh on every call, so local
    /// admissions are never stale. Only disk-only peer runs can lag by at
    /// most the TTL, which delays position display without misordering
    /// execution (the scheduler never consults this cache). Stale display
    /// still serves the exact-body validator for up to the TTL by design:
    /// the snapshot ETag digests whatever body is served, so a lagging
    /// position yields a lagging-but-exact validator, never a mismatched
    /// one (E12/E16 trade-off; the strict validator lives in
    /// `server::conditional_response`). Errors are
    /// never cached: every failure re-scans on the next call.
    pub(super) async fn queued_disk_candidates(
        &self,
        exclude: &std::collections::BTreeSet<String>,
    ) -> Result<(Vec<QueueOrderEntry>, bool), ApiError> {
        // The async guard coalesces concurrent refreshes for this service.
        // The filesystem work runs in the bounded blocking pool.
        let mut cache = self.inner.queue_cache.lock().await;
        if cache
            .as_ref()
            .is_none_or(|entry| entry.at.elapsed() >= std::time::Duration::from_secs(1))
        {
            let directory = self.inner.runs_dir.clone();
            let cap = self.inner.deployment_policy.max_directory_scan_entries;
            let store = self.inner.read_store.clone();
            let scanned = self
                .blocking_read(move || {
                    Self::scan_queue_disk_candidates(&directory, cap, &store)
                        .map_err(|error| crate::ServiceError::Invalid(error.to_string()))
                })
                .await?;
            *cache = Some(scanned);
        }
        let cached = cache.as_ref().expect("queue cache was refreshed");
        Ok((
            cached
                .entries
                .iter()
                .filter(|(id, _, _)| !exclude.contains(id))
                .cloned()
                .collect(),
            cached.incomplete,
        ))
    }

    /// One bounded store scan for queue-eligible disk runs (E16). A peer
    /// without a journal file is not a queued run. An unreadable or corrupt
    /// peer journal is skipped with a warning instead of failing an
    /// unrelated run's snapshot. Excluding any unreadable run marks the
    /// displayed position estimated. Recovery keeps
    /// the stricter fail-closed rule because it must execute from what it
    /// reads; this read path only displays order.
    /// The started-vs-queued comparison reads both markers from the same
    /// per-peer journal snapshot, so no intra-peer TOCTOU exists; a peer
    /// appending mid-scan only shifts this advisory position within the
    /// 1 s cache bound (E16).
    pub(super) fn scan_queue_disk_candidates(
        runs_dir: &Utf8Path,
        max_entries: usize,
        store: &crate::read_store::ReadStore,
    ) -> Result<QueueCache, ApiError> {
        let mut entries = Vec::new();
        let mut scanned = 0_usize;
        let mut truncated = false;
        let read_dir = match std::fs::read_dir(runs_dir) {
            Ok(read_dir) => read_dir,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(QueueCache {
                    at: std::time::Instant::now(),
                    entries,
                    incomplete: false,
                });
            }
            Err(error) => return Err(ApiError::internal(error.to_string())),
        };
        for entry in read_dir {
            let entry = entry.map_err(|error| ApiError::internal(error.to_string()))?;
            if crate::run_dirs::is_coordination_entry(&entry) {
                continue;
            }
            scanned = scanned.saturating_add(1);
            if scanned > max_entries {
                // Graceful degrade: stop scanning, keep capped peers, flag
                // truncation explicitly instead of failing the snapshot.
                truncated = true;
                break;
            }
            let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                // Non-UTF8 run dirs never match memory ids via lossy conversion (E16).
                continue;
            };
            let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                ApiError::internal(format!("run path is not UTF-8: {}", path.display()))
            })?;
            if !is_regular_directory(&run_dir).map_err(api_internal)? {
                continue;
            }
            let journal_path = crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl");
            let symlink_meta = match std::fs::symlink_metadata(&journal_path) {
                Ok(meta) => meta,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    truncated = true;
                    tracing::warn!(run_dir = %run_dir, %error, "skipping uninspectable peer journal in queue scan");
                    continue;
                }
            };
            if !symlink_meta.is_file() {
                continue;
            }
            let view = match store.read(&run_dir) {
                Ok(view) => view,
                Err(error) => {
                    truncated = true;
                    tracing::warn!(run_dir = %run_dir, %error, "skipping unreadable peer journal in queue scan");
                    continue;
                }
            };
            let folded = &view.state;
            if folded.terminal.is_some()
                || folded.settled_pending().is_some()
                || folded.cancel_requested
            {
                continue;
            }
            // Count only actually-queued disk runs: a journal with a
            // `run_started` newer than its latest `run_queued` is
            // executing elsewhere, not queued behind the target (E16).
            // Preempted runs carry a newer `run_queued` and count.
            if view.last_started > view.last_queued {
                continue;
            }
            let state = folded;
            let queued_at = state
                .queued_at
                .as_deref()
                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                .map(|at| at.with_timezone(&chrono::Utc));
            entries.push((file_name, state.priority, queued_at));
        }
        if truncated {
            // The API marks this position estimated; operators also receive
            // the scan diagnostics.
            tracing::warn!(
                runs_dir = %runs_dir,
                scanned,
                included = entries.len(),
                "queue scan is incomplete; snapshot positions are estimates"
            );
        }
        Ok(QueueCache {
            at: std::time::Instant::now(),
            entries,
            incomplete: truncated,
        })
    }
    /// Cost metrics for one run with a USD estimate and pricing coverage.
    pub async fn run_cost_metrics(&self, id: String) -> Result<RunCostMetrics, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let view = self.read_view(run_dir.clone()).await?;
        let state = &view.state;
        let journal_events = &view.events;
        let metrics = crate::summaries::read_run_metrics_from_view(journal_events, state)
            .map_err(api_internal)?
            .unwrap_or_default();
        let memory_state = self
            .inner
            .runs
            .read()
            .await
            .get(&id)
            .map(|record| record.state);
        // Pricing coverage degrades loudly, never silently: an unloadable
        // contract reports totals as potentially understated instead of
        // failing metrics the journal already supports.
        let priced = match self.pricing_covered(journal_events, &run_dir).await {
            Ok(priced) => priced,
            Err(error) => {
                tracing::warn!(run_id = %id, %error, "pricing coverage check failed; reporting unpriced");
                false
            }
        };
        Ok(RunCostMetrics {
            run_id: id,
            state: state
                .terminal
                .as_ref()
                .map(crate::lifecycle::terminal_status)
                .or(memory_state)
                .unwrap_or(RunStatus::Queued),
            metrics: metrics.clone(),
            cost_usd: metrics.cost_microusd as f64 / 1_000_000.0,
            priced,
        })
    }

    /// True when every billed LLM call resolves to contract pricing.
    /// Unpriced calls may understate the total, so callers must surface this.
    /// Takes already-read events so pricing never re-scans the journal.
    pub(super) async fn pricing_covered(
        &self,
        events: &[api::RunEvent],
        run_dir: &Utf8Path,
    ) -> Result<bool, ApiError> {
        let calls: Vec<(&str, &str, u64, u64)> = events
            .iter()
            .filter_map(|event| match &event.data {
                RunEventData::LlmCall(data) => Some((
                    data.provider.as_str(),
                    data.model.as_str(),
                    data.tokens.input.saturating_add(data.tokens.output),
                    data.cost_microusd,
                )),
                _ => None,
            })
            .collect();
        if calls.is_empty() {
            return Ok(true);
        }
        let directory = run_dir.to_owned();
        let identity = crate::summaries::run_identity_event(run_dir, events)
            .map_err(api_internal)?
            .1
            .clone();
        let contract = self
            .blocking_read(move || {
                if identity.package_sha256.is_some() {
                    crate::run_package::load_identity(
                        &directory,
                        identity.package_sha256.as_deref(),
                        Some(&identity.contract_sha256),
                    )
                } else {
                    // Historical observation only; never used for execution.
                    Contract::load(Utf8PathBuf::from(identity.generator_path))
                        .map_err(|error| ServiceError::Invalid(error.to_string()))
                }
            })
            .await?;
        let priced: Vec<(String, String)> = contract
            .manifest
            .llm
            .as_ref()
            .map(|llm| {
                llm.models
                    .iter()
                    .chain(llm.model.as_ref())
                    .filter(|model| {
                        model.input_cost_per_million_usd.is_some()
                            && model.output_cost_per_million_usd.is_some()
                    })
                    .map(|model| (model.provider.clone(), model.model.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(calls.into_iter().all(|(provider, model, tokens, cost)| {
            tokens == 0
                || cost > 0
                || priced.iter().any(|(entry_provider, entry_model)| {
                    entry_provider == provider && entry_model == model
                })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn step_summaries_project_folded_outcomes_and_canonical_paths() {
        let mut state = RunState::default();
        for event in [
            json!({"t":"step_finished", "seq":1, "node":"successful", "status":"success", "output":"live output"}),
            json!({"t":"step_finished", "seq":2, "node":"failed", "status":"failed", "reason":{"code":"check_failed", "message":"check failed"}}),
            json!({"t":"step_skipped", "seq":3, "node":"skipped", "reason":{"code":"when_false", "message":"condition was false"}}),
            json!({"t":"step_finished", "seq":4, "node":"each[0]/emit", "status":"success"}),
            json!({"t":"step_finished", "seq":5, "node":"each[1]/emit", "status":"success"}),
            json!({"t":"step_finished", "seq":6, "node":"build@repair.2/fix", "status":"repaired"}),
        ] {
            state.apply(&event).expect("journal event should fold");
        }

        // Deliberately make the live ValueBag disagree with the durable fold.
        // Snapshot summaries must reflect NodeOutcome, not transient values.
        for path in state.nodes.keys() {
            state.vars.set_step_outcome(
                path.as_str(),
                "failed",
                Some(json!({"code":"execution_failed", "message":"transient value"})),
            );
        }
        let summaries = step_summaries_from_nodes(&state.nodes);
        assert_eq!(summaries.len(), 6);
        assert_eq!(
            summaries["successful"].status,
            api::StepSummaryStatus::Success
        );
        assert_eq!(summaries["successful"].reason, None);
        assert_eq!(
            serde_json::to_value(&summaries["successful"]).unwrap(),
            json!({"status":"success", "reason":null})
        );

        assert_eq!(summaries["failed"].status, api::StepSummaryStatus::Failed);
        assert_eq!(
            serde_json::to_value(summaries["failed"].status).unwrap(),
            json!("failed")
        );
        assert_eq!(
            summaries["failed"]
                .reason
                .as_ref()
                .map(|detail| detail.code),
            Some(model::FailureCode::CheckFailed)
        );
        assert_eq!(
            summaries["failed"].reason.as_ref().unwrap().message,
            "check failed"
        );
        assert_eq!(summaries["skipped"].status, api::StepSummaryStatus::Skipped);
        assert_eq!(
            summaries["skipped"]
                .reason
                .as_ref()
                .map(|detail| detail.code),
            Some(model::FailureCode::WhenFalse)
        );
        assert!(summaries["skipped"].reason.is_some());

        assert_eq!(
            summaries["each[0]/emit"].status,
            api::StepSummaryStatus::Success
        );
        assert_eq!(
            summaries["each[1]/emit"].status,
            api::StepSummaryStatus::Success
        );
        assert!(summaries.contains_key("build@repair.2/fix"));
        assert_eq!(
            summaries["build@repair.2/fix"].status,
            api::StepSummaryStatus::Success,
            "repaired outcomes map to terminal success without rewriting the canonical path"
        );
        assert_eq!(
            summaries
                .keys()
                .filter(|path| path.starts_with("each["))
                .count(),
            2,
            "each canonical foreach child path must occupy its own map entry"
        );
    }
}
