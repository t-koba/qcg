//! Subscription operations.
use super::*;

impl LocalService {
    /// Event stream contract: history first, then the live tail. A
    /// broadcast lag ends the live tail with a `lagged` marker carrying
    /// the last actually-delivered seq; the client resubscribes with that
    /// seq as `Last-Event-ID` and the journal replay yields the next real
    /// event, so no event is skipped (E12a). A settled run returns history
    /// only and never pends on the broadcast (E12).
    /// Shared journal poller, one per run. The first subscriber spawns the
    /// underlying poll task, which polls every
    /// the deployment poll cadence (default 250 ms); later subscribers reuse its
    /// broadcast
    /// instead of spawning their own task, so N subscribers cost one poller
    /// (E12). The poller starts from the creating subscriber's cursor;
    /// every subscriber filters by its own history end, so an older cursor
    /// overlaps already-broadcast events (skipped by seq, never missed) and
    /// a newer cursor skips nothing the journal replay did not already
    /// serve (E12). Check-insert is atomic under one lock so concurrent
    /// subscribes never spawn two pollers for the same run, and the exit
    /// protocol below is race-free: exit-removal happens only under the
    /// same lock with a receiver-count re-check, so a concurrent subscribe
    /// either attaches first (keeping this task alive) or finds no entry
    /// (spawning a successor). No subscriber is ever stranded on an exited
    /// poller and no transient double poller ever broadcasts (E12).
    pub(super) fn shared_poll_receiver(
        &self,
        run_dir: Utf8PathBuf,
        run_id: String,
        start_seq: u64,
    ) -> broadcast::Receiver<RunEvent> {
        let (sender, receiver) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
        {
            let mut pollers = self
                .inner
                .journal_pollers
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Singleflight under one lock (E12): at most one current poller
            // per run. A present entry is reused by presence, not by
            // receiver count: the exit protocol below removes its entry
            // only under this same lock with a zero re-check, so a present
            // entry always belongs to a task that is alive or will stay
            // alive for this attach.
            if let Some(existing) = pollers.get(&run_id) {
                return existing.subscribe();
            }
            pollers.insert(run_id.clone(), sender.clone());
        }
        let service = self.clone();
        let shutdown = self.inner.shutdown.clone();
        let poll_interval_millis = self.inner.deployment_policy.journal_poll_interval_millis;
        tokio::spawn(async move {
            let mut poll_stream = poll_journal_events(
                run_dir,
                run_id.to_string(),
                start_seq,
                poll_interval_millis,
                shutdown,
            );
            use futures_util::StreamExt as _;
            // The poll task owns one sender clone; the map owns the other.
            // Cleanup below removes the map entry only when it still points
            // to this task's channel (same_channel), so a successor poller
            // is never deleted (E12). Every exit path cleans up when still
            // ours: leaving a dead channel behind would let the next
            // subscribe reuse a poller that can never deliver the terminal
            // event.
            let run_id_for_cleanup = run_id.clone();
            let service_for_cleanup = service.clone();
            let cleanup = |sender: &broadcast::Sender<RunEvent>| {
                let mut pollers = service_for_cleanup
                    .inner
                    .journal_pollers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if pollers
                    .get(&run_id_for_cleanup)
                    .is_some_and(|existing| existing.same_channel(sender))
                {
                    pollers.remove(&run_id_for_cleanup);
                }
            };
            // Returns true when this task must exit: receiverless under the
            // pollers lock with the entry still ours. The lock pairs with
            // the attach in `shared_poll_receiver` above: a subscribe that
            // attached first raised the count (this task stays alive for
            // it); one that locks after the removal finds no entry and
            // spawns a successor (E12).
            let service_for_exit = service.clone();
            let run_id_for_exit = run_id.clone();
            let should_exit = |sender: &broadcast::Sender<RunEvent>| {
                if sender.receiver_count() != 0 {
                    return false;
                }
                let mut pollers = service_for_exit
                    .inner
                    .journal_pollers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if sender.receiver_count() == 0
                    && pollers
                        .get(&run_id_for_exit)
                        .is_some_and(|existing| existing.same_channel(sender))
                {
                    pollers.remove(&run_id_for_exit);
                    true
                } else {
                    false
                }
            };
            while let Some(event) = poll_stream.next().await {
                let is_terminal = api::is_terminal_event_kind(event.kind.as_str());
                // Exit early when no subscriber remains; the exit above is
                // race-free, so a concurrent attach keeps this task alive
                // instead of stranding on an exited poller (E12).
                if should_exit(&sender) {
                    return;
                }
                // A send fails only with zero receivers. A concurrent
                // attach that landed between the check and the send made
                // the send succeed, so a failure still means receiverless:
                // re-confirm under the lock and exit, otherwise redeliver
                // the still-owned event to the newcomer instead of
                // dropping it.
                if let Err(error) = sender.send(event) {
                    if should_exit(&sender) {
                        return;
                    }
                    if sender.send(error.0).is_err() && should_exit(&sender) {
                        return;
                    }
                }
                if is_terminal {
                    break;
                }
            }
            // Terminal runs never need a poller again (future subscribes
            // return history-only), so remove the entry to avoid leaking one
            // sender per settled run. Non-terminal exits (shutdown close,
            // failure close) also remove when still ours so a dead channel
            // is never reused (E12).
            cleanup(&sender);
        });
        receiver
    }

    /// Event stream contract: history first, then the live tail. A
    /// broadcast lag ends the live tail with a `lagged` marker carrying
    /// the last actually-delivered seq; the client resubscribes with that
    /// seq as `Last-Event-ID` and the journal replay yields the next real
    /// event, so no event is skipped (E12a). A `stream_error` marker ends
    /// the tail after delivery so failure-close is distinguishable from
    /// terminal-close (E05). A settled run returns history only and never
    /// pends on the broadcast (E12).
    /// Cursor-0 subscription for non-SSE callers (tests, internal
    /// tails): replays the full history. SSE callers use
    /// `subscribe_with_cursor` with their `Last-Event-ID` (E12).
    pub async fn subscribe(&self, id: String) -> Result<BoxStream<'static, RunEvent>, ApiError> {
        self.subscribe_with_cursor(id, 0).await
    }

    /// Event stream from a client cursor. A cursor ahead of the known
    /// history replays from the start: the journal cannot shrink, so the
    /// client never saw those events and skipping them would lose real
    /// events forever (E12). A cursor behind replays from the journal;
    /// markers always pass.
    /// Cursor-failure policy (E12): `Last-Event-ID` is client-controlled
    /// (`run_detail` is FOREIGN). Empty, missing, or garbage cursors fail
    /// toward REPLAY (never skip): unparseable values read as 0 (full
    /// replay) and future values clamp to 0 below, so a corrupt cursor can
    /// only duplicate (filtered by seq) never lose real events.
    pub async fn subscribe_with_cursor(
        &self,
        id: String,
        after_seq: u64,
    ) -> Result<BoxStream<'static, RunEvent>, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        // Shared mode always follows the durable journal (~250ms poll) so
        // every subscriber observes identical progress even when ownership
        // changes mid-run (HITL hand-off); only an Exclusive store owns its
        // local broadcast, so the mode check alone decides here (E12b).
        // This is a deliberate correctness-first trade-off, not a missing
        // optimization: attaching shared subscribers to a local broadcast
        // would pin them to one owner generation and stall them across a
        // hand-off, while the journal poll reflects every generation by
        // construction. The cost is bounded poll latency, never staleness:
        // an HITL answer lands on both streams up to one 250 ms tick plus
        // dispatch later, and liveness timeouts (not latency bounds) are
        // what tests pin (E12).
        // Disk-only runs (no memory record) have no live channel and always
        // use the shared poller. A missing memory record is an expected
        // fallback to the shared poller, not a hidden failure (E12).
        // Store-lock participation (E12): this subscribe path deliberately
        // does NOT join the runs-directory store lock. The store lock
        // serializes store writers (exclusive boot vs shared peer boots,
        // held for the process lifetime at construction); subscribing is a
        // read-only observation that must keep serving while any owner
        // writes. Per-run authority stays with the execution lease plus the
        // journal lock, and history always re-derives from journal truth,
        // so an unscannable journal fails the subscribe instead of serving
        // a stale "no events" view.
        // No pre-created poller exists here (E12): the shared poller is
        // created AFTER the history read and terminal check below with the
        // clamped history end, so terminal-known-upfront streams never pay
        // a zero-receiver futile poll, and the start seq never uses the
        // pre-clamp cursor (which would miss events on future cursors).
        // The Exclusive live receiver stays pre-attached (needed to avoid
        // losing broadcasts between history read and subscribe); shared
        // mode has no live broadcast to pre-attach.
        // The Exclusive live receiver is attached BEFORE the history read:
        // events broadcast between the history read and the subscription
        // would otherwise belong to neither half and be lost (E12). The
        // live tail below filters by the history end, so early duplicates
        // are skipped, never missed.
        let live_receiver = if self.inner.run_store_mode == RunStoreMode::Exclusive {
            self.live_receiver(&id).await.ok()
        } else {
            None
        };
        // Owner pinning for the Exclusive live tail (E12): capture the
        // memory owner at attach. An empty attach-time owner means the run
        // was rehydrated from disk but never claimed in this process yet:
        // pinning it would mistake the local spawn's owner claim for a
        // hand-off and cut resume-following streams. Only a known (non-empty)
        // owner pins; the same-process claim reuses the same broadcast
        // channel and needs no re-pin. The live tail below re-checks the
        // owner per event and ends with a `lagged` marker on hand-off, so
        // the client resubscribes and re-resolves instead of stalling on a
        // previous owner's channel. Owner ids are unique per process boot,
        // so an owner change fully captures a hand-off; same-owner restarts
        // reuse the same broadcast channel and need no re-pin. Shared-mode
        // subscribers need no pinning: the journal poll reflects every
        // generation by construction.
        let pinned_owner: Option<String> = if live_receiver.is_some() {
            self.inner
                .runs
                .read()
                .await
                .get(&id)
                .map(|record| record.owner_id.clone())
                .filter(|owner| !owner.is_empty())
        } else {
            None
        };
        let (history_last_seq, cursor, settled) =
            self.prepare_replay(run_dir.clone(), after_seq).await?;
        let history_settled = Arc::new(std::sync::atomic::AtomicBool::new(settled));
        let history_stream = self.replay_stream(
            run_dir.clone(),
            cursor,
            history_last_seq,
            history_settled.clone(),
        );
        if settled {
            return Ok(history_stream);
        }
        let lag_id = id.clone();
        // The shared poller is created AFTER the terminal check above with
        // the clamped history end (`history_last_seq`): terminal-known
        // streams return history-only without ever creating a poller (no
        // zero-receiver futile poll), and every live tail shares one start
        // seq instead of pre-clamp vs fallback inconsistency (E12). N
        // subscribers share one underlying poll task instead of each
        // spawning their own. Exclusive disk-only runs (no memory record,
        // hence no live channel) use the same lazily created poller here.
        let receiver = match live_receiver {
            Some(receiver) => receiver,
            None => self.shared_poll_receiver(run_dir, id.clone(), history_last_seq),
        };
        // Owner watch for the Exclusive live tail: `Some` only when pinned
        // at attach above. Shared-poller tails carry `None` (the journal
        // reflects every generation by construction).
        let owner_watch: Option<(LocalService, String, String)> =
            pinned_owner.map(|owner| (self.clone(), id.clone(), owner));
        let live_stream = {
            // Each subscriber filters by its own history position. Skipped
            // history must not end the stream: `scan` returning `None`
            // terminates, so an unfold loop skips stale seqs and only ends
            // after the terminal, lagged, or failure marker (E12b). The
            // poller starts at or before this subscriber's history end, so
            // the skip loop only handles overlap, not full replays (E12).
            // History carries no marker arms by construction: markers are
            // synthesized, never journaled, so the journal read cannot
            // yield them; the live tail below handles markers explicitly
            // (E12).
            let rx = BroadcastStream::new(receiver);
            futures_util::stream::unfold(
                (rx, history_last_seq, false, lag_id, owner_watch),
                |(mut rx, mut delivered_seq, mut done, lag_id, owner_watch)| async move {
                    use futures_util::StreamExt as _;
                    if done {
                        return None;
                    }
                    // Owner hand-off check for pinned Exclusive tails: when
                    // the memory owner no longer matches the attach-time
                    // owner (or the record is gone), end with a `lagged`
                    // marker at the last delivered position. The client
                    // resubscribes and re-resolves from journal truth
                    // instead of stalling on the previous owner's channel
                    // (E12).
                    if let Some((service, run_id, expected)) = &owner_watch {
                        let current = service.inner.runs.read().await;
                        let handed_off = current
                            .get(run_id.as_str())
                            .is_none_or(|record| record.owner_id != *expected);
                        drop(current);
                        if handed_off {
                            tracing::info!(run_id = %run_id, "execution owner changed during live tail; ending stream for resubscribe");
                            let lagged = RunEvent::lagged(
                                lag_id.clone(),
                                lagged_resync_seq(delivered_seq, 0),
                            );
                            return Some((
                                lagged,
                                (rx, delivered_seq, true, lag_id, owner_watch),
                            ));
                        }
                    }
                    loop {
                        let next = rx.next().await?;
                        match next {
                            // No `lagged` arm here by construction (E12):
                            // `lagged` markers are synthesized, never
                            // journaled and never broadcast through these
                            // channels — a broadcast lag surfaces as the
                            // `Err(Lagged)` arm below, and the shared
                            // poller forwards only journal events plus the
                            // `stream_error` failure marker. The journal
                            // history above carries no marker arms either.
                            Ok(event)
                                if event.seq > delivered_seq
                                    || event.kind.as_str() == "stream_error" =>
                            {
                                // The failure marker carries the last
                                // delivered seq rather than a new one, so
                                // only advance on real journal events.
                                if event.kind.as_str() != "stream_error" {
                                    delivered_seq = event.seq;
                                }
                                // End the live stream on the shared terminal set
                                // and on failure markers, so all layers agree and
                                // the stream closes even when the SSE wrapper is
                                // bypassed (E12/E05).
                                if api::is_terminal_event_kind(event.kind.as_str())
                                    || event.kind.as_str() == "stream_error"
                                {
                                    done = true;
                                }
                                return Some((
                                    event,
                                    (rx, delivered_seq, done, lag_id, owner_watch),
                                ));
                            }
                            Ok(_) => continue,
                            Err(
                                tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(
                                    skipped,
                                ),
                            ) => {
                                // Never fabricate a cursor from the dropped
                                // count: report the last position actually
                                // delivered and end the stream. The client
                                // reconnects and the journal replay resumes
                                // from that real position, so no event is
                                // skipped (E12a).
                                done = true;
                                let lagged = RunEvent::lagged(
                                    lag_id.clone(),
                                    lagged_resync_seq(delivered_seq, skipped),
                                );
                                return Some((
                                    lagged,
                                    (rx, delivered_seq, done, lag_id, owner_watch),
                                ));
                            }
                        }
                    }
                },
            )
            .boxed()
        };
        let live = futures_util::stream::once(async move {
            if history_settled.load(std::sync::atomic::Ordering::Relaxed) {
                futures_util::stream::empty().boxed()
            } else {
                live_stream
            }
        })
        .flatten();
        Ok(history_stream.chain(live).boxed())
    }
}
