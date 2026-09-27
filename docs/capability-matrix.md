# Capability Matrix

Each row maps a user-visible guarantee to the live test that covers it.
`scripts/check-capability-matrix.sh` fails when a named test no longer
exists, so a guarantee cannot be silently dropped while its claim stays
behind (ADR 0001: no inert promises).

| ID | Guarantee | Test |
|----|-----------|------|
| F01 | Graceful drain keeps abort handles and releases the store lock | `abort_handles_survive_graceful_drain` |
| F02a | Approved POST sends the original body once | `approved_post_sends_original_body_once` |
| F02b | Approved fs.write executes the original content once | `approved_fs_write_executes_original_content_once` |
| F02c | Pending sidecar restores the non-secret payload with digest binding | `pending_sidecar_restores_nonsecret_payload_with_digest_binding` |
| F03 | Discovery reflection screening rejects credential echo | `discovery_reflection_screening_rejects_credential_echo` |
| F07a | Ordinary execution errors reach failure hooks | `ordinary_execution_errors_reach_failure_hooks` |
| F07b | Warned run-started hook does not rerun after HITL resume | `warned_run_started_hook_does_not_rerun_after_hitl_resume` |
| F08 | Digest truncation never panics on any UTF-8 boundary | `truncate_with_digest_never_panics_on_any_boundary` |
| F09a | Catalog staleness returns after TTL and the view agrees | `staleness_returns_after_ttl_and_view_agrees` |
| F09b | Cache entries from other sources are ignored | `cache_from_other_sources_is_ignored` |
| F09c | Cache save failures are reported, not silent | `cache_save_failures_are_reported_not_silent` |
| F09d | View matches the runtime overlay for partial explicit config | `view_matches_runtime_overlay_for_partial_explicit` |
| F12 | Parallel HITL interruption resumes siblings | `parallel_hitl_interruption_resumes_siblings` |
| F13a | Budget charges match attempts live and after fold | `budget_charges_match_attempts_live_and_after_fold` |
| F13b | Foreach single charge matches live and fold | `foreach_single_charge_matches_live_and_fold` |
| F13c | Budget boundary exact succeeds and over fails | `budget_boundary_exact_succeeds_and_over_fails` |
| F14a | Backup with missing target recovers before sweep | `backup_with_missing_target_recovers_before_sweep` |
| F14b | Fresh marked backup survives foreign sweep | `fresh_marked_backup_survives_foreign_sweep` |
| F14c | Corrupt marked backup is never swept | `corrupt_marked_backup_is_never_swept` |
| F14d | Failed commit restores old version and converges on rerun | `failed_commit_restores_old_version_and_converges_on_rerun` |
| F06a | Concurrent same-file patches fail closed on base drift | `concurrent_same_file_patches_fail_closed_on_base_drift` |
| F06b | Commit-time recheck refuses a foreign writer | `commit_time_recheck_refuses_a_foreign_writer` |
| G01-01 | Forced cancel settles without self-conflict (no 409 without a peer) | `g01_forced_cancel_settles_without_self_conflict` |
| G01-02 | Peer execution lease blocks false local settlement | `g01_peer_execution_lease_blocks_false_settlement` |
| G01-03 | Natural completion racing cancel commits exactly one terminal | `completion_racing_with_cancel_commits_exactly_one_terminal_state` |
| G02-01 | Patch vs ordinary write serializes without losing updates | `g02_patch_vs_ordinary_write_serializes_without_loss` |
| G02-02 | Repair-style verify plus preserving write serializes vs ordinary write | `g02_repair_style_verify_then_preserving_write_serializes` |
| G02-03 | Delete/recreate keeps presence expectations through commit | `g02_delete_recreate_keeps_presence_expectation` |
| G02-04 | Guarded writer does not self-deadlock; parallelism kept | `g02_guarded_writer_does_not_self_deadlock_and_keeps_parallelism` |
| G03-01 | Accumulated admissions do not exhaust the scan budget | `g03_accumulated_admissions_do_not_exhaust_scan_budget` |
| G03-01b | Real admit plus GC plus reboot recovers under a tiny budget | `g03_gc_then_restart_recovers_under_small_budget` |
| G03-02 | Store scans skip coordination in lists and GC | `g03_store_scans_skip_coordination_in_lists_and_gc` |
| G03-02b | Shared peer init and refresh recover despite legacy locks | `g03_shared_refresh_recovers_despite_legacy_locks` |
| G03-03 | Concurrent same-run admission stays exclusive | `g03_concurrent_same_run_admission_stays_exclusive` |
| G06-01 | Patch preserves the executable bit and the script still runs | `g06_patch_preserves_executable_bit_and_runs` |
| G06-02 | Data mode kept; special bits sanitized; new files owner-only | `g06_data_mode_and_special_bits_are_sanitized` |
| G06-03 | Rejected patch leaves content and mode untouched | `g06_rejected_patch_leaves_content_and_mode_untouched` |
| G04 | SSE immediate delivery and chunk-split framing | See `scripts/check-sdk-behavior.sh` (Python/Node live-server behavior tests) |
| G05 | TypeScript redirect is finite and consistent per runtime | See `scripts/check-sdk-behavior.sh` (loop/cycle/limit/auth/release tests) |
| G07 | Capability gate and SDK freshness/behavior separation | See `scripts/check-capability-matrix.sh` and `scripts/check-sdk-behavior.sh` |
