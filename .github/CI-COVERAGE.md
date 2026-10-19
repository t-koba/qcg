# CI / test risk & coverage inventory

Evidence-backed map of what each CI job and Rust test group validates.
Documentation only: it changes no commands, matrices, tests, or coverage.

- Snapshot: 2026-10-20 (Asia/Tokyo), Linux container listing.
- Test-list source: `cargo test --workspace --locked --offline -- --list`
  → **1,428 *listed* tests across 21 targets** (see §3). Re-run that command
  to refresh. "Listed" is deliberate: this is the `-- --list` registry on
  Linux, not a claim that 1,428 runtime tests execute on every platform —
  it includes the ignored `read_store::benchmarks::read_view_performance_matrix`
  benchmark, which normal `cargo test` skips. Unix-gated tests do not
  register on Windows builds (see §4), and the
  `mcp` `tests/all` target lists 0 tests by default (by design, see §3/§4).
- No matrix/job/test removal is justified by this inventory alone.

## 1. Expanded CI job executions

`ci.yml` runs on `main`/`mizu` pushes, pull requests, and dispatch (7 executions):

| # | Job | Runner(s) | What runs | Risk / validation scope |
|---|-----|-----------|-----------|---------------|
| 1–3 | `rust` (matrix) | ubuntu-latest, macOS-latest, windows-latest | `scripts/check-core.sh` + `scripts/check-dist-bundle.sh --dependencies-installed` | Same commands on 3 OSes. Platform-specific filesystem and process behavior (concrete source/test examples in §4) and archive tooling are checked per OS; whether the `fmt`/`clippy` repetition itself adds signal beyond one OS is unresolved (see §6). |
| 4 | `demo-frontend` | ubuntu-latest | `generate:api`, `generate:wasm`, `npm audit`, `check:api`, `check` (svelte-check), `vitest run`, `vite build` | Generated TS/WASM inputs match the Rust spec; frontend typecheck/unit/build health; known-vulnerable npm deps. |
| 5 | `e2e` (needs demo-frontend) | ubuntu-latest | `node scripts/e2e-ui-playwright.mjs` (builds `qcg`, runs Playwright Chromium) | Real browser against real server in two modes: vite-proxy and serve-assets. Browser-level UI assertions, beyond frontend unit/build checks. |
| 6 | `msrv` | ubuntu-latest, Rust 1.99.0 | `cargo check --workspace --locked` | Compile-only floor: workspace still builds on the minimum toolchain. No runtime assertions. |
| 7 | `security` | ubuntu-latest | `cargo audit`, `cargo deny check`, `cargo machete` | Advisories (audit), license/ban policy (deny), unused-dependency policy (machete). Supply-chain policy, not product behavior. |

`release.yml` (NOT part of PR/base CI; tags `v*` or dispatch only):

| Job | Runner(s) | What runs | Risk / validation scope |
|-----|-----------|-----------|---------------|
| `bundle` (matrix ×3) | ubuntu/macos/windows | `verify-version`, pinned `wasm-bindgen-cli`, `package-dist.sh --out-dir dist`, `check-dist-bundle.sh --archive`, upload artifacts | Per-OS release artifact builds, version-tag consistency, and archive smoke (see §5). |
| `github-release` | ubuntu-latest | `softprops/action-gh-release` | Publishing only; no validation risk of its own. |

## 2. `scripts/check-core.sh`: command → risk

Canonical Rust/API/SDK verification shared by local runs and all 3 OS jobs.

| Command | Risk validated |
|---------|----------------|
| `node --test scripts/{product,capabilities,spdx,rust-line-budget}.test.mjs` | Product/capability/SPDX/line-budget JS oracles pass. |
| `node scripts/rust-line-budget.mjs` | Rust file-size budget enforced. |
| `cargo fmt --all -- --check` | Formatting drift. Cross-OS repetition value unresolved (§6). |
| `bash scripts/check-generated-docs.sh` | Generated `step-schemas`/`run-events`/`openapi.json` match `docs/`; local doc links resolve. |
| `bash scripts/check-capability-matrix.sh` (`cargo test --workspace -- --list` + `check-capabilities.mjs`) | Names explicitly named in `docs/capability-matrix.md` remain registered in the Cargo test listing for the current build (with a `#[cfg(unix)]` allowance on Windows). It does NOT check the reverse direction: unmapped registered tests do not fail. |
| `bash scripts/check-sdk.sh` | Checked-in `clients/` match the generator (freshness/diff). |
| `bash scripts/check-sdk-behavior.sh` | Generated clients actually work: Python SSE delivery/framing + chunk matrix over a live server; TS client SSE/redirect behavior (G04/G05). Freshness ≠ behavior; neither subsumes the other. |
| `python3 scripts/comparison/test_run_cleanup.py` | Run-cleanup comparison oracle. |
| `bash scripts/check-third-party-notices.sh` | `THIRD-PARTY-NOTICES` completeness. |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Lints over all targets (unit + integration + bins). MSRV job keeps a separate plain `cargo check`. Cross-OS repetition value unresolved (§6). |
| `cargo test --workspace --locked` | Default-feature suite: 1,428 listed tests on Linux (§3). |
| `cargo test -p mcp --no-default-features` | mcp without `mcp-oauth`: 29 listed (vs 32 default). 4 OAuth/session tests drop out (`authorization_cannot_be_cleared_while_sessions_are_active`, `authorization_status_does_not_perform_network_discovery`, `keyring_unavailable_stays_actionable_while_protocol_errors_stay_opaque`, `oauth_redirect_requires_https_or_loopback_http`) and 1 no-OAuth assertion appears (`oauth_specs_are_rejected_without_the_oauth_feature`). |
| `cargo test -p server --no-default-features` | server without `server-cors`: 111 listed, identical registered names to default. Both configurations run tests; the CORS implementation difference is compile-gated, so identical names do not prove identical exercised branches. |
| `bash scripts/check-fixtures.sh` | Example/fixture smoke stays green. |
| `bash scripts/package-dist.sh --dry-run` | Bundle shape/packaging logic without producing artifacts. |
| Linux-only: `cargo test -p mcp --features mcp-test-server` | Live MCP servers (stdio via the `mcp_stdio_test_server` binary + streamable HTTP in-process). 46 listed in-package: 32 lib + 14 integration (`http` 10 + `stdio` 4). Real-transport assertions. |
| Linux-only: `cargo test -p mcp --no-default-features --features mcp-test-server` | Same live-server coverage without OAuth: 43 listed (29 lib + 14 integration). |
| Linux-only: `bash scripts/e2e-server-smoke.sh` | Boots `qcg serve`, asserts `healthz`, openapi, generators list/detail (`package_sha256` shape), served assets, and a full run submission lifecycle over HTTP. |

## 3. Rust test targets (default features): 1,428 listed, 21 targets

Package/target attribution comes from `cargo metadata --no-deps --offline`
and the package manifests (including `tests/integration/Cargo.toml`). Each
target was independently listed with `cargo test -p PACKAGE --lib` (libraries),
`--bin qcg` (CLI), or `--test TARGET` (integration targets), followed by
`--locked --offline -- --list`. Groups below partition those Linux listings
within each target; counts sum to each target total. Representative evidence
uses actual leaf test names; resolve them under the row's prefix in that target.
Top-level `tests::*` buckets include only direct children, not nested modules.
Counts describe registration, not execution or unique coverage.

### `api` lib — 15

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `events::tests` (15) | Closed event payload decoding: unknown-field rejection, flat→typed mapping | `known_event_data_rejects_unknown_fields`, `run_event_decodes_flat_step_finished_into_closed_data` |

### `api` `tests/all` — 15

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `schema_components` (13) | OpenAPI/route-error/schema snapshot fidelity | `every_route_documents_exact_declared_errors`, `openapi_path_index_matches_snapshot`, `request_body_defaults_stay_optional_for_generated_clients` |
| `run_event_reference` (2) | Generated run-events doc fidelity | `run_event_reference_matches_generated_markdown`, `cache_creation_usage_is_reported_in_event_schema_and_roundtrips` |

### `cli` bin (`qcg`) — 102

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `cli::install` (67) | Install/commit/stage/backup/lock lifecycle incl. crash recovery | `backup_cleaning_collision_survives_retry_maintenance_and_uninstall`, `failed_commit_restores_old_version_and_converges_on_rerun`, `backup_with_missing_target_recovers_before_sweep` |
| `cli::skill` (5) | Skill library validation UX (CLI layer; parser risk lives in `contract`) | `rejects_a_skill_without_description`, `validates_a_single_skill_directory` |
| `cli::dev` (3) | Dev watcher/args | `dev_arguments_default_and_reject_zero_watch_interval`, `poll_watcher_reports_only_mtime_changes` |
| `cli::eval` (3) | Eval oracles | `event_sequence_tolerates_noise_but_count_cap_catches_extra`, `metric_max_reads_run_finished_only_and_fails_closed_otherwise` |
| `cli::gc` (3) | GC retention | `failed_runs_use_separate_retention_budget_from_successful_runs` |
| `cli::package_cmd` (3) | Packaging symlink/SBOM/mode safety | `pack_refuses_a_planted_symlink_in_the_input`, `pack_records_sbom_modes_and_publishes_final_output_mode` |
| `registry::tests` (3) | Registry trust | `index_urls_reject_plain_http`, `package_signatures_verify_against_trusted_keys` |
| `cli::inputs` (2) | Confirmations parsing | `confirmation_ids_reject_two_part_forms` |
| `cli::new` (2) | Skeleton generation | `new_generator_creates_valid_skeleton` |
| top-level `tests::*` singles (11) | Packaging/verify/plan/replay/GC/pricing display | `package_verification_accepts_pinned_hash_and_ed25519_signature`, `plan_command_allowlist_matches_exact_and_wildcard_args`, `replay_seed_is_read_from_original_llm_call` |

### `cli` `container_timeouts` — 1

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| top-level (1) | Daemon-policy container gating before work/bind | `invalid_daemon_policy_blocks_container_consumers_before_work_or_bind` (file `crates/cli/tests/container_timeouts.rs`) |

### `cli` `invalidation` — 1

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| top-level (1) | Invalidation CLI round trip incl. GC lock | `invalidation_cli_round_trip_conflict_status_delete_and_gc_lock` (file `crates/cli/tests/invalidation.rs`) |

### `container` lib — 37

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `session::cleanup_stderr_tests` (16) | Async cleanup/stderr reaping budgets | `inherited_stderr_cannot_extend_async_cleanup_budget`, `canceled_admin_retains_reaping_ownership_after_runtime_destruction` |
| `session::tests` (10) | Session lifecycle classification | `create_stages_classify_existing_names_without_touching_other_stages` |
| `plans::tests` (2) | Instance naming | `generated_instance_names_are_distinct_and_safe` |
| `session::daemon_policy_tests` (1) | Daemon policy reaches every cleanup path | `captured_policy_reaches_every_cleanup_path` |
| top-level `tests::*` singles (8) | Backend argv/image/instance-shape pinning | `docker_run_argv_matches_the_hardened_shape`, `incus_lifecycle_argv_is_fully_pinned`, `pinned_image_split_accepts_fingerprints_only` |

### `contract` lib — 95

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `manifest::tests` (39) | Manifest validation | `agent_failure_policy_returns_recoverable_errors_and_propagates_run_boundaries` |
| `skill::tests` (14) | Skill front-matter parsing/validation | `parses_skill_front_matter`, `rejects_unterminated_front_matter`, `reports_duplicate_keys_as_warnings` |
| `expr::tests` (13) + `expr::bag` (2) | Expression evaluation/repair | `expression_corpus_bool_cases`, `canonical_outcomes_resolve_repair_paths_and_patch_clears_reason` |
| `graph::tests` (6) + `graph::block_needs_tests` (1) | DAG validation | `join_all_reports_every_terminal_dependency_problem`, `foreach_block_children_must_not_declare_needs` |
| `manifest::flow` (6) | Flow retry/routing bounds | `block_children_share_flow_retry_bounds` |
| `manifest::hooks` (5) | Hook shadowing/suspension rules | `hook_cannot_shadow_a_flow_node_or_another_hook` |
| `manifest::resources` (3) | Container image/runtime rules | `container_image_rules_follow_the_declared_runtime` |
| `path::tests` (3) | Symlink/traversal rejection before filesystem touch | `rejects_symlink_escape_even_when_the_textual_path_is_relative` |
| `manifest::llm` (1), `manifest::validate` (1), `schema::tests` (1) | Prompt-cache policy, input options, field-type custom kinds | `prompt_cache_policy_requires_an_explicit_value`, `inputs_reject_dynamic_options` |

### `engine` lib — 337

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `engine::tests` (97) | Run execution core: crash/replay/token/budget gaps | `acrfence_crash_after_commit_and_token_resurrection_replay_gap`, `active_direct_short_episodes_preserve_subseconds_and_pin_mode` |
| `gateway::tests` (44) | Gateway fs/command guards | `bounded_tree_stats_rejects_symlinks_limits_and_swapped_parents`, `atomic_write_replaces_terminal_symlink_target_safely` |
| `journal::tests` (22) + `journal::writer` (5) | Journal durability/digests/audit degradation | `agent_checkpoint_is_durable_and_cleared_by_step_completion`, `audit_digest_replaces_the_payload_with_its_digest` |
| `engine::run_context` (19) | Resume/approval scope across invocations | `agent_call_id_guard_covers_every_resume_flow` |
| `gateway::fs` (17) | Fence share/revoke | `fence_starts_armed_and_revokes_once` |
| `gateway::http` (16) | Egress allowlist incl. DNS preflight | `any_bad_address_in_the_set_denies_the_hop` |
| `llm_gateway::tests` (14) + `llm_gateway::complete` (6) | Routing/pricing/budget before transport | `cache_categories_price_once_and_enforce_post_usage_budget`, `insufficient_run_tokens_reject_before_transport` |
| `engine::elapsed` (10) | Clock/identity init | `backward_clock_and_overflow_refuse_without_closing` |
| `engine::replay` (10) | Resume refusal on missing history | `historical_pins_missing_blobs_refuse_resume` |
| `state::tests` (10) | Budget/blob retention typing | `budget_charged_amount_must_typecheck` |
| `resource::skill` (9) | Skill discovery bounds (engine side; contract side is `contract` `skill::tests`) | `discover_library_enumeration_honors_max_entries` |
| `validation::validation_rules_tests` (8) | Schema findings bounds | `additional_properties_false_rejects_unknown_keys` |
| `engine::run` (7) | Run start/budget | `fresh_run_gets_the_full_budget` |
| `artifact::tests` (6) | Glob safety | `artifact_glob_distinguishes_segment_and_recursive_wildcards` |
| `resource::tests` (6) | Directory hashing bounds | `directory_limits_count_empty_directories_and_bound_depth` |
| `secret::tests` (6) | Secret detection/redaction | `debug_redacts_secret_values` |
| `gateway::decider` (5) | Destination decisions | `destination_decision_writer_maps_journal_errors_to_node_failures` |
| `step::tests` (5) | Template size limits | `template_service_rejects_context_larger_than_runtime_limit` |
| `gateway::error` (3) | Error prefix pinning | `generic_error_pins_both_prefixes` |
| `engine::types` (2) | Failure-code distinctions | `failure_codes_distinguish_cancel_timeout_elapsed_and_execution` |
| `gateway::command` (2) | Daemon policy freeze + argv redaction | `gateway_daemon_policy_is_frozen_and_not_a_workload_limit`, `command_plan_redacts_secrets_but_binds_full_argv` |
| `mcp_form::tests` (2) | Elicitation form order/shape | `elicitation_form_preserves_caller_order_and_schema` |
| `sources::tests` (2) | Link sources/query hygiene | `preserves_public_query_and_removes_credential_query` |
| `validation::tests` (2) | Schema validator recursion | `schema_validator_reports_missing_required_property` |
| `engine::execute` (1), `engine::foreach` (1) | Wave-error markers, sibling drain | `combined_wave_errors_preserve_resumable_agent_marker`, `drain_absorbs_completed_siblings_and_ignores_the_rest` |

### `expr-wasm` lib — 6

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| top-level `tests::*` singles (6) | WASM bool-eval parity with contract semantics + context limits | `eval_bool_json_agrees_with_the_shared_corpus`, `eval_bool_json_rejects_context_over_byte_limit` |

### `files` lib — 19

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `anchored::tests` (7) | Hash-bound patch edits | `base_mismatch_fails_before_anchor_checks`, `bottom_up_multi_edit_keeps_line_numbers_stable` |
| top-level `tests::*` singles (12) | Hash/nofollow/walk/atomic-write symlink & staging safety | `hash_refuses_a_symlink_leaf_without_following_it`, `walk_dir_never_follows_symbolic_links`, `write_file_atomic_replaces_destination_content` |

### `integration-tests` `tests/all` — 61 (package `integration-tests`, `tests/integration`)

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `examples` (59) | End-to-end generator/run behaviors: skill activation, secret redaction/caching, HTTP assets/SSE, fake-agent LLM delegation, out-of-contract modes, repair, run refs | `agent_output_guardrail_rejection_finishes_without_caching`, `direct_run_events_match_journal_event_sequence`, `skill_library_child_without_description_fails_contract_validation` |
| `self_hosting` (2) | Generator self-reproduction + LLM-mode proposal acceptance | `generator_reproduces_itself_and_the_clone_reproduces_again`, `llm_mode_accepts_a_packaged_proposal` |

### `llm` lib — 127

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `catalog::tests` (14) | Provider catalog/cache source hygiene | `cache_from_other_sources_is_ignored`, `cache_save_failures_are_reported_not_silent` |
| `text_gate::tests` (9) | Stream text framing | `clean_stream_publishes_with_bounded_holdback` |
| `http_provider::loopback_stream_tests` (7) | Loopback streaming incl. prompt-cache error | `clean_stream_publishes_in_full` |
| `decision::tests` (5) | Answer envelopes | `rejects_invalid_answers_and_usage` |
| `payload::tests` (5) | Prompt-cache keys | `auto_cache_adds_a_stable_openai_prompt_cache_key` |
| `search::tests` (5) | Registry guidance | `missing_registry_has_actionable_guidance` |
| `tests::cache_usage` (4) | Cache-rate validation | `cache_rates_validate_and_preserve_explicit_zero_in_model_config` |
| `http_provider::stream_credential_tests` (1) | Split/escaped reflection | `split_and_escaped_credential_reflection_is_detected` |
| top-level `tests::*` singles (77) | Endpoints/auth/schema/retry/redirect/parsing/budget/router/registry | `credentialed_http_requires_https_except_for_loopback`, `redirects_are_not_followed`, `router_retries_retryable_provider_error` |

### `llm-steps` lib — 134

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `agent_runtime::tests` (16) | Agent execution detail binding | `agent_and_plain_http_details_are_cross_equal`, `agent_failure_codes_distinguish_timeout_and_elapsed` |
| `agent::tests` (14) | Agent orchestration/suspension | `answered_builtin_suspension_resumes_with_the_same_call_id` |
| `tool_events::tests` (13) | Tool event redaction/bounds | `tool_call_event_redacts_secrets_from_journal` |
| `agent_runtime::approval_e2e` (9) | HITL approval + MCP sidecar redaction/retry | `approved_fs_write_executes_original_content_once`, `verified_mcp_sidecar_executes_original_bytes_and_redacted_success_resends` |
| `decide::tests` (8) | Routing decisions | `decision_total_ceiling_is_independent_of_global_chat_output_controls` |
| `repair::tests` (7) | Repair flows | `changed_snapshot_refuses_self_repair_write_back` |
| `mcp_tools::tests` (3) | MCP tool shaping | `decider_defers_static_miss_but_never_widens_narrowing` |
| `request_policy::tests` (3) | Request policy layering | `dynamic_effort_conflicts_with_same_layer_sampling_controls` |
| `skill_tool::tests` (3) | Skill-as-tool invocation | `skill_catalog_spec_enforces_the_same_explicit_bound` |
| `agent_tools::tests` (2) | Agent tool schemas | `ask_user_schema_accepts_notes_through_real_validation_and_id_path` |
| `prompting::tests` (1), `routes::tests` (1) | Prompting/routes | `truncate_with_digest_never_panics_on_any_boundary`, `explicit_contract_rates_override_catalog_field_by_field` |
| top-level `tests::*` singles (54) | Schema/credential/budget/guardrail/elicitation/web-search/transport strictness | `builtin_guardrails_validate_and_detect_violations`, `web_search_requires_credential_and_network_permission`, `mcp_schema_validation_supports_internal_references_and_composition` |

### `mcp` lib — 32

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `bounded_stdio::tests` (8) | Child-process line discipline/reaping | `line_reader_rejects_oversized_messages_before_forwarding_the_chunk`, `close_reaps_a_gracefully_exited_child_without_descendants` |
| `bounded_http::tests` (2) | Content-type/protocol errors | `content_type_matching_uses_the_complete_media_type` |
| `mcp::runtime` (2) | Reservation counts | `reservation_returns_count_on_drop` |
| top-level `tests::*` singles (20) | Allowlists/IPv6/profiles/credentials/OAuth/daemon/bounds | `destination_allowlist_accepts_hosts_and_wildcard_only`, `oauth_redirect_requires_https_or_loopback_http`, `mcp_daemon_policy_reaches_provision_and_failed_connection_cleanup` |

### `mcp` `tests/all` — 0

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `http` + `stdio` modules (0 listed) | Live-server assertions exist only under `mcp-test-server`; both files start with `#![cfg(feature = "mcp-test-server")]`, so the target compiles but registers nothing by default | (none by design; see §4 for the 14 live tests) |

### `model` lib — 4

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| top-level `tests::*` singles (4) | Base64 policy + file-value validation | `base64_policy_is_canonical_padded_everywhere`, `file_value_requires_exclusive_content_and_safe_name` |

### `policy` lib — 46

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `schema::tests` (10) | Closed-schema/hostile-pattern bounds | `bounds_hostile_nested_quantifiers`, `accepts_a_small_closed_schema` |
| `credential::tests` (9) | Credential redaction/URL hygiene | `all_query_values_are_redacted_but_keys_stay_visible` |
| `audit::tests` (7) | Audit classification/floors | `durable_class_override_is_rejected` |
| `path::tests` (4) | Traversal/component-prefix rules | `prefix_matches_components_not_string_prefixes` |
| `container::tests` (3) | Container timeout knobs | `defaults_overrides_and_freeze` |
| `cost::tests` (3) | Pricing fallback | `prefers_complete_pricing_over_first_match` |
| `destination_decider::tests` (3) | Allow-verdict strictness | `only_exact_allow_verdict_passes` |
| `scheduling::tests` (3) | Elapsed/preemption modes | `preemption_mode_is_strict_and_boundary_by_default` |
| `service::tests` (2) | Service name resolution | `service_name_resolution_order` |
| `limits::bool_env_tests` (1), `params::tests` (1) | Bool knobs, params schema closure | `bool_knobs_reject_garbage_instead_of_folding`, `params_schema_is_closed_with_required_keys` |

### `server` lib — 111

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `server::idempotency` (27) | Idempotent start/claim lifecycle | `commit_requires_a_live_matching_claim`, `invalid_idempotency_policy_names_its_variable` |
| `server::serve` (26) | Serve/abort/drain lifecycle | `abort_handles_survive_graceful_drain`, `abort_path_releases_engine_tasks_and_the_store_lock` |
| `server::rate_limit` (9) | Rate limiting incl. healthz exemption | `bucket_admits_the_burst_then_recovers_with_time` |
| `server::run_detail` (9) | Run-detail/journal validators | `body_etag_is_the_exact_body_digest` |
| `server::otlp` (8) | Collector outcomes/policy | `collector_outcomes_follow_the_defined_policy` |
| `server::middleware` (7) | Hosts/assets auth surface | `allowed_hosts_derive_from_loopback_origins` |
| `server::config` (2) | Flag defaults/rejection | `max_request_bytes_flag_falls_back_to_default` |
| top-level `tests::*` singles (23) | Concurrency/idempotency/ETag/routing/shutdown/OAuth/invalidation | `concurrent_idempotent_start_creates_one_run`, `router_implements_every_documented_route`, `shutdown_rejects_mutating_requests_and_closes_sse` |

### `service` lib — 216

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `tests::fork_adoption` (25) | Fork/adoption/rehydration after shutdown | `a_rebuilt_service_rehydrates_a_waiting_run_after_shutdown` |
| `tests::admission` (24) | Admission/concurrency convergence | `concurrent_admission_of_one_run_id_waits_then_converges` |
| `run_dirs::tests` (22) | Run-dir lifecycle | `acquired_execution_lease_remains_held_after_shutdown` |
| `tests::execution` (21) | Execution paths | `agent_ask_user_questions_are_scoped_per_call` |
| `package::tests` (19) | Package handling | `copy_dir_all_preserves_nested_package_identity` |
| `tests::cancel` (17) | Cancellation | `accepted_cancel_converges_to_terminal_without_a_second_cancel` |
| `tests::scheduling` (9) | Scheduling | `active_open_prompt_crash_recovers_before_publishing_waiting` |
| `tests::shared_subscribe` (9) | Shared subscribe | `concurrent_conflicting_answers_journal_once` |
| `read_store::tests` (8) | Read-store bounds | `concurrent_reads_coalesce_and_append_applies_only_new_events` |
| `tests::endpoints` (7) | Endpoint behaviors | `concurrent_hitl_sessions_keep_answers_and_artifacts_isolated` |
| `tests::package_pinning` (7) | Package pinning | `checkpoint_failure_cleans_only_its_fresh_target` |
| `lifecycle::shared_recovery_tests` (6) | Shared recovery | `aborting_supervisor_does_not_detach_shared_worker` |
| `run_refs::tests` (6) | Run refs | `invalidation_selectors_refuse_or_fall_back_and_corruption_fails_closed` |
| `summaries::gc` (6) | GC summaries incl. admission skip | `admission_owned_candidate_is_skipped_without_consuming_keep_slots` |
| `tests::invalidation` (6) | Invalidation | `invalidation_allows_forks_without_inheriting_metadata` |
| `tests::artifacts` (5) | Artifacts | `artifact_zip_contains_manifest_artifacts` |
| `runs_api::preemption` (3) | Preemption API | `gc_first_preemption_cannot_recreate_deleted_metadata` |
| `summaries::metrics` (3) | Metrics summaries | `cancelled_shutdown_ends_journal_poll_before_the_first_tick` |
| `runs_api::lagged_tests` (2) | Lagged runs API | `broadcast_overflow_reports_delivered_not_skipped` |
| `tests::misc` (2), `tests::providers` (2) | Misc/providers | `e05_direct_run_refuses_during_shutdown`, `explicit_providers_path_is_loaded_without_fallback` |
| `event_reader::tests` (1) | Event-reader bounded batching / partial-line deferral | `merged_batches_are_bounded_and_incomplete_lines_wait` |
| `invalidation::tests` (1) | Atomic no-clobber invalidation publication / restart readability | `invalidation_publication_is_atomic_no_clobber_and_restart_readable` |
| `queue::tests` (1) | Priority eviction only when the queue is full | `preemption_evicts_lowest_priority_only_when_full` |
| `read_store::benchmarks` (1) | Read-view performance matrix (ignored; manual measurement, not normal CI assertions) | `read_view_performance_matrix` |
| `runs_api::query` (1) | Folded step summary projection / canonical paths | `step_summaries_project_folded_outcomes_and_canonical_paths` |
| `types::policy_tests` (1) | Service ceiling caps contract limits with origin | `service_ceiling_caps_contract_with_origin` |
| `types::send_probe_tests` (1) | Compile-time Send bounds for service futures | `probe_send_bounds` |

### `service` `tests/all` — 17

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `service_subscribe` (8) | Subscribe replay/resubscribe/overflow | `subscribe_replays_history_and_resumes_an_orphaned_run`, `broadcast_overflow_continues_through_resubscribe` |
| `zip_entry_budget` (7) | Zip entry-budget parsing guards | `lying_counts_fail_before_extraction_even_when_unbounded` |
| `step_params_schema` (2) | Built-in step parameter schemas | `built_in_registry_exposes_parameter_schemas_for_all_steps` |

### `steps` lib — 52

| Group (n) | Risk | Representative actual tests |
|-----------|------|-----------------------------|
| `http::tests` (10) | Step HTTP surface | `approval_guard_execute_share_one_details_object` |
| `ask_user::tests` (8) | ask_user localization/values | `ask_user_preserves_localized_option_labels_without_changing_values` |
| `checks::tests` (7) | Check steps | `base64_decode_applies_requested_unix_mode_after_commit` |
| `common::output_expect` (7) | Output expectations | `absent_expect_yields_no_findings` |
| `mcp_call::direct` (7) | Direct MCP call continuation/forms | `absent_continuation_starts_fresh`, `continuation_keys_bind_the_full_invocation`, `corrupt_or_replayed_continuation_is_refused` |
| `common::base64` (4) | Base64 handling | `short_read_decoding_round_trips` |
| `check_container::tests` (2), `check_tool::tests` (2) | Container/tool checks | `check_container_invocation_preserves_declared_mounts`, `disabled_tool_fallback_does_not_request_confirmation` |
| `common::unix_mode` (1), `foreach::tests` (1) | Unix mode, foreach | `world_writable_modes_are_refused`, `foreach_rejects_iteration_and_parallelism_above_runtime_bounds` |
| `mcp_call::step` (1), `mcp_call::tests` (1) | Destination policy, source extraction | `direct_destination_policy_denies_unlisted_fetch_but_passes_query`, `direct_mcp_source_extraction_keeps_text_and_resource_sources` |
| `transform::tests` (1) | Directory zip transform | `zip_transform_preserves_directory_entries_metadata_and_contents` |

Execution context: the default suite runs on all 3 OSes via the `rust`
matrix, subject to ignore attributes and platform gating. The no-default-feature
runs in §2 also run on all 3 OSes; only rows marked Linux-only are restricted
to ubuntu-latest by `check-core.sh`.

## 4. Feature / platform configurations

| Configuration | Effect on coverage | Runtime vs compile-only |
|---------------|--------------------|-------------------------|
| `mcp` default (`mcp-oauth`) | 32 lib tests incl. OAuth/session paths | Runtime |
| `mcp --no-default-features` | 29 lib tests: 4 OAuth/session tests drop out, 1 no-OAuth assertion appears (net −3; §2 names the tests) | Runtime under reduced feature set |
| `server` default (`server-cors`) vs `--no-default-features` | 111 = 111, identical registered names | Runtime in both; CORS difference is compile-gated |
| `mcp --features mcp-test-server` (Linux CI only) | 46 in-package (32 lib + 14 integration: `http` 10 + `stdio` 4) | Runtime against real spawned servers via `mcp_stdio_test_server` |
| `mcp --no-default-features --features mcp-test-server` (Linux CI only) | 43 in-package (29 lib + 14 integration) | Same live-server runtime without OAuth |
| `scripts/e2e-server-smoke.sh` (Linux CI only) | HTTP-level assertions over a booted `qcg serve` | Runtime; exercises the packaged binary path, not just library code |
| MSRV `cargo check` | Whole workspace compiles on 1.99 | Compile-only; zero runtime assertions |

Notes:

- `mcp` `tests/all` listing **0 tests by default is by design** — both
  `http.rs` and `stdio.rs` start with `#![cfg(feature = "mcp-test-server")]`
  and only register tests under that feature. The empty target compiles on
  every run, but the gated module bodies are not compiled or asserted until
  the Linux-only feature rows enable them.
- Concrete platform scope (source branches are not proof that every branch
  has a runtime assertion):
  - `crates/mcp/src/bounded_stdio.rs`: `#[cfg(unix)]` creates a process
    group; the individually Unix-gated
    `bounded_stdio::tests::close_reaps_a_gracefully_exited_child_without_descendants`
    exercises child reaping via `/bin/sh`. That case is not registered on Windows.
  - `crates/service/src/invalidation.rs::open_record`: `#[cfg(windows)]`
    opens reparse points without following them and checks handle metadata;
    the non-Windows branch delegates to `files::open_read_nofollow`, whose
    Unix branch uses `O_NOFOLLOW`. The service invalidation rows cover
    publication/read behavior, not an assertion of exhaustive Windows reparse coverage.
  - `crates/server/src/server/serve.rs::shutdown_signal`: Unix waits for
    Ctrl-C/SIGTERM, Windows for Ctrl-C/console close/break. Running the matrix
    compiles these distinct branches; the listing alone does not prove
    delivery of each native signal is exercised.
- The 1,428 baseline is a Linux listing. `check-capabilities.mjs` has a
  syntactic Unix-module name scanner for its Windows allowance, not a full
  platform count oracle (it does not enumerate every individual cfg branch).
  Exact Windows/macOS listed counts and deltas were not captured here (§8).
- `rg '#\[ignore' crates tests` finds the performance benchmark in
  `crates/service/src/read_store.rs`. Its
  `read_store::benchmarks::read_view_performance_matrix` case is included in
  the 216 service library cases and 1,428 workspace listing, but ignored by
  normal `cargo test`. Run it explicitly with
  `cargo test -p service --lib --locked --offline -- --ignored --exact read_store::benchmarks::read_view_performance_matrix`
  when performance evidence is needed; this inventory did not execute it.

## 5. Non-Rust checks and their risks

- Frontend (`demo-frontend` + e2e): `generate:api`/`generate:wasm` (deterministic regeneration from the Rust
  spec), `check:api` (generated types match), `check` = `svelte-check`, `vitest run` (frontend unit tests),
  `vite build`, `npm audit --audit-level=moderate`. E2E runs both `vite-proxy` and `serve-assets` modes in
  `scripts/e2e-ui-playwright.mjs`, covering proxied vs server-served UI delivery.
- Node oracle suites in `check-core.sh`: product, capabilities, SPDX, rust-line-budget tests + budget script.
- `check-capability-matrix.sh`: fails when a name explicitly listed in `docs/capability-matrix.md`
  is no longer registered in the build's Cargo test listing. It does not fail on registered tests
  that have no matrix row, and it makes no platform-risk claim.
- `check-sdk.sh` (freshness: checked-in clients equal generator output) vs `check-sdk-behavior.sh`
  (behavior: live SSE/chunk-matrix/redirect/typed-reader assertions). Complementary, not duplicates.
- `check-fixtures.sh`, `comparison/test_run_cleanup.py`: fixture/example and cleanup oracles.
- `check-third-party-notices.sh`: license notice completeness.
- Dist: `package-dist.sh --dry-run` (shape logic) in core; full `check-dist-bundle.sh` (build debug bundle,
  extract, boot server, assert endpoints incl. run lifecycle) in the `rust` matrix; release `bundle` repeats
  build + `--archive` smoke per OS for the shipped artifacts plus `verify-version` tag consistency.

## 6. Overlaps

Evidence-bound (different depths, keep):

- SDK freshness vs SDK behavior; dist dry-run vs bundle verify vs release archive smoke — different depths.
- No-default runs re-execute the same lib tests minus gated features — proves the feature-off configuration
  (with the exact mcp 32→29 delta recorded in §2, and identical registered server test names).
- The `mcp` `tests/all` target is empty by default; only `mcp-test-server` compiles
  and asserts its gated live-server modules.

Unresolved (requires timing or additional coverage evidence):

- Whether 3-OS `clippy`/`fmt` adds signal over a single-OS lint plus 3-OS `cargo test`.
- Whether `demo-frontend`'s generate steps duplicate e2e's regenerate, or guard different drift.
- How much `e2e-server-smoke.sh` curl assertions overlap `server` lib HTTP tests (`serve`, `run_detail`,
  `idempotency`) vs its unique value (real binary + packaging path).
- How much live `mcp-test-server` tests overlap mocked `bounded_stdio`/`bounded_http` unit tests.
- Whether MSRV `cargo check` overlaps the stable `clippy --all-targets` compile coverage for breakage detection.
- Exact Windows/macOS listed counts and per-group timing/ratcheting budgets.

No removal candidate is justified by this inventory alone.

## 7. Maintenance rule

Update the inventory when a CI job/check, matrix axis, Cargo target, feature
configuration, or behavioral module/path group is added, removed, or materially
changed. A new test inside an existing group needs no row change; the dated
per-group counts are a snapshot baseline, not a CI gate, and may be refreshed
with a later inventory review. When refreshing, re-run the workspace listing and
independent package/target listings, confirm each current name belongs to one
listed group, and re-resolve the representative leaves. Check
`rg '#\[ignore' crates tests` separately because Cargo's list output does not
label ignored tests. Update feature and target counts together, and keep
platform counts explicitly unmeasured until collected on those platforms.
Retain target→behavioral-group granularity; do not grow an individual-test
registry or add a framework.

## 8. Accounting and gaps

Independent per-package/per-target Linux listings: 15 + 15 + 102 + 1 + 1
+ 37 + 95 + 337 + 6 + 19 + 61 + 127 + 134 + 32 + 0 + 4 + 46 + 111 + 216
+ 17 + 52 = **1,428 listed cases across 21 targets**. For each target, the
§3 row counts sum to its listed total, every nonempty prefix resolves,
and every listed name belongs to exactly one row (direct-child buckets
exclude nested modules). All representative leaves resolve within their
row's target/prefix. One case is the ignored performance benchmark, not
an executed CI assertion. The zero-test MCP target is intentional (§4).
The feature-required `mcp_stdio_test_server` binary is absent from the
default target set; it supports the Linux live-server runs.

Known gaps (not claimed complete):

- Exact Windows/macOS listed counts and group deltas have not been measured;
  the Linux partition is not a platform-independent count.
- Listing proves registration, not passing runtime assertions or exhaustive
  behavior coverage. Overlapping groups need not validate unique risks.
- Test/group timings, job-minutes, and fixed/ratcheting budgets are not
  measured or established by this inventory.
