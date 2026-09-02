# RunEvent Reference

This reference is generated from the OpenAPI `RunEvent` schema. Update it with
`qcg docs run-events`.

<!-- qcg-run-events:start -->
## RunEvent Reference

Generated from the OpenAPI `RunEvent` schema. Every event uses the required envelope fields `seq`, `ts`, `run_id`, `trace_id`, `span_id`, `kind`, and `data`; `path` is present for node-scoped events. Trace and span IDs use W3C-compatible hexadecimal widths. Unknown `kind` values are preserved with opaque `data`.

| Event | Required `data` fields |
|---|---|
| `run_queued` | `generator`, `generator_path`, `contract_sha256`, `inputs`, `qcg`, `schema_version` |
| `run_started` | `generator`, `generator_path`, `contract_sha256`, `inputs`, `qcg`, `schema_version` |
| `run_resumed` | none |
| `graph_resolved` | `nodes` |
| `resource` | `name`, `type`, `source`, `sha256`, `bytes`, `cache`, `trust`, `llm_visible` |
| `step_started` | `type`, `attempt` |
| `step_finished` | `status` |
| `step_replayed` | `status` |
| `step_skipped` | `reason` |
| `foreach_iteration` | `index` |
| `foreach_budget_exhausted` | `requested_iterations`, `executed_iterations`, `max_iterations` |
| `repair_attempt_started` | `repair`, `recheck`, `attempt`, `max_attempts` |
| `repair_attempt_finished` | `attempt`, `status` |
| `regenerate_attempt_started` | `attempt`, `max_attempts` |
| `regenerate_attempt_finished` | `attempt`, `status` |
| `llm_call` | `provider`, `model`, `max_tokens`, `tokens`, `cost_microusd` |
| `llm_delta` | `provider`, `model`, `index`, `text` |
| `agent_checkpoint` | `turn`, `phase`, `checkpoint` |
| `agent_delegated` | `agent`, `tool_call_id`, `tools`, `max_calls`, `max_iterations`, `max_tokens_total`, `max_tool_calls_total` |
| `agent_completed` | `agent`, `tool_call_id`, `turn`, `tokens_total` |
| `agent_failed` | `agent`, `tool_call_id`, `code`, `action`, `message` |
| `agent_handoff` | `agent`, `tool_call_id` |
| `context_compacted` | none |
| `llm_validation_failed` | `attempt`, `message` |
| `llm_route_failed` | `provider`, `model`, `attempt`, `kind` |
| `tool_call` | `tool`, `id`, `status`, `phase`, `duration_ms`, `arguments`, `result`, `sources`, `truncated` |
| `guardrail_evaluated` | `guardrail`, `kind`, `stage`, `passed`, `tripwire` |
| `guardrail_error` | `guardrail`, `kind`, `stage`, `error_kind`, `code`, `message`, `policy` |
| `guardrail_tripwire` | `guardrail`, `kind`, `stage`, `violation` |
| `tool_backend_resolved` | `tool`, `backend`, `argv` |
| `user_interaction` | none |
| `out_of_contract` | `policy`, `reason` |
| `confirm_request` | `confirm` |
| `side_effect` | `kind`, `target`, `decision` |
| `dry_run` | `kind`, `target` |
| `artifact` | `path`, `sha256`, `bytes`, `label`, `required` |
| `run_waiting` | `question_id`, `question` |
| `run_error` | `error` |
| `run_canceled` | `reason` |
| `run_interrupted` | `reason` |
| `run_finished` | `status`, `metrics` |
| `lagged` | `action` |
<!-- qcg-run-events:end -->
