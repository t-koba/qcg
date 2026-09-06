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
| `step_retry` | `attempt`, `max_attempts`, `error` |
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

## Durability records

The following journal records are not typed `RunEvent` variants; they travel
as opaque `Unknown` kinds and exist so restarts and shared-store peers resume
identical work. `user_answered` carries `question_id` and `values`,
`user_confirmed` carries `confirmation_id` and `approved`, and
`user_cancel_requested` carries `run_id`. `mcp_input_pending` carries `node`,
`pending_key`, `question_id`, `server`/`tool` (direct calls) or `alias`
(agent calls), `arguments`, `request_state`, and `input_requests` so a resumed
MCP call answers the original remote request instead of starting a new one.

## Cost metrics

Every terminal event (`run_finished`, `run_error`, `run_canceled`,
`run_interrupted`) carries a `metrics` object with accumulated step counts,
`llm_calls`, `tokens_input`, `tokens_output`, `tokens_cached_input`,
`tokens_total`, `cost_microusd`, and `duration_ms`. Totals on failed or
canceled runs therefore stay queryable without replaying `llm_call` rows.

`llm_call.tokens` reports `input`, `output`, `reasoning` (included in
`output`), and `cached_input` (included in `input`) as parsed from the
provider response. Costs are `microUSD` integers computed per call from the
contract unit prices (`input_cost_per_million_usd` /
`output_cost_per_million_usd`, USD per 1M tokens): `USD = cost_microusd /
1_000_000`. Cached input is billed at the input rate; cache discounts are not
modeled. Unit prices are contract-declared estimates, not provider invoices.
Calls without resolvable pricing contribute `0` unless a cost budget forces an
error, so totals mixing priced and unpriced calls may understate spend.
