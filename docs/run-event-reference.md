# RunEvent Reference

This reference is generated from the OpenAPI `RunEvent` schema. Update it with
`qcg docs run-events`.

API-run identity events additionally carry `package_sha256`, the admitted full
package identity preserved across start, requeue, and fork. It is optional in the
schema for legacy/direct journals; the service refuses recovery or fork when it
is absent. `generator_path` remains the original source path, not the derived
per-run snapshot location. See [operations](operations.md#per-run-generator-package-snapshots).

Parallel waves emit a durable `parallel_task_joined` fact at each actual join,
using the stable `node` rather than Tokio's process-local task ID. Its
`classification` distinguishes completion, task error/cancellation, interaction,
panic, scheduler abort, and parent cancellation; `drain_cause` and
`cancellation_cause` retain why the task stopped. Wave join reports retain only
outcome and interaction IDs; they copy no prompt, answer, output, or raw error.
`foreach_sibling_drained` retains the exact iteration path and its existing
task-detail fields. The fold keeps unreported owners unresolved rather than
inventing a failed step. Resume first
surfaces their saved interactions, then refuses to re-enter an unresolved owner
unless its `StepExecutor` explicitly declares guarded replay. That admission emits
`parallel_replay_authorized` with the executor, capability, owner, and original
fact sequence; parallel safety alone does not authorize replay. An unresolved
foreach iteration is currently refused: its drain fact names the parent and
iteration, not the child executor whose effects are unknown, so the parent's
capability cannot authorize it. On parent cancellation, outcomes already
reported by joined tasks are still settled per owner before the run returns
canceled; only tasks actually aborted remain interrupted with unknown effects. These facts do not choose mixed-outcome
failure precedence or change fail-fast settlement.

Explicit continuation rule: a generic `StepError` from any node kind,
including `llm.agent`, settles as an ordinary failure and never synthesizes a
resumable continuation; node kind and checkpoint presence alone are not
continuation evidence. Typed interactions (`NeedsUser` / `NeedsConfirm`,
surfaced as `EngineError::NeedsUser` / `EngineError::NeedsConfirm`) retain
their stable question/confirmation identity across scheduler boundaries.
Unreported-task replay remains separately guarded by the executor's explicit
`replay_safe_after_interruption` capability; checkpoints alone never authorize
replay.

Direct `mcp.call` cleanup failures emit a durable `mcp_cleanup_settled` record
before settlement, alongside the observation-only `tool_call`. Both retain the
execution `status` and a bounded result (64 KiB, `truncated` flag) independently
of `cleanup_error`; the exact result stays in the operation replay state for
redispatch-free replay. `cleanup_settlement` identifies the explicitly
configured `policy` (`preserve_execution` or `fail_fast`) and
`source: node_parameter` only for a completed call or input request that uses
that policy; null means no policy was configured or the execution itself
failed, not a selected disposition. A missing policy after a completed call or
input request produces a "cleanup settlement policy required" refusal. The
operation result and `mcp_input_pending` continuation remain durable under
every disposition. Independent execution errors and optional degradation retain
their existing handling. See [`mcp.call`](contract-reference.md).

<!-- run-events:start -->
## RunEvent Reference

Generated from the OpenAPI `RunEvent` schema. Every event uses the required envelope fields `seq`, `ts`, `run_id`, `trace_id`, `span_id`, `kind`, and `data`; `path` is present for node-scoped events. Trace and span IDs use W3C-compatible hexadecimal widths. Unknown `kind` values are preserved with opaque `data`.

| Event | Required `data` fields |
|---|---|
| `elapsed_accounted` | `active_nanoseconds`, `active_started_at` |
| `run_queued` | `generator`, `generator_path`, `contract_sha256`, `inputs`, `schema_version`, `elapsed_budget_mode` |
| `run_started` | `generator`, `generator_path`, `contract_sha256`, `inputs`, `schema_version`, `elapsed_budget_mode` |
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
| `agent_file_changes` | `removed_files`, `written_files` |
| `agent_delegated` | `agent`, `tool_call_id`, `tools`, `max_calls`, `max_iterations`, `max_tokens_total`, `max_tool_calls_total` |
| `agent_completed` | `agent`, `tool_call_id`, `turn`, `tokens_total` |
| `agent_failed` | `agent`, `tool_call_id`, `code`, `action`, `message` |
| `agent_handoff` | `agent`, `tool_call_id` |
| `context_compacted` | none |
| `llm_validation_failed` | `attempt`, `message` |
| `llm_route_failed` | `provider`, `model`, `attempt`, `kind` |
| `tool_call` | `tool`, `id`, `status`, `phase`, `duration_ms`, `arguments`, `result`, `sources`, `truncated` |
| `mcp_cleanup_settled` | `tool`, `id`, `status`, `phase`, `duration_ms`, `arguments`, `result`, `sources`, `truncated` |
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
<!-- run-events:end -->

## Durability records

`agent_file_changes` is a typed durable event emitted after agent removals and writes. Its `removed_files` and `written_files` are workspace-relative per-call facts folded into per-node run state; it never carries pending side-effect payloads. `tool_call` remains observation-only and is not the durability carrier.

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
`tokens_cache_creation_input`,
`tokens_total`, `cost_microusd`, and `duration_ms`. Totals on failed or
canceled runs therefore stay queryable without replaying `llm_call` rows.

`llm_call.tokens` reports total `input`, `output`, `reasoning` (included in
`output`), `cached_input` (cache reads), and `cache_creation_input` (cache writes).
The two cache buckets are disjoint subsets of `input`, not extra tokens to add
to it. Missing optional cache counters are zero; malformed counts, overlapping
subsets, and arithmetic overflow fail closed. OpenAI Chat/Responses already
report inclusive input totals; Anthropic's ordinary input, cache reads, and
cache creation counts are summed to obtain total input, including in streams.

Costs are rounded `microUSD` integers (`USD = cost_microusd / 1_000_000`).
For the selected provider/model pricing row, the per-call estimate is:

```text
ordinary_input = input - cached_input - cache_creation_input
microUSD = ordinary_input * input_cost_per_million_usd
         + cached_input * cache_read_input_cost_per_million_usd
         + cache_creation_input * cache_creation_input_cost_per_million_usd
         + output * output_cost_per_million_usd
```

Rates are USD per 1M tokens. On the base row, each omitted cache rate falls
back to that row's ordinary input rate; explicit zero is honored. A tier row
inherits the base row's cache rate instead (see below). `require_pricing` still
requires ordinary input/output rates, not cache rates. Totals remain estimates,
not provider invoices, especially unless all actual categories are configured.
No vendor discounts or cache TTL tiers are inferred. Admission reservations
use prompt bytes/4, the maximum requested output, and the highest configured
input-category rate on the selected row; they remain heuristic because token
count and fallback usage are not known before the request. Reported usage drives
the post-call budget check.

A selected row may declare context tiers (`cost_tiers`). The tier is chosen by
the request's whole prompt input, that is `ordinary_input + cached_input +
cache_creation_input`, and the tier with the greatest threshold the request
reaches prices the whole request: no token is split across two tiers and no
token is counted twice. A tier rate wins over the base row's rate for the same
category; a rate the tier does not name inherits the base row's rate for that
category. A cache rate the tier does not name therefore inherits the base
row's cache rate, never the tier's own ordinary input rate: an ordinary input
price is not a cache price. An output rate the tier does not name falls back
to the base row's output rate. A request that stays below every threshold is
priced at the base row. Admission reservations cannot prove which tier a
request reaches, so they reserve at the dearest rate any tier on the row
names.

Calls without resolvable pricing contribute `0` unless a cost budget forces an
error, so totals mixing priced and unpriced calls may understate spend.
