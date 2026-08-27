# RunEvent Reference

This reference is generated from the OpenAPI `RunEvent` schema. Update it with
`qcg docs run-events`.

<!-- qcg-run-events:start -->
## RunEvent Reference

Generated from the OpenAPI `RunEvent` schema. Every event uses the required envelope fields `seq`, `ts`, `run_id`, `kind`, and `data`; `path` is present for node-scoped events. Unknown `kind` values are preserved with opaque `data`.

| Event | Required `data` fields |
|---|---|
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
| `llm_call` | `provider`, `tokens`, `cost_microusd` |
| `llm_validation_failed` | `attempt`, `message` |
| `tool_call` | `tool`, `id`, `ok` |
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
