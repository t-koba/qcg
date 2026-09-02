# Agent and MCP verification matrix

This document defines the acceptance boundary for the bounded generation
harness. A successful browser demo or end-to-end run is not sufficient: every
layer below must pass independently before the feature is considered working.

## Agent contract and state machine

| Area | Required behavior | Verification |
|---|---|---|
| Final response | Plain text is accepted only without a schema. Schema-bound output must parse as JSON and pass local JSON Schema validation. | `agent_final_schema_is_locally_enforced_after_robust_json_parsing` |
| Validation retry | Invalid final output is journaled, returned to the model as explicit correction feedback, and checkpointed before the next bounded turn. Exhaustion reports the validation error. | `parse_agent_final`, agent checkpoint tests, package validation |
| Stop reason | `end_turn` requires no tool calls; `tool_use` requires tool calls. Token exhaustion and refusal fail explicitly. | `agent_stop_reason_must_match_the_response_shape` |
| Turn and token budgets | Parent and specialist agents have independent positive iteration and aggregate-token limits. | manifest validation and agent execution tests |
| Tool-call budgets | An aggregate limit and every declared per-tool limit are charged before execution. | `agent_tool_budgets_apply_total_and_per_tool_limits` |
| Delegation | A specialist can use only named sibling tools, cannot delegate recursively, and inherits side-effect restrictions. A deterministic integration fixture executes the complete parent-to-specialist-to-parent sequence. | `specialist_agents_are_bounded_and_inherit_delegated_side_effects`, `specialist_agents_reject_unknown_and_recursive_delegation`, `llm-agent-fake` fixture smoke |
| Specialist schema | Specialist input and output schemas are package-relative, size/complexity bounded, compiled during validation, and output is validated locally. | manifest and step validation tests |
| Package paths | Prompt, response schema, specialist schema, resource, asset, and bundled executable paths are resolved through one canonical package boundary. Absolute paths, parent traversal, and symlink escapes fail before built-in loader dispatch or command execution. | `qcg-contract` path tests and `contract_load_rejects_resource_paths_before_loader_dispatch` |
| Specialist model | An override is resolved and capability-checked before the request; the parent model is not used as an intermediate validation target. | provider capability tests |
| Parallel calls | Multiple read-only calls may be returned in one model turn and are completed in deterministic order. Interactive or side-effectful calls in a parallel batch fail before execution. | specialist and side-effect tests |
| Interruption | Durable checkpoints preserve provider state, messages, token counts, and tool counts. An indeterminate side effect is never replayed automatically. | engine journal and agent checkpoint tests |
| HITL and handoff | User input, confirmation, MCP input-required, and handoff are distinct typed outcomes. Nested specialist handoff fails explicitly. | MCP form and specialist tests |

## Structured output and provider transports

| Area | Required behavior | Verification |
|---|---|---|
| Mode selection | `auto` selects native strict, native compatible, or prompt plus local validation from the schema and provider capabilities. | `explicit_native_strict_rejects_incompatible_schema_before_transport` |
| Structured output with tools | Native schema mode is used during tool-enabled turns only when the provider advertises `structured_output_with_tools`; otherwise `auto` selects prompt mode. An explicit unsupported native mode fails. | `structured_output_with_tools_respects_the_provider_capability` |
| Local validation | The complete supported JSON Schema dialect is compiled once per validation operation and reports every violation, including references, combinators, strings, numbers, arrays, and closed objects. Provider-native compatibility is a separate transport decision and never weakens local validation. | `qcg-engine` validation tests and response-schema step validation |
| OpenAI Chat | Tool call IDs, arguments, finish reasons, streaming deltas, and usage are normalized. | `qcg-llm` Chat request/response and stream tests |
| OpenAI Responses | Provider state is preserved across tool continuations and validated as an array. | `qcg-llm` Responses tests and agent checkpoint path |
| Anthropic Messages | The internal structured-response tool becomes final text and cannot be mixed with external calls; streaming and non-streaming paths agree. | `parses_anthropic_schema_tool_as_text`, `streams_anthropic_schema_tool_as_a_completed_structured_response` |
| Cancellation and limits | Requests have timeouts, bounded bodies, cancellation, and token accounting. | provider transport and engine gateway tests |

## MCP discovery and call contract

| Area | Required behavior | Verification |
|---|---|---|
| Lifecycle | Profiles choose `initialize` or `discover`; no implicit protocol fallback exists. Known public profiles are pinned to the lifecycle they actually implement. | `public_defaults_are_anonymous_and_pinned_to_exact_hosts`, live public contract test |
| Transport | Streamable HTTP and stdio perform real initialization, listing, calls, isolation, cancellation, timeout, and close/reap behavior. A malformed successful HTTP JSON body fails immediately instead of entering asynchronous task polling. | `qcg-mcp` HTTP and stdio integration tests, including `malformed_success_json_fails_immediately_instead_of_timing_out` |
| Discovery bounds | Pagination, response size, schema size, depth, node count, object width, and string length are bounded. | MCP schema and transport bound tests |
| Schema trust | Descriptions and annotations are sanitized, internal references/composition work, and external references are rejected. | `mcp_schema_removes_untrusted_annotations_without_dropping_property_names`, reference and complexity tests |
| Exact binding | A contract fixes both profile ID and remote tool name; the model sees only the declared alias. | manifest validation and `McpAgentTools::prepare` |
| Input | Model arguments are validated locally against the discovered input schema before transport. | real Parallel wire-shape and live public tests |
| Complete result | `content` must be an array. `isError` must be boolean when present. A successful typed result requires valid `structuredContent`; a typed tool error remains recoverable without it. | `mcp_result_requires_structured_content_only_for_successful_typed_results` |
| Tasks and input-required | Task polling, cancellation, supported form elicitation, stable question IDs, and durable resume are bounded. Unsupported request methods fail. | `modern_mrtr_is_exposed_for_durable_hitl_and_can_resume`, MCP input-required tests |
| Failure classification | Tool-declared errors return to the model. Transport, protocol, schema, credential-reflection, and cancellation errors fail the step explicitly. | `mcp_tool_error_is_recoverable_but_transport_error_is_not` and transport tests |
| Secret handling | Credentials never enter model-visible schemas/events, are scanned in results/errors, and are not printed by debug formatting. | qcg-mcp credential and reflection tests |

## Public Exa and Parallel profiles

The `exa-public` and `parallel-public` profiles are anonymous defaults, but a
generator must still declare exact tools and network hosts. Their acceptance
test performs real network operations and is intentionally separate from the
local transport suite:

```bash
cargo test -p qcg-llm-steps --locked \
  public_mcp_tools_accept_real_calls_and_validate_real_results \
  -- --ignored --test-threads=1
```

The test connects through qcg's MCP runtime, lists the real tools, validates
representative arguments with the discovered schemas, calls both services,
validates the actual results, extracts public source URLs, and closes both
sessions. CI runs it on Ubuntu. A captured real Parallel wire-shape test remains
in the ordinary unit suite so incompatible parsing is caught even when an
external service is unavailable.

## Observability and demo UX

| Area | Required behavior | Verification |
|---|---|---|
| Tool events | Include tool ID/name, specialist name, typed status and phase, typed error, duration, bounded arguments/result, truncation, and sanitized source links. Every issued call reaches a terminal success, failure, input-required, or confirmation-required event even when validation, guardrails, budgets, execution, or output checks fail. | tool-event terminal-path tests, `tool_call_event_preserves_details_and_sanitizes_sources`, payload bound test |
| Specialist events | Start, handoff, completion, and failure are typed events with budgets, tools, turn, token totals, and stable failure codes. | run-event schema generation and frontend type checks |
| Browser display | The event log labels tool and specialist activity and renders public HTTP(S) sources as safe external links. | Svelte type check and browser E2E |
| Artifact UX | Artifact descriptions, preview metadata, safe inline preview, download, and deterministic archive behavior remain separate from agent/MCP output. | service/API/UI tests and distribution checks |
| Demo use | The built-in generator defaults to Exa plus Parallel, assigns schemas and explicit budgets to parent and specialist agents, and validates the generated package as the source of truth. | generator validation, fixture check, UI E2E, live public contract test |

## Performance and resource boundaries

No unbounded agent or MCP loop is accepted. Turns, aggregate tokens, aggregate
and per-tool calls, HTTP bodies, schemas, discovery pages, tool event payloads,
task polling, context size, command output, elapsed time, and parallel DAG work
all have explicit limits. Every serialized provider request and the complete
agent transcript are measured, not only the latest prompt. Truncation policies
compact tool results first, then textual message content with the configured
head/tail policy while preserving system instructions, tool-call identity, and
provider state; an impossible compaction fails explicitly. Sessions
and HTTP clients are shared only at the appropriate layer: process-level
credential/client state may be reused, while each run owns and closes its
protocol session. Resource behavior is checked by the concurrency, cancellation,
timeout, size-bound, workspace, and release distribution tests; correctness
limits do not rely on a timing-sensitive E2E assertion.

## Required validation order

1. Format, compile, generated-document, fixture, and contract validation.
2. `qcg-llm`, `qcg-mcp`, and `qcg-llm-steps` unit/integration suites.
3. The real anonymous Exa and Parallel contract test.
4. Full workspace tests and Clippy with warnings denied.
5. Frontend API generation check, Svelte check, and Vitest.
6. Distribution, server, and browser E2E checks.
7. GitHub CI on the pushed commit.

A failure at any layer blocks release; a later-layer pass never overrides an
earlier-layer failure.
