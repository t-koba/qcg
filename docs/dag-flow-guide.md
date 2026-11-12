# DAG Flow Guide

qcg executes only the nodes declared in `[[flow]]`. A node becomes ready when
its dependencies have finished according to its `on_deps` policy.

## Control Fields

- `needs`: explicit node ids that must complete before this node can run. When
  omitted, a node depends on the previous flow entry.
- `when`: boolean expression over `inputs.*` and `steps.*`.
- `on_deps = "all_succeeded"`: all dependencies must succeed.
- `on_deps = "any_succeeded"`: at least one dependency must succeed.
- `on_deps = "none_failed"`: every dependency reached a terminal state and
  none failed. Dependencies skipped by their own `when` satisfy this policy,
  so a conditional branch can stop cascading skips at one node.
- `on_fail`: optional repair, route, or fail behavior.

Skipped dependencies propagate through the graph. Repair cycles are bounded by
their declared attempt count and each attempt is written to the journal. Skip
and failure reasons use a typed code/message structure; dependency skips list
every failed or skipped dependency instead of hiding all but the first.

### Conditional branches

Use `when` on the branch node and `on_deps = "none_failed"` on its direct
successor to keep the rest of the flow running when the branch is skipped:

```toml
[[flow]]
id = "propose"
type = "llm.fill"
when = "inputs.design_mode == 'llm'"
output = "design_out"

[[flow]]
id = "collect_answers"
type = "ask_user"
on_deps = "none_failed"
```

If the proposal is skipped (manual mode), `collect_answers` still runs; if it
fails, the failure still blocks the successor. Only the direct successor needs
the relaxed policy — once it succeeds, later nodes see a successful dependency.

The same shape drives dynamic file emission: a `foreach` over a design-provided
source map plus a `[[blocks.*]]` write node (`content = "{{ item.value }}"`,
`output_file = "generator/{{ item.key }}"`) materializes arbitrary sources
declared by data instead of hard-coded steps. Object iteration uses the generic
`{key, value}` item shape. See the bundled `generator`.

### Adaptive HITL and MCP

If an operator decision changes the workflow, declare it as an `ask_user` node
with explicit `needs`, then gate the resulting branches with `when` over that
node's output. An MCP agent binding is fixed to its configured server and
remote tool, while `side_effects = true` routes calls through the normal
`[permissions].side_effects` policy and can pause at a confirmation boundary.
Use an explicit `ask_user` node when the answer changes the MCP tool or DAG
branch; do not add a duplicate confirmation node solely to mirror the runtime
side-effect check.

## Parallel Waves

Declare a contiguous parallel group once at the manifest root, for example
`parallel = ["lint", "test"]`. Each member depends on the entry before the
group, and the following implicit node depends on every member. A node cannot
combine membership in `parallel` with an explicit `needs` list.

When multiple pending nodes become ready in the same scheduler wave, qcg runs
them in parallel if every node in that wave is deterministic and has no
`on_fail` strategy. The current parallel-safe step set is:

- `render`
- `write`
- `copy`
- `read_anchored`
- `patch`
- `transform`
- `check.schema`
- `check.format`
- `check.contract`
- `check.tool`

Each parallel task receives the same immutable `ValueBag` snapshot. Outputs are
merged back into the main run state in graph order after the wave completes, so
downstream nodes see deterministic `steps.*` values. Parallel `patch` nodes
must target disjoint files: overlapping patches stay caller-exclusive and the
second writer fails with a base or anchor mismatch instead of overwriting
silently. The direct `command`, `check.command`, and `check.container` step
types, along with HTTP, `ask_user`, `foreach`, `llm.decide` and `llm.catalog`,
stay on the single-node path. The other LLM step types run in parallel waves. `check.tool` is a parallel-safe validator step even when its selected
tool backend uses command or container execution; existing fallback
confirmation behavior still applies.

## Continuing a run

`qcg runs fork <id> --reuse` (or `POST /api/runs/{id}/fork` with
`{"reuse": "matching"}`) starts a child of a failed, canceled or finished run
from the source's initial state, with the same inputs unless patched. The child
walks the whole DAG again. Each model request is digested together with its
node id, step type and the complete provider request (model, parameters,
rendered messages, schema, tools). When the source recorded a response for the
same node and digest under a step that finished successfully, the child serves
that response instead of calling the provider and journals `llm_memo` with
`reused_from`; the step's own logic (validation, file writes, output) still
runs on it. Any change to a rendered prompt, a step definition, a model
parameter or a read file changes the digest, so that request is repeated and
steps reading its result see new inputs. Responses are not served from steps
that failed, and a missing or altered recorded response is repeated. Effects
other than model calls are never reused, and answers and confirmations are
supplied again with the fork request. A step's `reuse` key can set `never`.

Parallel starts and finishes are marked with `"parallel": true` in the journal.

`foreach` has a separate bounded concurrency setting under `[flow.params]`:

```toml
[flow.params]
items = "inputs.sites"
subflow = "site"
max_iterations = 20
parallel = 4
```

Registry-backed validation (`qcg validate`, install, and run preflight) rejects
`parallel > 1` when a block child is an unsafe plain step (for example,
`command`) or declares any `on_fail` routing. This depends on the declaration,
not the eventual item count: even zero or one item requires `parallel = 1`
for these children. Nested control-flow children remain allowed, with each
foreach validated against its own declared parallelism and block.

`items` may resolve to an array or an object. Array entries are exposed directly
as `item`; object entries are exposed as `item.key` and `item.value`. Items
beyond `max_iterations` are not executed. The step returns requested and
executed counts and emits `foreach_budget_exhausted`; every admitted iteration
uses a distinct hierarchical path such as `emit_sites[3]/write_site`.

## Reproducibility

Each service run stores generated files under `<run>/workspace` and durable
`journal.jsonl`, `state.json`, `outputs.json`, and resource pins under
`<run>/meta`. Direct CLI runs keep the same metadata under the output parent's
`.data/runs/<direct-id>/meta`, so generator writes cannot overwrite execution
state. The journal includes graph events, LLM calls, interactions, side-effect
decisions, and artifact hashes.
`qcg runs replay <id>` reruns a recorded input set and compares artifact hashes.
