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
- `transform`
- `check.schema`
- `check.format`
- `check.contract`

Each parallel task receives the same immutable `ValueBag` snapshot. Outputs are
merged back into the main run state in graph order after the wave completes, so
downstream nodes see deterministic `steps.*` values. Side-effecting, LLM,
interactive, container, command, HTTP, and `foreach` nodes stay on the
single-node path.

Parallel starts and finishes are marked with `"parallel": true` in the journal.

`foreach` has a separate bounded concurrency setting under `[flow.params]`:

```toml
[flow.params]
items = "inputs.sites"
subflow = "site"
max_iterations = 20
parallel = 4
```

`items` may resolve to an array or an object. Array entries are exposed directly
as `item`; object entries are exposed as `item.key` and `item.value`. Items
beyond `max_iterations` are not executed. The step returns requested and
executed counts and emits `foreach_budget_exhausted`; every admitted iteration
uses a distinct hierarchical path such as `emit_sites[3]/write_site`.

## Reproducibility

Each service run stores generated files under `<run>/workspace` and durable
`journal.jsonl`, `state.json`, `outputs.json`, and resource pins under
`<run>/meta`. Direct CLI runs keep the same metadata under the output parent's
`.qcg/runs/<direct-id>/meta`, so generator writes cannot overwrite execution
state. The journal includes graph events, LLM calls, interactions, side-effect
decisions, and artifact hashes.
`qcg runs replay <id>` reruns a recorded input set and compares artifact hashes.
