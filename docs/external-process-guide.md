# External Process Guide

qcg generator packages run specialized generation and validation logic as
ordinary external processes. They communicate through standard input and
output, with no qcg-specific plugin interface. This keeps the generator
portable while preserving qcg's limits, workspace containment, journal,
artifacts, and permission checks.

## Structured command steps

Use a `command` step when a process performs generation, transformation, or
validation as part of the flow:

```toml
[[flow]]
id = "generate"
type = "command"

[flow.params]
command = ["my-generator", "--stdio"]
input = { request = "{{ inputs.request }}" }
result = "structured"
output_schema = { type = "object", required = ["summary"], properties = { summary = { type = "string" } } }
```

qcg writes the canonical JSON value from `input` to stdin. Alternatively,
`input_file` reads bounded bytes from the workspace or package. The process
writes exactly one UTF-8 JSON object to stdout:

```json
{
  "status": "success",
  "output": { "summary": "generated" },
  "files": ["result.txt"],
  "findings": []
}
```

`status` is `success` or `check_failed`. A `check_failed` result requires at
least one typed finding. `files` contains workspace-relative regular files
created by the process. qcg validates `output` against `output_schema` when it
is declared and rejects unknown fields, invalid UTF-8 or JSON, unsafe paths,
unreadable or non-regular files, nonzero exits, and limit violations.

Use the default `result = "process"` only when stdout and stderr are ordinary
process output rather than the structured protocol.

## External resources

Use an `exec` resource when external data or generated context must be captured
before the flow starts:

```toml
[resources.catalog]
type = "exec"
llm_visible = true
trust = "untrusted"

[resources.catalog.params]
command = ["catalog-export", "--json"]
max_bytes = 1048576
```

The process writes bounded UTF-8 data to stdout. qcg snapshots it once,
optionally checks it against `pin_sha256`, and reuses it as an immutable
resource. The complete command must be declared in `permissions.commands`.

## External guardrails

Use a `command` guardrail when `regex_deny` or `json_schema` cannot express a
policy. qcg writes the inspected JSON value to stdin. The process returns one
of these closed JSON shapes:

```json
{ "status": "pass" }
```

```json
{
  "status": "violation",
  "code": "policy.denied",
  "message": "The value violates the policy",
  "details": { "rule": "example" }
}
```

```json
{
  "status": "error",
  "code": "policy.unavailable",
  "message": "The policy service could not evaluate the value"
}
```

Configure it on an `llm.agent` guardrail declaration:

```toml
[[flow.params.guardrails]]
name = "organization_policy"
stage = "tool_output"
kind = "command"
tripwire = true
on_error = "fail"
params = { command = ["policy-check", "--stdio"], timeout_seconds = 30, output_limit_bytes = 65536 }
```

The command uses the same declared command permission and isolation gateway as
command steps. `on_error = "fail"` propagates evaluation errors;
`on_error = "block"` converts them into typed policy violations.

## Permission and limit requirements

Every external process must have an exact `permissions.commands` declaration.
Choose explicit isolation, command timeout, input/output limits, filesystem
permissions, network hosts, and side-effect policy for the work it performs.
These controls are part of the generator contract rather than an implicit
runtime fallback.

The stock qcg binary does not discover dynamic Rust step, resource, validation,
or guardrail plugins. Built-in steps are internal workspace components. A
generator that needs new behavior should compose the manifest DAG, schemas,
templates, MCP tools, and the process boundaries above; no qcg source change is
required.
