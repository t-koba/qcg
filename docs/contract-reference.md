# Contract Reference

Every generator is rooted at a `qcg.toml` manifest. The parser rejects unknown
fields, so this file is the authoritative surface developers can rely on.

## Package Layout

```text
my-generator/
  qcg.toml
  prompts/
  templates/
  schemas/
  resources/
  assets/
```

Only `qcg.toml` is required. Other directories are conventional paths used by
steps and resources.

## `[generator]`

Required:

- `id`: safe generator id.
- `name`: display name.
- `version`: generator version.

Optional:

- `description`
- `authors`
- `qcg_version`

## `[llm]`

The whole section is optional. If omitted, LLM steps are invalid and non-LLM
generators continue to work.

Fields:

- `model`: a single `{ provider, model }` entry declaring which registered
  provider and model every LLM request uses. When omitted, the `[default]`
  model declared in `providers.toml` is used; a run fails when neither is
  present. Provider IDs come from the `providers.toml` registry documented in
  `docs/llm-provider-guide.md`.
- `input_cost_per_million_usd` / `output_cost_per_million_usd` on `model`:
  required when `budget.max_cost_usd` is set.
- `temperature`
- `max_tokens`
- `max_context_bytes`
- `max_context_tokens`
- `system`: generator-specific system text appended after qcg's
  mechanism-owned guardrail.
- `retry_prompt`: minijinja text used after schema validation failures.
  Available variables are `error` and `attempt`.
- `seed`: optional deterministic seed passed to providers that support it.
- `requires`: provider capabilities required by all LLM nodes. Known
  capabilities are `tool_use`, `json_schema`, `streaming`, and `seed`.

Provider credentials and endpoint overrides are declared per row in
`providers.toml`; see `docs/llm-provider-guide.md`.

## `[[inputs.stages]]`

Stage fields:

- `id`
- `when`: optional boolean expression.
- `fields`: nested field array.

Input field fields:

- `id`
- `type`: `string`, `text`, `number`, `boolean`, `select`, `multiselect`,
  `list`, `file`, `json`, or `natural_language`. A `json` field holds any
  JSON value (object, array, scalar); the Web UI submits it as a
  pretty-printed JSON textarea and the engine validates that the value is
  well-formed JSON.
- `required`
- `default`
- `pattern`
- `options`
- `min_items`
- `item_type`

Only active stages are resolved. A stage is active when `when` is absent or
evaluates true.

## `[resources.<name>]`

Fields:

- `type`: `file`, `dir`, `url`, `skill`, or `openapi`
- `path`: local package-relative path
- `url`: remote URL, fetched through the network allowlist
- `trust`: `trusted` or `untrusted`
- `llm_visible`: required before an LLM context can include the resource
- `pin_sha256`: optional hash pin for snapshotted URL/OpenAPI resources
- `cache_ttl_seconds`: optional remote snapshot TTL

LLM context selectors:

- `resources.name`
- `resources.openapi#paths`
- `resources.openapi#operations`
- `resources.openapi#operations(tag=tag-name)`
- `resources.skill#meta`
- `resources.skill#instructions`
- `resources.skill#files/path`

## `[permissions]`

Workspace reads and writes are allowed by default. Network access, commands,
containers, and side effects are denied unless declared.

- `fs_read`: include `workspace` to allow steps to read generated workspace
  files. Paths are normalized and symlink escapes are rejected.
- `fs_write`: include `workspace` to allow workspace writes.
- `network`: allowed host names.
- `commands`: allowlisted `{ bin, args, purpose }` shapes.
- `containers`: `{ enabled, images, on_missing }`.
- `side_effects`: `none`, `confirm`, `dry_run_first`, or `allowed`.

Commands run without a shell, inside the workspace, with a minimal environment,
a timeout, and output limits.

## `[tools.<name>]`

Logical tools describe what a flow needs without forcing the flow to know how
the runtime will execute it. `check.tool` currently supports validator tools.

Common fields:

- `kind`: currently `validator`
- `input`: default input path for the tool
- `command`: logical command vector. `{input}` is replaced with the node input.
- `network`: `none` or `permissioned`
- `workspace`: `read_only`, `writable`, or `none`
- `timeout_seconds`
- `output_limit_bytes`

Resolution:

```toml
[tools.qpx_validate.resolution]
allowed_backends = ["bundled", "container", "host"]
preferred_backends = ["bundled", "container", "host"]
fallback = "explicit"
```

`fallback = "explicit"` is the default. If an earlier backend is unavailable,
the runtime requires user confirmation before falling back. Non-interactive runs
therefore fail instead of silently using a different backend.

Backends:

- `backends.host`: host binary. The resulting command must be allowed by
  `[permissions].commands`.
- `backends.bundled`: generator-relative binary path plus `sha256`.
- `backends.container`: pinned image and mount path. The image must be allowed
  by `[permissions].containers`.

Only `host`, `bundled`, and `container` are accepted backend fields.

## `[secrets.<name>]`

Each secret declares an environment variable:

```toml
[secrets.api_token]
env = "API_TOKEN"
```

Secret values are loaded at runtime. They are not written into manifests,
journals, or LLM prompts; secret placeholders may be injected by the
`inject_secrets` transform.

## `[[flow]]`

Common node fields:

- `id`
- `type`
- `needs` (omission means the previous flow entry; roots omit it)
- `when`
- `on_deps`: `all_succeeded`, `any_succeeded`, or `none_failed` (skipped
  dependencies satisfy the policy; only failures block)
- `context`
- `output`
- `artifact`
- `on_fail`
- `failure`
- `params` (closed, step-specific table)

Root-level `parallel = ["lint", "test"]` explicitly identifies a contiguous
parallel group; normal flow order is sequential. `when` expressions may inspect
both `steps.X.output` and `steps.X.status`.

Resource context supports a closed table form in addition to short strings:

```toml
context = [
  { resource = "todo_api", select = "operations", tag = "todos" },
  { resource = "guide", select = "file", path = "README.md" },
]
```

Step-specific fields such as `prompt`, `output_file`, `command`, or `expect`
must appear under `[flow.params]`. Unknown fields and invalid types are rejected
during contract loading with a source line.

## Step Types

`render`
: Render a package template to `output_file`.

`write`
: Render inline `content` to `output_file`.

`copy`
: Copy workspace/package content from `source` to `target`.

`transform`
: Requires `transform`, `source`, and `target`. Supported transforms are
  `inject_secrets`, `json_pretty`, `json_compact`, `toml_to_json`,
  `json_to_toml`, `json_merge`, and `zip`. `json_merge` also requires `with`;
  values from `source` win on key conflicts.

`command`
: Execute an allowlisted command vector.

`http`
: Execute an allowlisted HTTP request. Non-GET/HEAD methods are side effects.

`ask_user`
: Ask for a scalar answer, a static multi-field form through `fields`, or a
  dynamic form loaded from `fields_from`. `options` restrict scalar answers.

`check.schema`
: Validate a workspace JSON file against a package JSON schema.

`check.format`
: Validate JSON or TOML syntax.

`check.command`
: Run a command and check `expect.exit_code` and/or
  `expect.stdout_contains`.

`check.tool`
: Resolve and run a logical validator tool. The flow declares only `tool` and
  `input`; backend choice is handled by runtime resolution.

`check.container`
: Run a container check when a container runtime exists and the manifest allows
  the image. `on_missing` controls missing-runtime behavior.

`check.contract`
: Load and validate a generated qcg package.

`llm.generate`
: Produce text, optionally writing it to `output_file`.

`llm.fill`
: Produce JSON and validate it against `schema`. Invalid JSON/schema responses
  retry up to `max_iterations` or 3 attempts.

`llm.choose`
: Choose from the closed `options` list. Out-of-set responses retry up to
  `max_iterations` or 3 attempts.

`llm.repair`
: Ask the LLM to rewrite `source` into `target`.

`llm.agent`
: Run a bounded tool loop. Requires `max_iterations`,
  `max_tokens_total`, and declared `tools`.

`foreach`
: Iterate over an array or object at `items` and execute the named `subflow`
  block. Array entries are `item`; object entries expose `item.key` and
  `item.value`. Requires `max_iterations`.

## Generated Step Parameter Schemas

The following block is generated from the registered `StepExecutor::params_schema()`
metadata. Update it with `qcg docs step-schemas`.

<!-- qcg-step-schemas:start -->
### `ask_user`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "type": "string"
    },
    "fields": {
      "items": {
        "type": "object"
      },
      "type": "array"
    },
    "fields_from": {
      "type": "string"
    },
    "options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "required": [
    "content"
  ],
  "type": "object"
}
```

### `check.command`

```json
{
  "additionalProperties": false,
  "properties": {
    "command": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "expect": {
      "properties": {
        "exit_code": {
          "type": "integer"
        },
        "exit_code_in": {
          "items": {
            "type": "integer"
          },
          "type": "array"
        },
        "stderr_contains": {
          "type": "string"
        },
        "stdout_contains": {
          "type": "string"
        },
        "stdout_matches": {
          "type": "string"
        }
      },
      "type": "object"
    }
  },
  "required": [
    "command"
  ],
  "type": "object"
}
```

### `check.container`

```json
{
  "additionalProperties": false,
  "properties": {
    "command": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "content": {
      "type": "string"
    },
    "expect": {
      "properties": {
        "exit_code": {
          "type": "integer"
        },
        "exit_code_in": {
          "items": {
            "type": "integer"
          },
          "type": "array"
        },
        "stderr_contains": {
          "type": "string"
        },
        "stdout_contains": {
          "type": "string"
        },
        "stdout_matches": {
          "type": "string"
        }
      },
      "type": "object"
    },
    "image": {
      "type": "string"
    },
    "mounts": {
      "items": {
        "properties": {
          "from": {
            "type": "string"
          },
          "mode": {
            "enum": [
              "ro",
              "rw"
            ],
            "type": "string"
          },
          "to": {
            "type": "string"
          }
        },
        "required": [
          "from",
          "to"
        ],
        "type": "object"
      },
      "type": "array"
    }
  },
  "required": [
    "command"
  ],
  "type": "object"
}
```

### `check.contract`

```json
{
  "additionalProperties": false,
  "properties": {
    "source": {
      "type": "string"
    }
  },
  "required": [
    "source"
  ],
  "type": "object"
}
```

### `check.format`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "enum": [
        "json",
        "toml"
      ],
      "type": "string"
    },
    "source": {
      "type": "string"
    }
  },
  "required": [
    "source",
    "content"
  ],
  "type": "object"
}
```

### `check.schema`

```json
{
  "additionalProperties": false,
  "properties": {
    "schema": {
      "type": "string"
    },
    "source": {
      "type": "string"
    }
  },
  "required": [
    "source",
    "schema"
  ],
  "type": "object"
}
```

### `check.tool`

```json
{
  "additionalProperties": false,
  "properties": {
    "input": {
      "type": "string"
    },
    "tool": {
      "type": "string"
    }
  },
  "required": [
    "tool"
  ],
  "type": "object"
}
```

### `command`

```json
{
  "additionalProperties": false,
  "properties": {
    "command": {
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "required": [
    "command"
  ],
  "type": "object"
}
```

### `copy`

```json
{
  "additionalProperties": false,
  "properties": {
    "source": {
      "type": "string"
    },
    "target": {
      "type": "string"
    }
  },
  "required": [
    "source",
    "target"
  ],
  "type": "object"
}
```

### `fail`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "type": "string"
    }
  },
  "required": [],
  "type": "object"
}
```

### `foreach`

```json
{
  "additionalProperties": false,
  "properties": {
    "items": {
      "type": "string"
    },
    "max_iterations": {
      "minimum": 1,
      "type": "integer"
    },
    "parallel": {
      "minimum": 1,
      "type": "integer"
    },
    "subflow": {
      "type": "string"
    }
  },
  "required": [
    "items",
    "subflow",
    "max_iterations"
  ],
  "type": "object"
}
```

### `http`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "type": "string"
    },
    "headers": {
      "additionalProperties": {
        "type": "string"
      },
      "type": "object"
    },
    "method": {
      "type": "string"
    },
    "output_file": {
      "type": "string"
    },
    "url": {
      "type": "string"
    }
  },
  "required": [
    "url"
  ],
  "type": "object"
}
```

### `llm.agent`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "max_iterations": {
      "minimum": 1,
      "type": "integer"
    },
    "max_tokens_total": {
      "minimum": 1,
      "type": "integer"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "schema": {
      "type": "string"
    },
    "tools": {
      "items": {
        "properties": {
          "command": {
            "items": {
              "type": "string"
            },
            "type": "array"
          },
          "description": {
            "type": "string"
          },
          "hosts": {
            "items": {
              "type": "string"
            },
            "type": "array"
          },
          "input_schema": {
            "type": "object"
          },
          "kind": {
            "enum": [
              "fs.write",
              "command",
              "http",
              "ask_user"
            ],
            "type": "string"
          },
          "methods": {
            "items": {
              "type": "string"
            },
            "type": "array"
          },
          "name": {
            "type": "string"
          },
          "path_prefix": {
            "type": "string"
          }
        },
        "required": [
          "name",
          "kind"
        ],
        "type": "object"
      },
      "type": "array"
    }
  },
  "required": [
    "prompt",
    "max_iterations",
    "max_tokens_total"
  ],
  "type": "object"
}
```

### `llm.choose`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "max_iterations": {
      "minimum": 1,
      "type": "integer"
    },
    "options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "schema": {
      "type": "string"
    }
  },
  "required": [
    "prompt",
    "options"
  ],
  "type": "object"
}
```

### `llm.fill`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "max_iterations": {
      "minimum": 1,
      "type": "integer"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "schema": {
      "type": "string"
    }
  },
  "required": [
    "prompt"
  ],
  "type": "object"
}
```

### `llm.generate`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "schema": {
      "type": "string"
    }
  },
  "required": [
    "prompt"
  ],
  "type": "object"
}
```

### `llm.repair`

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "items": {
        "oneOf": [
          {
            "type": "string"
          },
          {
            "additionalProperties": false,
            "properties": {
              "path": {
                "type": "string"
              },
              "resource": {
                "type": "string"
              },
              "select": {
                "type": "string"
              },
              "tag": {
                "type": "string"
              }
            },
            "required": [
              "resource"
            ],
            "type": "object"
          }
        ]
      },
      "type": "array"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "schema": {
      "type": "string"
    },
    "source": {
      "type": "string"
    },
    "target": {
      "type": "string"
    }
  },
  "required": [
    "prompt"
  ],
  "type": "object"
}
```

### `render`

```json
{
  "additionalProperties": false,
  "properties": {
    "output_file": {
      "type": "string"
    },
    "template": {
      "type": "string"
    }
  },
  "required": [
    "template",
    "output_file"
  ],
  "type": "object"
}
```

### `transform`

```json
{
  "additionalProperties": false,
  "properties": {
    "secrets": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "source": {
      "type": "string"
    },
    "target": {
      "type": "string"
    },
    "transform": {
      "enum": [
        "inject_secrets",
        "json_pretty",
        "json_compact",
        "toml_to_json",
        "json_to_toml",
        "json_merge",
        "zip"
      ],
      "type": "string"
    },
    "with": {
      "type": "string"
    }
  },
  "required": [
    "transform",
    "source",
    "target"
  ],
  "type": "object"
}
```

### `write`

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "type": "string"
    },
    "output_file": {
      "type": "string"
    }
  },
  "required": [
    "output_file",
    "content"
  ],
  "type": "object"
}
```
<!-- qcg-step-schemas:end -->

`fail`
: Fail intentionally with `content`.

## `on_fail`

Supported strategies:

- `{ action = "fail" }`
- `{ action = "route", to = "node_id" }`
- `{ action = "ask_user" }`
- `{ action = "regenerate", max_attempts = 2 }`
- `{ action = "repair", repair = "node_id", recheck = "node_id",
  max_attempts = 2, on_exhausted = { action = "fail" } }`

Repair exhaustion also supports `on_exhausted = { action = "route", to = "..." }`.

## Agent Tools

Tool declarations use:

- `name`
- `kind`: `fs.write`, `command`, `http`, or `ask_user`
- `input_schema`: optional JSON Schema for tool arguments
- `resource`
- `methods`
- `hosts`
- `path_prefix`
- `command`

Each tool declaration must be a subset of `[permissions]`, and each tool call is
checked against the declared tool and the same runtime permission gateways as
deterministic steps.

## `[outputs]`

Declare primary artifacts on the producing flow node so the path has one source
of truth:

```toml
[[flow]]
id = "write_report"
type = "write"
artifact = { label = "Report", required = true, mime = "text/markdown" }

[flow.params]
output_file = "report.md"
content = "..."
```

`artifact` metadata is valid only when the step has a statically identifiable
`output_file`, `target`, or `destination`. Use `[[outputs.extras]]` only for
additional files selected by a workspace-relative `glob`:

```toml
[[outputs.extras]]
glob = "reports/**/*.json"
label = "Report data"
required = false
```

Artifacts are collected from the run workspace and hashed in `outputs.json`.
Symlinks and paths that resolve outside the workspace are rejected.

## `[runtime]`, `[budget]`, `[failure]`, `[journal]`, `[assets]`

These sections are parsed as policy/display surfaces for generators. Unknown
fields are rejected when their structs define a closed schema.

`[runtime]`
:: Single source for command and HTTP timeout/body limits:
   `command_timeout_seconds`, `command_output_limit_bytes`,
   `http_timeout_seconds`, `http_body_limit_bytes`, and
   `http_redirect_limit`. Confirmation plans display these same values.

`[budget]`
:: Run-wide limits that survive suspend/resume rounds: `max_steps`,
   `max_tokens`, `max_cost_usd`, and `max_elapsed_seconds`. A cost limit
   requires input/output pricing on the declared `[llm].model` entry.

`[failure]`
:: Hierarchical policy with `default` plus `[failure.by_kind]` entries for
   `schema`, `range`, `permission`, `out_of_contract`, and `execution`.
   Supported actions are `reject`, `clarify`, `clamp`, and `fail`. A flow node
   may declare its own `failure` table to override the generator policy.
   `out_of_contract = true` LLM responses are journaled before the selected
   policy is applied.

`[journal].retain_days`
:: Retention window used by `qcg runs gc` in addition to `--keep`.

`[assets]`
:: Optional client assets declared by safe relative package paths. `files`
   lists regular files that must exist when the contract loads. `dirs` exposes
   declared subtrees whose files are resolved at request time, allowing an
   unbuilt derived UI directory to return 404 without invalidating the package.
   Files are served by `GET /api/generators/{id}/assets/{path}`; overlapping,
   nested, duplicate, and unsafe declarations are rejected, and canonical path
   containment prevents symlink escapes. `meta` is a free-form JSON object
   forwarded to clients without backend interpretation. File extensions and
   UI entry-point conventions are client responsibilities.
