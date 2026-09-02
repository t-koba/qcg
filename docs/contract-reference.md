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
generators continue to work. It defines generator-wide defaults and resource
ceilings. Invocation policy belongs in each LLM node's `params.request`; an
agent-as-tool may add a final `request` layer. The deterministic order is
`[llm]` then node `request` then specialist `request`.

Fields:

- `model`: a single `{ provider, model }` entry declaring the default route.
  An LLM node can override it with `params.model` and declare an ordered
  `params.fallback_models` list. A retryable route failure advances through
  that list and records `llm_route_failed`. When omitted, the `[default]`
  model declared in `providers.toml` is used; a run fails when neither is
  present. Provider IDs come from the `providers.toml` registry documented in
  `docs/llm-provider-guide.md`.
- `input_cost_per_million_usd` / `output_cost_per_million_usd` on `model`:
  required when `budget.max_cost_usd` is set.
- `models`: additional priced model entries available to node-level routing.
- `temperature`: optional sampling temperature from `0` through `2`. Omit it
  when `reasoning_effort` is set.
- `top_p`: optional nucleus sampling value from `0` through `1`. It is mutually
  exclusive with `temperature` and `reasoning_effort`.
- `max_tokens`: required positive output limit. For reasoning models this
  includes both hidden reasoning tokens and visible output tokens; qcg maps it
  to the field required by the selected API.
- `max_context_bytes`
- `max_context_tokens`
- `max_media_bytes`: required aggregate byte limit when an LLM node declares
  image, audio, or file `params.media` input. Media paths remain confined to
  the run workspace and are encoded only after this bound is checked.
- `context_overflow`: `error` by default, or explicitly `truncate_head` /
  `truncate_tail`. Truncation is UTF-8 safe, visibly marked, deterministic,
  and recorded as `context_compacted`; no silent compaction occurs.
- `system`: generator-specific system text appended after qcg's
  mechanism-owned guardrail. Node and specialist `request.system` values are
  appended in layer order; all are rendered against the node context.
- `retry_prompt`: minijinja text used after schema validation failures.
  Available variables are `error` and `attempt`.
- `seed`: optional best-effort deterministic seed passed only to providers
  that advertise it. It cannot be combined with `reasoning_effort`.
- `reasoning_effort`: optional `none`, `minimal`, `low`, `medium`, `high`,
  `xhigh`, or `max`. Supported values are model-specific and must also appear
  in the selected registry row. Omission means that qcg sends no effort value;
  it is distinct from the explicit value `none`.
- `structured_output`: `auto` by default. `auto` uses strict native JSON Schema
  only when the Schema is compatible, otherwise uses the provider's compatible
  native mode, and falls back to prompt transport when the provider has no
  native Schema capability. `native_strict`, `native_compatible`, and `prompt`
  are explicit overrides. Every mode is followed by qcg's own Schema
  validation, so provider transport never becomes the correctness boundary.
  An explicit `native_strict` contract is rejected locally when any object
  Schema is open or has optional properties; it is never sent as a knowingly
  incompatible strict request.
- `stop_sequences`: at most eight non-empty strings of at most 1024 bytes.
- `tool_choice`: `none`, `auto`, `required`, or `{ tool = "declared_name" }`.
  It is valid only for tool-enabled invocations.
- `parallel_tool_calls`: optional explicit permission for the provider to emit
  parallel tool calls. qcg still serializes side-effecting and interactive
  tools at execution time.
- `verbosity`: optional `low`, `medium`, or `high` response-detail policy for a
  provider that advertises the mechanism.
- `requires`: provider capabilities required by all LLM nodes. Known
  capabilities are `tool_use`, `json_schema`,
  `structured_output_with_tools`, `seed`, `reasoning_effort`, `image_input`,
  `audio_input`, `file_input`, `streaming`, `temperature`, `top_p`,
  `stop_sequences`, `tool_choice`, `parallel_tool_calls`, and `verbosity`.
  Configured controls automatically require their corresponding capability.

Every LLM node accepts `params.request` with `clear`, `system`, `temperature`, `top_p`,
`max_tokens`, `stop_sequences`, `seed`, `reasoning_effort`,
`structured_output`, `tool_choice`, `parallel_tool_calls`, `verbosity`,
`stream`, `requires`, `max_context_bytes`, `max_context_tokens`,
`max_media_bytes`, `context_overflow`, and `retry_prompt`. Node and specialist
limits may tighten, but never raise, generator-wide ceilings. An explicit
`stop_sequences = []` clears the inherited list. Sampling and reasoning
controls clear incompatible inherited controls rather than sending an invalid
combination. `clear` is a typed list containing any of `temperature`, `top_p`,
`stop_sequences`, `seed`, `reasoning_effort`, `tool_choice`,
`parallel_tool_calls`, or `verbosity`; it explicitly omits inherited optional
controls before applying the current layer. Safety ceilings, system policy,
and required capabilities cannot be cleared. Agent tools additionally own `model`, `fallback_models`, and the
same `request` object; an explicitly empty specialist `fallback_models` list
disables fallback instead of inheriting the parent node's routes.

Provider credentials and endpoint overrides are declared per row in
`providers.toml`; see `docs/llm-provider-guide.md`.

## `[[inputs.stages]]`

Stage fields:

- `id`
- `when`: optional boolean expression.
- `fields`: nested field array.

Input field fields:

- `id`
- `label`: optional default display label. Clients fall back to a humanized
  `id` when it is absent.
- `label_i18n`: optional locale-to-label map. Clients match the display locale
  exactly, then by its primary language, and finally use `label`.
- `description` / `description_i18n`: explanatory text displayed next to the
  control.
- `placeholder` / `placeholder_i18n`: localized input hint.
- `type`: `string`, `text`, `number`, `boolean`, `select`, `multiselect`,
  `list`, `file`, `json`, or `natural_language`. A `json` field holds any
  JSON value (object, array, scalar); the Web UI submits it as a
  pretty-printed JSON textarea and the engine validates that the value is
  well-formed JSON. Namespaced lowercase custom kinds are also preserved;
  their value shape is defined by `schema` instead of an implicit string
  fallback.
- `required`
- `default`
- `pattern`
- `options`
- `option_labels_i18n`: optional locale-to-option-label map. Its innermost keys
  are values from `options`; localized labels never change submitted values.
- `min_items`
- `item_type`
- `schema`: arbitrary JSON Schema applied after the canonical field-type
  conversion. This is the authoritative extension surface for constraints not
  represented by the convenience fields above.
- `ui`: renderer-neutral metadata. The bundled Web UI understands `widget`,
  `input_type`, and `rows`; other clients may define additional keys without
  changing the execution contract.

Only active stages are resolved. A stage is active when `when` is absent or
evaluates true.

## `[resources.<name>]`

Fields:

- `type`: `file`, `dir`, `url`, `skill`, `openapi`, or `exec`
- `path`: local package-relative path
- `url`: remote URL, fetched through the network allowlist
- `trust`: `trusted` or `untrusted`
- `llm_visible`: required before an LLM context can include the resource
- `pin_sha256`: optional hash pin for snapshotted URL/OpenAPI resources
- `cache_ttl_seconds`: optional remote snapshot TTL
- `params`: bounded settings for the selected built-in resource type.
  `file`, `url`, and `openapi` resources accept a positive
  `max_bytes` limit, defaulting to 16 MiB. Built-in `dir` and `skill` resources
  accept positive `max_files`, `max_bytes`, and `max_selected_bytes`, defaulting
  to 100,000 files, 1 GiB total, and 16 MiB per selected text file. Reads stop
  at the bound; limit violations and directory walk failures are explicit
  errors.

An `exec` resource is the stock declarative extension boundary for an external
data source. It forbids `path` and `url`, requires
`params.command = ["program", "arg", ...]`, and accepts an optional positive
`params.max_bytes`, defaulting to and never exceeding
`runtime.command_output_limit_bytes`. The complete command shape must also be
present in `permissions.commands`. qcg runs it through the ordinary isolated command
gateway before the flow starts, bounds stdout, snapshots it under run metadata,
checks `pin_sha256` when declared, and then exposes the immutable UTF-8 snapshot
through ordinary resource context. Non-zero exit, non-UTF-8 output, permission
denial, and limit violations fail explicitly.

LLM context selectors:

- `resources.name`
- `resources.openapi#paths`
- `resources.openapi#operations`
- `resources.openapi#operations(tag=tag-name)`
- `resources.skill#meta`
- `resources.skill#instructions`
- `resources.skill#files/path`
- `resources.directory#tree` / `resources.directory#files`: sorted file
  metadata and content hashes for a `dir` resource.
- `resources.directory#files/path`: one UTF-8 file from a `dir` resource.
  Parent traversal, absolute paths, and symlink escapes are rejected.

## `[permissions]`

Workspace reads and writes, network access, commands, containers, and side
effects are denied unless explicitly declared.

- `fs_read`: include `workspace` to allow steps to read generated workspace
  files. Paths are normalized and symlink escapes are rejected.
- `fs_write`: include `workspace` to allow workspace writes.
- `network`: allowed host names.
- `commands`: allowlisted `{ bin, args, purpose, isolation, image? }` shapes.
  `isolation` is mandatory. `container` requires an `image` pinned with
  `@sha256:` and present in `permissions.containers.images`; `trusted_host`
  explicitly grants execution under the qcg OS identity and cannot name an
  image.
- `containers`: `{ enabled, runtime, images, on_missing }`. Enabled containers
  must select `docker`, `podman`, or `docker_runsc`; runtime auto-detection is
  deliberately forbidden. Every image must be digest-pinned.
- `side_effects`: `none`, `confirm`, `dry_run_first`, or `allowed`.

Container commands run without a shell in Docker or Podman with no network, a
read-only root, all capabilities dropped, no-new-privileges, a PID limit, and
only the run workspace mounted at `/work`. Trusted-host commands run without a
shell with a cleared environment, timeout, process-tree cancellation, and
output limits. Stdio MCP processes use the same declared isolation mode.

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

Each secret declares exactly one source. `env` reads a value directly:

```toml
[secrets.api_token]
env = "API_TOKEN"
```

`file_env` instead names an environment variable containing an absolute path
to a private, non-symlink UTF-8 file no larger than 64 KiB. This supports
Vault Agent and mounted-secret rotation at run boundaries without embedding a
secret in process configuration:

```toml
[secrets.api_token]
file_env = "API_TOKEN_FILE"
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

Blocks may contain nested `foreach` nodes. Every level consumes the global step
budget, applies its own required iteration bound, uses a fully qualified node
path, and restores the parent `item` after the nested block finishes.

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
  `default` may select one declared scalar option for form-capable clients and
  for an empty interactive CLI answer.
  `content_i18n` localizes the rendered question, while
  `option_labels_i18n` localizes scalar option labels without changing their
  values. Static and dynamic fields use the input-field localization members
  described above.

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
  retry up to `max_iterations` attempts.

`llm.choose`
: Choose from the closed `options` list. Out-of-set responses retry up to
  `max_iterations` attempts.

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
    "content_i18n": {
      "additionalProperties": {
        "type": "string"
      },
      "type": "object"
    },
    "default": {
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
    "option_labels_i18n": {
      "additionalProperties": {
        "additionalProperties": {
          "type": "string"
        },
        "type": "object"
      },
      "type": "object"
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
    },
    "input": {},
    "input_file": {
      "type": "string"
    },
    "input_file_scope": {
      "enum": [
        "workspace",
        "package"
      ],
      "type": "string"
    },
    "output_schema": {},
    "result": {
      "enum": [
        "process",
        "structured"
      ],
      "type": "string"
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
      "maximum": 10000,
      "minimum": 1,
      "type": "integer"
    },
    "parallel": {
      "maximum": 256,
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
    "body_base64": {
      "type": "string"
    },
    "body_file": {
      "type": "string"
    },
    "body_file_scope": {
      "enum": [
        "workspace",
        "package"
      ],
      "type": "string"
    },
    "body_json": {},
    "body_text": {
      "type": "string"
    },
    "content_type": {
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
    "output": {
      "enum": [
        "text",
        "json",
        "base64",
        "file"
      ],
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
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "guardrails": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "kind": {
            "enum": [
              "regex_deny",
              "json_schema",
              "command"
            ]
          },
          "name": {
            "type": "string"
          },
          "on_error": {
            "enum": [
              "fail",
              "block"
            ]
          },
          "params": {},
          "stage": {
            "enum": [
              "input",
              "output",
              "tool_input",
              "tool_output"
            ]
          },
          "tool": {
            "type": "string"
          },
          "tripwire": {
            "type": "boolean"
          }
        },
        "required": [
          "name",
          "stage",
          "kind"
        ],
        "type": "object"
      },
      "type": "array"
    },
    "max_iterations": {
      "maximum": 32,
      "minimum": 1,
      "type": "integer"
    },
    "max_tokens_total": {
      "maximum": 100000000,
      "minimum": 1,
      "type": "integer"
    },
    "max_tool_calls_total": {
      "maximum": 4096,
      "minimum": 1,
      "type": "integer"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "maximum": 268435456,
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
    },
    "tools": {
      "items": {
        "oneOf": [
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "input_schema": {
                "type": "object"
              },
              "kind": {
                "const": "fs.write"
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
              "kind",
              "path_prefix"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
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
              "input_schema": {
                "type": "object"
              },
              "kind": {
                "const": "command"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind",
              "command"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
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
                "const": "http"
              },
              "methods": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind",
              "methods",
              "hosts"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "input_schema": {
                "type": "object"
              },
              "kind": {
                "const": "ask_user"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "kind": {
                "const": "web.search"
              },
              "max_calls": {
                "maximum": 10,
                "minimum": 1,
                "type": "integer"
              },
              "max_results": {
                "maximum": 20,
                "minimum": 1,
                "type": "integer"
              },
              "name": {
                "type": "string"
              },
              "provider": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "kind": {
                "const": "mcp"
              },
              "max_calls": {
                "maximum": 10,
                "minimum": 1,
                "type": "integer"
              },
              "name": {
                "type": "string"
              },
              "server": {
                "type": "string"
              },
              "side_effects": {
                "type": "boolean"
              },
              "tool": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "kind",
              "server",
              "tool"
            ],
            "type": "object"
          },
          {
            "additionalProperties": false,
            "properties": {
              "description": {
                "type": "string"
              },
              "fallback_models": {
                "items": {
                  "additionalProperties": false,
                  "properties": {
                    "clear": {
                      "items": {
                        "enum": [
                          "temperature",
                          "top_p",
                          "stop_sequences",
                          "seed",
                          "reasoning_effort",
                          "tool_choice",
                          "parallel_tool_calls",
                          "verbosity"
                        ]
                      },
                      "type": "array",
                      "uniqueItems": true
                    },
                    "input_cost_per_million_usd": {
                      "minimum": 0,
                      "type": "number"
                    },
                    "model": {
                      "minLength": 1,
                      "type": "string"
                    },
                    "output_cost_per_million_usd": {
                      "minimum": 0,
                      "type": "number"
                    },
                    "provider": {
                      "minLength": 1,
                      "type": "string"
                    }
                  },
                  "required": [
                    "provider",
                    "model"
                  ],
                  "type": "object"
                },
                "maxItems": 8,
                "type": "array"
              },
              "handoff": {
                "type": "boolean"
              },
              "input_schema": {
                "type": "object"
              },
              "instructions": {
                "type": "string"
              },
              "kind": {
                "const": "agent"
              },
              "max_calls": {
                "maximum": 10,
                "minimum": 1,
                "type": "integer"
              },
              "max_iterations": {
                "maximum": 32,
                "minimum": 1,
                "type": "integer"
              },
              "max_tokens_total": {
                "maximum": 100000000,
                "minimum": 1,
                "type": "integer"
              },
              "max_tool_calls_total": {
                "maximum": 4096,
                "minimum": 1,
                "type": "integer"
              },
              "model": {
                "additionalProperties": false,
                "properties": {
                  "clear": {
                    "items": {
                      "enum": [
                        "temperature",
                        "top_p",
                        "stop_sequences",
                        "seed",
                        "reasoning_effort",
                        "tool_choice",
                        "parallel_tool_calls",
                        "verbosity"
                      ]
                    },
                    "type": "array",
                    "uniqueItems": true
                  },
                  "input_cost_per_million_usd": {
                    "minimum": 0,
                    "type": "number"
                  },
                  "model": {
                    "minLength": 1,
                    "type": "string"
                  },
                  "output_cost_per_million_usd": {
                    "minimum": 0,
                    "type": "number"
                  },
                  "provider": {
                    "minLength": 1,
                    "type": "string"
                  }
                },
                "required": [
                  "provider",
                  "model"
                ],
                "type": "object"
              },
              "name": {
                "type": "string"
              },
              "on_failure": {
                "additionalProperties": false,
                "properties": {
                  "by_code": {
                    "additionalProperties": false,
                    "properties": {
                      "guardrail_rejected": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "iteration_budget_exceeded": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "provider_failed": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "token_budget_exceeded": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "tool_call_budget_exceeded": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "tool_failed": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      },
                      "validation_failed": {
                        "enum": [
                          "fail",
                          "return_error"
                        ]
                      }
                    },
                    "type": "object"
                  },
                  "default": {
                    "enum": [
                      "fail",
                      "return_error"
                    ]
                  }
                },
                "type": "object"
              },
              "output_schema": {
                "type": "string"
              },
              "request": {
                "additionalProperties": false,
                "properties": {
                  "context_overflow": {
                    "enum": [
                      "error",
                      "truncate_head",
                      "truncate_tail"
                    ]
                  },
                  "max_context_bytes": {
                    "maximum": 1073741824,
                    "minimum": 1,
                    "type": "integer"
                  },
                  "max_context_tokens": {
                    "maximum": 268435456,
                    "minimum": 1,
                    "type": "integer"
                  },
                  "max_media_bytes": {
                    "maximum": 1073741824,
                    "minimum": 1,
                    "type": "integer"
                  },
                  "max_tokens": {
                    "minimum": 1,
                    "type": "integer"
                  },
                  "parallel_tool_calls": {
                    "type": "boolean"
                  },
                  "reasoning_effort": {
                    "enum": [
                      "none",
                      "minimal",
                      "low",
                      "medium",
                      "high",
                      "xhigh",
                      "max"
                    ]
                  },
                  "requires": {
                    "items": {
                      "enum": [
                        "tool_use",
                        "json_schema",
                        "structured_output_with_tools",
                        "seed",
                        "reasoning_effort",
                        "image_input",
                        "audio_input",
                        "file_input",
                        "streaming",
                        "temperature",
                        "top_p",
                        "stop_sequences",
                        "tool_choice",
                        "parallel_tool_calls",
                        "verbosity"
                      ]
                    },
                    "type": "array",
                    "uniqueItems": true
                  },
                  "retry_prompt": {
                    "type": "string"
                  },
                  "seed": {
                    "minimum": 0,
                    "type": "integer"
                  },
                  "stop_sequences": {
                    "items": {
                      "maxLength": 1024,
                      "minLength": 1,
                      "type": "string"
                    },
                    "maxItems": 8,
                    "type": "array"
                  },
                  "stream": {
                    "type": "boolean"
                  },
                  "structured_output": {
                    "enum": [
                      "auto",
                      "native_strict",
                      "native_compatible",
                      "prompt"
                    ]
                  },
                  "system": {
                    "type": "string"
                  },
                  "temperature": {
                    "maximum": 2,
                    "minimum": 0,
                    "type": "number"
                  },
                  "tool_choice": {
                    "oneOf": [
                      {
                        "enum": [
                          "none",
                          "auto",
                          "required"
                        ]
                      },
                      {
                        "additionalProperties": false,
                        "properties": {
                          "tool": {
                            "minLength": 1,
                            "type": "string"
                          }
                        },
                        "required": [
                          "tool"
                        ],
                        "type": "object"
                      }
                    ]
                  },
                  "top_p": {
                    "maximum": 1,
                    "minimum": 0,
                    "type": "number"
                  },
                  "verbosity": {
                    "enum": [
                      "low",
                      "medium",
                      "high"
                    ]
                  }
                },
                "type": "object"
              },
              "tools": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              }
            },
            "required": [
              "name",
              "kind",
              "instructions",
              "max_tool_calls_total"
            ],
            "type": "object"
          }
        ]
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
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "max_iterations": {
      "maximum": 32,
      "minimum": 1,
      "type": "integer"
    },
    "max_tokens_total": {
      "maximum": 100000000,
      "minimum": 1,
      "type": "integer"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
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
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "maximum": 268435456,
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
    }
  },
  "required": [
    "prompt",
    "options",
    "max_iterations",
    "max_tokens_total"
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
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "max_iterations": {
      "maximum": 32,
      "minimum": 1,
      "type": "integer"
    },
    "max_tokens_total": {
      "maximum": 100000000,
      "minimum": 1,
      "type": "integer"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "maximum": 268435456,
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
    },
    "schema": {
      "type": "string"
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
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "maximum": 268435456,
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
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
    "fallback_models": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "clear": {
            "items": {
              "enum": [
                "temperature",
                "top_p",
                "stop_sequences",
                "seed",
                "reasoning_effort",
                "tool_choice",
                "parallel_tool_calls",
                "verbosity"
              ]
            },
            "type": "array",
            "uniqueItems": true
          },
          "input_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "model": {
            "minLength": 1,
            "type": "string"
          },
          "output_cost_per_million_usd": {
            "minimum": 0,
            "type": "number"
          },
          "provider": {
            "minLength": 1,
            "type": "string"
          }
        },
        "required": [
          "provider",
          "model"
        ],
        "type": "object"
      },
      "maxItems": 8,
      "type": "array"
    },
    "media": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "detail": {
            "enum": [
              "auto",
              "low",
              "high"
            ]
          },
          "kind": {
            "enum": [
              "image",
              "audio",
              "file"
            ]
          },
          "media_type": {
            "type": "string"
          },
          "path": {
            "type": "string"
          }
        },
        "required": [
          "kind",
          "path",
          "media_type"
        ],
        "type": "object"
      },
      "maxItems": 16,
      "type": "array"
    },
    "model": {
      "additionalProperties": false,
      "properties": {
        "clear": {
          "items": {
            "enum": [
              "temperature",
              "top_p",
              "stop_sequences",
              "seed",
              "reasoning_effort",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "input_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "model": {
          "minLength": 1,
          "type": "string"
        },
        "output_cost_per_million_usd": {
          "minimum": 0,
          "type": "number"
        },
        "provider": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "provider",
        "model"
      ],
      "type": "object"
    },
    "output_file": {
      "type": "string"
    },
    "prompt": {
      "type": "string"
    },
    "request": {
      "additionalProperties": false,
      "properties": {
        "context_overflow": {
          "enum": [
            "error",
            "truncate_head",
            "truncate_tail"
          ]
        },
        "max_context_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_context_tokens": {
          "maximum": 268435456,
          "minimum": 1,
          "type": "integer"
        },
        "max_media_bytes": {
          "maximum": 1073741824,
          "minimum": 1,
          "type": "integer"
        },
        "max_tokens": {
          "minimum": 1,
          "type": "integer"
        },
        "parallel_tool_calls": {
          "type": "boolean"
        },
        "reasoning_effort": {
          "enum": [
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max"
          ]
        },
        "requires": {
          "items": {
            "enum": [
              "tool_use",
              "json_schema",
              "structured_output_with_tools",
              "seed",
              "reasoning_effort",
              "image_input",
              "audio_input",
              "file_input",
              "streaming",
              "temperature",
              "top_p",
              "stop_sequences",
              "tool_choice",
              "parallel_tool_calls",
              "verbosity"
            ]
          },
          "type": "array",
          "uniqueItems": true
        },
        "retry_prompt": {
          "type": "string"
        },
        "seed": {
          "minimum": 0,
          "type": "integer"
        },
        "stop_sequences": {
          "items": {
            "maxLength": 1024,
            "minLength": 1,
            "type": "string"
          },
          "maxItems": 8,
          "type": "array"
        },
        "stream": {
          "type": "boolean"
        },
        "structured_output": {
          "enum": [
            "auto",
            "native_strict",
            "native_compatible",
            "prompt"
          ]
        },
        "system": {
          "type": "string"
        },
        "temperature": {
          "maximum": 2,
          "minimum": 0,
          "type": "number"
        },
        "tool_choice": {
          "oneOf": [
            {
              "enum": [
                "none",
                "auto",
                "required"
              ]
            },
            {
              "additionalProperties": false,
              "properties": {
                "tool": {
                  "minLength": 1,
                  "type": "string"
                }
              },
              "required": [
                "tool"
              ],
              "type": "object"
            }
          ]
        },
        "top_p": {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        "verbosity": {
          "enum": [
            "low",
            "medium",
            "high"
          ]
        }
      },
      "type": "object"
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

### `mcp.call`

```json
{
  "additionalProperties": false,
  "properties": {
    "arguments": {
      "type": "object"
    },
    "input_schema": {},
    "output_schema": {},
    "server": {
      "type": "string"
    },
    "side_effects": {
      "type": "boolean"
    },
    "timeout_seconds": {
      "minimum": 1,
      "type": "integer"
    },
    "tool": {
      "type": "string"
    }
  },
  "required": [
    "server",
    "tool"
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
    "remove_source": {
      "type": "boolean"
    },
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
        "base64_decode",
        "base64_encode",
        "zip"
      ],
      "type": "string"
    },
    "unix_mode": {
      "pattern": "^0[6-7][0-7]{2}$",
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
    },
    "unix_mode": {
      "pattern": "^0[6-7][0-7]{2}$",
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
- `{ action = "regenerate", max_attempts = 2, on_exhausted = { action = "fail" } }`
- `{ action = "repair", repair = "node_id", recheck = "node_id",
  max_attempts = 2, on_exhausted = { action = "fail" } }`

Repair and regenerate exhaustion support `fail`, `route`, and a typed
`ask_user` action. `ask_user` accepts an optional `title` and the same `fields`
array as normal input stages; when fields are omitted qcg supplies one required
text field. Answers are durable and resume through the normal run boundary.

## Agent Tools

Every declaration has a unique non-empty `name`, a `kind`, and an optional
`description`. `llm.agent` accepts these closed tool variants:

- `fs.write`: `path_prefix`, plus an optional `input_schema`.
- `command`: a fixed `command`, plus an optional `input_schema`.
- `http`: fixed `methods` and `hosts`, plus an optional `input_schema`.
- `ask_user`: an optional `input_schema` for a runtime-generated form.
- `web.search`: an opt-in search profile selected from the unified
  `providers.toml` registry. Its model-visible input is always the closed
  `{query, limit?}` schema and cannot be replaced.
- `mcp`: a fixed tool binding into one generic `[[mcp_server]]` profile. Its
  model-visible input schema is discovered from the MCP server at run time.
- `agent`: a bounded specialist with `instructions`, an allowlist of sibling
  `tools`, invocation bound `max_calls`, `max_iterations`, `max_tokens_total`, required
  `max_tool_calls_total`, optional `input_schema` and package-relative
  `output_schema`, an optional `model`, typed `on_failure`, and an optional `handoff`. Specialist
  agents cannot delegate other agent tools. The output schema is applied to
  provider-native structured output when supported and is always validated
  locally. A normal result is returned to the parent as agent-as-tool data;
  `handoff = true` makes the specialist result the node's final output.

`on_failure.default` and `on_failure.by_code` select `return_error` or `fail`.
The default is `return_error`, so a recoverable specialist failure becomes a
closed `{isError, agent, error: {code, message, retryable, call_number, limits}}`
tool result and the parent can
continue, retry, or choose another declared tool. The error also carries a
`retryable` flag and the effective specialist limits. Invalid specialist arguments,
specialist input guardrail rejection, and specialist execution failures use the same
policy-controlled result path. `max_calls` bounds
parent-driven retries independently; each invocation receives fresh iteration,
token, and tool-call budgets. `retryable` is true only while the failure is recoverable
and every relevant parent and specialist bound permits another invocation. Token, tool-call, iteration,
validation, provider, guardrail, and tool failures have distinct codes.
Run-wide budget exhaustion and cancellation always propagate; contracts cannot
override these execution boundaries.

`max_tool_calls_total` bounds all tool calls made by one `llm.agent` node and
defaults to `32` when omitted.

An `llm.agent` node may also declare `schema`, using the same package-relative
JSON Schema path as `llm.fill`. Invalid final responses consume another bounded
agent turn with explicit validation feedback; token-limit and refusal stop
reasons are failures rather than partial successes.

`guardrails` is an ordered array of named checks for `input`, `output`,
`tool_input`, or `tool_output`. A tool-stage check may select one declared
tool. qcg ships `regex_deny` (`params.pattern`), `json_schema`
(`params.schema`), and `command` (`params.command`). The `command` kind is the
external extension boundary: it sends the inspected value as JSON on stdin and
accepts a typed pass, violation, or error JSON object on stdout. Every
evaluation is journaled without the inspected value. A
violating guardrail with `tripwire = true` terminates the node immediately.
Executors return typed configuration/evaluation errors and typed violations.
`on_error = "fail"` propagates an executor error; `on_error = "block"` converts
it into a policy violation. Both paths emit structured events with stable code,
kind, message, policy, and violation details so clients can handle them without
parsing strings.

Node-level `model.provider` and `model.model` may be templates. They are
resolved from the durable run variables immediately before each request, then
the selected provider and its required capabilities are validated. This lets a
contract ask the operator to select an explicitly configured model without an
implicit fallback.

### Web search

`web.search` is implemented by the qcg agent harness and is independent of the
selected LLM provider. The model decides whether to call it, but cannot select
the provider's HTTP method, endpoint, headers, response mapping, or request
body. Search is enabled only when the contract declares this tool. The
selected `[[search_provider]]` row is explicit; there is no implicit search
profile or fallback. Selection is either the tool's `provider` or an explicit
`[default].search` entry in the registry; when neither exists, validation fails.

```toml
[[flow.params.tools]]
name = "search_web"
kind = "web.search"
description = "Search public documentation when current information is needed"
provider = "tinyfish-api"
max_results = 5
max_calls = 3

[permissions]
network = ["api.search.tinyfish.ai"]
```

The search-specific contract fields are `provider`, `max_results`, and
`max_calls`. `provider` names a `[[search_provider]]` row; it is required when
no explicit default is configured. `max_results` defaults to `5` and is
limited to `20`, and `max_calls` defaults to `3` and is limited to `10`. The
selected registry row owns the endpoint, query parameters, fixed headers, RFC
6901 result mapping, and authentication. Those transport fields are not valid
inline contract fields. The bundled API-key REST profile is named
`tinyfish-api`; it is separate from the OAuth MCP profile named `tinyfish`.
The built-in `exa-public` and `parallel-public` MCP profiles are anonymous
public research endpoints and remain registered even when no `providers.toml`
exists. A contract still binds exact MCP tools and grants the corresponding
hosts explicitly. Their ids are reserved and cannot be overridden by registry
rows.

The API key is read from the selected profile's `api_key_env` and injected into
its configured authentication header at run time. It is not a generator
secret and must not appear in `qcg.toml`, prompts, resources, URLs, query
parameters, or generated artifacts. A missing profile or credential fails
explicitly; qcg does not silently select another profile. Credentialed remote
profiles require HTTPS and do not follow redirects.

Results are reduced to `title`, `url`, and `snippet`, bounded by the declared
result limit, ranked in provider order, and marked with
`content_trust = "untrusted"`. Titles are limited to 512 characters and
snippets to 4096; oversized or incorrectly typed fields fail explicitly.
Search result URLs must be absolute HTTP(S) URLs without embedded credentials.
Search result URLs are citations, not an implicit permission to fetch those
pages; fetching page content requires a separately declared `http` tool and
network permission.

Each tool declaration must be a subset of `[permissions]`, and each tool call is
checked against the declared tool and the same runtime permission gateways as
deterministic steps.
Agent `http` calls do not follow redirects. Methods other than `GET` and `HEAD`
also pass through the declared side-effect confirmation policy.

### MCP tools

An MCP declaration binds one model-visible alias to one configured server and
one remote tool:

```toml
[[flow.params.tools]]
name = "lookup"
kind = "mcp"
description = "Look up a record when the request needs current data"
server = "my-tools"
tool = "lookup"
max_calls = 3
side_effects = false

[permissions]
network = ["mcp.example.com"]
side_effects = "none"
```

The required fields are `name`, `kind`, `server`, and `tool`. `server` must
match either a built-in public profile or an `[[mcp_server]]` row in the
selected `providers.toml`; `tool` is the remote MCP name and is never chosen by
the model. `max_calls` defaults to 3 and is limited to 10. `side_effects`
defaults to `true`; set it to `false` only for a known read-only operation. A
true value routes the call through the
contract's `[permissions].side_effects` policy: `none` denies it, `confirm`
and `dry_run_first` create the normal HITL boundary, and `allowed` permits it.

For Streamable HTTP, every host in the profile's `allowed_hosts` must also be
listed in `permissions.network`. For stdio, the complete profile `command`
vector must be listed in `permissions.commands`. qcg opens a separate MCP
protocol session for each run, even though OAuth credentials and token refresh
state are shared by the process-level profile runtime.

Before the first model request, qcg connects to each declared MCP server and
discovers its `tools/list` schema. That input schema is authoritative for
argument validation; untrusted descriptions, titles, defaults, examples, and
comments are removed from the model-facing schema. If the server advertises an
`outputSchema`, qcg validates the returned `structuredContent` against it.
Internal JSON Schema references and composition keywords are supported;
external references are rejected instead of being resolved over the network
or from the filesystem.
MCP result text remains untrusted data and is scanned for declared secret
values before it is returned to the model. Discovery is limited to 100 pages,
each input and output schema is limited to 256 KiB, and the profile's
`max_response_bytes` limits protocol and metadata bodies.

MCP calls use the profile's timeout (120 seconds by default), honor the run
cancellation token, and close the protocol session when the run ends. A run
cancelled through the API therefore cancels in-flight MCP discovery or calls as
well as the LLM loop.

Each MCP profile explicitly selects the `initialize` lifecycle or the 2026-07-28
`server/discover` lifecycle. Custom profiles default to `discover`, while known
public profiles are pinned to their verified lifecycle. The client advertises the
Tasks extension and supports multi-round-trip
`input_required` tool results. MCP elicitation requests become runtime-generated
forms on the ordinary durable DAG pause/resume boundary; answers and the opaque
request state are returned to the original tool call. Unsupported input request
methods fail explicitly. Client sampling and roots are not exposed.
Side-effect journals contain only argument names and encoded size, never raw MCP
argument values.

## `[outputs]`

Declare primary artifacts on the producing flow node so the path has one source
of truth:

```toml
[[flow]]
id = "write_report"
type = "write"
artifact = { label = "Report", description = "Generated analysis report", preview = "text", required = true, mime = "text/markdown" }

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
`description` is displayed by clients. `preview` is `auto`, `text`, `image`,
`html`, `json`, `markdown`, `pdf`, `audio`, `video`, or `none`; it controls
browser presentation and must be compatible with the artifact MIME type.
Symlinks and paths that resolve outside the workspace are rejected.

## `[runtime]`, `[budget]`, `[failure]`, `[journal]`, `[assets]`

These sections are parsed as policy/display surfaces for generators. Unknown
fields are rejected when their structs define a closed schema.

`[runtime]`
:: Single source for command, HTTP, file, and template limits:
   `command_timeout_seconds`, `command_input_limit_bytes`,
   `command_output_limit_bytes`, `http_timeout_seconds`,
   `http_body_limit_bytes`, `http_redirect_limit`, `file_input_limit_bytes`,
   `file_count_limit`, `input_total_limit_bytes`, `output_file_limit_bytes`,
   `output_total_limit_bytes`,
   `output_artifact_limit`, `template_source_limit_bytes`,
   `template_context_limit_bytes`, `journal_event_limit_bytes`,
   `journal_total_limit_bytes`, `journal_event_count_limit`,
   `state_limit_bytes`, `template_output_limit_bytes`, and `template_fuel`.
   Template output is streamed through a bounded writer and each render gets a
   fresh fuel budget; exceeding either limit fails the render explicitly.
   Template source and serialized context are bounded before compilation and
   evaluation. `file_input_limit_bytes` bounds every individual input value
   (including `file`, `json`, and schema-backed custom fields), while
   `input_total_limit_bytes` bounds the sum of encoded input values. These
   limits apply to initial inputs and interactive form answers. The defaults
   are `input_total_limit_bytes = 268435456` (256 MiB),
   `output_file_limit_bytes = 67108864` (64 MiB),
   `output_total_limit_bytes = 268435456` (256 MiB),
   `output_artifact_limit = 10000`, `template_source_limit_bytes = 1048576`
   (1 MiB), `template_context_limit_bytes = 16777216` (16 MiB),
   `journal_event_limit_bytes = 16777216` (16 MiB),
   `journal_total_limit_bytes = 268435456` (256 MiB),
   `journal_event_count_limit = 100000`, `state_limit_bytes = 67108864`
   (64 MiB),
   `template_output_limit_bytes = 16777216` (16 MiB), and
   `template_fuel = 1000000` instructions.
   Manifests cannot disable bounding by supplying extreme values: timeouts are
   capped at 604800 seconds, byte limits at 1073741824 (1 GiB), count limits at
   1000000, redirects at 32, and template fuel at 100000000. Run budgets are
   capped at 1000000 steps, 10000000000 tokens, and 2592000 elapsed seconds.
   Values above a hard ceiling are rejected rather than clamped.
   Confirmation plans display the command limits used by execution.

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
