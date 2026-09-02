# Built-in Generator

qcg ships one generator named `generator`. It is both the standard browser
demo and a general generator-authoring application. Its SPA is an ordinary
`[assets]` directory and uses the same HTTP API available to any custom client.

The authoring flow collects the new generator's identity, complete package
manifest, typed source files, and operator authority. Permissions and secrets
are assembled deterministically from the explicit authority answer; the LLM
proposal schema rejects `permissions` and `secrets` inside its manifest while
requiring the core generator metadata fields. Credential variables from loaded LLM, search, or MCP provider
registry rows are reserved and cannot be reused as generator secrets.

The design stage has two modes:

- `manual` accepts a complete `package` in `ask_manual_form` and performs no
  LLM call.
- `llm` asks the configured model for a proposal, then validates the generated
  contract as the source of truth.

The LLM design vocabulary includes `ask_user`, dynamic `fields_from` forms,
explicit `needs`, conditional `when` branches, parallel groups, bounded nested
`foreach`, and checkpointed `llm.agent` tools. The design and research nodes
stream output, carry explicit context limits, and run under step, token, and
elapsed-time budgets with strict failure and journal policies. The trusted
authoring reference also teaches resource context, multimodal media, explicit
model fallbacks, isolation, validation gates, and artifact declarations.

Before LLM design, the demo offers `Exa + Parallel public MCP` as its first and
recommended research choice, followed by either provider alone or no external
research. The built-in `exa-public` and `parallel-public` profiles require no
registry file or API key and expose only exact read-only search/fetch bindings
with per-tool and agent-wide call limits. Their network permissions are fixed
to `mcp.exa.ai` and `search.parallel.ai`. Search results remain untrusted model context. The
`tinyfish` OAuth profile and credentialed REST `[[search_provider]]` profiles
remain explicit alternatives rather than the no-setup default.

Generated contracts fix MCP `server` and `tool` names; transport, runtime
schema, authentication, and credentials remain in the external provider
registry. The authoring model inserts HITL only where human judgment changes
the workflow and routes the downstream DAG from that answer. A side-effecting
MCP binding pauses at the ordinary confirmation boundary according to
`[permissions].side_effects`. The completed package is loaded by
`check.contract`, so an invalid or unsupported interactive graph is rejected
before delivery.

When the requested workflow does not need current external information, select
`none`; no research MCP session is opened. Likewise, a generated contract that
does not need current external information should contain no search or MCP tool
and no corresponding network permission.

The LLM mode requires a `package` design carrying a complete manifest body and
a path-to-typed-source map. Each source is `{encoding = "utf8"|"base64",
content = "..."}` with an optional canonical octal `unix_mode` from `0600`
through `0777`; base64 sources are decoded and their staging files removed.
Map keys make paths unique by construction; the package schema also rejects
unsafe paths and reserves the root `qcg.toml` for the deterministically
assembled manifest. Manual mode uses the same package form. This is a general
reproduction mechanism. Self-reproduction is only one test-supplied intent
used to verify it, not product behavior embedded in the generator.

Run the authoring flow directly:

```bash
qcg run generators/generator \
  --answer 'ask_purpose={"description":"Makes a small file"}' \
  --answer ask_design_mode=manual \
  --answer 'ask_manual_form={"package":{"manifest":{"generator":{"id":"my-generator","name":"My Generator","version":"0.1.0","qcg_version":"^0.1","description":"Makes a small file","authors":[]},"inputs":{"stages":[{"id":"main","fields":[{"id":"request","type":"natural_language","required":true}]}]},"flow":[{"id":"emit","type":"render","artifact":{"label":"Generated README","preview":"text","required":true},"params":{"template":"templates/readme.j2","output_file":"README.md"}}]},"sources":{"templates/readme.j2":{"encoding":"utf8","content":"# My Generator\\n{{ inputs.request }}"}}}}' \
  --answer 'ask_authority={"permissions":{"fs_read":[],"fs_write":["workspace"],"network":[],"commands":[],"containers":{"enabled":false,"images":[],"on_missing":"error"},"side_effects":"none"},"secrets":{}}' \
  --output out \
  --yes
```

After building the frontend, the same application is available at
`/api/generators/generator/assets/ui/index.html` from `qcg serve`.

Run the real-provider two-generation equivalence check with:

```bash
bash scripts/self-hosting-check.sh
```

The script uses `QCG_OPENROUTER_API_KEY` when set, otherwise reads the
OpenRouter key from the OpenCode authentication file without printing it.
