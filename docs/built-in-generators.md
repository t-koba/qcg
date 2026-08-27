# Built-in Generator

qcg ships one generator named `generator`. It is both the standard browser
demo and a general generator-authoring application. Its SPA is an ordinary
`[assets]` directory and uses the same HTTP API available to any custom client.

The authoring flow collects the new generator's identity, schema, behavior,
and permissions. Permissions are assembled deterministically from explicit
operator answers; the LLM proposal schema rejects `generator` and `permissions`
inside its manifest. A proposal may declare ordinary generator secrets, which
remain visible in the generated contract's permission summary when it is run;
credential variables from loaded LLM provider registry rows are reserved and
cannot be reused as generator secrets.

The design stage has two modes:

- `manual` accepts a structured `design_json` in `ask_manual_form` and performs
  no LLM call.
- `llm` asks the configured model for a proposal, then validates the generated
  contract as the source of truth.

The LLM design vocabulary includes `ask_user`, dynamic `fields_from` forms,
explicit `needs`, and conditional `when` branches. The authoring model is
instructed to insert HITL only where human judgment changes the workflow and to
route the downstream DAG from that answer. The completed package is then loaded
by `check.contract`, so an invalid or unsupported interactive graph is rejected
before delivery.

The LLM mode requires a `package` design carrying a complete manifest body and
a path-to-UTF-8-content source map. Map keys make paths unique by construction;
the package schema also rejects unsafe paths and reserves the root `qcg.toml`
for the deterministically assembled manifest. Manual mode may use the same
package form or its compact authoring form. This is a general reproduction
mechanism. Self-reproduction is only one test-supplied intent used to verify
it, not product behavior embedded in the generator.

Run the authoring flow directly:

```bash
qcg run generators/generator \
  --answer 'ask_purpose={"description":"Makes a small file"}' \
  --answer ask_design_mode=manual \
  --answer 'ask_manual_form={"generator_id":"my-generator","generator_name":"My Generator","artifact_path":"README.md","primary_step_type":"render","design_json":{"input_fields":[{"id":"request","type":"natural_language","required":true}]},"include_readme":false}' \
  --answer 'ask_manual_render_details={"artifact_content":"# My Generator"}' \
  --answer ask_fs_write=workspace \
  --answer ask_network=none \
  --answer ask_commands=none \
  --answer ask_containers=none \
  --answer ask_side_effects=none \
  --answer ask_secrets=none \
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
