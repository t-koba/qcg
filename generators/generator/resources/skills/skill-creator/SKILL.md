---
name: skill-creator
description: >-
  Create purpose-specific Agent Skills for a generated qcg generator. Use when
  the requested workflow needs specialized procedural knowledge, a reusable
  checklist, or reference material that should be loaded on demand instead of
  living in the main prompt.
license: Apache-2.0
compatibility: Works with any qcg generator package.
metadata:
  author: qcg
  version: "1.0"
---

# Skill Creator

Create a skill only when the target generator benefits from procedural
knowledge that is too detailed for its main prompts. A skill is loaded on
demand, so it keeps the base prompt small while giving the model precise
instructions when the task matches.

## When to include a skill

Include one or more skills when the requested generator:

- has a workflow with ordered steps, checks, or decision points;
- needs domain reference material (schemas, policies, examples) that is not
  relevant to every run;
- benefits from a specialized specialist prompt that should stay reusable.

Do not create a skill for a single prompt that already fits in the flow node.

## Layout

Place each skill inside the generated package:

```text
resources/skills/<skill-name>/SKILL.md
resources/skills/<skill-name>/references/<topic>.md
```

- `SKILL.md` is required. `references/` holds detail loaded on demand.
- The `name` frontmatter field must match `<skill-name>`.
- Keep `SKILL.md` under 500 lines and reference files focused.

## Frontmatter

```yaml
---
name: <skill-name>
description: What the skill does and when to use it.
---
```

`name` and `description` are required. `license`, `compatibility`, `metadata`
(a string map), and `allowed-tools` are optional. `allowed-tools` is
informational: it never grants command or network permissions. Every command
or host still needs an explicit declaration in the generated manifest.

## Register the skill in the manifest

Declare the skill resource and use it as node context:

```toml
[resources.<resource-name>]
type = "skill"
path = "resources/skills/<skill-name>"
trust = "trusted"
llm_visible = true

[[flow]]
id = "draft"
type = "llm.generate"
context = [{ resource = "<resource-name>", select = "instructions" }]

[flow.params]
prompt = "prompts/draft.j2"
output_file = "draft.txt"
```

Selectors:

- `select = "instructions"` loads the `SKILL.md` body.
- `select = "meta"` loads name, description, and spec metadata.
- `select = "tree"` lists bundled files without loading them.
- `select = "files"`, `path = "references/topic.md"` loads one reference file.

## Authoring rules

- Write the description so a model can decide when the skill applies.
- Put the short procedure in `SKILL.md`; put long examples and tables in
  `references/`.
- Never rely on a skill to grant capabilities. Skills are data; permissions
  stay in the manifest and remain operator-approved.
