#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
# mktemp template below uses a dedicated scratch prefix; a naive
# `grep -R XXX` would match the template itself, so TODO greps must use
# word boundaries (e.g. `grep -Rw 'TODO|XXX|FIXME' --exclude-dir=target .`).
scratch_root="$(mktemp -d "${TMPDIR:-/tmp}/qcg-example-smoke.XXXXXX")"
trap 'rm -rf "$scratch_root"' EXIT

run_qcg() {
  (cd "$scratch_root" && CARGO_TARGET_DIR="$repo_root/target" cargo run --manifest-path "$repo_root/Cargo.toml" -p qcg -- "$@")
}

for dir in generators/* fixtures/generators/*; do
  if [ ! -f "$dir/qcg.toml" ]; then
    continue
  fi
  run_qcg validate "$repo_root/$dir" >/dev/null
  echo "valid: $dir"
done

run_qcg run "$repo_root/fixtures/generators/hello-template" \
  --input name=qcg \
  --output "$scratch_root/hello-template" \
  --yes >/dev/null

run_qcg run "$repo_root/fixtures/generators/transform-formats" \
  --output "$scratch_root/transform-formats" \
  --yes >/dev/null

run_qcg run "$repo_root/fixtures/generators/dynamic-form" \
  --answer 'collect:ask_user:2a1789047a371f6a664855aaeae4c210427b74ad58d988ee0e0815f8f0536fd3={"decision":"keep","reason":"smoke"}' \
  --output "$scratch_root/dynamic-form" \
  --yes >/dev/null
grep -q 'Decision: keep' "$scratch_root/dynamic-form/decision.txt"

run_qcg run "$repo_root/fixtures/generators/parallel-wave" \
  --output "$scratch_root/parallel-wave" \
  --yes >/dev/null
grep -R -q '"parallel":true' "$scratch_root/.qcg/runs"

run_qcg run "$repo_root/fixtures/generators/logical-tool-host" \
  --output "$scratch_root/logical-tool-host" \
  --yes >/dev/null
grep -R -q 'tool_backend_resolved' "$scratch_root/.qcg/runs"

run_qcg run "$repo_root/fixtures/generators/llm-fill-retry" \
  --output "$scratch_root/llm-fill-retry" \
  --yes >/dev/null
grep -q 'retry passed' "$scratch_root/llm-fill-retry/result.json"

run_qcg run "$repo_root/fixtures/generators/llm-agent-fake" \
  --output "$scratch_root/llm-agent-fake" \
  --yes >/dev/null
grep -q 'agent delegated and wrote this' "$scratch_root/llm-agent-fake/drafts/result.txt"
grep -R -q 'agent_delegated' "$scratch_root/.qcg/runs"
grep -R -q 'agent_completed' "$scratch_root/.qcg/runs"

run_qcg run "$repo_root/fixtures/generators/on-fail-ask-user" \
  --output "$scratch_root/on-fail-ask-user" \
  --answer check:on_fail=accepted \
  --yes >/dev/null
grep -q 'accepted' "$scratch_root/on-fail-ask-user/decision.txt"

run_qcg run "$repo_root/fixtures/generators/repair-exhausted-route" \
  --output "$scratch_root/repair-exhausted-route" \
  --yes >/dev/null
grep -q 'repair exhausted' "$scratch_root/repair-exhausted-route/fallback.txt"

run_qcg run "$repo_root/generators/generator" \
  --output "$scratch_root/generator-authoring" \
  --answer 'ask_purpose:ask_user:d334a65de4f45a9aa0ef33a23d19020d6f579dfd647c1180018c5cc9225e4527={"description":"Smoke generated package"}' \
  --answer ask_design_mode:ask_user:c56e2588ba87dccdb2a232dbd3a3536e6bdca3fba6bbf2ee3a8ad5c5767de5cc=manual \
  --answer 'ask_manual_form:ask_user:cfb35af7507ce1c535b14dc0a5b605073cf27e420192ef2b6c4d074c510e3eb6={"package":{"manifest":{"generator":{"id":"smoke-gen","name":"Smoke Gen","version":"0.1.0","qcg_version":"^0.1","description":"Generate a smoke artifact","authors":[]},"inputs":{"stages":[{"id":"main","fields":[{"id":"request","type":"natural_language","required":true}]}]},"flow":[{"id":"emit","type":"render","artifact":{"label":"Smoke artifact","preview":"text","required":true},"params":{"template":"templates/artifact.txt.j2","output_file":"README.md"}}]},"sources":{"templates/artifact.txt.j2":{"encoding":"utf8","content":"# Smoke"}}}}' \
  --answer 'ask_authority:ask_user:85bcc546274c9fde9ed86f57701f8d9afac56baf0c630e430a5fc85ec1258da6={"permissions":{"fs_read":[],"fs_write":["workspace"],"network":[],"commands":[],"containers":{"enabled":false,"images":[],"on_missing":"error"},"side_effects":"none","side_effects_scope":"invocation"},"secrets":{}}' \
  --yes >/dev/null
run_qcg validate "$scratch_root/generator-authoring/generator" >/dev/null
run_qcg run "$scratch_root/generator-authoring/generator" \
  --input request='Smoke request' \
  --output "$scratch_root/generated-generator" \
  --yes >/dev/null
test -f "$scratch_root/generated-generator/README.md"

printf '%s\n' '{"enabled":true}' >"$scratch_root/file-input.json"
run_qcg run "$repo_root/fixtures/generators/file-input" \
  --input-file "config_file=$scratch_root/file-input.json" \
  --output "$scratch_root/file-input" \
  --yes >/dev/null
grep -q 'files/config_file/file-input.json' "$scratch_root/file-input/summary.md"

if command -v cc >/dev/null 2>&1; then
  run_qcg run "$repo_root/fixtures/generators/hello-c-builder" \
    --output "$scratch_root/hello-c-builder" \
    --yes >/dev/null
else
  echo "skip hello-c-builder run: cc not found"
fi

echo "fixture smoke ok"
