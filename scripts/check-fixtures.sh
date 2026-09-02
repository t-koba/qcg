#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/qcg-example-smoke.XXXXXX")"
trap 'rm -rf "$tmp_root"' EXIT

run_qcg() {
  (cd "$tmp_root" && CARGO_TARGET_DIR="$repo_root/target" cargo run --manifest-path "$repo_root/Cargo.toml" -p qcg -- "$@")
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
  --output "$tmp_root/hello-template" \
  --yes >/dev/null

run_qcg run "$repo_root/fixtures/generators/transform-formats" \
  --output "$tmp_root/transform-formats" \
  --yes >/dev/null

run_qcg run "$repo_root/fixtures/generators/dynamic-form" \
  --answer 'collect={"decision":"keep","reason":"smoke"}' \
  --output "$tmp_root/dynamic-form" \
  --yes >/dev/null
grep -q 'Decision: keep' "$tmp_root/dynamic-form/decision.txt"

run_qcg run "$repo_root/fixtures/generators/parallel-wave" \
  --output "$tmp_root/parallel-wave" \
  --yes >/dev/null
grep -R -q '"parallel":true' "$tmp_root/.qcg/runs"

run_qcg run "$repo_root/fixtures/generators/logical-tool-host" \
  --output "$tmp_root/logical-tool-host" \
  --yes >/dev/null
grep -R -q 'tool_backend_resolved' "$tmp_root/.qcg/runs"

run_qcg run "$repo_root/fixtures/generators/llm-fill-retry" \
  --output "$tmp_root/llm-fill-retry" \
  --yes >/dev/null
grep -q 'retry passed' "$tmp_root/llm-fill-retry/result.json"

run_qcg run "$repo_root/fixtures/generators/llm-agent-fake" \
  --output "$tmp_root/llm-agent-fake" \
  --yes >/dev/null
grep -q 'agent delegated and wrote this' "$tmp_root/llm-agent-fake/drafts/result.txt"
grep -R -q 'agent_delegated' "$tmp_root/.qcg/runs"
grep -R -q 'agent_completed' "$tmp_root/.qcg/runs"

run_qcg run "$repo_root/fixtures/generators/on-fail-ask-user" \
  --output "$tmp_root/on-fail-ask-user" \
  --answer check:on_fail=accepted \
  --yes >/dev/null
grep -q 'accepted' "$tmp_root/on-fail-ask-user/decision.txt"

run_qcg run "$repo_root/fixtures/generators/repair-exhausted-route" \
  --output "$tmp_root/repair-exhausted-route" \
  --yes >/dev/null
grep -q 'repair exhausted' "$tmp_root/repair-exhausted-route/fallback.txt"

run_qcg run "$repo_root/generators/generator" \
  --output "$tmp_root/generator-authoring" \
  --answer 'ask_purpose={"description":"Smoke generated package"}' \
  --answer ask_design_mode=manual \
  --answer 'ask_manual_form={"package":{"manifest":{"generator":{"id":"smoke-gen","name":"Smoke Gen","version":"0.1.0","qcg_version":"^0.1","description":"Generate a smoke artifact","authors":[]},"inputs":{"stages":[{"id":"main","fields":[{"id":"request","type":"natural_language","required":true}]}]},"flow":[{"id":"emit","type":"render","artifact":{"label":"Smoke artifact","preview":"text","required":true},"params":{"template":"templates/artifact.txt.j2","output_file":"README.md"}}]},"sources":{"templates/artifact.txt.j2":{"encoding":"utf8","content":"# Smoke"}}}}' \
  --answer 'ask_authority={"permissions":{"fs_read":[],"fs_write":["workspace"],"network":[],"commands":[],"containers":{"enabled":false,"images":[],"on_missing":"error"},"side_effects":"none"},"secrets":{}}' \
  --yes >/dev/null
run_qcg validate "$tmp_root/generator-authoring/generator" >/dev/null
run_qcg run "$tmp_root/generator-authoring/generator" \
  --input request='Smoke request' \
  --output "$tmp_root/generated-generator" \
  --yes >/dev/null
test -f "$tmp_root/generated-generator/README.md"

printf '%s\n' '{"enabled":true}' >"$tmp_root/file-input.json"
run_qcg run "$repo_root/fixtures/generators/file-input" \
  --input-file "config_file=$tmp_root/file-input.json" \
  --output "$tmp_root/file-input" \
  --yes >/dev/null
grep -q 'files/config_file/file-input.json' "$tmp_root/file-input/summary.md"

if command -v cc >/dev/null 2>&1; then
  run_qcg run "$repo_root/fixtures/generators/hello-c-builder" \
    --output "$tmp_root/hello-c-builder" \
    --yes >/dev/null
else
  echo "skip hello-c-builder run: cc not found"
fi

echo "fixture smoke ok"
