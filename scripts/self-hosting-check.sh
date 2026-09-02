#!/usr/bin/env bash
# Self-hosting verification against a real OpenRouter model.
#
# The checked-in generator is copied to a temporary directory and configured
# to use the selected free model. It then produces a generator-authoring
# clone, and that clone produces a second clone. This is intentionally separate
# from CI: the provider is remote and model output is not deterministic.
set -euo pipefail

cd "$(dirname "$0")/.."
repo_root="$(pwd)"

: "${QCG_OPENROUTER_API_KEY:?set QCG_OPENROUTER_API_KEY}"

model="${QCG_LLM_MODEL:-minimax/minimax-m3:free}"
case "$model" in
  *[![:alnum:]_.:/-]*)
    echo "QCG_LLM_MODEL contains unsupported characters: $model" >&2
    exit 2
    ;;
esac

tmp="$(mktemp -d "${TMPDIR:-/tmp}/qcg-self-hosting.XXXXXX")"
cleanup() {
  local status="$?"
  if [ "$status" -ne 0 ] && [ "${QCG_SELF_HOSTING_KEEP_TEMP:-false}" = "true" ]; then
    echo "self-hosting worktree retained after failure: $tmp" >&2
  else
    rm -rf "$tmp"
  fi
}
trap cleanup EXIT

if [ -n "${QCG_PROVIDERS:-}" ]; then
  providers="$QCG_PROVIDERS"
  if [[ "$providers" != /* ]]; then
    providers="$repo_root/$providers"
  fi
else
  providers="$tmp/providers.toml"
  printf '%s\n' \
    '[[provider]]' \
    'id = "openrouter"' \
    'api = "chat_completions"' \
    'base_url = "https://openrouter.ai/api/v1"' \
    'base_url_env = "QCG_OPENROUTER_BASE_URL"' \
    'api_key_env = "QCG_OPENROUTER_API_KEY"' \
    'timeout_seconds = 300' \
    'capabilities = { tool_use = true, json_schema = true, seed = false }' \
    >"$providers"
fi

# Keep the source checkout immutable while selecting the remote model for the
# llm.fill step. This also lets depth 2 use the same provider if the first
# model proposal omitted an explicit [llm] section.
configure_llm() {
  local manifest="$1"
  python3 - "$manifest" "$model" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
model = sys.argv[2]
text = path.read_text()
newline = "\r\n" if "\r\n" in text else "\n"
lines = text.splitlines(keepends=True)
section = None
changed = False
for index, line in enumerate(lines):
    stripped = line.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        section = stripped
    if section == "[llm.model]":
        if re.match(r"^\s*model\s*=", line):
            lines[index] = f'model = "{model}"{newline}'
            changed = True
        elif re.match(r"^\s*provider\s*=", line):
            lines[index] = f'provider = "openrouter"{newline}'
            changed = True
    elif section == "[llm]" and re.match(r"^\s*model\s*=", line):
        lines[index] = f'model = {{ provider = "openrouter", model = "{model}" }}{newline}'
        changed = True

if not changed:
    if lines and not lines[-1].endswith(("\n", "\r")):
        lines[-1] += newline
    lines.extend([
        newline,
        "[llm]" + newline,
        f'model = {{ provider = "openrouter", model = "{model}" }}' + newline,
    ])

path.write_text("".join(lines))
PY
}

source="$tmp/source"
cp -R generators/generator "$source"
configure_llm "$source/qcg.toml"

# Build the exact package proposal that the model must return. The UI subtree
# is a derived build artifact, so it does not belong to the behavioral
# reproduction blueprint.
write_blueprint() {
  local generator_root="$1"
  local output="$2"
  python3 - "$generator_root" "$output" <<'PY'
import base64
import json
import pathlib
import sys
import tomllib

root = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
manifest = tomllib.loads((root / "qcg.toml").read_text())
asset_dirs = [pathlib.PurePosixPath(value) for value in manifest.get("assets", {}).get("dirs", [])]
manifest.pop("permissions", None)
manifest.pop("secrets", None)
manifest.pop("assets", None)

sources = {}
for path in sorted(root.rglob("*")):
    if not path.is_file():
        continue
    relative = pathlib.PurePosixPath(path.relative_to(root).as_posix())
    if relative.as_posix() == "qcg.toml":
        continue
    if any(relative == directory or directory in relative.parents for directory in asset_dirs):
        continue
    content = path.read_bytes()
    try:
        sources[relative.as_posix()] = {
            "encoding": "utf8",
            "content": content.decode("utf-8"),
        }
    except UnicodeDecodeError:
        sources[relative.as_posix()] = {
            "encoding": "base64",
            "content": base64.b64encode(content).decode("ascii"),
        }

proposal = {
    "package": {"manifest": manifest, "sources": sources},
}
output.write_text(json.dumps(proposal, ensure_ascii=False, separators=(",", ":")))
PY
}

purpose_for_blueprint() {
  local blueprint="$1"
  python3 - "$blueprint" <<'PY'
import json
import pathlib
import sys

proposal = pathlib.Path(sys.argv[1]).read_text()
parsed = json.loads(proposal)
manifest = parsed["package"]["manifest"]
flow = manifest["flow"]
sources = parsed["package"]["sources"]
instruction = (
    "Reproduce the attached qcg generator authoring tool with equivalent behavior. "
    "The attached proposal is final data, not an example and not a design request. "
    "Return the attached proposal JSON exactly: preserve package.manifest and every "
    "package.sources path and content byte-for-byte; do not summarize, omit, rename, "
        "reorder object keys, or redesign anything. A single missing key fails verification. "
    f"The manifest keys must remain {sorted(manifest)}, flow must contain exactly "
    f"{len(flow)} entries, and sources must contain exactly {len(sources)} entries. "
    "In particular, preserve output=design_out on the design_proposal flow entry. "
    "Preserve Jinja source bytes literally, including nested braces such as "
    "{{ '{{' }}; never render, simplify, or normalize template source. Preserve "
    "every when expression literally, including the distinction between .package "
    "and .package.sources. "
    "Attached proposal JSON: " + proposal
)
print(json.dumps({"description": instruction}, ensure_ascii=False, separators=(",", ":")))
PY
}

verify_equivalent() {
  local expected="$1"
  local actual="$2"
  python3 - "$expected" "$actual" <<'PY'
import base64
import hashlib
import json
import pathlib
import sys
import tomllib

def representation(root_value):
    root = pathlib.Path(root_value)
    manifest = tomllib.loads((root / "qcg.toml").read_text())
    asset_dirs = [pathlib.PurePosixPath(value) for value in manifest.get("assets", {}).get("dirs", [])]
    manifest.pop("permissions", None)
    manifest.pop("secrets", None)
    manifest.pop("assets", None)
    sources = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = pathlib.PurePosixPath(path.relative_to(root).as_posix())
        if relative.as_posix() == "qcg.toml":
            continue
        if any(relative == directory or directory in relative.parents for directory in asset_dirs):
            continue
        content = path.read_bytes()
        try:
            sources[relative.as_posix()] = {
                "encoding": "utf8",
                "content": content.decode("utf-8"),
            }
        except UnicodeDecodeError:
            sources[relative.as_posix()] = {
                "encoding": "base64",
                "content": base64.b64encode(content).decode("ascii"),
            }
    return {"manifest": manifest, "sources": sources}

expected = representation(sys.argv[1])
actual = representation(sys.argv[2])
if expected != actual:
    expected_paths = set(expected["sources"])
    actual_paths = set(actual["sources"])
    details = []
    if expected["manifest"] != actual["manifest"]:
        details.append("manifest differs")
    if expected_paths != actual_paths:
        details.append(f"source paths differ: missing={sorted(expected_paths - actual_paths)} extra={sorted(actual_paths - expected_paths)}")
    changed = sorted(path for path in expected_paths & actual_paths if expected["sources"][path] != actual["sources"][path])
    if changed:
        details.append(f"source contents differ: {changed}")
    raise SystemExit("equivalence check failed: " + "; ".join(details))

canonical = json.dumps(actual, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
print(hashlib.sha256(canonical).hexdigest())
PY
}

run_generation() {
  local input_generator="$1"
  local output_dir="$2"
  local blueprint="$output_dir.proposal.json"
  local purpose
  write_blueprint "$input_generator" "$blueprint"
  purpose="$(purpose_for_blueprint "$blueprint")"
  (
    cd "$tmp"
    CARGO_TARGET_DIR="$repo_root/target" cargo run --manifest-path "$repo_root/Cargo.toml" -q -p qcg -- \
      --providers "$providers" run "$input_generator" \
      --answer "ask_purpose=$purpose" \
      --answer ask_design_mode=llm \
      --answer ask_research=none \
      --answer 'ask_authority={"permissions":{"fs_read":["workspace"],"fs_write":["workspace"],"network":["mcp.exa.ai","search.parallel.ai"],"commands":[],"containers":{"enabled":false,"images":[],"on_missing":"error"},"side_effects":"none"},"secrets":{}}' \
      --output "$output_dir" \
      --yes
  )
}

generation_attempts="${QCG_SELF_HOSTING_ATTEMPTS:-3}"
case "$generation_attempts" in
  ''|*[!0-9]*|0)
    echo "QCG_SELF_HOSTING_ATTEMPTS must be a positive integer" >&2
    exit 2
    ;;
esac

generated_root=""
generated_fingerprint=""
generate_equivalent() {
  local input_generator="$1"
  local label="$2"
  local attempt output_dir candidate fingerprint
  for attempt in $(seq 1 "$generation_attempts"); do
    output_dir="$tmp/$label-attempt-$attempt"
    echo "self-hosting generation: label=$label attempt=$attempt/$generation_attempts model=$model"
    if ! run_generation "$input_generator" "$output_dir"; then
      continue
    fi
    candidate="$output_dir/generator"
    if ! CARGO_TARGET_DIR="$repo_root/target" cargo run --manifest-path "$repo_root/Cargo.toml" -q -p qcg -- --providers "$providers" validate "$candidate"; then
      continue
    fi
    if fingerprint="$(verify_equivalent "$input_generator" "$candidate")"; then
      generated_root="$candidate"
      generated_fingerprint="$fingerprint"
      return 0
    fi
  done
  echo "failed to reproduce an equivalent generator for $label after $generation_attempts attempts" >&2
  return 1
}

generate_equivalent "$source" clone-a
clone_a="$generated_root"
clone_a_fingerprint="$generated_fingerprint"
configure_llm "$clone_a/qcg.toml"

generate_equivalent "$clone_a" clone-b
clone_b_fingerprint="$generated_fingerprint"

if [ "$clone_a_fingerprint" != "$clone_b_fingerprint" ]; then
  echo "generation fingerprints differ: depth 1=$clone_a_fingerprint depth 2=$clone_b_fingerprint" >&2
  exit 1
fi

echo "self-hosting check ok: generations=2 model=$model fingerprint=$clone_b_fingerprint"
