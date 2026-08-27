#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cargo run -q -p qcg -- docs step-schemas > "$tmp/step-schemas"
cargo run -q -p qcg -- docs run-events > "$tmp/run-events"
cargo run -q -p qcg -- docs openapi > "$tmp/openapi.json"

trim_trailing_blank_lines() {
  local source="$1"
  local target="$2"
  awk '{lines[NR]=$0; if ($0 !~ /^[[:space:]]*$/) last=NR} END {for (i=1; i<=last; i++) print lines[i]}' "$source" > "$target"
}

trim_trailing_blank_lines "$tmp/step-schemas" "$tmp/step-schemas-normalized"
trim_trailing_blank_lines "$tmp/run-events" "$tmp/run-events-normalized"
trim_trailing_blank_lines "$tmp/openapi.json" "$tmp/openapi-normalized.json"

awk '/<!-- qcg-step-schemas:start -->/{inside=1; next} /<!-- qcg-step-schemas:end -->/{inside=0} inside' docs/contract-reference.md | awk '{lines[NR]=$0; if ($0 !~ /^[[:space:]]*$/) last=NR} END {for (i=1; i<=last; i++) print lines[i]}' > "$tmp/step-schemas-doc"
awk '/<!-- qcg-run-events:start -->/{inside=1; next} /<!-- qcg-run-events:end -->/{inside=0} inside' docs/run-event-reference.md | awk '{lines[NR]=$0; if ($0 !~ /^[[:space:]]*$/) last=NR} END {for (i=1; i<=last; i++) print lines[i]}' > "$tmp/run-events-doc"

diff -u "$tmp/step-schemas-normalized" "$tmp/step-schemas-doc"
diff -u "$tmp/run-events-normalized" "$tmp/run-events-doc"
diff -u "$tmp/openapi-normalized.json" docs/openapi.json

python3 - <<'PY'
import pathlib
import re
import sys
import urllib.parse

root = pathlib.Path.cwd()
documents = [root / "README.md", root / "SECURITY.md"]
documents.extend(
    path
    for path in (root / "docs").rglob("*.md")
    if "internal" not in path.relative_to(root / "docs").parts
)
link_pattern = re.compile(r"!?\[[^]]*\]\(([^)]+)\)")
errors = []

for document in documents:
    text = document.read_text(encoding="utf-8")
    for match in link_pattern.finditer(text):
        destination = match.group(1).strip()
        if destination.startswith("<") and destination.endswith(">"):
            destination = destination[1:-1]
        else:
            destination = destination.split(maxsplit=1)[0]
        destination = urllib.parse.unquote(destination.split("#", 1)[0])
        if not destination or destination.startswith(("/", "http://", "https://", "mailto:")):
            continue
        target = (document.parent / destination).resolve()
        if not target.exists():
            line = text.count("\n", 0, match.start()) + 1
            errors.append(
                f"{document.relative_to(root)}:{line}: missing local link `{destination}`"
            )

if errors:
    print("\n".join(errors), file=sys.stderr)
    sys.exit(1)
PY

echo "documentation checks passed"
