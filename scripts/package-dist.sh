#!/usr/bin/env bash
set -euo pipefail

profile="release"
target_profile="release"
dry_run="false"
out_dir="dist"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --debug)
      profile="dev"
      target_profile="debug"
      ;;
    --dry-run)
      dry_run="true"
      ;;
    --out-dir)
      shift
      out_dir="${1:?missing --out-dir value}"
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

require_path() {
  if [ ! -e "$1" ]; then
    echo "missing required bundle path: $1" >&2
    exit 1
  fi
}

public_docs=(
  "built-in-generators.md"
  "cli-reference.md"
  "contract-reference.md"
  "custom-extension-guide.md"
  "dag-flow-guide.md"
  "dynamic-ui-guide.md"
  "http-server-guide.md"
  "llm-provider-guide.md"
  "openapi.json"
  "operations.md"
  "run-event-reference.md"
  "security.md"
  "tool-backend-guide.md"
)

require_path "generators/generator/qcg.toml"
require_path "frontend/generator/package.json"
for document in "${public_docs[@]}"; do
  require_path "docs/$document"
done
require_path "providers.toml"
require_path "README.md"
require_path "THIRD-PARTY-NOTICES"

if [ "$dry_run" = "true" ]; then
  echo "bundle dry-run ok"
  exit 0
fi

npm --prefix frontend/generator ci
npm --prefix frontend/generator run generate:api
npm --prefix frontend/generator run generate:wasm
npm --prefix frontend/generator run build
require_path "generators/generator/ui/index.html"

build_root="$(pwd -P)"
remap_flags="--remap-path-prefix=${build_root}=."
if [ -n "${HOME:-}" ]; then
  remap_flags="${remap_flags} --remap-path-prefix=${HOME}=/qcg-build"
fi
RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }${remap_flags}" \
  cargo build -p qcg --locked --profile "$profile"

case "$(uname -s)" in
  Darwin) os="macos" ;;
  Linux) os="linux" ;;
  MINGW*|MSYS*|CYGWIN*) os="windows" ;;
  *) os="$(uname -s | tr '[:upper:]' '[:lower:]')" ;;
esac
arch="$(uname -m)"
name="qcg-${os}-${arch}"
staging="${out_dir}/${name}"
bin_name="qcg"
if [ "$os" = "windows" ]; then
  bin_name="qcg.exe"
fi

rm -rf "$staging"
mkdir -p "$staging/bin" "$staging/share/qcg"
cp "target/${target_profile}/${bin_name}" "$staging/bin/"
cp README.md "$staging/share/qcg/"
cp providers.toml "$staging/share/qcg/providers.toml"
cp THIRD-PARTY-NOTICES "$staging/share/qcg/"
node scripts/generate-sbom.mjs "$staging/share/qcg/SBOM.spdx.json"
mkdir -p "$staging/share/qcg/docs"
for document in "${public_docs[@]}"; do
  cp "docs/$document" "$staging/share/qcg/docs/"
done
mkdir -p "$staging/share/qcg/generators"
cp -R generators/generator "$staging/share/qcg/generators/generator"

mkdir -p "$out_dir"
if [ "$os" = "windows" ]; then
  archive="${out_dir}/${name}.zip"
  rm -f "$archive"
  (cd "$out_dir" && 7z a -tzip "${name}.zip" "$name")
else
  archive="${out_dir}/${name}.tar.gz"
  if [ "$os" = "macos" ]; then
    COPYFILE_DISABLE=1 tar \
      --no-xattrs \
      --uid 0 \
      --gid 0 \
      --uname root \
      --gname root \
      -C "$out_dir" \
      -czf "$archive" \
      "$name"
  else
    tar \
      --owner=0 \
      --group=0 \
      --numeric-owner \
      -C "$out_dir" \
      -czf "$archive" \
      "$name"
  fi
fi

hash_tool=""
if command -v sha256sum >/dev/null 2>&1; then
  hash_tool="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  hash_tool="shasum -a 256"
else
  echo "sha256sum or shasum is required" >&2
  exit 1
fi
cp "$staging/share/qcg/SBOM.spdx.json" "$out_dir/${name}.sbom.spdx.json"
rm -rf "$staging"
hash_value="$($hash_tool "$archive" | awk '{print $1}')"
checksum_file="$out_dir/SHA256SUMS-${os}-${arch}"
printf '%s  %s\n' "$hash_value" "$(basename "$archive")" > "$checksum_file"
echo "$archive"
