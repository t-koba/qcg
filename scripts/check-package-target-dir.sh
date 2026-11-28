#!/usr/bin/env bash
# Regression: package-dist.sh must archive the binary Cargo actually built,
# selected from Cargo's compiler-artifact record, rather than from a path
# reconstructed from one env variable. Cargo honors CARGO_TARGET_DIR, the
# higher-precedence CARGO_BUILD_TARGET_DIR, and CARGO_BUILD_TARGET (which adds
# the target triple). Only `cargo build` is stubbed; metadata, SBOM and archive
# steps stay real, so packaging must follow Cargo's reported executable.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.."

tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/qcg-package-target-dir.XXXXXX")"
trap 'rm -rf "$tmp_root"' EXIT

real_cargo="$(command -v cargo)"
product_id="$(node scripts/product.mjs name)"
binary_name="$(node scripts/product.mjs binary)"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    bin_name="${product_id}.exe"
    source_binary="${binary_name}.exe"
    ;;
  *)
    bin_name="$product_id"
    source_binary="$binary_name"
    ;;
esac

stub_dir="$tmp_root/stub"
mkdir -p "$stub_dir"

# The stub writes a distinct fixture only to the artifact path it reports.
cat > "$stub_dir/cargo" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "build" ]; then
  mkdir -p "$(dirname "$QCG_TEST_ARTIFACT_PATH")"
  printf '%s\n' "$QCG_TEST_FIXTURE" > "$QCG_TEST_ARTIFACT_PATH"
  artifact_json="$(node -e 'process.stdout.write(JSON.stringify(process.argv[1]))' "$QCG_TEST_ARTIFACT_PATH")"
  printf '{"reason":"compiler-artifact","target":{"kind":["bin"],"name":"qcg"},"profile":{"test":false},"executable":%s}\n' "$artifact_json"
  exit 0
fi
exec "$QCG_TEST_REAL_CARGO" "$@"
EOF
chmod +x "$stub_dir/cargo"

# Optional stale path set by a case; the archive must not contain that content.
unexpected_binary=""

# check_case <label> <artifact path> <cargo env assignment...>
check_case() {
  local label="$1" artifact_path="$2"
  shift 2
  local case_out="$tmp_root/dist-$label"
  local case_extract="$tmp_root/extract-$label"
  local fixture="qcg-target-dir-fixture-${label}-${tmp_root##*/}"
  mkdir -p "$(dirname "$artifact_path")" "$case_out" "$case_extract"

  local archive
  archive="$(
    env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR -u CARGO_BUILD_TARGET \
      "$@" \
      QCG_TEST_REAL_CARGO="$real_cargo" \
      QCG_TEST_ARTIFACT_PATH="$artifact_path" \
      QCG_TEST_FIXTURE="$fixture" \
      PATH="$stub_dir:$PATH" \
      bash scripts/package-dist.sh --frontend-built --debug --out-dir "$case_out" | tail -n 1
  )"

  case "$archive" in
    *.tar.gz) tar -C "$case_extract" -xzf "$archive" ;;
    *.zip) unzip -q "$archive" -d "$case_extract" ;;
    *) echo "unsupported archive: $archive" >&2; exit 1 ;;
  esac

  local packaged_binary
  packaged_binary="$(find "$case_extract" -type f -name "$bin_name" | head -n 1)"
  if [ -z "$packaged_binary" ]; then
    echo "packaged binary was not found ($label)" >&2
    exit 1
  fi
  if ! cmp -s "$packaged_binary" "$artifact_path"; then
    echo "packaged binary differs from the executable Cargo reported ($label)" >&2
    exit 1
  fi
  if [ -n "$unexpected_binary" ] && cmp -s "$packaged_binary" "$unexpected_binary"; then
    echo "packaged binary is the stale binary ($label)" >&2
    exit 1
  fi
}

# Trailing slash is valid for CARGO_TARGET_DIR; packaging must still honor it.
target_dir="$tmp_root/cargo-target/"
check_case cargo-target-dir "$target_dir/debug/$source_binary" \
  "CARGO_TARGET_DIR=$target_dir"

# With CARGO_TARGET_DIR unset, Cargo uses CARGO_BUILD_TARGET_DIR; packaging
# must follow Cargo's resolved directory rather than assume the default target/.
target_dir="$tmp_root/cargo-build-target"
check_case cargo-build-target-dir "$target_dir/debug/$source_binary" \
  "CARGO_BUILD_TARGET_DIR=$target_dir"

# A host-triple build puts the fresh binary under <target>/<triple>/debug/
# while an old binary can remain at <target>/debug/. Packaging must take the
# executable Cargo reported, never the stale one.
host_triple="$(rustc -vV | awk '/^host:/{print $2}')"
if [ -z "$host_triple" ]; then
  echo "could not determine Rust host triple" >&2
  exit 1
fi
target_dir="$tmp_root/cargo-build-target-host"
fresh_binary="$target_dir/$host_triple/debug/$source_binary"
stale_binary="$target_dir/debug/$source_binary"
mkdir -p "$(dirname "$stale_binary")"
printf '%s\n' "qcg-stale-binary-${tmp_root##*/}" > "$stale_binary"
unexpected_binary="$stale_binary"
check_case cargo-build-target-host "$fresh_binary" \
  "CARGO_BUILD_TARGET=$host_triple" \
  "CARGO_TARGET_DIR=$target_dir"
unexpected_binary=""

echo "package-dist effective target dir check ok"
