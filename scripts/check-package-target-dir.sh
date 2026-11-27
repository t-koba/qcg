#!/usr/bin/env bash
# Regression: package-dist.sh must copy the binary from the directory Cargo
# actually built into, not from a path reconstructed from one env variable.
# Cargo honors both CARGO_TARGET_DIR and the higher-precedence
# CARGO_BUILD_TARGET_DIR. Only `cargo build` is stubbed; metadata, SBOM and
# archive steps stay real, so packaging must follow Cargo's resolved directory.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.."

tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/qcg-package-target-dir.XXXXXX")"
trap 'rm -rf "$tmp_root"' EXIT

real_cargo="$(command -v cargo)"
product_id="$(node scripts/product.mjs name)"
source_binary="$(node scripts/product.mjs binary)"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    bin_name="${product_id}.exe"
    source_binary="${source_binary}.exe"
    ;;
  *)
    bin_name="$product_id"
    ;;
esac

stub_dir="$tmp_root/stub"
mkdir -p "$stub_dir"

# The stub writes a distinct fixture only where the build is told to place it.
cat > "$stub_dir/cargo" <<EOF
#!/usr/bin/env bash
if [ "\${1:-}" = "build" ]; then
  printf '%s\\n' "\$QCG_TEST_FIXTURE" > "\$QCG_TEST_TARGET_DIR/debug/$source_binary"
  exit 0
fi
exec "$real_cargo" "\$@"
EOF
chmod +x "$stub_dir/cargo"

# check_case <label> <resolved target dir> <cargo env assignment...>
check_case() {
  local label="$1" target_dir="$2"
  shift 2
  local case_out="$tmp_root/dist-$label"
  local case_extract="$tmp_root/extract-$label"
  local fixture="qcg-target-dir-fixture-${label}-${tmp_root##*/}"
  mkdir -p "$target_dir/debug" "$case_out" "$case_extract"

  local archive
  archive="$(
    env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR \
      "$@" \
      QCG_TEST_TARGET_DIR="$target_dir" \
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
  if ! cmp -s "$packaged_binary" "$target_dir/debug/$source_binary"; then
    echo "packaged binary differs from the binary Cargo built ($label)" >&2
    exit 1
  fi
}

# Trailing slash is valid for CARGO_TARGET_DIR; packaging must still honor it.
check_case cargo-target-dir "$tmp_root/cargo-target/" \
  "CARGO_TARGET_DIR=$tmp_root/cargo-target/"

# With CARGO_TARGET_DIR unset, Cargo uses CARGO_BUILD_TARGET_DIR; packaging
# must follow Cargo's resolved directory rather than assume the default target/.
check_case cargo-build-target-dir "$tmp_root/cargo-build-target" \
  "CARGO_BUILD_TARGET_DIR=$tmp_root/cargo-build-target"

echo "package-dist effective target dir check ok"
