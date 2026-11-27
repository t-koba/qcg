#!/usr/bin/env bash
# Regression: package-dist.sh must copy the binary cargo built under CARGO_TARGET_DIR.
# Only `cargo build` is stubbed; metadata, SBOM and archive steps stay real.
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

target_dir="$tmp_root/cargo-target"
out_dir="$tmp_root/dist"
stub_dir="$tmp_root/stub"
fixture="qcg-target-dir-fixture-${tmp_root##*/}"
mkdir -p "$stub_dir" "$target_dir/debug" "$out_dir" "$tmp_root/extract"

cat > "$stub_dir/cargo" <<EOF
#!/usr/bin/env bash
if [ "\${1:-}" = "build" ]; then
  printf '%s\n' "$fixture" > "\$CARGO_TARGET_DIR/debug/$source_binary"
  exit 0
fi
exec "$real_cargo" "\$@"
EOF
chmod +x "$stub_dir/cargo"

# Trailing slash is part of the environment cargo accepts; packaging must too.
archive="$(
  CARGO_TARGET_DIR="$target_dir/" \
    PATH="$stub_dir:$PATH" \
    bash scripts/package-dist.sh --frontend-built --debug --out-dir "$out_dir" | tail -n 1
)"

case "$archive" in
  *.tar.gz)
    tar -C "$tmp_root/extract" -xzf "$archive"
    ;;
  *.zip)
    unzip -q "$archive" -d "$tmp_root/extract"
    ;;
  *)
    echo "unsupported archive: $archive" >&2
    exit 1
    ;;
esac

packaged_binary="$(find "$tmp_root/extract" -type f -name "$bin_name" | head -n 1)"
if [ -z "$packaged_binary" ]; then
  echo "packaged binary was not found" >&2
  exit 1
fi
if ! cmp -s "$packaged_binary" "$target_dir/debug/$source_binary"; then
  echo "packaged binary differs from the binary built in CARGO_TARGET_DIR" >&2
  exit 1
fi

echo "package-dist CARGO_TARGET_DIR check ok"
