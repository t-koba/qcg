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
  record_count="${QCG_TEST_RECORD_COUNT:-1}"
  i=0
  while [ "$i" -lt "$record_count" ]; do
    printf '{"reason":"compiler-artifact","target":{"kind":["bin"],"name":"qcg"},"profile":{"test":false},"executable":%s}\n' "$artifact_json"
    i=$((i + 1))
  done
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

# check_fail_case <label> <record count> <cargo env assignment...>
# A zero- or two-record build must fail closed with the exact guard message and
# leave no archive; otherwise a future weakening of the guard goes unnoticed.
check_fail_case() {
  local label="$1" record_count="$2"
  shift 2
  local case_out="$tmp_root/dist-$label"
  local artifact_path="$tmp_root/fail-artifact-$label/debug/$source_binary"
  local fixture="qcg-target-dir-fixture-${label}-${tmp_root##*/}"
  local log="$tmp_root/log-$label"
  mkdir -p "$(dirname "$artifact_path")" "$case_out"

  if env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR -u CARGO_BUILD_TARGET \
      "$@" \
      QCG_TEST_REAL_CARGO="$real_cargo" \
      QCG_TEST_ARTIFACT_PATH="$artifact_path" \
      QCG_TEST_FIXTURE="$fixture" \
      QCG_TEST_RECORD_COUNT="$record_count" \
      PATH="$stub_dir:$PATH" \
      bash scripts/package-dist.sh --frontend-built --debug --out-dir "$case_out" \
        >"$log" 2>&1; then
    echo "packaging unexpectedly succeeded with $record_count matching records ($label)" >&2
    exit 1
  fi
  if ! grep -q "expected exactly one compiler-artifact record for qcg, found $record_count" "$log"; then
    echo "packaging failed for an unexpected reason ($label)" >&2
    cat "$log" >&2
    exit 1
  fi
  if find "$case_out" -type f \( -name '*.tar.gz' -o -name '*.zip' \) | grep -q .; then
    echo "packaging left an archive with $record_count matching records ($label)" >&2
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

# A foreign Cargo target would be archived under an uname-derived host label.
# Packaging must refuse it before staging or archiving anything.
foreign_target="wasm32-unknown-unknown"
check_foreign_case() {
  local label="$1" artifact_path="$2"
  shift 2
  local case_out="$tmp_root/dist-$label"
  local log="$tmp_root/log-$label"
  local fixture="qcg-target-dir-fixture-${label}-${tmp_root##*/}"
  mkdir -p "$(dirname "$artifact_path")" "$case_out"
  if env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR -u CARGO_BUILD_TARGET \
      "$@" \
      QCG_TEST_REAL_CARGO="$real_cargo" \
      QCG_TEST_ARTIFACT_PATH="$artifact_path" \
      QCG_TEST_FIXTURE="$fixture" \
      PATH="$stub_dir:$PATH" \
      bash scripts/package-dist.sh --frontend-built --debug --out-dir "$case_out" \
        >"$log" 2>&1; then
    echo "packaging unexpectedly succeeded with a foreign Cargo target ($label)" >&2
    exit 1
  fi
  if ! grep -qF "refusing to package foreign Cargo target: $foreign_target (host target: $host_triple)" "$log"; then
    echo "packaging failed for an unexpected reason ($label)" >&2
    cat "$log" >&2
    exit 1
  fi
  if find "$case_out" -type f \( -name '*.tar.gz' -o -name '*.zip' \) | grep -q .; then
    echo "packaging left an archive for a foreign Cargo target ($label)" >&2
    exit 1
  fi
}

# Mirrors `[build] target` in config: the executable carries a foreign triple
# segment while CARGO_BUILD_TARGET stays unset.
foreign_config_root="$tmp_root/foreign-config-target"
check_foreign_case cargo-config-target-foreign "$foreign_config_root/$foreign_target/debug/$source_binary" \
  "CARGO_TARGET_DIR=$foreign_config_root"

# Explicit CARGO_BUILD_TARGET selection with a matching foreign path.
foreign_env_root="$tmp_root/foreign-env-target"
check_foreign_case cargo-build-target-foreign "$foreign_env_root/$foreign_target/debug/$source_binary" \
  "CARGO_BUILD_TARGET=$foreign_target" \
  "CARGO_TARGET_DIR=$foreign_env_root"

# A symlinked foreign cache resolves physically into the host triple directory.
# A guard that canonicalizes paths would accept it and archive a mislabeled
# foreign binary, so the logical Cargo path must still be refused.
link_root="$tmp_root/symlink-foreign-target"
mkdir -p "$link_root/$host_triple/debug"
ln -s "$host_triple" "$link_root/$foreign_target"
check_foreign_case cargo-target-dir-symlink-foreign "$link_root/$foreign_target/debug/$source_binary" \
  "CARGO_TARGET_DIR=$link_root"

# Missing output (zero records) must fail closed.
check_fail_case no-artifact-record 0 \
  "CARGO_TARGET_DIR=$tmp_root/fail-target-none"

# Ambiguous output (two records) must fail closed.
check_fail_case ambiguous-artifact-record 2 \
  "CARGO_TARGET_DIR=$tmp_root/fail-target-two"

echo "package-dist effective target dir check ok"
echo "package-dist foreign Cargo target check ok"
