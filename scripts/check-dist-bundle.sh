#!/usr/bin/env bash
set -euo pipefail

tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/qcg-dist-smoke.XXXXXX")"
server_pid=""
archive=""
cleanup() {
  if [ -n "$server_pid" ]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$tmp_root"
}
trap cleanup EXIT

while [ "$#" -gt 0 ]; do
  case "$1" in
    --archive)
      shift
      archive="${1:?missing --archive value}"
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

if [ -z "$archive" ]; then
  archive="$(bash scripts/package-dist.sh --debug --out-dir "$tmp_root/dist" | tail -n 1)"
elif [ ! -f "$archive" ]; then
  echo "archive not found: $archive" >&2
  exit 1
fi

case "$archive" in
  *.tar.gz)
    tar -C "$tmp_root" -xzf "$archive"
    ;;
  *.zip)
    unzip -q "$archive" -d "$tmp_root"
    ;;
  *)
    echo "unsupported archive: $archive" >&2
    exit 1
    ;;
esac

bundle_dir="$(find "$tmp_root" -mindepth 1 -maxdepth 1 -type d -name 'qcg-*' | head -n 1)"
if [ -z "$bundle_dir" ]; then
  echo "bundle directory was not extracted" >&2
  exit 1
fi

bin="$bundle_dir/bin/qcg"
if [ -f "$bundle_dir/bin/qcg.exe" ]; then
  bin="$bundle_dir/bin/qcg.exe"
fi

"$bin" validate "$bundle_dir/share/qcg/generators/generator" >/dev/null
test -f "$bundle_dir/share/qcg/generators/generator/ui/index.html"
test -f "$bundle_dir/share/qcg/docs/contract-reference.md"
test -f "$bundle_dir/share/qcg/docs/run-event-reference.md"
test -f "$bundle_dir/share/qcg/docs/operations.md"
test -f "$bundle_dir/share/qcg/docs/dynamic-ui-guide.md"
test ! -e "$bundle_dir/share/qcg/docs/internal"
test -f "$bundle_dir/share/qcg/THIRD-PARTY-NOTICES"
test -f "$bundle_dir/share/qcg/SBOM.spdx.json"
test ! -e "$bundle_dir/share/qcg/web"
archive_dir="$(dirname "$archive")"
checksum_count="$(find "$archive_dir" -maxdepth 1 -type f -name 'SHA256SUMS-*' | wc -l | tr -d ' ')"
test "$checksum_count" = "1"
if find "$archive_dir" -mindepth 1 -maxdepth 1 -type d -name 'qcg-*' | grep -q .; then
  echo "distribution staging directory was not removed" >&2
  exit 1
fi

mkdir -p "$tmp_root/sample-run"
(
  cd "$tmp_root/sample-run"
  "$bin" run "$bundle_dir/share/qcg/generators/generator" \
    --answer 'ask_purpose={"description":"Bundle smoke generated package"}' \
    --answer 'ask_design_mode=manual' \
    --answer 'ask_manual_form={"package":{"manifest":{"generator":{"id":"sample-gen","name":"Sample Gen","version":"0.1.0","qcg_version":"^0.1","description":"Generate a sample artifact","authors":[]},"inputs":{"stages":[{"id":"main","fields":[{"id":"request","type":"natural_language","required":true}]}]},"flow":[{"id":"emit","type":"render","artifact":{"label":"Sample artifact","preview":"text","required":true},"params":{"template":"templates/artifact.txt.j2","output_file":"README.md"}}]},"sources":{"templates/artifact.txt.j2":{"encoding":"utf8","content":"# Sample"}}}}' \
    --answer 'ask_authority={"permissions":{"fs_read":[],"fs_write":["workspace"],"network":[],"commands":[],"containers":{"enabled":false,"images":[],"on_missing":"error"},"side_effects":"none"},"secrets":{}}' \
    --output "$tmp_root/sample-run" \
    --yes >/dev/null
)
"$bin" validate "$tmp_root/sample-run/generator" >/dev/null

mkdir -p "$tmp_root/server-run"
(
  cd "$tmp_root/server-run"
  "$bin" serve --port 0
) >"$tmp_root/server.log" 2>&1 &
server_pid="$!"
server_url=""
for _ in $(seq 1 100); do
  server_url="$(sed -n 's/^qcg server listening on //p' "$tmp_root/server.log" | tail -n 1)"
  if [ -n "$server_url" ]; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$tmp_root/server.log" >&2
    echo "bundled server exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.1
done
if [ -z "$server_url" ]; then
  cat "$tmp_root/server.log" >&2
  echo "timed out waiting for bundled server" >&2
  exit 1
fi
curl -fsS "$server_url/healthz" | grep -q '"ok":true'
curl -fsS "$server_url/api/openapi.json" | grep -q '"openapi"'
curl -fsS "$server_url/api/generators/generator/assets/ui/index.html" | grep -q '<script'
kill "$server_pid" 2>/dev/null || true
wait "$server_pid" 2>/dev/null || true
server_pid=""
echo "dist bundle smoke ok"
