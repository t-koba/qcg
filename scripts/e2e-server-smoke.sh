#!/usr/bin/env bash
set -euo pipefail

tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/qcg-server-smoke.XXXXXX")"
runs_dir="$tmp_root/runs"
generators_dir="$tmp_root/generators"
port="${QCG_SMOKE_PORT:-58017}"
server_pid=""
mkdir -p "$runs_dir"
# ShellCheck cannot infer that this function is invoked by the EXIT trap.
# shellcheck disable=SC2329
cleanup() {
  if [ -n "$server_pid" ]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_root"
}
trap cleanup EXIT

# Merge fixtures/generators with the user root into one directory; the
# user root wins on duplicate ids like the multi-root service lookup.
merge_generators() {
  local target="$1" source entry
  mkdir -p "$target"
  for source in fixtures/generators generators; do
    [ -d "$source" ] || continue
    for entry in "$source"/*; do
      [ -e "$entry" ] || continue
      ln -sfn "$(pwd)/$entry" "$target/$(basename "$entry")"
    done
  done
}
merge_generators "$generators_dir"

cargo run -p qcg -- serve \
  --bind 127.0.0.1 \
  --port "$port" \
  --generators-dir "$generators_dir" \
  --runs-dir "$runs_dir" \
  >"$tmp_root/server.log" 2>&1 &
server_pid="$!"

for _ in $(seq 1 80); do
  if curl -fs "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

if ! curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null; then
  echo "server did not become ready" >&2
  cat "$tmp_root/server.log" >&2
  exit 1
fi
curl -fsS "http://127.0.0.1:$port/api/openapi.json" | grep -q '"openapi"'
curl -fsS "http://127.0.0.1:$port/api/generators" | grep -q 'hello-template'
curl -fsS "http://127.0.0.1:$port/api/generators/assets-demo/assets/index.html" | grep -q '<script'
curl -fsS "http://127.0.0.1:$port/api/generators/assets-demo/assets/ui/app.js" | grep -q 'Assets loaded'

run_id="$(
  curl -fsS \
    -H 'content-type: application/json' \
    -d '{"generator_id":"hello-template","inputs":{"name":"server"}}' \
    "http://127.0.0.1:$port/api/runs" \
    | sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p'
)"

if [ -z "$run_id" ]; then
  echo "failed to parse run_id" >&2
  cat "$tmp_root/server.log" >&2
  exit 1
fi

for _ in $(seq 1 80); do
  snapshot="$(curl -fsS "http://127.0.0.1:$port/api/runs/$run_id")"
  if printf '%s' "$snapshot" | grep -q '"state":"succeeded"'; then
    echo "server smoke ok"
    exit 0
  fi
  sleep 0.25
done

echo "run did not finish successfully" >&2
printf '%s\n' "$snapshot" >&2
cat "$tmp_root/server.log" >&2
exit 1
