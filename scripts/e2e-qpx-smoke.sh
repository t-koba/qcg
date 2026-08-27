#!/usr/bin/env bash
set -euo pipefail

QCG_BIN="${QCG_BIN:-target/debug/qcg}"
QPXD_BIN="${QPXD_BIN:-qpxd}"
QCG_PORT="${QCG_PORT:-18080}"
QPX_PORT="${QPX_PORT:-18081}"

if [[ ! -x "$QCG_BIN" ]]; then
  echo "qcg binary is not executable: $QCG_BIN" >&2
  exit 1
fi
if ! command -v "$QPXD_BIN" >/dev/null 2>&1 && [[ ! -x "$QPXD_BIN" ]]; then
  echo "qpxd binary is not available: $QPXD_BIN" >&2
  exit 1
fi

tmp="$(mktemp -d)"
qcg_pid=""
qpx_pid=""
cleanup() {
  [[ -z "$qpx_pid" ]] || kill "$qpx_pid" 2>/dev/null || true
  [[ -z "$qcg_pid" ]] || kill "$qcg_pid" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

cat > "$tmp/qpx.yaml" <<EOF
state_dir: "$tmp/qpx-state"
edges:
- kind: reverse
  name: qcg-smoke
  listen: 127.0.0.1:$QPX_PORT
  routes:
  - streaming_requirement: preferred
    match:
      host: ["*"]
    timeout_ms: 600000
    target:
      type: upstream
      upstreams: ["http://127.0.0.1:$QCG_PORT"]
      lb: round_robin
EOF

"$QPXD_BIN" check --config "$tmp/qpx.yaml"
# Merge fixtures/generators with the user root into one directory; the
# user root wins on duplicate ids like the multi-root service lookup.
generators_dir="$tmp/generators"
mkdir -p "$generators_dir"
for source in fixtures/generators generators; do
  if [ -d "$source" ]; then
    for entry in "$source"/*; do
      [ -e "$entry" ] || continue
      ln -sfn "$(pwd)/$entry" "$generators_dir/$(basename "$entry")"
    done
  fi
done

"$QCG_BIN" serve --bind 127.0.0.1 --port "$QCG_PORT" --generators-dir "$generators_dir" \
  --runs-dir "$tmp/runs" >"$tmp/qcg.log" 2>&1 &
qcg_pid=$!
for _ in {1..100}; do
  curl -fsS "http://127.0.0.1:$QCG_PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
curl -fsS "http://127.0.0.1:$QCG_PORT/healthz" >/dev/null

"$QPXD_BIN" run --config "$tmp/qpx.yaml" >"$tmp/qpx.log" 2>&1 &
qpx_pid=$!
for _ in {1..100}; do
  curl -fsS "http://127.0.0.1:$QPX_PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
curl -fsS "http://127.0.0.1:$QPX_PORT/api/openapi.json" | grep -q '"openapi"'

response="$(curl -fsS -X POST "http://127.0.0.1:$QPX_PORT/api/runs" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: qpx-smoke' \
  --data '{"generator_id":"hello-template","inputs":{"name":"qpx"}}')"
run_id="$(printf '%s' "$response" | sed -n 's/.*"run_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
if [[ -z "$run_id" ]]; then
  echo "run response did not contain run_id: $response" >&2
  exit 1
fi

set +e
curl -fsSN --max-time 2 "http://127.0.0.1:$QPX_PORT/api/runs/$run_id/events" >"$tmp/events.sse" 2>/dev/null
curl_status=$?
set -e
if [[ "$curl_status" -ne 0 && "$curl_status" -ne 28 ]]; then
  echo "SSE request failed with curl status $curl_status" >&2
  exit 1
fi
grep -q 'data:' "$tmp/events.sse"
echo "qpx smoke passed for run $run_id"
