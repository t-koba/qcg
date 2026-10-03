#!/usr/bin/env bash
set -euo pipefail

run_step() {
  echo "==> $*"
  "$@"
}

case "${1:-}" in
  "") run_step npm --prefix frontend/generator ci ;;
  --dependencies-installed) ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac
if [[ "$#" -gt 1 ]]; then
  echo "unexpected extra arguments" >&2
  exit 2
fi
run_step npm --prefix frontend/generator run generate:api
run_step npm --prefix frontend/generator run generate:wasm
run_step npm --prefix frontend/generator run check:api
run_step npm --prefix frontend/generator run check
run_step npm --prefix frontend/generator test
run_step node scripts/e2e-ui-playwright.mjs
run_step bash scripts/check-dist-bundle.sh --frontend-built

echo "local demo CI audit ok"
