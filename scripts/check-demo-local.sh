#!/usr/bin/env bash
set -euo pipefail

run_step() {
  echo "==> $*"
  "$@"
}

run_step npm --prefix frontend/generator ci
run_step npm --prefix frontend/generator run generate:api
run_step npm --prefix frontend/generator run generate:wasm
run_step npm --prefix frontend/generator run check:api
run_step npm --prefix frontend/generator run check
run_step npm --prefix frontend/generator test
run_step npm --prefix frontend/generator run build
run_step node scripts/e2e-ui-playwright.mjs
run_step bash scripts/check-dist-bundle.sh

echo "local demo CI audit ok"
