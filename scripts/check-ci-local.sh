#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
verification_venv="$(mktemp -d "${TMPDIR:-/tmp}/qcg-verification-python.XXXXXX")"
trap 'rm -rf "$verification_venv"' EXIT
python3 -m venv --without-pip "$verification_venv"
python3 -m pip --python "$verification_venv/bin/python3" install -r scripts/requirements-spdx.txt
export PATH="$verification_venv/bin:$PATH"
npm --prefix frontend/generator ci
npm --prefix frontend/generator audit --audit-level=moderate
bash scripts/check-core.sh
bash scripts/check-demo-local.sh
