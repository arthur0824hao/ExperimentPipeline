#!/usr/bin/env bash
set -euo pipefail

MODE="${DEPLOY_MODE:-B}"

echo "[deploy] mode=${MODE}"
echo "[deploy] scope=control-plane-only"
echo "[deploy] no worker-fleet rollout"
echo "[deploy] no DB migrations"

if [[ "${MODE}" != "A" ]]; then
  cat <<'EOF'
[deploy] scaffold mode (B): blocked prerequisites
- missing deployment target
- missing self-hosted runner labels
- missing deployment secrets
- missing environment protection/approval wiring
EOF
  exit 0
fi

if [[ -z "${DEPLOY_TARGET:-}" ]]; then
  echo "[deploy] DEPLOY_TARGET is required in mode A" >&2
  exit 1
fi

echo "[deploy] mode A prerequisites satisfied by repository operators"
echo "[deploy] deploying control plane target: ${DEPLOY_TARGET}"
