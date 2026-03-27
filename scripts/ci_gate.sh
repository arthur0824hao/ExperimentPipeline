#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT_DIR"

changed_files() {
  if [[ -n "${GITHUB_BASE_SHA:-}" && -n "${GITHUB_SHA:-}" ]]; then
    git diff --name-only "$GITHUB_BASE_SHA" "$GITHUB_SHA"
    return
  fi

  if [[ -n "${GITHUB_BASE_REF:-}" ]]; then
    local base_ref="origin/${GITHUB_BASE_REF}"
    if git rev-parse --verify "$base_ref" >/dev/null 2>&1; then
      local merge_base
      merge_base="$(git merge-base "$base_ref" HEAD)"
      git diff --name-only "$merge_base" HEAD
      return
    fi
  fi

  if git rev-parse --verify HEAD~1 >/dev/null 2>&1; then
    git diff --name-only HEAD~1 HEAD
    return
  fi
}

is_full_path_change() {
  local path
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    case "$path" in
      pipeline/*|configs/*|.github/*|scripts/ci_gate.sh)
        return 0
        ;;
    esac
  done
  return 1
}

is_docs_only_change() {
  local path
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    case "$path" in
      docs/*|pipeline/docs/*|note/*|*.md|*.txt)
        ;;
      *)
        return 1
        ;;
    esac
  done
  return 0
}

workflow_sanity() {
  python3 - <<'PY'
from pathlib import Path

required = {
    Path('.github/workflows/ci.yml'): ['jobs:', 'ep-gate:'],
    Path('.github/workflows/deploy.yml'): ['jobs:', 'control-plane-deploy:'],
}

for path, tokens in required.items():
    text = path.read_text(encoding='utf-8')
    for token in tokens:
        if token not in text:
            raise SystemExit(f"workflow sanity failed: {path} missing '{token}'")
PY
}

run_common_checks() {
  bash -n scripts/ci_gate.sh
  if [[ -f scripts/deploy_control_plane.sh ]]; then
    bash -n scripts/deploy_control_plane.sh
  fi
  workflow_sanity
}

run_full_checks() {
  echo "[ep-gate] full path: running test + import + syntax checks"
  run_common_checks
  export PGCONNECT_TIMEOUT=1
  PYTHONPATH="${ROOT_DIR}/pipeline:${PYTHONPATH:-}" python3 -m pytest pipeline/tests/ -q --tb=line -x --timeout=30
  PYTHONPATH="${ROOT_DIR}/pipeline:${PYTHONPATH:-}" python3 - <<'PY'
import allocator
import artifact
import cluster
import compare
import control_plane
import ep_cli
import health
import run_manifest
import terminal_state
import worker

print('import smoke OK')
PY
  python3 -m py_compile pipeline/*.py
}

run_docs_checks() {
  echo "[ep-gate] docs-only path: running syntax/sanity checks"
  run_common_checks
}

CHANGED="$(changed_files || true)"

if [[ -z "$CHANGED" ]]; then
  run_full_checks
  exit 0
fi

if is_full_path_change <<<"$CHANGED"; then
  run_full_checks
  exit 0
fi

if is_docs_only_change <<<"$CHANGED"; then
  run_docs_checks
  exit 0
fi

run_full_checks
