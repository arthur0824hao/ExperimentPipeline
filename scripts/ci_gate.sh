#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "[ci_gate] Repo: ${REPO_ROOT}"

if ! python3 - <<'PY'
import importlib.util
import sys
missing = []
for mod in ("pytest", "yaml"):
    if importlib.util.find_spec(mod) is None:
        missing.append(mod)
if missing:
    print("[ci_gate] missing python modules:", ", ".join(missing))
    sys.exit(1)
PY
then
  echo "[ci_gate] install requirements before running gate"
  exit 1
fi

declare -a modified_py=()
while IFS= read -r line; do
  [[ -n "${line}" ]] && modified_py+=("${line}")
done < <(
  {
    git diff --name-only --diff-filter=ACMRTUXB -- '*.py'
    git diff --cached --name-only --diff-filter=ACMRTUXB -- '*.py'
  } | sort -u
)

if [[ ${#modified_py[@]} -gt 0 ]]; then
  echo "[ci_gate] py_compile on modified Python files"
  python3 -m py_compile "${modified_py[@]}"
else
  echo "[ci_gate] No modified Python files detected; skipping py_compile"
fi

tkt_changed=0
while IFS= read -r path; do
  [[ -z "${path}" ]] && continue
  case "${path}" in
    .tkt/*)
      tkt_changed=1
      break
      ;;
  esac
done < <(
  {
    git diff --name-only --diff-filter=ACMRTUXB -- '.tkt/**'
    git diff --cached --name-only --diff-filter=ACMRTUXB -- '.tkt/**'
  } | sort -u
)

echo "[ci_gate] pytest key handler regression"
python3 -m pytest -q pipeline/tests/test_key_handler.py

echo "[ci_gate] pytest runtime config regression"
python3 -m pytest -q pipeline/tests/test_runtime_config_resolution.py

echo "[ci_gate] JSON lint configs/*.json"
shopt -s nullglob
for json_file in configs/*.json; do
  python3 -m json.tool "${json_file}" > /dev/null
done
shopt -u nullglob

if [[ ${tkt_changed} -eq 1 ]]; then
  echo "[ci_gate] YAML lint + sanitizer check for changed .tkt scope"
  python3 pipeline/tools/tkt_yaml_sanitize.py --target "${REPO_ROOT}"
else
  echo "[ci_gate] No .tkt changes detected; skipping repo-wide .tkt sanitizer"
fi

echo "[ci_gate] PASS"
