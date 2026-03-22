#!/usr/bin/env bash
set -euo pipefail
EXP_NAME="${1:?Usage: check_registry.sh <exp_name>}"
PHASE_ROOT="${2:-/datas/store162/arthur0824hao/Study/GNN/FraudDetect/SubProject/Phase3}"
cd "$PHASE_ROOT"
python3 -c "
import json, sys
sys.path.insert(0, '.')
from db_registry import DBExperimentsDB

db = DBExperimentsDB()
exp = db.get_experiment('$EXP_NAME')

print('# Registry Safety Report: $EXP_NAME')
print()
if exp is None:
    print('**Status: NOT FOUND** — experiment not in registry')
    print(json.dumps({'found': False, 'safe_to_reset': False}))
    sys.exit(0)

status = str(exp.get('status', 'UNKNOWN')).upper()
print(f'**Status: {status}**')
print()

if status == 'RUNNING':
    ro = exp.get('running_on', {}) or {}
    print(f'- Worker: {ro.get(\"worker\", \"unknown\")}')
    print(f'- GPU: {ro.get(\"gpu\", \"unknown\")}')
    print(f'- PID: {ro.get(\"pid\", \"unknown\")}')
    print(f'- Started: {ro.get(\"started_at\", \"unknown\")}')
    print()
    print('**Recommendation: DO NOT RESET** — experiment is actively running')
    print(json.dumps({'found': True, 'status': status, 'safe_to_reset': False}))
elif status == 'COMPLETED':
    r = exp.get('result', {}) or {}
    print(f'- F1: {r.get(\"f1_score\", \"N/A\")}')
    print(f'- AUC: {r.get(\"auc_score\", \"N/A\")}')
    print(f'- Completed: {exp.get(\"completed_at\", \"unknown\")}')
    print()
    print('**Recommendation: Safe to rerun if needed**')
    print(json.dumps({'found': True, 'status': status, 'safe_to_reset': True}))
else:
    ei = exp.get('error_info', {}) or {}
    if ei:
        print(f'- Error: {ei.get(\"type\", \"unknown\")}')
        print(f'- Message: {ei.get(\"message\", \"\")}')
    print(f'- Retry count: {exp.get(\"retry_count\", 0)}')
    print()
    print('**Recommendation: Safe to reset/rerun**')
    print(json.dumps({'found': True, 'status': status, 'safe_to_reset': True}))
"
