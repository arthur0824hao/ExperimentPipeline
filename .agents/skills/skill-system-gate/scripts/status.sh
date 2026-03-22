#!/usr/bin/env bash
set -euo pipefail
PHASE_ROOT="${1:-/datas/store162/arthur0824hao/Study/GNN/FraudDetect/SubProject/Phase3}"
cd "$PHASE_ROOT"
python3 -c "
import json, sys
sys.path.insert(0, '.')
from db_registry import DBExperimentsDB

db = DBExperimentsDB()
data = db.load()
experiments = data.get('experiments', [])
completed = data.get('completed', [])

running = [e for e in experiments if str(e.get('status','')).upper() == 'RUNNING']
needs_rerun = [e for e in experiments if str(e.get('status','')).upper() == 'NEEDS_RERUN']
errored = [e for e in needs_rerun if e.get('error_info')]

print('# Experiment Registry Status')
print()
print(f'| State | Count |')
print(f'|---|---|')
print(f'| RUNNING | {len(running)} |')
print(f'| NEEDS_RERUN | {len(needs_rerun)} |')
print(f'| COMPLETED | {len(completed)} |')
print(f'| With errors | {len(errored)} |')
print(f'| **Total** | **{len(experiments) + len(completed)}** |')
print()

if running:
    print('## Running')
    for e in running:
        ro = e.get('running_on', {}) or {}
        print(f'- {e[\"name\"]} on {ro.get(\"worker\",\"?\")} (GPU {ro.get(\"gpu\",\"?\")})')
    print()

print(json.dumps({'running': len(running), 'needs_rerun': len(needs_rerun), 'completed': len(completed), 'total': len(experiments) + len(completed)}))"
