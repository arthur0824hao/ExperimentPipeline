#!/usr/bin/env bash
set -euo pipefail
EXP_NAME="${1:?Usage: validate_exp.sh <exp_name> [phase_root]}"
PHASE_ROOT="${2:-/datas/store162/arthur0824hao/Study/GNN/FraudDetect/SubProject/Phase3}"
cd "$PHASE_ROOT"
python3 -c "
import json, sys
sys.path.insert(0, '.')
from preprocess_lib.gate_engine import load_rules, run_gate_rules
from pathlib import Path

rules = load_rules(Path('gate_bank.json'))
exp = {'name': '$EXP_NAME'}
report = run_gate_rules(exp, Path('.'), rules)

print('# Gate Validation Report: $EXP_NAME')
print()
if report.has_errors:
    print('**Verdict: FAIL**')
elif report.has_warnings:
    print('**Verdict: PASS (with warnings)**')
else:
    print('**Verdict: PASS**')
print()
print(report.summary())
print()
result = {'passed': not report.has_errors, 'errors': len(report.errors), 'warnings': len(report.warnings)}
print(json.dumps(result))
"
