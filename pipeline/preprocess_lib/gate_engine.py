#!/usr/bin/env python3
"""
Gate Engine — rule-based experiment validation.

Loads rules from ``gate_bank.json`` and evaluates them against an experiment
before it is registered into ``experiments.json``.

Rule types
----------
source_contains
    Check that the experiment's ``train.py`` contains a literal token.
source_not_contains
    Check that ``train.py`` does NOT contain a forbidden token.
stderr_scan
    Scan ``logs/<name>*.err`` for a regex pattern (catches runtime issues
    from previous runs).
file_exists
    Assert that a file exists relative to the experiment directory.
file_min_size
    Assert that a file meets a minimum byte size.

Severity levels
---------------
error   — blocks the gate (experiment will NOT be registered)
warning — logged but does not block the gate
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class RuleResult:
    rule_id: str
    severity: str
    passed: bool
    message: str = ""


@dataclass
class GateReport:
    experiment: str
    results: List[RuleResult] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(r.severity == "error" and not r.passed for r in self.results)

    @property
    def has_warnings(self) -> bool:
        return any(r.severity == "warning" and not r.passed for r in self.results)

    @property
    def errors(self) -> List[RuleResult]:
        return [r for r in self.results if r.severity == "error" and not r.passed]

    @property
    def warnings(self) -> List[RuleResult]:
        return [r for r in self.results if r.severity == "warning" and not r.passed]

    def summary(self) -> str:
        lines: List[str] = []
        for r in self.errors:
            lines.append(f"  [ERROR] {r.rule_id}: {r.message}")
        for r in self.warnings:
            lines.append(f"  [WARN]  {r.rule_id}: {r.message}")
        return "\n".join(lines) if lines else "  All rules passed."


def load_rules(gate_bank_path: Path) -> List[Dict[str, Any]]:
    if not gate_bank_path.exists():
        return []
    with open(gate_bank_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("rules", [])


def _read_train_source(
    exp_dir: Path, phase3_root: Path, exp: Dict[str, Any]
) -> Optional[str]:
    script = exp.get("script")
    if script:
        p = Path(script)
        train_path = p if p.is_absolute() else phase3_root / script
    else:
        train_path = exp_dir / "scripts" / "train.py"
    if not train_path.exists():
        return None
    return train_path.read_text(encoding="utf-8")


def _derive_stderr_prefixes(exp_name: str) -> List[str]:
    prefixes = [exp_name]
    parts = exp_name.split("_")
    for i in range(2, len(parts)):
        prefixes.append("_".join(parts[:i]))
    return prefixes


def _collect_stderr_files(exp_name: str, logs_dir: Path) -> List[Path]:
    if not logs_dir.is_dir():
        return []
    prefixes = _derive_stderr_prefixes(exp_name)
    results: List[Path] = []
    for p in logs_dir.iterdir():
        if p.suffix != ".err":
            continue
        stem = p.stem
        if any(stem.startswith(pfx) for pfx in prefixes):
            results.append(p)
    return sorted(results, key=lambda p: p.stat().st_mtime, reverse=True)


def _exec_source_contains(
    rule: Dict[str, Any], source: str, exp_dir: Path
) -> RuleResult:
    rid = rule["id"]
    pattern = rule["pattern"]
    sev = rule.get("severity", "error")
    skip_if = rule.get("skip_if_source_contains")
    if isinstance(skip_if, list):
        skip_tokens = [str(x) for x in skip_if]
    elif isinstance(skip_if, str):
        skip_tokens = [x.strip() for x in skip_if.split("||") if x.strip()]
    else:
        skip_tokens = []
    if any(tok in source for tok in skip_tokens):
        return RuleResult(rid, sev, True)
    if pattern in source:
        return RuleResult(rid, sev, True)
    return RuleResult(rid, sev, False, rule.get("description", f"Missing: {pattern}"))


def _exec_source_not_contains(
    rule: Dict[str, Any], source: str, exp_dir: Path
) -> RuleResult:
    rid = rule["id"]
    pattern = rule["pattern"]
    sev = rule.get("severity", "warning")
    if pattern not in source:
        return RuleResult(rid, sev, True)
    return RuleResult(
        rid, sev, False, rule.get("description", f"Forbidden pattern found: {pattern}")
    )


def _exec_stderr_scan(
    rule: Dict[str, Any], exp_name: str, logs_dir: Path
) -> RuleResult:
    rid = rule["id"]
    pattern = rule["pattern"]
    sev = rule.get("severity", "warning")
    max_files = rule.get("max_files", 5)

    err_files = _collect_stderr_files(exp_name, logs_dir)
    if not err_files:
        return RuleResult(rid, sev, True)

    regex = re.compile(pattern, re.IGNORECASE)
    hits: List[str] = []
    for ef in err_files[:max_files]:
        try:
            content = ef.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        matches = regex.findall(content)
        if matches:
            hits.append(f"{ef.name}: {len(matches)} match(es)")

    if not hits:
        return RuleResult(rid, sev, True)
    detail = "; ".join(hits)
    desc = rule.get("description", pattern)
    return RuleResult(rid, sev, False, f"{desc} — {detail}")


def _exec_file_exists(rule: Dict[str, Any], exp_dir: Path) -> RuleResult:
    rid = rule["id"]
    rel_path = rule["path"]
    sev = rule.get("severity", "error")
    target = exp_dir / rel_path
    if target.exists():
        return RuleResult(rid, sev, True)
    return RuleResult(rid, sev, False, rule.get("description", f"Missing: {rel_path}"))


def _exec_file_min_size(rule: Dict[str, Any], exp_dir: Path) -> RuleResult:
    rid = rule["id"]
    rel_path = rule["path"]
    min_bytes = rule.get("min_bytes", 1)
    sev = rule.get("severity", "warning")
    target = exp_dir / rel_path
    if not target.exists():
        return RuleResult(rid, sev, False, f"File not found: {rel_path}")
    size = target.stat().st_size
    if size >= min_bytes:
        return RuleResult(rid, sev, True)
    return RuleResult(
        rid,
        sev,
        False,
        rule.get("description", f"{rel_path} is {size}B, need >= {min_bytes}B"),
    )


def run_gate_rules(
    exp: Dict[str, Any],
    phase3_root: Path,
    rules: List[Dict[str, Any]],
) -> GateReport:
    exp_name = exp.get("name", "UNKNOWN")
    exp_dir = phase3_root / "experiments" / exp_name
    logs_dir = phase3_root / "logs"
    source = _read_train_source(exp_dir, phase3_root, exp)

    report = GateReport(experiment=exp_name)

    for rule in rules:
        if not rule.get("enabled", True):
            continue

        rtype = rule.get("type", "")

        if rtype == "source_contains":
            if source is None:
                report.results.append(
                    RuleResult(rule["id"], "error", False, "train.py not found")
                )
                continue
            report.results.append(_exec_source_contains(rule, source, exp_dir))

        elif rtype == "source_not_contains":
            if source is None:
                continue
            report.results.append(_exec_source_not_contains(rule, source, exp_dir))

        elif rtype == "stderr_scan":
            report.results.append(_exec_stderr_scan(rule, exp_name, logs_dir))

        elif rtype == "file_exists":
            report.results.append(_exec_file_exists(rule, exp_dir))

        elif rtype == "file_min_size":
            report.results.append(_exec_file_min_size(rule, exp_dir))

        else:
            report.results.append(
                RuleResult(
                    rule.get("id", "?"), "warning", False, f"Unknown rule type: {rtype}"
                )
            )

    return report
