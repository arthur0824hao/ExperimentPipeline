#!/usr/bin/env python3
"""Experiment comparison engine.

Compares two run manifests and produces a structured diff dict suitable
for CLI --json output or programmatic consumption.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from run_manifest import build_manifest
    from db_registry import DBExperimentsDB
except ModuleNotFoundError:
    from pipeline.run_manifest import build_manifest
    from pipeline.db_registry import DBExperimentsDB


def _safe_diff(a: Any, b: Any) -> Optional[Dict[str, Any]]:
    if a == b:
        return None
    return {"a": a, "b": b}


def _delta(a: Any, b: Any) -> Optional[float]:
    if a is None or b is None:
        return None
    try:
        return float(b) - float(a)
    except (TypeError, ValueError):
        return None


def _metric_diff(result_a: Optional[Dict], result_b: Optional[Dict]) -> Dict[str, Any]:
    def _get(r: Optional[Dict[str, Any]], key: str) -> Any:
        return r.get(key) if isinstance(r, dict) else None

    f1_a, f1_b = _get(result_a, "f1_score"), _get(result_b, "f1_score")
    auc_a, auc_b = _get(result_a, "auc_score"), _get(result_b, "auc_score")
    peak_a, peak_b = _get(result_a, "peak_memory_mb"), _get(result_b, "peak_memory_mb")

    diff: Dict[str, Any] = {}
    if f1_a != f1_b:
        diff["f1_score"] = {"a": f1_a, "b": f1_b, "delta": _delta(f1_a, f1_b)}
    if auc_a != auc_b:
        diff["auc_score"] = {"a": auc_a, "b": auc_b, "delta": _delta(auc_a, auc_b)}
    if peak_a != peak_b:
        diff["peak_memory_mb"] = {
            "a": peak_a,
            "b": peak_b,
            "delta": _delta(peak_a, peak_b),
        }
    return diff


def _dict_diff(a: Optional[Dict], b: Optional[Dict], keys: List[str]) -> Dict[str, Any]:
    a = a or {}
    b = b or {}
    diff: Dict[str, Any] = {}
    for key in keys:
        va, vb = a.get(key), b.get(key)
        if va != vb:
            diff[key] = {"a": va, "b": vb}
    return diff


def compare_manifests(manifest_a: Dict[str, Any], manifest_b: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "experiments": [manifest_a["name"], manifest_b["name"]],
        "status": _safe_diff(manifest_a.get("status"), manifest_b.get("status")),
        "terminal_reason": _safe_diff(
            manifest_a.get("terminal_reason"), manifest_b.get("terminal_reason")
        ),
        "outcome": _metric_diff(manifest_a.get("result"), manifest_b.get("result")),
        "config": _dict_diff(
            manifest_a.get("config"), manifest_b.get("config"), ["batch_size", "eval_batch_size"]
        ),
        "lineage": _dict_diff(
            manifest_a.get("lineage"),
            manifest_b.get("lineage"),
            ["parent_experiment", "group_id", "condition_parent", "role"],
        ),
        "script": _safe_diff(manifest_a.get("script"), manifest_b.get("script")),
        "memory_contract_diff": _safe_diff(
            manifest_a.get("memory_contract"), manifest_b.get("memory_contract")
        ),
        "retry": _dict_diff(
            manifest_a, manifest_b, ["retry_count", "oom_retry_count", "max_retries"]
        ),
    }


def compare_experiments(db: DBExperimentsDB, name_a: str, name_b: str) -> Optional[Dict[str, Any]]:
    ma = build_manifest(db, name_a)
    mb = build_manifest(db, name_b)
    if ma is None or mb is None:
        return None
    return compare_manifests(ma, mb)
