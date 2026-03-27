#!/usr/bin/env python3
"""Run manifest contract — canonical schema for experiment run data.

Defines the authoritative shape of a run manifest and provides builders
that produce manifests from DB registry + on-disk artifacts.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from artifact import (
        _read_result_payload,
        _read_resource_usage,
        get_terminal_reason,
        RESULTS_DB_DIR,
        BASE_DIR,
        LOGS_DIR,
    )
    from db_registry import DBExperimentsDB
except ModuleNotFoundError:
    from pipeline.artifact import (
        _read_result_payload,
        _read_resource_usage,
        get_terminal_reason,
        RESULTS_DB_DIR,
        BASE_DIR,
        LOGS_DIR,
    )
    from pipeline.db_registry import DBExperimentsDB


MANIFEST_SCHEMA_VERSION = "1.0"


def _artifact_refs(exp_name: str) -> List[Dict[str, Any]]:
    candidates = [
        ("result", RESULTS_DB_DIR / f"{exp_name}.json"),
        ("result_alt", BASE_DIR / "experiments" / exp_name / "outputs" / "results.json"),
        ("resource_usage", BASE_DIR / "experiments" / exp_name / "resource_usage.json"),
        ("log_stderr", LOGS_DIR / f"{exp_name}.err"),
    ]
    refs: List[Dict[str, Any]] = []
    for art_type, path in candidates:
        path = Path(path)
        exists = path.exists()
        refs.append(
            {
                "type": art_type,
                "path": str(path),
                "exists": exists,
                "mtime": path.stat().st_mtime if exists else None,
            }
        )
    return refs


def build_manifest(db: DBExperimentsDB, name: str) -> Optional[Dict[str, Any]]:
    exp = db.get_experiment(name)
    if exp is None:
        return None

    _, _ = _read_result_payload(name)
    _, _ = _read_resource_usage(name)
    artifact_refs = _artifact_refs(name)

    result = exp.get("result")
    error_info = exp.get("error_info")
    running_on = exp.get("running_on")

    panel_truth = db.get_panel_truth(name)
    terminal_metadata = None
    canonical_result = None
    if panel_truth:
        terminal_metadata = panel_truth.get("terminal_metadata")
        canonical_result = panel_truth.get("canonical_result")
    terminal_reason = get_terminal_reason(
        name, exp.get("status", ""), result, error_info, terminal_metadata
    )

    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "name": name,
        "run_id": exp.get("run_id"),
        "batch_id": exp.get("batch_id"),
        "status": exp.get("status", "UNKNOWN"),
        "started_at": running_on.get("started_at") if running_on else None,
        "completed_at": exp.get("completed_at"),
        "worker_id": running_on.get("worker") if running_on else None,
        "gpu_id": running_on.get("gpu") if running_on else None,
        "pid": running_on.get("pid") if running_on else None,
        "result": result,
        "error_info": error_info,
        "terminal_reason": terminal_reason,
        "canonical_result": canonical_result,
        "terminal_metadata": terminal_metadata,
        "script": exp.get("script"),
        "memory_contract": exp.get("memory_contract"),
        "config": {
            "batch_size": exp.get("batch_size"),
            "eval_batch_size": exp.get("eval_batch_size"),
        },
        "lineage": {
            "parent_experiment": exp.get("parent_experiment"),
            "group_id": exp.get("group_id"),
            "depends_on_group": exp.get("depends_on_group"),
            "condition_parent": exp.get("condition_parent"),
            "gate_type": exp.get("gate_type"),
            "gate_evidence_ref": exp.get("gate_evidence_ref"),
            "role": exp.get("role"),
            "main_experiment": exp.get("main_experiment"),
        },
        "artifacts": artifact_refs,
        "retry_count": exp.get("retry_count", 0),
        "oom_retry_count": exp.get("oom_retry_count", 0),
        "max_retries": exp.get("max_retries", 0),
        "display_order": exp.get("display_order"),
    }


def build_manifest_batch(
    db: DBExperimentsDB, names: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    if names is None:
        rows = db.load_all_for_panel() or []
        names = [r["name"] for r in rows if "name" in r]
    manifests: List[Dict[str, Any]] = []
    for name in names:
        m = build_manifest(db, name)
        if m is not None:
            manifests.append(m)
    return manifests
