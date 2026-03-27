#!/usr/bin/env python3
"""Tests for run manifest contract."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from run_manifest import build_manifest, build_manifest_batch, MANIFEST_SCHEMA_VERSION


def _mock_db(exp_dict=None, panel_truth=None, all_rows=None):
    db = MagicMock()
    db.get_experiment.return_value = exp_dict
    db.get_panel_truth.return_value = panel_truth
    db.load_all_for_panel.return_value = all_rows or []
    return db


@patch("run_manifest._read_result_payload", return_value=(None, None))
@patch("run_manifest._read_resource_usage", return_value=(None, None))
@patch("run_manifest.get_terminal_reason", return_value="COMPLETED")
class TestBuildManifest:
    def test_returns_none_for_missing(self, *_):
        db = _mock_db(exp_dict=None)
        assert build_manifest(db, "nope") is None

    def test_basic_manifest_shape(self, *_):
        exp = {
            "name": "exp1",
            "status": "COMPLETED",
            "batch_id": "b1",
            "result": {"f1_score": 0.9, "auc_score": 0.8, "peak_memory_mb": 5000},
            "error_info": None,
            "running_on": None,
            "completed_at": "2026-01-01T00:00:00",
            "script": "train.py",
            "memory_contract": {"est_mem_decision_mb": 8000},
            "parent_experiment": None,
            "group_id": "g1",
            "depends_on_group": None,
            "condition_parent": None,
            "gate_type": None,
            "gate_evidence_ref": None,
            "role": "main",
            "main_experiment": "exp1",
            "retry_count": 0,
            "oom_retry_count": 0,
            "max_retries": 2,
            "display_order": 1,
        }
        db = _mock_db(
            exp_dict=exp,
            panel_truth={"canonical_result": None, "terminal_metadata": None},
        )
        m = build_manifest(db, "exp1")
        assert m is not None
        assert m["schema_version"] == MANIFEST_SCHEMA_VERSION
        assert m["name"] == "exp1"
        assert m["result"]["f1_score"] == 0.9
        assert m["lineage"]["group_id"] == "g1"
        assert isinstance(m["artifacts"], list)

    def test_batch_builds_all(self, *_):
        exp = {"name": "e1", "status": "RUNNING", "running_on": None, "result": None, "error_info": None}
        db = _mock_db(exp_dict=exp, all_rows=[{"name": "e1"}, {"name": "e2"}])
        results = build_manifest_batch(db)
        assert len(results) == 2
