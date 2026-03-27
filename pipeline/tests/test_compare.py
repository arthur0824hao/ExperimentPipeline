#!/usr/bin/env python3
"""Tests for comparison engine."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from compare import compare_manifests, _metric_diff, _delta


def _manifest(
    name="exp1",
    status="COMPLETED",
    f1=0.9,
    auc=0.8,
    peak=5000,
    script="train.py",
    batch_size=32,
    group_id=None,
    role="main",
):
    return {
        "schema_version": "1.0",
        "name": name,
        "status": status,
        "result": {"f1_score": f1, "auc_score": auc, "peak_memory_mb": peak},
        "terminal_reason": "COMPLETED" if status == "COMPLETED" else None,
        "script": script,
        "config": {"batch_size": batch_size, "eval_batch_size": None},
        "lineage": {
            "parent_experiment": None,
            "group_id": group_id,
            "condition_parent": None,
            "role": role,
        },
        "memory_contract": None,
        "retry_count": 0,
        "oom_retry_count": 0,
        "max_retries": 2,
    }


class TestCompareManifests:
    def test_identical(self):
        m = _manifest()
        diff = compare_manifests(m, m)
        assert diff["experiments"] == ["exp1", "exp1"]
        assert diff["outcome"] == {}
        assert diff["status"] is None

    def test_metric_diff(self):
        a = _manifest(name="a", f1=0.85)
        b = _manifest(name="b", f1=0.92)
        diff = compare_manifests(a, b)
        assert "f1_score" in diff["outcome"]
        assert diff["outcome"]["f1_score"]["delta"] == pytest.approx(0.07)

    def test_config_diff(self):
        a = _manifest(name="a", batch_size=32)
        b = _manifest(name="b", batch_size=64)
        diff = compare_manifests(a, b)
        assert "batch_size" in diff["config"]

    def test_lineage_diff(self):
        a = _manifest(name="a", group_id="g1")
        b = _manifest(name="b", group_id="g2")
        diff = compare_manifests(a, b)
        assert "group_id" in diff["lineage"]


class TestDelta:
    def test_numeric(self):
        assert _delta(0.8, 0.9) == pytest.approx(0.1)

    def test_none_input(self):
        assert _delta(None, 0.9) is None
