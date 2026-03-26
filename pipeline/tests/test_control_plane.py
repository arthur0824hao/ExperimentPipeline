#!/usr/bin/env python3
"""Tests for the shared control-plane query/service layer."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from control_plane import ControlPlaneService


def _make_service(experiments=None, cluster_status=None):
    """Create a ControlPlaneService with mocked DB and cluster manager."""
    db = MagicMock()
    db.load_all_for_panel.return_value = experiments or []
    db.get_experiment.return_value = None
    db.load.return_value = {"experiments": experiments or [], "completed": []}
    db.is_worker_disabled.return_value = False

    cluster_mgr = MagicMock()
    cluster_mgr.get_cluster_status.return_value = cluster_status or {}

    return ControlPlaneService(db=db, cluster_mgr=cluster_mgr)


class TestListExperiments:
    def test_empty(self):
        svc = _make_service()
        result = svc.list_experiments()
        assert result["items"] == []
        assert result["total"] == 0
        assert result["page"] == 1

    def test_pagination(self):
        exps = [{"name": f"exp{i}", "status": "READY"} for i in range(10)]
        svc = _make_service(experiments=exps)
        result = svc.list_experiments(page=2, per_page=3)
        assert len(result["items"]) == 3
        assert result["page"] == 2
        assert result["total"] == 10
        assert result["total_pages"] == 4


class TestGetExperiment:
    def test_found(self):
        svc = _make_service()
        svc.db.get_experiment.return_value = {"name": "exp1", "status": "RUNNING"}
        result = svc.get_experiment("exp1")
        assert result is not None
        assert result["name"] == "exp1"

    def test_not_found(self):
        svc = _make_service()
        assert svc.get_experiment("nope") is None


class TestGetClusterHealth:
    def test_returns_cluster_dict(self):
        svc = _make_service(cluster_status={"w1": {"status": "ONLINE"}})
        result = svc.get_cluster_health()
        assert "cluster" in result
        assert result["cluster"]["w1"]["status"] == "ONLINE"


class TestGetStatusSummary:
    def test_counts(self):
        exps = [
            {"name": "a", "status": "RUNNING"},
            {"name": "b", "status": "RUNNING"},
            {"name": "c", "status": "NEEDS_RERUN"},
        ]
        svc = _make_service(
            experiments=exps,
            cluster_status={"w1": {"status": "ONLINE"}},
        )
        svc.db.load.return_value = {
            "experiments": exps,
            "completed": [{"name": "d"}],
        }
        result = svc.get_status_summary()
        assert result["active_experiments"] == 3
        assert result["completed_experiments"] == 1
        assert result["by_status"]["RUNNING"] == 2
        assert result["workers_online"] == 1
