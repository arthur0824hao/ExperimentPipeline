#!/usr/bin/env python3
"""Tests for Agent CLI v1."""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(autouse=True)
def _mock_control_plane(monkeypatch):
    """Prevent real DB/cluster connections during CLI tests."""
    mock_svc_cls = MagicMock()
    mock_svc = MagicMock()
    mock_svc.get_status_summary.return_value = {
        "active_experiments": 3,
        "completed_experiments": 1,
        "by_status": {"RUNNING": 2, "NEEDS_RERUN": 1},
        "workers_online": 1,
        "workers_total": 2,
    }
    mock_svc.list_experiments.return_value = {
        "items": [{"name": "exp1", "status": "RUNNING"}],
        "page": 1,
        "per_page": 50,
        "total": 1,
        "total_pages": 1,
    }
    mock_svc.get_experiment.return_value = {"name": "exp1", "status": "RUNNING"}
    mock_svc.get_cluster_health.return_value = {
        "cluster": {"w1": {"status": "ONLINE"}},
    }
    mock_svc_cls.return_value = mock_svc
    monkeypatch.setattr("ep_cli.ControlPlaneService", mock_svc_cls)


def _run_cli(argv):
    from ep_cli import main

    return main(argv)


class TestStatusCommand:
    def test_json_output(self, capsys):
        code = _run_cli(["status", "--output", "json"])
        assert code == 0
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["status"] == "ok"
        assert data["data"]["active_experiments"] == 3

    def test_text_output(self, capsys):
        code = _run_cli(["status", "--output", "text"])
        assert code == 0


class TestExperimentsCommand:
    def test_json_output(self, capsys):
        code = _run_cli(["experiments", "--output", "json"])
        assert code == 0
        data = json.loads(capsys.readouterr().out)
        assert data["data"]["total"] == 1


class TestExperimentDetailCommand:
    def test_found(self, capsys):
        code = _run_cli(["experiment", "exp1", "--output", "json"])
        assert code == 0
        data = json.loads(capsys.readouterr().out)
        assert data["data"]["name"] == "exp1"


class TestClusterCommand:
    def test_json_output(self, capsys):
        code = _run_cli(["cluster", "--output", "json"])
        assert code == 0
        data = json.loads(capsys.readouterr().out)
        assert "cluster" in data["data"]


class TestNoCommand:
    def test_returns_error(self, capsys):
        code = _run_cli([])
        assert code == 1
