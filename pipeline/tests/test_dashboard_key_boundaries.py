"""Boundary tests for experiment dashboard key handling."""

import pytest
import sys
from unittest.mock import MagicMock

sys.path.insert(0, "pipeline")
sys.path.insert(0, "pipeline/preprocess_lib")
from experiments import UnifiedDashboard


def _make_dashboard(experiments=None, completed=None):
    cluster_mgr = MagicMock()
    cluster_mgr.stop_node.return_value = (True, "Stopped")
    cluster_mgr.start_node.return_value = (True, "Started")
    cluster_mgr.get_cluster_status.return_value = {}

    db = MagicMock()
    db.load.return_value = {
        "experiments": experiments or [],
        "archived": completed or [],
    }
    db.get_experiment.return_value = None
    db.list_experiments.return_value = {
        "experiments": experiments or [],
        "completed": completed or [],
        "total": len(experiments or []) + len(completed or []),
    }

    dashboard = UnifiedDashboard("worker-1", cluster_mgr, db, is_watch=True)
    return dashboard


class TestExpSelectionBoundaries:
    def test_w_at_top_stays_at_0(self):
        exps = [{"name": f"exp{i}"} for i in range(3)]
        dashboard = _make_dashboard(experiments=exps)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 0
        dashboard.handle_key("w", ["node1"])
        assert dashboard.selected_exp_idx == 0

    def test_s_at_bottom_stays(self):
        exps = [{"name": f"exp{i}"} for i in range(3)]
        dashboard = _make_dashboard(experiments=exps)
        dashboard.focus_mode = "experiments"
        dashboard.selected_exp_idx = 2
        dashboard.handle_key("s", ["node1"])
        assert dashboard.selected_exp_idx == 2

    def test_ws_with_empty_experiments(self):
        dashboard = _make_dashboard(experiments=[])
        dashboard.focus_mode = "experiments"
        dashboard.handle_key("w", ["node1"])
        dashboard.handle_key("s", ["node1"])


class TestPageBoundaries:
    def test_N_wraps_at_last_page(self):
        exps = [{"name": f"exp{i}"} for i in range(25)]
        dashboard = _make_dashboard(experiments=exps)
        dashboard.focus_mode = "experiments"
        dashboard.exp_page_size = 20
        dashboard.exp_total_pages = 2
        dashboard.exp_page = 1
        dashboard.handle_key("N", ["node1"])
        assert dashboard.exp_page == 0

    def test_P_wraps_at_first_page(self):
        exps = [{"name": f"exp{i}"} for i in range(25)]
        dashboard = _make_dashboard(experiments=exps)
        dashboard.focus_mode = "experiments"
        dashboard.exp_page_size = 20
        dashboard.exp_total_pages = 2
        dashboard.exp_page = 0
        dashboard.handle_key("P", ["node1"])
        assert dashboard.exp_page == 1


class TestTwoStepKeyBoundaries:
    def test_esc_cancels_scope(self):
        dashboard = _make_dashboard()
        dashboard.focus_mode = "experiments"
        dashboard.exp_two_step = type(
            "TwoStep", (), {"handle_key": lambda self, k: None, "state": "idle"}
        )()
        dashboard.handle_key("\x1b", ["node1"])
        assert dashboard.exp_two_step is not None
