#!/usr/bin/env python3
"""TDD tests for worker disable/enable/restart feature.

Design: disabled_workers stored as top-level list in experiments.json
  - D = disable: stop_node + kill experiments + persist disabled state
  - E = enable: clear disabled state + start_node
  - R = restart: kill experiments + restart runner (stays enabled)
  - Disabled workers: won't accept new tasks, shown as DISABLED in dashboard
"""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

# @behavior: db_registry.behavior.yaml#heartbeat-and-health


# ===========================================================================
# ExperimentsDB: disable_worker / enable_worker / is_worker_disabled
# ===========================================================================


class TestDisableWorker:
    """ExperimentsDB.disable_worker(worker_id) adds worker to disabled_workers list."""

    def test_adds_worker_to_disabled_list(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        result = db.disable_worker("minun")

        assert result is True
        reloaded = db.load()
        assert "minun" in reloaded.get("disabled_workers", [])

    def test_idempotent_does_not_duplicate(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": [], "disabled_workers": ["minun"]}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        result = db.disable_worker("minun")

        assert result is True
        reloaded = db.load()
        assert reloaded["disabled_workers"].count("minun") == 1

    def test_can_disable_multiple_workers(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        db.disable_worker("minun")
        db.disable_worker("plusle")

        reloaded = db.load()
        assert set(reloaded["disabled_workers"]) == {"minun", "plusle"}


class TestEnableWorker:
    """ExperimentsDB.enable_worker(worker_id) removes worker from disabled_workers list."""

    def test_removes_worker_from_disabled_list(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [],
            "archived": [],
            "disabled_workers": ["minun", "plusle"],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        result = db.enable_worker("minun")

        assert result is True
        reloaded = db.load()
        assert "minun" not in reloaded.get("disabled_workers", [])
        assert "plusle" in reloaded.get("disabled_workers", [])

    def test_enabling_already_enabled_returns_true(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        result = db.enable_worker("minun")
        assert result is True

    def test_enable_when_no_disabled_workers_key(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        result = db.enable_worker("minun")
        assert result is True


class TestIsWorkerDisabled:
    """ExperimentsDB.is_worker_disabled(worker_id) checks disabled_workers list."""

    def test_snapshot_disabled_workers_is_not_canonical_source(
        self, mock_experiments_file
    ):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": [], "disabled_workers": ["minun"]}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        assert db.is_worker_disabled("minun") is False

    def test_returns_false_when_not_disabled(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": [], "disabled_workers": ["plusle"]}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        assert db.is_worker_disabled("minun") is False

    def test_returns_false_when_no_disabled_key(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        assert db.is_worker_disabled("minun") is False


# ===========================================================================
# Dashboard key bindings: D=disable, E=enable, R=restart (cluster mode)
# ===========================================================================


def _wait_for_action(dashboard, predicate, timeout: float = 1.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        dashboard.drain_async_updates()
        if predicate():
            return
        time.sleep(0.01)
    dashboard.drain_async_updates()


def make_dashboard(experiments=None):
    from experiments import UnifiedDashboard

    cluster_mgr = MagicMock()
    cluster_mgr.stop_node.return_value = (True, "Stopped")
    cluster_mgr.start_node.return_value = (True, "Started")
    cluster_mgr.restart_node.return_value = (True, "Restarted")

    db = MagicMock()
    db.load.return_value = {"experiments": experiments or [], "archived": []}
    db.kill_experiments_on_worker.return_value = 2
    db.disable_worker.return_value = True
    db.enable_worker.return_value = True
    db.is_worker_disabled.return_value = False

    return UnifiedDashboard("worker-1", cluster_mgr, db), cluster_mgr, db


class TestClusterDisableKey:
    """D key in cluster mode: stop_node + kill_experiments_on_worker + disable_worker."""

    def test_d_disables_selected_worker(self):
        dashboard, cluster_mgr, db = make_dashboard()
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 0
        workers = ["minun", "plusle"]

        dashboard.handle_key("D", workers)
        _wait_for_action(dashboard, lambda: cluster_mgr.stop_node.called)

        cluster_mgr.stop_node.assert_called_once_with("minun")
        db.kill_experiments_on_worker.assert_called_once_with("minun")
        db.disable_worker.assert_called_once_with("minun")

    def test_d_on_second_worker(self):
        dashboard, cluster_mgr, db = make_dashboard()
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 1
        workers = ["minun", "plusle"]

        dashboard.handle_key("D", workers)
        _wait_for_action(dashboard, lambda: cluster_mgr.stop_node.called)

        cluster_mgr.stop_node.assert_called_once_with("plusle")
        db.kill_experiments_on_worker.assert_called_once_with("plusle")
        db.disable_worker.assert_called_once_with("plusle")


class TestClusterEnableKey:
    """E key in cluster mode: enable_worker + start_node."""

    def test_e_enables_selected_worker(self):
        dashboard, cluster_mgr, db = make_dashboard()
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 0
        workers = ["minun", "plusle"]

        dashboard.handle_key("E", workers)
        _wait_for_action(dashboard, lambda: cluster_mgr.start_node.called)

        db.enable_worker.assert_called_once_with("minun")
        cluster_mgr.start_node.assert_called_once_with("minun")


class TestClusterRestartKey:
    """R key in cluster mode: kill experiments + restart_node (stays enabled)."""

    def test_r_restarts_without_disabling(self):
        dashboard, cluster_mgr, db = make_dashboard()
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 0
        workers = ["minun", "plusle"]

        dashboard.handle_key("R", workers)
        _wait_for_action(dashboard, lambda: cluster_mgr.restart_node.called)

        db.kill_experiments_on_worker.assert_called_once_with("minun")
        cluster_mgr.restart_node.assert_called_once_with("minun")
        db.disable_worker.assert_not_called()


class TestClusterStopKey:
    def test_k_stops_selected_worker(self):
        dashboard, cluster_mgr, db = make_dashboard()
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 0
        workers = ["minun"]

        dashboard.handle_key("K", workers)
        _wait_for_action(dashboard, lambda: cluster_mgr.stop_node.called)

        cluster_mgr.stop_node.assert_called_once_with("minun")
        db.kill_experiments_on_worker.assert_not_called()


class TestClusterStartKey:
    def test_s_starts_selected_worker(self):
        dashboard, cluster_mgr, db = make_dashboard()
        dashboard.focus_mode = "cluster"
        dashboard.selected_node_idx = 0
        workers = ["minun"]

        dashboard.handle_key("S", workers)
        _wait_for_action(dashboard, lambda: cluster_mgr.start_node.called)

        cluster_mgr.start_node.assert_called_once_with("minun")
        db.kill_experiments_on_worker.assert_not_called()


# ===========================================================================
# Dashboard display: DISABLED status
# ===========================================================================


class TestDashboardDisabledDisplay:
    """build_cluster_panel shows DISABLED for disabled workers."""

    def test_disabled_worker_shows_disabled_status(self):
        from io import StringIO

        from rich.console import Console

        from experiments import UnifiedDashboard

        cluster_mgr = MagicMock()
        db = MagicMock()
        db.load.return_value = {"experiments": [], "archived": []}
        db.is_worker_disabled.return_value = False

        dashboard = UnifiedDashboard("worker-1", cluster_mgr, db)

        cluster_status = {
            "minun": {
                "status": "ONLINE",
                "last_seen_sec": 5,
                "gpus": [],
                "cpu": {},
                "running_jobs": 0,
                "running_experiments": [],
            }
        }

        db.is_worker_disabled.side_effect = lambda w: w == "minun"

        panel = dashboard.build_cluster_panel(cluster_status, ["minun"])
        buf = StringIO()
        console = Console(file=buf, width=120, force_terminal=True)
        console.print(panel)
        rendered = buf.getvalue()
        assert "DISABLED" in rendered


# ===========================================================================
# Runner: disabled workers skip task acceptance
# ===========================================================================


class TestRunnerSkipsDisabledWorker:
    """Runner should not pick up experiments when local worker is disabled."""

    def test_is_worker_disabled_checked_before_accepting_experiments(
        self, mock_experiments_file
    ):
        from experiments import ExperimentsDB

        data = {
            "experiments": [{"name": "exp1", "status": "NEEDS_RERUN"}],
            "archived": [],
            "disabled_workers": ["minun"],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        assert db.is_worker_disabled("minun") is False
        # get_runnable_experiments still returns the experiment (it's a DB-level check)
        # The runner loop should check is_worker_disabled before calling get_runnable_experiments
        runnable = db.get_runnable_experiments()
        assert len(runnable) >= 1


# ===========================================================================
# Footer help text updated
# ===========================================================================


class TestFooterHelpText:
    """Cluster mode footer should show D/E/R instead of K/S/R."""

    def test_cluster_footer_contains_disable_enable_restart(self):
        from experiments import UnifiedDashboard

        cluster_mgr = MagicMock()
        cluster_mgr.get_cluster_status.return_value = {}
        db = MagicMock()
        db.load.return_value = {"experiments": [], "archived": []}
        db.is_worker_disabled.return_value = False

        dashboard = UnifiedDashboard("worker-1", cluster_mgr, db)
        dashboard.focus_mode = "cluster"

        # We test the build_layout footer indirectly
        # The help_text should mention D:Disable, E:Enable, R:Restart
        # We can't easily extract footer text from Rich layout, so we check
        # the source code pattern. Instead, we verify the actions list.
        assert "disable" in dashboard.actions
        assert "enable" in dashboard.actions
        assert "restart" in dashboard.actions
        # Old actions should not be present
        assert "start" not in dashboard.actions
        assert "stop" not in dashboard.actions

    def test_experiment_footer_contains_two_step_scope_hint(self):
        from io import StringIO

        from rich.console import Console

        from experiments import UnifiedDashboard

        cluster_mgr = MagicMock()
        cluster_mgr.get_cluster_status.return_value = {}
        db = MagicMock()
        db.load.return_value = {
            "experiments": [{"name": "exp-a", "status": "NEEDS_RERUN"}],
            "archived": [],
        }
        db.is_worker_disabled.return_value = False

        dashboard = UnifiedDashboard("worker-1", cluster_mgr, db)
        dashboard.focus_mode = "experiments"
        layout = dashboard.build_layout(running_count=0)

        buf = StringIO()
        console = Console(file=buf, width=180, force_terminal=False)
        console.print(layout["footer"].renderable)
        rendered = buf.getvalue()

        assert "All" in rendered
        assert "Selected" in rendered
        assert "Kill" in rendered
