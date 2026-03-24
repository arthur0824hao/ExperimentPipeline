#!/usr/bin/env python3
import pytest
import time
from unittest.mock import patch, MagicMock

from rich.console import Console

# @behavior: experiments.behavior.yaml#heartbeat-and-ui-loop


def _wait_for_action(dashboard, predicate, timeout: float = 1.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        dashboard.drain_async_updates()
        if predicate():
            return
        time.sleep(0.01)
    dashboard.drain_async_updates()


class TestUnifiedDashboardInit:
    def test_initializes_attributes(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("test_worker", cm, db)
        assert dashboard.worker_id == "test_worker"
        assert dashboard.selected_node_idx == 0
        assert dashboard.action_mode is False


class TestUnifiedDashboardHandleKey:
    def test_q_returns_false(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        result = dashboard.handle_key("q", ["node1"])
        assert result is False

    def test_Q_returns_false(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        result = dashboard.handle_key("Q", ["node1"])
        assert result is False

    def test_w_moves_up(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.selected_node_idx = 1
        dashboard.handle_key("w", ["node1", "node2"])
        assert dashboard.selected_node_idx == 0

    def test_s_moves_down(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.selected_node_idx = 0
        dashboard.handle_key("s", ["node1", "node2"])
        assert dashboard.selected_node_idx == 1

    def test_enter_toggles_action_mode(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.handle_key("\r", ["node1"])
        assert dashboard.action_mode is True

    def test_escape_exits_action_mode(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.action_mode = True
        dashboard.handle_key("\x1b", ["node1"])
        assert dashboard.action_mode is False

    def test_a_d_navigate_actions(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.action_mode = True
        dashboard.action_idx = 1
        dashboard.handle_key("a", ["node1"])
        assert dashboard.action_idx == 0
        dashboard.handle_key("d", ["node1"])
        assert dashboard.action_idx == 1

    def test_empty_workers_q_still_works(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        result = dashboard.handle_key("q", [])
        assert result is False

    def test_none_key_returns_true(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        result = dashboard.handle_key(None, ["node1"])
        assert result is True

    def test_experiment_mode_lower_s_keeps_navigation_direct(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.focus_mode = "experiments"
        dashboard._panel_exp_rows = [{"name": "exp-a"}, {"name": "exp-b"}]
        dashboard.selected_exp_idx = 0

        dashboard.handle_key("s", ["node1"])

        assert dashboard.selected_exp_idx == 1
        assert dashboard.exp_two_step.state == "idle"


class TestUnifiedDashboardSetMessage:
    def test_sets_message_and_time(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        before = time.time()
        dashboard.set_message("Test message")
        after = time.time()
        assert dashboard.message == "Test message"
        assert before <= dashboard.message_time <= after


class TestUnifiedDashboardQuickAction:
    @patch("experiments.ClusterManager.start_node")
    def test_S_starts_node(self, mock_start, db):
        from experiments import UnifiedDashboard, ClusterManager

        mock_start.return_value = (True, "Started")
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.handle_key("S", ["node1"])
        _wait_for_action(dashboard, lambda: mock_start.called)
        mock_start.assert_called_once_with("node1", db=db)
        dashboard.shutdown()

    @patch("experiments.ClusterManager.restart_node")
    def test_R_restarts_node(self, mock_restart, db):
        from experiments import UnifiedDashboard, ClusterManager

        mock_restart.return_value = (True, "Restarted")
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.handle_key("R", ["node1"])
        _wait_for_action(dashboard, lambda: mock_restart.called)
        mock_restart.assert_called_once_with("node1", db=db)
        dashboard.shutdown()

    @patch("experiments.ClusterManager.stop_node")
    def test_K_stops_node(self, mock_stop, db):
        from experiments import UnifiedDashboard, ClusterManager

        mock_stop.return_value = (True, "Stopped")
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.handle_key("K", ["node1"])
        _wait_for_action(dashboard, lambda: mock_stop.called)
        mock_stop.assert_called_once_with("node1")
        dashboard.shutdown()


class TestUnifiedDashboardBuildPanels:
    @patch("experiments.get_pid_gpu_map")
    def test_build_experiments_panel_returns_panel(self, mock_pid, populated_db):
        from experiments import UnifiedDashboard, ClusterManager
        from rich.panel import Panel

        mock_pid.return_value = {}
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, populated_db)
        panel = dashboard.build_experiments_panel()
        assert isinstance(panel, Panel)

    def test_build_cluster_panel_returns_panel(self, db):
        from experiments import UnifiedDashboard, ClusterManager
        from rich.panel import Panel

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_cluster_panel(
            {"node1": {"status": "ONLINE", "gpus": [], "cpu": {}}}, ["node1"]
        )
        assert isinstance(panel, Panel)

    def test_cluster_panel_shows_gpu_probe_error_when_present(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_cluster_panel(
            {
                "minun": {
                    "status": "ONLINE",
                    "gpus": [],
                    "cpu": {
                        "load_percent": 12,
                        "load1": 1.0,
                        "load5": 1.0,
                        "load15": 1.0,
                        "cpu_count": 12,
                    },
                    "gpu_probe_error": "empty nvidia-smi output",
                }
            },
            ["minun"],
        )
        console = Console(record=True, width=160)
        console.print(panel)
        text = console.export_text()
        assert "GPU: Probe error" in text
        assert "nvidia-smi" in text

    @patch("experiments.get_all_gpu_status")
    @patch("experiments.get_pid_gpu_map")
    def test_build_layout_returns_layout(self, mock_pid, mock_gpus, populated_db):
        from experiments import UnifiedDashboard, ClusterManager
        from rich.layout import Layout

        mock_pid.return_value = {}
        mock_gpus.return_value = []
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        with patch.object(cm, "get_cluster_status", return_value={}):
            dashboard = UnifiedDashboard("worker", cm, populated_db)
            layout = dashboard.build_layout(running_count=2)
            assert isinstance(layout, Layout)

    @patch("experiments.get_all_gpu_status")
    @patch("experiments.get_pid_gpu_map")
    def test_build_layout_shows_only_one_full_panel_per_tab(
        self, mock_pid, mock_gpus, populated_db
    ):
        from experiments import UnifiedDashboard, ClusterManager

        mock_pid.return_value = {}
        mock_gpus.return_value = []
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        cluster_status = {"node1": {"status": "ONLINE", "gpus": [], "cpu": {}}}
        with patch.object(cm, "get_cluster_status", return_value=cluster_status):
            dashboard = UnifiedDashboard("worker", cm, populated_db)
            cluster_layout = dashboard.build_layout(running_count=0)
            cluster_console = Console(record=True, width=220)
            cluster_console.print(cluster_layout)
            cluster_text = cluster_console.export_text()

            dashboard.handle_key("\t", ["node1"])
            exp_layout = dashboard.build_layout(running_count=0)
            exp_console = Console(record=True, width=220)
            exp_console.print(exp_layout)
            exp_text = exp_console.export_text()

        assert "Cluster (1 online / 1 total)" in cluster_text
        assert "Experiments (0 active" not in cluster_text
        assert "Experiments (" in exp_text


class TestGPUMarkerCoexistence:
    """Tests for GPU marker rendering - star and triangle can coexist."""

    @patch("experiments.get_all_gpu_status")
    @patch("experiments.get_pid_gpu_map")
    def test_star_marker_when_gpu_in_our_gpu_ids(self, mock_pid, mock_gpus, populated_db):
        """When GPU is in our_gpu_ids, show only star marker."""
        from experiments import UnifiedDashboard, ClusterManager
        from rich.console import Console

        mock_pid.return_value = {"node1": {0}}  # GPU 0 used by our experiment
        mock_gpus.return_value = [{"index": 0, "used": 100, "total": 8192, "util": 5}]
        
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        cluster_status = {
            "node1": {
                "status": "ONLINE",
                "gpus": [{"index": 0, "used": 100, "total": 8192, "util": 5}],
                "cpu": {"load1": 0.5},
                "our_gpu_ids": [0],
            }
        }
        with patch.object(cm, "get_cluster_status", return_value=cluster_status):
            dashboard = UnifiedDashboard("worker", cm, populated_db)
            panel = dashboard.build_cluster_panel(cluster_status, ["node1"])
        
        console = Console(record=True, width=160)
        console.print(panel)
        text = console.export_text()

        assert "★ GPU0" in text
        assert "★▲ GPU0" not in text

    @patch("experiments.get_all_gpu_status")
    @patch("experiments.get_pid_gpu_map")
    def test_triangle_marker_when_foreign_usage(self, mock_pid, mock_gpus, populated_db):
        """When GPU has foreign usage (>500MB) and not in our_gpu_ids, show triangle."""
        from experiments import UnifiedDashboard, ClusterManager
        from rich.console import Console

        mock_pid.return_value = {"node1": set()}  # No our experiments
        mock_gpus.return_value = [{"index": 0, "used": 600, "total": 8192, "util": 10}]
        
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        cluster_status = {
            "node1": {
                "status": "ONLINE",
                "gpus": [{"index": 0, "used": 600, "total": 8192, "util": 10}],
                "cpu": {"load1": 0.5},
                "our_gpu_ids": [],  # Not our GPU
            }
        }
        with patch.object(cm, "get_cluster_status", return_value=cluster_status):
            dashboard = UnifiedDashboard("worker", cm, populated_db)
            panel = dashboard.build_cluster_panel(cluster_status, ["node1"])
        
        console = Console(record=True, width=160)
        console.print(panel)
        text = console.export_text()
        
        assert "▲" in text
        assert "★" not in text

    @patch("experiments.get_all_gpu_status")
    @patch("experiments.get_pid_gpu_map")
    def test_both_markers_coexist(self, mock_pid, mock_gpus, populated_db):
        """When GPU is in our_gpu_ids AND has foreign usage, show both markers."""
        from experiments import UnifiedDashboard, ClusterManager
        from rich.console import Console

        mock_pid.return_value = {"node1": {0}}  # GPU 0 used by our experiment
        # GPU 0: our experiment + foreign usage (our job uses some, foreign uses >500MB)
        mock_gpus.return_value = [{"index": 0, "used": 2000, "total": 8192, "util": 25}]
        
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        cluster_status = {
            "node1": {
                "status": "ONLINE",
                "gpus": [{"index": 0, "used": 2000, "total": 8192, "util": 25}],
                "cpu": {"load1": 0.5},
                "our_gpu_ids": [0],  # Our experiment on this GPU
            }
        }
        with patch.object(cm, "get_cluster_status", return_value=cluster_status):
            dashboard = UnifiedDashboard("worker", cm, populated_db)
            panel = dashboard.build_cluster_panel(cluster_status, ["node1"])
        
        console = Console(record=True, width=160)
        console.print(panel)
        text = console.export_text()
        
        # Both markers should be present
        assert "★" in text
        assert "▲" in text

    @patch("experiments.get_all_gpu_status")
    @patch("experiments.get_pid_gpu_map")
    def test_no_marker_for_free_gpu(self, mock_pid, mock_gpus, populated_db):
        """When GPU is free (no our job, no foreign usage), show no marker."""
        from experiments import UnifiedDashboard, ClusterManager
        from rich.console import Console

        mock_pid.return_value = {"node1": set()}
        mock_gpus.return_value = [{"index": 0, "used": 100, "total": 8192, "util": 2}]
        
        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        cluster_status = {
            "node1": {
                "status": "ONLINE",
                "gpus": [{"index": 0, "used": 100, "total": 8192, "util": 2}],
                "cpu": {"load1": 0.5},
                "our_gpu_ids": [],
            }
        }
        with patch.object(cm, "get_cluster_status", return_value=cluster_status):
            dashboard = UnifiedDashboard("worker", cm, populated_db)
            panel = dashboard.build_cluster_panel(cluster_status, ["node1"])
        
        console = Console(record=True, width=160)
        console.print(panel)
        text = console.export_text()

        # No markers for free GPU
        assert "★ GPU0" not in text
        assert "▲ GPU0" not in text


class TestOfflineStaleMetadata:
    def test_offline_card_labels_last_reported_fields(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()

        cluster_status = {
            "node1": {
                "status": "OFFLINE",
                "last_seen_sec": 300,
                "pid": 4321,
                "running_jobs": 2,
                "running_experiments": ["EXP_A"],
                "gpus": [{"index": 0, "used": 700, "total": 8192, "util": 12}],
                "cpu": {"load_percent": 20, "cpu_count": 8, "load1": 1.2},
                "our_gpu_ids": [0],
            }
        }

        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_cluster_panel(cluster_status, ["node1"])
        console = Console(record=True, width=180)
        console.print(panel)
        text = console.export_text()

        assert "Last PID" in text
        assert "Last Run" in text

    def test_online_card_keeps_live_labels(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()

        cluster_status = {
            "node1": {
                "status": "ONLINE",
                "last_seen_sec": 10,
                "pid": 4321,
                "running_jobs": 1,
                "running_experiments": ["EXP_B"],
                "gpus": [{"index": 0, "used": 600, "total": 8192, "util": 10}],
                "cpu": {"load_percent": 10, "cpu_count": 8, "load1": 0.6},
                "our_gpu_ids": [0],
            }
        }

        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_cluster_panel(cluster_status, ["node1"])
        console = Console(record=True, width=180)
        console.print(panel)
        text = console.export_text()

        assert "Last PID" not in text
        assert "Last Run" not in text
        assert "PID" in text
        assert "Run" in text
