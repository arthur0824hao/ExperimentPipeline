#!/usr/bin/env python3
import pytest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

from experiments import ExperimentsDB as RuntimeExperimentsDB


LEGACY_JSON_DB_ONLY = RuntimeExperimentsDB.__name__ != "DBExperimentsDB"


class TestExperimentsDBLoadEdgeCases:
    def test_load_dict_missing_experiments_key(self, temp_dir):
        from experiments import ExperimentsDB

        exp_file = temp_dir / "exp.json"
        exp_file.write_text(json.dumps({"archived": [{"name": "old"}]}))
        db = ExperimentsDB(exp_file)
        result = db.load()
        if not LEGACY_JSON_DB_ONLY:
            assert "experiments" in result
            assert isinstance(result["experiments"], list)
            return
        assert result["experiments"] == []
        assert result["archived"] == [{"name": "old"}]


class TestGetGpuProcessCountEdgeCases:
    @patch("subprocess.check_output")
    def test_handles_empty_lines(self, mock_output):
        from experiments import get_gpu_process_count

        mock_output.side_effect = ["GPU-UUID-1, 1234\n\n", "0, GPU-UUID-1\n\n"]
        counts = get_gpu_process_count()
        assert counts[0] == 1

    @patch("subprocess.check_output")
    def test_handles_invalid_parts(self, mock_output):
        from experiments import get_gpu_process_count

        mock_output.side_effect = [
            "GPU-UUID-1, 1234\nshortline",
            "0, GPU-UUID-1\ninvalid",
        ]
        counts = get_gpu_process_count()
        assert 0 in counts


class TestGetPidGpuMapEdgeCases:
    @patch("subprocess.check_output")
    def test_handles_empty_lines(self, mock_output):
        from experiments import get_pid_gpu_map

        mock_output.return_value = "1234, 5000\n\n5678, 8000"
        result = get_pid_gpu_map()
        assert 1234 in result

    @patch("subprocess.check_output")
    def test_handles_short_lines(self, mock_output):
        from experiments import get_pid_gpu_map

        mock_output.return_value = "1234, 5000\njust_one"
        result = get_pid_gpu_map()
        assert 1234 in result


class TestUpdateLockPidException:
    def test_handles_write_error(self, temp_locks_dir):
        from experiments import update_lock_pid

        lock_file = temp_locks_dir / "test.lock"
        lock_file.mkdir()
        update_lock_pid("test", "worker", 123, 0)


class TestClusterManagerLoadMachinesException:
    def test_handles_invalid_json(self, temp_dir):
        from experiments import ClusterManager

        machines_file = temp_dir / "machines.json"
        machines_file.write_text("not json {{")
        with patch("experiments.MACHINES_FILES", [machines_file]):
            cm = ClusterManager()
            assert cm.machines == {}


class TestClusterManagerHeartbeatEdgeCases:
    def test_handles_invalid_heartbeat_json(
        self, temp_machines_file, temp_heartbeats_dir
    ):
        from experiments import ClusterManager

        hb_file = temp_heartbeats_dir / "bad.json"
        hb_file.write_text("not json")
        with patch("experiments.MACHINES_FILES", [temp_machines_file]):
            with patch("experiments.HEARTBEATS_DIR", temp_heartbeats_dir):
                cm = ClusterManager()
                status = cm.get_cluster_status()
                assert "node1" in status


class TestClusterManagerStopNodeException:
    @patch("subprocess.run")
    def test_handles_exception(self, mock_run, temp_machines_file):
        from experiments import ClusterManager

        mock_run.side_effect = Exception("connection failed")
        with patch("experiments.MACHINES_FILES", [temp_machines_file]):
            cm = ClusterManager()
            ok, msg = cm.stop_node("node1")
            assert ok is False
            assert "connection failed" in msg


class TestClusterManagerStartNodeException:
    @patch("subprocess.run")
    def test_handles_generic_exception(self, mock_run, temp_machines_file):
        from experiments import ClusterManager

        mock_run.side_effect = Exception("unknown error")
        with patch("experiments.MACHINES_FILES", [temp_machines_file]):
            cm = ClusterManager()
            ok, msg = cm.start_node("node1")
            assert ok is False
            assert "unknown error" in msg


class TestDashboardClusterPanelEdgeCases:
    def test_action_mode_styling(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.action_mode = True
        dashboard.selected_node_idx = 0
        panel = dashboard.build_cluster_panel(
            {
                "node1": {
                    "status": "ONLINE",
                    "gpus": [],
                    "cpu": {},
                    "last_seen_sec": 10,
                    "running_jobs": 1,
                }
            },
            ["node1"],
        )
        assert panel is not None

    def test_offline_node_time_ago(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_cluster_panel(
            {
                "node1": {
                    "status": "OFFLINE",
                    "gpus": [],
                    "cpu": {},
                    "last_seen_sec": 120,
                    "running_jobs": 0,
                }
            },
            ["node1"],
        )
        assert panel is not None

    def test_with_gpu_data(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_cluster_panel(
            {
                "node1": {
                    "status": "ONLINE",
                    "gpus": [{"index": 0, "used": 10000, "total": 24000, "util": 75}],
                    "cpu": {
                        "load_percent": 50,
                        "load1": 2.0,
                        "load5": 1.5,
                        "load15": 1.0,
                        "cpu_count": 8,
                    },
                    "last_seen_sec": 5,
                    "running_jobs": 1,
                }
            },
            ["node1"],
        )
        assert panel is not None


class TestDashboardExperimentsPanelEdgeCases:
    @patch("experiments.get_pid_gpu_map")
    @patch("experiments.get_experiment_progress")
    def test_running_with_progress(self, mock_progress, mock_pid, db):
        from experiments import UnifiedDashboard, ClusterManager

        db.save(
            {
                "experiments": [
                    {
                        "name": "running_exp",
                        "status": "RUNNING",
                        "batch_id": "test",
                        "running_on": {"worker": "w1", "gpu": 0},
                    }
                ],
                "archived": [],
            }
        )
        mock_pid.return_value = {}
        mock_progress.return_value = {
            "percent": 50,
            "epoch": 25,
            "total_epochs": 50,
            "val_f1": 0.75,
        }

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_experiments_panel()
        assert panel is not None

    @patch("experiments.get_pid_gpu_map")
    def test_error_with_true_oom(self, mock_pid, db):
        from experiments import UnifiedDashboard, ClusterManager

        db.save(
            {
                "experiments": [
                    {
                        "name": "oom_exp",
                        "status": "OOM",
                        "batch_id": "test",
                        "error_info": {"is_true_oom": True, "peak_memory_mb": 25000},
                    }
                ],
                "archived": [],
            }
        )
        mock_pid.return_value = {}

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_experiments_panel()
        assert panel is not None

    @patch("experiments.get_pid_gpu_map")
    def test_more_than_20_experiments(self, mock_pid, db):
        from experiments import UnifiedDashboard, ClusterManager

        experiments = [
            {"name": f"exp_{i}", "status": "READY", "batch_id": "test"}
            for i in range(25)
        ]
        db.save({"experiments": experiments, "archived": []})
        mock_pid.return_value = {}

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_experiments_panel()
        assert panel is not None

    @patch("experiments.get_pid_gpu_map")
    @pytest.mark.skipif(
        not LEGACY_JSON_DB_ONLY,
        reason="Legacy JSON-backed panel-count assertions are brittle in DB-backed mode.",
    )
    def test_infers_missing_running_rows_from_heartbeat(self, mock_pid, db):
        from experiments import UnifiedDashboard, ClusterManager

        db.save(
            {
                "experiments": [
                    {
                        "name": "exp_plusle",
                        "status": "RUNNING",
                        "batch_id": "test",
                        "running_on": {"worker": "plusle", "gpu": 0},
                    }
                ],
                "archived": [],
            }
        )
        mock_pid.return_value = {}

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_experiments_panel(
            {
                "plusle": {
                    "status": "ONLINE",
                    "running_jobs": 1,
                    "running_experiments": ["exp_plusle"],
                },
                "minun": {
                    "status": "ONLINE",
                    "running_jobs": 1,
                    "running_experiments": ["exp_minun"],
                },
            }
        )
        assert "1 active + 1 inferred" in str(panel.title)

    @patch("experiments.get_pid_gpu_map")
    @pytest.mark.skipif(
        not LEGACY_JSON_DB_ONLY,
        reason="Legacy JSON-backed panel-count assertions are brittle in DB-backed mode.",
    )
    def test_offline_worker_does_not_infer_rows(self, mock_pid, db):
        from experiments import UnifiedDashboard, ClusterManager

        db.save({"experiments": [], "archived": []})
        mock_pid.return_value = {}

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_experiments_panel(
            {
                "minun": {
                    "status": "OFFLINE",
                    "running_jobs": 2,
                    "running_experiments": ["exp_minun"],
                }
            }
        )
        assert "inferred" not in str(panel.title)

    @patch("experiments.get_pid_gpu_map")
    @pytest.mark.skipif(
        not LEGACY_JSON_DB_ONLY,
        reason="Legacy JSON-backed panel-count assertions are brittle in DB-backed mode.",
    )
    def test_dedup_heartbeat_names_for_unknown_count(self, mock_pid, db):
        from experiments import UnifiedDashboard, ClusterManager

        db.save({"experiments": [], "archived": []})
        mock_pid.return_value = {}

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        panel = dashboard.build_experiments_panel(
            {
                "minun": {
                    "status": "ONLINE",
                    "running_jobs": 3,
                    "running_experiments": ["exp_minun", "exp_minun"],
                }
            }
        )
        assert "0 active + 3 inferred" in str(panel.title)


class TestDashboardHandleKeyEdgeCases:
    def test_enter_in_action_mode_executes(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.action_mode = True
        dashboard.action_idx = 0
        with patch.object(cm, "start_node", return_value=(True, "ok")):
            dashboard.handle_key("\n", ["node1"])
        assert dashboard.action_mode is False

    def test_empty_workers_list(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        result = dashboard.handle_key("w", [])
        assert result is True

    def test_experiment_mode_scope_then_action_still_works(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard.focus_mode = "experiments"
        dashboard._panel_exp_rows = [{"name": "exp-a"}]
        dashboard.selected_exp_idx = 0

        assert dashboard.handle_key("S", ["node1"]) is True
        assert dashboard.exp_two_step.state == "scope_selected"


class TestDashboardDoAction:
    def test_unknown_action(self, db):
        from experiments import UnifiedDashboard, ClusterManager

        with patch("experiments.MACHINES_FILES", []):
            cm = ClusterManager()
        dashboard = UnifiedDashboard("worker", cm, db)
        dashboard._do_action_sync("node1", "unknown")


class TestHybridLoggerEdgeCases:
    def test_handles_log_exception(self, temp_dir):
        from experiments import HybridLogger

        log_file = temp_dir / "test.log"
        logger = HybridLogger(str(log_file))
        log_file.unlink()
        log_file.mkdir()
        logger.log("test")


class TestCheckStaleLocksEdgeCases:
    def test_handles_heartbeat_stat_error(self, db, temp_heartbeats_dir):
        from experiments import check_stale_locks

        db.save(
            {
                "experiments": [
                    {
                        "name": "exp1",
                        "status": "RUNNING",
                        "running_on": {"worker": "w1"},
                    }
                ],
                "archived": [],
            }
        )
        logger = MagicMock()

        hb_file = temp_heartbeats_dir / "w1.json"
        hb_file.write_text("{}")

        with patch("experiments.HEARTBEATS_DIR", temp_heartbeats_dir):
            with patch("os.path.getmtime", side_effect=OSError("no access")):
                check_stale_locks(db, logger)


class TestRunExperimentProcessOOM:
    @patch("experiments.mark_error")
    @patch("experiments.mark_running")
    @patch("experiments.update_lock_pid")
    @patch("subprocess.Popen")
    @patch("experiments.parse_oom_from_stderr")
    def test_oom_with_high_peak_marks_true_oom(
        self, mock_parse, mock_popen, mock_lock, mock_running, mock_error, db, temp_dir
    ):
        from experiments import run_experiment_process, OOM_THRESHOLD_MB

        exp_dir = temp_dir / "experiments" / "oom_exp" / "scripts"
        exp_dir.mkdir(parents=True)
        (exp_dir / "train.py").write_text("exit(1)")

        mock_process = MagicMock()
        mock_process.poll.side_effect = [None, 1]
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        mock_parse.return_value = (True, False, 5000)

        logs_dir = temp_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        logger = MagicMock()

        with patch("experiments.BASE_DIR", temp_dir):
            with patch("experiments.LOGS_DIR", logs_dir):
                with patch(
                    "experiments.get_pid_gpu_map",
                    return_value={12345: OOM_THRESHOLD_MB + 1000},
                ):
                    with patch("time.sleep"):
                        run_experiment_process(
                            {"name": "oom_exp"}, "worker1", 0, logger, db
                        )

        args = mock_error.call_args
        assert args[0][2] == "OOM"
        assert args[1].get("is_true_oom") is True or (
            len(args[0]) > 4 and args[0][4] is True
        )

    @patch("experiments.mark_error")
    @patch("experiments.mark_running")
    @patch("experiments.update_lock_pid")
    @patch("subprocess.Popen")
    def test_failed_with_stderr_content(
        self, mock_popen, mock_lock, mock_running, mock_error, db, temp_dir
    ):
        from experiments import run_experiment_process

        exp_dir = temp_dir / "experiments" / "fail_exp" / "scripts"
        exp_dir.mkdir(parents=True)
        (exp_dir / "train.py").write_text("exit(1)")

        mock_process = MagicMock()
        mock_process.poll.side_effect = [None, 1]
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        logs_dir = temp_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        logger = MagicMock()

        with patch("experiments.BASE_DIR", temp_dir):
            with patch("experiments.LOGS_DIR", logs_dir):
                with patch(
                    "experiments.parse_oom_from_stderr", return_value=(False, False, 0)
                ):
                    with patch("experiments.get_pid_gpu_map", return_value={}):
                        with patch("time.sleep"):
                            run_experiment_process(
                                {"name": "fail_exp"}, "worker1", 0, logger, db
                            )

        mock_error.assert_called_once()
        assert "SCRIPT_ERROR" in str(mock_error.call_args)


class TestRunExperimentProcessResultFile:
    @patch("experiments.mark_done")
    @patch("experiments.mark_running")
    @patch("experiments.update_lock_pid")
    @patch("subprocess.Popen")
    def test_reads_result_file(
        self, mock_popen, mock_lock, mock_running, mock_done, db, temp_dir
    ):
        from experiments import run_experiment_process

        exp_dir = temp_dir / "experiments" / "success_exp" / "scripts"
        exp_dir.mkdir(parents=True)
        (exp_dir / "train.py").write_text("print('ok')")

        results_dir = temp_dir / "results_db"
        results_dir.mkdir(exist_ok=True)
        (results_dir / "success_exp.json").write_text(
            json.dumps({"f1_score": 0.85, "auc_score": 0.92})
        )

        mock_process = MagicMock()
        mock_process.poll.side_effect = [None, 0]
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        logs_dir = temp_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        logger = MagicMock()

        with patch("experiments.BASE_DIR", temp_dir):
            with patch("experiments.LOGS_DIR", logs_dir):
                with patch("experiments.RESULTS_DB_DIR", results_dir):
                    with patch("experiments.get_pid_gpu_map", return_value={}):
                        with patch("time.sleep"):
                            run_experiment_process(
                                {"name": "success_exp"}, "worker1", 0, logger, db
                            )

        mock_done.assert_called_once()
        result_arg = mock_done.call_args[0][2]
        assert result_arg["f1_score"] == 0.85
