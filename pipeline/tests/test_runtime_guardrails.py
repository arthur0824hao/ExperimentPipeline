#!/usr/bin/env python3

import signal
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from cluster import ClusterManager
from db_registry import DBExperimentsDB
from experiments import _is_management_only_worker


def test_watch_mode_parser_has_no_watcher_subcommand():
    src = Path(__file__).resolve().parents[1] / "experiments.py"
    text = src.read_text(encoding="utf-8")
    assert 'add_parser("watcher"' not in text
    assert "watch_mode.lock" not in text


def test_is_management_only_worker_from_machine_config():
    cluster_mgr = MagicMock()
    cluster_mgr.machines = {
        "nv960": {"max_gpus": 0},
        "gpu-node": {"max_gpus": 4},
    }

    assert _is_management_only_worker("nv960", cluster_mgr) is True
    assert _is_management_only_worker("gpu-node", cluster_mgr) is False
    assert _is_management_only_worker("missing", cluster_mgr) is False


def test_cluster_start_node_skips_destructive_restart_when_alive():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }

    with patch.object(mgr, "_is_remote_runner_alive", return_value=True):
        with patch.object(mgr, "_is_heartbeat_fresh", return_value=(True, 10)):
            ok, msg = mgr.start_node("node-a")

    assert ok is True
    assert "Already running" in msg


def test_cluster_restart_node_remains_force_restart_path():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }

    proc = MagicMock()
    proc.returncode = 0
    proc.stderr = ""
    with patch("cluster.subprocess.run", return_value=proc) as run_mock:
        ok, msg = mgr.restart_node("node-a")

    assert ok is True
    assert "Started node-a" in msg
    cmd_args = run_mock.call_args.args[0]
    assert any("kill-session -t exp_runner" in str(x) for x in cmd_args)


def test_cluster_stop_node_uses_self_safe_kill_patterns():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }

    proc = MagicMock()
    proc.returncode = 0
    proc.stderr = ""
    proc.stdout = ""
    with patch("cluster.subprocess.run", return_value=proc) as run_mock:
        ok, msg = mgr.stop_node("node-a")

    assert ok is True
    assert "Stopped node-a" in msg
    cmd_args = run_mock.call_args.args[0]
    remote_cmd = str(cmd_args[-1])
    assert "[e]xperiments\\.py --worker_id node-a" in remote_cmd
    assert "exit 0" in remote_cmd


class _FallbackCursor:
    def __init__(self):
        self._execute_count = 0
        self._rows = [
            (
                "EXP_A",
                "NEEDS_RERUN",
                "worker-1",
                {"role": "normal"},
                10,
                "",
                1234,
                datetime(2026, 3, 24, 0, 0, 0),
            )
        ]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self._execute_count += 1
        if self._execute_count == 1 and "claimed_at" in query:
            raise Exception('column "claimed_at" does not exist')

    def fetchall(self):
        return self._rows


def test_load_all_for_panel_falls_back_when_claimed_at_missing():
    cursor = _FallbackCursor()
    conn = MagicMock()
    conn.cursor.return_value = cursor

    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        rows = db.load_all_for_panel()

    assert len(rows) == 1
    assert rows[0]["name"] == "EXP_A"
    assert rows[0]["started_at"].startswith("2026-03-24")
    assert "claimed_at" not in rows[0]


def test_cluster_start_node_returns_ok_for_fresh_heartbeat():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }
    
    with patch.object(mgr, "_is_remote_runner_alive", return_value=True):
        with patch.object(mgr, "_is_heartbeat_fresh", return_value=(True, 30)):
            ok, msg = mgr.start_node("node-a")

    assert ok is True
    assert "Already running" in msg
    assert "fresh" in msg.lower()


def test_cluster_start_node_restarts_when_heartbeat_stale():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }
    proc = MagicMock()
    proc.returncode = 0
    proc.stderr = ""

    with patch.object(mgr, "_is_remote_runner_alive", return_value=True):
        with patch.object(mgr, "_is_heartbeat_fresh", return_value=(False, 150)):
            with patch.object(mgr, "_wait_for_heartbeat_resume", return_value=(True, 3)):
                with patch("cluster.subprocess.run", return_value=proc) as run_mock:
                    ok, msg = mgr.start_node("node-a")

    assert ok is True
    assert "Started node-a" in msg
    cmd_args = run_mock.call_args.args[0]
    remote_cmd = str(cmd_args[-1])
    assert "Graceful shutdown requested for worker PIDs" in remote_cmd
    assert "kill-session -t exp_runner" in remote_cmd
    assert ".runner_launch_node-a.sh" in remote_cmd
    assert "cat > /tmp/.runner_launch_node-a.sh" in remote_cmd
    assert "nohup python experiments.py --worker_id node-a" in remote_cmd
    assert "Runner already alive" not in str(cmd_args[-1])


def test_cluster_start_node_starts_fresh_when_no_process():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }
    proc = MagicMock()
    proc.returncode = 0
    proc.stderr = ""

    with patch.object(mgr, "_is_remote_runner_alive", return_value=False):
        with patch.object(mgr, "_wait_for_heartbeat_resume", return_value=(True, 2)):
            with patch("cluster.subprocess.run", return_value=proc) as run_mock:
                ok, msg = mgr.start_node("node-a")

    assert ok is True
    assert "Started node-a" in msg
    cmd_args = run_mock.call_args.args[0]
    assert any("new-session" in str(x) for x in cmd_args)


def test_cluster_start_node_restarts_when_pid_alive_but_tmux_missing_and_stale():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }
    proc = MagicMock()
    proc.returncode = 0
    proc.stderr = ""

    with patch.object(mgr, "_is_remote_runner_alive", return_value=False):
        with patch.object(mgr, "_is_remote_worker_pid_alive", return_value=True):
            with patch.object(mgr, "_is_heartbeat_fresh", return_value=(False, 160)):
                with patch.object(mgr, "_wait_for_heartbeat_resume", return_value=(True, 2)):
                    with patch("cluster.subprocess.run", return_value=proc) as run_mock:
                        ok, msg = mgr.start_node("node-a")

    assert ok is True
    assert "graceful shutdown attempt" in msg
    remote_cmd = str(run_mock.call_args.args[-1])
    assert "Graceful shutdown requested for worker PIDs" in remote_cmd
    assert ".runner_launch_node-a.sh" in remote_cmd


def test_cluster_start_node_reports_failure_if_heartbeat_not_resumed():
    mgr = ClusterManager()
    mgr.machines = {
        "node-a": {
            "host": "node-a.local",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
        }
    }

    db = MagicMock()
    proc = MagicMock()
    proc.returncode = 0
    proc.stderr = ""

    with patch.object(mgr, "_is_remote_runner_alive", return_value=False):
        with patch.object(mgr, "_wait_for_heartbeat_resume", return_value=(False, None)):
            with patch("cluster.subprocess.run", return_value=proc):
                ok, msg = mgr.start_node("node-a", db=db)

    assert ok is False
    assert "heartbeat not fresh" in msg


def test_cluster_start_node_refuses_local_management_only_machine():
    mgr = ClusterManager()
    mgr.machines = {
        "nv960": {
            "host": "nv960",
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
            "max_gpus": 0,
        }
    }

    with patch("cluster.platform.node", return_value="nv960"):
        ok, msg = mgr.start_node("nv960")

    assert ok is False
    assert "management-only" in msg


def test_cluster_start_node_uses_machine_host_and_port_for_sota():
    mgr = ClusterManager()
    mgr.machines = {
        "SOTA": {
            "host": "140.122.185.39",
            "ssh_port": 3900,
            "tmux_session": "exp_runner",
            "work_dir": "/tmp",
            "max_gpus": 4,
        }
    }
    proc = MagicMock()
    proc.returncode = 0
    proc.stderr = ""

    with patch.object(mgr, "_is_remote_runner_alive", return_value=False):
        with patch.object(mgr, "_wait_for_heartbeat_resume", return_value=(True, 2)):
            with patch("cluster.subprocess.run", return_value=proc) as run_mock:
                ok, _ = mgr.start_node("SOTA")

    assert ok is True
    cmd_args = run_mock.call_args.args[0]
    assert "140.122.185.39" in cmd_args
    assert "3900" in cmd_args


def test_cluster_enable_disable_node_via_cli_commands():
    from experiments import _handle_cli_command

    cluster_mgr = MagicMock()
    cluster_mgr.machines = {"Bigram": {"max_gpus": 1}}
    db = MagicMock()
    db.disable_worker.return_value = True
    db.enable_worker.return_value = True

    disable_args = SimpleNamespace(
        command="cluster",
        cluster_cmd="disable",
        node="Bigram",
        force_restart=False,
        all=False,
    )
    enable_args = SimpleNamespace(
        command="cluster",
        cluster_cmd="enable",
        node="Bigram",
        force_restart=False,
        all=False,
    )

    disable_rc = _handle_cli_command(disable_args, cluster_mgr, db)
    enable_rc = _handle_cli_command(enable_args, cluster_mgr, db)

    assert disable_rc == 0
    assert enable_rc == 0
    db.disable_worker.assert_called_once_with("Bigram")
    db.enable_worker.assert_called_once_with("Bigram")


def test_cluster_enable_all_skips_management_only_nodes():
    from experiments import _handle_cli_command

    cluster_mgr = MagicMock()
    cluster_mgr.machines = {
        "nv960": {"max_gpus": 0},
        "Bigram": {"max_gpus": 1},
        "SOTA": {"max_gpus": 4},
    }
    db = MagicMock()
    db.enable_worker.return_value = True

    args = SimpleNamespace(
        command="cluster",
        cluster_cmd="enable",
        node=None,
        force_restart=False,
        all=True,
    )

    rc = _handle_cli_command(args, cluster_mgr, db)

    assert rc == 0
    db.enable_worker.assert_any_call("Bigram")
    db.enable_worker.assert_any_call("SOTA")
    assert db.enable_worker.call_count == 2


def test_watch_loop_has_no_auto_wake_call_in_watch_branch():
    src = Path(__file__).resolve().parents[1] / "experiments.py"
    text = src.read_text(encoding="utf-8")
    watch_anchor = "if args.watch:"
    assert watch_anchor in text
    watch_block = text.split(watch_anchor, 1)[1].split("continue", 1)[0]
    assert "auto_wake_offline_nodes(" not in watch_block
    assert "process_remote_termination_requests(" not in watch_block
    assert "maybe_archive_completed(" not in watch_block


def test_runner_ignores_sighup_in_signal_handler():
    src = Path(__file__).resolve().parents[1] / "experiments.py"
    text = src.read_text(encoding="utf-8")
    marker = "def _signal_handler(signum, frame):"
    assert marker in text
    handler_block = text.split(marker, 1)[1].split("def run_and_cleanup", 1)[0]
    assert "if signum == signal.SIGHUP:" in handler_block
    assert "Ignored by runner" in handler_block


def test_cli_cluster_start_still_calls_cluster_manager_start_node():
    from experiments import _handle_cli_command

    args = SimpleNamespace(
        command="cluster", cluster_cmd="start", node="node-a", force_restart=False
    )
    cluster_mgr = MagicMock()
    cluster_mgr.start_node.return_value = (True, "ok")
    db = MagicMock()

    rc = _handle_cli_command(args, cluster_mgr, db)

    assert rc == 0
    cluster_mgr.start_node.assert_called_once_with("node-a", force_restart=False, db=db)


def test_cli_cluster_restart_passes_db_to_cluster_manager():
    from experiments import _handle_cli_command

    args = SimpleNamespace(
        command="cluster", cluster_cmd="restart", node="node-a", force_restart=False
    )
    cluster_mgr = MagicMock()
    cluster_mgr.restart_node.return_value = (True, "ok")
    db = MagicMock()

    rc = _handle_cli_command(args, cluster_mgr, db)

    assert rc == 0
    cluster_mgr.restart_node.assert_called_once_with("node-a", db=db)
