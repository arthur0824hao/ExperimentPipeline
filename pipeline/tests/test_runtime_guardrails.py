#!/usr/bin/env python3

from datetime import datetime
from unittest.mock import MagicMock, patch

from cluster import ClusterManager
from db_registry import DBExperimentsDB
from experiments import WatchModeLock


def test_watch_mode_lock_rejects_duplicate_holder(tmp_path):
    lock_file = tmp_path / "watch_mode.lock"
    lock_a = WatchModeLock("nv960", lock_path=lock_file)
    ok_a, msg_a = lock_a.acquire()
    assert ok_a is True
    assert msg_a == "acquired"

    lock_b = WatchModeLock("nv960", lock_path=lock_file)
    ok_b, msg_b = lock_b.acquire()
    assert ok_b is False
    assert "already held" in msg_b

    lock_a.release()


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
