import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_registry import DBExperimentsDB

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


def _mock_conn_and_cursor():
    conn = MagicMock(name="conn")
    cur = MagicMock(name="cursor")

    conn_cm = MagicMock(name="conn_cm")
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = None

    cur_cm = MagicMock(name="cur_cm")
    cur_cm.__enter__.return_value = cur
    cur_cm.__exit__.return_value = None
    conn.cursor.return_value = cur_cm

    return conn_cm, conn, cur


@patch("db_registry.get_conn")
@patch("db_registry._load_worker_whitelist", return_value=["w1"])
def test_update_heartbeat_upsert_with_payload(mock_whitelist, mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    db = DBExperimentsDB(dsn="postgresql://fake")

    db.update_heartbeat(
        worker_id="w1",
        pid=4242,
        running_jobs=2,
        running_experiments=["exp_a", "exp_b"],
        gpu_info=[{"id": 0, "util": 51}],
        cpu_info={"usage": 72.5},
    )

    cur.execute.assert_called_once()
    sql, params = cur.execute.call_args.args
    assert "update_heartbeat" in sql
    assert params[0] == "w1"
    assert params[1] == 4242
    assert params[2] == 2
    assert params[3] == ["exp_a", "exp_b"]
    assert params[4] == '[{"id": 0, "util": 51}]'
    assert params[5] == '{"usage": 72.5}'


@patch("db_registry.get_conn")
@patch("db_registry._load_worker_whitelist", return_value=["w2"])
def test_update_heartbeat_defaults_gpu_cpu_to_empty_json(mock_whitelist, mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    db = DBExperimentsDB(dsn="postgresql://fake")

    db.update_heartbeat(
        worker_id="w2",
        pid=99,
        running_jobs=0,
        running_experiments=[],
        gpu_info=None,
        cpu_info=None,
    )

    _, params = cur.execute.call_args.args
    assert params[4] == "[]"
    assert params[5] == "{}"


@patch("db_registry.get_conn", side_effect=RuntimeError("db down"))
@patch("db_registry._load_worker_whitelist", return_value=["w"])
def test_update_heartbeat_handles_db_exception(mock_whitelist, mock_get_conn):
    db = DBExperimentsDB(dsn="postgresql://fake")
    ok = db.update_heartbeat("w", 1, 1, ["exp"])
    assert ok is False
    assert mock_get_conn.call_count == 2


@patch("db_registry.get_conn")
@patch("db_registry._load_worker_whitelist", return_value=["w1", "w2"])
def test_update_heartbeat_ignores_worker_not_in_whitelist(
    mock_whitelist, mock_get_conn
):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    db = DBExperimentsDB(dsn="postgresql://fake")

    ok = db.update_heartbeat("rogue-worker", 1, 0, [])

    assert ok is True
    cur.execute.assert_not_called()
    assert "not in machines whitelist" in db.get_last_heartbeat_error()
    mock_whitelist.assert_called()


@patch("db_registry.get_conn")
def test_get_cluster_heartbeats_returns_expected_dict_shape(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    now = datetime.now(timezone.utc)
    cur.fetchall.return_value = [
        {
            "worker_id": "w1",
            "last_seen": now - timedelta(seconds=5),
            "pid": 123,
            "running_jobs": 2,
            "running_experiments": ["exp1"],
            "gpu_info": [{"id": 0}],
            "cpu_info": {"cores": 16, "_gpu_probe_error": "nvml unavailable"},
        }
    ]
    db = DBExperimentsDB(dsn="postgresql://fake")

    hb = db.get_cluster_heartbeats()

    assert set(hb.keys()) == {"w1"}
    row = hb["w1"]
    assert row["worker_id"] == "w1"
    assert row["pid"] == 123
    assert row["running_jobs"] == 2
    assert row["running_experiments"] == ["exp1"]
    assert row["gpus"] == [{"id": 0}]
    assert row["cpu"] == {"cores": 16, "_gpu_probe_error": "nvml unavailable"}
    assert row["gpu_probe_error"] == "nvml unavailable"
    assert row["timestamp"]
    assert row["last_seen_sec"] == pytest.approx(5, abs=2)


@patch("db_registry.get_conn")
@patch("db_registry._load_worker_whitelist", return_value=["w1"])
def test_get_filtered_cluster_heartbeats_respects_whitelist(
    mock_whitelist, mock_get_conn
):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    now = datetime.now(timezone.utc)
    cur.fetchall.return_value = [
        {
            "worker_id": "w1",
            "last_seen": now - timedelta(seconds=5),
            "pid": 123,
            "running_jobs": 1,
            "running_experiments": ["exp1"],
            "gpu_info": [],
            "cpu_info": {},
        },
        {
            "worker_id": "oc-rerun4",
            "last_seen": now - timedelta(seconds=5),
            "pid": 456,
            "running_jobs": 1,
            "running_experiments": ["ghost"],
            "gpu_info": [],
            "cpu_info": {},
        },
    ]
    db = DBExperimentsDB(dsn="postgresql://fake")

    hb = db.get_filtered_cluster_heartbeats()

    assert set(hb.keys()) == {"w1"}
    mock_whitelist.assert_called()


@patch("db_registry.get_conn")
@patch("db_registry._load_worker_whitelist", return_value=["w1"])
def test_get_worker_heartbeat_fail_closed_for_unknown_worker(
    mock_whitelist, mock_get_conn
):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    now = datetime.now(timezone.utc)
    cur.fetchall.return_value = [
        {
            "worker_id": "oc-rerun4",
            "last_seen": now - timedelta(seconds=5),
            "pid": 456,
            "running_jobs": 1,
            "running_experiments": ["ghost"],
            "gpu_info": [],
            "cpu_info": {},
        }
    ]
    db = DBExperimentsDB(dsn="postgresql://fake")

    hb = db.get_worker_heartbeat("oc-rerun4")

    assert hb == {}
    mock_whitelist.assert_called()


@patch("db_registry.get_conn")
def test_cleanup_worker_heartbeats_deletes_unknown_rows(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    cur.rowcount = 3
    mock_get_conn.return_value = conn_cm
    db = DBExperimentsDB(dsn="postgresql://fake")

    deleted = db.cleanup_worker_heartbeats(["plusle", "minun"])

    assert deleted == 3
    sql, params = cur.execute.call_args.args
    assert "DELETE FROM exp_registry.worker_heartbeats" in sql
    assert params == (["minun", "plusle"],)


@patch("db_registry.get_conn")
def test_record_buddy_report_writes_insert(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    db = DBExperimentsDB(dsn="postgresql://fake")

    ok = db.record_buddy_report(
        reporter_id="watcher",
        target_id="SOTA",
        target_process_alive=True,
        target_db_reachable=False,
        target_gpu_ok=True,
    )

    assert ok is True
    assert cur.execute.call_count >= 3
    sql, params = cur.execute.call_args.args
    assert "INSERT INTO exp_registry.buddy_reports" in sql
    assert params == ("watcher", "SOTA", True, False, True)


@patch("db_registry.get_conn")
def test_get_latest_buddy_reports_returns_target_map(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    now = datetime.now(timezone.utc)
    cur.fetchall.return_value = [
        {
            "target_id": "SOTA",
            "reporter_id": "plusle",
            "target_process_alive": True,
            "target_db_reachable": False,
            "target_gpu_ok": True,
            "checked_at": now,
            "age_sec": 12.4,
        }
    ]
    db = DBExperimentsDB(dsn="postgresql://fake")

    reports = db.get_latest_buddy_reports(ttl_sec=90)

    assert set(reports.keys()) == {"SOTA"}
    row = reports["SOTA"]
    assert row["reporter_id"] == "plusle"
    assert row["target_process_alive"] is True
    assert row["target_db_reachable"] is False
    assert row["target_gpu_ok"] is True
    assert row["age_sec"] == pytest.approx(12.4)


@patch("db_registry.get_conn")
def test_check_stale_experiments_uses_buddy_guard_query(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    cur.fetchall.return_value = [("exp_a", "worker_a")]
    db = DBExperimentsDB(dsn="postgresql://fake")

    with patch.object(db, "_sync_snapshot") as sync_snapshot:
        stale = db.check_stale_experiments(
            stale_sec=120,
            caller_worker="caller",
            buddy_report_ttl_sec=90,
        )

    assert stale == [("exp_a", "worker_a")]
    sql, params = cur.execute.call_args.args
    assert "WITH stale_candidates AS" in sql
    assert params == ("caller", "caller", 120, 90)
    sync_snapshot.assert_called_once()


@patch("db_registry.get_conn")
def test_get_cluster_heartbeats_cpu_info_sanitized_and_null_last_seen(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    cur.fetchall.return_value = [
        {
            "worker_id": "w2",
            "last_seen": None,
            "pid": 11,
            "running_jobs": 0,
            "running_experiments": [],
            "gpu_info": [],
            "cpu_info": "not-a-dict",
        }
    ]
    db = DBExperimentsDB(dsn="postgresql://fake")

    hb = db.get_cluster_heartbeats()

    row = hb["w2"]
    assert row["cpu"] == {}
    assert row["gpu_probe_error"] == ""
    assert row["timestamp"] == ""
    assert row["last_seen_sec"] == 999999


@patch("db_registry.get_conn")
def test_get_cluster_heartbeats_supports_stale_classification_via_last_seen_sec(
    mock_get_conn,
):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    now = datetime.now(timezone.utc)
    cur.fetchall.return_value = [
        {
            "worker_id": "fresh",
            "last_seen": now - timedelta(seconds=10),
            "cpu_info": {},
        },
        {
            "worker_id": "stale",
            "last_seen": now - timedelta(seconds=300),
            "cpu_info": {},
        },
    ]
    db = DBExperimentsDB(dsn="postgresql://fake")

    hb = db.get_cluster_heartbeats()

    assert (hb["fresh"]["last_seen_sec"] > 120) is False
    assert (hb["stale"]["last_seen_sec"] > 120) is True


@patch("db_registry.get_conn", side_effect=RuntimeError("db down"))
def test_get_cluster_heartbeats_returns_empty_dict_on_error(mock_get_conn):
    db = DBExperimentsDB(dsn="postgresql://fake")
    assert db.get_cluster_heartbeats() == {}
    mock_get_conn.assert_called_once()


@patch("db_registry.get_conn")
def test_check_stale_experiments_returns_rows_and_syncs_snapshot(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    cur.fetchall.return_value = [("exp_a", "worker_a"), ("exp_b", "worker_b")]
    db = DBExperimentsDB(dsn="postgresql://fake")

    with patch.object(db, "_sync_snapshot") as sync_snapshot:
        stale = db.check_stale_experiments(stale_sec=90, caller_worker="caller")

    assert stale == [("exp_a", "worker_a"), ("exp_b", "worker_b")]
    sql, params = cur.execute.call_args.args
    assert "check_stale_experiments" in sql
    assert params == (90, "caller")
    sync_snapshot.assert_called_once()


@patch("db_registry.get_conn")
def test_check_stale_experiments_empty_result_does_not_sync(mock_get_conn):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    cur.fetchall.return_value = []
    db = DBExperimentsDB(dsn="postgresql://fake")

    with patch.object(db, "_sync_snapshot") as sync_snapshot:
        stale = db.check_stale_experiments()

    assert stale == []
    sync_snapshot.assert_not_called()


@patch("db_registry.get_conn", side_effect=RuntimeError("db down"))
def test_check_stale_experiments_returns_empty_on_error(mock_get_conn):
    db = DBExperimentsDB(dsn="postgresql://fake")
    assert db.check_stale_experiments() == []
    mock_get_conn.assert_called_once()


@patch("db_registry.os.kill")
@patch("db_registry.get_conn")
def test_check_zombie_processes_resets_dead_processes_and_returns_zombies(
    mock_get_conn, mock_kill
):
    conn_cm, conn, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    cur.fetchall.return_value = [
        ("exp_alive", 1001, "run-1"),
        ("exp_dead", 1002, "run-2"),
        ("exp_perm", 1003, "run-3"),
        ("exp_skip", 1004, "run-4"),
        ("exp_no_pid", None, "run-5"),
    ]

    def _kill_side_effect(pid, sig):
        if pid == 1002:
            raise ProcessLookupError
        if pid == 1003:
            raise PermissionError
        return None

    mock_kill.side_effect = _kill_side_effect
    db = DBExperimentsDB(dsn="postgresql://fake")

    with patch.object(db, "_sync_snapshot") as sync_snapshot:
        zombies = db.check_zombie_processes("worker-a", exclude_names={"exp_skip"})

    assert zombies == [("exp_dead", 1002)]
    conn.commit.assert_called_once()
    sync_snapshot.assert_called_once()

    assert mock_kill.call_count == 3
    mock_kill.assert_any_call(1001, 0)
    mock_kill.assert_any_call(1002, 0)
    mock_kill.assert_any_call(1003, 0)

    update_calls = [
        c
        for c in cur.execute.call_args_list
        if "UPDATE exp_registry.experiments" in c.args[0]
    ]
    assert len(update_calls) == 1
    _, update_params = update_calls[0].args
    assert update_params[1] == "exp_dead"
    assert update_params[2] == "run-2"
    assert "Process 1002 died unexpectedly" in update_params[0]


@patch("db_registry.os.kill", side_effect=RuntimeError("unexpected os error"))
@patch("db_registry.get_conn")
def test_check_zombie_processes_handles_kill_exception_and_returns_empty(
    mock_get_conn, mock_kill
):
    conn_cm, _, cur = _mock_conn_and_cursor()
    mock_get_conn.return_value = conn_cm
    cur.fetchall.return_value = [("exp", 2222, "run-x")]
    db = DBExperimentsDB(dsn="postgresql://fake")

    with patch.object(db, "_sync_snapshot") as sync_snapshot:
        zombies = db.check_zombie_processes("worker-a")

    assert zombies == []
    sync_snapshot.assert_not_called()
    mock_kill.assert_called_once_with(2222, 0)


@patch("db_registry.get_conn", side_effect=RuntimeError("db down"))
def test_check_zombie_processes_returns_empty_when_db_fails(mock_get_conn):
    db = DBExperimentsDB(dsn="postgresql://fake")
    assert db.check_zombie_processes("worker-a") == []
    mock_get_conn.assert_called_once()
