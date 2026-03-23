#!/usr/bin/env python3

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_registry import DBExperimentsDB

from unittest.mock import MagicMock, patch


def _mocked_db_context():
    cursor = MagicMock(name="cursor")

    cursor_cm = MagicMock(name="cursor_cm")
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = False

    conn = MagicMock(name="conn")
    conn.cursor.return_value = cursor_cm

    conn_cm = MagicMock(name="conn_cm")
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    return conn_cm, conn, cursor


def test_kill_experiment_success_transitions_to_needs_rerun():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchone.side_effect = [(42,)]
    cur.rowcount = 1

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db.clear_run_id = MagicMock()
        db._sync_snapshot = MagicMock()

        ok = db.kill_experiment("exp_a")

    assert ok is True
    assert cur.execute.call_count == 2
    assert "MAX(display_order)" in cur.execute.call_args_list[0].args[0]
    update_sql, update_params = cur.execute.call_args_list[1].args
    assert "status = 'NEEDS_RERUN'" in update_sql
    assert "run_id = NULL" in update_sql
    assert "worker_id = NULL" in update_sql
    assert "display_order = %s" in update_sql
    assert update_params == (42, "exp_a")
    db.clear_run_id.assert_called_once_with("exp_a")
    db._sync_snapshot.assert_called_once()


def test_kill_experiment_no_updated_row_returns_false():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchone.side_effect = [(7,)]
    cur.rowcount = 0

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db.clear_run_id = MagicMock()
        db._sync_snapshot = MagicMock()

        ok = db.kill_experiment("missing")

    assert ok is False
    db.clear_run_id.assert_called_once_with("missing")
    db._sync_snapshot.assert_called_once()


def test_freeze_experiment_success_updates_state_and_clears_run_id():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.rowcount = 1

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db.clear_run_id = MagicMock()
        db._sync_snapshot = MagicMock()

        ok = db.freeze_experiment("exp_f")

    assert ok is True
    freeze_sql, freeze_params = cur.execute.call_args.args
    assert "status = 'NEEDS_RERUN'" in freeze_sql
    assert "error_type = 'MANUAL_FREEZE'" in freeze_sql
    assert "run_id = NULL" in freeze_sql
    assert freeze_params == ("exp_f",)
    db.clear_run_id.assert_called_once_with("exp_f")
    db._sync_snapshot.assert_called_once()


def test_freeze_experiment_no_matching_row_does_not_clear_run_id():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.rowcount = 0

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db.clear_run_id = MagicMock()
        db._sync_snapshot = MagicMock()

        ok = db.freeze_experiment("ghost")

    assert ok is False
    db.clear_run_id.assert_not_called()
    db._sync_snapshot.assert_called_once()


def test_rerun_experiment_success_resets_error_and_running_fields():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchone.side_effect = [(-5,)]
    cur.rowcount = 1

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db.clear_run_id = MagicMock()
        db._sync_snapshot = MagicMock()

        ok = db.rerun_experiment("exp_r")

    assert ok is True
    assert cur.execute.call_count == 2
    assert "MIN(display_order)" in cur.execute.call_args_list[0].args[0]
    rerun_sql, rerun_params = cur.execute.call_args_list[1].args
    assert "status = 'NEEDS_RERUN'" in rerun_sql
    assert "error_type = NULL" in rerun_sql
    assert "error_message = NULL" in rerun_sql
    assert "worker_id = NULL" in rerun_sql
    assert "display_order = %s" in rerun_sql
    assert rerun_params == (-5, "exp_r")
    db.clear_run_id.assert_called_once_with("exp_r")
    db._sync_snapshot.assert_called_once()


def test_reset_failed_experiments_returns_rowcount_and_syncs_snapshot():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.rowcount = 3

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._sync_snapshot = MagicMock()

        count = db.reset_failed_experiments()

    assert count == 3
    reset_sql = cur.execute.call_args.args[0]
    assert "SET status = 'NEEDS_RERUN'" in reset_sql
    assert "error_type = NULL" in reset_sql
    assert "error_message = NULL" in reset_sql
    assert "retry_count = 0" in reset_sql
    db._sync_snapshot.assert_called_once()


def test_move_experiment_up_swaps_display_order_with_previous_in_frame():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchall.return_value = [
        (10, 100, None),
        (20, 200, None),
        (30, 300, None),
    ]

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._get_id_by_name = MagicMock(return_value=20)
        db._sync_snapshot = MagicMock()

        ok = db.move_experiment("exp_mid", "up")

    assert ok is True
    assert cur.execute.call_count == 3
    first_update = cur.execute.call_args_list[1].args
    second_update = cur.execute.call_args_list[2].args
    assert first_update[1] == (100, 20)
    assert second_update[1] == (200, 10)
    db._sync_snapshot.assert_called_once()


def test_move_experiment_down_at_boundary_returns_false_without_updates():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchall.return_value = [
        (1, 10, "frame-a"),
        (2, 20, "frame-a"),
    ]

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._get_id_by_name = MagicMock(return_value=2)
        db._sync_snapshot = MagicMock()

        ok = db.move_experiment("exp_last", "down")

    assert ok is False
    assert cur.execute.call_count == 1
    db._sync_snapshot.assert_not_called()


def test_move_experiment_not_found_returns_false():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchall.return_value = [(1, 10, None), (2, 20, None)]

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._get_id_by_name = MagicMock(return_value=None)
        db._sync_snapshot = MagicMock()

        ok = db.move_experiment("ghost", "up")

    assert ok is False
    assert cur.execute.call_count == 1
    db._sync_snapshot.assert_not_called()


def test_assign_experiment_worker_sets_preferred_worker():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchone.return_value = ("NEEDS_RERUN", None)

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._sync_snapshot = MagicMock()

        ok = db.assign_experiment_worker("exp_assign", "worker-2")

    assert ok is True
    assert cur.execute.call_count == 2
    update_sql, update_params = cur.execute.call_args_list[1].args
    assert "SET preferred_worker = %s" in update_sql
    assert update_params == ("worker-2", "exp_assign")
    db._sync_snapshot.assert_called_once()


def test_assign_experiment_worker_blank_value_clears_preferred_worker():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchone.return_value = ("NEEDS_RERUN", None)

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._sync_snapshot = MagicMock()

        ok = db.assign_experiment_worker("exp_assign", "   ")

    assert ok is True
    _, update_params = cur.execute.call_args_list[1].args
    assert update_params == (None, "exp_assign")
    db._sync_snapshot.assert_called_once()


def test_assign_experiment_worker_running_on_other_worker_triggers_reset():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchone.return_value = ("RUNNING", "worker-1")

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._sync_snapshot = MagicMock()

        ok = db.assign_experiment_worker("exp_run", "worker-2")

    assert ok is True
    assert cur.execute.call_count == 3
    third_sql, third_params = cur.execute.call_args_list[2].args
    assert "SET status = 'NEEDS_RERUN'" in third_sql
    assert "WHERE name = %s AND status = 'RUNNING'" in third_sql
    assert third_params == ("exp_run",)
    db._sync_snapshot.assert_called_once()


def test_assign_experiment_worker_missing_experiment_returns_false():
    conn_cm, _conn, cur = _mocked_db_context()
    cur.fetchone.return_value = None

    with patch("db_registry.get_conn", return_value=conn_cm):
        db = DBExperimentsDB("fake-dsn")
        db._sync_snapshot = MagicMock()

        ok = db.assign_experiment_worker("ghost", "worker-2")

    assert ok is False
    assert cur.execute.call_count == 1
    db._sync_snapshot.assert_not_called()
