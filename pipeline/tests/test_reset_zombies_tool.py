#!/usr/bin/env python3

from contextlib import contextmanager
from pathlib import Path

from tools import reset_zombies


class _FakeCursor:
    def __init__(self, zombie_rows, stale_rows, summary_row):
        self._zombie_rows = zombie_rows
        self._stale_rows = stale_rows
        self._summary_row = summary_row
        self._fetchall_count = 0
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self.executed.append((str(query), params))

    def fetchall(self):
        self._fetchall_count += 1
        if self._fetchall_count == 1:
            return self._zombie_rows
        return self._stale_rows

    def fetchone(self):
        return self._summary_row


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _fake_conn_cm(cursor):
    @contextmanager
    def _cm(_dsn):
        yield _FakeConn(cursor)

    return _cm


def test_cleanup_updates_zombies_and_stale_errors(monkeypatch, tmp_path):
    zombie_rows = [
        ("EXP_P3_GS_H56_ORIGIN_V1", "RUNNING", "run-a", None),
        ("EXP_P3_GS_H56_SENIOR10_V1", "RUNNING", "run-b", None),
        ("EXP_P3_GS_H56_YOUR32_V1", "NEEDS_RERUN", None, None),
    ]
    stale_rows = [
        ("EXP_P1_GS_H56_SENIOR10_V1", "NEEDS_RERUN", None, "SCRIPT_ERROR"),
        ("EXP_P1_GS_H56_YOUR32_V1", "NEEDS_RERUN", None, "SCRIPT_ERROR"),
        ("EXP_P1_ZB_H30_COMBINED_V1", "NEEDS_RERUN", None, None),
    ]
    summary_row = (0, 15, 1, 0, 16)
    cursor = _FakeCursor(zombie_rows, stale_rows, summary_row)

    sync_calls = []

    monkeypatch.setattr(reset_zombies, "get_conn", _fake_conn_cm(cursor))
    monkeypatch.setattr(
        reset_zombies,
        "sync_snapshot_to_json",
        lambda json_path, dsn: sync_calls.append((str(json_path), dsn)),
    )

    out = reset_zombies.cleanup(
        dsn="fake-dsn",
        json_path=tmp_path / "experiments.json",
        dry_run=False,
    )

    assert out["final_counts"]["running"] == 0
    assert out["final_counts"]["needs_rerun"] == 15
    assert out["final_counts"]["completed"] == 1
    assert out["zombies_reset"] == [
        "EXP_P3_GS_H56_ORIGIN_V1",
        "EXP_P3_GS_H56_SENIOR10_V1",
    ]
    assert out["stale_errors_cleared"] == [
        "EXP_P1_GS_H56_SENIOR10_V1",
        "EXP_P1_GS_H56_YOUR32_V1",
    ]
    assert len(sync_calls) == 1

    update_status_sql = [
        sql
        for sql, _ in cursor.executed
        if "UPDATE exp_registry.experiments" in sql and "status = 'NEEDS_RERUN'" in sql
    ]
    update_error_sql = [
        sql
        for sql, _ in cursor.executed
        if "UPDATE exp_registry.experiments" in sql and "error_type = NULL" in sql
    ]
    log_sql = [
        sql for sql, _ in cursor.executed if "INSERT INTO exp_registry.status_log" in sql
    ]
    assert len(update_status_sql) == 1
    assert len(update_error_sql) == 1
    assert len(log_sql) == 4


def test_cleanup_dry_run_has_no_writes(monkeypatch, tmp_path):
    zombie_rows = [("EXP_P3_GS_H56_ORIGIN_V1", "RUNNING", "run-a", None)]
    stale_rows = [("EXP_P1_GS_H56_SENIOR10_V1", "NEEDS_RERUN", None, "SCRIPT_ERROR")]
    summary_row = (1, 14, 1, 2, 16)
    cursor = _FakeCursor(zombie_rows, stale_rows, summary_row)

    sync_calls = []
    monkeypatch.setattr(reset_zombies, "get_conn", _fake_conn_cm(cursor))
    monkeypatch.setattr(
        reset_zombies,
        "sync_snapshot_to_json",
        lambda json_path, dsn: sync_calls.append((str(json_path), dsn)),
    )

    out = reset_zombies.cleanup(
        dsn="fake-dsn",
        json_path=Path(tmp_path / "experiments.json"),
        dry_run=True,
    )

    assert out["dry_run"] is True
    assert len(sync_calls) == 0
    assert all("UPDATE exp_registry.experiments" not in sql for sql, _ in cursor.executed)
    assert all("INSERT INTO exp_registry.status_log" not in sql for sql, _ in cursor.executed)
