import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_registry import DBExperimentsDB, _normalize_registry_status

from unittest.mock import MagicMock, patch

import pytest


def _build_mock_pool_with_cursor(fetchone_value=(True,)):
    pool = MagicMock()
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_value

    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = None
    conn.cursor.return_value = cursor_cm

    pool.getconn.return_value = conn
    return pool, conn, cursor


@pytest.mark.parametrize(
    "raw_status, expected",
    [
        ("READY", "NEEDS_RERUN"),
        ("ERROR", "NEEDS_RERUN"),
        ("OOM", "NEEDS_RERUN"),
        ("DONE", "COMPLETED"),
        ("SKIPPED", "COMPLETED"),
        ("RUNNING", "RUNNING"),
        ("COMPLETED", "COMPLETED"),
        ("needs_rerun", "NEEDS_RERUN"),
    ],
)
def test_normalize_registry_status_known_mappings(raw_status, expected):
    assert _normalize_registry_status(raw_status) == expected


def test_normalize_registry_status_defaults_to_needs_rerun_for_unknown_or_empty():
    assert _normalize_registry_status(None) == "NEEDS_RERUN"
    assert _normalize_registry_status("") == "NEEDS_RERUN"
    assert _normalize_registry_status("weird") == "NEEDS_RERUN"


def test_mark_running_updates_started_at_when_run_id_and_started_at_provided():
    pool, conn, cursor = _build_mock_pool_with_cursor(fetchone_value=(True,))
    db = DBExperimentsDB()

    with (
        patch("db_registry.get_pool", return_value=pool),
        patch.object(db, "claim_experiment", return_value="run-123"),
        patch.object(db, "_sync_snapshot") as sync_snapshot,
    ):
        run_id = db.mark_running(
            "exp_a", "worker-1", 0, 1111, started_at="2026-03-22T10:00:00Z"
        )

    assert run_id == "run-123"
    cursor.execute.assert_called_once_with(
        "UPDATE exp_registry.experiments SET started_at = %s WHERE name = %s AND run_id = %s::uuid",
        ("2026-03-22T10:00:00Z", "exp_a", "run-123"),
    )
    conn.commit.assert_called_once()
    pool.putconn.assert_called_once_with(conn)
    sync_snapshot.assert_called_once()


def test_mark_running_skips_started_at_update_when_not_provided():
    db = DBExperimentsDB()

    with patch.object(db, "claim_experiment", return_value="run-123"):
        run_id = db.mark_running("exp_a", "worker-1", 0, 1111)

    assert run_id == "run-123"


def test_mark_done_uses_result_metrics_and_clears_run_id_on_success():
    pool, conn, cursor = _build_mock_pool_with_cursor(fetchone_value=(True,))
    db = DBExperimentsDB()

    with (
        patch("db_registry.get_pool", return_value=pool),
        patch.object(db, "_sync_snapshot") as sync_snapshot,
    ):
        db.set_run_id("exp_done", "run-abc")
        ok = db.mark_done(
            "exp_done",
            result={"f1_score": 0.91, "auc_score": 0.88, "peak_memory_mb": 3210},
        )

    assert ok is True
    cursor.execute.assert_called_once_with(
        "SELECT exp_registry.complete_experiment(%s, %s::uuid, %s::double precision, %s::double precision, %s::integer)",
        ("exp_done", "run-abc", 0.91, 0.88, 3210),
    )
    assert db.get_run_id("exp_done") is None
    conn.commit.assert_called_once()
    pool.putconn.assert_called_once_with(conn)
    sync_snapshot.assert_called_once()


def test_mark_done_supports_test_metric_fallback_keys():
    pool, _, cursor = _build_mock_pool_with_cursor(fetchone_value=(True,))
    db = DBExperimentsDB()

    with (
        patch("db_registry.get_pool", return_value=pool),
        patch.object(db, "_sync_snapshot"),
    ):
        ok = db.mark_done(
            "exp_done",
            result={"test_f1": 0.77, "test_auc": 0.81, "peak_memory_mb": "2000"},
            run_id="run-fallback",
        )

    assert ok is True
    cursor.execute.assert_called_once_with(
        "SELECT exp_registry.complete_experiment(%s, %s::uuid, %s::double precision, %s::double precision, %s::integer)",
        ("exp_done", "run-fallback", 0.77, 0.81, 2000),
    )


def test_mark_done_returns_false_without_run_id_and_avoids_db_call():
    db = DBExperimentsDB()

    with patch("db_registry.get_pool") as get_pool_mock:
        ok = db.mark_done("exp_missing")

    assert ok is False
    get_pool_mock.assert_not_called()


def test_mark_error_oom_path_passes_true_oom_flag_and_peak_memory():
    pool, conn, cursor = _build_mock_pool_with_cursor(fetchone_value=(True,))
    db = DBExperimentsDB()

    with (
        patch("db_registry.get_pool", return_value=pool),
        patch.object(db, "_sync_snapshot") as sync_snapshot,
    ):
        db.set_run_id("exp_oom", "run-oom")
        ok = db.mark_error(
            "exp_oom",
            error_type="OOM",
            message="CUDA out of memory",
            is_true_oom=True,
            peak_memory_mb=24576,
        )

    assert ok is True
    cursor.execute.assert_called_once_with(
        "SELECT exp_registry.fail_experiment(%s, %s::uuid, %s, %s, %s, %s)",
        ("exp_oom", "run-oom", "OOM", "CUDA out of memory", True, 24576),
    )
    assert db.get_run_id("exp_oom") is None
    conn.commit.assert_called_once()
    pool.putconn.assert_called_once_with(conn)
    sync_snapshot.assert_called_once()


def test_mark_error_non_oom_path_passes_false_oom_flag_and_retry_payload():
    pool, _, cursor = _build_mock_pool_with_cursor(fetchone_value=(True,))
    db = DBExperimentsDB()

    with (
        patch("db_registry.get_pool", return_value=pool),
        patch.object(db, "_sync_snapshot"),
    ):
        ok = db.mark_error(
            "exp_err",
            error_type="SCRIPT_ERROR",
            message="traceback here",
            is_true_oom=False,
            peak_memory_mb=1024,
            run_id="run-err",
        )

    assert ok is True
    cursor.execute.assert_called_once_with(
        "SELECT exp_registry.fail_experiment(%s, %s::uuid, %s, %s, %s, %s)",
        ("exp_err", "run-err", "SCRIPT_ERROR", "traceback here", False, 1024),
    )


def test_mark_error_returns_false_without_run_id_and_avoids_db_call():
    db = DBExperimentsDB()

    with patch("db_registry.get_pool") as get_pool_mock:
        ok = db.mark_error("exp_missing", error_type="SCRIPT_ERROR", message="err")

    assert ok is False
    get_pool_mock.assert_not_called()
