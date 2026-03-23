import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_registry import DBExperimentsDB

import threading
import uuid

from unittest.mock import MagicMock, patch


def _mock_db_context(mock_cursor: MagicMock):
    mock_conn = MagicMock(name="mock_conn")
    cursor_cm = MagicMock(name="cursor_cm")
    cursor_cm.__enter__.return_value = mock_cursor
    cursor_cm.__exit__.return_value = False
    mock_conn.cursor.return_value = cursor_cm

    conn_cm = MagicMock(name="conn_cm")
    conn_cm.__enter__.return_value = mock_conn
    conn_cm.__exit__.return_value = False
    return conn_cm


def test_claim_experiment_success_returns_run_id_and_sets_fencing_token():
    db = DBExperimentsDB(dsn="postgresql://mock")
    cur = MagicMock()
    run_uuid = uuid.uuid4()
    cur.fetchone.return_value = (run_uuid,)

    with patch("db_registry.get_conn", return_value=_mock_db_context(cur)):
        with patch.object(db, "_claim_allowed_for_worker", return_value=True):
            run_id = db.claim_experiment("exp-1", "worker-a", 0, 1111)

    assert run_id == str(run_uuid)
    assert db.get_run_id("exp-1") == str(run_uuid)
    cur.execute.assert_called_with(
        "SELECT exp_registry.claim_experiment(%s, %s, %s, %s)",
        ("exp-1", "worker-a", 0, 1111),
    )


def test_claim_experiment_fails_when_already_running_returns_none():
    db = DBExperimentsDB(dsn="postgresql://mock")
    cur = MagicMock()
    cur.fetchone.return_value = None

    with patch("db_registry.get_conn", return_value=_mock_db_context(cur)):
        with patch.object(db, "_claim_allowed_for_worker", return_value=True):
            run_id = db.claim_experiment("exp-2", "worker-a", 1, 2222)

    assert run_id is None
    assert db.get_run_id("exp-2") is None


def test_claim_experiment_updates_fencing_token_on_reclaim():
    db = DBExperimentsDB(dsn="postgresql://mock")
    cur = MagicMock()
    first = uuid.uuid4()
    second = uuid.uuid4()
    cur.fetchone.side_effect = [(first,), (second,)]

    with patch("db_registry.get_conn", return_value=_mock_db_context(cur)):
        with patch.object(db, "_claim_allowed_for_worker", return_value=True):
            first_run_id = db.claim_experiment("exp-3", "worker-a", 0, 3333)
            second_run_id = db.claim_experiment("exp-3", "worker-a", 0, 3333)

    assert first_run_id == str(first)
    assert second_run_id == str(second)
    assert db.get_run_id("exp-3") == str(second)


def test_claim_experiment_returns_none_when_worker_not_allowed():
    db = DBExperimentsDB(dsn="postgresql://mock")
    cur = MagicMock()

    with patch("db_registry.get_conn", return_value=_mock_db_context(cur)):
        with patch.object(db, "_claim_allowed_for_worker", return_value=False):
            run_id = db.claim_experiment("exp-4", "worker-b", 0, 4444)

    assert run_id is None
    cur.execute.assert_not_called()


def test_claim_allowed_for_worker_with_none_preferred_worker():
    db = DBExperimentsDB()
    cur = MagicMock()
    cur.fetchone.return_value = {
        "preferred_worker": None,
        "extra": {"memory_contract": {"est_mem_decision_mb": 0}},
    }

    allowed = db._claim_allowed_for_worker(cur, "exp-a", "worker-a")

    assert allowed is True


def test_claim_allowed_for_worker_when_same_worker_is_preferred():
    db = DBExperimentsDB()
    cur = MagicMock()
    cur.fetchone.return_value = {
        "preferred_worker": "worker-a",
        "extra": {"memory_contract": {"est_mem_decision_mb": 512}},
    }

    allowed = db._claim_allowed_for_worker(cur, "exp-b", "worker-a")

    assert allowed is True


def test_claim_allowed_for_worker_rejects_other_worker_without_memory_headroom():
    db = DBExperimentsDB()
    cur = MagicMock()
    cur.fetchone.return_value = {
        "preferred_worker": "worker-pref",
        "extra": {"memory_contract": {"est_mem_decision_mb": 4096}},
    }

    with patch.object(db, "_get_preferred_worker_max_mb", return_value=8192):
        allowed = db._claim_allowed_for_worker(cur, "exp-c", "worker-other")

    assert allowed is False


def test_claim_allowed_for_worker_allows_fallback_when_memory_exceeds_preferred_cap():
    db = DBExperimentsDB()
    cur = MagicMock()
    cur.fetchone.return_value = {
        "preferred_worker": "worker-pref",
        "extra": {"memory_contract": {"est_mem_decision_mb": 12000}},
    }

    with patch.object(db, "_get_preferred_worker_max_mb", return_value=8192):
        allowed = db._claim_allowed_for_worker(cur, "exp-d", "worker-other")

    assert allowed is True


def test_get_runnable_experiments_filters_manual_stop_blocked_condition_and_main_with_unfinished_children():
    db = DBExperimentsDB(dsn="postgresql://mock")
    cur = MagicMock()

    all_rows = [
        {
            "name": "main-exp",
            "status": "NEEDS_RERUN",
            "parent_experiment": None,
            "role": "main",
            "error_type": None,
        },
        {
            "name": "child-running",
            "status": "RUNNING",
            "parent_experiment": "main-exp",
            "role": "",
            "error_type": None,
        },
        {
            "name": "cond-parent",
            "status": "NEEDS_RERUN",
            "parent_experiment": None,
            "role": "",
            "error_type": None,
        },
    ]
    runnable_rows = [
        {
            "name": "manual-stop-exp",
            "status": "NEEDS_RERUN",
            "error_type": "MANUAL_STOP",
            "extra": {},
        },
        {
            "name": "blocked-cond-exp",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "extra": {"condition_parent": "cond-parent"},
        },
        {
            "name": "main-exp",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "extra": {"role": "main"},
        },
        {
            "name": "ok-exp",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "extra": {},
        },
    ]
    cur.fetchall.side_effect = [all_rows, runnable_rows]

    with patch("db_registry.get_conn", return_value=_mock_db_context(cur)):
        runnable = db.get_runnable_experiments(worker_id=None)

    names = [r["name"] for r in runnable]
    assert names == ["ok-exp"]


def test_get_runnable_experiments_includes_fallback_worker_candidates_when_allowed():
    db = DBExperimentsDB(dsn="postgresql://mock")
    cur = MagicMock()

    all_rows = [
        {
            "name": "preferred-parent",
            "status": "COMPLETED",
            "parent_experiment": None,
            "role": "",
            "error_type": None,
        }
    ]
    worker_rows = [
        {
            "name": "owned-exp",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "preferred_worker": "worker-1",
            "extra": {},
        }
    ]
    fallback_rows = [
        {
            "name": "owned-exp",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "preferred_worker": "worker-1",
            "extra": {},
        },
        {
            "name": "fallback-allowed",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "preferred_worker": "worker-2",
            "extra": {"memory_contract": {"est_mem_decision_mb": 12000}},
        },
        {
            "name": "fallback-rejected",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "preferred_worker": "worker-3",
            "extra": {"memory_contract": {"est_mem_decision_mb": 1024}},
        },
        {
            "name": "no-preferred-worker",
            "status": "NEEDS_RERUN",
            "error_type": None,
            "preferred_worker": None,
            "extra": {},
        },
    ]
    cur.fetchall.side_effect = [all_rows, worker_rows, fallback_rows]

    with patch("db_registry.get_conn", return_value=_mock_db_context(cur)):
        with patch.object(db, "_get_preferred_worker_max_mb", return_value=8192):
            runnable = db.get_runnable_experiments(
                local_gpu_total=24000,
                worker_id="worker-1",
            )

    names = [r["name"] for r in runnable]
    assert "owned-exp" in names
    assert "fallback-allowed" in names
    assert "fallback-rejected" not in names
    assert "no-preferred-worker" not in names


def test_run_id_methods_use_lock_and_manage_state_consistently():
    db = DBExperimentsDB()
    lock = MagicMock(name="run_ids_lock")
    lock.__enter__.return_value = None
    lock.__exit__.return_value = False
    db._run_ids_lock = lock

    db.set_run_id("exp-lock", "run-1")
    assert db.get_run_id("exp-lock") == "run-1"
    db.clear_run_id("exp-lock")
    assert db.get_run_id("exp-lock") is None

    assert lock.__enter__.call_count == 4
    assert lock.__exit__.call_count == 4


def test_run_id_methods_remain_consistent_under_concurrent_access():
    db = DBExperimentsDB()

    def worker(idx: int):
        name = f"exp-{idx}"
        run_id = f"run-{idx}"
        db.set_run_id(name, run_id)
        assert db.get_run_id(name) == run_id
        db.clear_run_id(name)
        assert db.get_run_id(name) is None

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert db._run_ids == {}
