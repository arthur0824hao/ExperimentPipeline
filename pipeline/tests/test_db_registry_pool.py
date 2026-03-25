import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_registry import (
    DBExperimentsDB,
    derive_progression_status,
    _get_dsn,
    _get_dsn_candidates,
)
import db_registry


@pytest.fixture(autouse=True)
def reset_pool_singleton():
    db_registry._pool = None
    yield
    db_registry._pool = None


def _mock_conn_with_snapshot(snapshot):
    cur = MagicMock()
    cur.fetchone.return_value = (snapshot,)
    cur_cm = MagicMock()
    cur_cm.__enter__.return_value = cur

    conn = MagicMock()
    conn.cursor.return_value = cur_cm

    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    return conn_cm, cur


def test_get_dsn_prefers_exp_env_vars_over_pg_and_config():
    fake_cfg = {
        "experiment": {
            "host": "cfg-host",
            "port": "5555",
            "dbname": "cfg-db",
            "user": "cfg-user",
            "connect_timeout": "9",
        }
    }
    fake_config_file = MagicMock()
    fake_config_file.read_text.return_value = json.dumps(fake_cfg)

    with (
        patch.dict(
            os.environ,
            {
                "EXP_PGHOST": "exp-host",
                "PGHOST": "pg-host",
                "EXP_PGPORT": "6432",
                "PGPORT": "7432",
                "EXP_PGDATABASE": "exp-db",
                "EXP_PGUSER": "exp-user",
                "PGUSER": "pg-user",
                "EXP_PGCONNECT_TIMEOUT": "11",
            },
            clear=True,
        ),
        patch("db_registry.DATABASE_CONFIG_FILE", fake_config_file),
    ):
        dsn = _get_dsn()

    assert (
        dsn == "host=exp-host port=6432 dbname=exp-db user=exp-user connect_timeout=11"
    )


def test_get_dsn_uses_database_json_defaults_when_env_missing():
    fake_cfg = {
        "experiment": {
            "host": "cfg-host",
            "port": "6543",
            "dbname": "cfg-db",
            "user": "cfg-user",
            "connect_timeout": "4",
        }
    }
    fake_config_file = MagicMock()
    fake_config_file.read_text.return_value = json.dumps(fake_cfg)

    with (
        patch.dict(os.environ, {}, clear=True),
        patch("db_registry.DATABASE_CONFIG_FILE", fake_config_file),
    ):
        dsn = _get_dsn()

    assert (
        dsn == "host=cfg-host port=6543 dbname=cfg-db user=cfg-user connect_timeout=4"
    )


def test_get_dsn_falls_back_to_builtin_defaults_when_config_read_fails():
    fake_config_file = MagicMock()
    fake_config_file.read_text.side_effect = OSError("no file")

    with (
        patch.dict(os.environ, {"USER": "fallback-user"}, clear=True),
        patch("db_registry.DATABASE_CONFIG_FILE", fake_config_file),
    ):
        dsn = _get_dsn()

    assert (
        dsn
        == "host=localhost port=5432 dbname=ExperimentPipeline-database user=fallback-user connect_timeout=3"
    )


def test_get_dsn_candidates_prefers_local_tunnel_for_matched_machine():
    fake_cfg = {
        "experiment": {
            "host": "192.168.1.4",
            "port": "5432",
            "dbname": "ExperimentPipeline-database",
            "user": "arthur0824hao",
            "connect_timeout": "3",
        }
    }
    fake_config_file = MagicMock()
    fake_config_file.read_text.return_value = json.dumps(fake_cfg)
    fake_machines = {
        "SOTA": {"host": "140.122.185.39", "db_tunnel_port": 15432},
        "dino4ur": {"host": "140.122.185.39", "db_tunnel_port": 15433},
    }

    with (
        patch.dict(os.environ, {}, clear=True),
        patch("db_registry.DATABASE_CONFIG_FILE", fake_config_file),
        patch("db_registry._load_machine_constraints", return_value=fake_machines),
        patch("db_registry.socket.gethostname", return_value="SOTA"),
        patch("db_registry.socket.getfqdn", return_value="SOTA"),
    ):
        candidates = _get_dsn_candidates()

    assert candidates[0].startswith(
        "host=localhost port=15432 dbname=ExperimentPipeline-database"
    )
    assert any(c.startswith("host=192.168.1.4 port=5432") for c in candidates)


def test_get_dsn_candidates_keeps_env_host_first_then_tunnel():
    fake_cfg = {
        "experiment": {
            "host": "192.168.1.4",
            "port": "5432",
            "dbname": "ExperimentPipeline-database",
            "user": "arthur0824hao",
            "connect_timeout": "3",
        }
    }
    fake_config_file = MagicMock()
    fake_config_file.read_text.return_value = json.dumps(fake_cfg)
    fake_machines = {
        "SOTA": {"host": "140.122.185.39", "db_tunnel_port": 15432}
    }

    with (
        patch.dict(os.environ, {"EXP_PGHOST": "db.override", "EXP_PGPORT": "6432"}, clear=True),
        patch("db_registry.DATABASE_CONFIG_FILE", fake_config_file),
        patch("db_registry._load_machine_constraints", return_value=fake_machines),
        patch("db_registry.socket.gethostname", return_value="SOTA"),
        patch("db_registry.socket.getfqdn", return_value="SOTA"),
    ):
        candidates = _get_dsn_candidates()

    assert candidates[0].startswith("host=db.override port=6432")
    assert candidates[1].startswith("host=localhost port=15432")


def test_get_pool_uses_first_successful_candidate_and_closes_failed_pool():
    fail_pool = MagicMock()
    fail_probe = MagicMock()
    fail_cur = MagicMock()
    fail_cur.fetchone.return_value = (None,)
    fail_cur_cm = MagicMock()
    fail_cur_cm.__enter__.return_value = fail_cur
    fail_probe.cursor.return_value = fail_cur_cm
    fail_pool.getconn.return_value = fail_probe
    fail_pool.closed = False

    ok_pool = MagicMock()
    ok_probe = MagicMock()
    ok_cur = MagicMock()
    ok_cur.fetchone.return_value = ("exp_registry",)
    ok_cur_cm = MagicMock()
    ok_cur_cm.__enter__.return_value = ok_cur
    ok_probe.cursor.return_value = ok_cur_cm
    ok_pool.getconn.return_value = ok_probe
    ok_pool.closed = False

    with (
        patch("db_registry._get_dsn_candidates", return_value=["bad", "good"]),
        patch(
            "db_registry.psycopg2.pool.ThreadedConnectionPool",
            side_effect=[fail_pool, ok_pool],
        ) as pool_ctor,
    ):
        pool = db_registry.get_pool()

    assert pool is ok_pool
    assert pool_ctor.call_count == 2
    fail_pool.closeall.assert_called_once()
    ok_pool.putconn.assert_called_once_with(ok_probe)


def test_get_pool_returns_existing_singleton_without_new_connections():
    existing_pool = MagicMock()
    existing_pool.closed = False
    db_registry._pool = existing_pool

    with patch("db_registry.psycopg2.pool.ThreadedConnectionPool") as pool_ctor:
        pool = db_registry.get_pool()

    assert pool is existing_pool
    pool_ctor.assert_not_called()


def test_get_pool_raises_last_exception_when_all_candidates_fail():
    err1 = RuntimeError("candidate-1 failed")
    err2 = RuntimeError("candidate-2 failed")
    with (
        patch("db_registry._get_dsn_candidates", return_value=["a", "b"]),
        patch(
            "db_registry.psycopg2.pool.ThreadedConnectionPool", side_effect=[err1, err2]
        ),
    ):
        with pytest.raises(RuntimeError, match="candidate-2 failed"):
            db_registry.get_pool()


def test_sync_snapshot_to_json_writes_atomically_with_tmp_then_replace():
    snapshot = {
        "experiments": [{"name": "e1", "status": "NEEDS_RERUN"}],
        "archived": [],
    }
    conn_cm, cur = _mock_conn_with_snapshot(snapshot)

    json_path = MagicMock(spec=Path)
    json_path.parent = Path("/virtual")
    json_path.name = "experiments.json"
    json_path.exists.return_value = False

    mock_file = MagicMock()
    fdopen_cm = MagicMock()
    fdopen_cm.__enter__.return_value = mock_file

    with (
        patch("db_registry.get_conn", return_value=conn_cm),
        patch(
            "db_registry.enrich_progression_snapshot", side_effect=lambda x: x
        ) as enrich,
        patch("tempfile.mkstemp", return_value=(99, "/virtual/.tmp.json")),
        patch("os.fdopen", return_value=fdopen_cm) as fdopen_mock,
        patch("json.dump") as json_dump,
        patch("os.replace") as replace_mock,
        patch("os.path.exists", return_value=False),
        patch("os.unlink") as unlink_mock,
    ):
        db_registry.sync_snapshot_to_json(json_path)

    cur.execute.assert_called_once_with("SELECT exp_registry.snapshot_as_json()")
    enrich.assert_called_once_with(snapshot)
    fdopen_mock.assert_called_once_with(99, "w", encoding="utf-8")
    json_dump.assert_called_once()
    replace_mock.assert_called_once_with("/virtual/.tmp.json", json_path)
    unlink_mock.assert_not_called()


def test_sync_snapshot_to_json_returns_early_when_query_has_no_row():
    cur = MagicMock()
    cur.fetchone.return_value = None
    cur_cm = MagicMock()
    cur_cm.__enter__.return_value = cur

    conn = MagicMock()
    conn.cursor.return_value = cur_cm

    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn

    json_path = MagicMock(spec=Path)

    with (
        patch("db_registry.get_conn", return_value=conn_cm),
        patch("tempfile.mkstemp") as mkstemp_mock,
    ):
        db_registry.sync_snapshot_to_json(json_path)

    mkstemp_mock.assert_not_called()


def test_row_to_dict_handles_nulls_and_jsonb_defaults():
    row = {
        "name": "exp-null",
        "status": "NEEDS_RERUN",
        "extra": None,
        "batch_id": None,
        "retry_count": None,
        "oom_retry_count": None,
        "max_retries": None,
        "display_order": None,
        "script_path": None,
    }

    out = DBExperimentsDB._row_to_dict(row)

    assert out["name"] == "exp-null"
    assert out["running_on"] is None
    assert out["result"] is None
    assert out["error_info"] is None
    assert out["memory_contract"] is None
    assert out["condition_parent"] is None
    assert out["progression_status"] == "READY"


def test_row_to_dict_builds_running_result_error_and_progression_fields():
    now = datetime(2026, 3, 1, 2, 3, 4)
    row = {
        "name": "exp-rich",
        "status": "RUNNING",
        "worker_id": "plusle",
        "gpu_id": 1,
        "pid": 321,
        "started_at": now,
        "peak_memory_mb": 987,
        "result_f1": 0.91,
        "result_auc": 0.95,
        "result_peak_mb": 654,
        "error_type": "OOM",
        "is_true_oom": True,
        "error_message": "oom happened",
        "error_peak_mb": 1200,
        "failed_at": now,
        "completed_at": now,
        "doc_processed_at": now,
        "retry_count": 2,
        "oom_retry_count": 1,
        "max_retries": 5,
        "extra": {
            "condition_parent": "parent_exp",
            "gate_type": "f1",
            "gate_evidence_ref": "ev-1",
            "role": "child",
            "main_experiment": "root",
            "memory_contract": {"gpu": "16g"},
        },
        "condition_parent_status": "NEEDS_RERUN",
        "preferred_worker": "plusle",
        "group_id": "g-1",
        "depends_on_group": "g-0",
        "parent_experiment": "root",
        "display_order": 7,
        "script_path": "scripts/run.py",
    }

    out = DBExperimentsDB._row_to_dict(row)

    assert out["running_on"]["worker"] == "plusle"
    assert out["running_on"]["gpu"] == 1
    assert out["running_on"]["started_at"] == now.isoformat()
    assert out["result"]["f1_score"] == 0.91
    assert out["error_info"]["type"] == "OOM"
    assert out["condition_parent"] == "parent_exp"
    assert out["progression_status"] == "RUNNING"
    assert out["memory_contract"] == {"gpu": "16g"}


def test_derive_progression_status_completed():
    status, reason = derive_progression_status("COMPLETED")
    assert status == "COMPLETED"
    assert reason is None


def test_derive_progression_status_running_and_warmup():
    status, reason = derive_progression_status("RUNNING", warmup_hint=True)
    assert status == "WARM"
    assert reason is None


def test_derive_progression_status_blocked_by_unmet_parent():
    status, reason = derive_progression_status(
        "NEEDS_RERUN",
        condition_parent="parent_a",
        condition_parent_status="RUNNING",
    )
    assert status == "BLOCKED_CONDITION"
    assert reason == "condition_parent_unmet:parent_a"


def test_derive_progression_status_ready_when_parent_completed():
    status, reason = derive_progression_status(
        "NEEDS_RERUN",
        condition_parent="parent_a",
        condition_parent_status="COMPLETED",
    )
    assert status == "READY"
    assert reason is None
