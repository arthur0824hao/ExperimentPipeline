#!/usr/bin/env python3

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from preprocess import move_completed_experiments
from experiments import (
    DBExperimentsDB,
    _build_ready_queue_entry,
    _insert_registered_configs,
    _load_ready_queue_data,
    consume_ready_queue_registration_handoff,
)


def _mock_conn_with_cursor(cur: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    return conn_cm


def test_load_ready_queue_data_missing_file_returns_defaults():
    path = MagicMock(spec=Path)
    path.exists.return_value = False

    result = _load_ready_queue_data(path)

    assert result == {"ready_to_process": 0, "experiments": [], "feature_jobs": []}


def test_load_ready_queue_data_json_error_returns_defaults():
    path = MagicMock(spec=Path)
    path.exists.return_value = True

    with (
        patch("builtins.open", mock_open(read_data="{bad")),
        patch("experiments.json.load", side_effect=ValueError("invalid")),
    ):
        result = _load_ready_queue_data(path)

    assert result == {"ready_to_process": 0, "experiments": [], "feature_jobs": []}


def test_load_ready_queue_data_legacy_list_payload_sets_ready_flag():
    path = MagicMock(spec=Path)
    path.exists.return_value = True
    payload = [{"name": "exp_a"}]

    with (
        patch("builtins.open", mock_open(read_data="[]")),
        patch("experiments.json.load", return_value=payload),
    ):
        result = _load_ready_queue_data(path)

    assert result["ready_to_process"] == 1
    assert result["experiments"] == payload
    assert result["feature_jobs"] == []
    assert str(result["batch_id"]).startswith("legacy_")


def test_load_ready_queue_data_dict_normalizes_missing_fields():
    path = MagicMock(spec=Path)
    path.exists.return_value = True
    payload = {"experiments": "not-a-list", "feature_jobs": None}

    with (
        patch("builtins.open", mock_open(read_data="{}")),
        patch("experiments.json.load", return_value=payload),
    ):
        result = _load_ready_queue_data(path)

    assert result["experiments"] == []
    assert result["feature_jobs"] == []
    assert result["ready_to_process"] == 0


def test_build_ready_queue_entry_sets_defaults():
    exp = {}

    result = _build_ready_queue_entry(exp, "demo_exp")

    assert result["name"] == "demo_exp"
    assert result["script"] == "experiments/demo_exp/scripts/train.py"
    assert result["description"] == "Re-pipeline from experiments panel"
    assert result["features_ready"] is True
    assert result["gate_status"] == "PASSED"
    assert isinstance(result["gate_passed_at"], str)


def test_build_ready_queue_entry_copies_optional_fields():
    exp = {
        "batch_id": "b1",
        "priority": 7,
        "max_retries": 3,
        "preferred_worker": "w0",
        "group_id": "g1",
        "parent_experiment": "base",
        "role": "ablation",
        "main_experiment": "main",
        "condition_parent": "cond",
        "memory_contract": {"execution_mode": "neighborloader"},
        "env": {"A": 1, "B": "x"},
        "batch_size": 64,
        "eval_batch_size": 256,
    }

    result = _build_ready_queue_entry(exp, "exp_full")

    assert result["batch_id"] == "b1"
    assert result["priority"] == 7
    assert result["max_retries"] == 3
    assert result["preferred_worker"] == "w0"
    assert result["group_id"] == "g1"
    assert result["parent_experiment"] == "base"
    assert result["role"] == "ablation"
    assert result["main_experiment"] == "main"
    assert result["condition_parent"] == "cond"
    assert result["memory_contract"] == {"execution_mode": "neighborloader"}
    assert result["env"] == {"A": 1, "B": "x"}
    assert result["batch_size"] == 64
    assert result["eval_batch_size"] == 256


def test_insert_registered_configs_empty_configs_returns_zero():
    db = MagicMock(spec=DBExperimentsDB)

    assert _insert_registered_configs(db, []) == 0


def test_insert_registered_configs_inserts_and_serializes_extra_fields():
    db = MagicMock(spec=DBExperimentsDB)
    db.dsn = "postgres://mock"
    db._sync_snapshot = MagicMock()

    cur = MagicMock()
    cur.fetchone.side_effect = [(41,), ("exp_one",), None]

    configs = [
        {
            "name": "exp_one",
            "batch_id": "batchA",
            "script": "run_one.py",
            "priority": 5,
            "description": "first",
            "role": "main",
            "main_experiment": "main",
            "memory_contract": {"execution_mode": "fullbatch"},
            "env": {"OMP_NUM_THREADS": 8, "NULL_VAL": None},
            "batch_size": 128,
            "eval_batch_size": 1024,
            "max_retries": 4,
            "preferred_worker": "worker-a",
            "group_id": "grp",
            "parent_experiment": "parent",
        },
        {
            "name": "exp_two",
            "batch_id": "batchA",
            "script": "run_two.py",
        },
    ]

    with patch("experiments.get_conn", return_value=_mock_conn_with_cursor(cur)):
        inserted = _insert_registered_configs(db, configs)

    assert inserted == 1
    assert cur.execute.call_count == 3
    insert_call_one = cur.execute.call_args_list[1]
    insert_call_two = cur.execute.call_args_list[2]
    params_one = insert_call_one.args[1]
    params_two = insert_call_two.args[1]

    assert params_one[3] == 42
    assert params_two[3] == 43

    extra_one = json.loads(params_one[8])
    assert extra_one["priority"] == 5
    assert extra_one["description"] == "first"
    assert extra_one["role"] == "main"
    assert extra_one["main_experiment"] == "main"
    assert extra_one["memory_contract"] == {"execution_mode": "fullbatch"}
    assert extra_one["env"] == {"OMP_NUM_THREADS": "8"}
    assert extra_one["batch_size"] == 128
    assert extra_one["eval_batch_size"] == 1024
    db._sync_snapshot.assert_called_once()


def test_consume_ready_queue_registration_handoff_returns_zero_on_empty_queue():
    db = MagicMock(spec=DBExperimentsDB)

    with (
        patch(
            "experiments._load_ready_queue_data",
            return_value={"ready_to_process": 1, "experiments": []},
        ),
        patch("experiments._save_json_atomic") as save_mock,
    ):
        result = consume_ready_queue_registration_handoff(db)

    assert result == {"registered": 0, "consumed": 0, "skipped_existing": 0}
    save_mock.assert_not_called()


def test_consume_ready_queue_registration_handoff_dedupes_and_persists_remaining():
    db = MagicMock(spec=DBExperimentsDB)
    db.load.return_value = {
        "experiments": [{"name": "already_exists"}],
        "completed": [],
        "archived": [],
    }
    ready_data = {
        "ready_to_process": 1,
        "experiments": [
            {"name": "new_exp", "features_ready": True},
            {"name": "already_exists", "features_ready": True},
            {"name": "new_exp", "features_ready": True},
            {"name": "blocked", "features_ready": False},
        ],
    }

    with (
        patch("experiments._load_ready_queue_data", return_value=ready_data),
        patch(
            "experiments.register_experiment",
            return_value={
                "experiments": [
                    {
                        "name": "new_exp",
                        "batch_id": "auto_batch",
                        "script": "train.py",
                        "description": "d",
                    }
                ]
            },
        ) as register_mock,
        patch("experiments._insert_registered_configs", return_value=1) as insert_mock,
        patch("experiments._save_json_atomic") as save_mock,
    ):
        result = consume_ready_queue_registration_handoff(db)

    assert result == {"registered": 1, "consumed": 3, "skipped_existing": 2}
    insert_mock.assert_called_once()
    register_args = register_mock.call_args.args
    assert register_args[0]["name"] == "new_exp"
    assert str(register_args[2]).startswith("ready_handoff_")

    saved_payload = save_mock.call_args.args[1]
    assert saved_payload["experiments"] == [
        {"name": "blocked", "features_ready": False}
    ]
    assert saved_payload["ready_to_process"] == 1


def test_consume_ready_queue_registration_handoff_handles_insert_exception():
    db = MagicMock(spec=DBExperimentsDB)
    db.load.return_value = {"experiments": [], "completed": [], "archived": []}
    logger = MagicMock()

    ready_data = {
        "ready_to_process": 1,
        "batch_id": "batchX",
        "experiments": [{"name": "exp_a", "features_ready": True}],
    }

    with (
        patch("experiments._load_ready_queue_data", return_value=ready_data),
        patch(
            "experiments.register_experiment",
            return_value={
                "experiments": [
                    {"name": "exp_a", "batch_id": "batchX", "script": "train.py"}
                ]
            },
        ),
        patch(
            "experiments._insert_registered_configs", side_effect=RuntimeError("boom")
        ),
        patch("experiments._save_json_atomic") as save_mock,
    ):
        result = consume_ready_queue_registration_handoff(db, logger=logger)

    assert result == {"registered": 0, "consumed": 0, "skipped_existing": 0}
    save_mock.assert_not_called()
    logger.log.assert_called_once()


def test_move_completed_experiments_moves_done_and_completed_statuses():
    exp_data = {
        "experiments": [
            {"name": "e1", "status": "done"},
            {"name": "e2", "status": "COMPLETED"},
            {"name": "e3", "status": "RUNNING"},
        ],
        "completed": [{"name": "existing", "status": "DONE"}],
    }

    updated = move_completed_experiments(exp_data)

    assert [e["name"] for e in updated["experiments"]] == ["e3"]
    completed_names = [e["name"] for e in updated["completed"]]
    assert completed_names == ["existing", "e1", "e2"]
    moved_e1 = next(e for e in updated["completed"] if e["name"] == "e1")
    moved_e2 = next(e for e in updated["completed"] if e["name"] == "e2")
    assert isinstance(moved_e1.get("completed_at"), str)
    assert isinstance(moved_e2.get("completed_at"), str)


def test_move_completed_experiments_preserves_existing_completed_at():
    exp_data = {
        "experiments": [
            {
                "name": "e1",
                "status": "DONE",
                "completed_at": "2025-01-01T00:00:00",
            }
        ],
        "completed": [],
    }

    updated = move_completed_experiments(exp_data)

    assert updated["experiments"] == []
    assert updated["completed"][0]["completed_at"] == "2025-01-01T00:00:00"
