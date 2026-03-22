#!/usr/bin/env python3
"""TDD tests for experiment management features.

C1: Kill machine → kill all experiments on that worker + mark DOWN
C2: Experiment priority (move up/down), kill experiment, start immediately
"""

import json

import pytest

# @behavior: experiments.behavior.yaml#run-training-process

LEGACY_JSON_DB_REASON = "legacy JSON-backed ExperimentsDB expectations are incompatible with the current DB-backed alias"


# ---------------------------------------------------------------------------
# C1: kill_experiments_on_worker
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason=LEGACY_JSON_DB_REASON)
class TestKillExperimentsOnWorker:
    def test_marks_running_experiments_on_worker_as_needs_rerun(
        self, mock_experiments_file
    ):
        from experiments import ExperimentsDB, STATUS_NEEDS_RERUN

        data = {
            "experiments": [
                {
                    "name": "exp_a",
                    "status": "RUNNING",
                    "running_on": {"worker": "minun", "gpu": 1},
                },
                {
                    "name": "exp_b",
                    "status": "RUNNING",
                    "running_on": {"worker": "plusle", "gpu": 0},
                },
                {"name": "exp_c", "status": "READY"},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        count = db.kill_experiments_on_worker("minun")

        assert count == 1
        reloaded = db.load()
        exp_a = next(e for e in reloaded["experiments"] if e["name"] == "exp_a")
        exp_b = next(e for e in reloaded["experiments"] if e["name"] == "exp_b")
        assert exp_a["status"] == STATUS_NEEDS_RERUN
        assert exp_a.get("running_on") is None
        assert exp_b["status"] == "RUNNING"

    def test_kills_multiple_experiments_on_same_worker(self, mock_experiments_file):
        from experiments import ExperimentsDB, STATUS_NEEDS_RERUN

        data = {
            "experiments": [
                {"name": "e1", "status": "RUNNING", "running_on": {"worker": "minun"}},
                {"name": "e2", "status": "RUNNING", "running_on": {"worker": "minun"}},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        count = db.kill_experiments_on_worker("minun")
        assert count == 2

    def test_returns_zero_when_no_running_on_worker(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [
                {"name": "e1", "status": "READY"},
                {"name": "e2", "status": "RUNNING", "running_on": {"worker": "plusle"}},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        count = db.kill_experiments_on_worker("minun")
        assert count == 0


# ---------------------------------------------------------------------------
# C2a: move_experiment (priority reorder)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason=LEGACY_JSON_DB_REASON)
class TestMoveExperiment:
    def _names(self, db):
        return [e["name"] for e in db.load()["experiments"]]

    def test_move_up_swaps_with_previous(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [
                {"name": "e1", "status": "READY"},
                {"name": "e2", "status": "READY"},
                {"name": "e3", "status": "READY"},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.move_experiment("e3", "up")
        assert ok is True
        assert self._names(db) == ["e1", "e3", "e2"]

    def test_move_down_swaps_with_next(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [
                {"name": "e1", "status": "READY"},
                {"name": "e2", "status": "READY"},
                {"name": "e3", "status": "READY"},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.move_experiment("e1", "down")
        assert ok is True
        assert self._names(db) == ["e2", "e1", "e3"]

    def test_move_up_at_top_returns_false(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [{"name": "e1"}, {"name": "e2"}],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.move_experiment("e1", "up")
        assert ok is False
        assert self._names(db) == ["e1", "e2"]

    def test_move_down_at_bottom_returns_false(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [{"name": "e1"}, {"name": "e2"}],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.move_experiment("e2", "down")
        assert ok is False

    def test_move_nonexistent_returns_false(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [{"name": "e1"}], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.move_experiment("ghost", "up")
        assert ok is False


# ---------------------------------------------------------------------------
# C2b: kill_experiment
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason=LEGACY_JSON_DB_REASON)
class TestKillExperiment:
    def test_kill_running_experiment_marks_needs_rerun_and_moves_to_bottom(
        self, mock_experiments_file
    ):
        from experiments import ExperimentsDB, STATUS_NEEDS_RERUN

        data = {
            "experiments": [
                {"name": "e1", "status": "RUNNING", "running_on": {"worker": "plusle"}},
                {"name": "e2", "status": "READY"},
                {"name": "e3", "status": "READY"},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.kill_experiment("e1")
        assert ok is True

        reloaded = db.load()
        names = [e["name"] for e in reloaded["experiments"]]
        assert names[-1] == "e1"
        killed = reloaded["experiments"][-1]
        assert killed["status"] == STATUS_NEEDS_RERUN
        assert killed.get("running_on") is None

    def test_kill_ready_experiment_moves_to_bottom(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [
                {"name": "e1", "status": "READY"},
                {"name": "e2", "status": "READY"},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.kill_experiment("e1")
        assert ok is True
        names = [e["name"] for e in db.load()["experiments"]]
        assert names == ["e2", "e1"]

    def test_kill_nonexistent_returns_false(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [{"name": "e1"}], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.kill_experiment("ghost")
        assert ok is False


# ---------------------------------------------------------------------------
# C2c: start_experiment_now (bump to top + set READY)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason=LEGACY_JSON_DB_REASON)
class TestStartExperimentNow:
    def test_moves_experiment_to_top_and_sets_ready(self, mock_experiments_file):
        from experiments import ExperimentsDB, STATUS_NEEDS_RERUN

        data = {
            "experiments": [
                {"name": "e1", "status": "READY"},
                {"name": "e2", "status": "READY"},
                {"name": "e3", "status": STATUS_NEEDS_RERUN},
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.start_experiment_now("e3")
        assert ok is True

        reloaded = db.load()
        names = [e["name"] for e in reloaded["experiments"]]
        assert names[0] == "e3"
        assert reloaded["experiments"][0]["status"] == STATUS_NEEDS_RERUN

    def test_clears_error_info_on_start(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [
                {
                    "name": "e1",
                    "status": "NEEDS_RERUN",
                    "error_info": {"type": "OOM"},
                    "retry_count": 2,
                },
            ],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        db.start_experiment_now("e1")
        exp = db.load()["experiments"][0]
        assert exp.get("error_info") is None
        assert exp.get("retry_count", 0) == 0

    def test_already_at_top_returns_true(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {
            "experiments": [{"name": "e1", "status": "READY"}],
            "archived": [],
        }
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.start_experiment_now("e1")
        assert ok is True

    def test_nonexistent_returns_false(self, mock_experiments_file):
        from experiments import ExperimentsDB

        data = {"experiments": [], "archived": []}
        mock_experiments_file.write_text(json.dumps(data))
        db = ExperimentsDB(mock_experiments_file)

        ok = db.start_experiment_now("ghost")
        assert ok is False

    def test_start_now_revives_zero_retry_budget(self, mock_experiments_file):
        import db_registry
        from experiments import ExperimentsDB

        executed = []

        class FakeCursor:
            rowcount = 1

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchone(self):
                return (-5,)

        class FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def cursor(self, *args, **kwargs):
                return FakeCursor()

        db = ExperimentsDB(mock_experiments_file)
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(db_registry, "get_conn", lambda _dsn: FakeConn())
        monkeypatch.setattr(db, "_sync_snapshot", lambda: None)
        monkeypatch.setattr(db, "clear_run_id", lambda _name: None)
        try:
            ok = db.start_experiment_now("e_zero")
        finally:
            monkeypatch.undo()

        assert ok is True
        update_sql = executed[1][0]
        assert "max_retries = GREATEST(COALESCE(max_retries, 0), 1)" in update_sql


class TestReadyHandoffRegistration:
    def test_consume_ready_queue_registers_once_and_skips_duplicates(
        self, tmp_path, monkeypatch
    ):
        import experiments

        ready_file = tmp_path / "ready.json"
        ready_file.write_text(
            json.dumps(
                {
                    "ready_to_process": 0,
                    "batch_id": "handoff-batch",
                    "experiments": [
                        {
                            "name": "exp_new",
                            "features_ready": True,
                            "gate_status": "PASSED",
                            "priority": 7,
                            "script": "experiments/exp_new/scripts/train.py",
                            "parent_experiment": "lineage_parent",
                            "condition_parent": "condition_parent_exp",
                            "gate_type": "prereq_done",
                            "gate_evidence_ref": "results_db/condition_parent_exp.json",
                        },
                        {
                            "name": "exp_new",
                            "features_ready": True,
                            "gate_status": "PASSED",
                        },
                        {
                            "name": "exp_existing",
                            "features_ready": True,
                            "gate_status": "PASSED",
                        },
                        {
                            "name": "exp_waiting_features",
                            "features_ready": False,
                        },
                    ],
                    "feature_jobs": [],
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(experiments, "READY_QUEUE_FILE", ready_file)

        inserted_configs = []

        def _fake_insert(_db, configs):
            inserted_configs.extend(configs)
            return len(configs)

        monkeypatch.setattr(experiments, "_insert_registered_configs", _fake_insert)

        class _FakeDB:
            dsn = None

            @staticmethod
            def load():
                return {
                    "experiments": [{"name": "exp_existing"}],
                    "completed": [],
                    "archived": [],
                }

        result = experiments.consume_ready_queue_registration_handoff(_FakeDB())

        assert result["registered"] == 1
        assert result["consumed"] == 3
        assert result["skipped_existing"] == 2
        assert len(inserted_configs) == 1
        assert inserted_configs[0]["name"] == "exp_new"
        assert inserted_configs[0]["batch_id"] == "handoff-batch"
        assert inserted_configs[0]["parent_experiment"] == "lineage_parent"
        assert inserted_configs[0]["condition_parent"] == "condition_parent_exp"
        assert inserted_configs[0]["gate_type"] == "prereq_done"
        assert (
            inserted_configs[0]["gate_evidence_ref"]
            == "results_db/condition_parent_exp.json"
        )

        remaining = json.loads(ready_file.read_text(encoding="utf-8"))
        assert [exp["name"] for exp in remaining["experiments"]] == [
            "exp_waiting_features"
        ]

        second = experiments.consume_ready_queue_registration_handoff(_FakeDB())
        assert second["registered"] == 0
        assert second["consumed"] == 0


class TestDBRowMetadataProjection:
    def test_row_to_dict_projects_condition_metadata(self):
        from db_registry import DBExperimentsDB

        row = {
            "name": "exp_condition",
            "batch_id": "b1",
            "status": "NEEDS_RERUN",
            "parent_experiment": "lineage_parent",
            "preferred_worker": None,
            "group_id": None,
            "depends_on_group": None,
            "display_order": 1,
            "script_path": "experiments/exp_condition/scripts/train.py",
            "retry_count": 0,
            "oom_retry_count": 0,
            "max_retries": 2,
            "error_type": None,
            "result_f1": None,
            "result_auc": None,
            "extra": {
                "role": "child",
                "condition_parent": "gate_parent",
                "gate_type": "prereq_done",
                "gate_evidence_ref": "results_db/gate_parent.json",
            },
        }

        converted = DBExperimentsDB._row_to_dict(row)
        assert converted["parent_experiment"] == "lineage_parent"
        assert converted["condition_parent"] == "gate_parent"
        assert converted["gate_type"] == "prereq_done"
        assert converted["gate_evidence_ref"] == "results_db/gate_parent.json"

    def test_row_to_dict_legacy_leaf_defaults_condition_metadata_to_none(self):
        from db_registry import DBExperimentsDB

        row = {
            "name": "exp_legacy",
            "batch_id": "b1",
            "status": "NEEDS_RERUN",
            "parent_experiment": "lineage_only_parent",
            "preferred_worker": None,
            "group_id": None,
            "depends_on_group": None,
            "display_order": 2,
            "script_path": "experiments/exp_legacy/scripts/train.py",
            "retry_count": 0,
            "oom_retry_count": 0,
            "max_retries": 2,
            "error_type": None,
            "result_f1": None,
            "result_auc": None,
            "extra": {"role": "child"},
        }

        converted = DBExperimentsDB._row_to_dict(row)
        assert converted["parent_experiment"] == "lineage_only_parent"
        assert converted["condition_parent"] is None
        assert converted["gate_type"] is None
        assert converted["gate_evidence_ref"] is None
