#!/usr/bin/env python3
import sys
import os
from pathlib import Path
import pytest
import tempfile
import json

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_experiments_file(temp_dir):
    exp_file = temp_dir / "experiments.json"
    exp_file.write_text(json.dumps({"experiments": [], "archived": []}))
    return exp_file


@pytest.fixture
def db(mock_experiments_file):
    import experiments

    return experiments.ExperimentsDB(mock_experiments_file)


@pytest.fixture
def sample_experiments_data():
    return {
        "experiments": [
            {"name": "exp1", "status": "READY", "batch_id": "test-batch"},
            {
                "name": "exp2",
                "status": "RUNNING",
                "running_on": {"worker": "w1", "gpu": 0},
            },
            {"name": "exp3", "status": "DONE", "result": {"f1_score": 0.85}},
            {
                "name": "exp4",
                "status": "ERROR",
                "retry_count": 1,
                "error_info": {"type": "SCRIPT_ERROR"},
            },
            {
                "name": "exp5",
                "status": "OOM",
                "retry_count": 0,
                "error_info": {"is_true_oom": False, "peak_memory_mb": 15000},
            },
            {
                "name": "exp6",
                "status": "OOM",
                "retry_count": 0,
                "error_info": {"is_true_oom": True, "peak_memory_mb": 25000},
            },
        ],
        "archived": [{"name": "old_exp", "status": "DONE"}],
    }


@pytest.fixture
def populated_db(mock_experiments_file, sample_experiments_data):
    import experiments

    mock_experiments_file.write_text(json.dumps(sample_experiments_data))
    return experiments.ExperimentsDB(mock_experiments_file)


@pytest.fixture
def temp_locks_dir(temp_dir):
    locks = temp_dir / "locks"
    locks.mkdir()
    return locks


@pytest.fixture
def temp_heartbeats_dir(temp_dir):
    hb = temp_dir / "heartbeats"
    hb.mkdir()
    return hb


@pytest.fixture
def temp_machines_file(temp_dir):
    machines = temp_dir / "machines.json"
    machines.write_text(
        json.dumps(
            {
                "node1": {
                    "host": "node1.local",
                    "tmux_session": "runner",
                    "work_dir": "/tmp",
                },
                "node2": {"host": "node2.local", "max_jobs_per_gpu": 2},
            }
        )
    )
    return machines
