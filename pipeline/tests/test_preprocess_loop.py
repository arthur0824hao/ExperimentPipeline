import json
import builtins
import importlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

# @behavior: preprocess.behavior.yaml#bootstrap-runtime
# @behavior: preprocess.behavior.yaml#load-ready-batch
# @behavior: preprocess.behavior.yaml#scan-feature-availability
# @behavior: preprocess.behavior.yaml#archive-completed
# @behavior: preprocess.behavior.yaml#generate-missing-features
# @behavior: preprocess.behavior.yaml#gate-each-experiment
# @behavior: preprocess.behavior.yaml#register-ready-experiments
# @behavior: preprocess.behavior.yaml#persist-and-reset
# @behavior: preprocess.behavior.yaml#loop-supervision

import preprocess as preprocess


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _setup_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    phase3_root = tmp_path / "Phase3"
    ready_file = phase3_root / "ready.json"
    experiments_file = phase3_root / "experiments.json"
    locks_dir = phase3_root / "locks"
    feature_bank_dir = phase3_root / "data" / "feature_bank"
    registry_file = feature_bank_dir / "registry.json"

    locks_dir.mkdir(parents=True, exist_ok=True)
    feature_bank_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(preprocess, "PHASE3_ROOT", phase3_root)
    monkeypatch.setattr(preprocess, "READY_FILE", ready_file)
    monkeypatch.setattr(preprocess, "EXPERIMENTS_FILE", experiments_file)
    monkeypatch.setattr(preprocess, "LOCKS_DIR", locks_dir)
    monkeypatch.setattr(preprocess, "FEATURE_BANK_DIR", feature_bank_dir)
    monkeypatch.setattr(preprocess, "REGISTRY_FILE", registry_file)

    return ready_file, experiments_file, registry_file, locks_dir


def _gate_pass(_exp):
    return {"passed": True, "status": "PASSED", "message": ""}


def _gate_pass_with_memory(_exp):
    return {
        "passed": True,
        "status": "PASSED",
        "message": "",
        "memory_contract": {
            "memory_family": "fullbatch_sparse_gnn",
            "execution_mode": "fullbatch",
            "runtime_batch_adjustable": False,
            "neighborloader_applicable": True,
            "neighborloader_recommended": False,
            "fallback_mode": "switch_to_neighborloader",
            "est_train_mem_mb": 7732,
            "est_eval_mem_mb": 7150,
            "est_predict_mem_mb": 7200,
            "est_mem_upper_mb": 7732,
            "est_mem_decision_mb": 7732,
        },
    }


def test_run_once_consumes_ready_experiment_without_registry_registration(
    tmp_path, monkeypatch
):
    ready_file, experiments_file, registry_file, _ = _setup_paths(tmp_path, monkeypatch)

    monkeypatch.setattr(preprocess, "run_experiment_gate", _gate_pass)

    _write_json(
        registry_file,
        {"features": {"base_34dim_cut_d152": {"file": None}}},
    )

    _write_json(
        ready_file,
        [
            {
                "name": "TestExp",
                "features": ["base_34dim_cut_d152"],
                "priority": 7,
            }
        ],
    )

    preprocess.run_once()

    with open(experiments_file, "r", encoding="utf-8") as f:
        experiments = json.load(f)

    assert experiments == []

    with open(ready_file, "r", encoding="utf-8") as f:
        remaining = json.load(f)
    assert remaining["experiments"] == []
    assert remaining["ready_to_process"] == 0


def test_run_once_passes_runtime_overrides_into_gate_before_consuming_item(
    tmp_path, monkeypatch
):
    ready_file, experiments_file, registry_file, _ = _setup_paths(tmp_path, monkeypatch)

    seen = {}

    def _gate_capture(exp):
        seen.update(exp)
        return _gate_pass(exp)

    monkeypatch.setattr(preprocess, "run_experiment_gate", _gate_capture)

    _write_json(
        registry_file,
        {"features": {"base_34dim_cut_d152": {"file": None}}},
    )

    _write_json(
        ready_file,
        {
            "ready_to_process": 1,
            "batch_id": "diag-batch",
            "experiments": [
                {
                    "name": "DiagExp",
                    "features": ["base_34dim_cut_d152"],
                    "priority": 9,
                    "max_retries": 0,
                    "preferred_worker": "plusle",
                    "batch_size": 128,
                    "eval_batch_size": 256,
                    "env": {"MAX_EPOCHS": 1, "HIDDEN_DIM": 10},
                }
            ],
            "feature_jobs": [],
        },
    )

    preprocess.run_once()

    with open(experiments_file, "r", encoding="utf-8") as f:
        experiments = json.load(f)

    assert experiments == []
    assert seen["name"] == "DiagExp"
    assert seen["max_retries"] == 0
    assert seen["preferred_worker"] == "plusle"
    assert seen["batch_size"] == 128
    assert seen["eval_batch_size"] == 256
    assert seen["env"] == {"MAX_EPOCHS": 1, "HIDDEN_DIM": 10}

    with open(ready_file, "r", encoding="utf-8") as f:
        remaining = json.load(f)
    assert remaining["experiments"] == []
    assert remaining["ready_to_process"] == 0


def test_run_once_consumes_gate_pass_with_memory_contract_without_registry_write(
    tmp_path, monkeypatch
):
    ready_file, experiments_file, registry_file, _ = _setup_paths(tmp_path, monkeypatch)

    monkeypatch.setattr(preprocess, "run_experiment_gate", _gate_pass_with_memory)

    _write_json(
        registry_file,
        {"features": {"base_34dim_cut_d152": {"file": None}}},
    )

    _write_json(
        ready_file,
        {
            "ready_to_process": 1,
            "batch_id": "mem-batch",
            "experiments": [
                {
                    "name": "MemExp",
                    "features": ["base_34dim_cut_d152"],
                }
            ],
            "feature_jobs": [],
        },
    )

    preprocess.run_once()

    with open(experiments_file, "r", encoding="utf-8") as f:
        experiments = json.load(f)

    assert experiments == []

    with open(ready_file, "r", encoding="utf-8") as f:
        remaining = json.load(f)
    assert remaining["experiments"] == []
    assert remaining["ready_to_process"] == 0


def test_run_once_skips_when_features_missing(tmp_path, monkeypatch):
    ready_file, experiments_file, registry_file, _ = _setup_paths(tmp_path, monkeypatch)

    monkeypatch.setattr(preprocess, "run_experiment_gate", _gate_pass)

    _write_json(registry_file, {"features": {}})
    _write_json(
        ready_file,
        [{"name": "MissingFeat", "features": ["velocity_3dim"]}],
    )
    _write_json(experiments_file, [])

    preprocess.run_once()

    with open(experiments_file, "r", encoding="utf-8") as f:
        experiments = json.load(f)
    assert experiments == []

    with open(ready_file, "r", encoding="utf-8") as f:
        remaining = json.load(f)
    assert len(remaining["experiments"]) == 1


def test_run_once_blocks_when_gate_fails(tmp_path, monkeypatch):
    ready_file, experiments_file, registry_file, _ = _setup_paths(tmp_path, monkeypatch)

    _write_json(
        registry_file,
        {"features": {"base_34dim_cut_d152": {"file": None}}},
    )
    _write_json(
        ready_file,
        [
            {
                "name": "GateFailExp",
                "features": ["base_34dim_cut_d152"],
            }
        ],
    )

    monkeypatch.setattr(
        preprocess,
        "run_experiment_gate",
        lambda _exp: {"passed": False, "status": "FAILED", "message": "boom"},
    )

    preprocess.run_once()

    with open(experiments_file, "r", encoding="utf-8") as f:
        experiments = json.load(f)
    assert experiments == []

    with open(ready_file, "r", encoding="utf-8") as f:
        remaining = json.load(f)
    assert len(remaining["experiments"]) == 1
    assert remaining["experiments"][0]["gate_status"] == "FAILED"


def test_archive_doc_processed_completed_moves_done(tmp_path, monkeypatch):
    _, experiments_file, _, _ = _setup_paths(tmp_path, monkeypatch)

    _write_json(
        experiments_file,
        {
            "experiments": [{"name": "KeepExp", "status": "READY"}],
            "completed": [
                {"name": "DoneExp", "status": "COMPLETED", "doc_processed": True}
            ],
            "archived": [],
        },
    )

    exp_data = preprocess.load_experiments_json()
    archived = preprocess.archive_doc_processed_completed(exp_data)
    assert [e["name"] for e in archived["experiments"]] == ["KeepExp"]
    assert [e["name"] for e in archived["archived"]] == ["DoneExp"]


def test_collect_missing_tasks_deduplicates(tmp_path, monkeypatch):
    _setup_paths(tmp_path, monkeypatch)

    ready_queue = [
        {"name": "E1", "features": ["feat_a", "feat_b"]},
        {"name": "E2", "features": ["feat_a"]},
    ]
    tasks = preprocess.collect_missing_tasks(ready_queue, available_features=set())

    assert set(tasks.keys()) == {"feat_a", "feat_b"}
    assert tasks["feat_a"]["name"] in {"E1", "E2"}


def test_collect_feature_jobs_normalizes_and_filters_available(tmp_path, monkeypatch):
    _setup_paths(tmp_path, monkeypatch)

    tasks, jobs = preprocess.collect_feature_jobs(
        ["feat_a", {"name": "feat_b"}, {"name": "feat_a"}, {"x": 1}],
        available_features={"feat_b"},
    )

    assert [job["name"] for job in jobs] == ["feat_a", "feat_b", "feat_a"]
    assert set(tasks.keys()) == {"feat_a"}


def test_run_once_processes_feature_jobs_without_experiments(tmp_path, monkeypatch):
    ready_file, experiments_file, registry_file, _ = _setup_paths(tmp_path, monkeypatch)

    _write_json(registry_file, {"features": {}})
    _write_json(
        ready_file,
        {
            "ready_to_process": 1,
            "batch_id": "feature-only",
            "experiments": [],
            "feature_jobs": [{"name": "velocity_3dim_cut_d152"}],
        },
    )
    _write_json(experiments_file, [])

    generated = []
    state = {"available": set()}

    monkeypatch.setattr(
        preprocess, "get_available_features", lambda: set(state["available"])
    )

    def _fake_generate(feat_name, exp):
        generated.append((feat_name, exp["name"]))
        state["available"].add(feat_name)
        return True

    monkeypatch.setattr(preprocess, "generate_single_feature", _fake_generate)
    monkeypatch.setattr(preprocess, "run_experiment_gate", _gate_pass)

    preprocess.run_once()

    assert generated == [("velocity_3dim_cut_d152", "velocity_3dim_cut_d152")]
    with open(ready_file, "r", encoding="utf-8") as f:
        ready_after = json.load(f)
    assert ready_after["feature_jobs"] == []
    assert ready_after["experiments"] == []
    assert ready_after["ready_to_process"] == 0


def test_run_once_rechecks_available_features_after_generation(tmp_path, monkeypatch):
    ready_file, experiments_file, registry_file, _ = _setup_paths(tmp_path, monkeypatch)

    _write_json(registry_file, {"features": {}})
    _write_json(
        ready_file,
        {
            "ready_to_process": 1,
            "batch_id": "feature-recheck",
            "experiments": [],
            "feature_jobs": [{"name": "base_34dim_cut_d090"}],
        },
    )
    _write_json(experiments_file, [])

    state = {"available": set()}

    monkeypatch.setattr(
        preprocess, "get_available_features", lambda: set(state["available"])
    )
    monkeypatch.setattr(preprocess, "run_experiment_gate", _gate_pass)

    def _fake_generate(feat_name, exp):
        state["available"].add(feat_name)
        return True

    monkeypatch.setattr(preprocess, "generate_single_feature", _fake_generate)

    preprocess.run_once()

    with open(ready_file, "r", encoding="utf-8") as f:
        ready_after = json.load(f)
    assert ready_after["feature_jobs"] == []


def test_fallback_import_path_keeps_memory_estimator(monkeypatch, tmp_path):
    class _GateReport:
        has_errors = False
        has_warnings = False

        @staticmethod
        def summary():
            return ""

    real_import = builtins.__import__
    real_import_module = importlib.import_module

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"preprocess_lib.gate_engine", "preprocess_lib.memory_estimator"}:
            raise ImportError("libtorch_global_deps.so")
        return real_import(name, globals, locals, fromlist, level)

    fake_gate = SimpleNamespace(
        load_rules=lambda _path: [],
        run_gate_rules=lambda *_args, **_kwargs: _GateReport(),
    )
    fake_estimator = SimpleNamespace(
        estimate_experiment_memory_contract=lambda _exp, _root: {
            "est_train_mem_mb": 1234,
            "est_eval_mem_mb": 1111,
            "est_predict_mem_mb": 1122,
            "est_mem_upper_mb": 1234,
            "est_mem_decision_mb": 1234,
        },
        infer_memory_contract_for_exp=lambda _exp, _root: {
            "est_train_mem_mb": 1234,
            "est_eval_mem_mb": 1111,
            "est_predict_mem_mb": 1122,
            "est_mem_upper_mb": 1234,
            "est_mem_decision_mb": 1234,
        },
    )

    def _fake_import_module(name, package=None):
        if name == "gate_engine":
            return fake_gate
        if name == "memory_estimator":
            return fake_estimator
        return real_import_module(name, package)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    monkeypatch.setattr(importlib, "import_module", _fake_import_module)

    module_path = Path(preprocess.__file__)
    spec = importlib.util.spec_from_file_location(
        "preprocess_fallback_test", module_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    test_path = tmp_path / "fake_gate_test.py"
    test_path.write_text("def test_ok():\n    assert True\n", encoding="utf-8")
    monkeypatch.setattr(module, "get_experiment_test_path", lambda _exp: test_path)
    monkeypatch.setattr(
        module,
        "subprocess",
        SimpleNamespace(
            run=lambda *args, **kwargs: SimpleNamespace(
                returncode=0, stdout="", stderr=""
            )
        ),
    )

    result = module.run_experiment_gate({"name": "FallbackExp", "features": ["feat"]})

    assert result["passed"] is True
    assert result["memory_contract"]["est_train_mem_mb"] == 1234
    assert result["memory_contract"]["est_mem_decision_mb"] == 1234
