import sys, os
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from preprocess import (
    collect_feature_jobs,
    collect_missing_tasks,
    generate_single_feature,
    is_experiment_ready,
    run_experiment_gate,
)


class _GateReport:
    def __init__(self, has_errors=False, has_warnings=False, text=""):
        self.has_errors = has_errors
        self.has_warnings = has_warnings
        self._text = text

    def summary(self):
        return self._text


def test_generate_single_feature_dispatches_named_generator_success():
    fake_gen = MagicMock()
    with patch.dict(
        "preprocess.FEATURE_GENERATORS", {"velocity": fake_gen}, clear=True
    ):
        assert generate_single_feature("velocity", {}) is True
    fake_gen.assert_called_once_with()


def test_generate_single_feature_uses_normalized_name_fallback():
    fake_gen = MagicMock()
    with patch.dict(
        "preprocess.FEATURE_GENERATORS", {"velocity_3dim": fake_gen}, clear=True
    ):
        assert generate_single_feature("Velocity-3Dim", {}) is True
    fake_gen.assert_called_once_with()


def test_generate_single_feature_returns_false_on_generator_exception():
    broken = MagicMock(side_effect=RuntimeError("boom"))
    with patch.dict("preprocess.FEATURE_GENERATORS", {"burst": broken}, clear=True):
        assert generate_single_feature("burst", {}) is False
    broken.assert_called_once_with()


def test_generate_single_feature_cutoff_mode_calls_ensure_cut_feature():
    with (
        patch.dict("preprocess.FEATURE_GENERATORS", {}, clear=True),
        patch("preprocess._ensure_cut_feature") as ensure_cut,
    ):
        assert generate_single_feature("base_34dim_cut_d152", {}) is True
    ensure_cut.assert_called_once_with("base_34dim_cut_d152")


def test_generate_single_feature_recipe_mode_runs_script_and_updates_registry():
    recipe_exp = {
        "feature_recipes": {
            "custom_feat": {
                "script": "scripts/build_custom.py",
                "args": [1, "x"],
                "dims": "8",
                "description": "Custom recipe",
                "file": "custom_feat.pt",
            }
        }
    }
    with (
        patch.dict("preprocess.FEATURE_GENERATORS", {}, clear=True),
        patch("preprocess._run_script") as run_script,
        patch("preprocess._update_registry") as update_registry,
    ):
        assert generate_single_feature("custom_feat", recipe_exp) is True

    run_script.assert_called_once_with(Path("scripts/build_custom.py"), ["1", "x"])
    update_registry.assert_called_once_with(
        "custom_feat", "custom_feat.pt", 8, "Custom recipe"
    )


def test_generate_single_feature_recipe_missing_script_returns_false():
    recipe_exp = {"feature_recipes": {"custom_feat": {"dims": 4}}}
    with (
        patch.dict("preprocess.FEATURE_GENERATORS", {}, clear=True),
        patch("preprocess._run_script") as run_script,
    ):
        assert generate_single_feature("custom_feat", recipe_exp) is False
    run_script.assert_not_called()


def test_generate_single_feature_unknown_feature_returns_false():
    with patch.dict("preprocess.FEATURE_GENERATORS", {}, clear=True):
        assert generate_single_feature("nonexistent_feat", {}) is False


def test_collect_missing_tasks_returns_deduplicated_missing_features():
    ready_queue = [
        {"name": "exp1", "features": ["a", "b"]},
        {"name": "exp2", "features": ["b", "c"]},
    ]
    tasks = collect_missing_tasks(ready_queue, {"a"})
    assert set(tasks.keys()) == {"b", "c"}
    assert tasks["b"]["name"] == "exp1"
    assert tasks["c"]["name"] == "exp2"


def test_collect_feature_jobs_normalizes_and_filters_available_features():
    raw_jobs = [
        "feat_a",
        {"name": " feat_b ", "priority": 3},
        {"name": "feat_b", "priority": 10},
        123,
        {"name": ""},
    ]
    tasks, normalized = collect_feature_jobs(raw_jobs, {"feat_a"})

    assert [job["name"] for job in normalized] == ["feat_a", "feat_b", "feat_b"]
    assert set(tasks.keys()) == {"feat_b"}
    assert tasks["feat_b"]["priority"] == 3


def test_run_experiment_gate_returns_rule_blocked_when_gate_errors():
    exp = {"name": "RuleBlockedExp"}
    report = _GateReport(has_errors=True, text="hard rule violation")
    with (
        patch("preprocess.load_rules", return_value={"rules": [1]}),
        patch("preprocess.run_gate_rules", return_value=report),
        patch("preprocess.subprocess.run") as subproc,
    ):
        result = run_experiment_gate(exp)

    assert result["passed"] is False
    assert result["status"] == "RULE_BLOCKED"
    assert "hard rule violation" in result["message"]
    subproc.assert_not_called()


def test_run_experiment_gate_returns_missing_test_when_no_test_files_exist():
    exp_test = Path("/virtual/test_exp.py")
    default_test = Path("/virtual/default_gate.py")
    with (
        patch("preprocess.load_rules", return_value={}),
        patch("preprocess.get_experiment_test_path", return_value=exp_test),
        patch("preprocess.DEFAULT_GATE_TEST", default_test),
        patch.object(Path, "exists", side_effect=[False, False]),
    ):
        result = run_experiment_gate({"name": "NoTestsExp"})

    assert result["passed"] is False
    assert result["status"] == "MISSING_TEST"
    assert str(exp_test) in result["message"]


def test_run_experiment_gate_uses_default_test_and_returns_passed_memory_contract():
    exp_test = Path("/virtual/test_exp.py")
    default_test = Path("/virtual/default_gate.py")
    subproc_result = MagicMock(returncode=0, stdout="", stderr="")
    with (
        patch("preprocess.load_rules", return_value={}),
        patch("preprocess.get_experiment_test_path", return_value=exp_test),
        patch("preprocess.DEFAULT_GATE_TEST", default_test),
        patch.object(Path, "exists", side_effect=[False, True]),
        patch("preprocess.subprocess.run", return_value=subproc_result) as subproc,
        patch(
            "preprocess.estimate_experiment_memory_contract",
            return_value={"est_mem_upper_mb": 1234},
        ),
    ):
        result = run_experiment_gate({"name": "GoodExp"})

    assert result == {
        "passed": True,
        "status": "PASSED",
        "message": "",
        "memory_contract": {"est_mem_upper_mb": 1234},
    }
    called_cmd = subproc.call_args.args[0]
    assert called_cmd[1:3] == ["-m", "pytest"]
    assert called_cmd[3] == str(default_test)


def test_run_experiment_gate_no_tests_status_on_pytest_code_5():
    exp_test = Path("/virtual/test_exp.py")
    subproc_result = MagicMock(returncode=5, stdout="", stderr="")
    with (
        patch("preprocess.load_rules", return_value={}),
        patch("preprocess.get_experiment_test_path", return_value=exp_test),
        patch.object(Path, "exists", return_value=True),
        patch("preprocess.subprocess.run", return_value=subproc_result),
    ):
        result = run_experiment_gate({"name": "NoCollectedTests"})

    assert result["passed"] is False
    assert result["status"] == "NO_TESTS"
    assert result["message"] == "No tests collected"


def test_run_experiment_gate_failed_status_uses_fallback_message_when_empty_output():
    exp_test = Path("/virtual/test_exp.py")
    subproc_result = MagicMock(returncode=2, stdout="", stderr="")
    with (
        patch("preprocess.load_rules", return_value={}),
        patch("preprocess.get_experiment_test_path", return_value=exp_test),
        patch.object(Path, "exists", return_value=True),
        patch("preprocess.subprocess.run", return_value=subproc_result),
    ):
        result = run_experiment_gate({"name": "FailedGate"})

    assert result["passed"] is False
    assert result["status"] == "FAILED"
    assert result["message"] == "pytest failed with code 2"


def test_is_experiment_ready_true_for_subset_and_empty_features():
    assert is_experiment_ready({"features": ["a", "b"]}, {"a", "b", "c"}) is True
    assert is_experiment_ready({}, {"a"}) is True


def test_is_experiment_ready_false_when_required_feature_missing():
    assert is_experiment_ready({"features": ["a", "z"]}, {"a", "b"}) is False
