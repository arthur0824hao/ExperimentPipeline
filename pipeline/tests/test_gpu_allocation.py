import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from gpu import (  # noqa: E402
    _parse_nvidia_query_output,
    collect_gpu_status_with_error,
)
from allocator import GPUAllocator  # noqa: E402
from memory_contract import get_required_mem_mb  # noqa: E402


def test_parse_nvidia_query_output_with_util():
    output = "0, 12000, 4000, 16000, 35"
    parsed = _parse_nvidia_query_output(output, include_util=True)
    assert parsed == [
        {"index": 0, "free": 12000, "used": 4000, "total": 16000, "util": 35}
    ]


def test_parse_nvidia_query_output_without_util_sets_zero():
    output = "1, 9000, 7000, 16000"
    parsed = _parse_nvidia_query_output(output, include_util=False)
    assert parsed == [
        {"index": 1, "free": 9000, "used": 7000, "total": 16000, "util": 0}
    ]


def test_parse_nvidia_query_output_ignores_blank_lines_and_coerces_units():
    output = "\n0, 10000 MiB, 6000 MiB, 16000 MiB, 25 %\n\n"
    parsed = _parse_nvidia_query_output(output, include_util=True)
    assert parsed[0]["free"] == 10000
    assert parsed[0]["used"] == 6000
    assert parsed[0]["util"] == 25


def test_parse_nvidia_query_output_raises_for_short_row():
    with patch("gpu.subprocess.check_output", MagicMock()):
        try:
            _parse_nvidia_query_output("0,100,200", include_util=False)
            assert False, "expected ValueError"
        except ValueError:
            pass


@patch("gpu.subprocess.check_output")
def test_collect_gpu_status_with_error_first_command_success(mock_check_output):
    mock_check_output.return_value = "0, 12000, 4000, 16000, 10"
    gpus, err = collect_gpu_status_with_error()
    assert err == ""
    assert gpus == [
        {"index": 0, "free": 12000, "used": 4000, "total": 16000, "util": 10}
    ]
    assert mock_check_output.call_count == 1


@patch("gpu.subprocess.check_output")
def test_collect_gpu_status_with_error_fallback_after_empty_output(mock_check_output):
    mock_check_output.side_effect = ["", "0, 11000, 5000, 16000, 5"]
    gpus, err = collect_gpu_status_with_error()
    assert err == ""
    assert len(gpus) == 1
    assert gpus[0]["index"] == 0
    assert mock_check_output.call_count == 2


@patch("gpu.subprocess.check_output")
def test_collect_gpu_status_with_error_timeout_all_variants(mock_check_output):
    import subprocess

    mock_check_output.side_effect = subprocess.TimeoutExpired(
        cmd="nvidia-smi", timeout=8
    )
    gpus, err = collect_gpu_status_with_error()
    assert gpus == []
    assert err == "nvidia-smi timeout"
    assert mock_check_output.call_count == 4


@patch("gpu.subprocess.check_output")
def test_collect_gpu_status_with_error_not_found_all_variants(mock_check_output):
    mock_check_output.side_effect = FileNotFoundError("missing")
    gpus, err = collect_gpu_status_with_error()
    assert gpus == []
    assert err == "nvidia-smi not found"
    assert mock_check_output.call_count == 4


@patch("gpu.get_gpu_process_count")
@patch("gpu.get_all_gpu_status")
def test_gpu_allocator_allocate_picks_least_loaded_gpu(
    mock_get_all_gpu_status, mock_get_proc_count
):
    mock_get_all_gpu_status.return_value = [
        {"index": 0, "free": 14000, "used": 2000, "total": 16000, "util": 10},
        {"index": 1, "free": 13000, "used": 3000, "total": 16000, "util": 20},
    ]
    mock_get_proc_count.return_value = {0: 1, 1: 0}

    with patch.object(
        GPUAllocator, "_refresh_gpus", side_effect=lambda *args, **kwargs: None
    ):
        allocator = GPUAllocator(max_jobs_per_gpu=2)
        allocator.gpu_jobs[0] = ["exp_a"]
        allocator.gpu_jobs[1] = []
        allocator.gpus = [
            {"index": 0, "free": 14000, "used": 2000, "total": 16000, "util": 10},
            {"index": 1, "free": 13000, "used": 3000, "total": 16000, "util": 20},
        ]

        with (
            patch.object(allocator, "_is_warmup_complete", return_value=True),
            patch("allocator.time.time", return_value=123.0),
        ):
            chosen = allocator.allocate("new_exp", required_mem_mb=4000)

    assert chosen == 1
    assert "new_exp" in allocator.gpu_jobs[1]


@patch("gpu.get_gpu_process_count")
@patch("gpu.get_all_gpu_status")
def test_gpu_allocator_allocate_blocks_when_max_jobs_reached(
    mock_get_all_gpu_status, mock_get_proc_count
):
    mock_get_all_gpu_status.return_value = [
        {"index": 0, "free": 15000, "used": 1000, "total": 16000, "util": 5}
    ]
    mock_get_proc_count.return_value = {0: 1}

    allocator = GPUAllocator(max_jobs_per_gpu=1)
    allocator.gpu_jobs[0] = ["running_exp"]

    with (
        patch.object(allocator, "_is_warmup_complete", return_value=True),
        patch.object(allocator, "_refresh_gpus", return_value=None),
        patch("allocator.ALLOW_WARMUP_OVERLAP", False),
    ):
        chosen = allocator.allocate("new_exp", required_mem_mb=4000)

    assert chosen is None


@patch("gpu.get_gpu_process_count")
@patch("gpu.get_all_gpu_status")
def test_gpu_allocator_allocate_warmup_overlap_allows_extra_slot(
    mock_get_all_gpu_status, mock_get_proc_count
):
    mock_get_all_gpu_status.return_value = [
        {"index": 0, "free": 15000, "used": 1000, "total": 16000, "util": 5}
    ]
    mock_get_proc_count.return_value = {0: 1}

    allocator = GPUAllocator(max_jobs_per_gpu=1)
    allocator.gpu_jobs[0] = ["warmup_exp"]

    with (
        patch("allocator.ALLOW_WARMUP_OVERLAP", True),
        patch("allocator.MAX_PARALLEL_WARMUP_JOBS_PER_GPU", 2),
        patch.object(allocator, "_is_warmup_complete", return_value=False),
        patch.object(allocator, "_refresh_gpus", return_value=None),
    ):
        chosen = allocator.allocate("new_exp", required_mem_mb=4000)

    assert chosen == 0
    assert "new_exp" in allocator.gpu_jobs[0]


@patch("gpu.get_gpu_process_count")
@patch("gpu.get_all_gpu_status")
def test_gpu_allocator_allocate_high_mem_exclusive_blocks_busy_gpu(
    mock_get_all_gpu_status, mock_get_proc_count
):
    mock_get_all_gpu_status.return_value = [
        {"index": 0, "free": 20000, "used": 4000, "total": 24000, "util": 10}
    ]
    mock_get_proc_count.return_value = {0: 1}

    allocator = GPUAllocator(max_jobs_per_gpu=4)
    allocator.gpu_jobs[0] = ["running_exp"]

    with (
        patch.object(allocator, "_is_warmup_complete", return_value=True),
        patch.object(allocator, "_refresh_gpus", return_value=None),
        patch("allocator.ALLOW_WARMUP_OVERLAP", False),
        patch("allocator.HIGH_MEM_EXCLUSIVE_THRESHOLD_MB", 8000),
        patch("allocator.HIGH_MEM_EXCLUSIVE_RATIO", 0.8),
    ):
        chosen = allocator.allocate("high_mem_exp", required_mem_mb=9000)

    assert chosen is None


@patch("gpu.get_all_gpu_status")
def test_gpu_allocator_release_removes_job_and_assignment(mock_get_all_gpu_status):
    mock_get_all_gpu_status.return_value = [
        {"index": 0, "free": 15000, "used": 1000, "total": 16000, "util": 5}
    ]
    allocator = GPUAllocator(max_jobs_per_gpu=2)
    allocator.gpu_jobs[0] = ["exp_x"]
    allocator.gpu_job_assigned_at["exp_x"] = 10.0

    allocator.release("exp_x")

    assert allocator.gpu_jobs[0] == []
    assert "exp_x" not in allocator.gpu_job_assigned_at


@patch("gpu.get_all_gpu_status")
def test_gpu_allocator_release_unknown_job_cleans_assignment_only(
    mock_get_all_gpu_status,
):
    mock_get_all_gpu_status.return_value = [
        {"index": 0, "free": 15000, "used": 1000, "total": 16000, "util": 5}
    ]
    allocator = GPUAllocator(max_jobs_per_gpu=2)
    allocator.gpu_jobs[0] = ["exp_a"]
    allocator.gpu_job_assigned_at["orphan"] = 99.0

    allocator.release("orphan")

    assert allocator.gpu_jobs[0] == ["exp_a"]
    assert "orphan" not in allocator.gpu_job_assigned_at


@patch("memory_contract.get_memory_contract")
def test_get_required_mem_mb_uses_max_of_contract_fields(mock_contract):
    mock_contract.return_value = {
        "est_mem_decision_mb": "4500",
        "est_mem_upper_mb": 6200,
        "est_mem_initial_mb": 5000,
    }
    exp = {}
    assert get_required_mem_mb(exp) == 6200


@patch("memory_contract.get_memory_contract")
def test_get_required_mem_mb_oom_uses_peak_plus_500(mock_contract):
    mock_contract.return_value = {"est_mem_decision_mb": 3000}
    exp = {"error_info": {"type": "oom", "peak_memory_mb": 7000}}
    assert get_required_mem_mb(exp) == 7500


@patch("memory_contract.get_memory_contract")
def test_get_required_mem_mb_non_oom_uses_peak_plus_256(mock_contract):
    mock_contract.return_value = {"est_mem_decision_mb": 3000}
    exp = {"error_info": {"type": "runtime", "peak_memory_mb": "5000"}}
    assert get_required_mem_mb(exp) == 5256


@patch("memory_contract.get_memory_contract")
def test_get_required_mem_mb_ignores_invalid_values_and_enforces_floor(mock_contract):
    mock_contract.return_value = {
        "est_mem_decision_mb": "bad",
        "est_mem_upper_mb": None,
        "est_mem_initial_mb": "",
    }
    exp = {"error_info": {"type": "OOM", "peak_memory_mb": "oops"}}
    assert get_required_mem_mb(exp) == 4000
