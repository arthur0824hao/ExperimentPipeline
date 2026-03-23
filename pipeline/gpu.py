import os
import re
import subprocess
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

try:
    from runtime_config import cfg_int, get_runtime_section
except ModuleNotFoundError:
    from .runtime_config import cfg_int, get_runtime_section


_RUNNER_CFG = get_runtime_section("experiments_runner")
GPU_JOB_COUNT_MIN_MEMORY_MB = cfg_int(_RUNNER_CFG, "gpu_job_count_min_memory_mb", 512)


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def get_all_gpu_status():
    gpus, _probe_error = collect_gpu_status_with_error()
    return gpus


def _coerce_nvidia_int(raw: str) -> int:
    text = str(raw).strip()
    if not text or text.upper() == "N/A":
        return 0
    try:
        return int(text)
    except ValueError:
        match = re.search(r"-?\d+", text)
        if not match:
            raise
        return int(match.group(0))


def _parse_nvidia_query_output(output: str, include_util: bool) -> List[Dict[str, int]]:
    gpus: List[Dict[str, int]] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = [part.strip() for part in line.split(",")]
        if include_util:
            if len(parts) < 5:
                raise ValueError(f"unexpected nvidia-smi row: {line!r}")
            idx, free, used, total, util = (
                _coerce_nvidia_int(part) for part in parts[:5]
            )
        else:
            if len(parts) < 4:
                raise ValueError(f"unexpected nvidia-smi row: {line!r}")
            idx, free, used, total = (_coerce_nvidia_int(part) for part in parts[:4])
            util = 0
        gpus.append(
            {"index": idx, "free": free, "used": used, "total": total, "util": util}
        )
    return gpus


def collect_gpu_status_with_error() -> Tuple[List[Dict[str, int]], str]:
    commands = [
        (
            [
                "nvidia-smi",
                "--query-gpu=index,memory.free,memory.used,memory.total,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            True,
        ),
        (
            [
                "/usr/bin/nvidia-smi",
                "--query-gpu=index,memory.free,memory.used,memory.total,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            True,
        ),
        (
            [
                "nvidia-smi",
                "--query-gpu=index,memory.free,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            False,
        ),
        (
            [
                "/usr/bin/nvidia-smi",
                "--query-gpu=index,memory.free,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            False,
        ),
    ]

    last_error = ""
    for cmd, include_util in commands:
        try:
            output = subprocess.check_output(
                cmd, encoding="utf-8", stderr=subprocess.DEVNULL, timeout=8
            ).strip()
            if not output:
                last_error = "empty nvidia-smi output"
                continue
            parsed = _parse_nvidia_query_output(output, include_util=include_util)
            if parsed:
                return parsed, ""
        except subprocess.TimeoutExpired:
            last_error = "nvidia-smi timeout"
        except FileNotFoundError:
            last_error = "nvidia-smi not found"
        except Exception as e:
            last_error = str(e)
    return [], last_error


def get_cpu_load():
    try:
        load1, load5, load15 = os.getloadavg()
        cpu_count = os.cpu_count() or 1
        return {
            "load1": round(load1, 2),
            "load5": round(load5, 2),
            "load15": round(load15, 2),
            "cpu_count": cpu_count,
            "load_percent": round(load1 / cpu_count * 100, 1),
        }
    except Exception:
        return {"load1": 0, "load5": 0, "load15": 0, "cpu_count": 1, "load_percent": 0}


def collect_system_info():
    gpus, gpu_probe_error = collect_gpu_status_with_error()
    cpu: Dict[str, Any] = dict(get_cpu_load())
    if gpu_probe_error:
        cpu["_gpu_probe_error"] = gpu_probe_error
    return {"gpus": gpus, "cpu": cpu}


def get_gpu_process_count():
    gpu_counts = {}
    try:
        cmd = [
            "nvidia-smi",
            "--query-compute-apps=gpu_uuid,pid,used_memory",
            "--format=csv,noheader",
        ]
        output = subprocess.check_output(
            cmd, encoding="utf-8", stderr=subprocess.DEVNULL, timeout=5
        ).strip()

        uuid_cmd = ["nvidia-smi", "--query-gpu=index,uuid", "--format=csv,noheader"]
        uuid_output = subprocess.check_output(
            uuid_cmd, encoding="utf-8", stderr=subprocess.DEVNULL, timeout=5
        ).strip()
        uuid_to_index = {}
        for line in uuid_output.split("\n"):
            if not line.strip():
                continue
            parts = line.split(", ")
            if len(parts) >= 2:
                idx = int(parts[0].strip())
                uuid = parts[1].strip()
                uuid_to_index[uuid] = idx
                gpu_counts[idx] = 0

        for line in output.split("\n"):
            if not line.strip():
                continue
            parts = line.split(", ")
            if len(parts) >= 3:
                uuid = parts[0].strip()
                used_memory_mb = _coerce_nvidia_int(parts[2])
                if used_memory_mb < GPU_JOB_COUNT_MIN_MEMORY_MB:
                    continue
                if uuid in uuid_to_index:
                    gpu_idx = uuid_to_index[uuid]
                    gpu_counts[gpu_idx] = gpu_counts.get(gpu_idx, 0) + 1
    except subprocess.TimeoutExpired:
        return {}
    except Exception:
        pass
    return gpu_counts


def get_pid_gpu_map():
    pid_map = {}
    try:
        cmd = [
            "nvidia-smi",
            "--query-compute-apps=pid,used_memory",
            "--format=csv,noheader,nounits",
        ]
        output = subprocess.check_output(
            cmd, encoding="utf-8", stderr=subprocess.DEVNULL, timeout=5
        ).strip()
        for line in output.split("\n"):
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) >= 2:
                pid, mem = int(parts[0]), int(parts[1])
                pid_map[pid] = mem
    except subprocess.TimeoutExpired:
        return {}
    except Exception:
        pass
    return pid_map


def detect_running_experiments_from_gpu_pids(
    pid_map: Dict[int, int],
) -> Dict[str, List[Dict[str, Any]]]:
    """Best-effort detection of running experiments from GPU PIDs.

    This is display-only (dashboard aid). It must not mutate registry state.
    """
    import re

    detected: Dict[str, List[Dict[str, Any]]] = {}
    for pid, used_mb in pid_map.items():
        try:
            cmdline = (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode("utf-8", errors="replace")
            )
        except Exception:
            continue

        m = re.search(
            r"(?:^|\s)(?:\S+/)?Phase3/experiments/([^/]+)/scripts/train\.py", cmdline
        )
        if not m:
            m = re.search(
                r"(?:^|\s)(?:\S+/)?experiments/([^/]+)/scripts/train\.py", cmdline
            )
        exp_name: Optional[str] = None
        if m:
            exp_name = m.group(1)
        elif "train_ensemble_member.py" in cmdline:
            arg_match = re.search(r"--experiment-name(?:=|\s+)([^\s]+)", cmdline)
            if arg_match:
                exp_name = arg_match.group(1).strip().strip("\"'")

        if not exp_name:
            continue
        detected.setdefault(exp_name, []).append({"pid": pid, "used_mb": used_mb})

    return detected


def _build_worker_gpu_free_maps(
    cluster_status: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[int, int]], int]:
    worker_gpu_free: Dict[str, Dict[int, int]] = {}
    global_best_free_mb = 0
    for worker_id, info in (cluster_status or {}).items():
        gpu_map: Dict[int, int] = {}
        for gpu in info.get("gpus", []) or []:
            try:
                gpu_idx = int(gpu.get("index"))
            except (TypeError, ValueError):
                continue
            free_mb = _coerce_int(gpu.get("free"))
            gpu_map[gpu_idx] = free_mb
            global_best_free_mb = max(global_best_free_mb, free_mb)
        worker_gpu_free[str(worker_id)] = gpu_map
    return worker_gpu_free, global_best_free_mb


def _best_free_mb_for_worker(
    worker_gpu_free: Dict[str, Dict[int, int]], worker_id: Optional[str]
) -> int:
    if not worker_id:
        return 0
    gpu_map = worker_gpu_free.get(str(worker_id), {})
    return max(gpu_map.values(), default=0)


def _free_mb_for_worker_gpu(
    worker_gpu_free: Dict[str, Dict[int, int]], worker_id: Optional[str], gpu_id: Any
) -> int:
    if not worker_id:
        return 0
    gpu_map = worker_gpu_free.get(str(worker_id), {})
    try:
        gpu_idx = int(gpu_id)
    except (TypeError, ValueError):
        return max(gpu_map.values(), default=0)
    return _coerce_int(gpu_map.get(gpu_idx))
