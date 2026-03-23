import os
import signal
import subprocess
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

try:
    from runtime_config import cfg_bool, cfg_float, cfg_int, get_runtime_section
except ModuleNotFoundError:
    from .runtime_config import cfg_bool, cfg_float, cfg_int, get_runtime_section

try:
    import gpu as gpu_module
except ModuleNotFoundError:
    from . import gpu as gpu_module

try:
    from artifact import get_experiment_progress
except ModuleNotFoundError:
    from .artifact import get_experiment_progress


_RUNNER_CFG = get_runtime_section("experiments_runner")
MAX_JOBS_PER_GPU = cfg_int(_RUNNER_CFG, "default_max_jobs_per_gpu", 1)
GPU_PROCESS_WARMUP_SEC = cfg_float(_RUNNER_CFG, "gpu_process_warmup_sec", 180.0)
WARMUP_COMPLETION_EPOCH = cfg_int(_RUNNER_CFG, "warmup_completion_epoch", 1)
ALLOW_WARMUP_OVERLAP = cfg_bool(_RUNNER_CFG, "allow_warmup_overlap", True)
MAX_PARALLEL_WARMUP_JOBS_PER_GPU = cfg_int(
    _RUNNER_CFG, "max_parallel_warmup_jobs_per_gpu", 1
)
WARMUP_OVERLAP_BYPASS_HIGH_MEM_EXCLUSIVE = cfg_bool(
    _RUNNER_CFG, "warmup_overlap_bypass_high_mem_exclusive", True
)
GPU_CLAIM_HEADROOM_MB = cfg_int(_RUNNER_CFG, "gpu_claim_headroom_mb", 1024)
HIGH_MEM_EXCLUSIVE_THRESHOLD_MB = cfg_int(
    _RUNNER_CFG, "high_mem_exclusive_threshold_mb", 21000
)
HIGH_MEM_EXCLUSIVE_RATIO = cfg_float(_RUNNER_CFG, "high_mem_exclusive_ratio", 0.85)


def get_all_gpu_status():
    getter = getattr(gpu_module, "get_all_gpu_status", None)
    if callable(getter):
        return getter()
    gpus, _probe_error = gpu_module.collect_gpu_status_with_error()
    return gpus


class GPUAllocator:
    def __init__(
        self,
        max_jobs_per_gpu=MAX_JOBS_PER_GPU,
        max_gpus: Optional[int] = None,
        preferred_gpu: Optional[int] = None,
    ):
        self.max_jobs = max_jobs_per_gpu
        self.max_gpus = max_gpus
        self.preferred_gpu = preferred_gpu
        self.gpu_jobs = {}
        self.gpu_job_assigned_at: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._refresh_gpus()

    def _refresh_gpus(self):
        self.gpus = get_all_gpu_status()
        # If preferred_gpu is set, use ONLY that GPU (critical for minun hardware constraint)
        if self.preferred_gpu is not None:
            self.gpus = [g for g in self.gpus if g["index"] == self.preferred_gpu]
        elif self.max_gpus is not None:
            self.gpus = [g for g in self.gpus if g["index"] < self.max_gpus]
        for g in self.gpus:
            if g["index"] not in self.gpu_jobs:
                self.gpu_jobs[g["index"]] = []

        active_indices = {g["index"] for g in self.gpus}
        stale_indices = [idx for idx in self.gpu_jobs if idx not in active_indices]
        for idx in stale_indices:
            del self.gpu_jobs[idx]

    def _is_warmup_complete(self, exp_name: str) -> bool:
        progress = get_experiment_progress(exp_name)
        if not isinstance(progress, dict):
            return False
        try:
            epoch = int(progress.get("epoch", 0) or 0)
        except (TypeError, ValueError):
            return False
        return epoch >= WARMUP_COMPLETION_EPOCH

    def allocate(self, exp_name, required_mem_mb=4000) -> Optional[int]:
        with self._lock:
            self._refresh_gpus()
            real_process_counts = gpu_module.get_gpu_process_count()

            candidates = []
            for g in self.gpus:
                idx = g["index"]
                tracked_names = list(self.gpu_jobs.get(idx, []))
                tracked_jobs = len(tracked_names)
                real_processes = real_process_counts.get(idx, 0)
                effective_jobs = max(tracked_jobs, real_processes)
                finished_warmup = [
                    name for name in tracked_names if self._is_warmup_complete(name)
                ]
                unfinished_warmup = [
                    name for name in tracked_names if not self._is_warmup_complete(name)
                ]
                allow_extra_warmup = (
                    ALLOW_WARMUP_OVERLAP
                    and MAX_PARALLEL_WARMUP_JOBS_PER_GPU > 0
                    and len(unfinished_warmup) < MAX_PARALLEL_WARMUP_JOBS_PER_GPU
                )
                allowed_job_limit = self.max_jobs + (
                    MAX_PARALLEL_WARMUP_JOBS_PER_GPU if allow_extra_warmup else 0
                )

                if tracked_jobs > 0 and unfinished_warmup and not allow_extra_warmup:
                    continue

                if effective_jobs >= allowed_job_limit:
                    continue

                total_mb = int(float(g.get("total", 0) or 0))
                ratio_exclusive_mb = int(total_mb * HIGH_MEM_EXCLUSIVE_RATIO)
                if (
                    required_mem_mb >= HIGH_MEM_EXCLUSIVE_THRESHOLD_MB
                    or required_mem_mb >= ratio_exclusive_mb
                ) and effective_jobs > 0:
                    if not (
                        allow_extra_warmup and WARMUP_OVERLAP_BYPASS_HIGH_MEM_EXCLUSIVE
                    ):
                        continue

                if g["free"] > (required_mem_mb + GPU_CLAIM_HEADROOM_MB):
                    candidates.append(g)

            if not candidates:
                return None

            candidates.sort(
                key=lambda x: (len(self.gpu_jobs.get(x["index"], [])), -x["free"])
            )

            best_gpu = candidates[0]["index"]
            self.gpu_jobs[best_gpu].append(exp_name)
            self.gpu_job_assigned_at[exp_name] = time.time()
            return best_gpu

    def release(self, exp_name):
        with self._lock:
            for idx in self.gpu_jobs:
                if exp_name in self.gpu_jobs[idx]:
                    self.gpu_jobs[idx].remove(exp_name)
                    self.gpu_job_assigned_at.pop(exp_name, None)
                    return
            self.gpu_job_assigned_at.pop(exp_name, None)


def enforce_formal_slot_serialization(
    running_processes: Dict[str, subprocess.Popen],
    running_processes_lock: threading.Lock,
    running_gpu_ids: Dict[str, int],
    running_gpu_ids_lock: threading.Lock,
    allocator: GPUAllocator,
    paused_formal_jobs: Set[str],
    logger,
) -> None:
    with running_processes_lock:
        proc_snapshot = {
            name: proc
            for name, proc in running_processes.items()
            if proc.poll() is None
        }
    with running_gpu_ids_lock:
        gpu_snapshot = dict(running_gpu_ids)

    current_names = set(proc_snapshot.keys())
    for stale_name in list(paused_formal_jobs):
        if stale_name not in current_names:
            paused_formal_jobs.discard(stale_name)

    # Group ALL running jobs by GPU (including warmup-incomplete ones)
    # so SE can enforce serialization across the full lifecycle.
    jobs_by_gpu: Dict[int, List[Tuple[float, str, subprocess.Popen]]] = {}
    for name, proc in proc_snapshot.items():
        gpu_id = gpu_snapshot.get(name)
        if gpu_id is None:
            continue
        assigned_at = float(allocator.gpu_job_assigned_at.get(name, 0.0) or 0.0)
        jobs_by_gpu.setdefault(int(gpu_id), []).append((assigned_at, name, proc))

    desired_paused: Set[str] = set()
    for _gpu_id, jobs in jobs_by_gpu.items():
        if len(jobs) < 2:
            continue
        jobs.sort(key=lambda item: (item[0], item[1]))
        # Check if the oldest (first-assigned) job has reached stable epoch
        first_name = jobs[0][1]
        first_stable = False
        first_progress = get_experiment_progress(first_name)
        if isinstance(first_progress, dict):
            try:
                first_stable = (
                    int(first_progress.get("epoch", 0) or 0) >= WARMUP_COMPLETION_EPOCH
                )
            except (TypeError, ValueError):
                pass
        # If oldest job is not yet stable, pause all others on this GPU
        if not first_stable:
            for _assigned_at, name, _proc in jobs[1:]:
                desired_paused.add(name)

    for name, proc in proc_snapshot.items():
        if name in desired_paused and name not in paused_formal_jobs:
            try:
                os.kill(proc.pid, signal.SIGSTOP)
                paused_formal_jobs.add(name)
                logger.log(
                    f"Paused formal-overlap job {name} until earlier formal slot frees"
                )
            except Exception as exc:
                logger.log(f"Failed to pause overlap job {name}: {exc}")

    for name in list(paused_formal_jobs):
        if name in desired_paused:
            continue
        proc = proc_snapshot.get(name)
        if proc is None:
            paused_formal_jobs.discard(name)
            continue
        try:
            os.kill(proc.pid, signal.SIGCONT)
            logger.log(f"Resumed formal-overlap job {name} after slot became free")
        except Exception as exc:
            logger.log(f"Failed to resume overlap job {name}: {exc}")
        finally:
            paused_formal_jobs.discard(name)
