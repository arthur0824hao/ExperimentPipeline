#!/usr/bin/env python3

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from gpu import get_pid_gpu_map, detect_running_experiments_from_gpu_pids
from memory_contract import (
    _copy_memory_contract,
    _update_oom_policy_contract,
    _persist_oom_policy_contract,
)
from oom import (
    parse_oom_from_stderr,
    _resolve_batch_overrides,
    _next_smaller_batches,
    MIN_RUNTIME_BATCH_SIZE,
    OOM_RETRY_EST_MEM_BUMP_MB,
    OOM_EXPECTED_FREE_MARGIN_MB,
)
from artifact import (
    _read_result_payload,
    _read_resource_usage,
    _best_error_peak_mb,
    update_running_peak,
)
from dashboard_input import read_dashboard_key
from formatting import normalize_status
from db_registry import DBExperimentsDB
from runtime_config import cfg_bool, cfg_float, cfg_int, get_runtime_section


BASE_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = BASE_DIR.parent
RESULTS_DB_DIR = BASE_DIR / "results_db"
LOGS_DIR = BASE_DIR / "logs"
_RUNNER_CFG = get_runtime_section("experiments_runner")

OOM_THRESHOLD_MB = cfg_int(_RUNNER_CFG, "true_oom_threshold_mb", 24000)
MAX_RETRY_COUNT = cfg_int(_RUNNER_CFG, "max_retry_count", 2)
ARTIFACT_RECONCILE_GRACE_SEC = cfg_float(
    _RUNNER_CFG, "artifact_reconcile_grace_sec", 120.0
)
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
MEMORY_CHECK_INTERVAL = cfg_int(_RUNNER_CFG, "memory_check_interval_sec", 3)
GPU_JOB_COUNT_MIN_MEMORY_MB = cfg_int(_RUNNER_CFG, "gpu_job_count_min_memory_mb", 512)

STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"


ExperimentsDB = DBExperimentsDB


def _build_terminal_metadata(
    result_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    payload = result_payload or {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "child_returncode": payload.get("child_returncode"),
        "child_failure_type": payload.get("child_failure_type"),
        "ownership_verdict": payload.get("ownership_verdict"),
        "failure_fingerprint": payload.get("failure_fingerprint"),
        "result_status": payload.get("status"),
    }


def _build_canonical_result(result_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = result_payload or {}
    if not isinstance(payload, dict):
        payload = {}
    canonical: Dict[str, Any] = {}
    for key in ("test_f1", "test_auc", "epochs_ran"):
        if key in payload:
            canonical[key] = payload.get(key)
    return canonical


def _clean_experiment_artifacts(exp_name: str) -> List[str]:
    exp_dir = BASE_DIR / "experiments" / exp_name
    targets = [
        exp_dir / ".progress",
        exp_dir / "resource_usage.json",
        exp_dir / "outputs",
        exp_dir / "results_db",
        exp_dir / "checkpoints",
        RESULTS_DB_DIR / f"{exp_name}.json",
        LOGS_DIR / f"{exp_name}.out",
        LOGS_DIR / f"{exp_name}.err",
    ]
    pycache_dirs = list(exp_dir.rglob("__pycache__"))
    pyc_files = list(exp_dir.rglob("*.pyc"))
    targets.extend(pycache_dirs)
    targets.extend(pyc_files)
    for scripts_pycache in (BASE_DIR / "scripts").rglob("__pycache__"):
        targets.append(scripts_pycache)
    removed: List[str] = []
    for path in targets:
        try:
            if path.is_dir():
                for attempt in range(2):
                    try:
                        shutil.rmtree(path)
                        break
                    except OSError as e:
                        if e.errno != 39 or attempt == 1:
                            raise
                        time.sleep(0.1)
                removed.append(str(path.relative_to(BASE_DIR)))
            elif path.exists():
                path.unlink()
                removed.append(str(path.relative_to(BASE_DIR)))
        except FileNotFoundError:
            continue
    return removed


def _clear_runtime_markers(exp_name: str) -> List[str]:
    exp_dir = BASE_DIR / "experiments" / exp_name
    targets = [
        exp_dir / ".progress",
        exp_dir / "resource_usage.json",
        LOGS_DIR / f"{exp_name}.out",
        LOGS_DIR / f"{exp_name}.err",
    ]
    removed: List[str] = []
    for path in targets:
        try:
            if path.exists() and path.is_file():
                path.unlink()
                removed.append(str(path.relative_to(BASE_DIR)))
        except FileNotFoundError:
            continue
    return removed


def mark_running(
    db: ExperimentsDB, exp_name: str, worker_hostname: str, gpu_id: int, pid: int
):
    return db.mark_running(exp_name, worker_hostname, gpu_id, pid)


def mark_done(db: ExperimentsDB, exp_name: str, result: Dict, run_id: str):
    return db.mark_done(exp_name, result, run_id)


def mark_error(
    db: ExperimentsDB,
    exp_name: str,
    error_type: str,
    message: str,
    is_true_oom: bool = False,
    peak_memory_mb: int = 0,
    run_id: Optional[str] = None,
):
    return db.mark_error(
        exp_name,
        error_type,
        message,
        is_true_oom,
        peak_memory_mb,
        run_id,
    )


def update_lock_pid(exp_name: str, worker_hostname: str, pid: int, gpu_id: int):
    return


def release_distributed_lock(exp_name: str):
    return


def run_experiment_process(
    exp_config: Dict,
    worker_hostname: str,
    gpu_id: int,
    logger,
    db: ExperimentsDB,
    running_processes: Optional[Dict[str, subprocess.Popen]] = None,
    running_processes_lock: Optional[threading.Lock] = None,
    python_env: Optional[str] = None,
):
    exp_name = exp_config["name"]
    script_path = exp_config.get("script", f"experiments/{exp_name}/scripts/train.py")
    full_script_path = BASE_DIR / script_path

    if not full_script_path.exists():
        logger.log(f"Script not found: {full_script_path}")
        run_id = db.get_run_id(exp_name)
        mark_error(
            db,
            exp_name,
            "SCRIPT_ERROR",
            f"Script not found: {script_path}",
            run_id=run_id,
        )
        return

    runtime_removed = _clear_runtime_markers(exp_name)
    if runtime_removed:
        logger.log(
            f"Reset runtime markers for {exp_name}: {len(runtime_removed)} removed"
        )
    logger.log(f"Starting {exp_name} on GPU {gpu_id}...")
    stdout_log = LOGS_DIR / f"{exp_name}.out"
    stderr_log = LOGS_DIR / f"{exp_name}.err"
    current_batch_size, current_eval_batch_size = _resolve_batch_overrides(
        exp_name, exp_config, full_script_path
    )
    memory_contract = _copy_memory_contract(exp_config)
    if memory_contract:
        exp_config["memory_contract"] = memory_contract
    soft_oom_retries = 0
    max_soft_oom_retries = max(
        0, int(exp_config.get("max_retries", MAX_RETRY_COUNT) or MAX_RETRY_COUNT)
    )
    run_id: Optional[str] = None
    peak_memory_mb = int(exp_config.get("peak_memory_mb", 0) or 0)

    with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
        python_path = os.path.expanduser(
            python_env or "~/miniconda3/envs/gnn_fraud/bin/python"
        )
        while True:
            env = os.environ.copy()
            env["CUDA_VISIBLE_DEVICES"] = str(gpu_id)
            env["BATCH_SIZE"] = str(current_batch_size)
            env["EVAL_BATCH_SIZE"] = str(current_eval_batch_size)
            extra_env = exp_config.get("env") or {}
            if isinstance(extra_env, dict):
                for key, value in extra_env.items():
                    if key is None or value is None:
                        continue
                    env[str(key)] = str(value)
            out.write(
                f"[Runner] launch attempt={soft_oom_retries + 1} BATCH_SIZE={current_batch_size} EVAL_BATCH_SIZE={current_eval_batch_size}\n"
            )
            out.flush()

            process = subprocess.Popen(
                [python_path, str(full_script_path)],
                cwd=BASE_DIR,
                env=env,
                stdout=out,
                stderr=err,
                text=True,
                start_new_session=True,
            )

            if running_processes is not None:
                if running_processes_lock is not None:
                    with running_processes_lock:
                        running_processes[exp_name] = process
                else:
                    running_processes[exp_name] = process

            if not run_id:
                run_id = db.get_run_id(exp_name)
                if not run_id:
                    run_id = mark_running(
                        db, exp_name, worker_hostname, gpu_id, process.pid
                    )
                if not run_id:
                    logger.log(f"Claim failed for {exp_name}. Terminating process.")
                    try:
                        process.terminate()
                    except Exception:
                        pass
                    return
            else:
                db.update_experiment(
                    exp_name,
                    {
                        "running_on": {
                            "worker": worker_hostname,
                            "gpu": gpu_id,
                            "pid": process.pid,
                            "started_at": datetime.now().isoformat(),
                            "peak_memory_mb": peak_memory_mb,
                        }
                    },
                )

            update_lock_pid(exp_name, worker_hostname, process.pid, gpu_id)
            last_memory_check = 0

            while True:
                retcode = process.poll()
                if retcode is not None:
                    break

                current_time = time.time()
                if current_time - last_memory_check >= MEMORY_CHECK_INTERVAL:
                    pid_map = get_pid_gpu_map()
                    current_mem = pid_map.get(process.pid, 0)
                    if current_mem <= 0:
                        try:
                            detected = detect_running_experiments_from_gpu_pids(pid_map)
                            inferred = detected.get(exp_name, [])
                            if inferred:
                                current_mem = max(
                                    int(item.get("used_mb", 0)) for item in inferred
                                )
                        except Exception:
                            current_mem = 0
                    peak_memory_mb = max(peak_memory_mb, current_mem)
                    update_running_peak(db, exp_name, peak_memory_mb)
                    last_memory_check = current_time

                time.sleep(1)

            if retcode == 0:
                logger.log(f"Finished {exp_name} successfully.")

                result_file = RESULTS_DB_DIR / f"{exp_name}.json"
                result = {
                    "peak_memory_mb": peak_memory_mb,
                    "batch_size": current_batch_size,
                    "eval_batch_size": current_eval_batch_size,
                }
                if result_file.exists():
                    try:
                        with open(result_file, "r") as f:
                            result.update(json.load(f))
                    except Exception:
                        pass

                if result.get("f1_score") is None:
                    preferred_f1 = result.get("test_f1")
                    if preferred_f1 is not None:
                        result["f1_score"] = preferred_f1

                if result.get("auc_score") is None:
                    preferred_auc = result.get("test_auc")
                    if preferred_auc is not None:
                        result["auc_score"] = preferred_auc

                db.update_experiment(
                    exp_name,
                    {
                        "extra": {
                            "canonical_result": _build_canonical_result(result),
                            "terminal_metadata": _build_terminal_metadata(result),
                        }
                    },
                )

                done_ok = mark_done(db, exp_name, result, run_id)
                if not done_ok:
                    latest_run_id = db.get_run_id_db(exp_name)
                    if latest_run_id and latest_run_id != run_id:
                        logger.log(
                            f"mark_done fencing mismatch for {exp_name}; retry with latest run_id {latest_run_id[:8]}"
                        )
                        done_ok = mark_done(db, exp_name, result, latest_run_id)
                if not done_ok:
                    logger.log(
                        f"mark_done failed for {exp_name}; experiment may be reset by zombie guard"
                    )
                break

            logger.log(f"Failed {exp_name} with code {retcode}.")

            is_oom, is_true_oom, requested_mb = parse_oom_from_stderr(stderr_log)
            resource_path, resource_payload = _read_resource_usage(exp_name)
            result_path, result_payload = _read_result_payload(exp_name)
            if (not is_oom) and isinstance(resource_payload, dict):
                resource_status = str(resource_payload.get("status") or "").upper()
                resource_error = str(resource_payload.get("error_type") or "").upper()
                if (
                    bool(resource_payload.get("is_oom"))
                    or resource_status == "OOM"
                    or resource_error == "OOM"
                ):
                    is_oom = True
                    requested_mb = int(resource_payload.get("peak_memory_mb") or 0)

            if is_oom:
                requested_mb_int = int(requested_mb or 0)
                expected_base_mb = _best_error_peak_mb(
                    int(peak_memory_mb),
                    requested_mb_int,
                    resource_payload,
                    result_payload,
                )
                expected_required_free_mb = (
                    expected_base_mb + OOM_EXPECTED_FREE_MARGIN_MB
                )
                runtime_batch_adjustable = bool(
                    memory_contract.get("runtime_batch_adjustable", True)
                )
                oom_policy_mode = str(
                    memory_contract.get("oom_policy_mode") or "batch_adjustable"
                )
                if (
                    requested_mb_int > OOM_THRESHOLD_MB
                    or expected_required_free_mb > OOM_THRESHOLD_MB
                ):
                    is_true_oom = True
                peak_for_error = expected_base_mb
                err_kind = "OOM"
                err_message = (
                    str(resource_payload.get("error_message") or "").strip()
                    if isinstance(resource_payload, dict)
                    else ""
                )
                if not err_message:
                    err_message = f"CUDA OOM (peak: {peak_memory_mb}MB, requested: {requested_mb}MB, expected_free: {expected_required_free_mb}MB)"

                next_batch_size, next_eval_batch_size = _next_smaller_batches(
                    current_batch_size, current_eval_batch_size
                )

                if not runtime_batch_adjustable and oom_policy_mode == "not_applicable":
                    old_est = int(
                        memory_contract.get("est_mem_decision_mb")
                        or memory_contract.get("est_mem_upper_mb")
                        or 0
                    )
                    retry_est_mb = max(old_est + OOM_RETRY_EST_MEM_BUMP_MB, old_est + 1)
                    force_true_mem = retry_est_mb > OOM_THRESHOLD_MB
                    memory_contract = _update_oom_policy_contract(
                        memory_contract,
                        current_batch_size=current_batch_size,
                        current_eval_batch_size=current_eval_batch_size,
                        next_batch_size=current_batch_size,
                        next_eval_batch_size=current_eval_batch_size,
                        expected_required_free_mb=retry_est_mb,
                        stop_reason=(
                            "no_batch_path_estmem_threshold_exceeded"
                            if force_true_mem
                            else "no_batch_path_retry_with_higher_estmem"
                        ),
                        force_true_mem=force_true_mem,
                    )
                    exp_config["memory_contract"] = memory_contract
                    _persist_oom_policy_contract(
                        db, exp_name, memory_contract, soft_oom_retries + 1
                    )
                    if not force_true_mem:
                        logger.log(
                            f"Soft OOM for {exp_name}; bumping est_mem_decision_mb {old_est}->{retry_est_mb} and requeueing"
                        )
                    is_true_oom = force_true_mem
                elif runtime_batch_adjustable and not is_true_oom:
                    candidate_contract = _update_oom_policy_contract(
                        memory_contract,
                        current_batch_size=current_batch_size,
                        current_eval_batch_size=current_eval_batch_size,
                        next_batch_size=next_batch_size,
                        next_eval_batch_size=next_eval_batch_size,
                        expected_required_free_mb=expected_required_free_mb,
                        stop_reason="retry_with_smaller_batch",
                        force_true_mem=False,
                    )
                    candidate_est = int(
                        candidate_contract.get("est_mem_after_retry")
                        or candidate_contract.get("est_mem_decision_mb")
                        or 0
                    )
                    can_retry_with_smaller_batch = (
                        soft_oom_retries < max_soft_oom_retries
                        and current_batch_size > MIN_RUNTIME_BATCH_SIZE
                        and next_batch_size < current_batch_size
                        and candidate_est <= OOM_THRESHOLD_MB
                    )
                    if can_retry_with_smaller_batch:
                        memory_contract = candidate_contract
                        exp_config["memory_contract"] = memory_contract
                        _persist_oom_policy_contract(
                            db,
                            exp_name,
                            memory_contract,
                            soft_oom_retries + 1,
                        )
                        logger.log(
                            f"Soft OOM for {exp_name}; retrying with smaller batch {current_batch_size}->{next_batch_size}, eval {current_eval_batch_size}->{next_eval_batch_size}"
                        )
                        current_batch_size = next_batch_size
                        current_eval_batch_size = next_eval_batch_size
                        soft_oom_retries += 1
                        continue

                    force_true_mem = candidate_est > OOM_THRESHOLD_MB
                    stop_reason = (
                        "estmem_threshold_exceeded"
                        if force_true_mem
                        else "batch_floor_reached_below_trueoom_threshold"
                    )
                    memory_contract = _update_oom_policy_contract(
                        memory_contract,
                        current_batch_size=current_batch_size,
                        current_eval_batch_size=current_eval_batch_size,
                        next_batch_size=next_batch_size,
                        next_eval_batch_size=next_eval_batch_size,
                        expected_required_free_mb=expected_required_free_mb,
                        stop_reason=stop_reason,
                        force_true_mem=force_true_mem,
                    )
                    exp_config["memory_contract"] = memory_contract
                    _persist_oom_policy_contract(
                        db, exp_name, memory_contract, soft_oom_retries + 1
                    )
                    is_true_oom = force_true_mem

                err_ok = mark_error(
                    db,
                    exp_name,
                    err_kind,
                    err_message,
                    is_true_oom,
                    peak_for_error,
                    run_id,
                )
                db.update_experiment(
                    exp_name,
                    {
                        "extra": {
                            "terminal_metadata": _build_terminal_metadata(
                                _read_result_payload(exp_name)[1]
                            )
                        }
                    },
                )
                if not err_ok:
                    latest_run_id = db.get_run_id_db(exp_name)
                    if latest_run_id and latest_run_id != run_id:
                        err_ok = mark_error(
                            db,
                            exp_name,
                            err_kind,
                            err_message,
                            is_true_oom,
                            peak_for_error,
                            latest_run_id,
                        )
                if not err_ok:
                    logger.log(f"mark_error failed for {exp_name} (OOM)")
                break

            try:
                with open(stderr_log, "r") as f:
                    lines = f.readlines()
                    error_msg = "".join(lines[-20:])
            except Exception:
                error_msg = f"Return code: {retcode}"
            err_ok = mark_error(
                db,
                exp_name,
                "SCRIPT_ERROR",
                error_msg,
                False,
                peak_memory_mb,
                run_id,
            )
            db.update_experiment(
                exp_name,
                {
                    "extra": {
                        "terminal_metadata": _build_terminal_metadata(
                            _read_result_payload(exp_name)[1]
                        )
                    }
                },
            )
            if not err_ok:
                latest_run_id = db.get_run_id_db(exp_name)
                if latest_run_id and latest_run_id != run_id:
                    err_ok = mark_error(
                        db,
                        exp_name,
                        "SCRIPT_ERROR",
                        error_msg,
                        False,
                        peak_memory_mb,
                        latest_run_id,
                    )
            if not err_ok:
                logger.log(f"mark_error failed for {exp_name} (SCRIPT_ERROR)")
            break


def get_key(input_stream=None):
    return read_dashboard_key(input_stream)
