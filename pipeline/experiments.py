#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 3 Unified Runner & Dashboard (v4.0 — PostgreSQL-backed)

Usage:
    python experiments.py              # Full mode: Dashboard + Worker
    python experiments.py --watch      # Watch-only mode: Dashboard without worker

State is stored in PostgreSQL (exp_registry schema in FraudDetect-experiment DB).
experiments.json is synced as a read-only snapshot for backward compat.
"""

import os
import sys
import json
import time
import argparse
import subprocess
import platform
import threading
import queue
import select
import shlex
import shutil
import tty
import re
import importlib
import importlib.util
import termios
import signal
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple, Set, cast
from concurrent.futures import ThreadPoolExecutor, Future

from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.console import Group
from rich import box
from rich.text import Text
from rich.markup import escape
from rich.columns import Columns

from cli_shared import add_common_args, emit_result, setup_logging
from db_registry import DBExperimentsDB, derive_progression_status, get_conn
from experiment_registration import build_experiment_config, register_experiment
from condition import (
    _normalize_name_list,
    _load_condition_nodes_from_runtime,
    RUNTIME_CONDITION_NODES,
    _load_staged_matrix_entries,
    RUNTIME_STAGED_MATRIX,
    _resolve_gate_evidence_status,
    _build_condition_node_rows,
    _build_staged_matrix_rows,
)
from oom import (
    parse_oom_from_stderr,
    _coerce_positive_int,
    _script_env_default,
    _resolve_batch_overrides,
    _next_smaller_batches,
    MIN_RUNTIME_BATCH_SIZE,
    OOM_RETRY_EST_MEM_BUMP_MB,
    OOM_EXPECTED_FREE_MARGIN_MB,
)
from runtime_config import (
    cfg_bool,
    cfg_float,
    cfg_int,
    get_runtime_section,
)
from tui_keys import Action, SCOPE_KEYS, TwoStepKeyHandler
from gpu import (
    get_all_gpu_status,
    _coerce_nvidia_int,
    _parse_nvidia_query_output,
    collect_gpu_status_with_error,
    get_cpu_load,
    collect_system_info,
    get_gpu_process_count,
    get_pid_gpu_map,
    detect_running_experiments_from_gpu_pids,
    _build_worker_gpu_free_maps,
    _best_free_mb_for_worker,
    _free_mb_for_worker_gpu,
)
from artifact import (
    _artifact_timestamp,
    _failed_timestamp,
    _artifact_is_fresh,
    _stderr_is_empty,
    _read_resource_usage,
    _read_result_payload,
    _coerce_completed_result,
    _coerce_float,
    _coerce_int,
    _extract_peak_from_payload,
    _best_error_peak_mb,
    update_running_peak,
    get_experiment_progress,
    get_completed_result_summary,
    get_terminal_reason,
)
from formatting import (
    format_time_ago,
    make_bar,
    make_status_badge,
    _parse_iso_ts,
    _SPINNER_FRAMES,
    _render_wait_progress,
    normalize_status,
    format_terminal_reason_text,
    normalize_initial_exp_page,
)
from memory_contract import (
    _copy_memory_contract,
    _update_oom_policy_contract,
    _persist_oom_policy_contract,
    _should_reestimate_memory_contract,
    get_memory_contract,
    format_memory_contract_fields,
    get_required_mem_mb,
)
from allocator import GPUAllocator, enforce_formal_slot_serialization
from cluster import ClusterManager
from logger_hybrid import HybridLogger
from worker import (
    run_experiment_process,
    mark_running,
    mark_done,
    mark_error,
    update_lock_pid,
    release_distributed_lock,
    _clean_experiment_artifacts,
    _clear_runtime_markers,
    get_key,
)
from health import (
    cleanup_on_startup,
    enforce_running_pid_registration,
    reap_orphan_runner_processes,
    reap_orphan_training_processes,
    check_stale_locks,
    self_heal_heartbeat_worker_conflicts,
    check_zombie_processes,
    process_remote_termination_requests,
    _get_active_runner_pids_from_db,
    _kill_local_pid_tree,
    _extract_exp_name_from_cmd,
)


def _should_fallback_memory_estimator_import(exc: Exception) -> bool:
    if not isinstance(exc, (ImportError, ModuleNotFoundError, OSError)):
        return False
    msg = f"{type(exc).__name__}: {exc}"
    markers = ("libtorch_global_deps.so", "preprocess_lib/__init__.py", "torch")
    return any(marker in msg for marker in markers)


PREPROCESS_LIB_DIR = Path(__file__).resolve().parent / "preprocess_lib"

try:
    from preprocess_lib.memory_estimator import infer_memory_contract_for_exp
except Exception as exc:
    if not _should_fallback_memory_estimator_import(exc):
        raise
    if str(PREPROCESS_LIB_DIR) not in sys.path:
        sys.path.insert(0, str(PREPROCESS_LIB_DIR))
    _memory_estimator = importlib.import_module("memory_estimator")
    infer_memory_contract_for_exp = _memory_estimator.infer_memory_contract_for_exp

BASE_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = BASE_DIR.parent
LOCKS_DIR = BASE_DIR / "locks"
RESULTS_DB_DIR = BASE_DIR / "results_db"
EXPERIMENTS_FILE = BASE_DIR / "experiments.json"
LOGS_DIR = BASE_DIR / "logs"
RUNNER_LOG_FILE = BASE_DIR / "runner.log"
_DEFAULT_MACHINES_FILE = PROJECT_ROOT / "configs" / "machines.json"
MACHINES_FILES = [
    PROJECT_ROOT / "configs" / "machines.json",
    PROJECT_ROOT / "configs" / "machines.phase3.json",
]
MACHINES_FILE = _DEFAULT_MACHINES_FILE
HEARTBEATS_DIR = BASE_DIR / "heartbeats"
PREPROCESS_PROGRESS_FILE = BASE_DIR / "preprocess_progress.json"
READY_QUEUE_FILE = BASE_DIR / "ready.json"
_RUNNER_CFG = get_runtime_section("experiments_runner")

MAX_JOBS_PER_GPU = cfg_int(_RUNNER_CFG, "default_max_jobs_per_gpu", 1)
OOM_THRESHOLD_MB = cfg_int(_RUNNER_CFG, "true_oom_threshold_mb", 24000)
AUTO_ARCHIVE_ENABLED = cfg_bool(_RUNNER_CFG, "auto_archive_enabled", False)
MAX_RETRY_COUNT = cfg_int(_RUNNER_CFG, "max_retry_count", 2)
HEARTBEAT_STALE_SEC = cfg_int(_RUNNER_CFG, "heartbeat_stale_sec", 120)
ORPHAN_REAPER_INTERVAL_SEC = cfg_int(_RUNNER_CFG, "orphan_reaper_interval_sec", 30)
ORPHAN_ETIMES_SEC = cfg_int(_RUNNER_CFG, "orphan_etimes_sec", 120)
ORPHAN_CONFIRMATION_SEC = cfg_int(_RUNNER_CFG, "orphan_confirmation_sec", 30)
ORPHAN_TRAINING_SEEN: Dict[int, float] = {}

STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"
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


RESULTS_DB_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
LOCKS_DIR.mkdir(exist_ok=True)

def _is_management_only_worker(worker_id: str, cluster_mgr: ClusterManager) -> bool:
    conf = cluster_mgr.machines.get(worker_id)
    if not isinstance(conf, dict):
        return False
    raw_max_gpus = conf.get("max_gpus")
    try:
        return int(raw_max_gpus) <= 0
    except (TypeError, ValueError):
        return False


# =============================================================================
# Experiment State Management (PostgreSQL-backed via db_registry.py)
# =============================================================================

ExperimentsDB = DBExperimentsDB


# =============================================================================
# GPU Utilities
# =============================================================================


def _is_truthy_flag(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"1", "true", "yes", "on"}


def _load_ready_queue_data(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"ready_to_process": 0, "experiments": [], "feature_jobs": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {"ready_to_process": 0, "experiments": [], "feature_jobs": []}

    if isinstance(payload, list):
        return {
            "ready_to_process": 1,
            "batch_id": f"legacy_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "experiments": payload,
            "feature_jobs": [],
        }
    if not isinstance(payload, dict):
        return {"ready_to_process": 0, "experiments": [], "feature_jobs": []}

    experiments = payload.get("experiments")
    if not isinstance(experiments, list):
        payload["experiments"] = []
    feature_jobs = payload.get("feature_jobs")
    if not isinstance(feature_jobs, list):
        payload["feature_jobs"] = []
    if "ready_to_process" not in payload:
        payload["ready_to_process"] = 0
    return payload


def _save_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp_path.replace(path)


def _archive_experiment_via_script(name: str) -> bool:
    target = str(name or "").strip()
    if not target:
        return False
    try:
        module_path = BASE_DIR.parent / "archive_script.py"
        spec = importlib.util.spec_from_file_location(
            "phase3_archive_script", module_path
        )
        if spec is None or spec.loader is None:
            return False
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        archive_selected = getattr(module, "archive_selected_experiments", None)
        if not callable(archive_selected):
            return False
        result = archive_selected([target])
        if not isinstance(result, dict):
            return False
        return int(result.get("count", 0) or 0) > 0
    except Exception:
        return False


def _delete_experiment_from_registry(db: Any, name: str) -> bool:
    target = str(name or "").strip()
    if not target:
        return False

    delete_fn = getattr(db, "delete_experiment", None)
    if callable(delete_fn):
        return bool(delete_fn(target))

    try:
        with get_conn(getattr(db, "dsn", None)) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM exp_registry.experiments WHERE name = %s",
                    (target,),
                )
                deleted = cur.rowcount > 0
        sync_snapshot = getattr(db, "_sync_snapshot", None)
        if callable(sync_snapshot):
            sync_snapshot()
        return deleted
    except Exception:
        return False


def _build_ready_queue_entry(exp: Dict[str, Any], name: str) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "name": name,
        "script": str(exp.get("script") or f"experiments/{name}/scripts/train.py"),
        "description": str(
            exp.get("description") or "Re-pipeline from experiments panel"
        ),
        "features_ready": True,
        "gate_status": "PASSED",
        "gate_passed_at": datetime.now().isoformat(),
    }

    for key in (
        "batch_id",
        "priority",
        "max_retries",
        "preferred_worker",
        "group_id",
        "parent_experiment",
        "role",
        "main_experiment",
        "condition_parent",
        "memory_contract",
        "env",
        "batch_size",
        "eval_batch_size",
    ):
        value = exp.get(key)
        if value is not None:
            entry[key] = value

    return entry


def _enqueue_repipeline_ready(name: str, exp: Dict[str, Any]) -> bool:
    target = str(name or "").strip()
    if not target:
        return False
    try:
        payload = _load_ready_queue_data(READY_QUEUE_FILE)
        queue_items = payload.get("experiments")
        if not isinstance(queue_items, list):
            queue_items = []
        deduped = [
            item
            for item in queue_items
            if not (
                isinstance(item, dict) and str(item.get("name") or "").strip() == target
            )
        ]
        deduped.append(_build_ready_queue_entry(exp, target))
        payload["experiments"] = deduped
        payload["ready_to_process"] = 1
        if not payload.get("batch_id"):
            payload["batch_id"] = (
                f"repipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
        _save_json_atomic(READY_QUEUE_FILE, payload)
        return True
    except Exception:
        return False


def _is_feature_ready_for_runner(exp: Dict[str, Any]) -> bool:
    if not isinstance(exp, dict):
        return False
    if _is_truthy_flag(exp.get("features_ready")):
        return True
    gate_status = str(exp.get("gate_status") or "").upper()
    if gate_status == "PASSED":
        return True
    return bool(exp.get("gate_passed_at"))


def _insert_registered_configs(db: Any, configs: List[Dict[str, Any]]) -> int:
    if not configs:
        return 0

    inserted = 0
    with get_conn(getattr(db, "dsn", None)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(display_order), 0) FROM exp_registry.experiments"
            )
            row = cur.fetchone()
            next_order = int(row[0] or 0) + 1 if row else 1

            for config in configs:
                extra: Dict[str, Any] = {
                    "priority": int(config.get("priority", 0) or 0),
                    "description": str(config.get("description") or ""),
                    "role": str(config.get("role") or ""),
                    "main_experiment": str(config.get("main_experiment") or ""),
                }
                memory_contract = config.get("memory_contract")
                if isinstance(memory_contract, dict) and memory_contract:
                    extra["memory_contract"] = dict(memory_contract)
                env_overrides = config.get("env")
                if isinstance(env_overrides, dict) and env_overrides:
                    extra["env"] = {
                        str(k): str(v)
                        for k, v in env_overrides.items()
                        if k is not None and v is not None
                    }
                if "batch_size" in config:
                    extra["batch_size"] = config.get("batch_size")
                if "eval_batch_size" in config:
                    extra["eval_batch_size"] = config.get("eval_batch_size")

                cur.execute(
                    """
                    INSERT INTO exp_registry.experiments (
                        name,
                        batch_id,
                        status,
                        script_path,
                        display_order,
                        max_retries,
                        preferred_worker,
                        group_id,
                        parent_experiment,
                        extra
                    )
                    VALUES (
                        %s, %s, 'NEEDS_RERUN', %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT (name) DO NOTHING
                    RETURNING name
                    """,
                    (
                        str(config.get("name") or "").strip(),
                        str(config.get("batch_id") or ""),
                        str(config.get("script") or ""),
                        next_order,
                        int(
                            config.get("max_retries", MAX_RETRY_COUNT)
                            or MAX_RETRY_COUNT
                        ),
                        str(config.get("preferred_worker") or "") or None,
                        str(config.get("group_id") or "") or None,
                        str(config.get("parent_experiment") or "") or None,
                        json.dumps(extra, ensure_ascii=False),
                    ),
                )
                if cur.fetchone():
                    inserted += 1
                next_order += 1

    sync_snapshot = getattr(db, "_sync_snapshot", None)
    if callable(sync_snapshot):
        sync_snapshot()
    return inserted


def consume_ready_queue_registration_handoff(
    db: Any,
    logger: Optional[Any] = None,
    ready_file: Optional[Path] = None,
) -> Dict[str, int]:
    queue_file = ready_file or READY_QUEUE_FILE
    ready_data = _load_ready_queue_data(queue_file)
    raw_queue = ready_data.get("experiments", [])
    if not isinstance(raw_queue, list) or not raw_queue:
        return {"registered": 0, "consumed": 0, "skipped_existing": 0}

    queue_items = [item for item in raw_queue if isinstance(item, dict)]
    existing = db.load()
    existing_names: Set[str] = set()
    for key in ("experiments", "completed", "archived"):
        bucket = existing.get(key, []) if isinstance(existing, dict) else []
        if not isinstance(bucket, list):
            continue
        for item in bucket:
            if isinstance(item, dict) and item.get("name"):
                existing_names.add(str(item["name"]))

    staged_names: Set[str] = set()
    configs_to_insert: List[Dict[str, Any]] = []
    remaining_items: List[Dict[str, Any]] = []
    consumed = 0
    skipped_existing = 0
    batch_id_fallback = str(
        ready_data.get("batch_id")
        or f"ready_handoff_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    for exp in queue_items:
        exp_name = str(exp.get("name") or "").strip()
        if not exp_name or not _is_feature_ready_for_runner(exp):
            remaining_items.append(exp)
            continue

        consumed += 1
        if exp_name in existing_names or exp_name in staged_names:
            skipped_existing += 1
            continue

        batch_id = str(exp.get("batch_id") or batch_id_fallback)
        config_data = register_experiment(exp, {"experiments": []}, batch_id)
        built = config_data.get("experiments", [])
        if not isinstance(built, list) or not built:
            continue
        configs_to_insert.append(built[-1])
        staged_names.add(exp_name)
        existing_names.add(exp_name)

    if consumed == 0:
        return {"registered": 0, "consumed": 0, "skipped_existing": 0}

    inserted = 0
    try:
        inserted = _insert_registered_configs(db, configs_to_insert)
    except Exception as e:
        if logger is not None:
            logger.log(f"Ready handoff registration failed: {e}")
        return {
            "registered": 0,
            "consumed": 0,
            "skipped_existing": skipped_existing,
        }

    ready_data["experiments"] = remaining_items
    if not remaining_items:
        ready_data["ready_to_process"] = 0
    _save_json_atomic(queue_file, ready_data)

    if logger is not None and (inserted > 0 or skipped_existing > 0):
        logger.log(
            "Ready handoff: "
            f"inserted={inserted}, skipped_existing={skipped_existing}, consumed={consumed}"
        )

    return {
        "registered": inserted,
        "consumed": consumed,
        "skipped_existing": skipped_existing,
    }


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


def _artifact_truth_mismatch(
    exp_name: str,
    status: str,
    result: Optional[Dict[str, Any]],
    error_info: Optional[Dict[str, Any]],
    terminal_metadata: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    result_path, result_payload = _read_result_payload(exp_name)
    if result_path is None or not isinstance(result_payload, dict):
        return None

    status_norm = normalize_status(status)
    db_terminal = _get_db_terminal_reason(
        status_norm, result or {}, error_info or {}, terminal_metadata or {}
    )
    artifact_returncode = result_payload.get("child_returncode")
    artifact_failure = str(result_payload.get("child_failure_type") or "").lower()
    artifact_verdict = str(result_payload.get("ownership_verdict") or "").lower()
    artifact_test_f1 = _coerce_float(result_payload.get("test_f1"))

    if status_norm == STATUS_COMPLETED:
        if (
            artifact_returncode not in (None, 0)
            or "oom" in artifact_failure
            or "oom" in artifact_verdict
        ):
            return "artifact_failed_vs_db_completed"
        db_f1 = _coerce_float((result or {}).get("f1_score"))
        if (
            db_f1 is not None
            and artifact_test_f1 is not None
            and abs(db_f1 - artifact_test_f1) > 1e-9
        ):
            return "artifact_metric_drift"
    if status_norm == STATUS_NEEDS_RERUN and db_terminal == "FAILED_SCRIPT_ERROR":
        if "oom" in artifact_failure or "oom" in artifact_verdict:
            return "artifact_oom_vs_db_script_error"
    return None


def _get_db_terminal_reason(
    status_norm: str,
    result: Optional[Dict[str, Any]],
    error_info: Optional[Dict[str, Any]],
    terminal_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    error_info = error_info or {}
    terminal_metadata = terminal_metadata or {}
    error_type = str(error_info.get("type") or "").upper()
    child_returncode = terminal_metadata.get("child_returncode")
    child_failure_type = str(terminal_metadata.get("child_failure_type") or "").lower()
    ownership_verdict = str(terminal_metadata.get("ownership_verdict") or "").lower()
    test_f1 = (result or {}).get("f1_score")
    test_auc = (result or {}).get("auc_score")

    if status_norm == STATUS_RUNNING:
        return "RUNNING"
    if status_norm == STATUS_COMPLETED:
        if child_returncode not in (None, 0):
            if "oom" in child_failure_type or "oom" in ownership_verdict:
                return "FAILED_OOM"
            return "FAILED_SCRIPT_ERROR"
        if "oom" in child_failure_type or "oom" in ownership_verdict:
            return "FAILED_OOM"
        if test_f1 is None and test_auc is None:
            return "FAILED_WITHOUT_METRIC"
        return "COMPLETED"
    if status_norm == STATUS_NEEDS_RERUN:
        if error_type == "MANUAL_FREEZE":
            return "FROZEN"
        if error_type == "OOM":
            return (
                "FAILED_OOM"
                if bool(error_info.get("is_true_oom", False))
                else "QUEUED_RETRY"
            )
        if error_type in {"SCRIPT_ERROR", "ZOMBIE", "PID_MISSING"}:
            return "FAILED_SCRIPT_ERROR"
        return "QUEUED_RETRY"
    return status_norm or "UNKNOWN"


def reconcile_terminal_artifacts(db: ExperimentsDB, logger=None) -> List[str]:
    snapshot = db.load()
    repaired: List[str] = []
    for exp in snapshot.get("experiments", []):
        if not isinstance(exp, dict):
            continue
        if normalize_status(exp.get("status")) != STATUS_NEEDS_RERUN:
            continue

        exp_name = str(exp.get("name") or "").strip()
        if not exp_name:
            continue

        error_info = exp.get("error_info") or {}
        error_type = str(error_info.get("type") or "").upper()
        failed_ts = _failed_timestamp(exp)

        resource_path, resource_payload = _read_resource_usage(exp_name)
        if (
            error_type in {"SCRIPT_ERROR", "ZOMBIE"}
            and resource_path is not None
            and isinstance(resource_payload, dict)
            and (
                _artifact_is_fresh(resource_path, failed_ts)
                or not str(error_info.get("message") or "").strip()
                or (error_type == "ZOMBIE" and _stderr_is_empty(exp_name))
            )
            and (
                bool(resource_payload.get("is_oom"))
                or str(resource_payload.get("status") or "").upper() == "OOM"
                or str(resource_payload.get("error_type") or "").upper() == "OOM"
            )
        ):
            peak = resource_payload.get("peak_memory_mb", 0)
            message = str(resource_payload.get("error_message") or "CUDA out of memory")
            if db.update_experiment(
                exp_name,
                {
                    "status": STATUS_NEEDS_RERUN,
                    "running_on": None,
                    "error_info": {
                        "type": "OOM",
                        "message": message,
                        "is_true_oom": bool(error_info.get("is_true_oom", False)),
                        "peak_memory_mb": peak if isinstance(peak, (int, float)) else 0,
                        "failed_at": error_info.get("failed_at"),
                    },
                },
            ):
                repaired.append(f"{exp_name}:oom")
                if logger is not None:
                    logger.log(
                        f"Reconciled {exp_name}: SCRIPT_ERROR -> OOM from resource_usage.json"
                    )
                continue

        result_path, result_payload = _read_result_payload(exp_name)
        if (
            error_type in {"ZOMBIE", "SCRIPT_ERROR"}
            and result_path is not None
            and isinstance(result_payload, dict)
            and _artifact_is_fresh(result_path, failed_ts)
        ):
            result = _coerce_completed_result(result_payload, resource_payload)
            if result["f1_score"] is None and result["auc_score"] is None:
                continue
            if db.update_experiment(
                exp_name,
                {
                    "status": STATUS_COMPLETED,
                    "running_on": None,
                    "error_info": None,
                    "completed_at": datetime.now().isoformat(),
                    "result": result,
                },
            ):
                repaired.append(f"{exp_name}:completed")
                if logger is not None:
                    logger.log(
                        f"Reconciled {exp_name}: recovered COMPLETED state from result artifacts"
                    )
    return repaired


# =============================================================================
# Dashboard
# =============================================================================


class UnifiedDashboard:
    def __init__(
        self,
        worker_id: str,
        cluster_mgr: ClusterManager,
        db: ExperimentsDB,
        *,
        is_watch: bool = False,
    ):
        self.worker_id = worker_id
        self.cluster_mgr = cluster_mgr
        self.db = db
        self.is_watch = is_watch
        self.start_time = time.time()
        self.selected_node_idx = 0
        self.focus_mode = "experiments" if is_watch else "cluster"
        self.selected_exp_idx = 0
        self.selected_exp_name: Optional[str] = None
        self._panel_exp_rows: List[Dict[str, Any]] = []
        self._panel_exp_total = 0
        self._prev_val_f1: Dict[str, float] = {}
        self.exp_page = 0
        self.exp_page_size = 20
        self.exp_total_pages = 1
        self.action_mode = False
        self.action_idx = 0
        self.assign_mode = False
        self.assign_workers: List[str] = []
        self.exp_two_step = TwoStepKeyHandler()
        self.actions = ["disable", "enable", "restart"]
        self.cluster_cols = 1
        self.message = ""
        self.message_time = 0
        self._message_lock = threading.Lock()
        self._action_queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue()
        self._action_results: "queue.Queue[str]" = queue.Queue()
        self._pending_actions = 0
        self._pending_lock = threading.Lock()
        self._action_worker_count = 2
        self._action_pool: Optional[ThreadPoolExecutor] = None
        self._action_pool_running = False

    def _ensure_action_pool(self) -> None:
        if self._action_pool_running and self._action_pool is not None:
            return
        self._action_pool = ThreadPoolExecutor(
            max_workers=self._action_worker_count, thread_name_prefix="action"
        )
        self._action_pool_running = True
        for _ in range(self._action_worker_count):
            self._action_pool.submit(self._action_worker_loop)

    def _action_worker_loop(self) -> None:
        while True:
            request = self._action_queue.get()
            if request is None:
                return
            try:
                message = self._run_async_action(request)
            except Exception as e:
                message = f"✗ Action error: {e}"
            with self._pending_lock:
                self._pending_actions = max(0, self._pending_actions - 1)
            self._action_results.put(message)

    def shutdown(self) -> None:
        if not self._action_pool_running:
            return
        self._action_pool_running = False
        try:
            while True:
                self._action_queue.get_nowait()
        except queue.Empty:
            pass
        with self._pending_lock:
            self._pending_actions = 0
        for _ in range(self._action_worker_count):
            self._action_queue.put(None)
        assert self._action_pool is not None
        self._action_pool.shutdown(wait=True)
        self._action_pool = None

    def _enqueue_action(self, request: Dict[str, Any], label: str) -> None:
        self._ensure_action_pool()
        with self._pending_lock:
            self._pending_actions += 1
        self.set_message(f"⏳ Queued {label}")
        self._action_queue.put(dict(request))

    def _pending_action_count(self) -> int:
        with self._pending_lock:
            return self._pending_actions

    def drain_async_updates(self) -> None:
        while True:
            try:
                message = self._action_results.get_nowait()
            except queue.Empty:
                return
            self.set_message(message)

    def _run_async_action(self, request: Dict[str, Any]) -> str:
        action_type = str(request.get("type", ""))
        if action_type == "node_action":
            node_id = str(request.get("node_id", ""))
            action = str(request.get("action", ""))
            if not node_id or not action:
                return "✗ Invalid node action"
            return self._do_action_sync(node_id, action)

        if action_type == "assign_worker":
            name = str(request.get("name", ""))
            new_worker_raw = request.get("new_worker")
            new_worker = (
                str(new_worker_raw).strip() if new_worker_raw is not None else ""
            )
            old_worker = str(request.get("old_worker", ""))
            if not name:
                return "✗ Invalid assign action"
            if old_worker and old_worker != new_worker:
                self.cluster_mgr.stop_node(old_worker)
            ok = self.db.assign_experiment_worker(name, new_worker or None)
            if new_worker:
                return f"{'✓' if ok else '✗'} Assign {name} -> {new_worker}"
            return f"{'✓' if ok else '✗'} Clear machine assignment for {name}"

        if action_type == "reset_failed":
            count = self.db.reset_failed_experiments()
            if count > 0:
                return f"Reset {count} failed experiment(s) to READY"
            return "No failed experiments to reset"

        if action_type == "exp_kill":
            name = str(request.get("name", ""))
            if not name:
                return "✗ Invalid exp_kill"
            targets = self._get_cascade_targets(name)
            success = 0
            queued_count = 0
            for target in targets:
                ok, detail = self._reset_with_pid_guard(target, action="kill")
                if ok and detail.startswith("queued remote kill"):
                    queued_count += 1
                if ok:
                    success += 1
            return (
                f"{'✓' if success == len(targets) else '✗'} "
                f"Kill {name} cascade ({success}/{len(targets)}, queued_remote={queued_count})"
            )

        if action_type == "exp_freeze":
            name = str(request.get("name", ""))
            if not name:
                return "✗ Invalid exp_freeze"
            targets = self._get_cascade_targets(name)
            success = 0
            queued_count = 0
            for target in targets:
                ok, detail = self._reset_with_pid_guard(target, action="freeze")
                if ok and detail.startswith("queued remote kill"):
                    queued_count += 1
                if ok:
                    success += 1
            return (
                f"{'✓' if success == len(targets) else '✗'} "
                f"Freeze {name} cascade ({success}/{len(targets)}, queued_remote={queued_count})"
            )

        if action_type == "exp_rerun":
            name = str(request.get("name", ""))
            if not name:
                return "✗ Invalid exp_rerun"
            targets = self._get_cascade_targets(name)
            success = 0
            queued_count = 0
            cleaned_paths = 0
            for target in targets:
                cleaned_paths += len(_clean_experiment_artifacts(target))
                ok, detail = self._reset_with_pid_guard(target, action="rerun")
                if ok and detail.startswith("queued remote kill"):
                    queued_count += 1
                if ok:
                    success += 1
            return (
                f"{'✓' if success == len(targets) else '✗'} "
                f"Rerun {name} cascade ({success}/{len(targets)}, queued_remote={queued_count}, cleaned={cleaned_paths})"
            )

        if action_type == "exp_delete":
            name = str(request.get("name", ""))
            if not name:
                return "✗ Invalid exp_delete"
            ok = _delete_experiment_from_registry(self.db, name)
            return f"{'✓' if ok else '✗'} Delete {name}"

        if action_type == "exp_archive":
            name = str(request.get("name", ""))
            if not name:
                return "✗ Invalid exp_archive"
            try:
                from archive_card import generate_archive_card, get_metric_summary

                card_path = generate_archive_card(name)
                metric_summary = get_metric_summary(name)
            except Exception:
                card_path = None
                metric_summary = {}
            archive_fn = getattr(self.db, "archive_experiment", None)
            ok = bool(archive_fn(name)) if callable(archive_fn) else False
            if not ok:
                ok = _archive_experiment_via_script(name)
            return f"{'✓' if ok else '✗'} Archive {name}"

        if action_type == "exp_repipeline":
            name = str(request.get("name", ""))
            if not name:
                return "✗ Invalid exp_repipeline"
            payload = request.get("exp_payload")
            exp_payload = payload if isinstance(payload, dict) else {}
            if not exp_payload:
                existing = self.db.get_experiment(name)
                if isinstance(existing, dict):
                    exp_payload = dict(existing)
            if not exp_payload:
                exp_payload = {"name": name}
            removed = _delete_experiment_from_registry(self.db, name)
            if not removed:
                return f"✗ Re-pipeline {name} failed (remove from registry)"
            queued = _enqueue_repipeline_ready(name, exp_payload)
            if not queued:
                return f"✗ Re-pipeline {name} failed (ready queue write)"
            return f"✓ Re-pipeline {name} (removed + queued to ready.json)"

        if action_type == "exp_start_now":
            name = str(request.get("name", ""))
            if not name:
                return "✗ Invalid exp_start_now"
            ok, detail = self._reset_with_pid_guard(name, action="start_now")
            return f"{'✓' if ok else '✗'} Start-now experiment {name} ({detail})"

        if action_type == "exp_move":
            name = str(request.get("name", ""))
            direction = str(request.get("direction", ""))
            if not name or not direction:
                return "✗ Invalid exp_move"
            moved = self.db.move_experiment(name, direction)
            return f"{'✓' if moved else '✗'} Move {direction} {name}"

        return "✗ Unknown action"

    def _try_stop_local_experiment_pid(self, name: str) -> Tuple[bool, Optional[int]]:
        try:
            exp = self.db.get_experiment(name)
        except Exception:
            exp = None
        if not isinstance(exp, dict):
            return False, None
        running_on = exp.get("running_on") or {}
        running_worker = str(running_on.get("worker", "")).strip()
        if running_worker and running_worker != self.worker_id:
            return False, None
        pid = running_on.get("pid")
        if not isinstance(pid, int) or pid <= 1:
            return False, None
        stopped = _kill_local_pid_tree(pid)
        return stopped, pid

    def _reset_with_pid_guard(self, name: str, action: str) -> Tuple[bool, str]:
        exp = self.db.get_experiment(name)
        if not isinstance(exp, dict):
            if action == "kill":
                ok = self.db.kill_experiment(name)
            elif action == "freeze":
                ok = self.db.freeze_experiment(name)
            elif action == "start_now":
                ok = self.db.start_experiment_now(name)
            else:
                ok = self.db.rerun_experiment(name)
            return (ok, "reset done" if ok else "db reset failed")

        running_on = exp.get("running_on") or {}
        status = normalize_status(exp.get("status", STATUS_NEEDS_RERUN))
        worker = str(running_on.get("worker", "")).strip()
        pid = running_on.get("pid")

        if status == STATUS_RUNNING and isinstance(pid, int) and pid > 1:
            if worker and worker != self.worker_id:
                queued = self.db.queue_remote_termination(
                    name=name,
                    target_worker=worker,
                    pid=pid,
                    action=action,
                    requester_worker=self.worker_id,
                )
                if queued:
                    hb = self.db.get_cluster_heartbeats().get(worker, {})
                    hb_state = str(hb.get("status") or "").upper()
                    if hb_state == "ONLINE":
                        suffix = "worker online"
                    elif hb:
                        suffix = "worker offline/stale"
                    else:
                        suffix = "worker heartbeat unknown"
                    return (
                        True,
                        f"queued remote kill on {worker} pid={pid} ({suffix})",
                    )
                ok, msg = self.cluster_mgr.kill_remote_pid(worker, pid)
                if not ok:
                    return False, f"remote kill failed ({worker}:{pid}) {msg}"
            else:
                if not _kill_local_pid_tree(pid):
                    return False, f"local kill failed pid={pid}"

        if action == "kill":
            ok = self.db.kill_experiment(name)
        elif action == "freeze":
            ok = self.db.freeze_experiment(name)
        elif action == "start_now":
            ok = self.db.start_experiment_now(name)
        else:
            ok = self.db.rerun_experiment(name)
        return (ok, "reset done" if ok else "db reset failed")

    def _get_cascade_targets(self, root_name: str) -> List[str]:
        data = self.db.load()
        candidates = []
        for key in ("experiments", "completed"):
            items = data.get(key, [])
            if isinstance(items, list):
                candidates.extend(items)

        children_map: Dict[str, List[str]] = {}
        all_names: Set[str] = set()
        for exp in candidates:
            if not isinstance(exp, dict):
                continue
            name = str(exp.get("name", "")).strip()
            if not name:
                continue
            all_names.add(name)
            parent = str(exp.get("parent_experiment") or "").strip()
            if parent:
                children_map.setdefault(parent, []).append(name)

        ordered: List[str] = []
        seen: Set[str] = set()
        queue_names: List[str] = [root_name]
        while queue_names:
            current = queue_names.pop(0)
            if current in seen:
                continue
            seen.add(current)
            ordered.append(current)
            for child in children_map.get(current, []):
                if child not in seen:
                    queue_names.append(child)

        return [name for name in ordered if name in all_names or name == root_name]

    def _read_last_log_line(self, log_path: Path) -> str:
        try:
            with open(log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                if size <= 0:
                    return ""
                read_size = min(size, 2048)
                f.seek(-read_size, os.SEEK_END)
                chunk = f.read().decode("utf-8", errors="replace")
            lines = [line.strip() for line in chunk.splitlines() if line.strip()]
            return lines[-1] if lines else ""
        except Exception:
            return ""

    def _get_preprocess_status(self) -> Optional[str]:
        # preprocess.py currently does not emit this progress file.
        # Keep this as a forward-compatible dashboard integration stub.
        if not PREPROCESS_PROGRESS_FILE.exists():
            return None
        try:
            with open(PREPROCESS_PROGRESS_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            status = payload.get("status", "unknown")
            percent = payload.get("percent")
            current = payload.get("current")
            total = payload.get("total")
            parts = [f"status={status}"]
            if isinstance(percent, (int, float)):
                parts.append(f"{percent:.0f}%")
            if isinstance(current, int) and isinstance(total, int) and total > 0:
                parts.append(f"{current}/{total}")
            return "Preprocess: " + " ".join(parts)
        except Exception:
            return "Preprocess: status file unreadable"

    @staticmethod
    def _format_mem(val: int) -> str:
        return f"{val / 1000:.0f}K" if val >= 1000 else f"{val}M"

    def _move_cluster_selection(self, delta_row: int, delta_col: int, total: int) -> None:
        if total <= 0:
            self.selected_node_idx = 0
            return
        cols = max(1, int(self.cluster_cols))
        rows = (total + cols - 1) // cols
        idx = max(0, min(self.selected_node_idx, total - 1))
        row = idx // cols
        col = idx % cols
        next_row = max(0, min(rows - 1, row + delta_row))
        next_col = max(0, min(cols - 1, col + delta_col))
        next_idx = next_row * cols + next_col
        if next_idx >= total:
            next_idx = total - 1
        self.selected_node_idx = next_idx

    def build_cluster_panel(self, cluster_status: Dict, workers: List[str]) -> Panel:
        machine_cards = []
        online_count = 0
        card_width = 48
        term_width = shutil.get_terminal_size((160, 40)).columns
        self.cluster_cols = max(1, term_width // (card_width + 2))

        for i, w_id in enumerate(workers):
            info = cluster_status.get(w_id, {})
            status = info.get("status", "UNKNOWN")

            if self.db.is_worker_disabled(w_id):
                status = "DISABLED"

            if status == "ONLINE":
                online_count += 1

            is_selected = i == self.selected_node_idx

            title = f"▶ {w_id}" if is_selected else w_id

            if is_selected and not self.action_mode:
                border_style = "bold cyan"
            elif is_selected and self.action_mode:
                border_style = "bold yellow"
            elif status == "ONLINE":
                border_style = "blue"
            elif status == "OFFLINE":
                border_style = "red"
            else:
                border_style = "yellow"

            card_lines: List[Text] = []

            badge = make_status_badge(status)
            last_seen = info.get("last_seen_sec", 999999)
            gpus = info.get("gpus", [])

            our_gpu_ids = {
                int(gid)
                for gid in info.get("our_gpu_ids", [])
                if isinstance(gid, int) or (isinstance(gid, str) and gid.isdigit())
            }

            pid = info.get("pid", 0)
            stale_runtime = status != "ONLINE"
            pid_label = "Last PID" if stale_runtime else "PID"
            pid_str = f" [dim]{pid_label}:{pid}[/]" if pid and int(pid) > 0 else ""

            if status == "OFFLINE" and last_seen < 99999:
                card_lines.append(Text.from_markup(
                    f"{badge}  [dim]({format_time_ago(last_seen)} ago)[/]{pid_str}"
                ))
            else:
                jobs = int(info.get("running_jobs", 0) or 0)
                other_procs = sum(
                    1 for g in gpus
                    if float(g.get("used", 0) or 0) > 500
                    and g.get("index", 0) not in our_gpu_ids
                )
                mine_label = "Last Mine" if stale_runtime else "Mine"
                other_label = "Last ▲" if stale_runtime else "▲"
                card_lines.append(Text.from_markup(
                    f"{badge}  {mine_label}: {jobs}  {other_label}: {other_procs}{pid_str}"
                ))

            if last_seen < 99999:
                card_lines.append(Text.from_markup(f"[dim]Seen: {format_time_ago(last_seen)} ago[/]"))
            else:
                card_lines.append(Text.from_markup("[dim]Seen: --[/]"))

            cpu = info.get("cpu", {})
            if cpu:
                cpu_pct = cpu.get("load_percent", 0.0)
                cores = cpu.get("cpu_count", 0)
                load1 = cpu.get("load1", 0.0)
                card_lines.append(Text.from_markup(
                    f"CPU {make_bar(cpu_pct, 8)} {cpu_pct:.0f}% {cores}c L:{load1}"
                ))
            else:
                card_lines.append(Text.from_markup("[dim]CPU: --[/]"))

            gpu_probe_error = str(info.get("gpu_probe_error", "") or "")
            if gpus:
                for g in gpus:
                    try:
                        g_idx = int(g.get("index", 0))
                    except (TypeError, ValueError):
                        g_idx = 0
                    used = g.get("used", 0)
                    total = g.get("total", 1)
                    util = g.get("util", 0)

                    star_on = g_idx in our_gpu_ids
                    triangle_on = float(used or 0) > 500
                    if star_on and triangle_on:
                        marker = "[green]★[/][yellow]▲[/]"
                    elif star_on:
                        marker = "[green]★[/] "
                    elif triangle_on:
                        marker = "[yellow]▲[/] "
                    else:
                        marker = "  "

                    mem_pct = used / total * 100 if total > 0 else 0
                    util_color = "red" if util > 80 else ("yellow" if util > 50 else "green")

                    card_lines.append(Text.from_markup(
                        f"{marker}GPU{g_idx} {make_bar(mem_pct, 8)} "
                        f"{self._format_mem(used)}/{self._format_mem(total)} "
                        f"[{util_color}]{util}%[/]"
                    ))
            elif gpu_probe_error:
                card_lines.append(Text.from_markup(
                    f"[yellow]GPU: Probe error[/] [dim]({gpu_probe_error})[/]"
                ))
            else:
                card_lines.append(Text.from_markup("[dim]GPU: --[/]"))

            running_experiments = info.get("running_experiments") or []
            if isinstance(running_experiments, str):
                running_experiments = [running_experiments]
            if isinstance(running_experiments, list) and running_experiments:
                shown = [str(x) for x in running_experiments[:2] if str(x).strip()]
                if shown:
                    run_label = "Last Run" if stale_runtime else "Run"
                    card_lines.append(Text.from_markup(f"[dim]{run_label}:[/] {shown[0]}"))
                    if len(shown) > 1:
                        card_lines.append(Text.from_markup(f"[dim]     {shown[1]}[/]"))
                remain = max(0, len(running_experiments) - len(shown))
                if remain > 0:
                    card_lines.append(Text.from_markup(f"[dim]+{remain} more[/]"))

            machine_cards.append(Panel(
                Group(*card_lines), title=title,
                border_style=border_style, width=card_width,
            ))

        all_elements: List[Any] = [
            Columns(
                machine_cards,
                equal=True,
                expand=True,
                width=card_width,
            )
        ]

        if self.action_mode and workers:
            menu_items = []
            for i, act in enumerate(self.actions):
                sty = "reverse bold cyan" if i == self.action_idx else "dim"
                menu_items.append(f"[{sty}] {act.upper()} [/]")
            all_elements.append(Text.from_markup("Action: " + "  ".join(menu_items)))

        preprocess_status = self._get_preprocess_status()
        if preprocess_status:
            all_elements.append(Text.from_markup(f"[dim]{preprocess_status}[/]"))

        return Panel(
            Group(*all_elements),
            title=f"[bold]Cluster[/] ({online_count} online / {len(workers)} total)",
            border_style="blue",
        )

    def _resolve_exp_selection(self, experiments: list) -> None:
        """Resolve selected_exp_name → selected_exp_idx.

        If the previously selected experiment name still exists in the list,
        snap the index to its current position. Otherwise fall back to
        clamping the old index (and update the name to match).
        """
        if not experiments:
            self.selected_exp_idx = 0
            self.selected_exp_name = None
            return

        if self.selected_exp_name is not None:
            for i, exp in enumerate(experiments):
                if exp.get("name") == self.selected_exp_name:
                    self.selected_exp_idx = i
                    return

        # Name not found (or was None) — clamp index, adopt new name
        self.selected_exp_idx = max(0, min(self.selected_exp_idx, len(experiments) - 1))
        self.selected_exp_name = experiments[self.selected_exp_idx].get("name")

    def _apply_experiment_pagination(
        self, display_experiments: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        total = len(display_experiments)
        self._panel_exp_total = total
        self._refresh_experiment_pagination(total)
        if total <= 0:
            return []
        start = self.exp_page * self.exp_page_size
        end = start + self.exp_page_size
        return list(display_experiments[start:end])

    def _refresh_experiment_pagination(self, total: int) -> None:
        if total <= 0:
            self.exp_total_pages = 1
            self.exp_page = 0
            return
        self.exp_total_pages = max(
            1, (total + self.exp_page_size - 1) // self.exp_page_size
        )
        self.exp_page = self.exp_page % self.exp_total_pages

    def _change_experiment_page(self, delta: int) -> None:
        self.exp_page = (self.exp_page + delta) % max(1, self.exp_total_pages)
        self.selected_exp_idx = 0
        self.selected_exp_name = None

    @staticmethod
    def _is_non_actionable_row(exp: Dict[str, Any]) -> bool:
        return bool(exp.get("_non_actionable"))

    def build_experiments_panel(self, cluster_status: Optional[Dict] = None) -> Panel:
        data = self.db.load()
        all_db_experiments = []
        loader = getattr(self.db, "load_all_for_panel", None)
        if callable(loader):
            all_db_experiments = loader()
        experiments = data.get("experiments", [])
        if all_db_experiments:
            existing_names = {e.get("name") for e in experiments}
            for db_exp in all_db_experiments:
                if db_exp.get("name") not in existing_names:
                    experiments.append(db_exp)
        completed_items = data.get("completed", [])
        archived_count = len(data.get("archived", []))
        if cluster_status is None:
            cluster_status = self.cluster_mgr.get_cluster_status(self.db)

        table = Table(
            box=box.SIMPLE, expand=True, show_header=True, header_style="bold"
        )
        table.add_column("", width=2)
        table.add_column("Experiment", min_width=22, ratio=3)
        table.add_column("Parent", width=14)
        table.add_column("Lifecycle", width=11)
        table.add_column("Preferred", width=9)
        table.add_column("Actual", width=9)
        table.add_column("PID", width=6)
        table.add_column("Wait", min_width=12, ratio=2)
        table.add_column("Terminal", width=15)
        table.add_column("Progress", min_width=18, ratio=2)
        table.add_column("Phase", width=12)
        table.add_column("Elapsed", width=8)
        table.add_column("Stale?", width=5)
        table.add_column("Δf1", width=8)
        table.add_column("testF1", width=7, justify="right")
        table.add_column("Peak", width=6, justify="right")
        table.add_column("MemFam", width=10)
        table.add_column("EstMB", width=6, justify="right")
        table.add_column("VGate", width=14)
        table.add_column("Mode", width=10)
        table.add_column("NBLdr", width=6)

        pid_map = get_pid_gpu_map()
        detected = detect_running_experiments_from_gpu_pids(pid_map)

        worker_gpu_free, global_best_free_mb = _build_worker_gpu_free_maps(
            cluster_status
        )

        status_order = {
            STATUS_RUNNING: 0,
            STATUS_NEEDS_RERUN: 1,
            STATUS_COMPLETED: 2,
        }

        def _sort_key(exp: Dict[str, Any]):
            name = exp.get("name", "")
            status = normalize_status(exp.get("status", STATUS_NEEDS_RERUN))
            base = status_order.get(status, 99)
            non_actionable_rank = 1 if self._is_non_actionable_row(exp) else 0
            role = str(exp.get("role") or "")
            synthetic_role_rank = 0
            if non_actionable_rank:
                synthetic_role_rank = 0 if role == "condition_node" else 1
            if status != STATUS_RUNNING and name in detected:
                # Show "detected" processes near the top to highlight mismatch.
                base = min(base, 0.5)
            parent = str(
                exp.get("parent_experiment") or exp.get("condition_parent") or ""
            )
            anchor = parent if parent else str(name)
            is_child = 1 if parent else 0
            order = int(exp.get("display_order", 0) or 0)
            return (
                non_actionable_rank,
                synthetic_role_rank,
                anchor,
                base,
                is_child,
                order,
                str(name),
            )

        registry_names = {
            exp.get("name")
            for exp in (experiments + completed_items)
            if isinstance(exp, dict) and exp.get("name")
        }
        running_by_worker: Dict[str, Set[str]] = {}
        running_workers_by_exp: Dict[str, Set[str]] = {}
        for exp in experiments + completed_items:
            if not isinstance(exp, dict):
                continue
            if (
                normalize_status(exp.get("status", STATUS_NEEDS_RERUN))
                != STATUS_RUNNING
            ):
                continue
            running_on = exp.get("running_on") or {}
            worker = running_on.get("worker")
            exp_name = str(exp.get("name", ""))
            if worker:
                worker = str(worker)
                running_by_worker.setdefault(worker, set()).add(exp_name)
                if exp_name:
                    running_workers_by_exp.setdefault(exp_name, set()).add(worker)

        inferred_running: List[Dict[str, Any]] = []
        inferred_conflicts: Set[Tuple[str, str]] = set()
        for worker_id, info in (cluster_status or {}).items():
            worker = str(worker_id)
            if str(info.get("status", "OFFLINE")).upper() != "ONLINE":
                continue
            hb_running = info.get("running_experiments") or []
            if isinstance(hb_running, str):
                hb_running = [hb_running]
            if not isinstance(hb_running, list):
                hb_running = []
            hb_running_normalized: List[str] = []
            hb_seen: Set[str] = set()
            for item in hb_running:
                name = str(item).strip()
                if not name or name in hb_seen:
                    continue
                hb_seen.add(name)
                hb_running_normalized.append(name)

            for exp_name in hb_running_normalized:
                existing_workers = running_workers_by_exp.get(exp_name, set())
                if exp_name in registry_names:
                    if existing_workers and worker not in existing_workers:
                        conflict_key = (exp_name, worker)
                        if conflict_key not in inferred_conflicts:
                            inferred_running.append(
                                {
                                    "name": exp_name,
                                    "batch_id": "-",
                                    "status": STATUS_RUNNING,
                                    "running_on": {"worker": worker, "gpu": "?"},
                                    "_inferred_reason": "heartbeat_worker_conflict",
                                }
                            )
                            inferred_conflicts.add(conflict_key)
                    continue
                inferred_running.append(
                    {
                        "name": exp_name,
                        "batch_id": "-",
                        "status": STATUS_RUNNING,
                        "running_on": {"worker": worker, "gpu": "?"},
                        "_inferred_reason": "heartbeat_missing_registry",
                    }
                )
                registry_names.add(exp_name)
                running_workers_by_exp.setdefault(exp_name, set()).add(worker)

            hb_jobs = info.get("running_jobs", 0)
            try:
                hb_jobs = int(hb_jobs)
            except (TypeError, ValueError):
                hb_jobs = 0

            if hb_running_normalized:
                unknown_count = max(0, hb_jobs - len(hb_running_normalized))
            else:
                unknown_count = max(
                    0, hb_jobs - len(running_by_worker.get(worker, set()))
                )

            for idx in range(unknown_count):
                inferred_running.append(
                    {
                        "name": f"[unknown@{worker} #{idx + 1}]",
                        "batch_id": "-",
                        "status": STATUS_RUNNING,
                        "running_on": {"worker": worker, "gpu": "?"},
                        "_inferred_reason": "heartbeat_count_only",
                    }
                )

        display_experiments = experiments + completed_items + inferred_running
        status_lookup_by_name: Dict[str, str] = {
            str(item.get("name") or "").strip(): str(item.get("status") or "")
            for item in display_experiments
            if isinstance(item, dict) and str(item.get("name") or "").strip()
        }
        condition_nodes = _build_condition_node_rows(status_lookup_by_name)
        synthetic_status_lookup = dict(status_lookup_by_name)
        for row in condition_nodes:
            synthetic_status_lookup[str(row.get("name") or "")] = normalize_status(
                row.get("status", STATUS_NEEDS_RERUN)
            )
        staged_matrix_rows = _build_staged_matrix_rows(synthetic_status_lookup)
        display_experiments = display_experiments + condition_nodes + staged_matrix_rows
        display_experiments.sort(key=_sort_key)
        self._panel_exp_rows = self._apply_experiment_pagination(display_experiments)
        self._resolve_exp_selection(self._panel_exp_rows)
        selected_exp_name = self.selected_exp_name

        selected_marked = False
        for exp in self._panel_exp_rows:
            name = exp.get("name", "<unknown>")
            parent_name = str(exp.get("parent_experiment") or "")
            role = str(exp.get("role") or "")
            status = normalize_status(exp.get("status", STATUS_NEEDS_RERUN))
            batch_id = exp.get("batch_id", "-")
            running_on = exp.get("running_on")
            result = exp.get("result") or {}
            inferred_reason = exp.get("_inferred_reason")
            condition_parent = str(exp.get("condition_parent") or "").strip() or None
            depends_on = _normalize_name_list(exp.get("depends_on"))
            if condition_parent and condition_parent not in depends_on:
                depends_on = [condition_parent] + depends_on
            progression_status = (
                str(exp.get("progression_status") or "").strip().upper()
            )
            progression_block_reason = str(
                exp.get("block_reason") or exp.get("progression_block_reason") or ""
            ).strip()
            if not progression_status:
                parent_status = (
                    status_lookup_by_name.get(condition_parent)
                    if condition_parent
                    else None
                )
                progression_status, derived_reason = derive_progression_status(
                    status,
                    condition_parent=condition_parent,
                    condition_parent_status=parent_status,
                    warmup_hint=False,
                )
                if not progression_block_reason:
                    progression_block_reason = derived_reason or ""

            panel_truth = None
            if not inferred_reason and hasattr(self.db, "get_panel_truth"):
                try:
                    panel_truth = self.db.get_panel_truth(str(name))
                except Exception:
                    panel_truth = None
            truth_result = result
            truth_error_info = exp.get("error_info") or {}
            truth_terminal_metadata: Dict[str, Any] = {}
            truth_canonical_result: Dict[str, Any] = {}
            if isinstance(panel_truth, dict):
                truth_result = panel_truth.get("result") or truth_result
                truth_error_info = panel_truth.get("error_info") or truth_error_info
                truth_terminal_metadata = panel_truth.get("terminal_metadata") or {}
                truth_canonical_result = panel_truth.get("canonical_result") or {}

            preferred_worker = exp.get("preferred_worker")
            preferred_worker_str = str(preferred_worker) if preferred_worker else "-"
            actual_worker_str = "-"
            live_pid_str = "-"
            if running_on:
                actual_worker_str = (
                    f"{running_on.get('worker', '?')}:{running_on.get('gpu', '?')}"
                )
                pid_val = running_on.get("pid")
                if isinstance(pid_val, int) and pid_val > 0:
                    live_pid_str = str(pid_val)

            f1 = truth_result.get("f1_score", "-")
            if isinstance(f1, float):
                f1 = f"{f1:.4f}"

            if role == "diagnostic_compatibility_probe":
                f1 = "-"

            completed_epochs = None
            completed_test_f1 = None
            if status == STATUS_COMPLETED:
                completed_epochs, completed_test_f1 = get_completed_result_summary(
                    name, truth_result, truth_canonical_result
                )
                if (
                    completed_test_f1 is not None
                    and role != "diagnostic_compatibility_probe"
                ):
                    f1 = f"{completed_test_f1:.4f}"

            error_info = truth_error_info
            running_on_dict = running_on or {}
            peak_mb = (
                running_on_dict.get("peak_memory_mb")
                or truth_result.get("peak_memory_mb")
                or error_info.get("peak_memory_mb", 0)
            )
            peak_str = f"{peak_mb / 1024:.1f}GB" if peak_mb > 0 else "-"
            memory_fields = format_memory_contract_fields(exp)

            est_mem_decision = (
                int(memory_fields.get("est_mb") or 0)
                if str(memory_fields.get("est_mb", "")).strip().isdigit()
                else 0
            )
            vgate_worker = None
            if running_on:
                vgate_worker = str(running_on.get("worker") or "").strip() or None
            elif preferred_worker:
                vgate_worker = preferred_worker_str
            vgate_free_mb = (
                _best_free_mb_for_worker(worker_gpu_free, vgate_worker)
                if vgate_worker
                else global_best_free_mb
            )

            if (
                status == STATUS_NEEDS_RERUN
                and vgate_free_mb > 0
                and est_mem_decision > 0
            ):
                free_g = vgate_free_mb / 1024
                est_g = est_mem_decision / 1024
                if vgate_free_mb >= est_mem_decision:
                    vgate_str = f"[green]{free_g:.1f}/{est_g:.1f}G OK[/]"
                else:
                    vgate_str = f"[red]{free_g:.1f}/{est_g:.1f}G !![/]"
            elif status == STATUS_RUNNING and running_on:
                gpu_idx = running_on.get("gpu")
                gpu_free = _free_mb_for_worker_gpu(
                    worker_gpu_free,
                    str(running_on.get("worker") or "").strip() or None,
                    gpu_idx,
                )
                vgate_str = f"{gpu_free / 1024:.1f}G" if gpu_free > 0 else "-"
            else:
                vgate_str = "-"

            icons = {
                STATUS_RUNNING: "▶",
                STATUS_NEEDS_RERUN: "○",
                STATUS_COMPLETED: "✔",
            }
            colors = {
                STATUS_RUNNING: "yellow",
                STATUS_NEEDS_RERUN: "cyan",
                STATUS_COMPLETED: "green",
            }

            icon = icons.get(status, "?")
            color = colors.get(status, "white")

            if status != STATUS_RUNNING and name in detected:
                icon = "⚠"
                color = "bright_yellow"

            progress_str = ""
            phase_str = "-"
            elapsed_str = "-"
            stale_str = "-"
            delta_text = "-"
            lifecycle_stage = "queued"
            wait_reason = "-"
            terminal_reason = get_terminal_reason(
                name, status, truth_result, truth_error_info, truth_terminal_metadata
            )
            truth_mismatch = _artifact_truth_mismatch(
                str(name),
                status,
                truth_result,
                truth_error_info,
                truth_terminal_metadata,
            )
            if truth_mismatch:
                terminal_reason = f"{terminal_reason}*"
            if inferred_reason == "heartbeat_missing_registry":
                progress_str = (
                    "[bright_yellow]Heartbeat RUNNING; missing in registry[/]"
                )
                lifecycle_stage = "running"
                wait_reason = "heartbeat_missing_registry"
                icon = "⚠"
                color = "bright_yellow"
            elif inferred_reason == "heartbeat_count_only":
                progress_str = (
                    "[bright_yellow]Heartbeat reports running job; name unavailable[/]"
                )
                lifecycle_stage = "running"
                wait_reason = "heartbeat_count_only"
                icon = "⚠"
                color = "bright_yellow"
            elif inferred_reason == "heartbeat_worker_conflict":
                progress_str = (
                    "[bright_yellow]Heartbeat worker conflicts with registry[/]"
                )
                lifecycle_stage = "running"
                wait_reason = "heartbeat_worker_conflict"
                icon = "⚠"
                color = "bright_yellow"
            elif status != STATUS_RUNNING and name in detected:
                pids = ",".join(str(x.get("pid")) for x in detected.get(name, [])[:3])
                progress_str = (
                    f"[bright_yellow]GPU active (pid={pids}) registry={status}[/]"
                )
                if len(detected.get(name, [])) > 3:
                    progress_str += f" (+{len(detected.get(name, [])) - 3})"
                lifecycle_stage = "stale"
                wait_reason = f"registry_{status.lower()}_but_gpu_active"
                # If registry has no peak, use current GPU memory usage as a hint.
                if peak_mb == 0:
                    try:
                        peak_mb = max(
                            x.get("used_mb", 0) for x in detected.get(name, [])
                        )
                        peak_str = f"{peak_mb / 1024:.1f}GB" if peak_mb > 0 else "-"
                    except Exception:
                        pass
            elif truth_mismatch:
                lifecycle_stage = "stale"
                wait_reason = truth_mismatch
                progress_str = (
                    f"[bright_yellow]DB truth with artifact drift ({truth_mismatch})[/]"
                )
            elif status == STATUS_RUNNING:
                is_preflight = "preflight" in str(batch_id).lower()
                lifecycle_stage = "preflight" if is_preflight else "full-run"
                progress = get_experiment_progress(name)
                started_ts = _parse_iso_ts((running_on or {}).get("started_at"))
                hb_worker = str((running_on or {}).get("worker") or "").strip()
                hb_alive = False
                if hb_worker:
                    hb = (cluster_status or {}).get(hb_worker, {})
                    hb_status = str(hb.get("status") or "").upper()
                    hb_running = hb.get("running_experiments") or []
                    if isinstance(hb_running, str):
                        hb_running = [hb_running]
                    try:
                        hb_jobs = int(hb.get("running_jobs") or 0)
                    except (TypeError, ValueError):
                        hb_jobs = 0
                    hb_alive = hb_status == "ONLINE" and (
                        name in hb_running or hb_jobs > 0
                    )
                if progress:
                    pct = progress.get("percent", 0)
                    epoch = progress.get("epoch", 0)
                    total = progress.get("total_epochs", 1)
                    val_f1 = progress.get("val_f1", 0)
                    current_f1 = progress.get("val_f1", 0.0)
                    prev_f1 = self._prev_val_f1.get(name, current_f1)
                    delta = current_f1 - prev_f1
                    self._prev_val_f1[name] = current_f1
                    delta_text = (
                        f"+{delta:.3f}"
                        if delta > 0
                        else f"{delta:.3f}"
                        if delta < 0
                        else "="
                    )
                    progress_phase = progress.get("phase", "")
                    if progress_phase:
                        phase_str = str(progress_phase)
                    progress_ts = _parse_iso_ts(progress.get("timestamp"))
                    ts_iso = str(progress.get("timestamp") or "").strip()
                    if ts_iso:
                        try:
                            elapsed_seconds = (
                                datetime.now() - datetime.fromisoformat(ts_iso)
                            ).total_seconds()
                            if elapsed_seconds >= 3600:
                                elapsed_str = f"{int(elapsed_seconds // 3600)}h ago"
                            else:
                                elapsed_str = f"{int(elapsed_seconds // 60)}m ago"
                            if elapsed_seconds > 900:
                                stale_str = "🔴"
                            elif elapsed_seconds > 300:
                                stale_str = "⚠"
                            else:
                                stale_str = "-"
                        except Exception:
                            elapsed_str = "-"
                            stale_str = "-"
                    warmup_anchor = (
                        progress_ts if progress_ts is not None else started_ts
                    )
                    warmup_str = _render_wait_progress(
                        elapsed_sec=(time.time() - warmup_anchor)
                        if warmup_anchor is not None
                        else None,
                        total_sec=GPU_PROCESS_WARMUP_SEC,
                    )
                    in_warmup = int(epoch or 0) < WARMUP_COMPLETION_EPOCH
                    if progress_phase == "loader_init":
                        elapsed = (time.time() - warmup_anchor) if warmup_anchor else 0
                        mins, secs = divmod(int(elapsed), 60)
                        progress_str = (
                            f"⠿ neighbor_loader {mins}m{secs:02d}s E0/{total}"
                        )
                        wait_reason = "loader_init"
                        lifecycle_stage = "warm"
                        progression_status = "WARM"
                    elif in_warmup and warmup_str:
                        progress_str = (
                            f"{warmup_str} E{epoch}/{total} valF1={val_f1:.3f}"
                        )
                        wait_reason = "warmup_epoch0"
                        lifecycle_stage = "warm"
                        progression_status = "WARM"
                    else:
                        bar = make_bar(pct, 8)
                        progress_str = f"{bar} E{epoch}/{total} valF1={val_f1:.3f}"
                        wait_reason = "-"
                        progression_status = "RUNNING"

                    if (
                        not is_preflight
                        and int(epoch or 0) >= WARMUP_COMPLETION_EPOCH
                        and wait_reason != "warmup_epoch0"
                    ):
                        lifecycle_stage = "warmed"
                        progression_status = "RUNNING"
                elif name in detected:
                    # RUNNING but no .progress file yet; show GPU PID info
                    pids = ",".join(
                        str(x.get("pid")) for x in detected.get(name, [])[:3]
                    )
                    warmup_str = _render_wait_progress(
                        elapsed_sec=(time.time() - started_ts)
                        if started_ts is not None
                        else None,
                        total_sec=GPU_PROCESS_WARMUP_SEC,
                    )
                    progress_str = (
                        f"{warmup_str} pid={pids}"
                        if warmup_str
                        else f"Running (pid={pids}, awaiting progress)"
                    )
                    wait_reason = "awaiting_progress"
                    progression_status = "WARM" if warmup_str else "RUNNING"
                elif hb_alive:
                    warmup_str = _render_wait_progress(
                        elapsed_sec=(time.time() - started_ts)
                        if started_ts is not None
                        else None,
                        total_sec=GPU_PROCESS_WARMUP_SEC,
                    )
                    progress_str = (
                        f"{warmup_str} hb={hb_worker}"
                        if warmup_str
                        else f"Running ({hb_worker} heartbeat alive, awaiting progress)"
                    )
                    wait_reason = "awaiting_progress_remote_hb"
                    progression_status = "WARM" if warmup_str else "RUNNING"
                else:
                    if hb_worker and not cluster_status:
                        progress_str = "⏳ Heartbeat source unavailable"
                        wait_reason = "heartbeat_source_unavailable"
                    elif hb_worker and hb_worker not in (cluster_status or {}):
                        progress_str = f"⏳ Awaiting heartbeat ({hb_worker})"
                        wait_reason = "awaiting_worker_heartbeat"
                    else:
                        progress_str = "⏳ No heartbeat"
                        wait_reason = "no_heartbeat"
                    progression_status = "RUNNING"

                if peak_mb == 0 and name in detected:
                    try:
                        peak_mb = max(
                            x.get("used_mb", 0) for x in detected.get(name, [])
                        )
                        peak_str = f"{peak_mb / 1024:.1f}GB" if peak_mb > 0 else "-"
                    except Exception:
                        pass
            elif status == STATUS_NEEDS_RERUN:
                lifecycle_stage = "ready" if progression_status == "READY" else "queued"
                error_info = exp.get("error_info") or {}
                retry = exp.get("retry_count", 0)
                retry_limit = exp.get("max_retries", MAX_RETRY_COUNT)
                try:
                    retry_limit = int(retry_limit)
                except (TypeError, ValueError):
                    retry_limit = MAX_RETRY_COUNT
                if error_info:
                    err_type = error_info.get("type", "ERROR")
                    oom_retry = int(exp.get("oom_retry_count", 0) or 0)
                    is_true_oom = bool(error_info.get("is_true_oom", False))
                    if is_true_oom:
                        progress_str = (
                            f"[red]TrueOOM[/] (manual R required, c={oom_retry})"
                        )
                        lifecycle_stage = "blocked"
                        wait_reason = "true_oom"
                    elif err_type == "MANUAL_FREEZE":
                        progress_str = "[magenta]Frozen[/] (manual S/R required)"
                        lifecycle_stage = "frozen"
                        wait_reason = "manual_freeze"
                        icon = "❄"
                        color = "magenta"
                    elif err_type == "OOM":
                        peak_err_mb = int(error_info.get("peak_memory_mb", 0) or 0)
                        if peak_err_mb > 0:
                            progress_str = (
                                f"[cyan]Needs rerun[/] ({err_type}, a/b={retry}/{retry_limit}, c={oom_retry}, "
                                f"wait free>{peak_err_mb + 2000}MB)"
                            )
                            wait_reason = f"wait_free_gt_{peak_err_mb + 2000}mb"
                        else:
                            progress_str = f"[cyan]Needs rerun[/] ({err_type}, a/b={retry}/{retry_limit}, c={oom_retry})"
                            wait_reason = "soft_oom_retry"
                    else:
                        progress_str = f"[cyan]Needs rerun[/] ({err_type}, a/b={retry}/{retry_limit}, c={oom_retry})"
                        wait_reason = str(err_type).lower()
                else:
                    if progression_status == "BLOCKED_CONDITION":
                        parent_label = (
                            ",".join(depends_on)
                            if depends_on
                            else (condition_parent or "condition_parent")
                        )
                        progress_str = (
                            f"[yellow]Blocked by condition[/] ({parent_label})"
                        )
                        lifecycle_stage = "blocked"
                        wait_reason = (
                            progression_block_reason or "condition_parent_unmet"
                        )
                    elif role == "condition_node":
                        progress_str = "Condition node (display-only)"
                        wait_reason = "condition_ready_display_only"
                    elif (
                        vgate_free_mb > 0
                        and est_mem_decision > 0
                        and vgate_free_mb < est_mem_decision
                    ):
                        progress_str = f"[yellow]VRAM blocked[/] (free={vgate_free_mb}MB < est={est_mem_decision}MB)"
                        wait_reason = f"vram_blocked_{vgate_free_mb}mb"
                    else:
                        progress_str = "Waiting..."
                        wait_reason = "ready_for_claim"
            elif status == STATUS_COMPLETED:
                lifecycle_stage = (
                    "preflight_done"
                    if "preflight" in str(batch_id).lower()
                    else "completed"
                )
                if role == "condition_node":
                    gate_type = str(exp.get("gate_type") or "condition")
                    evidence_ref = str(exp.get("gate_evidence_ref") or "").strip()
                    progress_str = f"Condition met ({gate_type})"
                    wait_reason = (
                        evidence_ref if evidence_ref else "condition_satisfied"
                    )
                elif role == "diagnostic_compatibility_probe":
                    progress_str = "Probe terminal"
                else:
                    progress_str = "Complete"
                    if completed_epochs is not None and completed_epochs > 0:
                        progress_str = f"{progress_str} E{completed_epochs}"
                    if completed_test_f1 is not None:
                        progress_str = f"{progress_str} testF1={completed_test_f1:.3f}"

            inline_error = str(
                (exp.get("error_info") or {}).get("message") or ""
            ).strip()
            if inline_error:
                inline_error = " ".join(inline_error.split())
                if len(inline_error) > 72:
                    inline_error = inline_error[:69] + "..."
                progress_str = (
                    f"{progress_str} | {inline_error}" if progress_str else inline_error
                )

            is_selected = (
                self.focus_mode == "experiments"
                and not selected_marked
                and selected_exp_name
                and name == selected_exp_name
            )
            selected_prefix = "▶ " if is_selected else ""
            row_style = "reverse" if is_selected else None
            if is_selected:
                selected_marked = True

            if role == "main":
                name_display = f"▣ {name}"
            elif role == "condition_node":
                name_display = f"◇ {name}"
            elif parent_name:
                name_display = f"  └─ {name}"
            else:
                name_display = str(name)

            parent_display = parent_name
            if role == "condition_node" and condition_parent:
                parent_display = condition_parent

            table.add_row(
                f"[{color}]{icon}[/]",
                f"{selected_prefix}[{color}]{name_display}[/]",
                str(parent_display or ""),
                str(lifecycle_stage or ""),
                str(preferred_worker_str or ""),
                str(actual_worker_str or ""),
                str(live_pid_str or ""),
                str(wait_reason or ""),
                str(format_terminal_reason_text(terminal_reason) or ""),
                str(progress_str or ""),
                str(phase_str or ""),
                str(elapsed_str or ""),
                str(stale_str or ""),
                str(delta_text or ""),
                str(f1 or ""),
                str(peak_str or ""),
                str(memory_fields["mem_family"] or ""),
                str(memory_fields["est_mb"] or ""),
                str(vgate_str or ""),
                str(memory_fields["mem_mode"] or ""),
                str(memory_fields["nbldr"] or ""),
                style=row_style,
            )

        if len(display_experiments) > self.exp_page_size:
            page_start = self.exp_page * self.exp_page_size + 1
            page_end = min(
                (self.exp_page + 1) * self.exp_page_size, len(display_experiments)
            )
            table.add_row(
                "",
                f"[dim]Page {self.exp_page + 1}/{self.exp_total_pages} · showing {page_start}-{page_end} of {len(display_experiments)}[/]",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            )

        title_suffix = f"{len(experiments)} active, {len(completed_items)} completed"
        if inferred_running:
            title_suffix += f" + {len(inferred_running)} inferred"
        if condition_nodes:
            title_suffix += f" + {len(condition_nodes)} conditions"

        return Panel(
            table,
            title=f"[bold]Experiments[/] ({title_suffix}, {archived_count} archived)",
            border_style="cyan",
        )

    def build_layout(self, running_count: int = 0) -> Layout:
        cluster_status = self.cluster_mgr.get_cluster_status(self.db)
        workers = sorted(cluster_status.keys())

        if workers and self.selected_node_idx >= len(workers):
            self.selected_node_idx = len(workers) - 1

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=4),
        )

        uptime = int(time.time() - self.start_time)
        local_gpus = get_all_gpu_status()
        gpu_summary = (
            " | ".join([f"GPU{g['index']}:{g['util']}%" for g in local_gpus])
            if local_gpus
            else "No GPU"
        )

        pending_actions = self._pending_action_count()
        pending_str = (
            f" │ ActionQ: [yellow]{pending_actions}[/]" if pending_actions > 0 else ""
        )
        header_text = f"[bold]Phase 3 Runner v3.1[/] │ Worker: [cyan]{self.worker_id}[/] │ Running: [green]{running_count}[/]{pending_str} │ {gpu_summary} │ Uptime: {uptime}s │ {datetime.now().strftime('%H:%M:%S')}"
        layout["header"].update(Panel(header_text, style="on blue"))

        if self.focus_mode == "experiments":
            layout["main"].update(self.build_experiments_panel(cluster_status))
        else:
            layout["main"].update(self.build_cluster_panel(cluster_status, workers))

        msg = self.message if time.time() - self.message_time < 5 else ""
        assign_hint_line = ""
        two_step_line = ""
        if self.focus_mode == "experiments":
            controls_line = "[dim]w/s[/]:Sel  [dim]U/J[/]:Prio  [dim]T[/]:Start  [dim]A[/]:Assign  [dim]p[/]:RePipe  [dim]N/P[/]:Page  [dim]Tab[/]:Cluster  [dim]Q[/]:Quit"
            if self.exp_two_step.state == "idle":
                two_step_line = "[Selected default] [k]Kill [r]Rerun [d/x]Delete [v]Archive [f]Freeze  |  [a]All scope override  |  [Esc]Cancel"
            else:
                two_step_line = self.exp_two_step.prompt.replace("→", "->")
            if self.assign_mode:
                if self.assign_workers:
                    options = "  ".join(
                        f"[bold cyan][{i + 1}][/bold cyan]{worker}"
                        for i, worker in enumerate(self.assign_workers)
                    )
                else:
                    options = "[dim](no workers available)[/]"
                assign_hint_line = f"[bold yellow]Assign[/] {options}  [bold yellow][C][/bold yellow]Clear  [dim][Esc]Cancel[/]"
        else:
            controls_line = "[dim]W/A/S/D[/]:Move  [dim]Enter[/]:Action  [dim]D[/]:Disable  [dim]E[/]:Enable  [dim]R[/]:Restart  [dim]F[/]:Retry Failed  [dim]Tab[/]:→Experiments  [dim]Q[/]:Quit"
        last_log_line = self._read_last_log_line(RUNNER_LOG_FILE)
        status_parts: List[str] = []
        if pending_actions > 0:
            status_parts.append(f"[yellow]Pending actions: {pending_actions}[/]")
        if msg:
            status_parts.append(f"[yellow]{msg}[/]")
        if last_log_line:
            status_parts.append(f"[dim]Last:[/] {escape(last_log_line)}")
        if self.focus_mode == "experiments" and self.exp_total_pages > 1:
            status_parts.append(
                f"[dim]Page:[/] {self.exp_page + 1}/{self.exp_total_pages}"
            )
        status_line = " │ ".join(status_parts) if status_parts else "[dim]Last:[/] -"

        footer_group_items: List[Any] = []
        if assign_hint_line:
            footer_group_items.append(Text.from_markup(assign_hint_line))
        footer_group_items.append(Text.from_markup(controls_line))
        if self.focus_mode == "experiments":
            footer_group_items.append(Text(two_step_line))
        footer_group_items.append(Text.from_markup(status_line))

        layout["footer"].update(
            Panel(
                Group(*footer_group_items),
                title="Controls",
            )
        )

        return layout

    def set_message(self, msg: str):
        with self._message_lock:
            self.message = msg
            self.message_time = time.time()

    def handle_key(self, key: Optional[str], workers: List[str]) -> bool:
        if not key:
            return True

        if key.lower() == "q":
            return False

        if key == "\t":
            self.focus_mode = (
                "experiments" if self.focus_mode == "cluster" else "cluster"
            )
            self.action_mode = False
            self.exp_two_step = TwoStepKeyHandler()
            return True

        if self.focus_mode == "experiments":
            data = self.db.load()
            panel_experiments = self._panel_exp_rows
            if not panel_experiments:
                panel_experiments = data.get("experiments", [])
            self._resolve_exp_selection(panel_experiments)

            if self.assign_mode:
                if key == "\x1b":
                    self.assign_mode = False
                    self.set_message("Assign cancelled")
                    self.exp_two_step = TwoStepKeyHandler()
                elif key.upper() == "C":
                    selected_exp = panel_experiments[self.selected_exp_idx]
                    if self._is_non_actionable_row(selected_exp):
                        self.assign_mode = False
                        self.set_message("Condition node is display-only")
                        return True
                    name = str(selected_exp.get("name", ""))
                    if name:
                        self._enqueue_action(
                            {
                                "type": "assign_worker",
                                "name": name,
                                "old_worker": "",
                                "new_worker": None,
                            },
                            f"Clear machine assignment for {name}",
                        )
                    self.assign_mode = False
                elif key.isdigit():
                    choice = int(key)
                    if 1 <= choice <= len(self.assign_workers):
                        selected_worker = self.assign_workers[choice - 1]
                        selected_exp = panel_experiments[self.selected_exp_idx]
                        if self._is_non_actionable_row(selected_exp):
                            self.assign_mode = False
                            self.set_message("Condition node is display-only")
                            return True
                        name = str(selected_exp.get("name", ""))
                        running_on = selected_exp.get("running_on") or {}
                        old_worker = (
                            str(running_on.get("worker", "")) if running_on else ""
                        )
                        if name:
                            self._enqueue_action(
                                {
                                    "type": "assign_worker",
                                    "name": name,
                                    "old_worker": old_worker,
                                    "new_worker": selected_worker,
                                },
                                f"Assign {name} -> {selected_worker}",
                            )
                        self.assign_mode = False
                return True
            elif key == "A" and panel_experiments:
                self.exp_two_step = TwoStepKeyHandler()
                self.assign_mode = True
                machine_keys = sorted(self.cluster_mgr.load_machines().keys())
                candidate_workers = workers or machine_keys
                self.assign_workers = list(dict.fromkeys(candidate_workers))
                options = " ".join(
                    f"[{i + 1}]{worker}" for i, worker in enumerate(self.assign_workers)
                )
                self.set_message(
                    (
                        f"Assign to: {options} [C]clear"
                        if options
                        else "Assign to: (no workers) [C]clear"
                    )
                )
            elif key in {"w", "W", "\x1b[A"}:
                self.exp_two_step = TwoStepKeyHandler()
                self.selected_exp_idx = max(0, self.selected_exp_idx - 1)
                if panel_experiments:
                    self.selected_exp_name = panel_experiments[
                        self.selected_exp_idx
                    ].get("name")
            elif key in {"s", "S", "\x1b[B"}:
                self.exp_two_step = TwoStepKeyHandler()
                if key == "S" and key in SCOPE_KEYS:
                    self.exp_two_step.handle_key(key)
                elif panel_experiments:
                    self.selected_exp_idx = min(
                        len(panel_experiments) - 1, self.selected_exp_idx + 1
                    )
                    self.selected_exp_name = panel_experiments[
                        self.selected_exp_idx
                    ].get("name")
            elif key == "N":
                self.exp_two_step = TwoStepKeyHandler()
                total = self._panel_exp_total or (
                    len(data.get("experiments", [])) + len(data.get("completed", []))
                )
                self._refresh_experiment_pagination(total)
                self._change_experiment_page(1)
            elif key == "P":
                self.exp_two_step = TwoStepKeyHandler()
                total = self._panel_exp_total or (
                    len(data.get("experiments", [])) + len(data.get("completed", []))
                )
                self._refresh_experiment_pagination(total)
                self._change_experiment_page(-1)
            elif key == "p" and panel_experiments:
                self.exp_two_step = TwoStepKeyHandler()
                name = self.selected_exp_name
                if not name:
                    self.set_message("No experiment selected")
                    return True
                exp_payload = next(
                    (e for e in panel_experiments if e.get("name") == name), None
                )
                if exp_payload is None:
                    self.set_message(f"Experiment {name} not found")
                    return True
                if self._is_non_actionable_row(exp_payload):
                    self.set_message("Condition node is display-only")
                    return True
                self._enqueue_action(
                    {
                        "type": "exp_repipeline",
                        "name": name,
                        "exp_payload": dict(exp_payload),
                    },
                    f"Re-pipeline {name}",
                )
            elif key.upper() == "T" and panel_experiments:
                self.exp_two_step = TwoStepKeyHandler()
                selected_exp = panel_experiments[self.selected_exp_idx]
                if self._is_non_actionable_row(selected_exp):
                    self.set_message("Condition node is display-only")
                    return True
                name = str(selected_exp.get("name", ""))
                if name:
                    self._enqueue_action(
                        {"type": "exp_start_now", "name": name},
                        f"Start-now {name}",
                    )
            elif key.upper() == "U" and panel_experiments:
                self.exp_two_step = TwoStepKeyHandler()
                selected_exp = panel_experiments[self.selected_exp_idx]
                if self._is_non_actionable_row(selected_exp):
                    self.set_message("Condition node is display-only")
                    return True
                name = str(selected_exp.get("name", ""))
                if name:
                    self._enqueue_action(
                        {"type": "exp_move", "name": name, "direction": "up"},
                        f"Move up {name}",
                    )
            elif key.upper() == "J" and panel_experiments:
                self.exp_two_step = TwoStepKeyHandler()
                selected_exp = panel_experiments[self.selected_exp_idx]
                if self._is_non_actionable_row(selected_exp):
                    self.set_message("Condition node is display-only")
                    return True
                name = str(selected_exp.get("name", ""))
                if name:
                    self._enqueue_action(
                        {"type": "exp_move", "name": name, "direction": "down"},
                        f"Move down {name}",
                    )
            else:
                key_lower = "d" if key.lower() == "x" else key.lower()
                action = self.exp_two_step.handle_key(key_lower)
                if (
                    action is None
                    and self.exp_two_step.state == "idle"
                    and key.lower() in {"k", "r", "d", "x", "v", "f"}
                ):
                    selected_action_map = {
                        "k": "kill",
                        "r": "rerun",
                        "d": "delete",
                        "x": "delete",
                        "v": "archive",
                        "f": "freeze",
                    }
                    mapped_action = selected_action_map.get(key_lower)
                    if mapped_action:
                        action = Action(scope="selected", action=mapped_action)
                if key == "\x1b" and self.exp_two_step.state == "idle":
                    self.set_message("Scope cancelled")
                if action and panel_experiments:
                    action_map = {
                        "kill": "exp_kill",
                        "delete": "exp_delete",
                        "archive": "exp_archive",
                        "rerun": "exp_rerun",
                        "freeze": "exp_freeze",
                    }
                    request_type = action_map.get(action.action)
                    if request_type is None:
                        self.set_message(
                            f"Unsupported experiment action: {action.action}"
                        )
                        return True
                    if action.scope == "all":
                        names = [
                            str(exp.get("name", ""))
                            for exp in panel_experiments
                            if str(exp.get("name", ""))
                            and not self._is_non_actionable_row(exp)
                        ]
                        dedup_names = sorted(dict.fromkeys(names))
                        for name in dedup_names:
                            verb = request_type.replace("exp_", "").replace("_", " ")
                            self._enqueue_action(
                                {"type": request_type, "name": name},
                                f"{verb.title()} {name}",
                            )
                    else:
                        selected_exp = panel_experiments[self.selected_exp_idx]
                        if self._is_non_actionable_row(selected_exp):
                            self.set_message("Condition node is display-only")
                            return True
                        name = str(selected_exp.get("name", ""))
                        if name:
                            verb = request_type.replace("exp_", "").replace("_", " ")
                            self._enqueue_action(
                                {"type": request_type, "name": name},
                                f"{verb.title()} {name}",
                            )
            return True

        if not workers:
            return True

        if key in {"w", "W", "\x1b[A"}:
            if not self.action_mode:
                self._move_cluster_selection(-1, 0, len(workers))
        elif key in {"s", "\x1b[B"}:
            if not self.action_mode:
                self._move_cluster_selection(1, 0, len(workers))
        elif key in {"a", "A", "\x1b[D"}:
            if self.action_mode:
                self.action_idx = max(0, self.action_idx - 1)
            else:
                self._move_cluster_selection(0, -1, len(workers))
        elif key in {"d", "\x1b[C"}:
            if self.action_mode:
                self.action_idx = min(len(self.actions) - 1, self.action_idx + 1)
            else:
                self._move_cluster_selection(0, 1, len(workers))
        elif key in ["\r", "\n"]:
            if not self.action_mode:
                self.action_mode = True
                self.action_idx = 0
            else:
                self._execute_action(workers)
                self.action_mode = False
        elif key.upper() == "D":
            node_id = workers[self.selected_node_idx]
            self._enqueue_action(
                {"type": "node_action", "node_id": node_id, "action": "disable"},
                f"DISABLE {node_id}",
            )
        elif key.upper() == "E":
            node_id = workers[self.selected_node_idx]
            self._enqueue_action(
                {"type": "node_action", "node_id": node_id, "action": "enable"},
                f"ENABLE {node_id}",
            )
        elif key.upper() == "R":
            node_id = workers[self.selected_node_idx]
            self._enqueue_action(
                {"type": "node_action", "node_id": node_id, "action": "restart"},
                f"RESTART {node_id}",
            )
        elif key.upper() == "S":
            node_id = workers[self.selected_node_idx]
            self._enqueue_action(
                {"type": "node_action", "node_id": node_id, "action": "start"},
                f"START {node_id}",
            )
        elif key.upper() == "K":
            node_id = workers[self.selected_node_idx]
            self._enqueue_action(
                {"type": "node_action", "node_id": node_id, "action": "stop"},
                f"STOP {node_id}",
            )
        elif key.upper() == "F":
            self._enqueue_action({"type": "reset_failed"}, "Reset failed experiments")
        elif key == "\x1b":
            self.action_mode = False

        return True

    def _execute_action(self, workers: List[str]):
        if not workers:
            return
        node_id = workers[self.selected_node_idx]
        action = self.actions[self.action_idx]
        self._enqueue_action(
            {"type": "node_action", "node_id": node_id, "action": action},
            f"{action.upper()} {node_id}",
        )

    def _quick_action(self, workers: List[str], action: str):
        if not workers:
            return
        node_id = workers[self.selected_node_idx]
        self._enqueue_action(
            {"type": "node_action", "node_id": node_id, "action": action},
            f"{action.upper()} {node_id}",
        )

    def _do_action_sync(self, node_id: str, action: str) -> str:
        # Watch mode = highest privilege (user-only); skip self-protection.
        if (
            not self.is_watch
            and node_id == self.worker_id
            and action
            in {
                "disable",
                "enable",
                "restart",
                "start",
                "stop",
            }
        ):
            return f"✗ Refused self-{action} on active runner {node_id}"
        if action == "disable":
            ok, msg = self.cluster_mgr.stop_node(node_id)
            if ok:
                killed = self.db.kill_experiments_on_worker(node_id)
                self.db.disable_worker(node_id)
                return f"✓ DISABLED {node_id}: {msg} | Killed {killed} experiment(s)"
            return f"✗ DISABLE {node_id} failed: {msg}"
        elif action == "enable":
            ok, msg = self.cluster_mgr.start_node(node_id, db=self.db)
            if ok:
                self.db.enable_worker(node_id)
                return f"✓ ENABLED {node_id}: {msg}"
            return f"✗ ENABLE {node_id} failed: {msg}"
        elif action == "restart":
            ok, msg = self.cluster_mgr.restart_node(node_id, db=self.db)
            if ok:
                killed = self.db.kill_experiments_on_worker(node_id)
                return f"✓ RESTART {node_id}: {msg} | Killed {killed} experiment(s)"
            return f"✗ RESTART {node_id} failed: {msg}"
        elif action == "start":
            ok, msg = self.cluster_mgr.start_node(node_id, db=self.db)
            return f"{'✓' if ok else '✗'} START {node_id}: {msg}"
        elif action == "stop":
            ok, msg = self.cluster_mgr.stop_node(node_id)
            return f"{'✓' if ok else '✗'} STOP {node_id}: {msg}"
        return f"✗ Unknown action: {action}"


# =============================================================================
# Logging
# =============================================================================


# =============================================================================
# Experiment Runner
# =============================================================================


# =============================================================================
# Main
# =============================================================================


def _handle_cli_command(args, cluster_mgr: ClusterManager, db: ExperimentsDB) -> int:
    if args.command == "cluster":
        if args.cluster_cmd == "status":
            payload = {
                "worker": args.worker_id,
                "cluster": cluster_mgr.get_cluster_status(db),
                "disabled_workers": sorted(
                    [wid for wid in cluster_mgr.machines if db.is_worker_disabled(wid)]
                ),
            }
            print(json.dumps(payload, ensure_ascii=True, default=str))
            return 0
        if args.cluster_cmd == "enable" and getattr(args, "all", False):
            changed = []
            skipped = []
            for node_id in sorted(cluster_mgr.machines.keys()):
                conf = cluster_mgr.machines.get(node_id, {})
                raw_max_gpus = conf.get("max_gpus") if isinstance(conf, dict) else None
                try:
                    max_gpus = int(raw_max_gpus)
                except (TypeError, ValueError):
                    max_gpus = None
                if max_gpus is not None and max_gpus <= 0:
                    skipped.append(node_id)
                    continue
                if db.enable_worker(node_id):
                    changed.append(node_id)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "command": "enable",
                        "scope": "all",
                        "changed": changed,
                        "skipped": skipped,
                    },
                    ensure_ascii=True,
                )
            )
            return 0

        if not args.node:
            print("cluster command requires --node", file=sys.stderr)
            return 2
        if args.cluster_cmd == "start":
            ok, msg = cluster_mgr.start_node(args.node, force_restart=args.force_restart, db=db)
        elif args.cluster_cmd == "stop":
            ok, msg = cluster_mgr.stop_node(args.node)
        elif args.cluster_cmd == "restart":
            ok, msg = cluster_mgr.restart_node(args.node, db=db)
        elif args.cluster_cmd == "enable":
            ok = db.enable_worker(args.node)
            msg = f"Enabled {args.node}" if ok else f"Enable failed for {args.node}"
        elif args.cluster_cmd == "disable":
            ok = db.disable_worker(args.node)
            msg = f"Disabled {args.node}" if ok else f"Disable failed for {args.node}"
        else:
            print(f"unknown cluster subcommand: {args.cluster_cmd}", file=sys.stderr)
            return 2
        print(
            json.dumps(
                {
                    "ok": ok,
                    "command": args.cluster_cmd,
                    "node": args.node,
                    "message": msg,
                },
                ensure_ascii=True,
            )
        )
        return 0 if ok else 1

    return 2


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(description="Phase 3 Unified Runner & Dashboard")
    parser.add_argument(
        "--worker_id", default=platform.node(), help="Worker identifier"
    )
    parser.add_argument(
        "--watch", action="store_true", help="Watch-only mode (no experiment execution)"
    )
    parser.add_argument(
        "--interval", type=int, default=5, help="Loop interval in seconds"
    )
    parser.add_argument(
        "--page",
        type=int,
        default=1,
        help="Initial experiment page in --watch mode (1-based)",
    )
    subparsers = parser.add_subparsers(dest="command")
    cluster_parser = subparsers.add_parser("cluster", help="Headless cluster operations")
    cluster_parser.add_argument(
        "--worker_id", default=platform.node(), help="Worker identifier"
    )
    cluster_parser.add_argument(
        "cluster_cmd",
        choices=["status", "start", "stop", "restart", "enable", "disable"],
        help="Cluster subcommand",
    )
    cluster_parser.add_argument("--node", help="Target worker/node id")
    cluster_parser.add_argument(
        "--all",
        action="store_true",
        help="Enable all non-management workers (for cluster enable)",
    )
    cluster_parser.add_argument(
        "--force-restart",
        action="store_true",
        help="Force restart when using cluster start",
    )
    add_common_args(parser)
    args = parser.parse_args()
    setup_logging(args)

    if args.dry_run:
        emit_result(
            args,
            {
                "dry_run": True,
                "worker_id": args.worker_id,
                "watch": args.watch,
                "interval": args.interval,
                "page": args.page,
                "experiments_file": str(EXPERIMENTS_FILE),
                "ready_queue_file": str(READY_QUEUE_FILE),
            },
        )
        return

    db = ExperimentsDB(json_path=EXPERIMENTS_FILE)
    cluster_mgr = ClusterManager()
    if args.command:
        raise SystemExit(_handle_cli_command(args, cluster_mgr, db))

    dashboard = UnifiedDashboard(args.worker_id, cluster_mgr, db, is_watch=args.watch)
    if args.watch:
        dashboard.exp_page = normalize_initial_exp_page(
            args.page, dashboard.exp_total_pages
        )
    logger = HybridLogger(str(RUNNER_LOG_FILE))

    logger.log(f"Runner started on {args.worker_id} (watch={args.watch})")
    if not args.watch:
        cleanup_on_startup(logger)

    current_max_jobs = MAX_JOBS_PER_GPU
    current_max_gpus = None
    current_preferred_gpu = None
    current_python_env = "~/miniconda3/envs/gnn_fraud/bin/python"
    if args.worker_id in cluster_mgr.machines:
        conf = cluster_mgr.machines[args.worker_id]
        if "max_jobs_per_gpu" in conf:
            current_max_jobs = conf["max_jobs_per_gpu"]
            logger.log(f"Config override: max_jobs_per_gpu={current_max_jobs}")
        if "max_gpus" in conf:
            current_max_gpus = conf["max_gpus"]
            logger.log(f"Config override: max_gpus={current_max_gpus}")
        if "preferred_gpu" in conf:
            current_preferred_gpu = conf["preferred_gpu"]
            logger.log(f"Config override: preferred_gpu={current_preferred_gpu}")
        if "python_env" in conf:
            current_python_env = conf["python_env"]
            logger.log(f"Config override: python_env={current_python_env}")

    if not args.watch and _is_management_only_worker(args.worker_id, cluster_mgr):
        logger.log(
            "This machine (max_gpus=0) is management-only; refusing worker loop startup"
        )
        print(
            "[worker-guard] This machine (max_gpus=0) is management-only",
            file=sys.stderr,
        )
        return

    allocator = GPUAllocator(
        max_jobs_per_gpu=current_max_jobs,
        max_gpus=current_max_gpus,
        preferred_gpu=current_preferred_gpu,
    )

    running_futures: Dict[str, Future] = {}
    running_futures_lock = threading.Lock()
    running_processes: Dict[str, subprocess.Popen] = {}
    running_processes_lock = threading.Lock()
    running_gpu_ids: Dict[str, int] = {}
    running_gpu_ids_lock = threading.Lock()
    paused_formal_jobs: Set[str] = set()

    auto_wake_interval = 60
    auto_wake_last: Dict[str, float] = {}
    orphan_reap_last = 0.0
    archive_trigger_count = 3
    archive_check_interval = 60.0
    archive_last_check = 0.0
    archive_last_signature = ""
    heartbeat_failure_count = 0
    heartbeat_last_error_log = 0.0
    heartbeat_log_interval = 30.0

    stop_requested = {"value": False}

    def _terminate_running_processes():
        with running_processes_lock:
            procs_snapshot = list(running_processes.items())
        for exp_name, proc in procs_snapshot:
            try:
                proc.terminate()
            except Exception:
                pass
        time.sleep(2)
        with running_processes_lock:
            procs_snapshot = list(running_processes.items())
        for exp_name, proc in procs_snapshot:
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass

    def _signal_handler(signum, frame):
        if signum == signal.SIGHUP:
            logger.log("Signal received: 1 (SIGHUP). Ignored by runner.")
            return
        stop_requested["value"] = True
        logger.log(f"Signal received: {signum}. Terminating running processes...")
        _terminate_running_processes()

    def run_and_cleanup(exp, gpu_id):
        try:
            run_experiment_process(
                exp,
                args.worker_id,
                gpu_id,
                logger,
                db,
                running_processes,
                running_processes_lock,
                current_python_env,
            )
        finally:
            allocator.release(exp["name"])
            with running_processes_lock:
                running_processes.pop(exp["name"], None)
            with running_gpu_ids_lock:
                running_gpu_ids.pop(exp["name"], None)

    def heal_registry_from_running_processes():
        with running_processes_lock:
            proc_snapshot = dict(running_processes)
        if not proc_snapshot:
            return
        for exp_name, proc in proc_snapshot.items():
            if proc.poll() is not None:
                continue
            exp = db.get_experiment(exp_name)
            if not exp:
                continue
            running_on = exp.get("running_on") or {}
            try:
                reg_pid = int(running_on.get("pid") or -1)
            except (TypeError, ValueError):
                reg_pid = -1
            is_running_ok = (
                normalize_status(exp.get("status")) == STATUS_RUNNING
                and str(running_on.get("worker", "")) == args.worker_id
                and reg_pid == int(proc.pid)
            )
            if is_running_ok:
                continue

            with running_gpu_ids_lock:
                gpu_id = running_gpu_ids.get(exp_name)
            if gpu_id is None:
                try:
                    gpu_id = int(running_on.get("gpu") or 0)
                except (TypeError, ValueError):
                    gpu_id = 0

            started_at = str(running_on.get("started_at") or datetime.now().isoformat())
            ok = db.heal_from_running_process(
                exp_name, args.worker_id, int(gpu_id), int(proc.pid), started_at
            )
            if ok:
                logger.log(
                    f"Healed RUNNING registry for {exp_name}: worker={args.worker_id} gpu={gpu_id} pid={proc.pid}"
                )
            else:
                logger.log(
                    f"Healing failed for {exp_name}: process alive pid={proc.pid}"
                )

    def scheduler_loop(executor: ThreadPoolExecutor):
        nonlocal orphan_reap_last
        nonlocal archive_last_check
        nonlocal archive_last_signature

        def auto_wake_offline_nodes(now: float):
            if now - auto_wake_last.get("_tick", 0) < auto_wake_interval:
                return
            cluster_status = cluster_mgr.get_cluster_status(db)
            for node_id in cluster_mgr.machines:
                if node_id == args.worker_id:
                    continue
                if db.is_worker_disabled(node_id):
                    continue
                status = cluster_status.get(node_id, {})
                if status.get("status") != "OFFLINE":
                    continue
                last_seen_sec = status.get("last_seen_sec", 999999)
                if isinstance(last_seen_sec, (int, float)) and last_seen_sec < HEARTBEAT_STALE_SEC:
                    continue
                last_wake = auto_wake_last.get(node_id, 0)
                if now - last_wake < auto_wake_interval:
                    continue
                ok, msg = cluster_mgr.start_node(node_id, db=db)
                logger.log(f"Auto-wake {node_id}: {'OK' if ok else 'FAIL'} {msg}")
                auto_wake_last[node_id] = now
            auto_wake_last["_tick"] = now

        def maybe_archive_completed(now: float):
            nonlocal archive_last_check
            nonlocal archive_last_signature
            if not AUTO_ARCHIVE_ENABLED:
                return
            if now - archive_last_check < archive_check_interval:
                return
            archive_last_check = now

            snapshot = db.load()
            completed = snapshot.get("completed", [])
            if not isinstance(completed, list):
                return
            candidates = []
            for exp in completed:
                if not isinstance(exp, dict):
                    continue
                if not exp.get("doc_processed_at"):
                    continue
                name = str(exp.get("name") or "").strip()
                if not name:
                    continue
                candidates.append(name)
            candidates.sort()
            if len(candidates) < archive_trigger_count:
                return
            signature = "|".join(candidates)
            if signature == archive_last_signature:
                return

            archive_script = BASE_DIR.parent / "archive_script.py"
            runner_python = os.path.expanduser(current_python_env or sys.executable)
            cmd = [runner_python, str(archive_script)]
            proc = subprocess.run(
                cmd,
                cwd=BASE_DIR.parent,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if proc.returncode == 0:
                archive_last_signature = signature
                report_path = BASE_DIR.parent / "docs" / "LATEST_BATCH_REPORT.md"
                report_ok = report_path.exists()
                logger.log(
                    f"Auto-archive executed (completed={len(candidates)}, report={report_ok})"
                )
            else:
                err = (proc.stderr or proc.stdout or "").strip()
                logger.log(f"Auto-archive failed: {err[:300]}")

        while not stop_requested["value"]:
            if args.watch:
                time.sleep(max(0.2, float(args.interval)))
                continue
            try:
                process_remote_termination_requests(db, args.worker_id, logger)
                maybe_archive_completed(time.time())
                heal_registry_from_running_processes()
                reconcile_terminal_artifacts(db, logger)
                check_stale_locks(
                    db,
                    logger,
                    local_worker_id=args.worker_id,
                    cluster_mgr=cluster_mgr,
                )
                self_heal_heartbeat_worker_conflicts(db, cluster_mgr, logger)
                enforce_running_pid_registration(db, logger)
                enforce_formal_slot_serialization(
                    running_processes,
                    running_processes_lock,
                    running_gpu_ids,
                    running_gpu_ids_lock,
                    allocator,
                    paused_formal_jobs,
                    logger,
                )
                with running_futures_lock:
                    protected_names = set(running_futures.keys())
                check_zombie_processes(
                    db, args.worker_id, logger, protected_names=protected_names
                )

                now = time.time()
                if now - orphan_reap_last >= ORPHAN_REAPER_INTERVAL_SEC:
                    reap_orphan_runner_processes(
                        logger, os.getpid(), db, worker_id=args.worker_id
                    )
                    with running_futures_lock:
                        active_exp_names = set(running_futures.keys())
                    reap_orphan_training_processes(
                        db,
                        logger,
                        args.worker_id,
                        active_exp_names,
                    )
                    orphan_reap_last = now

                consume_ready_queue_registration_handoff(db, logger)

                local_max_gpu = max((g["total"] for g in allocator.gpus), default=24000)
                runnable = db.get_runnable_experiments(
                    local_max_gpu, worker_id=args.worker_id
                )

                for exp in runnable:
                    if db.is_worker_disabled(args.worker_id):
                        break

                    exp_name = exp["name"]
                    with running_futures_lock:
                        if exp_name in running_futures:
                            continue

                    required_mem_mb = get_required_mem_mb(exp)
                    gpu_id = allocator.allocate(
                        exp_name, required_mem_mb=required_mem_mb
                    )
                    if gpu_id is None:
                        best_free = max((g["free"] for g in allocator.gpus), default=0)
                        if best_free < required_mem_mb:
                            logger.log(
                                f"VRAM gate: {exp_name} needs {required_mem_mb}MB, "
                                f"best free {best_free}MB → BLOCKED"
                            )
                        continue

                    run_id = db.claim_experiment(
                        exp_name, args.worker_id, gpu_id, os.getpid()
                    )
                    if run_id:
                        logger.log(
                            f"Claimed {exp_name} on GPU {gpu_id} (run_id={run_id[:8]})"
                        )
                        future = executor.submit(run_and_cleanup, exp, gpu_id)
                        with running_futures_lock:
                            running_futures[exp_name] = future
                        with running_gpu_ids_lock:
                            running_gpu_ids[exp_name] = int(gpu_id)
                    else:
                        allocator.release(exp_name)

            except Exception as e:
                logger.log(f"Scheduler loop error: {e}")

            time.sleep(max(0.2, float(args.interval)))

    num_gpus = len(allocator.gpus) if allocator.gpus else 1
    max_workers = num_gpus * current_max_jobs

    input_stream = sys.stdin
    input_stream_stack = ExitStack()
    use_input = True
    if not sys.stdin.isatty():
        try:
            input_stream = input_stream_stack.enter_context(open("/dev/tty", "r"))
        except Exception:
            use_input = False

    if use_input:
        fd = cast(int, input_stream.fileno())
        old_settings = termios.tcgetattr(fd)
    else:
        fd = None
        old_settings = None

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGHUP, _signal_handler)
        if use_input and fd is not None:
            tty.setcbreak(fd)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            scheduler_thread = threading.Thread(
                target=scheduler_loop, args=(executor,), daemon=True
            )
            scheduler_thread.start()
            with Live(
                dashboard.build_layout(0), refresh_per_second=8, screen=True
            ) as live:
                while True:
                    dashboard.drain_async_updates()

                    try:
                        sys_info: Dict[str, Any] = {"gpus": [], "cpu": {}}
                        try:
                            sys_info = collect_system_info()
                        except Exception as sys_err:
                            now = time.time()
                            if (
                                heartbeat_failure_count == 0
                                or now - heartbeat_last_error_log >= heartbeat_log_interval
                            ):
                                heartbeat_last_error_log = now
                                logger.log(
                                    "System info probe failed; heartbeat uses empty payload: "
                                    f"worker={args.worker_id} err={type(sys_err).__name__}: {sys_err}"
                                )

                        if not args.watch:
                            with running_futures_lock:
                                running_names = sorted(running_futures.keys())

                            heartbeat_ok = db.update_heartbeat(
                                args.worker_id,
                                os.getpid(),
                                len(running_names),
                                running_names,
                                sys_info.get("gpus"),
                                sys_info.get("cpu"),
                            )

                            if heartbeat_ok:
                                if heartbeat_failure_count:
                                    logger.log(
                                        "Heartbeat write recovered: "
                                        f"worker={args.worker_id} recover_after={heartbeat_failure_count}"
                                    )
                                heartbeat_failure_count = 0
                            else:
                                heartbeat_failure_count += 1
                                now = time.time()
                                if (
                                    heartbeat_failure_count == 1
                                    or now - heartbeat_last_error_log >= heartbeat_log_interval
                                ):
                                    heartbeat_last_error_log = now
                                    logger.log(
                                        "Heartbeat DB write failed: "
                                        f"worker={args.worker_id} pid={os.getpid()} "
                                        f"running_jobs={len(running_names)} "
                                        f"running_names={running_names[:5]} "
                                        f"consecutive_failures={heartbeat_failure_count} "
                                        f"detail={db.get_last_heartbeat_error()}"
                                    )
                    except Exception as e:
                        heartbeat_failure_count += 1
                        now = time.time()
                        if (
                            heartbeat_failure_count == 1
                            or now - heartbeat_last_error_log >= heartbeat_log_interval
                        ):
                            heartbeat_last_error_log = now
                            logger.log(
                                "Heartbeat loop failed: "
                                f"worker={args.worker_id} err={type(e).__name__}: {e} "
                                f"consecutive_failures={heartbeat_failure_count}"
                            )

                    if use_input:
                        quit_requested = False
                        while True:
                            rlist, _, _ = select.select([input_stream], [], [], 0)
                            if not rlist:
                                break
                            key = get_key(input_stream)
                            if key:
                                cluster_status = cluster_mgr.get_cluster_status(db)
                                workers = sorted(cluster_status.keys())
                                if not dashboard.handle_key(key, workers):
                                    quit_requested = True
                                    break
                        if quit_requested:
                            break

                    with running_futures_lock:
                        completed_names = [
                            name
                            for name, future in running_futures.items()
                            if future.done()
                        ]
                    for name in completed_names:
                        with running_futures_lock:
                            future = running_futures.pop(name, None)
                        if future is None:
                            continue
                        try:
                            future.result()
                        except Exception as e:
                            logger.log(f"Experiment {name} raised exception: {e}")

                    if stop_requested["value"]:
                        break

                    with running_futures_lock:
                        running_count = len(running_futures)
                    live.update(dashboard.build_layout(running_count))

                    time.sleep(0.1)

            stop_requested["value"] = True
            scheduler_thread.join(timeout=2)

    except KeyboardInterrupt:
        logger.log("Runner interrupted by user")
        _terminate_running_processes()
    finally:
        dashboard.shutdown()
        if use_input and fd is not None and old_settings is not None:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        input_stream_stack.close()
        logger.log("Runner stopped")


if __name__ == "__main__":
    main()
