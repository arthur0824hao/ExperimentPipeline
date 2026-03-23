import json
import os
import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

try:
    from runtime_config import cfg_float, get_runtime_section
except ModuleNotFoundError:
    from .runtime_config import cfg_float, get_runtime_section


BASE_DIR = Path(__file__).parent.absolute()
RESULTS_DB_DIR = BASE_DIR / "results_db"
PREPROCESS_PROGRESS_FILE = BASE_DIR / "preprocess_progress.json"
LOGS_DIR = BASE_DIR / "logs"
_RUNNER_CFG = get_runtime_section("experiments_runner")
ARTIFACT_RECONCILE_GRACE_SEC = cfg_float(
    _RUNNER_CFG, "artifact_reconcile_grace_sec", 120.0
)

STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"


def normalize_status(raw_status: Any) -> str:
    status = str(raw_status or "").upper()
    if status in (STATUS_NEEDS_RERUN, STATUS_RUNNING, STATUS_COMPLETED):
        return status
    if status == "DONE":
        return STATUS_COMPLETED
    if status == "SKIPPED":
        return STATUS_COMPLETED
    if status in ("READY", "ERROR", "OOM"):
        return STATUS_NEEDS_RERUN
    return STATUS_NEEDS_RERUN


def update_running_peak(db: Any, exp_name: str, peak_memory_mb: int):
    db.update_running_peak(exp_name, peak_memory_mb)


def _load_json_dict(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _artifact_timestamp(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def _failed_timestamp(exp: Dict[str, Any]) -> Optional[float]:
    error_info = exp.get("error_info") or {}
    failed_at = error_info.get("failed_at")
    if not failed_at:
        return None
    try:
        return datetime.fromisoformat(str(failed_at)).timestamp()
    except Exception:
        return None


def _artifact_is_fresh(path: Path, failed_ts: Optional[float]) -> bool:
    stamp = _artifact_timestamp(path)
    if stamp is None:
        return False
    if failed_ts is None:
        return True
    return stamp + ARTIFACT_RECONCILE_GRACE_SEC >= failed_ts


def _stderr_is_empty(exp_name: str) -> bool:
    stderr_path = LOGS_DIR / f"{exp_name}.err"
    try:
        return (not stderr_path.exists()) or stderr_path.stat().st_size == 0
    except Exception:
        return False


def _read_resource_usage(
    exp_name: str,
) -> tuple[Optional[Path], Optional[Dict[str, Any]]]:
    exp_dir = BASE_DIR / "experiments" / exp_name
    path = exp_dir / "resource_usage.json"
    return path, _load_json_dict(path)


def _read_result_payload(
    exp_name: str,
) -> tuple[Optional[Path], Optional[Dict[str, Any]]]:
    candidates = [
        RESULTS_DB_DIR / f"{exp_name}.json",
        BASE_DIR / "experiments" / exp_name / "outputs" / "results.json",
    ]
    freshest_path: Optional[Path] = None
    freshest_payload: Optional[Dict[str, Any]] = None
    freshest_ts = -1.0
    for path in candidates:
        payload = _load_json_dict(path)
        stamp = _artifact_timestamp(path)
        if payload is None or stamp is None:
            continue
        if stamp > freshest_ts:
            freshest_ts = stamp
            freshest_path = path
            freshest_payload = payload
    return freshest_path, freshest_payload


def _coerce_completed_result(
    result_payload: Dict[str, Any], resource_payload: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    f1 = result_payload.get("f1_score")
    if f1 is None:
        f1 = result_payload.get("test_f1")
    auc = result_payload.get("auc_score")
    if auc is None:
        auc = result_payload.get("test_auc")
    peak = result_payload.get("peak_memory_mb")
    if peak is None and isinstance(resource_payload, dict):
        peak = resource_payload.get("peak_memory_mb", 0)
    return {
        "f1_score": f1,
        "auc_score": auc,
        "peak_memory_mb": peak if isinstance(peak, (int, float)) else 0,
    }


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _extract_peak_from_payload(payload: Optional[Dict[str, Any]]) -> int:
    if not isinstance(payload, dict):
        return 0
    runtime_meta = payload.get("runtime_meta") or {}
    child_fp = runtime_meta.get("child_fingerprint") or {}
    candidates = [
        payload.get("peak_memory_mb"),
        runtime_meta.get("validated_peak_mb"),
        runtime_meta.get("current_peak_mb"),
        child_fp.get("validated_peak_mb"),
        child_fp.get("current_peak_mb"),
    ]
    return max((_coerce_int(v) for v in candidates), default=0)


def _best_error_peak_mb(
    tracked_peak_mb: int,
    requested_mb: int,
    resource_payload: Optional[Dict[str, Any]] = None,
    result_payload: Optional[Dict[str, Any]] = None,
) -> int:
    candidates: List[Any] = [tracked_peak_mb, requested_mb]
    if isinstance(resource_payload, dict):
        candidates.append(resource_payload.get("peak_memory_mb"))
        candidates.append(resource_payload.get("requested_peak_memory_mb"))
    candidates.append(_extract_peak_from_payload(result_payload))
    return max((_coerce_int(v) for v in candidates), default=0)


def get_experiment_progress(exp_name: str) -> Optional[Dict]:
    progress_file = BASE_DIR / "experiments" / exp_name / ".progress"
    if not progress_file.exists():
        return None
    try:
        with open(progress_file, "r") as f:
            return json.load(f)
    except Exception:
        return None


def get_completed_result_summary(
    exp_name: str,
    result: Optional[Dict],
    canonical_result: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[int], Optional[float]]:
    merged: Dict[str, Any] = {}
    if isinstance(result, dict):
        merged.update(result)
    if isinstance(canonical_result, dict):
        merged.update(canonical_result)
    elif exp_name:
        result_file = RESULTS_DB_DIR / f"{exp_name}.json"
        if result_file.exists():
            try:
                with open(result_file, "r") as f:
                    payload = json.load(f)
                if isinstance(payload, dict):
                    merged.update(payload)
            except Exception:
                pass

    epochs_raw = merged.get("epochs_ran")
    test_f1_raw = merged.get("test_f1")
    if test_f1_raw is None:
        test_f1_raw = merged.get("f1_score")

    epochs: Optional[int]
    if epochs_raw is None:
        epochs = None
    else:
        try:
            epochs = int(epochs_raw)
        except (TypeError, ValueError):
            epochs = None

    test_f1: Optional[float]
    if test_f1_raw is None:
        test_f1 = None
    else:
        try:
            test_f1 = float(test_f1_raw)
        except (TypeError, ValueError):
            test_f1 = None

    return epochs, test_f1


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


def get_terminal_reason(
    exp_name: str,
    status: str,
    result: Optional[Dict],
    error_info: Optional[Dict],
    terminal_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    status_norm = normalize_status(status)
    return _get_db_terminal_reason(status_norm, result, error_info, terminal_metadata)
