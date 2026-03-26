from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from artifact import (
    _artifact_is_fresh,
    _coerce_completed_result,
    _coerce_float,
    _failed_timestamp,
    _read_resource_usage,
    _read_result_payload,
    _stderr_is_empty,
)
from formatting import normalize_status


STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"


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
            return "FAILED_OOM" if bool(error_info.get("is_true_oom", False)) else "QUEUED_RETRY"
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
    _ = exp_name
    status_norm = normalize_status(status)
    return _get_db_terminal_reason(status_norm, result, error_info, terminal_metadata)


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
        if artifact_returncode not in (None, 0) or "oom" in artifact_failure or "oom" in artifact_verdict:
            return "artifact_failed_vs_db_completed"
        db_f1 = _coerce_float((result or {}).get("f1_score"))
        if db_f1 is not None and artifact_test_f1 is not None and abs(db_f1 - artifact_test_f1) > 1e-9:
            return "artifact_metric_drift"
    if status_norm == STATUS_NEEDS_RERUN and db_terminal == "FAILED_SCRIPT_ERROR":
        if "oom" in artifact_failure or "oom" in artifact_verdict:
            return "artifact_oom_vs_db_script_error"
    return None


def reconcile_terminal_artifacts(db: Any, logger: Optional[Any] = None) -> List[str]:
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
