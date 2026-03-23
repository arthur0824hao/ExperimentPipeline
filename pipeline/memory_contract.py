#!/usr/bin/env python3

import importlib
import sys
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from artifact import _read_result_payload
except ModuleNotFoundError:
    from pipeline.artifact import _read_result_payload

try:
    from runtime_config import cfg_int, get_runtime_section
except ModuleNotFoundError:
    from pipeline.runtime_config import cfg_int, get_runtime_section


def _should_fallback_memory_estimator_import(exc: Exception) -> bool:
    if not isinstance(exc, (ImportError, ModuleNotFoundError, OSError)):
        return False
    msg = f"{type(exc).__name__}: {exc}"
    markers = (
        "libtorch_global_deps.so",
        "preprocess_lib/__init__.py",
        "torch",
        "No module named 'preprocess_lib'",
    )
    return any(marker in msg for marker in markers)


PREPROCESS_LIB_DIR = Path(__file__).resolve().parent / "preprocess_lib"
if str(PREPROCESS_LIB_DIR) not in sys.path:
    sys.path.insert(0, str(PREPROCESS_LIB_DIR))

try:
    from memory_estimator import infer_memory_contract_for_exp
except Exception as exc:
    if not _should_fallback_memory_estimator_import(exc):
        raise
    _memory_estimator = importlib.import_module("memory_estimator")
    infer_memory_contract_for_exp = _memory_estimator.infer_memory_contract_for_exp

BASE_DIR = Path(__file__).parent.absolute()
RESULTS_DB_DIR = BASE_DIR / "results_db"
_RUNNER_CFG = get_runtime_section("experiments_runner")
OOM_THRESHOLD_MB = cfg_int(_RUNNER_CFG, "true_oom_threshold_mb", 24000)


def _copy_memory_contract(exp_config: Dict[str, Any]) -> Dict[str, Any]:
    contract = exp_config.get("memory_contract")
    if isinstance(contract, dict):
        return dict(contract)
    return {}


def _update_oom_policy_contract(
    contract: Dict[str, Any],
    *,
    current_batch_size: int,
    current_eval_batch_size: int,
    next_batch_size: int,
    next_eval_batch_size: int,
    expected_required_free_mb: int,
    stop_reason: str,
    force_true_mem: bool,
) -> Dict[str, Any]:
    updated = dict(contract)
    old_est = int(
        updated.get("est_mem_decision_mb") or updated.get("est_mem_upper_mb") or 0
    )
    new_est = max(old_est, int(expected_required_free_mb))
    if force_true_mem:
        new_est = max(new_est, OOM_THRESHOLD_MB + 1)
    updated.update(
        {
            "batch_size_before_retry": current_batch_size,
            "batch_size_after_retry": next_batch_size,
            "eval_batch_size_before_retry": current_eval_batch_size,
            "eval_batch_size_after_retry": next_eval_batch_size,
            "est_mem_before_retry": old_est,
            "est_mem_after_retry": new_est,
            "est_mem_decision_mb": new_est,
            "oom_retry_policy": str(
                updated.get("oom_policy_mode")
                or (
                    "batch_adjustable"
                    if updated.get("runtime_batch_adjustable")
                    else "not_applicable"
                )
            ),
            "policy_forced_true_mem": bool(force_true_mem),
            "stop_reason": stop_reason,
        }
    )
    return updated


def _persist_oom_policy_contract(
    db: Any,
    exp_name: str,
    contract: Dict[str, Any],
    oom_retry_count: int,
) -> None:
    try:
        db.update_experiment(
            exp_name,
            {
                "memory_contract": contract,
                "oom_retry_count": oom_retry_count,
            },
        )
    except Exception:
        pass


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _should_reestimate_memory_contract(
    contract: Dict[str, Any], result_payload: Optional[Dict[str, Any]]
) -> bool:
    if not isinstance(contract, dict) or not contract:
        return True
    if not isinstance(result_payload, dict):
        return False
    result_hidden_dim = _coerce_int(result_payload.get("hidden_dim"))
    contract_hidden_dim = _coerce_int(contract.get("hidden_dim"))
    return (
        result_hidden_dim > 0
        and contract_hidden_dim > 0
        and result_hidden_dim != contract_hidden_dim
    )


def get_memory_contract(exp: Dict[str, Any]) -> Dict[str, Any]:
    contract = exp.get("memory_contract") or {}
    exp_name = str(exp.get("name") or "").strip()
    result_payload: Optional[Dict[str, Any]] = None
    if exp_name:
        _result_path, result_payload = _read_result_payload(exp_name)
    if (
        isinstance(contract, dict)
        and contract
        and not _should_reestimate_memory_contract(contract, result_payload)
    ):
        return contract
    exp_for_infer = dict(exp)
    exp_for_infer.pop("memory_contract", None)
    try:
        inferred = infer_memory_contract_for_exp(exp_for_infer, BASE_DIR)
    except Exception:
        if isinstance(contract, dict) and contract:
            return contract
        return {}
    if isinstance(inferred, dict) and inferred:
        exp["memory_contract"] = inferred
        return inferred
    if isinstance(contract, dict) and contract:
        return contract
    return {}


def format_memory_contract_fields(exp: Dict[str, Any]) -> Dict[str, str]:
    contract = get_memory_contract(exp)
    family_raw = str(contract.get("memory_family") or "-")
    family_display = {
        "fullbatch_sparse_gnn": "fullbatch",
        "neighborloader_gnn": "neighbor",
        "temporal_edge_batch": "temporal",
        "no_batch_path_child": "no-batch",
    }
    mem_family = family_display.get(family_raw, family_raw)
    est_initial = contract.get("est_mem_decision_mb")
    est_mb = "-"
    if isinstance(est_initial, (int, float)) and est_initial > 0:
        est_mb = f"{int(est_initial)}"
    mode_raw = str(contract.get("execution_mode") or contract.get("memory_mode") or "-")
    mode_display = {
        "fullbatch": "fullbatch",
        "neighborloader": "neighbor",
        "temporal_batch": "temporal",
        "fullgraph_no_batch_path": "no-batch",
    }
    mem_mode = mode_display.get(mode_raw, mode_raw)
    if contract.get("neighborloader_recommended"):
        nbldr = "reco"
    elif contract.get("neighborloader_applicable"):
        nbldr = "yes"
    elif contract:
        nbldr = "no"
    else:
        nbldr = "-"
    return {
        "mem_family": mem_family,
        "est_mb": est_mb,
        "mem_mode": mem_mode,
        "nbldr": nbldr,
    }


def get_required_mem_mb(exp: Dict[str, Any]) -> int:
    contract = get_memory_contract(exp)
    est_decision = contract.get("est_mem_decision_mb")
    est_upper = contract.get("est_mem_upper_mb")
    est_initial = contract.get("est_mem_initial_mb")
    est_required = 0
    for value in (est_decision, est_upper, est_initial):
        try:
            if value is not None:
                est_required = max(est_required, int(value))
        except (TypeError, ValueError):
            continue

    err_info = exp.get("error_info") or {}
    err_type = str(err_info.get("type", "") or "").upper()
    try:
        err_peak_mb = int(err_info.get("peak_memory_mb", 0) or 0)
    except (TypeError, ValueError):
        err_peak_mb = 0
    buffer_mb = 500 if err_type == "OOM" else 256
    retry_required = err_peak_mb + buffer_mb if err_peak_mb > 0 else 0
    return max(4000, est_required, retry_required)
