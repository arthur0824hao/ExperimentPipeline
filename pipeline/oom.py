import re
import os
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any

try:
    from runtime_config import (
        cfg_int,
        get_experiment_env_overrides,
        get_runtime_section,
    )
except ModuleNotFoundError:
    from pipeline.runtime_config import (
        cfg_int,
        get_experiment_env_overrides,
        get_runtime_section,
    )

_RUNNER_CFG = get_runtime_section("experiments_runner")

OOM_RETRY_EST_MEM_BUMP_MB = cfg_int(_RUNNER_CFG, "oom_retry_est_mem_bump_mb", 1024)
OOM_EXPECTED_FREE_MARGIN_MB = cfg_int(_RUNNER_CFG, "oom_expected_free_margin_mb", 500)
MIN_RUNTIME_BATCH_SIZE = cfg_int(_RUNNER_CFG, "min_runtime_batch_size", 32)


def _coerce_positive_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _script_env_default(script_path: Path, env_name: str) -> Optional[int]:
    try:
        source = script_path.read_text(encoding="utf-8")
    except OSError:
        return None
    match = re.search(
        rf'os\.environ\.setdefault\(["\']{re.escape(env_name)}["\'],\s*["\'](\d+)["\']\)',
        source,
    )
    if not match:
        return None
    return _coerce_positive_int(match.group(1))


def _resolve_batch_overrides(
    exp_name: str, exp_config: Dict[str, Any], script_path: Path
) -> Tuple[int, int]:
    env_overrides = get_experiment_env_overrides(exp_name)
    batch_size = _coerce_positive_int(exp_config.get("batch_size"))
    if batch_size is None:
        batch_size = _coerce_positive_int(env_overrides.get("BATCH_SIZE"))
    if batch_size is None:
        batch_size = _script_env_default(script_path, "BATCH_SIZE") or 1024

    eval_batch_size = _coerce_positive_int(exp_config.get("eval_batch_size"))
    if eval_batch_size is None:
        eval_batch_size = _coerce_positive_int(env_overrides.get("EVAL_BATCH_SIZE"))
    if eval_batch_size is None:
        eval_batch_size = _script_env_default(script_path, "EVAL_BATCH_SIZE") or max(
            batch_size * 4, batch_size
        )
    return batch_size, max(eval_batch_size, batch_size)


def _next_smaller_batches(batch_size: int, eval_batch_size: int) -> Tuple[int, int]:
    next_batch = max(32, batch_size // 2)
    next_eval = max(next_batch, eval_batch_size // 2)
    return next_batch, next_eval


def parse_oom_from_stderr(stderr_path: Path) -> tuple:
    """Returns (is_oom, is_true_oom, requested_mb)"""
    import re

    try:
        with open(stderr_path, "r") as f:
            content = f.read()

        oom_indicators = [
            "CUDA out of memory",
            "OutOfMemoryError",
            "CUDA error: out of memory",
        ]
        is_oom = any(ind in content for ind in oom_indicators)

        if not is_oom:
            return False, False, 0

        pattern = r"[Tt]ried to allocate\s+([\d.]+)\s*(GiB|MiB|GB|MB)"
        match = re.search(pattern, content)
        requested_mb = 0
        if match:
            amount = float(match.group(1))
            unit = match.group(2).upper()
            requested_mb = int(amount * 1024) if unit in ("GIB", "GB") else int(amount)

        return True, False, requested_mb
    except Exception:
        return False, False, 0
