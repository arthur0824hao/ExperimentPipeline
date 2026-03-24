from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List


PHASE3_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PHASE3_DIR.parent.parent
PHASE3_RUNTIME_CONFIG_FILE = PROJECT_ROOT / "configs" / "phase3_runtime.json"
DEFAULT_PRE_WARM_FEATURE_SETS: Dict[str, List[str]] = {
    "ORIGIN": ["base_basic12_cut_d152"],
    "SENIOR10": ["base_basic12_cut_d152", "senior_gap_10dim_cut_d152"],
    "YOUR32": [
        "base_basic12_cut_d152",
        "base_regen22_cut_d152",
        "balance_vol_4dim_cut_d152",
        "velocity_3dim_cut_d152",
        "burst_3dim_cut_d152",
    ],
    "COMBINED": [
        "base_basic12_cut_d152",
        "base_regen22_cut_d152",
        "balance_vol_4dim_cut_d152",
        "velocity_3dim_cut_d152",
        "burst_3dim_cut_d152",
        "senior_gap_10dim_cut_d152",
    ],
}


def load_phase3_runtime_config() -> Dict[str, Any]:
    try:
        payload = json.loads(PHASE3_RUNTIME_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def get_runtime_section(name: str) -> Dict[str, Any]:
    payload = load_phase3_runtime_config()
    section = payload.get(name, {})
    return section if isinstance(section, dict) else {}


def cfg_int(section: Dict[str, Any], key: str, default: int) -> int:
    try:
        return int(section.get(key, default))
    except (TypeError, ValueError):
        return int(default)


def cfg_float(section: Dict[str, Any], key: str, default: float) -> float:
    try:
        return float(section.get(key, default))
    except (TypeError, ValueError):
        return float(default)


def cfg_bool(section: Dict[str, Any], key: str, default: bool) -> bool:
    raw = section.get(key, default)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return bool(raw)


def cfg_str(section: Dict[str, Any], key: str, default: str) -> str:
    raw = section.get(key, default)
    if raw is None:
        return str(default)
    return str(raw)


def cfg_list(section: Dict[str, Any], key: str, default: Iterable[int]) -> List[int]:
    raw = section.get(key)
    if isinstance(raw, list):
        values: List[int] = []
        for item in raw:
            try:
                values.append(int(item))
            except (TypeError, ValueError):
                continue
        if values:
            return values
    return [int(x) for x in default]


def get_pre_warm_config() -> Dict[str, Any]:
    section = get_runtime_section("pre_warm")
    workers_default = max((os.cpu_count() or 2) - 1, 1)
    workers = max(1, cfg_int(section, "parallel_workers", workers_default))

    raw_sets = section.get("phase1_feature_sets")
    parsed_sets: Dict[str, List[str]] = {}
    if isinstance(raw_sets, dict):
        for name, values in raw_sets.items():
            if not isinstance(values, list):
                continue
            feature_names = [
                str(feature).strip() for feature in values if str(feature).strip()
            ]
            if feature_names:
                parsed_sets[str(name).strip().upper()] = feature_names

    if not parsed_sets:
        parsed_sets = dict(DEFAULT_PRE_WARM_FEATURE_SETS)

    return {
        "enabled": cfg_bool(section, "enabled", False),
        "parallel_workers": workers,
        "phase1_feature_sets": parsed_sets,
    }


def resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def get_experiment_env_overrides(experiment_name: str) -> Dict[str, str]:
    section = get_runtime_section("experiment_env")
    raw = section.get(experiment_name, {})
    if not isinstance(raw, dict):
        return {}
    overrides: Dict[str, str] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if isinstance(value, list):
            overrides[str(key)] = ",".join(str(item) for item in value)
        else:
            overrides[str(key)] = str(value)
    return overrides
