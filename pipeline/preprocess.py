#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 3 Preprocessing Orchestrator

Reads `ready.json` (staging area), processes feature/graph requirements,
then auto-registers experiments to `experiments.json`.

Workflow:
    1. Load ready.json (pending experiments)
    2. When ready_to_process=1:
        a. Archive completed experiments (DONE → archived[])
        b. Generate missing features
        b2. Generate queued standalone feature jobs
        c. Register new experiments with batch_id
        d. Reset ready_to_process=0
    3. Clear processed entries from ready.json

Usage:
    python preprocess.py [--loop] [--interval 10]

Schema (ready.json):
    {
      "ready_to_process": 0,
      "batch_id": "3I-Series",
      "experiments": [
          {
            "name": "ExpName",
            "features": ["feat1", "feat2"],
            "script": "path/to/train.py",
            "description": "...",
            "priority": 10,
            "feature_recipes": {...}
          }
      ],
      "feature_jobs": [
          {
            "name": "base_34dim_cut_d152",
            "feature_recipes": {...}
          }
      ]
    }

Schema (experiments.json):
    {
      "experiments": [
          {
            "name": "ExpName",
            "batch_id": "3I-Series",
            "status": "READY|RUNNING|DONE|ERROR|OOM",
            "running_on": {"worker", "gpu", "pid", "started_at"},
            "completed_at": "...",
            "error_info": {"type", "is_true_oom", "message"}
          }
      ],
      "archived": [...]
    }
"""

import os
import sys
import json
import importlib
import glob
import re
import subprocess
import shutil
import time
import argparse
import select
import termios
import tty
from pathlib import Path
from typing import Dict, List, Set, Any, Tuple
from datetime import datetime
import concurrent.futures
import threading
from importlib import util as importlib_util
from rich import box
from rich.console import Console, Group
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cli_shared import add_common_args, emit_result, setup_logging  # type: ignore
from db_registry import derive_progression_status  # type: ignore
from tui_keys import TwoStepKeyHandler  # type: ignore

PHASE3_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PHASE3_ROOT.parent
PHASE3_RUNTIME_CONFIG_FILE = PROJECT_ROOT / "configs" / "phase3_runtime.json"
PREPROCESS_LIB_DIR = PHASE3_ROOT / "preprocess_lib"
RESULTS_DB_DIR = PHASE3_ROOT / "results_db"


def _load_phase3_runtime_config() -> Dict[str, Any]:
    try:
        payload = json.loads(PHASE3_RUNTIME_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _cfg_int(section: Dict[str, Any], key: str, default: int) -> int:
    try:
        raw = section.get(key, default)
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


_PHASE3_RUNTIME_CFG = _load_phase3_runtime_config()
_PREPROCESS_CFG = (
    _PHASE3_RUNTIME_CFG.get("preprocess", {})
    if isinstance(_PHASE3_RUNTIME_CFG.get("preprocess", {}), dict)
    else {}
)
DEFAULT_LOOP_INTERVAL_SEC = _cfg_int(_PREPROCESS_CFG, "loop_interval_sec", 10)
DEFAULT_WATCH_PAGE_SIZE = _cfg_int(_PREPROCESS_CFG, "watch_page_size", 20)
DEFAULT_WATCH_PAGE = _cfg_int(_PREPROCESS_CFG, "watch_page", 1)
GATE_BATCH_SIZE = _cfg_int(_PREPROCESS_CFG, "gate_batch_size", 256)


def _should_fallback_gate_engine_import(exc: Exception) -> bool:
    if not isinstance(exc, (ImportError, ModuleNotFoundError, OSError)):
        return False
    msg = f"{type(exc).__name__}: {exc}"
    markers = ("libtorch_global_deps.so", "preprocess_lib/__init__.py", "torch")
    return any(marker in msg for marker in markers)


from registry_io import load_registry, save_registry  # type: ignore

try:
    from preprocess_lib.gate_engine import load_rules, run_gate_rules  # type: ignore
    from preprocess_lib.memory_estimator import (
        estimate_experiment_memory_contract,
        infer_memory_contract_for_exp,
    )  # type: ignore
except Exception as exc:
    if not _should_fallback_gate_engine_import(exc):
        raise
    if str(PREPROCESS_LIB_DIR) not in sys.path:
        sys.path.insert(0, str(PREPROCESS_LIB_DIR))
    _gate_engine = importlib.import_module("gate_engine")
    _memory_estimator = importlib.import_module("memory_estimator")
    load_rules = _gate_engine.load_rules
    run_gate_rules = _gate_engine.run_gate_rules
    estimate_experiment_memory_contract = (
        _memory_estimator.estimate_experiment_memory_contract
    )
    infer_memory_contract_for_exp = _memory_estimator.infer_memory_contract_for_exp

REGISTRY_LOCK = threading.Lock()

# =============================================================================
# Paths
# =============================================================================

READY_FILE = PHASE3_ROOT / "ready.json"
EXPERIMENTS_FILE = PHASE3_ROOT / "experiments.json"
LOCKS_DIR = PHASE3_ROOT / "locks"
FEATURE_BANK_DIR = PHASE3_ROOT / "data" / "feature_bank"
REGISTRY_FILE = FEATURE_BANK_DIR / "registry.json"
LOG_FILE = PHASE3_ROOT / "preprocess.log"
TESTS_DIR = PHASE3_ROOT / "tests"
DEFAULT_GATE_TEST = TESTS_DIR / "test_default_gate.py"
GATE_BANK_FILE = PHASE3_ROOT / "gate_bank.json"


def _resolve_runner_python() -> str:
    explicit = os.environ.get("PHASE3_PYTHON", "").strip()
    if explicit and Path(explicit).exists():
        return explicit

    current = Path(sys.executable).resolve()
    if "envs/gnn_fraud/" in str(current):
        return str(current)

    conda_root = current.parent.parent
    candidate = conda_root / "envs" / "gnn_fraud" / "bin" / "python"
    if candidate.exists():
        return str(candidate)

    return str(current)


RUNNER_PYTHON = _resolve_runner_python()

# =============================================================================
# Helper Functions
# =============================================================================


class _TeeStream:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for stream in self._streams:
            stream.write(data)

    def flush(self):
        for stream in self._streams:
            stream.flush()


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except:
        return {}


def save_json(path: Path, data: Any):
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp_path.rename(path)


def _load_json_dict(path: Path) -> Dict[str, Any] | None:
    payload = load_json(path)
    return payload if isinstance(payload, dict) else None


def _artifact_timestamp(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _read_result_payload(exp_name: str) -> Tuple[Path | None, Dict[str, Any] | None]:
    candidates = [
        RESULTS_DB_DIR / f"{exp_name}.json",
        PHASE3_ROOT / "experiments" / exp_name / "outputs" / "results.json",
    ]
    freshest_path: Path | None = None
    freshest_payload: Dict[str, Any] | None = None
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


def _coerce_optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_watch_memory_contract(exp: Dict[str, Any]) -> Dict[str, Any]:
    existing = exp.get("memory_contract") or {}
    if not isinstance(existing, dict):
        existing = {}

    exp_name = str(exp.get("name") or "").strip()
    _result_path, result_payload = (
        _read_result_payload(exp_name) if exp_name else (None, None)
    )
    result_hidden_dim = _coerce_optional_int(
        (result_payload or {}).get("hidden_dim")
        if isinstance(result_payload, dict)
        else None
    )
    contract_hidden_dim = _coerce_optional_int(existing.get("hidden_dim"))

    needs_reestimate = not existing or (
        result_hidden_dim is not None
        and contract_hidden_dim is not None
        and result_hidden_dim != contract_hidden_dim
    )
    if not needs_reestimate:
        return existing

    exp_for_infer = dict(exp)
    exp_for_infer.pop("memory_contract", None)
    try:
        inferred = infer_memory_contract_for_exp(exp_for_infer, PHASE3_ROOT)
    except Exception:
        inferred = {}
    if isinstance(inferred, dict) and inferred:
        return inferred
    return existing


def _row_priority(row: Dict[str, str]) -> Tuple[int, int]:
    status_priority = {
        "COMPLETED": 4,
        "RUNNING": 3,
        "WARM": 3,
        "BLOCKED_CONDITION": 2,
        "READY": 2,
        "NEEDS_RERUN": 1,
    }
    bucket_priority = {"completed": 1, "active": 0}
    return (
        status_priority.get(str(row.get("status") or "").upper(), 0),
        bucket_priority.get(str(row.get("bucket") or ""), 0),
    )


def _slugify_experiment_name(name: str) -> str:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", spaced)
    return normalized.strip("_").lower()


def get_experiment_test_path(exp: Dict) -> Path:
    test_file = exp.get("test_file")
    if test_file:
        path = Path(test_file)
        return path if path.is_absolute() else PHASE3_ROOT / test_file
    slug = _slugify_experiment_name(exp["name"])
    return TESTS_DIR / f"test_{slug}.py"


def _compact_pytest_output(stdout: str, stderr: str, max_lines: int = 20) -> str:
    lines = []
    if stdout:
        lines.extend(stdout.splitlines())
    if stderr:
        lines.extend(stderr.splitlines())
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def _get_experiment_train_path(exp: Dict) -> Path:
    script = exp.get("script")
    if script:
        p = Path(script)
        return p if p.is_absolute() else PHASE3_ROOT / script
    name = exp.get("name")
    if not name:
        raise ValueError("Experiment is missing 'name'")
    return PHASE3_ROOT / "experiments" / name / "scripts" / "train.py"


def run_experiment_gate(exp: Dict) -> Dict[str, Any]:
    rules = load_rules(GATE_BANK_FILE)
    if rules:
        report = run_gate_rules(exp, PHASE3_ROOT, rules)
        if report.has_errors:
            return {
                "passed": False,
                "status": "RULE_BLOCKED",
                "message": f"Gate bank errors:\n{report.summary()}",
            }
        if report.has_warnings:
            print(f"  Gate bank warnings for {exp.get('name', '?')}:")
            print(report.summary())

    test_path = get_experiment_test_path(exp)
    if not test_path.exists():
        if DEFAULT_GATE_TEST.exists():
            test_path = DEFAULT_GATE_TEST
        else:
            return {
                "passed": False,
                "status": "MISSING_TEST",
                "message": f"Missing test file: {test_path}",
            }

    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    extra_paths = [str(PHASE3_ROOT)]
    env["PYTHONPATH"] = os.pathsep.join(
        extra_paths + ([python_path] if python_path else [])
    )
    env.setdefault("CUDA_LAUNCH_BLOCKING", "1")
    env.setdefault("PYTORCH_NO_CUDA_MEMORY_CACHING", "1")
    env.setdefault("PHASE3_GATE", "1")
    if exp.get("name"):
        env.setdefault("GATE_EXPERIMENT", exp["name"])
    env.setdefault("GATE_BATCH_SIZE", str(GATE_BATCH_SIZE))
    env.setdefault("BATCH_SIZE", str(GATE_BATCH_SIZE))

    command = [
        RUNNER_PYTHON,
        "-m",
        "pytest",
        str(test_path),
        "-x",
        "--tb=short",
        "-q",
    ]

    result = subprocess.run(
        command,
        cwd=PHASE3_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        # Estimator removed: warmup phase reveals actual memory.
        # Try estimation best-effort; never block on failure.
        memory_contract: Dict[str, Any] = {}
        try:
            memory_contract = estimate_experiment_memory_contract(exp, PHASE3_ROOT)
        except Exception:
            pass  # non-fatal: runner warmup will measure actual memory
        return {
            "passed": True,
            "status": "PASSED",
            "message": "",
            "memory_contract": memory_contract,
        }

    message = _compact_pytest_output(result.stdout, result.stderr)
    if result.returncode == 5:
        status = "NO_TESTS"
        message = message or "No tests collected"
    else:
        status = "FAILED"
        if not message:
            message = f"pytest failed with code {result.returncode}"

    return {"passed": False, "status": status, "message": message}


def load_experiments_json() -> Dict:
    """Load experiments.json with proper schema."""
    return load_registry(EXPERIMENTS_FILE)


def save_experiments_json(data: Dict):
    save_registry(EXPERIMENTS_FILE, data)


def get_available_features() -> Set[str]:
    def _cut_feature_aliases(name: str) -> Set[str]:
        aliases = {name}
        m = re.match(r"^(.*_cut_d)(\d+)$", name)
        if not m:
            return aliases
        prefix, digits = m.groups()
        value = int(digits)
        aliases.add(f"{prefix}{value}")
        aliases.add(f"{prefix}{value:03d}")
        return aliases

    if not REGISTRY_FILE.exists():
        return set()
    try:
        with open(REGISTRY_FILE, "r") as f:
            registry = json.load(f)
        features = registry.get("features", {})
        available = set()
        for name, info in features.items():
            feat_file = info.get("file")
            if feat_file is None:
                available.update(_cut_feature_aliases(name))
                continue
            feat_path = FEATURE_BANK_DIR / feat_file
            if feat_path.exists():
                available.update(_cut_feature_aliases(name))
                continue
            if "/" in str(feat_file):
                alt_path = PHASE3_ROOT / "data" / feat_file
                if alt_path.exists():
                    available.update(_cut_feature_aliases(name))
        return available
    except:
        return set()


def is_experiment_ready(exp: Dict, available_features: Set[str]) -> bool:
    required = set(exp.get("features", []))
    return required.issubset(available_features)


# =============================================================================
# Feature Processing
# =============================================================================


def _update_registry(feat_name: str, feat_file: str, dims: int, description: str):
    with REGISTRY_LOCK:
        registry = {}
        if REGISTRY_FILE.exists():
            try:
                with open(REGISTRY_FILE, "r") as f:
                    registry = json.load(f)
            except:
                pass

        if "features" not in registry:
            registry["features"] = {}
        if "presets" not in registry:
            registry["presets"] = {}

        registry["features"][feat_name] = {
            "file": feat_file,
            "dims": dims,
            "description": description,
            "generated_at": datetime.now().isoformat(),
        }

        with open(REGISTRY_FILE, "w") as f:
            json.dump(registry, f, indent=2)

    print(
        f"    [Thread-{threading.get_ident()}] Updated registry: {feat_name} ({dims} dims)"
    )


def _run_script(script_path: Path, args: List[str]):
    command = [RUNNER_PYTHON, str(script_path)] + args
    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    extra_paths = [str(PHASE3_ROOT), str(PHASE3_ROOT / "preprocess_lib")]
    env["PYTHONPATH"] = os.pathsep.join(
        extra_paths + ([python_path] if python_path else [])
    )
    env.setdefault("PHASE3_PYTHON", RUNNER_PYTHON)
    subprocess.run(command, check=True, cwd=PHASE3_ROOT, env=env)


def _parse_cutoff_suffix(feature_name: str):
    if "_cut_" not in feature_name:
        return None
    suffix = feature_name.split("_cut_", 1)[1]
    if suffix in {"split", "alert"}:
        return {"policy": suffix, "obs_end": None, "suffix": suffix}
    if suffix.startswith("d") and suffix[1:].isdigit():
        return {"policy": "fixed", "obs_end": int(suffix[1:]), "suffix": suffix}
    return None


def _ensure_cut_feature(feature_name: str):
    cutoff = _parse_cutoff_suffix(feature_name)
    if cutoff is None:
        raise ValueError(f"Unrecognized cutoff feature name: {feature_name}")

    policy = cutoff["policy"]
    obs_end = cutoff["obs_end"]
    suffix = cutoff["suffix"]

    scripts_dir = PHASE3_ROOT / "scripts"

    def _ensure_output_alias_for_requested_name():
        requested_path = FEATURE_BANK_DIR / f"{feature_name}.pt"
        if requested_path.exists():
            return

        if obs_end is None:
            return

        requested_suffix = f"d{obs_end:03d}"
        canonical_suffix = f"d{int(obs_end)}"
        if requested_suffix == canonical_suffix:
            return

        alt_name = feature_name.replace(
            f"_cut_{requested_suffix}", f"_cut_{canonical_suffix}"
        )
        alt_path = FEATURE_BANK_DIR / f"{alt_name}.pt"
        if alt_path.exists():
            shutil.copyfile(alt_path, requested_path)

    if feature_name.startswith("base_34dim_cut_"):
        script = scripts_dir / "compute_base_cutoff_features.py"
        args = ["--policies", policy]
        if obs_end is not None:
            args += ["--obs_end", str(obs_end)]
        _run_script(script, args)
        _ensure_output_alias_for_requested_name()
        _update_registry(
            feature_name,
            f"{feature_name}.pt",
            34,
            f"Base features with {policy} cutoff ({suffix})",
        )
        return

    if feature_name.startswith("balance_vol_4dim_cut_"):
        script = scripts_dir / "compute_features.py"
        args = ["--features", "balance_vol", "--cutoff_policy", policy]
        if obs_end is not None:
            args += ["--obs_end", str(obs_end)]
        _run_script(script, args)
        _ensure_output_alias_for_requested_name()
        _update_registry(
            feature_name,
            f"{feature_name}.pt",
            4,
            f"Balance volatility features with {policy} cutoff ({suffix})",
        )
        return

    if feature_name.startswith("velocity_3dim_cut_"):
        script = scripts_dir / "compute_features.py"
        args = ["--features", "velocity", "--cutoff_policy", policy]
        if obs_end is not None:
            args += ["--obs_end", str(obs_end)]
        _run_script(script, args)
        _ensure_output_alias_for_requested_name()
        _update_registry(
            feature_name,
            f"{feature_name}.pt",
            3,
            f"Velocity features with {policy} cutoff ({suffix})",
        )
        return

    if feature_name.startswith("burst_3dim_cut_"):
        script = scripts_dir / "compute_features.py"
        args = ["--features", "burst", "--cutoff_policy", policy]
        if obs_end is not None:
            args += ["--obs_end", str(obs_end)]
        _run_script(script, args)
        _ensure_output_alias_for_requested_name()
        _update_registry(
            feature_name,
            f"{feature_name}.pt",
            3,
            f"Burst features with {policy} cutoff ({suffix})",
        )
        return

    if feature_name.startswith("ratio_6dim_cut_"):
        script = scripts_dir / "compute_ratio_features.py"
        args = ["--policy", policy]
        if obs_end is not None:
            args += ["--obs_end", str(obs_end)]
        _run_script(script, args)
        _update_registry(
            feature_name,
            f"{feature_name}.pt",
            6,
            f"Ratio features with {policy} cutoff ({suffix})",
        )
        return

    if feature_name.startswith("seq_v1_cut_"):
        script = scripts_dir / "compute_sequence_bank.py"
        args = ["--policy", policy]
        if obs_end is not None:
            args += ["--obs_end", str(obs_end)]
        _run_script(script, args)
        seq_src = PHASE3_ROOT / "data" / "sequence_bank" / f"{feature_name}.pt"
        seq_dir = FEATURE_BANK_DIR / "sequence_bank"
        seq_dir.mkdir(parents=True, exist_ok=True)
        seq_dst = seq_dir / f"{feature_name}.pt"
        if seq_src.exists():
            shutil.copyfile(seq_src, seq_dst)
        seq_file = f"sequence_bank/{feature_name}.pt"
        _update_registry(
            feature_name, seq_file, 7, f"Sequence bank with {policy} cutoff ({suffix})"
        )
        return

    raise ValueError(f"Unsupported cutoff feature: {feature_name}")


def generate_velocity_features():
    import torch
    from preprocess_lib.feature_bank import get_legacy_graph_path  # type: ignore

    print("    Loading base graph for velocity computation...")
    graph_path = get_legacy_graph_path()
    if not os.path.exists(graph_path):
        raise FileNotFoundError(f"Need base graph to compute velocity: {graph_path}")

    data = torch.load(graph_path, weights_only=False)
    num_nodes = data.num_nodes

    velocity_1d = torch.zeros(num_nodes, 1)
    velocity_7d = torch.zeros(num_nodes, 1)
    velocity_30d = torch.zeros(num_nodes, 1)

    from torch_geometric.utils import degree  # pyright: ignore[reportMissingImports]

    deg = degree(data.edge_index[0], num_nodes=num_nodes)
    velocity_1d[:, 0] = deg / 30.0
    velocity_7d[:, 0] = deg / 7.0
    velocity_30d[:, 0] = deg / 1.0

    velocity_tensor = torch.cat([velocity_1d, velocity_7d, velocity_30d], dim=1)

    feat_file = "velocity_3dim.pt"
    torch.save(velocity_tensor, FEATURE_BANK_DIR / feat_file)
    _update_registry(
        "velocity_3dim", feat_file, 3, "Transaction velocity at 1d/7d/30d windows"
    )
    print(f"    Saved {feat_file}")


def generate_burst_features():
    import torch
    from preprocess_lib.feature_bank import get_legacy_graph_path  # type: ignore

    print("    Loading base graph for burst computation...")
    graph_path = get_legacy_graph_path()
    data = torch.load(graph_path, weights_only=False)
    num_nodes = data.num_nodes

    from torch_geometric.utils import degree  # pyright: ignore[reportMissingImports]

    deg = degree(data.edge_index[0], num_nodes=num_nodes)

    burst_score = torch.zeros(num_nodes, 1)
    burst_count = torch.zeros(num_nodes, 1)
    burst_intensity = torch.zeros(num_nodes, 1)

    mean_deg = deg.mean()
    burst_score[:, 0] = (deg - mean_deg).clamp(min=0) / (mean_deg + 1e-6)
    burst_count[:, 0] = (deg > mean_deg * 2).float()
    burst_intensity[:, 0] = torch.log1p(deg) / torch.log1p(deg.max() + 1e-6)

    burst_tensor = torch.cat([burst_score, burst_count, burst_intensity], dim=1)

    feat_file = "burst_3dim.pt"
    torch.save(burst_tensor, FEATURE_BANK_DIR / feat_file)
    _update_registry("burst_3dim", feat_file, 3, "Burst detection features")
    print(f"    Saved {feat_file}")


def generate_balance_vol_features():
    import torch
    from preprocess_lib.feature_bank import get_legacy_graph_path  # type: ignore

    print("    Loading base graph for balance volatility...")
    graph_path = get_legacy_graph_path()
    data = torch.load(graph_path, weights_only=False)
    num_nodes = data.num_nodes

    if data.x.shape[1] >= 34:
        balance_mean = data.x[:, 1:2]
        balance_std = data.x[:, 2:3].abs()
        balance_min = data.x[:, 0:1]
        balance_max = data.x[:, 3:4]
    else:
        balance_mean = torch.zeros(num_nodes, 1)
        balance_std = torch.zeros(num_nodes, 1)
        balance_min = torch.zeros(num_nodes, 1)
        balance_max = torch.zeros(num_nodes, 1)

    balance_tensor = torch.cat(
        [balance_mean, balance_std, balance_min, balance_max], dim=1
    )

    feat_file = "balance_vol_4dim.pt"
    torch.save(balance_tensor, FEATURE_BANK_DIR / feat_file)
    _update_registry("balance_vol_4dim", feat_file, 4, "Balance volatility features")
    print(f"    Saved {feat_file}")


FEATURE_GENERATORS = {
    "velocity": generate_velocity_features,
    "velocity_3dim": generate_velocity_features,
    "velocity_features": generate_velocity_features,
    "burst": generate_burst_features,
    "burst_3dim": generate_burst_features,
    "burst_features": generate_burst_features,
    "balance_vol": generate_balance_vol_features,
    "balance_vol_4dim": generate_balance_vol_features,
    "balance_volatility": generate_balance_vol_features,
}


def register_generator(name: str, func):
    FEATURE_GENERATORS[name] = func


def generate_single_feature(feat_name: str, exp: Dict) -> bool:
    recipes = exp.get("feature_recipes", {})

    generator = FEATURE_GENERATORS.get(feat_name)

    if generator is None:
        normalized = feat_name.replace("-", "_").lower()
        generator = FEATURE_GENERATORS.get(normalized)

    if generator is not None:
        print(f"  [AutoGen] Starting: {feat_name}")
        try:
            generator()
            print(f"  [AutoGen] SUCCESS: {feat_name}")
            return True
        except Exception as e:
            print(f"  [AutoGen] ERROR {feat_name}: {e}")
            import traceback

            traceback.print_exc()
            return False

    elif "_cut_" in feat_name:
        print(f"  [AutoGen] Starting cutoff: {feat_name}")
        try:
            _ensure_cut_feature(feat_name)
            print(f"  [AutoGen] SUCCESS: {feat_name}")
            return True
        except Exception as e:
            print(f"  [AutoGen] ERROR {feat_name}: {e}")
            import traceback

            traceback.print_exc()
            return False

    elif feat_name in recipes:
        recipe = recipes[feat_name]
        print(f"  [Recipe] Starting {feat_name}: {recipe}")
        script = recipe.get("script")
        args = recipe.get("args", [])
        dims = recipe.get("dims")
        description = recipe.get("description", "Custom feature")
        feat_file = recipe.get("file", f"{feat_name}.pt")
        if script is None:
            print(f"  [Recipe] WARNING: Missing script for {feat_name}, skipping")
            return False
        try:
            _run_script(Path(script), [str(a) for a in args])
            if dims is not None:
                _update_registry(feat_name, feat_file, int(dims), description)
            print(f"  [Recipe] SUCCESS: {feat_name}")
            return True
        except Exception as e:
            print(f"  [Recipe] ERROR {feat_name}: {e}")
            import traceback

            traceback.print_exc()
            return False

    else:
        print(f"  [Skip] No generator found for '{feat_name}'")
        return False


def collect_missing_tasks(
    ready_queue: List[Dict], available_features: Set[str]
) -> Dict[str, Dict]:
    tasks = {}
    for exp in ready_queue:
        required = set(exp.get("features", []))
        missing = required - available_features
        for feat in missing:
            if feat not in tasks:
                tasks[feat] = exp
    return tasks


def collect_feature_jobs(
    raw_feature_jobs: List[Any], available_features: Set[str]
) -> Tuple[Dict[str, Dict], List[Dict]]:
    tasks: Dict[str, Dict] = {}
    normalized_jobs: List[Dict] = []
    for job in raw_feature_jobs:
        if isinstance(job, str):
            job_dict = {"name": job}
        elif isinstance(job, dict):
            job_dict = dict(job)
        else:
            continue
        name = str(job_dict.get("name") or "").strip()
        if not name:
            continue
        job_dict["name"] = name
        normalized_jobs.append(job_dict)
        if name not in available_features and name not in tasks:
            tasks[name] = job_dict
    return tasks, normalized_jobs


# =============================================================================
# Experiment Registration & Archiving
# =============================================================================


def move_completed_experiments(exp_data: Dict) -> Dict:
    experiments = exp_data.get("experiments", [])
    completed = exp_data.get("completed", [])

    still_active = []
    newly_completed = []

    for exp in experiments:
        status = str(exp.get("status", "")).upper()
        if status in ("DONE", "COMPLETED"):
            exp["completed_at"] = exp.get("completed_at") or datetime.now().isoformat()
            completed.append(exp)
            newly_completed.append(exp["name"])
        else:
            still_active.append(exp)

    if newly_completed:
        print(
            f"Moved {len(newly_completed)} completed experiment(s) to completed[]: {newly_completed}"
        )

    exp_data["experiments"] = still_active
    exp_data["completed"] = completed
    return exp_data


def archive_doc_processed_completed(exp_data: Dict) -> Dict:
    completed = exp_data.get("completed", [])
    archived = exp_data.get("archived", [])
    remaining = []
    moved = []

    for exp in completed:
        if exp.get("doc_processed"):
            exp["archived_at"] = datetime.now().isoformat()
            archived.append(exp)
            moved.append(exp.get("name", "UNKNOWN"))
        else:
            remaining.append(exp)

    if moved:
        print(f"Archived {len(moved)} doc-processed completed experiment(s): {moved}")

    exp_data["completed"] = remaining
    exp_data["archived"] = archived
    return exp_data


# =============================================================================
# Main Orchestration
# =============================================================================


def run_once():
    ready_data = load_json(READY_FILE)

    if not isinstance(ready_data, dict):
        if isinstance(ready_data, list) and len(ready_data) > 0:
            ready_data = {
                "ready_to_process": 1,
                "batch_id": "legacy",
                "experiments": ready_data,
            }
        else:
            return

    flag = ready_data.get("ready_to_process", 0)
    if str(flag) not in ("1", "True", "true"):
        return

    print(f"[READY] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    batch_id = ready_data.get(
        "batch_id", f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    ready_queue = ready_data.get("experiments", [])
    raw_feature_jobs = ready_data.get("feature_jobs", [])

    print("=" * 60)
    print(f"Processing batch: {batch_id}")
    print(f"Experiments in queue: {len(ready_queue)}")
    print("=" * 60)

    experiments_file_exists = EXPERIMENTS_FILE.exists()
    raw_experiments = load_json(EXPERIMENTS_FILE)
    legacy_experiments_mode = False
    if not experiments_file_exists:
        legacy_experiments_mode = True
        exp_data = {"experiments": [], "completed": [], "archived": []}
    elif isinstance(raw_experiments, list):
        legacy_experiments_mode = True
        exp_data = {"experiments": raw_experiments, "completed": [], "archived": []}
    elif isinstance(raw_experiments, dict):
        exp_data = load_experiments_json()
    else:
        legacy_experiments_mode = False
        exp_data = load_experiments_json()
    available_features = get_available_features()

    exp_data = move_completed_experiments(exp_data)
    exp_data = archive_doc_processed_completed(exp_data)

    missing_tasks = collect_missing_tasks(ready_queue, available_features)
    feature_tasks, normalized_feature_jobs = collect_feature_jobs(
        raw_feature_jobs, available_features
    )
    for feat_name, job in feature_tasks.items():
        missing_tasks.setdefault(feat_name, job)

    if missing_tasks:
        print(f"Found {len(missing_tasks)} missing feature(s) to generate.")
        print(f"Tasks: {list(missing_tasks.keys())}")
        print("Starting parallel generation...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_feat = {
                executor.submit(
                    generate_single_feature, feat, missing_tasks[feat]
                ): feat
                for feat in missing_tasks
            }

            for future in concurrent.futures.as_completed(future_to_feat):
                feat = future_to_feat[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"  CRITICAL ERROR in worker for {feat}: {e}")

        available_features = get_available_features()

        print("Parallel generation completed.")
        print("-" * 60)

    processed = []
    for exp in ready_queue:
        name = exp.get("name", "UNKNOWN")
        print(f"Registration Check: {name}")

        if not is_experiment_ready(exp, available_features):
            print("  SKIPPED: Missing features still not available.")
            continue

        gate_result = run_experiment_gate(exp)
        exp["gate_status"] = gate_result["status"]
        exp["gate_checked_at"] = datetime.now().isoformat()
        if not gate_result["passed"]:
            exp["gate_message"] = gate_result["message"]
            print(f"  GATE FAILED: {gate_result['status']}")
            if gate_result["message"]:
                print(f"  GATE MESSAGE: {gate_result['message']}")
            continue

        memory_contract = gate_result.get("memory_contract")
        if isinstance(memory_contract, dict) and memory_contract:
            exp["memory_contract"] = dict(memory_contract)

        exp["features_ready"] = True
        exp["gate_passed_at"] = datetime.now().isoformat()
        processed.append(exp["name"])

    remaining_feature_jobs = []
    for job in normalized_feature_jobs:
        name = job["name"]
        if name not in available_features:
            remaining_feature_jobs.append(job)

    if legacy_experiments_mode:
        save_json(EXPERIMENTS_FILE, exp_data.get("experiments", []))
    else:
        save_experiments_json(exp_data)
    print(
        f"Saved experiments.json ({len(exp_data['experiments'])} active, {len(exp_data.get('completed', []))} completed, {len(exp_data['archived'])} archived)"
    )

    remaining = [e for e in ready_queue if e["name"] not in processed]
    ready_data["experiments"] = remaining
    ready_data["feature_jobs"] = remaining_feature_jobs
    ready_data["ready_to_process"] = 0
    save_json(READY_FILE, ready_data)
    print(
        f"Reset ready_to_process=0. Remaining experiments: {len(remaining)}, remaining feature_jobs: {len(remaining_feature_jobs)}, processed experiments: {len(processed)}"
    )


def _resolve_watch_feature_path(
    feature_name: str, feature: Dict[str, Any], artifact: Dict[str, Any]
) -> Path | None:
    candidates = [
        artifact.get("path") if isinstance(artifact, dict) else None,
        feature.get("file") if isinstance(feature, dict) else None,
        f"{feature_name}.pt",
    ]
    seen: Set[str] = set()
    fallback: Path | None = None
    for raw in candidates:
        rel_path = str(raw or "").strip()
        if not rel_path or rel_path in seen:
            continue
        seen.add(rel_path)
        candidate = FEATURE_BANK_DIR / rel_path
        if candidate.exists():
            return candidate
        alt_candidate = PHASE3_ROOT / "data" / rel_path
        if alt_candidate.exists():
            return alt_candidate
        fallback = candidate
    return fallback


def _derive_watch_feature_dependency_signal(
    feature: Dict[str, Any], artifact: Dict[str, Any]
) -> str:
    start_idx = _coerce_optional_int(feature.get("start_idx"))
    end_idx = _coerce_optional_int(feature.get("end_idx"))
    artifact_total_dim = _coerce_optional_int(artifact.get("total_dim"))
    if (
        start_idx is not None
        and end_idx is not None
        and artifact_total_dim is not None
        and (start_idx != 0 or end_idx != artifact_total_dim)
    ):
        return f"slice[{start_idx}:{end_idx}]"

    producer_script = str(artifact.get("producer_script") or "").strip()
    if producer_script:
        return Path(producer_script).name

    semantic_group = str(
        feature.get("semantic_group") or artifact.get("semantic_group") or ""
    ).strip()
    if semantic_group:
        return semantic_group

    feature_kind = str(feature.get("kind") or "").strip()
    if feature_kind:
        return feature_kind
    return "-"


def _collect_computing_feature_names(
    ready_queue: List[Dict[str, Any]], feature_jobs: List[Dict[str, Any]]
) -> Set[str]:
    available_features = get_available_features()
    missing_tasks = collect_missing_tasks(ready_queue, available_features)
    feature_tasks, _normalized_jobs = collect_feature_jobs(
        feature_jobs, available_features
    )
    return set(missing_tasks.keys()) | set(feature_tasks.keys())


def _collect_watch_rows(
    computing_features: Set[str] | None = None,
) -> List[Dict[str, str]]:
    if computing_features is None:
        ready_data = load_json(READY_FILE)
        ready_queue = []
        feature_jobs = []
        if isinstance(ready_data, dict):
            queue_raw = ready_data.get("experiments", [])
            jobs_raw = ready_data.get("feature_jobs", [])
            if isinstance(queue_raw, list):
                ready_queue = [item for item in queue_raw if isinstance(item, dict)]
            if isinstance(jobs_raw, list):
                feature_jobs = [item for item in jobs_raw if isinstance(item, dict)]
        computing_features = _collect_computing_feature_names(ready_queue, feature_jobs)

    registry = load_json(REGISTRY_FILE)
    if not isinstance(registry, dict):
        return []

    features = registry.get("features", {})
    artifacts = registry.get("artifacts", {})
    if not isinstance(features, dict):
        return []

    rows: List[Dict[str, str]] = []
    for feature_name, feature in features.items():
        if not isinstance(feature, dict):
            continue
        artifact_id = str(feature.get("artifact_id") or "-")
        artifact = artifacts.get(artifact_id, {}) if isinstance(artifacts, dict) else {}
        artifact = artifact if isinstance(artifact, dict) else {}
        artifact_path = _resolve_watch_feature_path(
            str(feature_name), feature, artifact
        )
        artifact_exists = bool(artifact_path and artifact_path.exists())
        rows.append(
            {
                "name": str(feature_name),
                "artifact_id": artifact_id,
                "status": (
                    "COMPUTING"
                    if str(feature_name) in computing_features
                    else ("GENERATED" if artifact_exists else "MISSING")
                ),
                "total_dim": str(
                    artifact.get("total_dim")
                    or feature.get("total_dim")
                    or feature.get("dims")
                    or "-"
                ),
                "depends_on": _derive_watch_feature_dependency_signal(
                    feature, artifact
                ),
            }
        )

    status_rank = {"COMPUTING": 0, "MISSING": 1, "GENERATED": 2}
    rows.sort(
        key=lambda row: (status_rank.get(str(row.get("status") or ""), 9), row["name"])
    )
    return rows


def _collect_watch_snapshot_rows() -> List[Dict[str, str]]:
    data = load_json(EXPERIMENTS_FILE)
    deduped_rows: Dict[str, Dict[str, str]] = {}

    all_rows: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        for bucket_name in ("experiments", "completed", "archived"):
            bucket_items = data.get(bucket_name, [])
            if isinstance(bucket_items, list):
                all_rows.extend(item for item in bucket_items if isinstance(item, dict))
    elif isinstance(data, list):
        all_rows.extend(item for item in data if isinstance(item, dict))

    status_lookup = {
        str(item.get("name") or "").strip(): str(item.get("status") or "")
        for item in all_rows
        if str(item.get("name") or "").strip()
    }

    def _add_rows(items: Any, bucket: str) -> None:
        if not isinstance(items, list):
            return
        for exp in items:
            if not isinstance(exp, dict):
                continue
            name = str(exp.get("name") or "<unknown>")
            status = str(exp.get("status") or "NEEDS_RERUN").upper()
            condition_parent = str(exp.get("condition_parent") or "").strip() or None
            _result_path, result_payload = _read_result_payload(name)
            has_terminal_metrics = isinstance(result_payload, dict) and any(
                result_payload.get(key) is not None
                for key in ("test_f1", "test_auc", "f1_score", "auc_score")
            )
            progression_status = str(exp.get("progression_status") or "").upper()
            if not progression_status:
                parent_status = (
                    status_lookup.get(condition_parent) if condition_parent else None
                )
                progression_status, _derived_reason = derive_progression_status(
                    status,
                    condition_parent=condition_parent,
                    condition_parent_status=parent_status,
                    warmup_hint=False,
                )
            if status != "COMPLETED" and has_terminal_metrics:
                progression_status = "COMPLETED"
                bucket = "completed"
            status = progression_status or status
            parent = str(exp.get("parent_experiment") or "-")
            batch = str(exp.get("batch_id") or "-")
            memory_contract = _resolve_watch_memory_contract(exp)
            row = {
                "name": name,
                "status": status,
                "parent": parent,
                "batch": batch,
                "bucket": bucket,
                "mem_family": {
                    "fullbatch_sparse_gnn": "fullbatch",
                    "neighborloader_gnn": "neighbor",
                    "temporal_edge_batch": "temporal",
                    "no_batch_path_child": "no-batch",
                }.get(
                    str(memory_contract.get("memory_family") or "-"),
                    str(memory_contract.get("memory_family") or "-"),
                ),
                "est_mb": str(memory_contract.get("est_mem_decision_mb") or "-"),
                "mem_mode": {
                    "fullbatch": "fullbatch",
                    "neighborloader": "neighbor",
                    "temporal_batch": "temporal",
                    "fullgraph_no_batch_path": "no-batch",
                }.get(
                    str(
                        memory_contract.get("execution_mode")
                        or memory_contract.get("memory_mode")
                        or "-"
                    ),
                    str(
                        memory_contract.get("execution_mode")
                        or memory_contract.get("memory_mode")
                        or "-"
                    ),
                ),
                "nbldr": (
                    "recommended"
                    if memory_contract.get("neighborloader_recommended")
                    else (
                        "yes"
                        if memory_contract.get("neighborloader_applicable")
                        else "no"
                    )
                ),
            }
            previous = deduped_rows.get(name)
            if previous is None or _row_priority(row) > _row_priority(previous):
                deduped_rows[name] = row

    if isinstance(data, dict):
        _add_rows(data.get("experiments", []), "active")
        _add_rows(data.get("completed", []), "completed")
    elif isinstance(data, list):
        _add_rows(data, "active")

    rows = list(deduped_rows.values())
    status_rank = {
        "RUNNING": 0,
        "WARM": 0,
        "READY": 1,
        "BLOCKED_CONDITION": 2,
        "NEEDS_RERUN": 3,
        "COMPLETED": 4,
    }
    rows.sort(key=lambda r: (status_rank.get(r["status"], 9), r["name"]))
    return rows


def _resolve_watch_selection(rows: List[Dict[str, str]], selected_name: str) -> str:
    names = [str(row.get("name") or "") for row in rows if str(row.get("name") or "")]
    if not names:
        return ""
    if selected_name in names:
        return selected_name
    return names[0]


def _move_watch_selection(
    rows: List[Dict[str, str]], selected_name: str, delta: int
) -> str:
    names = [str(row.get("name") or "") for row in rows if str(row.get("name") or "")]
    if not names:
        return ""
    selected_name = _resolve_watch_selection(rows, selected_name)
    current_idx = names.index(selected_name)
    next_idx = max(0, min(len(names) - 1, current_idx + delta))
    return names[next_idx]


def _load_archive_module():
    module_path = PHASE3_ROOT.parent / "archive_script.py"
    spec = importlib_util.spec_from_file_location("phase3_archive_script", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load archive_script from {module_path}")
    module = importlib_util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _archive_selected_from_watch(selected_name: str) -> str:
    rows = _collect_watch_snapshot_rows()
    selected_name = _resolve_watch_selection(rows, selected_name)
    if not selected_name:
        return "No experiment selected"
    selected_row = next((row for row in rows if row.get("name") == selected_name), None)
    if not selected_row:
        return f"Selection vanished: {selected_name}"
    archive_script = _load_archive_module()
    if selected_row.get("bucket") == "completed":
        result = archive_script.archive_completed_experiments([selected_name])
    else:
        result = archive_script.archive_selected_experiments([selected_name])
    if int(result.get("count", 0)) <= 0:
        return f"No archive target for {selected_name}"
    report = Path(str(result.get("batch_report") or ""))
    return (
        f"Archived {selected_name} -> {report.name if report.name else 'latest report'}"
    )


def _archive_all_completed_from_watch() -> str:
    rows = _collect_watch_snapshot_rows()
    completed_names = [
        str(row.get("name") or "") for row in rows if row.get("bucket") == "completed"
    ]
    completed_names = [name for name in completed_names if name]
    if not completed_names:
        return "No completed experiments to archive"
    archive_script = _load_archive_module()
    result = archive_script.archive_completed_experiments(completed_names)
    report = Path(str(result.get("batch_report") or ""))
    return f"Archived {int(result.get('count', 0))} completed -> {report.name if report.name else 'latest report'}"


def _clear_latest_archive_from_watch() -> str:
    archive_script = _load_archive_module()
    result = archive_script.clear_latest_archive_artifacts()
    return f"Cleared latest archive artifacts ({int(result.get('count', 0))} files)"


def _paginate_rows(
    rows: List[Dict[str, str]], page: int, page_size: int
) -> Tuple[List[Dict[str, str]], int, int, int, int]:
    total = len(rows)
    if total <= 0:
        return [], 1, 0, 0, 0

    total_pages = max(1, (total + page_size - 1) // page_size)
    page = page % total_pages
    start = page * page_size
    end = min(start + page_size, total)
    return rows[start:end], total_pages, page, start, end


def normalize_initial_watch_page(page: int, total_pages: int) -> int:
    if total_pages <= 0:
        return 0
    try:
        page_num = int(page)
    except (TypeError, ValueError):
        return 0
    return max(0, min(total_pages - 1, page_num - 1))


def _compute_watch_panel_sizes(
    terminal_width: int, main_height: int, ready_row_count: int
) -> Dict[str, int | bool]:
    stacked = terminal_width < 210
    if not stacked:
        ready_height = min(max(8, ready_row_count + 5), max(8, main_height // 3))
        return {
            "stacked": False,
            "feature_height": main_height,
            "ready_height": ready_height,
            "snapshot_height": max(8, main_height - ready_height),
        }

    ready_height = min(max(7, ready_row_count + 5), max(7, main_height // 4))
    snapshot_height = max(8, main_height // 4)
    feature_height = max(10, main_height - ready_height - snapshot_height)
    return {
        "stacked": True,
        "feature_height": feature_height,
        "ready_height": ready_height,
        "snapshot_height": snapshot_height,
    }


def _format_watch_status_text(status: str) -> Text:
    normalized = str(status or "-").upper()
    styles = {
        "COMPLETED": "bold green",
        "RUNNING": "bold yellow",
        "WARM": "bold yellow",
        "NEEDS_RERUN": "bold cyan",
        "READY": "bold bright_blue",
        "BLOCKED_CONDITION": "bold red",
    }
    return Text(normalized, style=styles.get(normalized, "bold white"))


def _format_watch_feature_status_text(status: str) -> Text:
    normalized = str(status or "-").upper()
    styles = {
        "GENERATED": "bold green",
        "COMPUTING": "bold yellow",
        "MISSING": "bold red",
    }
    return Text(normalized, style=styles.get(normalized, "bold white"))


def _format_watch_stage_text(stage: str) -> Text:
    normalized = str(stage or "-").upper()
    styles = {
        "GENERATING_FEATURES": "bold yellow",
        "REGISTERING_TO_DB": "bold blue",
        "READY_QUEUE": "bold green",
        "HANDOFF_TO_RUNNER": "bold magenta",
        "IDLE": "dim",
    }
    return Text(normalized, style=styles.get(normalized, "bold white"))


def _render_watch_panel(
    page: int,
    page_size: int,
    selected_name: str = "",
    status_msg: str = "",
    view_page: int = 0,
) -> Tuple[str, int, int]:
    ready_data = load_json(READY_FILE)
    ready_flag = 0
    ready_batch = "-"
    ready_queue: List[Dict[str, Any]] = []
    if isinstance(ready_data, dict):
        ready_flag = int(ready_data.get("ready_to_process", 0) or 0)
        ready_batch = str(ready_data.get("batch_id") or "-")
        queue_raw = ready_data.get("experiments", [])
        if isinstance(queue_raw, list):
            ready_queue = [q for q in queue_raw if isinstance(q, dict)]
    feature_jobs_raw = (
        ready_data.get("feature_jobs", []) if isinstance(ready_data, dict) else []
    )
    feature_jobs = (
        [f for f in feature_jobs_raw if isinstance(f, dict)]
        if isinstance(feature_jobs_raw, list)
        else []
    )

    snapshot_rows = _collect_watch_snapshot_rows()
    registered_names = {
        str(row.get("name") or "")
        for row in snapshot_rows
        if str(row.get("name") or "")
    }
    ready_queue = [
        item
        for item in ready_queue
        if str(item.get("name") or "") not in registered_names
    ]
    computing_features = _collect_computing_feature_names(ready_queue, feature_jobs)
    feature_rows = _collect_watch_rows(computing_features)
    page_rows, total_pages, page, start, end = _paginate_rows(
        feature_rows, page, page_size
    )

    selected_name = _resolve_watch_selection(snapshot_rows, selected_name)
    tick_time = "-"
    if status_msg.startswith("tick ok @ "):
        tick_time = status_msg.split("tick ok @ ", 1)[1].strip()

    current_stage = "IDLE"
    if feature_jobs:
        current_stage = "GENERATING_FEATURES"
    elif ready_queue and ready_flag:
        current_stage = "REGISTERING_TO_DB"
    elif ready_queue:
        current_stage = "READY_QUEUE"
    elif any(
        str(r.get("status") or "").upper() == "NEEDS_RERUN" for r in snapshot_rows
    ):
        current_stage = "HANDOFF_TO_RUNNER"

    terminal_size = shutil.get_terminal_size((180, 40))
    terminal_width = terminal_size.columns
    terminal_height = max(24, terminal_size.lines)

    header_height = 5
    footer_height = 4
    main_height = max(12, terminal_height - header_height - footer_height)
    ready_rows_count = len(ready_queue) + len(feature_jobs)
    panel_sizes = _compute_watch_panel_sizes(
        terminal_width=terminal_width,
        main_height=main_height,
        ready_row_count=ready_rows_count,
    )

    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=header_height),
        Layout(name="main", size=main_height),
        Layout(name="footer", size=footer_height),
    )
    header_lines = Group(
        Text.from_markup("[bold bright_white]Phase3 preprocess.py --watch[/]"),
        Text.from_markup(
            f"[cyan]View[/]: {'Operations' if view_page == 0 else 'Feature Bank'}  [white]|[/]  [dim]Tab[/] to switch"
        ),
        Text.from_markup(
            f"[cyan]Mode[/]: WATCH  [white]|[/]  [cyan]Stage[/]: {current_stage}  [white]|[/]  [cyan]Time[/]: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ),
        Text.from_markup(
            f"[green]Ready[/]: {ready_flag}  [white]|[/]  [magenta]Batch[/]: {ready_batch}  [white]|[/]  [yellow]Queue[/]: {len(ready_queue)}  [white]|[/]  [yellow]FeatureJobs[/]: {len(feature_jobs)}  [white]|[/]  [blue]Tick[/]: {tick_time}"
        ),
    )
    layout["header"].update(
        Panel(
            header_lines,
            title="[bold]Operator Console[/]",
            border_style="bright_blue",
            box=box.ROUNDED,
            padding=(0, 1),
        )
    )

    feature_height = int(panel_sizes["feature_height"])
    ready_height = int(panel_sizes["ready_height"])
    snapshot_height = int(panel_sizes["snapshot_height"])
    if view_page == 0:
        ops_ready_height = max(10, main_height // 2)
        ops_snapshot_height = main_height - ops_ready_height
        layout["main"].split_column(
            Layout(name="ready", size=ops_ready_height),
            Layout(name="snapshot", size=ops_snapshot_height),
        )
    else:
        layout["main"].split_column(
            Layout(name="features", size=main_height),
        )

    if view_page == 1:
        feature_summary = Group(
            Text.from_markup(
                f"[cyan]Feature bank[/]: total={len(feature_rows)}  [white]|[/]  [green]generated[/]={generated_count}  [white]|[/]  [red]missing[/]={missing_count}"
            ),
            Text.from_markup(f"[yellow]Current computing[/]: {computing_signal}"),
            feature_table,
        )
        layout["features"].update(
            Panel(
                feature_summary,
                title="[bold]Feature Bank Overview[/]",
                border_style="blue",
                box=box.ROUNDED,
                padding=(0, 1),
                height=feature_height,
            )
        )

    if view_page == 0:
        ready_table = Table(
            box=box.SIMPLE_HEAVY,
            expand=True,
            show_header=True,
            header_style="bold bright_white on dark_green",
            row_styles=["none", "dim"],
            pad_edge=False,
        )
        ready_table.add_column("#", width=4, justify="right")
        ready_table.add_column("Stage", width=18)
        ready_table.add_column("Ready Queue", ratio=1)
        ready_rows = [
            ("READY_QUEUE", item.get("name", "<unknown>")) for item in ready_queue
        ]
        ready_rows += [
            ("GENERATING_FEATURES", item.get("name", "<unknown>"))
            for item in feature_jobs
        ]
        if ready_rows:
            for idx, (stage_label, item_name) in enumerate(ready_rows[:8], 1):
                ready_table.add_row(
                    str(idx),
                    _format_watch_stage_text(stage_label),
                    Text(str(item_name), style="bright_white"),
                )
            remaining = len(ready_rows) - 8
            if remaining > 0:
                ready_table.add_row(
                    "…",
                    _format_watch_stage_text(current_stage),
                    Text(f"and {remaining} more", style="dim"),
                )
        else:
            ready_table.add_row(
                "",
                _format_watch_stage_text(current_stage),
                Text("(empty)", style="dim"),
            )
        layout["ready"].update(
            Panel(
                ready_table,
                title="[bold]Ready Queue[/]",
                border_style="green",
                box=box.ROUNDED,
                padding=(0, 1),
                height=ready_height,
            )
        )

        snapshot_table = Table(
            box=box.SIMPLE_HEAVY,
            expand=True,
            show_header=True,
            header_style="bold bright_white on dark_cyan",
            row_styles=["none", "dim"],
            pad_edge=False,
        )
        snapshot_table.add_column("", width=2)
        snapshot_table.add_column("#", width=4, justify="right")
        snapshot_table.add_column("Status", width=12)
        snapshot_table.add_column("Experiment", min_width=18, ratio=2)
        snapshot_table.add_column("Parent", width=12)
        snapshot_table.add_column("Batch", width=10)
        snapshot_table.add_column("MemFam", width=10)
        snapshot_table.add_column("EstMB", width=7, justify="right")
        snapshot_table.add_column("Mode", width=10)
        snapshot_table.add_column("NBLdr", width=7)
        snapshot_table.add_column("Stage", width=18)
        if snapshot_rows:
            for idx, row in enumerate(snapshot_rows[:8], 1):
                prefix = ">" if row.get("name") == selected_name else " "
                stage_label = (
                    "HANDOFF_TO_RUNNER"
                    if str(row.get("status") or "").upper() in {"NEEDS_RERUN", "READY"}
                    else "IDLE"
                )
                snapshot_table.add_row(
                    prefix,
                    str(idx),
                    _format_watch_status_text(str(row["status"])),
                    Text(str(row["name"]), style="bright_white"),
                    str(row["parent"]),
                    str(row["batch"]),
                    str(row.get("mem_family") or "-"),
                    str(row.get("est_mb") or "-"),
                    str(row.get("mem_mode") or "-"),
                    str(row.get("nbldr") or "-"),
                    _format_watch_stage_text(stage_label),
                    style="reverse" if row.get("name") == selected_name else None,
                )
        else:
            snapshot_table.add_row(
                "", "", "", "(no experiments)", "", "", "", "", "", "", current_stage
            )
        if len(snapshot_rows) > 8:
            snapshot_table.add_row(
                "",
                "",
                "",
                f"showing 8 of {len(snapshot_rows)} experiment rows",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            )
        layout["snapshot"].update(
            Panel(
                snapshot_table,
                title="[bold]Experiments Snapshot[/]",
                border_style="cyan",
                box=box.ROUNDED,
                padding=(0, 1),
                height=snapshot_height,
            )
        )

    if view_page == 0:
        controls_line = Text.from_markup(
            "[dim]W/S[/]:Exp Select  [dim]Tab[/]:Feature Bank  [dim][a]All [S]Selected -> [v]Archive [c]Clear [Esc]Cancel[/]  [dim]Q[/]:Quit"
        )
    else:
        controls_line = Text.from_markup(
            "[dim]N/P[/]:Feature Page  [dim]Tab[/]:Operations  [dim]Q[/]:Quit"
        )
    status_line = Text.from_markup(
        f"[yellow]{status_msg}[/]" if status_msg else "[dim]Status: idle[/]"
    )
    layout["footer"].update(
        Panel(
            Group(controls_line, status_line),
            title="[bold]Controls[/]",
            border_style="magenta",
            box=box.ROUNDED,
            padding=(0, 1),
        )
    )

    console = Console(record=True, width=terminal_width, height=terminal_height)
    console.print(layout)
    exported = console.export_text().rstrip("\n")
    lines = exported.splitlines()
    if len(lines) < terminal_height:
        lines.extend([""] * (terminal_height - len(lines)))
    elif len(lines) > terminal_height:
        lines = lines[:terminal_height]
    return "\n".join(lines), total_pages, page


def _run_watch_tick(status_msg: str) -> str:
    try:
        run_once()
        return f"tick ok @ {datetime.now().strftime('%H:%M:%S')}"
    except Exception as exc:
        return f"watch tick error: {exc}"


class _RawInputMode:
    def __init__(self):
        self._fd = None
        self._old = None

    def __enter__(self):
        if not sys.stdin.isatty():
            return self
        self._fd = sys.stdin.fileno()
        self._old = termios.tcgetattr(self._fd)
        tty.setcbreak(self._fd)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._fd is not None and self._old is not None:
            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old)


def _read_key_nonblocking(timeout_sec: float) -> str:
    if not sys.stdin.isatty():
        time.sleep(timeout_sec)
        return ""
    ready, _, _ = select.select([sys.stdin], [], [], timeout_sec)
    if not ready:
        return ""
    return sys.stdin.read(1)


def run_watch(interval: int, page_size: int, initial_page: int = 1) -> None:
    page = normalize_initial_watch_page(initial_page, 1)
    selected_name = ""
    status_msg = ""
    view_page = 0
    next_tick = 0.0
    first_render = True
    action_keys = TwoStepKeyHandler()
    if not sys.stdin.isatty():
        status_msg = _run_watch_tick(status_msg)
        panel, _, _ = _render_watch_panel(
            page, page_size, selected_name, status_msg, view_page=view_page
        )
        print(panel)
        return
    with _RawInputMode():
        while True:
            now = time.time()
            if now >= next_tick:
                status_msg = _run_watch_tick(status_msg)
                next_tick = now + max(float(interval), 0.1)
            snapshot_rows = _collect_watch_snapshot_rows()
            selected_name = _resolve_watch_selection(snapshot_rows, selected_name)
            if first_render:
                total_pages_preview = max(
                    1, (len(_collect_watch_rows()) + page_size - 1) // page_size
                )
                page = normalize_initial_watch_page(initial_page, total_pages_preview)
                first_render = False
            panel, total_pages, page = _render_watch_panel(
                page, page_size, selected_name, status_msg, view_page=view_page
            )
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.write(panel + "\n")
            sys.stdout.flush()

            wait_time = max(0.1, min(1.0, next_tick - time.time()))
            key = _read_key_nonblocking(wait_time)
            key_lower = key.lower()
            if key_lower == "q":
                break
            if key == "\t":
                view_page = (view_page + 1) % 2
                continue
            if key_lower == "n" and view_page == 1:
                page = (page + 1) % max(total_pages, 1)
            elif key_lower == "p" and view_page == 1:
                page = (page - 1) % max(total_pages, 1)
            elif key_lower == "w" and view_page == 0:
                selected_name = _move_watch_selection(snapshot_rows, selected_name, -1)
            elif key == "s" and view_page == 0:
                selected_name = _move_watch_selection(snapshot_rows, selected_name, 1)

            action_key = ""
            if key == "S":
                action_key = "s"
            elif key == "\x1b":
                action_key = key
            elif key_lower in {"a", "k", "r", "d", "v", "f", "c"}:
                action_key = key_lower

            if not action_key:
                continue

            action = action_keys.handle_key(action_key)
            if action is None:
                continue
            if action.scope == "selected" and action.action == "archive":
                status_msg = _archive_selected_from_watch(selected_name)
            elif action.scope == "all" and action.action == "archive":
                status_msg = _archive_all_completed_from_watch()
            elif action.scope == "selected" and action.action in {"delete", "clear"}:
                status_msg = _clear_latest_archive_from_watch()
            else:
                status_msg = (
                    f"No watch backend action for {action.scope}/{action.action}"
                )


def main():
    parser = argparse.ArgumentParser(description="Phase 3 Preprocessing Orchestrator")
    parser.add_argument(
        "--loop", action="store_true", help="Run in continuous loop mode"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch operator console with looped run_once ticks and rich dashboard",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_LOOP_INTERVAL_SEC,
        help="Polling interval in seconds",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_WATCH_PAGE_SIZE,
        help="Rows per page in --watch mode",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=DEFAULT_WATCH_PAGE,
        help="Initial page number in --watch mode (1-based)",
    )
    add_common_args(parser)
    args = parser.parse_args()
    setup_logging(args)

    if args.dry_run:
        emit_result(
            args,
            {
                "dry_run": True,
                "mode": "watch" if args.watch else ("loop" if args.loop else "once"),
                "interval": args.interval,
                "page_size": args.page_size,
                "page": args.page,
                "ready_file": str(READY_FILE),
                "experiments_file": str(EXPERIMENTS_FILE),
            },
        )
        return

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log_handle = open(LOG_FILE, "a", encoding="utf-8")
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = _TeeStream(original_stdout, log_handle)
    sys.stderr = _TeeStream(original_stderr, log_handle)

    try:
        print("=" * 60)
        print("Phase 3 Preprocessing Orchestrator")
        mode = "WATCH" if args.watch else ("LOOP" if args.loop else "ONCE")
        print(f"Mode: {mode}")
        print(f"Python: {sys.executable}")
        if "envs/gnn_fraud/" not in str(Path(sys.executable).resolve()):
            print(
                "[WARN] Not running inside gnn_fraud env. Some feature-generation steps may fail."
            )
        print("=" * 60)

        if args.watch:
            run_watch(args.interval, max(1, args.page_size), args.page)
        elif args.loop:
            print(f"Polling {READY_FILE} every {args.interval}s...")
            while True:
                try:
                    run_once()
                except Exception as e:
                    print(f"\n[LOOP ERROR] {e}")
                    import traceback

                    traceback.print_exc()

                time.sleep(args.interval)
        else:
            run_once()
            print("\nDone!")
            print("=" * 60)
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_handle.close()


if __name__ == "__main__":
    main()
