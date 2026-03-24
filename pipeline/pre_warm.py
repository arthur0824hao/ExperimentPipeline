#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import multiprocessing as mp
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingImports]
from sklearn.preprocessing import RobustScaler

from runtime_config import get_pre_warm_config


_TRAINER_SCRIPTS = [
    "train_phase1_graphsage_targeted.py",
    "train_phase1_zebra_targeted.py",
]
_SHARED_RUNTIME: Dict[str, Any] = {}


def _discover_fraud_root() -> Path:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[3]]
    for candidate in candidates:
        marker = candidate / "Experiment" / "preprocess_lib" / "train_utils.py"
        if marker.exists():
            return candidate
    raise FileNotFoundError(
        "Cannot find FraudDetect root containing Experiment/preprocess_lib/train_utils.py"
    )


def _ensure_shared_runtime(fraud_root: Path) -> Dict[str, Any]:
    key = str(fraud_root.resolve())
    existing = _SHARED_RUNTIME.get(key)
    if isinstance(existing, dict):
        return existing

    experiment_dir = fraud_root / "Experiment"
    if str(experiment_dir) not in sys.path:
        sys.path.insert(0, str(experiment_dir))

    train_utils = importlib.import_module("preprocess_lib.train_utils")
    preprocessing_path = fraud_root / "Copied_resources" / "senior_exp" / "preprocessing.py"
    spec = importlib.util.spec_from_file_location("phase1_senior_preprocessing", preprocessing_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load preprocessing module: {preprocessing_path}")
    preprocessing_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(preprocessing_module)

    payload = {
        "train_utils": train_utils,
        "preprocessing": preprocessing_module,
    }
    _SHARED_RUNTIME[key] = payload
    return payload


def _to_numpy(tensor_like: Any) -> np.ndarray:
    if isinstance(tensor_like, np.ndarray):
        return tensor_like.astype(np.float32, copy=False)
    if hasattr(tensor_like, "detach"):
        return tensor_like.detach().cpu().numpy().astype(np.float32, copy=False)
    return np.asarray(tensor_like, dtype=np.float32)


def _feature_dim_guess(feature_name: str) -> int:
    patterns = [r"(\d+)dim", r"basic(\d+)", r"regen(\d+)", r"burst_(\d+)", r"velocity_(\d+)"]
    for pattern in patterns:
        match = re.search(pattern, feature_name)
        if match:
            return int(match.group(1))
    return 0


def _feature_signature(
    train_utils: Any,
    trainer_script: Path,
    contract: Dict[str, Any],
    feature_names: Iterable[str],
) -> str:
    feature_bank_path = Path(contract["feature_bank_path"]).resolve()
    node_mapping_path = Path(contract["node_mapping_path"]).resolve()
    return "|".join(
        [
            "phase1-feature-bank-v3",
            f"trainer_hash={train_utils.script_content_hash(str(trainer_script))}",
            "feature_bank_fallback=phase1_node_features_on_miss_pad_or_truncate",
            "source=feature_bank",
            f"split_policy={contract.get('split_policy', '')}",
            f"split_strategy={contract.get('split_strategy', '')}",
            f"leakback_ratio={contract.get('leakback_ratio', '')}",
            f"min_test_positive_ratio={contract.get('min_test_positive_ratio', '')}",
            str(feature_bank_path),
            str(node_mapping_path),
            ",".join(feature_names),
        ]
    )


def _load_feature_blocks(
    train_utils: Any,
    contract: Dict[str, Any],
    feature_names: List[str],
) -> Tuple[Dict[str, int], List[np.ndarray]]:
    import torch

    feature_bank_dir = Path(contract["feature_bank_path"])
    registry = json.loads((feature_bank_dir / "registry.json").read_text(encoding="utf-8"))
    node_mapping = torch.load(contract["node_mapping_path"], weights_only=False)
    acct_to_node = node_mapping.get("acct_to_node")
    if not isinstance(acct_to_node, dict):
        raise ValueError("acct_to_node missing from phase1 node mapping")

    blocks: List[np.ndarray] = []
    for feature_name in feature_names:
        feature_meta = registry["features"][feature_name]
        artifact_meta = registry["artifacts"][feature_meta["artifact_id"]]
        tensor = torch.load(feature_bank_dir / artifact_meta["path"], weights_only=False)
        arr = _to_numpy(tensor)
        start = int(feature_meta["start_idx"])
        end = int(feature_meta["end_idx"])
        blocks.append(arr[:, start:end])
    return {str(k): int(v) for k, v in acct_to_node.items()}, blocks


def _build_feature_matrix(
    accounts: List[str],
    acct_to_phase3: Dict[str, int],
    blocks: List[np.ndarray],
    native_node_features: Any,
) -> np.ndarray:
    total_dim = sum(block.shape[1] for block in blocks)
    feature_rows = blocks[0].shape[0] if blocks else 0
    native = (
        _to_numpy(native_node_features) if native_node_features is not None else np.zeros((0, 0), dtype=np.float32)
    )
    fallback_rows = native if native.ndim == 2 and native.shape[0] == len(accounts) else None
    fallback_copy_dim = min(int(fallback_rows.shape[1]), total_dim) if fallback_rows is not None else 0

    rows = np.zeros((len(accounts), total_dim), dtype=np.float32)
    for idx, acct in enumerate(accounts):
        node_idx = acct_to_phase3.get(acct)
        if node_idx is None or node_idx < 0 or node_idx >= feature_rows:
            if fallback_rows is not None and fallback_copy_dim > 0:
                rows[idx, :fallback_copy_dim] = fallback_rows[idx, :fallback_copy_dim]
            continue
        rows[idx] = np.concatenate([block[node_idx] for block in blocks], axis=0)
    return rows


def _build_masks(
    train_utils: Any,
    preprocessing_module: Any,
    contract: Dict[str, Any],
    graph_dict: Dict[str, Any],
) -> Tuple[Any, Any, Any]:
    split_strategy = str(contract.get("split_strategy", "")).strip().lower()
    if split_strategy == "exact_senior":
        return preprocessing_module.create_split_masks(
            graph_dict["accounts"],
            graph_dict["esun_accounts"],
            graph_dict["test_accounts"],
            graph_dict["labels"],
            graph_dict["num_nodes"],
            val_ratio=float(contract["validation_split_ratio"]),
            random_state=int(contract["split_random_state"]),
        )
    if split_strategy == "senior_aligned":
        return train_utils.create_senior_aligned_split(
            graph_dict["accounts"],
            graph_dict["esun_accounts"],
            graph_dict["test_accounts"],
            graph_dict["labels"],
            graph_dict["num_nodes"],
            val_ratio=float(contract["validation_split_ratio"]),
            random_state=int(contract["split_random_state"]),
        )
    return train_utils.create_phase1_leakback_split(
        graph_dict["accounts"],
        graph_dict["esun_accounts"],
        graph_dict["test_accounts"],
        graph_dict["labels"],
        graph_dict["num_nodes"],
        leakback_ratio=float(contract["leakback_ratio"]),
        val_ratio=float(contract["validation_split_ratio"]),
        random_state=int(contract["split_random_state"]),
        min_test_positive_ratio=float(contract["min_test_positive_ratio"]),
    )


def _build_one_cache(task: Dict[str, Any]) -> Dict[str, Any]:
    start_ts = time.time()
    fraud_root = Path(task["fraud_root"])
    group_name = str(task["group_name"])
    feature_names = [str(v) for v in task["feature_names"]]
    force_rebuild = bool(task.get("force_rebuild", False))

    shared = _ensure_shared_runtime(fraud_root)
    train_utils = shared["train_utils"]
    preprocessing_module = shared["preprocessing"]

    contract = train_utils.get_phase1_data_contract()
    trainer_scripts = [
        fraud_root / "Experiment" / "scripts" / script_name for script_name in _TRAINER_SCRIPTS
    ]
    signatures = [
        _feature_signature(train_utils, script, contract, feature_names)
        for script in trainer_scripts
        if script.exists()
    ]

    if not signatures:
        raise FileNotFoundError("No Phase1 trainer scripts found for signature generation")

    missing_signatures = []
    if force_rebuild:
        missing_signatures = list(signatures)
    else:
        for signature in signatures:
            cached = train_utils.load_phase1_warm_cache(contract, feature_signature=signature)
            if cached is None:
                missing_signatures.append(signature)

    if not missing_signatures:
        return {
            "group": group_name,
            "status": "hit",
            "duration_sec": round(time.time() - start_ts, 2),
            "feature_dim": sum(_feature_dim_guess(name) for name in feature_names),
            "cache_count": 0,
        }

    transactions = pd.read_csv(contract["transactions_path"])
    transactions = preprocessing_module.convert_to_twd(transactions)
    alerts = pd.read_csv(contract["future_alert_path"])
    predict_df = pd.read_csv(contract["predict_path"])
    graph_dict = preprocessing_module.construct_graph(transactions, alerts, predict_df)

    train_mask, val_mask, test_mask = _build_masks(
        train_utils,
        preprocessing_module,
        contract,
        graph_dict,
    )

    accounts = [str(acct) for acct in graph_dict["accounts"]]
    acct_to_node = {str(acct): idx for idx, acct in enumerate(graph_dict["accounts"])}
    acct_to_phase3, feature_blocks = _load_feature_blocks(train_utils, contract, feature_names)
    features = _build_feature_matrix(
        accounts,
        acct_to_phase3,
        feature_blocks,
        graph_dict.get("node_features"),
    )
    if features.shape[0] != len(accounts):
        raise ValueError(
            f"Feature row count {features.shape[0]} does not match account count {len(accounts)}"
        )

    train_idx = train_mask.detach().cpu().numpy().astype(bool)
    if int(train_idx.sum()) == 0:
        raise ValueError("train_mask selects zero rows; cannot fit RobustScaler")
    scaler = RobustScaler()
    scaler.fit(features[train_idx])
    scaled = np.asarray(scaler.transform(features), dtype=np.float32)
    scaled = np.clip(scaled, -5, 5)

    data = preprocessing_module.build_pyg_data(
        scaled,
        graph_dict["edge_index"],
        graph_dict["labels"],
        train_mask,
        val_mask,
        test_mask,
    )
    scaler_state = {
        "center_": np.asarray(scaler.center_, dtype=np.float32).tolist(),
        "scale_": np.asarray(scaler.scale_, dtype=np.float32).tolist(),
    }
    for signature in missing_signatures:
        train_utils.save_phase1_warm_cache(
            data,
            accounts,
            acct_to_node,
            contract,
            feature_signature=signature,
            scaler_state=scaler_state,
        )

    return {
        "group": group_name,
        "status": "built",
        "duration_sec": round(time.time() - start_ts, 2),
        "feature_dim": int(scaled.shape[1]),
        "cache_count": len(missing_signatures),
    }


def run_pre_warm(
    *,
    enabled: bool,
    parallel_workers: int,
    phase1_feature_sets: Dict[str, List[str]],
    force_rebuild: bool = False,
) -> Dict[str, Any]:
    if not enabled:
        print("[PreWarm] Disabled by runtime config.", flush=True)
        return {"enabled": False, "built": 0, "hit": 0}

    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    fraud_root = _discover_fraud_root()

    tasks: List[Dict[str, Any]] = []
    for group_name, features in phase1_feature_sets.items():
        feature_list = [str(name).strip() for name in features if str(name).strip()]
        if not feature_list:
            continue
        dims = sum(_feature_dim_guess(name) for name in feature_list)
        print(f"[PreWarm] Building {group_name.upper()} ({dims}-dim)...", flush=True)
        tasks.append(
            {
                "fraud_root": str(fraud_root),
                "group_name": group_name.upper(),
                "feature_names": feature_list,
                "force_rebuild": force_rebuild,
            }
        )

    if not tasks:
        print("[PreWarm] No feature sets configured.", flush=True)
        return {"enabled": True, "built": 0, "hit": 0}

    built = 0
    hit = 0
    workers = max(1, min(int(parallel_workers), len(tasks)))
    if workers == 1:
        results = [_build_one_cache(task) for task in tasks]
    else:
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            results = list(pool.imap_unordered(_build_one_cache, tasks))

    for result in results:
        group = str(result["group"])
        duration = float(result["duration_sec"])
        status = str(result["status"])
        if status == "built":
            built += 1
            print(
                f"[PreWarm] Building {group} ({result['feature_dim']}-dim)... done ({duration:.0f}s)",
                flush=True,
            )
        else:
            hit += 1
            print(
                f"[PreWarm] Building {group} ({result['feature_dim']}-dim)... cache hit ({duration:.0f}s)",
                flush=True,
            )

    summary = {
        "enabled": True,
        "workers": workers,
        "built": built,
        "hit": hit,
        "total": len(results),
    }
    print(
        f"[PreWarm] Completed: built={built}, hit={hit}, total={len(results)}, workers={workers}",
        flush=True,
    )
    return summary


def run_pre_warm_from_runtime(force_rebuild: bool = False) -> Dict[str, Any]:
    cfg = get_pre_warm_config()
    return run_pre_warm(
        enabled=bool(cfg.get("enabled", False)),
        parallel_workers=int(cfg.get("parallel_workers", 1)),
        phase1_feature_sets=dict(cfg.get("phase1_feature_sets", {})),
        force_rebuild=force_rebuild,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Phase1 warm caches in parallel")
    parser.add_argument("--force", action="store_true", help="Rebuild even when cache exists")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override pre_warm.parallel_workers",
    )
    args = parser.parse_args()

    cfg = get_pre_warm_config()
    workers = int(args.workers) if args.workers is not None else int(cfg["parallel_workers"])
    run_pre_warm(
        enabled=bool(cfg["enabled"]),
        parallel_workers=workers,
        phase1_feature_sets=dict(cfg["phase1_feature_sets"]),
        force_rebuild=bool(args.force),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
