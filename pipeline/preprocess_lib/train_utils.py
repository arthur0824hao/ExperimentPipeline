#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Training Utilities - Shared components for all experiments.

Provides:
- Model architectures (GraphSAGE, GIN, GCN)
- Training loop helpers
- Evaluation utilities
- Loss functions (Focal, Asymmetric, Contrastive)
"""

import os
import json
import threading
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional, Callable

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingImports]
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import Adam
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from torch_geometric.nn import (
    SAGEConv,
    GINConv,
    GCNConv,
    GATv2Conv,
    RGCNConv,
    GINEConv,
    SignedConv,
    TransformerConv,
    APPNP,
    BatchNorm,
    MessagePassing,
)
from torch_geometric.utils import degree
from torch_geometric.loader import NeighborLoader
from torch_geometric.utils import softmax

from preprocess_lib.data_loader import create_neighbor_loaders, DEFAULT_NUM_WORKERS
from runtime_config import (
    cfg_float,
    cfg_int,
    cfg_str,
    get_runtime_section,
    resolve_project_path,
)


import atexit


class PeakMemoryTracker:
    """
    Context manager to track peak GPU memory usage and save to file on exit.

    CRITICAL: This class ensures resource_usage.json is ALWAYS written, even on crash.
    It also creates .error sidecar files for Runner detection.

    Usage:
        with PeakMemoryTracker(output_dir, experiment_name="3A1_Baseline"):
            train(...)
    """

    def __init__(self, output_dir: str, experiment_name: str = "unknown"):
        self.output_dir = output_dir
        self.experiment_name = experiment_name
        self.peak_memory = 0.0
        self._is_oom = False
        self._previous_peak_memory = 0.0

    def __enter__(self):
        self._previous_peak_memory = self._read_existing_peak_memory()
        if torch.cuda.is_available():
            torch.cuda.reset_peak_memory_stats()
            torch.cuda.empty_cache()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Always capture peak memory first
        if torch.cuda.is_available():
            try:
                self.peak_memory = torch.cuda.max_memory_allocated() / (
                    1024 * 1024
                )  # MB
            except Exception:
                self.peak_memory = 0.0
        self.peak_memory = max(self.peak_memory, self._previous_peak_memory)

        # Detect OOM explicitly
        error_type = None
        error_msg = None
        if exc_type is not None:
            error_msg = str(exc_val)
            if "CUDA out of memory" in error_msg or "OutOfMemoryError" in str(exc_type):
                self._is_oom = True
                error_type = "OOM"
            else:
                error_type = exc_type.__name__ if exc_type else "Unknown"

        # Build status dict
        status = "SUCCESS"
        if exc_type is not None:
            status = "OOM" if self._is_oom else "FAILED"

        stats = {
            "experiment": self.experiment_name,
            "peak_memory_mb": float(self.peak_memory),
            "timestamp": datetime.now().isoformat(),
            "status": status,
            "error_type": error_type,
            "error_message": error_msg,
            "is_oom": self._is_oom,
        }

        # GUARANTEED write to resource_usage.json
        self._safe_write_json(stats)

        # Print summary
        status_icon = (
            "✔" if status == "SUCCESS" else ("💾 OOM" if self._is_oom else "✖")
        )
        print(
            f"\n\n[Tracker] {status_icon} Peak GPU Memory: {self.peak_memory:.2f} MB",
            flush=True,
        )

        # Don't suppress the exception
        return False

    def _safe_write_json(self, stats: dict):
        """Guaranteed write with multiple fallback attempts."""
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except Exception:
            pass  # If we can't create dir, try writing anyway

        resource_file = os.path.join(self.output_dir, "resource_usage.json")
        try:
            if os.path.exists(resource_file):
                with open(resource_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                existing_peak = existing.get("peak_memory_mb")
                if isinstance(existing_peak, (int, float)):
                    stats["peak_memory_mb"] = max(
                        float(stats.get("peak_memory_mb", 0.0)), float(existing_peak)
                    )
        except Exception:
            pass

        # Attempt 1: Normal write
        try:
            with open(resource_file, "w") as f:
                json.dump(stats, f, indent=2)
            return
        except Exception as e:
            print(
                f"\n[Tracker] Warning: Failed to write {resource_file}: {e}", flush=True
            )

        # Attempt 2: Write to /tmp as fallback
        try:
            fallback = f"/tmp/{self.experiment_name}_resource_usage.json"
            with open(fallback, "w") as f:
                json.dump(stats, f, indent=2)
            print(f"\n[Tracker] Wrote fallback to {fallback}", flush=True)
        except Exception:
            pass  # Last resort failed, nothing we can do

    def _read_existing_peak_memory(self) -> float:
        resource_file = os.path.join(self.output_dir, "resource_usage.json")
        if not os.path.exists(resource_file):
            return 0.0
        try:
            with open(resource_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            peak = payload.get("peak_memory_mb")
            if isinstance(peak, (int, float)):
                return float(peak)
        except Exception:
            return 0.0
        return 0.0


class ProgressReporter:
    """
    Reports training progress to a .progress file for Dashboard integration.

    Usage:
        reporter = ProgressReporter(experiment_dir, experiment_name)
        for epoch in range(max_epochs):
            ...
            reporter.update(epoch + 1, max_epochs, val_f1, loss)
        reporter.finish()

    File Format (.progress):
        {
            "experiment": "3A1_Baseline",
            "epoch": 120,
            "total_epochs": 300,
            "val_f1": 0.5234,
            "loss": 0.0823,
            "timestamp": "2026-01-28T13:00:00"
        }
    """

    def __init__(self, experiment_dir: str, experiment_name: str):
        self.experiment_dir = experiment_dir
        self.history_file = os.path.join(experiment_dir, "metric_history.jsonl")
        self.experiment_name = experiment_name
        self.progress_file = os.path.join(experiment_dir, ".progress")
        self._last_update = -1
        self._update_interval = 1  # Update every N epochs (avoid I/O spam)

    def update(
        self,
        epoch: int,
        total_epochs: int,
        val_f1: float = 0.0,
        loss: float = 0.0,
        phase: Optional[str] = None,
        **metrics,
    ):
        """Update progress file. Called once per epoch."""
        if epoch - self._last_update < self._update_interval and epoch != total_epochs:
            return
        self._last_update = epoch

        progress = {
            "experiment": self.experiment_name,
            "epoch": epoch,
            "total_epochs": total_epochs,
            "val_f1": round(val_f1, 4),
            "loss": round(loss, 4),
            "percent": round(100 * epoch / max(total_epochs, 1), 1),
            "timestamp": datetime.now().isoformat(),
        }
        if phase is not None:
            progress["phase"] = phase

        try:
            with open(self.progress_file, "w") as f:
                json.dump(progress, f)
        except Exception:
            pass  # Non-critical, don't crash training

        history_entry = {
            "epoch": epoch,
            "total_epochs": total_epochs,
            "val_f1": round(val_f1, 4),
            "loss": round(loss, 4),
            "timestamp": datetime.now().isoformat(),
        }
        if phase is not None:
            history_entry["phase"] = phase
        for k, v in metrics.items():
            if isinstance(v, float):
                history_entry[k] = round(v, 4)
            elif isinstance(v, (int, str, bool)):
                history_entry[k] = v
        try:
            with open(self.history_file, "a") as f:
                f.write(json.dumps(history_entry) + "\n")
        except Exception:
            pass

    def set_phase(self, phase: str, total_epochs: int = 0):
        self._last_update = -1
        self.update(0, total_epochs, phase=phase)

    def finish(self):
        try:
            if os.path.exists(self.progress_file):
                os.remove(self.progress_file)
        except Exception:
            pass

    @staticmethod
    def read_history(experiment_dir: str) -> list:
        history_file = os.path.join(experiment_dir, "metric_history.jsonl")
        entries = []
        if not os.path.exists(history_file):
            return entries
        try:
            with open(history_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        entries.append(json.loads(line))
        except Exception:
            pass
        return entries


CANONICAL_TEST_DATASET = "acct_predi.csv"
CANONICAL_TEST_CONTRACT = "acct_predi_full_universe_gt"
PHASE1_EVAL_DATASET = "acct_alert.csv"
PHASE1_EVAL_CONTRACT = "acct_predict_phase2_alert"
PHASE1_DEFAULT_TXN_PATH = "esun_data/Phase1/data_acct/acct_transaction.csv"
PHASE1_DEFAULT_PREDICT_PATH = "esun_data/Phase1/data_acct/acct_predict.csv"
PHASE1_DEFAULT_ALERT_PATH = "esun_data/Phase2/acct_alert.csv"
PHASE1_DEFAULT_FEATURE_BANK_PATH = "Phase3/data/feature_bank"
PHASE1_DEFAULT_NODE_MAPPING_PATH = "Phase3/data/processed/node_mapping.pt"
PHASE3_DEFAULT_STRUCTURE_PATH = "Phase3/data/processed/structure_edge_temporal.pt"
PHASE3_DEFAULT_FEATURE_BANK_PATH = "Phase3/data/feature_bank"
PHASE3_DEFAULT_PREDI_PATH = "esun_data/Phase3/acct_predi.csv"
PHASE3_DEFAULT_GT_PATH = "esun_data/Phase3/acct_predi.csv"


def get_phase1_data_contract() -> Dict[str, Any]:
    section = get_runtime_section("phase1_data_contract")
    predict_path = cfg_str(section, "predict_path", PHASE1_DEFAULT_PREDICT_PATH)
    future_alert_path = cfg_str(section, "future_alert_path", PHASE1_DEFAULT_ALERT_PATH)
    return {
        "transactions_path": str(
            resolve_project_path(
                cfg_str(section, "transactions_path", PHASE1_DEFAULT_TXN_PATH)
            )
        ),
        "predict_path": str(resolve_project_path(predict_path)),
        "future_alert_path": str(resolve_project_path(future_alert_path)),
        "feature_bank_path": str(
            resolve_project_path(
                cfg_str(section, "feature_bank_path", PHASE1_DEFAULT_FEATURE_BANK_PATH)
            )
        ),
        "node_mapping_path": str(
            resolve_project_path(
                cfg_str(section, "node_mapping_path", PHASE1_DEFAULT_NODE_MAPPING_PATH)
            )
        ),
        "formal_eval_predict_path": str(
            resolve_project_path(
                cfg_str(section, "formal_eval_predict_path", predict_path)
            )
        ),
        "formal_eval_label_path": str(
            resolve_project_path(
                cfg_str(section, "formal_eval_label_path", future_alert_path)
            )
        ),
        "formal_eval_dataset": cfg_str(
            section, "formal_eval_dataset", PHASE1_EVAL_DATASET
        ),
        "formal_eval_contract": cfg_str(
            section, "formal_eval_contract", PHASE1_EVAL_CONTRACT
        ),
        "train_label_source": cfg_str(
            section,
            "train_label_source",
            "Phase2 acct_alert membership during Phase1 graph construction",
        ),
        "split_policy": cfg_str(
            section,
            "split_policy",
            "esun_predict_positive_leakback_stratified_train_val",
        ),
        "split_strategy": cfg_str(section, "split_strategy", "leakback"),
        "validation_surface": cfg_str(
            section,
            "validation_surface",
            "val_mask on the post-leakback Phase1 train pool",
        ),
        "test_surface": cfg_str(
            section,
            "test_surface",
            "acct_predict accounts held out except for guarded positive leak-back into train/val",
        ),
        "validation_split_ratio": cfg_float(section, "validation_split_ratio", 0.1),
        "split_random_state": cfg_int(section, "split_random_state", 42),
        "leakback_ratio": cfg_float(section, "leakback_ratio", 0.25),
        "min_test_positive_ratio": cfg_float(section, "min_test_positive_ratio", 0.5),
    }


def get_phase3_data_contract() -> Dict[str, Any]:
    section = get_runtime_section("phase3_data_contract")
    predi_path = cfg_str(section, "formal_eval_predict_path", PHASE3_DEFAULT_PREDI_PATH)
    gt_path = cfg_str(section, "formal_eval_label_path", PHASE3_DEFAULT_GT_PATH)
    return {
        "structure_path": str(
            resolve_project_path(
                cfg_str(section, "structure_path", PHASE3_DEFAULT_STRUCTURE_PATH)
            )
        ),
        "feature_bank_path": str(
            resolve_project_path(
                cfg_str(section, "feature_bank_path", PHASE3_DEFAULT_FEATURE_BANK_PATH)
            )
        ),
        "formal_eval_predict_path": str(resolve_project_path(predi_path)),
        "formal_eval_label_path": str(resolve_project_path(gt_path)),
        "formal_eval_dataset": cfg_str(
            section, "formal_eval_dataset", CANONICAL_TEST_DATASET
        ),
        "formal_eval_contract": cfg_str(
            section, "formal_eval_contract", CANONICAL_TEST_CONTRACT
        ),
        "train_label_source": cfg_str(
            section,
            "train_label_source",
            "labels and masks embedded in Phase3 structure artifact",
        ),
        "split_policy": cfg_str(
            section, "split_policy", "esun_non_predi_stratified_train_val"
        ),
        "validation_surface": cfg_str(
            section,
            "validation_surface",
            "val_mask on non-acct_predi E.SUN accounts inside the Phase3 graph",
        ),
        "test_surface": cfg_str(
            section,
            "test_surface",
            "acct_predi accounts held out by senior-style split for graph-side test masking",
        ),
        "validation_split_ratio": cfg_float(section, "validation_split_ratio", 0.1),
        "split_random_state": cfg_int(section, "split_random_state", 42),
    }


def build_phase3_senior_split_masks(
    data,
    predict_path: Optional[str] = None,
    val_ratio: float = 0.1,
    random_state: int = 42,
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    if not hasattr(data, "acct_to_node"):
        raise ValueError(
            "Phase3 data missing acct_to_node for senior split reconstruction"
        )
    if data.y is None:
        raise ValueError("Phase3 data missing labels for senior split reconstruction")

    predict_csv = str(
        predict_path or get_phase3_data_contract()["formal_eval_predict_path"]
    )
    df_predict = pd.read_csv(predict_csv)
    predict_accounts = set(df_predict["acct"].astype(str).drop_duplicates().tolist())
    raw_mapping = getattr(data, "acct_to_node")
    num_nodes = int(data.num_nodes)
    is_esun_mask = getattr(data, "is_esun_mask", None)

    candidate_indices: List[int] = []
    for acct, node_idx in raw_mapping.items():
        idx = int(node_idx)
        if idx < 0 or idx >= num_nodes:
            continue
        if is_esun_mask is not None and not bool(is_esun_mask[idx]):
            continue
        candidate_indices.append(idx)

    candidate_indices = sorted(set(candidate_indices))
    if not candidate_indices:
        raise ValueError(
            "Phase3 senior split reconstruction found no candidate account nodes"
        )

    test_idx = sorted(
        int(idx)
        for acct, idx in raw_mapping.items()
        if str(acct) in predict_accounts and int(idx) in candidate_indices
    )
    train_val_idx = sorted(set(candidate_indices) - set(test_idx))

    if not test_idx:
        raise ValueError(
            "Phase3 senior split reconstruction found no acct_predi nodes in graph"
        )
    if not train_val_idx:
        raise ValueError(
            "Phase3 senior split reconstruction found no train/val candidates"
        )

    labels_arr = data.y[train_val_idx].detach().cpu().numpy()
    stratify_labels = None
    unique_labels, counts = np.unique(labels_arr, return_counts=True)
    if len(unique_labels) > 1 and int(counts.min()) >= 2:
        stratify_labels = labels_arr

    train_idx, val_idx = train_test_split(
        train_val_idx,
        test_size=float(val_ratio),
        random_state=int(random_state),
        stratify=stratify_labels,
    )

    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    val_mask = torch.zeros(num_nodes, dtype=torch.bool)
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)
    train_mask[torch.tensor(train_idx, dtype=torch.long)] = True
    val_mask[torch.tensor(val_idx, dtype=torch.long)] = True
    test_mask[torch.tensor(test_idx, dtype=torch.long)] = True
    return train_mask, val_mask, test_mask


def create_senior_aligned_split(
    accounts: list,
    esun_accounts: set[str],
    test_accounts: set[str],
    labels: torch.Tensor,
    num_nodes: int,
    val_ratio: float = 0.1,
    random_state: int = 42,
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Create train/val/test masks using senior-style split semantics.

    Senior split logic:
    - Train pool: all esun accounts NOT in test_accounts (acct_predict)
    - Val: stratified 10% holdout from train pool
    - Test: full acct_predict accounts
    - Labels: from Phase2 acct_alert membership

    This ensures train set contains ~900+ positives from alert accounts
    that are NOT in the predict set, matching senior behavior.

    Args:
        accounts: List of all account IDs in graph order
        esun_accounts: Set of esun account IDs
        test_accounts: Set of acct_predict account IDs (test set)
        labels: Tensor of labels (1=alert, 0=normal)
        num_nodes: Total number of nodes
        val_ratio: Validation split ratio from train pool (default 0.1)
        random_state: Random seed for stratified split

    Returns:
        Tuple of (train_mask, val_mask, test_mask) as boolean tensors
    """
    labels_arr = (
        labels.detach().cpu().numpy()
        if isinstance(labels, torch.Tensor)
        else np.asarray(labels)
    )
    esun_accounts_str = {str(acct) for acct in esun_accounts}
    test_accounts_str = {str(acct) for acct in test_accounts}

    # Train+Val pool: esun accounts NOT in test_accounts (predict)
    train_val_idx = [
        idx
        for idx, acct in enumerate(accounts)
        if str(acct) in esun_accounts_str and str(acct) not in test_accounts_str
    ]

    # Test pool: esun accounts IN test_accounts (predict)
    test_idx = [
        idx
        for idx, acct in enumerate(accounts)
        if str(acct) in esun_accounts_str and str(acct) in test_accounts_str
    ]

    if not train_val_idx:
        raise ValueError(
            "Senior-aligned split requires at least one esun account not in test set"
        )

    # Stratified split train_val into train/val
    train_idx, val_idx = train_test_split(
        train_val_idx,
        test_size=val_ratio,
        random_state=random_state,
        stratify=labels_arr[train_val_idx],
    )

    # Create masks
    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    val_mask = torch.zeros(num_nodes, dtype=torch.bool)
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)

    train_mask[torch.tensor(train_idx, dtype=torch.long)] = True
    val_mask[torch.tensor(val_idx, dtype=torch.long)] = True
    test_mask[torch.tensor(test_idx, dtype=torch.long)] = True

    return train_mask, val_mask, test_mask


def create_phase1_leakback_split(
    accounts: list,
    esun_accounts: set[str],
    test_accounts: set[str],
    labels: torch.Tensor,
    num_nodes: int,
    leakback_ratio: float = 0.25,
    val_ratio: float = 0.1,
    random_state: int = 42,
    min_test_positive_ratio: float = 0.5,
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    if not 0.0 < leakback_ratio < 1.0:
        raise ValueError(f"leakback_ratio must be in (0, 1), got {leakback_ratio}")
    if not 0.0 < min_test_positive_ratio <= 1.0:
        raise ValueError(
            f"min_test_positive_ratio must be in (0, 1], got {min_test_positive_ratio}"
        )

    labels_arr = (
        labels.detach().cpu().numpy()
        if isinstance(labels, torch.Tensor)
        else np.asarray(labels)
    )
    esun_accounts_str = {str(acct) for acct in esun_accounts}
    test_accounts_str = {str(acct) for acct in test_accounts}

    positive_test_idx = [
        idx
        for idx, acct in enumerate(accounts)
        if str(acct) in esun_accounts_str
        and str(acct) in test_accounts_str
        and int(labels_arr[idx]) == 1
    ]
    if not positive_test_idx:
        raise ValueError(
            "Phase1 leakback split requires at least one positive account in the test surface"
        )

    original_positive_test_count = len(positive_test_idx)
    min_remaining_test_positives = int(
        np.ceil(original_positive_test_count * min_test_positive_ratio)
    )
    max_leakback_count = original_positive_test_count - min_remaining_test_positives
    desired_leakback_count = max(
        1, int(np.ceil(original_positive_test_count * leakback_ratio))
    )
    if desired_leakback_count > max_leakback_count:
        raise ValueError(
            "Configured leakback_ratio would reduce test positives below the minimum guard: "
            f"desired={desired_leakback_count}, max={max_leakback_count}"
        )

    rng = np.random.default_rng(random_state)
    leaked_positive_idx = set(
        int(i)
        for i in rng.choice(
            np.asarray(positive_test_idx, dtype=np.int64),
            size=desired_leakback_count,
            replace=False,
        ).tolist()
    )

    train_val_idx: List[int] = []
    test_idx: List[int] = []
    for idx, acct in enumerate(accounts):
        acct_key = str(acct)
        if acct_key not in esun_accounts_str:
            continue
        if acct_key in test_accounts_str and idx not in leaked_positive_idx:
            test_idx.append(idx)
        else:
            train_val_idx.append(idx)

    remaining_test_positives = sum(int(labels_arr[idx]) == 1 for idx in test_idx)
    if remaining_test_positives < min_remaining_test_positives:
        raise ValueError(
            "Leakback split violated minimum test-positive guard: "
            f"remaining={remaining_test_positives}, min={min_remaining_test_positives}"
        )

    train_idx, val_idx = train_test_split(
        train_val_idx,
        test_size=val_ratio,
        random_state=random_state,
        stratify=labels_arr[train_val_idx],
    )

    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    val_mask = torch.zeros(num_nodes, dtype=torch.bool)
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)
    train_mask[torch.tensor(train_idx, dtype=torch.long)] = True
    val_mask[torch.tensor(val_idx, dtype=torch.long)] = True
    test_mask[torch.tensor(test_idx, dtype=torch.long)] = True
    return train_mask, val_mask, test_mask


def summarize_mask_label_counts(
    labels: torch.Tensor,
    train_mask: torch.Tensor,
    val_mask: torch.Tensor,
    test_mask: torch.Tensor,
) -> Dict[str, int]:
    labels_arr = (
        labels.detach().cpu().numpy()
        if isinstance(labels, torch.Tensor)
        else np.asarray(labels)
    )

    def _count(mask: torch.Tensor) -> Tuple[int, int]:
        idx = mask.detach().cpu().numpy().astype(bool)
        total = int(idx.sum())
        positives = int(labels_arr[idx].sum())
        negatives = total - positives
        return positives, negatives

    train_pos, train_neg = _count(train_mask)
    val_pos, val_neg = _count(val_mask)
    test_pos, test_neg = _count(test_mask)
    return {
        "train_pos": train_pos,
        "train_neg": train_neg,
        "val_pos": val_pos,
        "val_neg": val_neg,
        "test_pos": test_pos,
        "test_neg": test_neg,
    }


def write_epoch_start_progress(
    reporter: ProgressReporter,
    experiment_name: str,
    epoch: int,
    total_epochs: int,
) -> None:
    previous_val_f1 = 0.0
    previous_loss = 0.0
    try:
        if os.path.exists(reporter.progress_file):
            with open(reporter.progress_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                previous_val_f1 = float(payload.get("val_f1", 0.0) or 0.0)
                previous_loss = float(payload.get("loss", 0.0) or 0.0)
    except Exception:
        previous_val_f1 = 0.0
        previous_loss = 0.0

    progress = {
        "experiment": experiment_name,
        "epoch": epoch,
        "total_epochs": total_epochs,
        "val_f1": round(previous_val_f1, 4),
        "loss": round(previous_loss, 4),
        "percent": round(100 * epoch / max(total_epochs, 1), 1),
        "phase": "epoch_start",
        "timestamp": datetime.now().isoformat(),
    }
    try:
        with open(reporter.progress_file, "w", encoding="utf-8") as f:
            json.dump(progress, f)
    except Exception:
        pass


def _resolve_canonical_test_paths() -> Tuple[str, str]:
    contract = get_phase3_data_contract()
    gt_path = contract["formal_eval_label_path"]
    predi_path = contract["formal_eval_predict_path"]
    if not gt_path:
        raise FileNotFoundError("Canonical GT file path is empty")
    if not predi_path:
        raise FileNotFoundError("Canonical predi file path is empty")
    if not os.path.exists(gt_path):
        raise FileNotFoundError(f"Canonical GT file not found: {gt_path}")
    if not os.path.exists(predi_path):
        raise FileNotFoundError(f"Canonical predi file not found: {predi_path}")
    return gt_path, predi_path


def _resolve_phase1_eval_paths() -> Tuple[str, str]:
    contract = get_phase1_data_contract()
    predict_path = contract["formal_eval_predict_path"]
    alert_path = contract["formal_eval_label_path"]
    if not predict_path:
        raise FileNotFoundError("Phase1 predict file path is empty")
    if not alert_path:
        raise FileNotFoundError("Phase2 alert file path is empty")
    if not os.path.exists(predict_path):
        raise FileNotFoundError(f"Phase1 predict file not found: {predict_path}")
    if not os.path.exists(alert_path):
        raise FileNotFoundError(f"Phase2 alert file not found: {alert_path}")
    return predict_path, alert_path


def _load_canonical_test_targets_uncached(
    gt_path: str, predi_path: str
) -> List[Tuple[str, int]]:
    import pandas as pd  # pyright: ignore[reportMissingImports]

    df_gt = pd.read_csv(gt_path)
    df_predi = pd.read_csv(predi_path)
    if "acct" not in df_gt.columns or "label" not in df_gt.columns:
        raise ValueError(f"GT file missing required columns: {gt_path}")
    if "acct" not in df_predi.columns:
        raise ValueError(f"Predi file missing required acct column: {predi_path}")

    gt_pairs = df_gt[["acct", "label"]].copy()
    gt_pairs["acct"] = gt_pairs["acct"].astype(str)
    gt_pairs["label"] = gt_pairs["label"].astype(int)

    predi_accounts = set(df_predi["acct"].astype(str).drop_duplicates())
    merged = gt_pairs[gt_pairs["acct"].isin(predi_accounts)]
    if merged.empty:
        raise ValueError("Canonical test target intersection is empty")

    return [(str(acct), int(label)) for acct, label in merged.itertuples(index=False)]


def _load_phase1_future_alert_targets_uncached(
    predict_path: str, alert_path: str
) -> List[Tuple[str, int]]:
    import pandas as pd  # pyright: ignore[reportMissingImports]

    df_predict = pd.read_csv(predict_path)
    df_alert = pd.read_csv(alert_path)
    if "acct" not in df_predict.columns:
        raise ValueError(f"Predict file missing required acct column: {predict_path}")
    if "acct" not in df_alert.columns:
        raise ValueError(f"Alert file missing required acct column: {alert_path}")

    predict_accounts = (
        df_predict[["acct"]]
        .copy()
        .astype({"acct": str})
        .drop_duplicates(subset=["acct"])
    )
    alert_accounts = set(df_alert["acct"].astype(str).drop_duplicates())
    if predict_accounts.empty:
        raise ValueError("Phase1 predict target set is empty")

    return [
        (str(acct), 1 if str(acct) in alert_accounts else 0)
        for acct in predict_accounts["acct"].tolist()
    ]


@lru_cache(maxsize=1)
def _load_default_canonical_test_targets() -> Tuple[Tuple[str, int], ...]:
    gt_path, predi_path = _resolve_canonical_test_paths()
    return tuple(_load_canonical_test_targets_uncached(gt_path, predi_path))


@lru_cache(maxsize=1)
def _load_default_phase1_future_alert_targets() -> Tuple[Tuple[str, int], ...]:
    predict_path, alert_path = _resolve_phase1_eval_paths()
    return tuple(_load_phase1_future_alert_targets_uncached(predict_path, alert_path))


def load_canonical_test_targets(
    gt_path: Optional[str] = None, predi_path: Optional[str] = None
) -> List[Tuple[str, int]]:
    if gt_path is None and predi_path is None:
        return list(_load_default_canonical_test_targets())
    if gt_path is None or predi_path is None:
        raise ValueError("gt_path and predi_path must both be provided together")
    return _load_canonical_test_targets_uncached(gt_path, predi_path)


def load_phase1_future_alert_targets(
    predict_path: Optional[str] = None, alert_path: Optional[str] = None
) -> List[Tuple[str, int]]:
    if predict_path is None and alert_path is None:
        return list(_load_default_phase1_future_alert_targets())
    if predict_path is None or alert_path is None:
        raise ValueError("predict_path and alert_path must both be provided together")
    return _load_phase1_future_alert_targets_uncached(predict_path, alert_path)


def predict_account_probabilities(
    model: nn.Module,
    data,
    device: torch.device,
    acct_to_node: Dict[str, int],
    accounts: List[str],
    *,
    use_minibatch: bool,
    batch_size: int,
    num_workers: int,
    num_neighbors: Optional[List[int]] = None,
) -> Dict[str, float]:
    ordered_pairs = [
        (acct, int(acct_to_node[acct])) for acct in accounts if acct in acct_to_node
    ]
    if not ordered_pairs:
        return {}

    if use_minibatch:
        temporal_loader_kwargs: Dict[str, Any] = {}
        if hasattr(data, "edge_time") and data.edge_time is not None:
            temporal_loader_kwargs = {
                "time_attr": "edge_time",
                "temporal_strategy": "last",
                "is_sorted": False,
            }
        loader = NeighborLoader(
            data,
            num_neighbors=num_neighbors or [10, 10],
            input_nodes=torch.tensor(
                [idx for _, idx in ordered_pairs], dtype=torch.long
            ),
            batch_size=batch_size,
            shuffle=False,
            num_workers=num_workers,
            **temporal_loader_kwargs,
        )
        probs, _labels = evaluate(model, loader, device)
        return {acct: float(prob) for (acct, _idx), prob in zip(ordered_pairs, probs)}

    if data.x is None or data.edge_index is None:
        raise ValueError("Missing x or edge_index for canonical full-batch inference")
    model.eval()
    with torch.no_grad():
        x = data.x if getattr(data.x, "device", device) == device else data.x.to(device)
        edge_index = (
            data.edge_index
            if getattr(data.edge_index, "device", device) == device
            else data.edge_index.to(device)
        )
        use_amp = bool(getattr(x, "is_cuda", False))
        with torch.cuda.amp.autocast(enabled=use_amp):
            logits = model(x, edge_index)
        selected = torch.tensor([idx for _, idx in ordered_pairs], device=logits.device)
        probs = torch.softmax(logits[selected], dim=1)[:, 1].detach().cpu().numpy()
    return {acct: float(prob) for (acct, _idx), prob in zip(ordered_pairs, probs)}


def summarize_canonical_test_metrics(
    account_probs: Dict[str, float],
    threshold: float,
    *,
    targets: Optional[List[Tuple[str, int]]] = None,
    evaluation_dataset: str = CANONICAL_TEST_DATASET,
    eval_contract: str = CANONICAL_TEST_CONTRACT,
) -> Dict[str, Any]:
    target_pairs = targets or load_canonical_test_targets()
    matched_accounts: List[str] = []
    labels: List[int] = []
    probs: List[float] = []

    for acct, label in target_pairs:
        if acct in account_probs:
            matched_accounts.append(acct)
        labels.append(int(label))
        probs.append(float(account_probs.get(acct, 0.0)))

    if not labels:
        raise ValueError(
            "No canonical test accounts matched current graph/account_probs"
        )

    labels_arr = np.asarray(labels, dtype=np.int64)
    probs_arr = np.asarray(probs, dtype=np.float64)
    preds = (probs_arr >= float(threshold)).astype(int)
    try:
        test_auc = float(roc_auc_score(labels_arr, probs_arr))
    except ValueError:
        test_auc = 0.0

    return {
        "test_f1": float(f1_score(labels_arr, preds, zero_division="warn")),
        "test_precision": float(
            precision_score(labels_arr, preds, zero_division="warn")
        ),
        "test_recall": float(recall_score(labels_arr, preds, zero_division="warn")),
        "test_auc": test_auc,
        "evaluation_dataset": evaluation_dataset,
        "eval_contract": eval_contract,
        "evaluation_account_count": len(target_pairs),
        "evaluation_missing_account_count": max(
            len(target_pairs) - len(matched_accounts), 0
        ),
    }


def summarize_senior_compatible_test_metrics(
    model,
    data,
    device,
    accounts_list,
    test_mask,
    label_file_path,
    predict_file_path,
    threshold_from_val,
    evaluation_dataset="",
    eval_contract="",
):
    """
    Senior-compatible test F1 evaluation:
    1. Full-graph inference -> all_probs = softmax(logits)[:,1]
    2. Extract test_mask nodes: test_probs = all_probs[test_mask], test_accounts = accounts_list[test_mask]
    3. Map test_accounts to predictions via threshold_from_val
    4. Load label file, build ground truth: y_true = 1 if acct in label_set else 0
    5. Compute F1, precision, recall, AUROC, AUPRC on the intersection
    6. Return metrics dict with metric_surface="senior_compatible"
    """
    import pandas as pd  # pyright: ignore[reportMissingImports]

    if data.x is None or data.edge_index is None:
        raise ValueError("Missing x or edge_index for senior-compatible inference")

    if accounts_list is None:
        raise ValueError("accounts_list is required")

    model.eval()
    with torch.no_grad():
        x = data.x if data.x.device == device else data.x.to(device)
        edge_index = (
            data.edge_index
            if data.edge_index.device == device
            else data.edge_index.to(device)
        )
        use_amp = bool(getattr(x, "is_cuda", False))
        with torch.cuda.amp.autocast(enabled=use_amp):
            logits = model(x, edge_index)
        all_probs = torch.softmax(logits, dim=1)[:, 1].detach().cpu().numpy()

        if not np.isfinite(all_probs).all():
            logits = model(x.float(), edge_index)
            all_probs = torch.softmax(logits, dim=1)[:, 1].detach().cpu().numpy()

    non_finite_prob_mask = ~np.isfinite(all_probs)
    if non_finite_prob_mask.any():
        all_probs = all_probs.copy()
        all_probs[non_finite_prob_mask] = 0.0

    if len(accounts_list) != len(all_probs):
        raise ValueError(
            f"accounts_list length ({len(accounts_list)}) does not match node count ({len(all_probs)})"
        )

    if isinstance(test_mask, torch.Tensor):
        test_mask_np = test_mask.detach().cpu().numpy().astype(bool)
    else:
        test_mask_np = np.asarray(test_mask, dtype=bool)

    if test_mask_np.shape[0] != len(all_probs):
        raise ValueError("test_mask length does not match node count")

    test_indices = np.flatnonzero(test_mask_np)
    if test_indices.size == 0:
        raise ValueError("test_mask has no positive nodes")

    test_accounts = [str(accounts_list[int(node_idx)]) for node_idx in test_indices]
    test_probs = all_probs[test_indices]
    df_test = pd.DataFrame({"acct": test_accounts, "prob": test_probs})
    df_test["label"] = (df_test["prob"] >= float(threshold_from_val)).astype(int)

    df_predict = pd.read_csv(predict_file_path)
    if "acct" not in df_predict.columns:
        raise ValueError(f"Predict file missing acct column: {predict_file_path}")
    df_predict = df_predict[["acct"]].copy()
    df_predict["acct"] = df_predict["acct"].astype(str)
    df_predict = df_predict.drop_duplicates(subset=["acct"])

    eval_df = df_predict.merge(df_test, on="acct", how="left", indicator=True)
    missing_pred_mask = eval_df["_merge"] == "left_only"
    eval_df = eval_df.drop(columns=["_merge"])
    eval_df["prob"] = eval_df["prob"].where(
        eval_df["prob"].notna() & np.isfinite(eval_df["prob"]), 0.0
    )
    eval_df["label"] = eval_df["label"].fillna(0).astype(int)
    metric_surface_mode = "predict_left_join_fill_zero"

    df_label = pd.read_csv(label_file_path)
    if "acct" not in df_label.columns:
        raise ValueError(f"Label file missing acct column: {label_file_path}")
    df_label["acct"] = df_label["acct"].astype(str)

    if "label" in df_label.columns:
        label_df = df_label[["acct", "label"]].copy()
        label_df["label"] = label_df["label"].astype(int)
        eval_df = eval_df.merge(label_df, on="acct", how="left")
        eval_df["label_y"] = eval_df["label_y"].fillna(0).astype(int)
        eval_df = eval_df.rename(
            columns={"label_y": "label_true", "label_x": "label_pred"}
        )
    else:
        label_set = set(df_label["acct"].drop_duplicates().tolist())
        eval_df = eval_df.rename(columns={"label": "label_pred"})
        eval_df["label_true"] = eval_df["acct"].apply(
            lambda acct: 1 if acct in label_set else 0
        )

    if eval_df.empty:
        raise ValueError("No labeled intersection rows available for evaluation")

    if "label_true" not in eval_df.columns:
        eval_df = eval_df.rename(
            columns={"label": "label_true", "label_pred": "label_pred"}
        )

    labels_arr = eval_df["label_true"].to_numpy(dtype=np.int64)
    probs_arr = eval_df["prob"].to_numpy(dtype=np.float64)
    preds_arr = (probs_arr >= float(threshold_from_val)).astype(int)

    try:
        test_auc = float(roc_auc_score(labels_arr, probs_arr))
    except ValueError:
        test_auc = 0.0

    try:
        test_auprc = float(average_precision_score(labels_arr, probs_arr))
    except ValueError:
        test_auprc = 0.0

    return {
        "test_f1": float(f1_score(labels_arr, preds_arr, zero_division="warn")),
        "test_precision": float(
            precision_score(labels_arr, preds_arr, zero_division="warn")
        ),
        "test_recall": float(recall_score(labels_arr, preds_arr, zero_division="warn")),
        "test_auc": test_auc,
        "test_auprc": test_auprc,
        "evaluation_dataset": evaluation_dataset,
        "eval_contract": eval_contract,
        "evaluation_account_count": int(len(eval_df)),
        "evaluation_missing_account_count": int(missing_pred_mask.sum()),
        "metric_surface": "senior_compatible",
        "metric_surface_mode": metric_surface_mode,
    }


# =============================================================================
# Warm Cache — shared pre-computed graph/structure across experiments
# =============================================================================

_WARM_CACHE_DIR = Path(__file__).resolve().parents[1] / "warm_cache"
_WARM_CACHE_LOCK = threading.Lock()


def _warm_cache_key(*parts: str) -> str:
    import hashlib

    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def _phase1_cache_key(
    contract: Dict[str, Any], feature_signature: Optional[str] = None
) -> str:
    parts = [
        contract["transactions_path"],
        contract["future_alert_path"],
        contract["predict_path"],
        str(contract.get("split_policy", "")),
        str(contract.get("split_strategy", "")),
        str(contract["validation_split_ratio"]),
        str(contract["split_random_state"]),
        str(contract.get("leakback_ratio", "")),
        str(contract.get("min_test_positive_ratio", "")),
    ]
    if feature_signature:
        parts.append(f"feature_signature={feature_signature}")
    return _warm_cache_key(*parts)


def save_phase1_warm_cache(
    data,
    accounts: list,
    acct_to_node: dict,
    contract: Dict[str, Any],
    feature_signature: Optional[str] = None,
    scaler_state: Optional[Dict[str, Any]] = None,
) -> Path:
    _WARM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_key = _phase1_cache_key(contract, feature_signature)
    cache_path = _WARM_CACHE_DIR / f"phase1_{cache_key}.pt"
    tmp_path = cache_path.with_suffix(".tmp")
    payload = {
        "x": data.x,
        "edge_index": data.edge_index,
        "y": data.y,
        "train_mask": data.train_mask,
        "val_mask": data.val_mask,
        "test_mask": data.test_mask,
        "accounts": accounts,
        "acct_to_node": acct_to_node,
        "cache_key": cache_key,
        "scaler_state": scaler_state,
    }
    torch.save(payload, tmp_path)
    tmp_path.rename(cache_path)
    return cache_path


def load_phase1_warm_cache(
    contract: Dict[str, Any], feature_signature: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    cache_key = _phase1_cache_key(contract, feature_signature)
    cache_path = _WARM_CACHE_DIR / f"phase1_{cache_key}.pt"
    if not cache_path.exists():
        return None
    return torch.load(cache_path, weights_only=False)


def save_phase3_warm_structure(
    data, accounts_list: list, contract: Dict[str, Any]
) -> Path:
    _WARM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_key = _warm_cache_key(
        contract["structure_path"],
        contract["formal_eval_predict_path"],
        str(contract["validation_split_ratio"]),
        str(contract["split_random_state"]),
    )
    cache_path = _WARM_CACHE_DIR / f"phase3_struct_{cache_key}.pt"
    tmp_path = cache_path.with_suffix(".tmp")
    payload = {
        "edge_index": data.edge_index,
        "y": data.y,
        "train_mask": data.train_mask,
        "val_mask": data.val_mask,
        "test_mask": data.test_mask,
        "num_nodes": int(data.num_nodes),
        "accounts_list": accounts_list,
        "cache_key": cache_key,
    }
    if hasattr(data, "acct_to_node"):
        payload["acct_to_node"] = data.acct_to_node
    if hasattr(data, "node_to_acct"):
        payload["node_to_acct"] = data.node_to_acct
    if hasattr(data, "is_esun_mask"):
        payload["is_esun_mask"] = data.is_esun_mask
    if hasattr(data, "predict_mask"):
        payload["predict_mask"] = data.predict_mask
    if hasattr(data, "edge_time"):
        payload["edge_time"] = data.edge_time
    if hasattr(data, "edge_attr"):
        payload["edge_attr"] = data.edge_attr
    torch.save(payload, tmp_path)
    tmp_path.rename(cache_path)
    return cache_path


def load_phase3_warm_structure(contract: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cache_key = _warm_cache_key(
        contract["structure_path"],
        contract["formal_eval_predict_path"],
        str(contract["validation_split_ratio"]),
        str(contract["split_random_state"]),
    )
    cache_path = _WARM_CACHE_DIR / f"phase3_struct_{cache_key}.pt"
    if not cache_path.exists():
        return None
    return torch.load(cache_path, weights_only=False)


# =============================================================================
# Model Components
# =============================================================================


class MLPHead(nn.Module):
    def __init__(
        self, in_channels, hidden_channels, out_channels, depth=1, dropout=0.3
    ):
        super().__init__()
        layers = []
        curr_in = in_channels
        for i in range(depth - 1):
            layers.append(nn.Linear(curr_in, hidden_channels))
            layers.append(BatchNorm(hidden_channels))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout))
            curr_in = hidden_channels
        layers.append(nn.Linear(curr_in, out_channels))
        self.mlp = nn.Sequential(*layers)

    def forward(self, x):
        return self.mlp(x)


# =============================================================================
# Model Architectures
# =============================================================================


class GraphSAGE_LSTM(nn.Module):
    def __init__(
        self, in_channels, hidden_channels, out_channels, num_layers=4, dropout=0.3
    ):
        super().__init__()
        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList(
            [
                SAGEConv(hidden_channels, hidden_channels, aggr="lstm")
                for _ in range(num_layers)
            ]
        )
        self.bns = nn.ModuleList(
            [BatchNorm(hidden_channels) for _ in range(num_layers)]
        )
        self.output_proj = nn.Linear(hidden_channels, out_channels)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, edge_attr=None):
        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class GATv2EdgeModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        edge_dim=2,
        **kwargs,
    ):
        super().__init__()
        heads = kwargs.get("heads", 2)
        self.edge_time_concat = kwargs.get("edge_time_concat", False)

        # Feature interaction support
        self.feature_interaction = kwargs.get("feature_interaction")
        self.feature_gating = kwargs.get("feature_gating", False)
        if self.feature_interaction == "bilinear":
            self.interaction_layer = nn.Linear(in_channels, in_channels)
        if self.feature_gating:
            self.gate_layer = nn.Linear(in_channels, in_channels)

        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        for _ in range(num_layers):
            self.convs.append(
                GATv2Conv(
                    hidden_channels,
                    hidden_channels,
                    heads=heads,
                    edge_dim=edge_dim,
                    add_self_loops=False,
                    concat=False,
                )
            )
            self.bns.append(BatchNorm(hidden_channels))
        self.output_proj = nn.Linear(hidden_channels, out_channels)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, edge_attr=None, **kwargs):
        # Apply interaction/gating if enabled
        if self.feature_gating:
            gate = torch.sigmoid(self.gate_layer(x))
            x = x * gate
        if self.feature_interaction == "bilinear":
            x = x + x * torch.tanh(self.interaction_layer(x))

        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            # Only pass edge_attr if the conv expects it (based on edge_dim at init)
            # We assume if edge_dim was set, edge_attr is required/provided.
            # But we handle the case where it might be None if passed explicitly.
            if edge_attr is not None:
                x = x + self.dropout(
                    F.relu(bn(conv(x, edge_index, edge_attr=edge_attr)))
                )
            else:
                # Fallback if edge_attr is missing but conv might expect it?
                # PyG GATv2Conv will error if edge_dim != None and edge_attr is missing.
                # So we assume the user configured edge_dim correctly.
                # If edge_dim was 0/None, edge_attr arg is ignored by conv usually.
                x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class GATModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()

        # Feature interaction support
        self.feature_interaction = kwargs.get("feature_interaction")
        self.feature_gating = kwargs.get("feature_gating", False)
        if self.feature_interaction == "bilinear":
            self.interaction_layer = nn.Linear(in_channels, in_channels)
        if self.feature_gating:
            self.gate_layer = nn.Linear(in_channels, in_channels)

        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        for _ in range(num_layers):
            self.convs.append(
                GATv2Conv(hidden_channels, hidden_channels, add_self_loops=False)
            )
            self.bns.append(BatchNorm(hidden_channels))
        self.output_proj = nn.Linear(hidden_channels, out_channels)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, *args, **kwargs):
        # Apply interaction/gating if enabled
        if self.feature_gating:
            gate = torch.sigmoid(self.gate_layer(x))
            x = x * gate
        if self.feature_interaction == "bilinear":
            x = x + x * torch.tanh(self.interaction_layer(x))

        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class APPNPModel(nn.Module):
    def __init__(
        self, in_channels, hidden_channels, out_channels, num_layers=4, dropout=0.3
    ):
        super().__init__()
        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.hidden_proj = nn.Linear(hidden_channels, hidden_channels)
        self.appnp = APPNP(K=num_layers, alpha=0.1, dropout=dropout)
        self.output_proj = nn.Linear(hidden_channels, out_channels)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, *args, **kwargs):
        x = self.dropout(F.relu(self.input_proj(x)))
        x = self.dropout(F.relu(self.hidden_proj(x)))
        x = self.appnp(x, edge_index)
        return self.output_proj(x)


class GraphSAGEModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        self.feature_interaction = kwargs.get("feature_interaction")
        self.feature_gating = kwargs.get("feature_gating", False)
        if self.feature_interaction == "bilinear":
            self.interaction_layer = nn.Linear(in_channels, in_channels)
        if self.feature_gating:
            self.gate_layer = nn.Linear(in_channels, in_channels)

        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList(
            [SAGEConv(hidden_channels, hidden_channels) for _ in range(num_layers)]
        )
        self.bns = nn.ModuleList(
            [BatchNorm(hidden_channels) for _ in range(num_layers)]
        )

        mlp_depth = kwargs.get("mlp_depth", 1)
        mlp_hidden = kwargs.get("mlp_hidden", hidden_channels)
        self.output_proj = MLPHead(
            hidden_channels, mlp_hidden, out_channels, mlp_depth, dropout
        )
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, *args, **kwargs):
        if self.feature_gating:
            gate = torch.sigmoid(self.gate_layer(x))
            x = x * gate
        if self.feature_interaction == "bilinear":
            x = x + x * torch.tanh(self.interaction_layer(x))

        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class SAGE_LSTM_Model(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        self.feature_interaction = kwargs.get("feature_interaction")
        self.feature_gating = kwargs.get("feature_gating", False)
        if self.feature_interaction == "bilinear":
            self.interaction_layer = nn.Linear(in_channels, in_channels)
        if self.feature_gating:
            self.gate_layer = nn.Linear(in_channels, in_channels)

        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList(
            [
                SAGEConv(hidden_channels, hidden_channels, aggr="lstm")
                for _ in range(num_layers)
            ]
        )
        self.bns = nn.ModuleList(
            [BatchNorm(hidden_channels) for _ in range(num_layers)]
        )

        mlp_depth = kwargs.get("mlp_depth", 1)
        mlp_hidden = kwargs.get("mlp_hidden", hidden_channels)
        self.output_proj = MLPHead(
            hidden_channels, mlp_hidden, out_channels, mlp_depth, dropout
        )
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, *args, **kwargs):
        if self.feature_gating:
            gate = torch.sigmoid(self.gate_layer(x))
            x = x * gate
        if self.feature_interaction == "bilinear":
            x = x + x * torch.tanh(self.interaction_layer(x))

        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class GINModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        self.feature_interaction = kwargs.get("feature_interaction")
        self.feature_gating = kwargs.get("feature_gating", False)
        if self.feature_interaction == "bilinear":
            self.interaction_layer = nn.Linear(in_channels, in_channels)
        if self.feature_gating:
            self.gate_layer = nn.Linear(in_channels, in_channels)

        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_channels, hidden_channels),
                nn.ReLU(),
                nn.Linear(hidden_channels, hidden_channels),
            )
            self.convs.append(GINConv(mlp))
            self.bns.append(BatchNorm(hidden_channels))

        mlp_depth = kwargs.get("mlp_depth", 1)
        mlp_hidden = kwargs.get("mlp_hidden", hidden_channels)
        self.output_proj = MLPHead(
            hidden_channels, mlp_hidden, out_channels, mlp_depth, dropout
        )
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, *args, **kwargs):
        if self.feature_gating:
            gate = torch.sigmoid(self.gate_layer(x))
            x = x * gate
        if self.feature_interaction == "bilinear":
            x = x + x * torch.tanh(self.interaction_layer(x))

        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class GCNModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        self.feature_interaction = kwargs.get("feature_interaction")
        self.feature_gating = kwargs.get("feature_gating", False)
        if self.feature_interaction == "bilinear":
            self.interaction_layer = nn.Linear(in_channels, in_channels)
        if self.feature_gating:
            self.gate_layer = nn.Linear(in_channels, in_channels)

        self.dropout = nn.Dropout(dropout)
        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList(
            [GCNConv(hidden_channels, hidden_channels) for _ in range(num_layers)]
        )
        self.bns = nn.ModuleList(
            [BatchNorm(hidden_channels) for _ in range(num_layers)]
        )

        mlp_depth = kwargs.get("mlp_depth", 1)
        mlp_hidden = kwargs.get("mlp_hidden", hidden_channels)
        self.output_proj = MLPHead(
            hidden_channels, mlp_hidden, out_channels, mlp_depth, dropout
        )
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, *args, **kwargs):
        if self.feature_gating:
            gate = torch.sigmoid(self.gate_layer(x))
            x = x * gate
        if self.feature_interaction == "bilinear":
            x = x + x * torch.tanh(self.interaction_layer(x))

        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class FullGraphMGDConv(MessagePassing):
    """Direction-aware diffusion conv (self/out/in views)."""

    def __init__(self, in_dim: int, out_dim: int, aggregation_mode: str = "add"):
        super().__init__(aggr=aggregation_mode)
        self.transform_self = nn.Linear(in_dim, out_dim)
        self.transform_context = nn.Linear(in_dim * 2, out_dim)
        self.fusion_layer = nn.Linear(out_dim * 3, out_dim)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        self_v = self.transform_self(x)
        out_msg = self.propagate(edge_index, x=x)
        out_v = self.transform_context(out_msg)

        row, col = edge_index
        rev_edge_index = torch.stack([col, row], dim=0)
        in_msg = self.propagate(rev_edge_index, x=x)
        in_v = self.transform_context(in_msg)

        combined = torch.cat([self_v, out_v, in_v], dim=-1)
        return self.fusion_layer(combined)

    def message(self, x_i, x_j):
        difference = torch.tanh(x_i - x_j)
        return torch.cat([difference, x_j], dim=-1)


class FullGraphMGDModel(nn.Module):
    def __init__(
        self, in_channels, hidden_channels, out_channels, num_layers=2, dropout=0.2
    ):
        super().__init__()
        self.num_layers = num_layers
        self.dropout = dropout

        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        for i in range(num_layers):
            in_dim = in_channels if i == 0 else hidden_channels
            self.convs.append(FullGraphMGDConv(in_dim, hidden_channels))
            self.bns.append(nn.BatchNorm1d(hidden_channels))

        self.classifier = nn.Sequential(
            nn.Linear(hidden_channels, hidden_channels),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_channels, out_channels),
        )

    def forward(self, x, edge_index, *args, **kwargs):
        x = x.contiguous()
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            x = self.bns[i](x)
            x = F.relu(x)
            if torch.isnan(x).any():
                x = torch.where(torch.isnan(x), torch.zeros_like(x), x)
            x = F.dropout(x, p=self.dropout, training=self.training)
        return self.classifier(x)


class DisConv(MessagePassing):
    """Difference-aware attention conv for Zebra."""

    def __init__(self, in_dim: int, out_dim: int, aggregation_mode: str = "add"):
        super().__init__(aggr=aggregation_mode)
        self.transform_self = nn.Linear(in_dim, out_dim)
        self.transform_context = nn.Linear(in_dim * 2, out_dim)
        self.fusion_layer = nn.Linear(out_dim * 3, out_dim)
        self.attn_proj = nn.Linear(in_dim, 1, bias=False)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        self_v = self.transform_self(x)
        out_msg = self.propagate(edge_index, x=x)
        out_v = self.transform_context(out_msg)

        row, col = edge_index
        rev_edge_index = torch.stack([col, row], dim=0)
        in_msg = self.propagate(rev_edge_index, x=x)
        in_v = self.transform_context(in_msg)

        combined = torch.cat([self_v, out_v, in_v], dim=-1)
        return self.fusion_layer(combined)

    def message(self, x_i, x_j, edge_index_i):
        difference = torch.tanh(x_i - x_j)
        e_vu = F.leaky_relu(self.attn_proj(difference))
        alpha_vu = softmax(e_vu, index=edge_index_i)
        return alpha_vu * torch.cat([difference, x_j], dim=-1)


class ZebraModel(nn.Module):
    def __init__(
        self, in_channels, hidden_channels, out_channels, num_layers=4, dropout=0.3
    ):
        super().__init__()
        self.num_layers = num_layers
        self.dropout = dropout

        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        self.layer_weights = nn.Parameter(torch.ones(num_layers))

        for i in range(num_layers):
            in_dim = in_channels if i == 0 else hidden_channels
            self.convs.append(DisConv(in_dim, hidden_channels))
            self.bns.append(nn.BatchNorm1d(hidden_channels))

        self.classifier = nn.Sequential(
            nn.Linear(hidden_channels, hidden_channels),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_channels, out_channels),
        )

    def forward(self, x, edge_index, *args, **kwargs):
        res = []
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            x = self.bns[i](x)
            x = F.relu(x)
            res.append(x)
            if torch.isnan(x).any():
                x = torch.where(torch.isnan(x), torch.zeros_like(x), x)
            x = F.dropout(x, p=self.dropout, training=self.training)

        res = torch.stack(res, dim=-1)
        x = torch.sum(res * self.layer_weights, dim=-1)
        return self.classifier(x)


class RGCNModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=2,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        num_relations = kwargs.get("num_relations", 5)
        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList()
        self.convs.append(RGCNConv(hidden_channels, hidden_channels, num_relations))
        for _ in range(num_layers - 1):
            self.convs.append(RGCNConv(hidden_channels, hidden_channels, num_relations))
        self.bns = nn.ModuleList(
            [BatchNorm(hidden_channels) for _ in range(num_layers)]
        )
        self.dropout = nn.Dropout(dropout)
        self.output_proj = nn.Linear(hidden_channels, out_channels)

    def forward(self, x, edge_index, edge_type=None, **kwargs):
        if edge_type is None:
            edge_type = torch.zeros(
                edge_index.size(1), dtype=torch.long, device=edge_index.device
            )
        x = self.input_proj(x)
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index, edge_type))))
        return self.output_proj(x)


class GINEModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        edge_dim = kwargs.get("edge_dim")
        self.use_edge_attr = edge_dim is not None

        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_channels, hidden_channels),
                nn.ReLU(),
                nn.Linear(hidden_channels, hidden_channels),
            )
            if self.use_edge_attr:
                self.convs.append(GINEConv(mlp, edge_dim=edge_dim))
            else:
                self.convs.append(GINConv(mlp, train_eps=True))

            self.bns.append(BatchNorm(hidden_channels))
        self.output_proj = nn.Linear(hidden_channels, out_channels)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, edge_attr=None, **kwargs):
        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            if self.use_edge_attr and edge_attr is not None:
                x = x + self.dropout(
                    F.relu(bn(conv(x, edge_index, edge_attr=edge_attr)))
                )
            else:
                x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class SignedGNNModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=2,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        self.out_channels = out_channels
        self.convs = nn.ModuleList()
        self.convs.append(SignedConv(in_channels, hidden_channels, True))
        for _ in range(num_layers - 1):
            self.convs.append(SignedConv(hidden_channels, hidden_channels, False))
        self.output_proj = None
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, edge_attr=None, **kwargs):
        if edge_attr is not None and edge_attr.dim() > 1:
            sign = edge_attr[:, 0]
            pos_mask = sign > 0
            neg_mask = sign <= 0
            pos_edge_index = edge_index[:, pos_mask]
            neg_edge_index = edge_index[:, neg_mask]
        else:
            pos_edge_index = edge_index
            neg_edge_index = torch.empty(
                (2, 0), dtype=torch.long, device=edge_index.device
            )

        curr_x = x
        for conv in self.convs:
            out = conv(curr_x, pos_edge_index, neg_edge_index)
            if isinstance(out, tuple):
                out_tuple = tuple(out)
                if len(out_tuple) >= 2:
                    x_pos, x_neg = out_tuple[0], out_tuple[1]
                elif len(out_tuple) == 1:
                    x_pos = x_neg = out_tuple[0]
                else:
                    raise ValueError("SignedConv returned an empty tuple")
            else:
                x_pos, x_neg = out, out
            x_pos = self.dropout(F.relu(x_pos))
            x_neg = self.dropout(F.relu(x_neg))
            curr_x = (x_pos, x_neg)

        out = torch.cat([curr_x[0], curr_x[1]], dim=-1)
        if self.output_proj is None:
            self.output_proj = nn.Linear(out.size(1), self.out_channels).to(out.device)
        return self.output_proj(out)


class DegreeEncoder(nn.Module):
    def __init__(self, max_degree=100, embed_dim=128):
        super().__init__()
        self.encoder = nn.Embedding(max_degree + 1, embed_dim)
        self.max_degree = max_degree

    def forward(self, edge_index, num_nodes):
        deg = degree(edge_index[0], num_nodes=num_nodes).long()
        deg = torch.clamp(deg, max=self.max_degree)
        return self.encoder(deg)


class DynamicGraphLearner(nn.Module):
    """
    Learns dynamic graph structure (Anchor-based or k-NN within batch).
    For Phase 3 3H8, we implement a simplified Metric Learning version.
    """

    def __init__(self, in_channels, k=10, epsilon=0.5):
        super().__init__()
        self.k = k
        self.epsilon = epsilon
        self.weight_vector = nn.Parameter(torch.Tensor(in_channels))
        nn.init.uniform_(self.weight_vector)

    def forward(self, x, edge_index):
        # Weighted Cosine Similarity
        # x: [N, D], w: [D]
        x_weighted = x * self.weight_vector

        # We can't compute N*N dense matrix for 1.8M nodes.
        # But in minibatch training (NeighborLoader), N is small (~1000-5000).
        # We assume this is called inside the model forward on the BATCH x.

        # Compute cosine sim
        x_norm = F.normalize(x_weighted, p=2, dim=1)
        sim_matrix = torch.mm(x_norm, x_norm.t())  # [N, N]

        # Prune small values
        mask = sim_matrix > self.epsilon
        # Also enforce k-NN?
        # For simplicity, just epsilon thresholding or masking existing edges.
        # Let's just return edge weights for existing edges to be safe.

        src, dst = edge_index
        edge_weights = sim_matrix[src, dst]

        return edge_weights


class TransformerModel(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=4,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        for _ in range(num_layers):
            self.convs.append(
                TransformerConv(
                    hidden_channels, hidden_channels, heads=2, dropout=dropout
                )
            )
            self.bns.append(BatchNorm(hidden_channels * 2))  # heads=2
        self.output_proj = nn.Linear(hidden_channels * 2, out_channels)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, edge_index, *args, **kwargs):
        x = self.dropout(F.relu(self.input_proj(x)))
        for conv, bn in zip(self.convs, self.bns):
            x = x + self.dropout(F.relu(bn(conv(x, edge_index))))
        return self.output_proj(x)


class EX_TGN_Transformer(nn.Module):
    def __init__(
        self,
        in_channels,
        hidden_channels,
        out_channels,
        num_layers=3,
        dropout=0.3,
        **kwargs,
    ):
        super().__init__()
        heads = int(kwargs.get("heads", 2))
        edge_dim = int(kwargs.get("edge_dim", 8))

        self.input_proj = nn.Linear(in_channels, hidden_channels)
        self.edge_proj = None
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        self.dropout = nn.Dropout(dropout)

        per_head = max(hidden_channels // heads, 1)
        conv_out = per_head * heads
        for i in range(num_layers):
            in_dim = hidden_channels if i == 0 else conv_out
            self.convs.append(
                TransformerConv(
                    in_channels=in_dim,
                    out_channels=per_head,
                    heads=heads,
                    edge_dim=edge_dim,
                    dropout=dropout,
                    beta=True,
                )
            )
            self.bns.append(BatchNorm(conv_out))

        self.output_proj = MLPHead(conv_out, hidden_channels, out_channels, 2, dropout)
        self.expected_edge_dim = edge_dim

    def _prepare_edge_attr(
        self, edge_attr: Optional[torch.Tensor], num_edges: int, device
    ):
        if edge_attr is None:
            return torch.zeros((num_edges, self.expected_edge_dim), device=device)

        if edge_attr.dim() == 1:
            edge_attr = edge_attr.view(-1, 1)

        if edge_attr.size(1) != self.expected_edge_dim:
            if self.edge_proj is None:
                self.edge_proj = nn.Linear(
                    edge_attr.size(1), self.expected_edge_dim
                ).to(device)
            edge_attr = self.edge_proj(edge_attr)

        return edge_attr

    def forward(self, x, edge_index, edge_attr=None, **kwargs):
        x = self.dropout(F.relu(self.input_proj(x)))
        edge_attr = self._prepare_edge_attr(edge_attr, edge_index.size(1), x.device)

        for conv, bn in zip(self.convs, self.bns):
            h = conv(x, edge_index, edge_attr=edge_attr)
            x = x + self.dropout(F.relu(bn(h)))

        return self.output_proj(x)


ARCHITECTURES = {
    "GraphSAGE": GraphSAGEModel,
    "GIN": GINModel,
    "GCN": GCNModel,
    "GraphSAGE_LSTM": GraphSAGE_LSTM,
    "SAGE_LSTM_Model": SAGE_LSTM_Model,
    "GATv2_Edge": GATv2EdgeModel,
    "GAT": GATModel,
    "Transformer": TransformerModel,
    "EX_TGN_Transformer": EX_TGN_Transformer,
    "APPNP": APPNPModel,
    "Zebra": ZebraModel,
    "FullGraphMGD": FullGraphMGDModel,
    "zebra": ZebraModel,
    "fullgraphmgd": FullGraphMGDModel,
    "sage": SAGEConv,
    "rgcn": RGCNModel,
    "gine": GINEModel,
    "signed_gnn": SignedGNNModel,
    "RGCN": RGCNModel,
    "GINE": GINEModel,
    "SignedGNN": SignedGNNModel,
}


# =============================================================================
# Loss Functions
# =============================================================================


class FocalLoss(nn.Module):
    def __init__(self, alpha=0.75, gamma=2.0):
        super().__init__()
        self.alpha = alpha
        self.gamma = gamma

    def forward(self, inputs, targets):
        if inputs.dim() > 1 and inputs.shape[1] == 1:
            inputs = inputs.squeeze(1)
        if targets.dim() > 1 and targets.shape[1] == 1:
            targets = targets.squeeze(1)

        # Handle binary classification with probabilities (e.g. from sigmoid) or logits
        if targets.is_floating_point():
            # Assume inputs are probabilities if in [0,1], but Focal Loss usually works on logits
            # However, if inputs are already probabilities (sigmoid applied), we use BCE
            # BCE Loss: - [y * log(p) + (1-y) * log(1-p)]
            # Focal adds: (1-pt)^gamma
            # If inputs are probs:
            p = inputs
            ce_loss = F.binary_cross_entropy(inputs, targets, reduction="none")
            p_t = p * targets + (1 - p) * (1 - targets)
            loss = ce_loss * ((1 - p_t) ** self.gamma)
            if self.alpha >= 0:
                alpha_t = self.alpha * targets + (1 - self.alpha) * (1 - targets)
                loss = alpha_t * loss
            return loss.mean()
        else:
            # Standard Multi-class Cross Entropy (expecting logits)
            ce_loss = F.cross_entropy(inputs, targets, reduction="none")
            pt = torch.exp(-ce_loss)
            focal_loss = self.alpha * (1 - pt) ** self.gamma * ce_loss
            return focal_loss.mean()


class PULoss(nn.Module):
    """Positive-Unlabeled Learning loss (ported from senior_exp/models.py).

    In fraud detection, label=0 means "unlabeled" (not confirmed negative).
    PU Loss treats these as a mixture of positives and negatives with
    prior probability ``alpha = num_positive / num_unlabeled``.
    """

    def __init__(self, prior_probability: float):
        super().__init__()
        self.alpha = prior_probability

    def forward(self, logits: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        prob_positive = F.softmax(logits, dim=1)[:, 1]
        epsilon = 1e-6
        loss_positive = -torch.log(prob_positive + epsilon)
        loss_negative = -torch.log(1 - prob_positive + epsilon)

        is_labeled_positive = targets == 1
        is_unlabeled = targets == 0

        total_loss = torch.tensor(0.0, device=logits.device)

        if is_labeled_positive.sum() > 0:
            total_loss = total_loss + loss_positive[is_labeled_positive].mean()

        if is_unlabeled.sum() > 0:
            unlabeled_loss = (
                self.alpha * loss_positive[is_unlabeled]
                + (1 - self.alpha) * loss_negative[is_unlabeled]
            ).mean()
            total_loss = total_loss + unlabeled_loss

        return total_loss


class AsymmetricLoss(nn.Module):
    def __init__(self, alpha_fn=3.0, alpha_fp=1.0, gamma=2.0):
        super().__init__()
        self.alpha_fn = alpha_fn
        self.alpha_fp = alpha_fp
        self.gamma = gamma

    def forward(self, inputs, targets):
        probs = F.softmax(inputs, dim=1)
        p_fraud = probs[:, 1]
        fn_loss = (
            -self.alpha_fn
            * targets.float()
            * ((1 - p_fraud) ** self.gamma)
            * torch.log(p_fraud + 1e-8)
        )
        fp_loss = (
            -self.alpha_fp
            * (1 - targets.float())
            * (p_fraud**self.gamma)
            * torch.log(1 - p_fraud + 1e-8)
        )
        return (fn_loss + fp_loss).mean()


class AUCMaxLoss(nn.Module):
    def __init__(self, focal_weight=0.5, num_pairs=100):
        super().__init__()
        self.focal = FocalLoss()
        self.focal_weight = focal_weight
        self.num_pairs = num_pairs

    def forward(self, inputs, targets):
        focal_loss = self.focal(inputs, targets)
        probs = F.softmax(inputs, dim=1)[:, 1]

        # Use nonzero() instead of boolean indexing to avoid CUDA illegal memory access
        pos_indices = (targets == 1).nonzero(as_tuple=True)[0]
        neg_indices = (targets == 0).nonzero(as_tuple=True)[0]

        if len(pos_indices) == 0 or len(neg_indices) == 0:
            return focal_loss

        pos_probs = probs[pos_indices]
        neg_probs = probs[neg_indices]

        # Sample pairs for pairwise ranking loss
        pos_sample_idx = torch.randint(
            0, len(pos_probs), (self.num_pairs,), device=probs.device
        )
        neg_sample_idx = torch.randint(
            0, len(neg_probs), (self.num_pairs,), device=probs.device
        )
        pairwise_loss = torch.clamp(
            1 - (pos_probs[pos_sample_idx] - neg_probs[neg_sample_idx]), min=0
        ).mean()

        return self.focal_weight * focal_loss + (1 - self.focal_weight) * pairwise_loss


class ContrastiveLoss(nn.Module):
    def __init__(self, temperature=0.1, weight=0.3):
        super().__init__()
        self.temperature = temperature
        self.weight = weight
        self.focal = FocalLoss()

    def forward(self, embeddings, inputs, targets):
        focal_loss = self.focal(inputs, targets)
        embeddings = F.normalize(embeddings, dim=1)
        sim_matrix = torch.mm(embeddings, embeddings.t()) / self.temperature
        labels_equal = targets.unsqueeze(0) == targets.unsqueeze(1)
        mask = torch.eye(len(targets), dtype=torch.bool, device=targets.device)
        labels_equal = labels_equal & ~mask

        if labels_equal.sum() == 0:
            return focal_loss

        exp_sim = torch.exp(sim_matrix)
        exp_sim = exp_sim * ~mask
        contrastive_loss = -torch.log(
            (exp_sim * labels_equal.float()).sum(1) / (exp_sim.sum(1) + 1e-8) + 1e-8
        ).mean()

        return focal_loss + self.weight * contrastive_loss


class PU_Loss(nn.Module):
    """Positive-Unlabeled loss (senior_exp port)."""

    def __init__(self, prior_probability: float):
        super().__init__()
        self.alpha = prior_probability

    def forward(self, logits: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        prob_positive = F.softmax(logits, dim=1)[:, 1]
        epsilon = 1e-6
        loss_positive = -torch.log(prob_positive + epsilon)
        loss_negative = -torch.log(1 - prob_positive + epsilon)

        is_labeled_positive = targets == 1
        is_unlabeled = targets == 0

        total_loss = torch.tensor(0.0, device=logits.device)
        if is_labeled_positive.sum() > 0:
            total_loss = total_loss + loss_positive[is_labeled_positive].mean()
        if is_unlabeled.sum() > 0:
            unlabeled_loss = (
                self.alpha * loss_positive[is_unlabeled]
                + (1 - self.alpha) * loss_negative[is_unlabeled]
            ).mean()
            total_loss = total_loss + unlabeled_loss

        return total_loss


class nnPU_Loss(nn.Module):
    """Non-negative PU loss (senior_exp port)."""

    def __init__(self, prior_probability: float, beta: float = 0.0, gamma: float = 1.0):
        super().__init__()
        self.pi = prior_probability
        self.beta = beta
        self.gamma = gamma

    def forward(self, logits: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        probs = torch.sigmoid(logits[:, 1] - logits[:, 0])

        pos_mask = (targets == 1).float()
        unlabeled_mask = (targets == 0).float()

        loss_pos = -torch.log(probs + 1e-7)
        loss_neg = -torch.log(1 - probs + 1e-7)

        n_pos = pos_mask.sum()
        n_unlabeled = unlabeled_mask.sum()
        if n_pos == 0 or n_unlabeled == 0:
            return F.cross_entropy(logits, targets)

        risk_pos = (pos_mask * loss_pos).sum() / n_pos
        risk_unlabeled_neg = (unlabeled_mask * loss_neg).sum() / n_unlabeled
        risk_pos_neg = (pos_mask * loss_neg).sum() / n_pos

        risk_neg = risk_unlabeled_neg - self.pi * risk_pos_neg
        if risk_neg < self.beta:
            return self.gamma * risk_pos

        return self.pi * risk_pos + risk_neg


# =============================================================================
# Training Utilities
# =============================================================================


def find_best_threshold(
    labels: np.ndarray,
    probs: np.ndarray,
    thresholds: Optional[List[float]] = None,
) -> Tuple[float, float]:
    metric_cfg = get_runtime_section("senior_metric_config")
    strategy = cfg_str(metric_cfg, "threshold_strategy", "f1").lower()
    if strategy == "f1":
        default_start = cfg_float(metric_cfg, "threshold_grid_start", 0.1)
        default_end = cfg_float(metric_cfg, "threshold_grid_end", 1.0)
        default_step = cfg_float(metric_cfg, "threshold_grid_step", 0.05)
        default_thresholds = list(np.arange(default_start, default_end, default_step))
    else:
        default_thresholds = list(np.arange(0.05, 0.96, 0.01))

    best_f1, best_thresh = 0.0, 0.5
    thresh_list = thresholds or default_thresholds
    for thresh in thresh_list:
        preds = (probs >= thresh).astype(int)
        f1 = float(f1_score(labels, preds, zero_division="warn"))
        if f1 > best_f1:
            best_f1, best_thresh = f1, thresh
    return float(best_thresh), float(best_f1)


def evaluate(
    model: nn.Module, loader: NeighborLoader, device: torch.device
) -> Tuple[np.ndarray, np.ndarray]:
    model.eval()
    all_probs, all_labels = [], []
    import inspect

    sig = inspect.signature(model.forward)
    with torch.no_grad():
        for batch in loader:
            batch = batch.to(device)
            f_kwargs = {}
            if (
                "edge_attr" in sig.parameters
                and hasattr(batch, "edge_attr")
                and batch.edge_attr is not None
            ):
                f_kwargs["edge_attr"] = batch.edge_attr
            if (
                "edge_type" in sig.parameters
                and hasattr(batch, "edge_type")
                and batch.edge_type is not None
            ):
                f_kwargs["edge_type"] = batch.edge_type
            if "velocity" in sig.parameters and hasattr(batch, "x"):
                # Heuristic for velocity access if model expects it
                v_idx = getattr(model, "velocity_idx", 38)
                f_kwargs["velocity"] = batch.x[:, v_idx : v_idx + 3]

            # Generic call with supported kwargs
            out = model(batch.x, batch.edge_index, **f_kwargs)[: batch.batch_size]

            if out.dim() == 1 or out.shape[1] == 1:
                probs = out.squeeze() if out.dim() > 1 else out
            else:
                probs = F.softmax(out, dim=1)[:, 1]

            all_probs.append(probs.cpu())
            all_labels.append(batch.y[: batch.batch_size].cpu())
    return torch.cat(all_probs).numpy(), torch.cat(all_labels).numpy()


def create_loaders(
    data,
    num_layers: int,
    batch_size: int = 4096,
    num_workers: int = DEFAULT_NUM_WORKERS,
    num_neighbors: Optional[List[int]] = None,
    filter_per_worker: Optional[bool] = None,
) -> Tuple[NeighborLoader, NeighborLoader, NeighborLoader]:
    extra_kwargs: Dict[str, Any] = {}
    if filter_per_worker is not None:
        extra_kwargs["filter_per_worker"] = filter_per_worker

    return create_neighbor_loaders(
        data,
        num_layers=num_layers,
        num_neighbors=num_neighbors,
        batch_size=batch_size,
        num_workers=num_workers,
        **extra_kwargs,
    )


def _torch_load_compat(path: str, map_location=None):
    try:
        return torch.load(path, map_location=map_location, weights_only=False)
    except TypeError:
        return torch.load(path, map_location=map_location)


def _save_training_checkpoint(
    checkpoint_path: str,
    model: nn.Module,
    optimizer: Adam,
    epoch: int,
    best_val_f1: float,
    patience_counter: int,
    best_epoch: Optional[int] = None,
):
    payload = {
        "format_version": 2,
        "epoch": int(epoch),
        "best_val_f1": float(best_val_f1),
        "patience_counter": int(patience_counter),
        "best_epoch": int(epoch if best_epoch is None else best_epoch),
        "model_state_dict": model.state_dict(),
        "optimizer_state_dict": optimizer.state_dict(),
    }
    torch.save(payload, checkpoint_path)


def _try_resume_training(
    checkpoint_path: str,
    model: nn.Module,
    optimizer: Adam,
    device: torch.device,
    model_name: str,
) -> Tuple[int, float, int, int]:
    if not os.path.exists(checkpoint_path):
        return 0, -1.0, 0, -1

    try:
        payload = _torch_load_compat(checkpoint_path, map_location=device)
    except Exception as e:
        print(
            f"  [{model_name}] Failed to load checkpoint for resume: {e}. Starting from epoch 0.",
            flush=True,
        )
        return 0, -1.0, 0, -1

    if isinstance(payload, dict) and "model_state_dict" in payload:
        model.load_state_dict(payload["model_state_dict"])
        opt_state = payload.get("optimizer_state_dict")
        if isinstance(opt_state, dict):
            try:
                optimizer.load_state_dict(opt_state)
            except Exception as e:
                print(
                    f"  [{model_name}] Optimizer state not restored ({e}); continue with fresh optimizer.",
                    flush=True,
                )
        start_epoch = int(payload.get("epoch", -1)) + 1
        best_val_f1 = float(payload.get("best_val_f1", -1.0))
        patience_counter = int(payload.get("patience_counter", 0))
        best_epoch = int(payload.get("best_epoch", start_epoch - 1))
        print(
            f"  [{model_name}] Resume from checkpoint: epoch={start_epoch}, best_val_f1={best_val_f1:.4f}",
            flush=True,
        )
        return start_epoch, best_val_f1, patience_counter, best_epoch

    if isinstance(payload, dict):
        try:
            model.load_state_dict(payload)
            print(
                f"  [{model_name}] Loaded legacy model-only checkpoint. Optimizer/epoch state unavailable.",
                flush=True,
            )
            return 0, -1.0, 0, -1
        except Exception:
            pass

    print(
        f"  [{model_name}] Unsupported checkpoint format. Starting from epoch 0.",
        flush=True,
    )
    return 0, -1.0, 0, -1


def _load_best_weights(checkpoint_path: str, model: nn.Module, device: torch.device):
    payload = _torch_load_compat(checkpoint_path, map_location=device)
    if isinstance(payload, dict) and "model_state_dict" in payload:
        model.load_state_dict(payload["model_state_dict"])
        return
    model.load_state_dict(payload)


def train_single_model(
    model: nn.Module,
    train_loader: NeighborLoader,
    val_loader: NeighborLoader,
    device: torch.device,
    model_name: str,
    checkpoint_path: str,
    criterion: nn.Module,
    max_epochs: int = 300,
    patience: int = 10,
    lr: float = 0.005,
    eval_every_epochs: int = 1,
    progress_callback: Optional[
        Callable
    ] = None,  # Called each epoch: (epoch, total, val_f1, loss, **metrics)
    **kwargs,  # Accept augmentation args
) -> Tuple[nn.Module, float]:
    import inspect

    optimizer = Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    forward_sig = inspect.signature(model.forward)
    start_epoch, best_val_f1, patience_counter, best_epoch = _try_resume_training(
        checkpoint_path, model, optimizer, device, model_name
    )
    if eval_every_epochs < 1:
        raise ValueError("eval_every_epochs must be >= 1")

    # Augmentation configs
    drop_node = kwargs.get("drop_node_rate", 0.0)
    drop_edge = kwargs.get("drop_edge_rate", 0.0)
    feature_noise = kwargs.get("feature_noise", 0.0)
    consistency_reg = kwargs.get("consistency_reg", 0.0)
    entropy_min = kwargs.get("entropy_min", 0.0)
    pseudo_label = bool(kwargs.get("pseudo_label", False))
    pseudo_threshold = float(kwargs.get("pseudo_threshold", 0.9))
    pseudo_weight = float(kwargs.get("pseudo_weight", 0.5))
    temporal_weighting = bool(kwargs.get("temporal_weighting", False))
    temporal_alpha = float(kwargs.get("temporal_alpha", 1.0))
    domain_adapt = bool(kwargs.get("domain_adapt", False))
    domain_weight = float(kwargs.get("domain_weight", 0.1))
    edge_weighting = kwargs.get("edge_weighting", None)

    for epoch in range(start_epoch, max_epochs):
        model.train()
        epoch_loss = 0.0
        num_batches = 0
        for batch in train_loader:
            batch = batch.to(device)

            # --- Pre-processing for edge weights (Similarity Gating) ---
            if edge_weighting == "similarity":
                # Compute cosine similarity between source and target nodes
                # edge_index: [2, E], x: [N, D]
                src, dst = batch.edge_index
                x_src = batch.x[src]
                x_dst = batch.x[dst]

                # Cosine similarity: dot(A, B) / (norm(A) * norm(B))
                # Add epsilon to avoid div by zero
                cos_sim = F.cosine_similarity(x_src, x_dst, dim=1)

                # Apply as edge_weight (used by GCN/SAGE if supported)
                # For GAT, it might be used differently, but here we target GCN/SAGE
                # SAGEConv doesn't natively take edge_weight in all versions, but GCN does.
                # If model is SAGE, we might need a custom conv or pre-multiplication.
                # However, PyG SAGEConv *does* support edge_weight if aggr is add/mean?
                # Actually, standard SAGEConv forward is (x, edge_index).
                # To support weighting, we check if the conv layer accepts it or we use GCN.

                # For this implementation, we will assign it to batch.edge_weight
                # and pass it to model. If model ignores it, it's a limitation of the model class.
                batch.edge_weight = cos_sim

                # For GAT-based models (like GATv2EdgeModel), use edge_weight as edge_attr
                if not hasattr(batch, "edge_attr") or batch.edge_attr is None:
                    batch.edge_attr = cos_sim.view(-1, 1)

            # 1. Feature Noise
            if feature_noise > 0:
                noise = torch.randn_like(batch.x) * feature_noise
                batch.x = (batch.x + noise).contiguous()

            # 2. Drop Node (Masking features of dropped nodes)
            if drop_node > 0:
                mask = torch.rand(batch.x.size(0), device=device) > drop_node
                # Mask out features (set to zero) or use custom mask logic
                # Simpler approach: zero out features of dropped nodes
                batch.x = batch.x * mask.unsqueeze(1)

            # 3. Drop Edge (Randomly remove edges)
            if drop_edge > 0:
                edge_mask = (
                    torch.rand(batch.edge_index.size(1), device=device) > drop_edge
                )
                batch.edge_index = batch.edge_index[:, edge_mask]
                if hasattr(batch, "edge_attr") and batch.edge_attr is not None:
                    batch.edge_attr = batch.edge_attr[edge_mask]

            optimizer.zero_grad()

            f_kwargs = {}
            if hasattr(batch, "edge_attr") and batch.edge_attr is not None:
                f_kwargs["edge_attr"] = batch.edge_attr
            if hasattr(batch, "edge_type") and batch.edge_type is not None:
                f_kwargs["edge_type"] = batch.edge_type

            call_kwargs = {
                k: v for k, v in f_kwargs.items() if k in forward_sig.parameters
            }
            full_out = model(batch.x, batch.edge_index, **call_kwargs)

            out = full_out[: batch.batch_size]

            # 4. Consistency Regularization (3F5)
            aux_loss = 0.0
            if consistency_reg > 0:
                out2 = model(batch.x, batch.edge_index, **call_kwargs)[
                    : batch.batch_size
                ]
                # MSE Consistency Loss
                aux_loss += F.mse_loss(out, out2) * consistency_reg

            base_loss = criterion(out, batch.y[: batch.batch_size])
            if temporal_weighting and hasattr(batch, "node_first_txn_time"):
                times = batch.node_first_txn_time[: batch.batch_size]
                t_min = float(times.min().item())
                t_max = float(times.max().item())
                denom = max(t_max - t_min, 1e-6)
                norm = (times - t_min) / denom
                scale = 1.0 + temporal_alpha * (norm.mean().item() - 0.5)
                base_loss = base_loss * float(scale)

            loss = base_loss + aux_loss

            # 5. Entropy Minimization (3F6)
            if entropy_min > 0:
                probs = torch.sigmoid(out)
                entropy = -(
                    probs * torch.log(probs + 1e-6)
                    + (1 - probs) * torch.log(1 - probs + 1e-6)
                )
                loss += entropy.mean() * entropy_min

            if pseudo_label and hasattr(batch, "train_mask"):
                non_train_mask = ~batch.train_mask
                if non_train_mask.any():
                    with torch.no_grad():
                        if full_out.dim() > 1 and full_out.shape[1] > 1:
                            pseudo_probs = F.softmax(full_out[non_train_mask], dim=1)[
                                :, 1
                            ]
                            pseudo_logits = full_out[non_train_mask]
                        else:
                            pseudo_probs = torch.sigmoid(
                                full_out[non_train_mask].squeeze()
                            )
                            pseudo_logits = full_out[non_train_mask]
                        pseudo_keep = pseudo_probs >= pseudo_threshold
                    if pseudo_keep.any():
                        pseudo_labels = torch.ones(
                            pseudo_logits[pseudo_keep].shape[0],
                            device=pseudo_logits.device,
                            dtype=torch.long,
                        )
                        loss += (
                            criterion(pseudo_logits[pseudo_keep], pseudo_labels)
                            * pseudo_weight
                        )

            if domain_adapt and hasattr(batch, "train_mask"):
                train_mask = batch.train_mask
                other_mask = ~train_mask
                if train_mask.any() and other_mask.any():
                    train_embed = full_out[train_mask]
                    other_embed = full_out[other_mask]
                    mean_diff = train_embed.mean(dim=0) - other_embed.mean(dim=0)
                    loss += (mean_diff.pow(2).mean()) * domain_weight

            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
            num_batches += 1

        avg_loss = epoch_loss / max(num_batches, 1)
        should_eval = (epoch + 1) % eval_every_epochs == 0 or epoch == max_epochs - 1
        val_f1 = best_val_f1 if best_val_f1 >= 0 else 0.0

        if should_eval:
            val_probs, val_labels = evaluate(model, val_loader, device)
            _, val_f1 = find_best_threshold(val_labels, val_probs)

            if val_f1 > best_val_f1:
                best_val_f1, patience_counter, best_epoch = val_f1, 0, epoch
                _save_training_checkpoint(
                    checkpoint_path,
                    model,
                    optimizer,
                    epoch,
                    best_val_f1,
                    patience_counter,
                    best_epoch,
                )
            else:
                patience_counter += 1

        # --- P1: Built-in progress callback ---
        if progress_callback is not None:
            try:
                progress_callback(
                    epoch + 1, max_epochs, val_f1, avg_loss, train_loss=avg_loss
                )
            except Exception:
                pass  # Never let callback failure break training

        if (epoch + 1) % 20 == 0:
            print(
                f"\n  {model_name} Epoch {epoch + 1}: Val F1={val_f1:.4f} (eval_every={eval_every_epochs})"
            )
    if os.path.exists(checkpoint_path):
        _load_best_weights(checkpoint_path, model, device)
    if best_epoch >= 0:
        print(
            f"  [{model_name}] Best checkpoint from epoch {best_epoch + 1} with Val F1={best_val_f1:.4f}",
            flush=True,
        )
    return model, best_val_f1


def train_ensemble(
    data,
    output_dir: str,
    architectures: List[str] = ["GraphSAGE", "GIN", "GCN"],
    criterion: Optional[nn.Module] = None,
    hidden_dim: int = 128,
    num_layers: int = 4,
    dropout: float = 0.3,
    max_epochs: int = 300,
    patience: int = 10,
    lr: float = 0.005,
    batch_size: int = 4096,
    num_workers: Optional[int] = None,
    filter_per_worker: Optional[bool] = None,
    thresholds: Optional[List[float]] = None,
    ensemble_method: str = "mean",
    **kwargs,  # Accept extra args like drop_node_rate
):
    """
    Train an ensemble of models.
    """
    os.makedirs(output_dir, exist_ok=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Pass kwargs (augmentations) to loader creation if needed,
    # but create_loaders currently doesn't support them directly in signature.
    # We might need to handle augmentation inside train_single_model or custom collate.
    # For DropNode, usually it's done in the loader or dataset transform.

    # Check if we need to apply transforms to data before creating loaders
    # or pass args to loaders. For now, let's assume create_loaders might need update
    # OR we use them in train_single_model loop.

    # Actually, DropNode/Edge usually happens per batch or epoch.
    # Let's pass these kwargs to train_single_model

    if criterion is None:
        criterion = FocalLoss()

    train_loader, val_loader, test_loader = create_loaders(
        data,
        num_layers,
        batch_size,
        num_workers=num_workers or DEFAULT_NUM_WORKERS,
        filter_per_worker=filter_per_worker,
    )
    os.makedirs(os.path.join(output_dir, "checkpoints"), exist_ok=True)

    trained_models = []
    all_val_probs = []
    all_test_probs = []
    val_f1s = []
    val_labels = None
    test_labels = None

    for arch_name in architectures:
        # Handle both class objects and string keys
        if isinstance(arch_name, type):
            ModelClass = arch_name
            arch_name_str = arch_name.__name__
        else:
            ModelClass = ARCHITECTURES[arch_name]
            arch_name_str = str(arch_name)
        print(f"\nTraining {arch_name_str}...")
        model = ModelClass(
            data.x.shape[1], hidden_dim, 2, num_layers, dropout, **kwargs
        ).to(device)
        checkpoint_path = os.path.join(
            output_dir, "checkpoints", f"{arch_name_str.lower()}_best.pt"
        )
        model, val_f1_single = train_single_model(
            model,
            train_loader,
            val_loader,
            device,
            arch_name_str,
            checkpoint_path,
            criterion,
            max_epochs,
            patience,
            lr,
            **kwargs,  # Pass augmentation args here
        )
        trained_models.append(model)
        val_f1s.append(val_f1_single)

        # Evaluate on VAL (for ensemble threshold tuning)
        v_probs, v_labels = evaluate(model, val_loader, device)
        all_val_probs.append(v_probs)
        val_labels = v_labels

        # Evaluate on TEST (for final metrics)
        t_probs, t_labels = evaluate(model, test_loader, device)
        all_test_probs.append(t_probs)
        test_labels = t_labels

        print(f"  {arch_name_str} Best Val F1: {val_f1_single:.4f}")

    ensemble_val_probs = np.mean(all_val_probs, axis=0)
    ensemble_test_probs = np.mean(all_test_probs, axis=0)
    method = (kwargs.get("ensemble_method") or ensemble_method or "mean").lower()
    if method == "weighted":
        weights = np.array(val_f1s, dtype=np.float32)
        if weights.sum() > 0:
            weights = weights / weights.sum()
            ensemble_val_probs = np.sum(
                np.stack(all_val_probs, axis=0) * weights[:, None], axis=0
            )
            ensemble_test_probs = np.sum(
                np.stack(all_test_probs, axis=0) * weights[:, None], axis=0
            )
    elif method == "stacking":
        if val_labels is not None:
            try:
                X_val = np.stack(all_val_probs, axis=1)
                X_test = np.stack(all_test_probs, axis=1)
                clf = LogisticRegression(max_iter=1000)
                clf.fit(X_val, val_labels)
                ensemble_val_probs = clf.predict_proba(X_val)[:, 1]
                ensemble_test_probs = clf.predict_proba(X_test)[:, 1]
            except Exception:
                pass

    # Tune threshold on VAL labels only (Correct ML practice)
    if val_labels is None:
        raise ValueError("val_labels must be available for ensemble threshold tuning")
    best_thresh, ensemble_val_f1 = find_best_threshold(
        val_labels, ensemble_val_probs, thresholds
    )
    print(
        f"\nEnsemble Best Threshold (tuned on Val): {best_thresh:.2f} (Val F1: {ensemble_val_f1:.4f})"
    )

    # Apply to TEST
    test_preds = (ensemble_test_probs >= best_thresh).astype(int)
    test_f1 = float(f1_score(test_labels, test_preds, zero_division="warn"))
    test_auc = float(roc_auc_score(test_labels, ensemble_test_probs))

    # Evaluate on Phase 2 Private Leaderboard
    p2_results = evaluate_offline_leaderboard(
        trained_models, data, device, method, val_f1s
    )

    return {
        "test_f1": test_f1,
        "test_precision": float(
            precision_score(test_labels, test_preds, zero_division="warn")
        ),
        "test_recall": float(
            recall_score(test_labels, test_preds, zero_division="warn")
        ),
        "test_auc": test_auc,
        "best_threshold": float(best_thresh),
        "ensemble_val_f1": float(ensemble_val_f1),
        **p2_results,
    }


def evaluate_offline_leaderboard(
    models: List[nn.Module],
    data,
    device: torch.device,
    ensemble_method: str = "mean",
    val_f1s: Optional[List[float]] = None,
) -> Dict[str, float]:
    """
    Evaluate ensemble on Phase 2 Private Leaderboard Ground Truth.
    """
    if not hasattr(data, "acct_to_node"):
        # print("[Eval] data.acct_to_node missing. Skipping offline leaderboard eval.")
        return {}

    # Locate GT file relative to this script
    # SubProject/Phase3/preprocess_lib/train_utils.py -> SubProject/esun_data/Phase3/acct_predi.csv
    base_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    gt_path = os.path.join(base_dir, "esun_data", "Phase3", "acct_predi.csv")

    if not os.path.exists(gt_path):
        # Fallback: check one level up if base_dir calculation was wrong
        gt_path = os.path.join(
            os.path.dirname(base_dir),
            "esun_data",
            "Phase3",
            "acct_predi.csv",
        )
        if not os.path.exists(gt_path):
            print(f"[Eval] GT file not found at {gt_path}. Skipping.")
            return {}

    try:
        import pandas as pd  # pyright: ignore[reportMissingImports]

        df_gt = pd.read_csv(gt_path)
    except Exception as e:
        print(f"[Eval] Error loading GT: {e}")
        return {}

    # Map accounts to indices
    valid_indices = []
    valid_labels = []

    acct_to_node = data.acct_to_node
    # Convert to set for faster lookup? acct_to_node is dict, O(1)

    # We must ensure we predict on ALL test accounts, filling missing with 0 if necessary
    # But here we only evaluate on nodes present in the graph.

    for _, row in df_gt.iterrows():
        acct = row["acct"]
        label = int(row["label"])
        if acct in acct_to_node:
            valid_indices.append(acct_to_node[acct])
            valid_labels.append(label)

    if not valid_indices:
        print("[Eval] No Phase 2 GT accounts found in current graph.")
        return {}

    print(f"\n[Eval] Evaluating on {len(valid_indices)} Phase 2 GT accounts...")

    # Create loader for these nodes
    input_nodes = torch.tensor(valid_indices, dtype=torch.long)
    temporal_loader_kwargs: Dict[str, Any] = {}
    if hasattr(data, "edge_time") and data.edge_time is not None:
        temporal_loader_kwargs = {
            "time_attr": "edge_time",
            "temporal_strategy": "last",
            "is_sorted": False,
        }
    loader = NeighborLoader(
        data,
        num_neighbors=[10, 10, 5, 5],
        input_nodes=input_nodes,
        batch_size=4096,
        shuffle=False,
        num_workers=4,
        **temporal_loader_kwargs,
    )

    # Run inference
    all_probs = []
    for model in models:
        probs, _ = evaluate(model, loader, device)
        all_probs.append(probs)

    # Ensemble
    if ensemble_method == "weighted" and val_f1s is not None:
        weights = np.array(val_f1s)
        if weights.sum() > 0:
            weights = weights / weights.sum()
            ensemble_probs = np.sum(np.stack(all_probs) * weights[:, None], axis=0)
        else:
            ensemble_probs = np.mean(all_probs, axis=0)
    else:
        ensemble_probs = np.mean(all_probs, axis=0)

    # Metrics
    y_true = np.array(valid_labels)
    best_thresh, best_f1 = find_best_threshold(y_true, ensemble_probs)
    try:
        auc = roc_auc_score(y_true, ensemble_probs)
    except:
        auc = 0.0

    print(
        f"[Eval] Phase 2 Leaderboard: F1={best_f1:.4f} (Thresh={best_thresh:.2f}), AUC={auc:.4f}"
    )

    return {
        "p2_f1": float(best_f1),
        "p2_auc": float(auc),
        "p2_thresh": float(best_thresh),
    }


def evaluate_predi_split(
    models: List[nn.Module],
    data,
    device: torch.device,
    split: str = "all",
    ensemble_method: str = "mean",
    val_f1s: Optional[List[float]] = None,
) -> Dict[str, float]:
    """Evaluate ensemble on acct_predi split(s): P1, P2, or both."""
    if not hasattr(data, "acct_to_node"):
        return {}

    base_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    predi_path = os.path.join(base_dir, "esun_data", "Phase3", "acct_predi.csv")

    if not os.path.exists(predi_path):
        predi_path = os.path.join(
            os.path.dirname(base_dir), "esun_data", "Phase3", "acct_predi.csv"
        )
        if not os.path.exists(predi_path):
            print(f"[Eval] Predi file not found at {predi_path}. Skipping.")
            return {}

    try:
        import pandas as pd  # pyright: ignore[reportMissingImports]

        df_predi = pd.read_csv(predi_path)
    except Exception as e:
        print(f"[Eval] Error loading predi file: {e}")
        return {}

    if "acct" not in df_predi.columns or "label" not in df_predi.columns:
        print("[Eval] acct_predi.csv missing required columns. Skipping.")
        return {}

    if "leaderboard" not in df_predi.columns:
        print("[Eval] acct_predi.csv missing leaderboard column. Skipping.")
        return {}

    split = split.lower()
    if split not in {"p1", "p2", "all"}:
        print(f"[Eval] Unknown split '{split}'. Expected 'p1' | 'p2' | 'all'.")
        return {}

    leaderboard_series = df_predi["leaderboard"]
    leaderboard_str = leaderboard_series.astype(str).str.strip()
    p1_mask = leaderboard_series.isna() | (leaderboard_str == "")
    p2_mask = ~p1_mask

    acct_to_node = data.acct_to_node

    def _evaluate_subset(df_subset, prefix: str, pretty_name: str) -> Dict[str, float]:
        valid_indices = []
        valid_labels = []

        for _, row in df_subset.iterrows():
            acct = row["acct"]
            label = int(row["label"])
            if acct in acct_to_node:
                valid_indices.append(acct_to_node[acct])
                valid_labels.append(label)

        if not valid_indices:
            print(f"[Eval] No {pretty_name} accounts found in current graph.")
            return {}

        input_nodes = torch.tensor(valid_indices, dtype=torch.long)
        temporal_loader_kwargs: Dict[str, Any] = {}
        if hasattr(data, "edge_time") and data.edge_time is not None:
            temporal_loader_kwargs = {
                "time_attr": "edge_time",
                "temporal_strategy": "last",
                "is_sorted": False,
            }
        loader = NeighborLoader(
            data,
            num_neighbors=[10, 10, 5, 5],
            input_nodes=input_nodes,
            batch_size=4096,
            shuffle=False,
            num_workers=4,
            **temporal_loader_kwargs,
        )

        all_probs = []
        for model in models:
            probs, _ = evaluate(model, loader, device)
            all_probs.append(probs)

        if (
            ensemble_method == "weighted"
            and val_f1s is not None
            and len(val_f1s) == len(all_probs)
        ):
            weights = np.array(val_f1s)
            if weights.sum() > 0:
                weights = weights / weights.sum()
                ensemble_probs = np.sum(np.stack(all_probs) * weights[:, None], axis=0)
            else:
                ensemble_probs = np.mean(all_probs, axis=0)
        else:
            ensemble_probs = np.mean(all_probs, axis=0)

        y_true = np.array(valid_labels)
        best_thresh, best_f1 = find_best_threshold(y_true, ensemble_probs)
        try:
            auc = roc_auc_score(y_true, ensemble_probs)
        except Exception:
            auc = 0.0

        print(
            f"[Eval] {pretty_name} Split: F1={best_f1:.4f} "
            f"(Thresh={best_thresh:.2f}), AUC={auc:.4f}"
        )
        return {
            f"{prefix}_f1": float(best_f1),
            f"{prefix}_auc": float(auc),
            f"{prefix}_thresh": float(best_thresh),
        }

    results = {}
    if split in {"p1", "all"}:
        results.update(_evaluate_subset(df_predi[p1_mask], "p1", "P1"))
    if split in {"p2", "all"}:
        results.update(_evaluate_subset(df_predi[p2_mask], "p2", "P2"))

    if not results:
        print("[Eval] No matching accounts available for requested predi split eval.")
    return results


def train_curriculum_model(
    model: nn.Module,
    train_loader_warmup,
    train_loader_main,
    val_loader,
    device: torch.device,
    model_name: str,
    checkpoint_path: str,
    criterion: nn.Module,
    max_epochs: int = 300,
    patience: int = 10,
    lr: float = 0.005,
    warmup_weight: float = 0.5,
    reporter=None,
) -> Tuple[nn.Module, float]:
    """Train with 2-stage curriculum: warmup features then main features per epoch."""
    optimizer = Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    start_epoch, best_val_f1, patience_counter, best_epoch = _try_resume_training(
        checkpoint_path, model, optimizer, device, model_name
    )

    for epoch in range(start_epoch, max_epochs):
        model.train()

        stage_a_loss_sum = 0.0
        stage_a_steps = 0
        for batch in train_loader_warmup:
            batch = batch.to(device)
            optimizer.zero_grad()

            f_kwargs = {}
            if hasattr(batch, "edge_attr") and batch.edge_attr is not None:
                f_kwargs["edge_attr"] = batch.edge_attr
            if hasattr(batch, "edge_type") and batch.edge_type is not None:
                f_kwargs["edge_type"] = batch.edge_type

            if isinstance(
                model,
                (GATv2EdgeModel, GINEModel, RGCNModel, SignedGNNModel, ZebraModel),
            ):
                full_out = model(batch.x, batch.edge_index, **f_kwargs)
            else:
                full_out = model(batch.x, batch.edge_index)

            out = full_out[: batch.batch_size]
            loss = criterion(out, batch.y[: batch.batch_size]) * warmup_weight
            loss.backward()
            optimizer.step()

            stage_a_loss_sum += float(loss.detach().item())
            stage_a_steps += 1

        stage_b_loss_sum = 0.0
        stage_b_steps = 0
        for batch in train_loader_main:
            batch = batch.to(device)
            optimizer.zero_grad()

            f_kwargs = {}
            if hasattr(batch, "edge_attr") and batch.edge_attr is not None:
                f_kwargs["edge_attr"] = batch.edge_attr
            if hasattr(batch, "edge_type") and batch.edge_type is not None:
                f_kwargs["edge_type"] = batch.edge_type

            if isinstance(
                model,
                (GATv2EdgeModel, GINEModel, RGCNModel, SignedGNNModel, ZebraModel),
            ):
                full_out = model(batch.x, batch.edge_index, **f_kwargs)
            else:
                full_out = model(batch.x, batch.edge_index)

            out = full_out[: batch.batch_size]
            loss = criterion(out, batch.y[: batch.batch_size])
            loss.backward()
            optimizer.step()

            stage_b_loss_sum += float(loss.detach().item())
            stage_b_steps += 1

        stage_a_loss = stage_a_loss_sum / max(stage_a_steps, 1)
        stage_b_loss = stage_b_loss_sum / max(stage_b_steps, 1)
        combined_loss = stage_a_loss + stage_b_loss

        val_probs, val_labels = evaluate(model, val_loader, device)
        _, val_f1 = find_best_threshold(val_labels, val_probs)

        if reporter is not None:
            reporter.update(epoch + 1, max_epochs, val_f1, combined_loss)

        print(
            f"\n  {model_name} Epoch {epoch + 1}: "
            f"StageA Loss={stage_a_loss:.6f}, "
            f"StageB Loss={stage_b_loss:.6f}, "
            f"Val F1={val_f1:.4f}, Combined Loss={combined_loss:.6f}",
            flush=True,
        )

        if val_f1 > best_val_f1:
            best_val_f1, patience_counter, best_epoch = val_f1, 0, epoch
            _save_training_checkpoint(
                checkpoint_path,
                model,
                optimizer,
                epoch,
                best_val_f1,
                patience_counter,
                best_epoch,
            )
        else:
            patience_counter += 1

    if os.path.exists(checkpoint_path):
        _load_best_weights(checkpoint_path, model, device)
    if best_epoch >= 0:
        print(
            f"  [{model_name}] Best checkpoint from epoch {best_epoch + 1} with Val F1={best_val_f1:.4f}",
            flush=True,
        )
    return model, best_val_f1


def save_results(output_dir: str, results: Dict[str, Any], experiment_name: str):
    def _read_peak_memory_mb() -> Optional[float]:
        resource_path = os.path.join(output_dir, "resource_usage.json")
        if not os.path.exists(resource_path):
            return None
        try:
            with open(resource_path, "r", encoding="utf-8") as f:
                usage = json.load(f)
            peak = usage.get("peak_memory_mb")
            if isinstance(peak, (int, float)):
                return float(peak)
        except Exception:
            return None
        return None

    peak_memory_mb = _read_peak_memory_mb()
    if peak_memory_mb is not None:
        results["peak_memory_mb"] = peak_memory_mb

    runtime_meta = results.get("runtime_meta")
    if not isinstance(runtime_meta, dict):
        runtime_meta = {}

    if "optimizer_params" not in runtime_meta:
        opt = results.get("optimizer_params")
        if isinstance(opt, dict):
            runtime_meta["optimizer_params"] = opt
        else:
            lr = results.get("lr")
            wd = results.get("weight_decay")
            if isinstance(lr, (int, float)):
                runtime_meta["optimizer_params"] = {
                    "lr": float(lr),
                    "weight_decay": float(wd) if isinstance(wd, (int, float)) else None,
                }

    for src_key, dst_key in (
        ("features", "features"),
        ("model_variant", "model_arch"),
        ("loss_type", "loss_type"),
        ("batch_size", "batch_size"),
        ("eval_batch_size", "eval_batch_size"),
        ("num_neighbors", "num_neighbors"),
        ("num_layers", "num_layers"),
        ("temporal_strategy", "temporal_strategy"),
        ("edge_mask_mode", "edge_mask_mode"),
        ("structure_path", "structure_path"),
        ("pu_prior", "pu_prior"),
    ):
        if dst_key not in runtime_meta and src_key in results:
            runtime_meta[dst_key] = results[src_key]

    results["runtime_meta"] = runtime_meta
    results["experiment"] = experiment_name
    os.makedirs(os.path.join(output_dir, "outputs"), exist_ok=True)
    with open(os.path.join(output_dir, "outputs", "results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n\n{'=' * 60}")
    print(f"\nResults: F1={results['test_f1']:.4f}, AUC={results['test_auc']:.4f}")
    print(f"\n{'=' * 60}")


# =============================================================================
# Window Ensemble API (Template for all window-based experiments)
# =============================================================================


def filter_train_mask_by_window(data, start_day: int, end_day: int) -> torch.Tensor:
    mode = os.environ.get("PHASE3_WINDOW_FILTER_MODE", "disabled").strip().lower()
    if mode in ("disabled", "off", "none", "0", "false", "no"):
        return data.train_mask

    if mode not in ("alert_time", "node_alert_time", "alert"):
        raise ValueError(f"Unknown PHASE3_WINDOW_FILTER_MODE: {mode}")

    alert_time = getattr(data, "node_alert_time", None)
    if alert_time is None:
        print(
            "  [WARNING] node_alert_time not found — window filtering DISABLED",
            flush=True,
        )
        return data.train_mask

    in_window = (alert_time >= start_day) & (alert_time <= end_day)
    filtered = data.train_mask & in_window
    print(
        f"  Window [{start_day}, {end_day}] via node_alert_time: {filtered.sum().item()} train nodes "
        f"(of {data.train_mask.sum().item()} total)",
        flush=True,
    )
    return filtered


def train_window_ensemble(
    data,
    experiment_dir: str,
    experiment_name: str,
    windows: List[Tuple[int, int]],
    features: List[str],
    architectures: Optional[List[str]] = None,
    hidden_dim: int = 128,
    num_layers: int = 4,
    dropout: float = 0.3,
    max_epochs: int = 300,
    patience: int = 10,
    lr: float = 0.005,
    batch_size: int = 8192,
    criterion: Optional[nn.Module] = None,
    ensemble_method: str = "mean",
    eval_every_epochs: int = 1,
    **kwargs,
) -> Dict[str, Any]:
    """Train models on multiple time windows, then ensemble.

    MANDATORY outputs: PeakMemoryTracker, ProgressReporter, p2_f1, save_results.
    OPTIONAL inputs: augmentation kwargs (drop_node_rate, drop_edge_rate, etc.),
                     custom architectures, ensemble_method.

    Returns:
        Results dict with test_f1, test_auc, p2_f1, etc.
    """
    if architectures is None:
        architectures = ["GraphSAGE", "GIN", "GCN"]
    if criterion is None:
        criterion = FocalLoss()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}", flush=True)

    checkpoint_root = os.path.join(experiment_dir, "checkpoints")
    os.makedirs(checkpoint_root, exist_ok=True)

    reporter = ProgressReporter(experiment_dir, experiment_name)
    all_models: List[nn.Module] = []
    all_val_f1s: List[float] = []

    # We need a stable val/test loader (built from original masks, not window-filtered)
    _, val_loader, test_loader = create_loaders(data, num_layers, batch_size)

    with PeakMemoryTracker(experiment_dir, experiment_name=experiment_name):
        for win_idx, (start_day, end_day) in enumerate(windows):
            print(
                f"\n{'=' * 20} Window {win_idx + 1}/{len(windows)}: "
                f"Day {start_day}-{end_day} {'=' * 20}",
                flush=True,
            )

            window_mask = filter_train_mask_by_window(data, start_day, end_day)
            original_train_mask = data.train_mask
            data.train_mask = window_mask

            train_loader, _, _ = create_loaders(data, num_layers, batch_size)
            data.train_mask = original_train_mask

            window_ckpt_dir = os.path.join(checkpoint_root, f"window_{win_idx + 1}")
            os.makedirs(window_ckpt_dir, exist_ok=True)

            for arch_name in architectures:
                ModelClass = ARCHITECTURES[arch_name]
                model = ModelClass(
                    data.x.shape[1], hidden_dim, 2, num_layers, dropout, **kwargs
                ).to(device)
                ckpt_path = os.path.join(window_ckpt_dir, f"best_{arch_name}.pt")

                model, best_val_f1 = train_single_model(
                    model,
                    train_loader,
                    val_loader,
                    device,
                    f"W{win_idx + 1}_{arch_name}",
                    ckpt_path,
                    criterion,
                    max_epochs=max_epochs,
                    patience=patience,
                    lr=lr,
                    eval_every_epochs=eval_every_epochs,
                    **kwargs,
                )
                all_models.append(model)
                all_val_f1s.append(best_val_f1)

                if reporter:
                    reporter.update(
                        (
                            win_idx * len(architectures)
                            + architectures.index(arch_name)
                            + 1
                        ),
                        len(windows) * len(architectures),
                        best_val_f1,
                    )

        # --- Ensemble evaluation ---
        print(f"\n--- Ensemble ({len(all_models)} models) ---", flush=True)

        method = (ensemble_method or "mean").lower()
        all_val_probs = []
        all_test_probs = []
        val_labels = None
        test_labels = None

        for m in all_models:
            vp, vl = evaluate(m, val_loader, device)
            tp, tl = evaluate(m, test_loader, device)
            all_val_probs.append(vp)
            all_test_probs.append(tp)
            val_labels = vl
            test_labels = tl

        ensemble_val_probs = np.mean(all_val_probs, axis=0)
        ensemble_test_probs = np.mean(all_test_probs, axis=0)

        if method == "weighted":
            weights = np.array(all_val_f1s, dtype=np.float32)
            if weights.sum() > 0:
                weights = weights / weights.sum()
                ensemble_val_probs = np.sum(
                    np.stack(all_val_probs) * weights[:, None], axis=0
                )
                ensemble_test_probs = np.sum(
                    np.stack(all_test_probs) * weights[:, None], axis=0
                )
        elif method == "stacking":
            try:
                X_val = np.stack(all_val_probs, axis=1)
                X_test = np.stack(all_test_probs, axis=1)
                clf = LogisticRegression(max_iter=1000)
                clf.fit(X_val, val_labels)
                ensemble_val_probs = clf.predict_proba(X_val)[:, 1]
                ensemble_test_probs = clf.predict_proba(X_test)[:, 1]
            except Exception:
                pass

        if val_labels is None:
            raise ValueError(
                "val_labels must be available for ensemble threshold tuning"
            )
        best_thresh, best_val_f1 = find_best_threshold(val_labels, ensemble_val_probs)

        test_preds = (ensemble_test_probs >= best_thresh).astype(int)
        test_f1 = float(f1_score(test_labels, test_preds, zero_division="warn"))
        test_auc = float(roc_auc_score(test_labels, ensemble_test_probs))
        test_prec = float(
            precision_score(test_labels, test_preds, zero_division="warn")
        )
        test_rec = float(recall_score(test_labels, test_preds, zero_division="warn"))

        # Phase 2 Private LB evaluation (MANDATORY)
        p2_results = evaluate_offline_leaderboard(
            all_models, data, device, method, all_val_f1s
        )

    reporter.finish()

    results = {
        "test_f1": test_f1,
        "test_precision": test_prec,
        "test_recall": test_rec,
        "test_auc": test_auc,
        "best_threshold": float(best_thresh),
        "best_val_f1": float(best_val_f1),
        "features": features,
        "windows": windows,
        "architectures": architectures,
        "num_models": len(all_models),
        "ensemble_method": method,
        **p2_results,
    }

    # Dual-write: experiment dir + results_db
    save_results(experiment_dir, results, experiment_name)
    results_db_dir = os.path.join(os.path.dirname(experiment_dir), "..", "results_db")
    os.makedirs(results_db_dir, exist_ok=True)
    with open(os.path.join(results_db_dir, f"{experiment_name}.json"), "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'=' * 60}", flush=True)
    print(
        f"FINAL: Test F1={test_f1:.4f}, AUC={test_auc:.4f}"
        + (f", p2_F1={p2_results.get('p2_f1', 'N/A')}" if p2_results else ""),
        flush=True,
    )
    print(f"{'=' * 60}", flush=True)

    return results
