#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Template: Window Ensemble Experiment
=====================================
Copy this file to your experiment's scripts/train.py and modify the CONFIG section only.
Everything below the CONFIG section should NOT be modified unless you have a specific reason.

Usage:
    cp Phase3/templates/train_window_ensemble.py Phase3/experiments/NEW_EXP/scripts/train.py
    # Edit CONFIG section
    # Register in ready.json

API Classification:
    MANDATORY (always active, do not remove):
        - PeakMemoryTracker: GPU memory tracking + OOM detection
        - ProgressReporter: Dashboard progress reporting
        - evaluate_offline_leaderboard: Phase 2 Private LB (p2_f1)
        - save_results: Dual-write (experiment dir + results_db)

    OPTIONAL (via kwargs):
        - drop_node_rate: float  (DropNode augmentation)
        - drop_edge_rate: float  (DropEdge augmentation)
        - feature_noise: float   (Gaussian noise on features)
        - consistency_reg: float (Consistency regularization weight)
        - domain_adapt: bool     (Domain adaptation loss)
        - domain_weight: float   (Domain adaptation weight)
        - ensemble_method: str   ("mean" | "weighted" | "stacking")
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Any

if os.environ.get("PHASE3_GATE") == "1":
    os.environ.setdefault("CUDA_LAUNCH_BLOCKING", "1")
    os.environ.setdefault("PYTORCH_NO_CUDA_MEMORY_CACHING", "1")

import torch

# =============================================================================
# Path Setup (DO NOT MODIFY)
# =============================================================================

_THIS_FILE = Path(__file__).resolve()
_IS_SOURCE_TEMPLATE = _THIS_FILE.parent.name == "templates"
PHASE3_DIR = _THIS_FILE.parents[1] if _IS_SOURCE_TEMPLATE else _THIS_FILE.parents[3]
EXPERIMENT_DIR = (
    PHASE3_DIR / "experiments" / "CHANGE_ME"
    if _IS_SOURCE_TEMPLATE
    else _THIS_FILE.parents[1]
)
sys.path.append(str(PHASE3_DIR))

from cli_shared import (
    SUCCESS,
    add_common_args,
    add_training_args,
    run_with_cli as run_with_cli_entry,
)
from preprocess_lib.feature_bank import load_data_with_features
from preprocess_lib.train_utils import (
    FocalLoss,
    GraphSAGEModel,
    train_window_ensemble,
    create_loaders,
)

# =============================================================================
# CONFIG — Modify this section for each experiment
# =============================================================================

EXPERIMENT_NAME = "CHANGE_ME"  # Must match directory name

FEATURES = [
    "base_34dim_cut_d152",
    "balance_vol_4dim_cut_d152",
    "velocity_3dim_cut_d152",
    "burst_3dim_cut_d152",
]

WINDOWS = [
    (90, 152),  # Broad
    (120, 152),  # Medium
    (136, 152),  # Narrow
]

ARCHITECTURES = ["GraphSAGE", "GIN", "GCN"]

HIDDEN_DIM = 128
NUM_LAYERS = 4
DROPOUT = 0.3
MAX_EPOCHS = 300
PATIENCE = 50
LR = 0.005
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "8192"))

CRITERION = FocalLoss(alpha=0.75, gamma=2.0)

AUGMENTATION_KWARGS = {
    # "drop_node_rate": 0.1,
    # "drop_edge_rate": 0.2,
    # "feature_noise": 0.01,
}

ENSEMBLE_METHOD = "mean"

# =============================================================================
# Gate Smoke Test (DO NOT MODIFY)
# =============================================================================


def gate_smoke_test():
    batch_size = int(os.environ.get("GATE_BATCH_SIZE", "256"))
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    data = load_data_with_features(features=FEATURES, verbose=False)
    if data is None or data.x is None:
        raise ValueError("Data not found")
    data.x = data.x.contiguous()

    _, val_loader, _ = create_loaders(data, NUM_LAYERS, batch_size)
    model = GraphSAGEModel(data.x.shape[1], HIDDEN_DIM, 2, NUM_LAYERS, DROPOUT).to(
        device
    )
    model.eval()

    batch = next(iter(val_loader))
    batch = batch.to(device)
    with torch.no_grad():
        out = model(batch.x, batch.edge_index)[: batch.batch_size]
    if torch.isnan(out).any() or torch.isinf(out).any():
        raise RuntimeError("gate_smoke_test: NaN/Inf detected in logits")


# =============================================================================
# Main (DO NOT MODIFY — all logic is in train_window_ensemble)
# =============================================================================


def main():
    print("=" * 60, flush=True)
    print(f"{EXPERIMENT_NAME}: Window Ensemble", flush=True)
    print(f"  Windows: {WINDOWS}", flush=True)
    print(f"  Architectures: {ARCHITECTURES}", flush=True)
    print(f"  Features: {len(FEATURES)} groups", flush=True)
    print("=" * 60, flush=True)

    data = load_data_with_features(features=FEATURES, verbose=True)
    if data is None or data.x is None:
        raise ValueError("Data not found")
    data.x = data.x.contiguous()

    train_window_ensemble(
        data=data,
        experiment_dir=str(EXPERIMENT_DIR),
        experiment_name=EXPERIMENT_NAME,
        windows=WINDOWS,
        features=FEATURES,
        architectures=ARCHITECTURES,
        hidden_dim=HIDDEN_DIM,
        num_layers=NUM_LAYERS,
        dropout=DROPOUT,
        max_epochs=MAX_EPOCHS,
        patience=PATIENCE,
        lr=LR,
        batch_size=BATCH_SIZE,
        criterion=CRITERION,
        ensemble_method=ENSEMBLE_METHOD,
        **AUGMENTATION_KWARGS,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"{EXPERIMENT_NAME}: Window Ensemble")
    add_common_args(parser)
    add_training_args(parser)
    return parser


def _main_with_cli(args: argparse.Namespace) -> tuple[int, Any]:
    global HIDDEN_DIM, LR, MAX_EPOCHS, BATCH_SIZE
    if args.hidden_dim is not None:
        HIDDEN_DIM = int(args.hidden_dim)
    if args.lr is not None:
        LR = float(args.lr)
    if args.max_epochs is not None:
        MAX_EPOCHS = int(args.max_epochs)
    if args.batch_size is not None:
        BATCH_SIZE = int(args.batch_size)
    if args.dry_run:
        return SUCCESS, {
            "dry_run": True,
            "experiment_name": EXPERIMENT_NAME,
            "features": FEATURES,
            "windows": WINDOWS,
            "architectures": ARCHITECTURES,
            "hidden_dim": HIDDEN_DIM,
            "lr": LR,
            "max_epochs": MAX_EPOCHS,
            "batch_size": BATCH_SIZE,
        }
    main()
    return SUCCESS, {"dry_run": False, "experiment_name": EXPERIMENT_NAME}


def run_with_cli(
    argv: list[str] | None = None, output_stream: Any | None = None
) -> int:
    return run_with_cli_entry(
        _main_with_cli,
        build_parser(),
        argv=argv,
        output_stream=output_stream,
    )


if __name__ == "__main__":
    raise SystemExit(run_with_cli())
