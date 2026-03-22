#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Utilities for temporal cutoff strategies."""

from typing import Optional

import numpy as np
import torch


def _get_field(structure, name: str):
    if hasattr(structure, name):
        return getattr(structure, name)
    if isinstance(structure, dict) and name in structure:
        return structure[name]
    raise KeyError(f"Missing field in structure: {name}")


def _to_numpy(arr) -> np.ndarray:
    if isinstance(arr, torch.Tensor):
        return arr.detach().cpu().numpy()
    return np.asarray(arr)


def compute_cutoff_times(
    structure,
    policy: str,
    obs_end_time: Optional[float] = None,
    predict_full: bool = True,
) -> np.ndarray:
    """
    Compute per-node cutoff times based on policy.

    Policies:
    - split: cutoff per split boundary (train/val/test)
    - alert: positives cutoff at alert time, others at obs_end_time
    - fixed: all train/val/test use obs_end_time
    """
    train_mask = _to_numpy(_get_field(structure, "train_mask")).astype(bool)
    val_mask = _to_numpy(_get_field(structure, "val_mask")).astype(bool)
    test_mask = _to_numpy(_get_field(structure, "test_mask")).astype(bool)
    predict_mask = _to_numpy(_get_field(structure, "predict_mask")).astype(bool)
    y = _to_numpy(_get_field(structure, "y"))

    node_first = _to_numpy(_get_field(structure, "node_first_txn_time"))
    node_last = _to_numpy(_get_field(structure, "node_last_txn_time"))
    node_alert = _to_numpy(_get_field(structure, "node_alert_time"))

    max_time = (
        float(np.nanmax(node_last))
        if node_last.size > 0
        else float(np.nanmax(node_first))
    )

    if obs_end_time is None:
        alert_times = node_alert[np.isfinite(node_alert)]
        obs_end_time = float(np.max(alert_times)) if alert_times.size > 0 else max_time

    cutoff = np.full_like(node_first, obs_end_time, dtype=np.float32)

    policy = policy.lower()
    if policy == "split":
        train_end = (
            float(np.max(node_first[train_mask])) if train_mask.any() else obs_end_time
        )
        val_end = float(np.max(node_first[val_mask])) if val_mask.any() else train_end
        test_end = float(np.max(node_first[test_mask])) if test_mask.any() else val_end

        cutoff[train_mask] = train_end
        cutoff[val_mask] = val_end
        cutoff[test_mask] = test_end
        if predict_full:
            cutoff[predict_mask] = max_time

    elif policy == "alert":
        alert_nodes = (y == 1) & np.isfinite(node_alert)
        cutoff[alert_nodes] = node_alert[alert_nodes]
        if predict_full:
            cutoff[predict_mask] = max_time

    elif policy == "fixed":
        cutoff[:] = obs_end_time
        if predict_full:
            cutoff[predict_mask] = max_time

    else:
        raise ValueError(f"Unknown cutoff policy: {policy}")

    return cutoff
