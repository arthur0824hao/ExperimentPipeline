#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Graph Builder - Build and save filtered graph data

Author: Claude
Date: 2026-01-13
"""

import os
import json
import torch
import numpy as np
import pandas as pd
from torch_geometric.data import Data
from typing import Set, List, Dict, Tuple, Any, Optional


def build_account_index(
    df_transaction: pd.DataFrame,
    retained_accounts: Set[str],
) -> Tuple[Dict[str, int], Dict[int, str], List[str]]:
    """
    Build account index mapping for retained accounts.

    Returns:
        acct_to_node, node_to_acct, all_accounts
    """
    print("\n" + "=" * 60)
    print("Building account index...")

    # Get all accounts from transactions
    from_accts = set(df_transaction["from_acct"].unique())
    to_accts = set(df_transaction["to_acct"].unique())
    all_accounts_raw = from_accts | to_accts

    # Filter to retained accounts
    # Use retained_accounts directly to ensure we include accounts that might be
    # in the test set/must_include list even if they have no transactions (isolated nodes)
    all_accounts = sorted(list(retained_accounts))

    acct_to_node = {acct: idx for idx, acct in enumerate(all_accounts)}
    node_to_acct = {idx: acct for acct, idx in acct_to_node.items()}

    print(f"  Original accounts: {len(all_accounts_raw):,}")
    print(f"  Retained accounts: {len(all_accounts):,}")

    return acct_to_node, node_to_acct, all_accounts


def create_bank_account_masks(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
) -> Tuple[torch.Tensor, Dict[str, int]]:
    """
    Create mask for E.SUN vs other bank accounts.
    Vectorized implementation.

    Returns:
        is_esun_mask, acct_type_dict
    """
    print("\n" + "=" * 60)
    print("Creating bank account masks (Vectorized)...")

    # 1. Extract (acct, type) pairs from both columns
    df_from = df_transaction[["from_acct", "from_acct_type"]].rename(
        columns={"from_acct": "acct", "from_acct_type": "type"}
    )
    df_to = df_transaction[["to_acct", "to_acct_type"]].rename(
        columns={"to_acct": "acct", "to_acct_type": "type"}
    )

    # 2. Concatenate and drop duplicates (keep min type for safety? actually type should be consistent)
    # If an account appears as both type 1 and type 2 (unlikely?), we prioritize 1 (E.SUN).
    # Type 1 = E.SUN, Type 2 = Other? (Assuming convention)
    df_all_types = pd.concat([df_from, df_to], ignore_index=True)

    # Group by account and take min type (assuming 1 < 2, so if seen as 1, it becomes 1)
    # This avoids iteration.
    # But wait, groupby is expensive on 11M rows?
    # Maybe drop_duplicates first?
    df_unique = df_all_types.drop_duplicates(subset=["acct", "type"])
    # Now group by acct -> min type
    acct_types = df_unique.groupby("acct")["type"].min()

    # Convert to dict for fast lookup (or map directly if all_accounts is aligned)
    acct_type_dict = acct_types.to_dict()

    num_nodes = len(all_accounts)
    is_esun_mask = torch.zeros(num_nodes, dtype=torch.bool)

    # Vectorized mask creation
    # Create a Series for all accounts, map type using dict
    # Missing accounts default to 2 (Other)

    # Efficient way:
    # 1. Filter acct_type_dict to only include keys in all_accounts (intersection)
    # 2. Get indices

    # Since acct_to_node is dense 0..N, we can iterate or map.
    # Iteration over 2M items is fast in Python list/dict, just DataFrame iterrows is slow.
    # Let's keep the loop over all_accounts but use the pre-built dict.
    # Actually, we can do better:

    # Identify E.SUN accounts (type == 1)
    esun_accounts = {acct for acct, type_val in acct_type_dict.items() if type_val == 1}

    # Get indices of E.SUN accounts
    esun_indices = []
    for acct in esun_accounts:
        if acct in acct_to_node:
            esun_indices.append(acct_to_node[acct])

    if esun_indices:
        is_esun_mask[esun_indices] = True

    esun_count = is_esun_mask.sum().item()
    print(f"  E.SUN accounts: {esun_count:,}")
    print(f"  Other bank: {num_nodes - esun_count:,}")

    return is_esun_mask, acct_type_dict


def build_edge_index(
    df_transaction: pd.DataFrame,
    acct_to_node: Dict[str, int],
    scaler_save_path: Optional[str] = None,
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Build multi-graph edges from transactions (no deduplication).

    Each transaction row is preserved as one directed edge.
    """
    print("\n" + "=" * 60)
    print("Building multi-graph edges (transaction-level)...")

    from .feature_computer import compute_edge_attributes

    src_nodes, dst_nodes, edge_attr_np, edge_meta = compute_edge_attributes(
        df_transaction=df_transaction,
        acct_to_node=acct_to_node,
        standardize=True,
    )
    edge_time_np = edge_meta.pop("edge_time")

    edge_index = torch.stack(
        [torch.from_numpy(src_nodes), torch.from_numpy(dst_nodes)],
        dim=0,
    ).to(torch.long)
    edge_attr = torch.from_numpy(edge_attr_np).to(torch.float32)
    edge_time = torch.from_numpy(edge_time_np).to(torch.float32)

    print(f"  Valid transactions (kept as edges): {edge_index.shape[1]:,}")
    print(f"  edge_attr dims: {edge_attr.shape[1]}")

    if scaler_save_path is not None:
        os.makedirs(os.path.dirname(scaler_save_path), exist_ok=True)
        with open(scaler_save_path, "w", encoding="utf-8") as f:
            json.dump(edge_meta, f, indent=2)
        print(f"  Saved edge scaler params: {scaler_save_path}")

    return edge_index, edge_attr, edge_time


def create_train_val_test_split(
    df_alert: pd.DataFrame,
    df_predict: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    node_first_txn_time: np.ndarray,
    is_esun_mask: torch.Tensor,
) -> Tuple[
    torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor
]:
    """
    Create temporal train/val/test split with bank masking.

    Returns:
        y, train_mask, val_mask, test_mask, predict_mask, node_alert_time
    """
    print("\n" + "=" * 60)
    print("Creating train/val/test split...")

    num_nodes = len(all_accounts)
    y = torch.zeros(num_nodes, dtype=torch.long)
    node_alert_time = torch.full((num_nodes,), float("inf"), dtype=torch.float32)

    # Label alert accounts
    alert_dict = {row["acct"]: row["event_date"] for _, row in df_alert.iterrows()}
    alert_count = 0
    for acct, event_date in alert_dict.items():
        if acct in acct_to_node:
            node_idx = acct_to_node[acct]
            y[node_idx] = 1
            node_alert_time[node_idx] = event_date
            alert_count += 1

    print(f"  Alert accounts: {alert_count:,}")

    # Prediction mask
    predict_mask = torch.zeros(num_nodes, dtype=torch.bool)
    for _, row in df_predict.iterrows():
        if row["acct"] in acct_to_node:
            predict_mask[acct_to_node[row["acct"]]] = True

    # Only E.SUN accounts for supervised learning
    trainable_mask = is_esun_mask & (~predict_mask)
    trainable_indices = torch.where(trainable_mask)[0]

    print(f"  Trainable (E.SUN only): {len(trainable_indices):,}")
    print(f"    Fraud: {y[trainable_indices].sum().item():,}")
    print(f"    Normal: {(1 - y[trainable_indices]).sum().item():,}")

    # Temporal split (60/20/20)
    trainable_times = node_first_txn_time[trainable_indices.numpy()]
    sorted_idx = np.argsort(trainable_times)
    sorted_trainable_indices = trainable_indices[sorted_idx]

    n = len(sorted_trainable_indices)
    train_size = int(0.6 * n)
    val_size = int(0.2 * n)

    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    val_mask = torch.zeros(num_nodes, dtype=torch.bool)
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)

    train_mask[sorted_trainable_indices[:train_size]] = True
    val_mask[sorted_trainable_indices[train_size : train_size + val_size]] = True
    test_mask[sorted_trainable_indices[train_size + val_size :]] = True

    print(f"\n  Split:")
    print(f"    Train: {train_mask.sum():,} (Fraud: {y[train_mask].sum().item()})")
    print(f"    Val: {val_mask.sum():,} (Fraud: {y[val_mask].sum().item()})")
    print(f"    Test: {test_mask.sum():,} (Fraud: {y[test_mask].sum().item()})")

    return y, train_mask, val_mask, test_mask, predict_mask, node_alert_time


def create_phase2_split(
    df_alert: pd.DataFrame,
    df_test_groundtruth: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    node_first_txn_time: np.ndarray,
    is_esun_mask: torch.Tensor,
) -> Tuple[
    torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor
]:
    """
    Create Phase 2 train/val/test split with explicit ground truth.

    - Test Set: Defined by df_test_groundtruth (with labels).
    - Train/Val: Remaining E.SUN accounts, split 80/20 temporally.
    """
    print("\n" + "=" * 60)
    print("Creating Phase 2 train/val/test split...")

    num_nodes = len(all_accounts)
    y = torch.zeros(num_nodes, dtype=torch.long)
    node_alert_time = torch.full((num_nodes,), float("inf"), dtype=torch.float32)

    # 1. Label confirmed alert accounts (Training Data)
    alert_dict = {row["acct"]: row["event_date"] for _, row in df_alert.iterrows()}
    alert_count = 0
    for acct, event_date in alert_dict.items():
        if acct in acct_to_node:
            node_idx = acct_to_node[acct]
            y[node_idx] = 1
            node_alert_time[node_idx] = event_date
            alert_count += 1
    print(f"  Alert accounts (Train): {alert_count:,}")

    # 2. Label Test Ground Truth (Test Data)
    # df_test_groundtruth has 'acct' and 'label'
    test_groundtruth_count = 0
    test_fraud_count = 0
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)

    gt_dict = {row["acct"]: row["label"] for _, row in df_test_groundtruth.iterrows()}

    for acct, label in gt_dict.items():
        if acct in acct_to_node:
            node_idx = acct_to_node[acct]
            y[node_idx] = int(label)  # Overwrite/Set label
            test_mask[node_idx] = True
            test_groundtruth_count += 1
            if label == 1:
                test_fraud_count += 1

    print(f"  Test Ground Truth accounts: {test_groundtruth_count:,}")
    print(f"    Fraud: {test_fraud_count:,}")
    print(f"    Normal: {test_groundtruth_count - test_fraud_count:,}")

    # 3. Define Trainable Nodes (E.SUN only, excluding Test Set)
    # We treat all non-test E.SUN accounts as potential training data
    trainable_mask = is_esun_mask & (~test_mask)
    trainable_indices = torch.where(trainable_mask)[0]

    print(f"  Trainable Candidates (E.SUN - Test): {len(trainable_indices):,}")
    print(f"    Fraud: {y[trainable_indices].sum().item():,}")
    print(f"    Normal: {(1 - y[trainable_indices]).sum().item():,}")

    # 4. Temporal Split for Train/Val (80/20)
    trainable_times = node_first_txn_time[trainable_indices.numpy()]
    sorted_idx = np.argsort(trainable_times)
    sorted_trainable_indices = trainable_indices[sorted_idx]

    n = len(sorted_trainable_indices)
    train_size = int(0.8 * n)  # 80% Train
    val_size = n - train_size  # 20% Val

    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    val_mask = torch.zeros(num_nodes, dtype=torch.bool)

    # Predict mask is effectively the test mask in this context, but we keep the variable for compatibility
    predict_mask = test_mask.clone()

    train_mask[sorted_trainable_indices[:train_size]] = True
    val_mask[sorted_trainable_indices[train_size:]] = True

    print(f"\n  Final Split:")
    print(f"    Train: {train_mask.sum():,} (Fraud: {y[train_mask].sum().item()})")
    print(f"    Val:   {val_mask.sum():,} (Fraud: {y[val_mask].sum().item()})")
    print(f"    Test:  {test_mask.sum():,} (Fraud: {y[test_mask].sum().item()})")

    return y, train_mask, val_mask, test_mask, predict_mask, node_alert_time


def save_graph_data(
    output_path: str,
    features: np.ndarray,
    edge_index: torch.Tensor,
    edge_attr: torch.Tensor,
    edge_time: torch.Tensor,
    y: torch.Tensor,
    train_mask: torch.Tensor,
    val_mask: torch.Tensor,
    test_mask: torch.Tensor,
    predict_mask: torch.Tensor,
    is_esun_mask: torch.Tensor,
    node_first_txn_time: np.ndarray,
    node_last_txn_time: np.ndarray,
    node_alert_time: torch.Tensor,
    node_to_acct: Dict[int, str],
    acct_to_node: Dict[str, int],
    filter_name: str = "",
) -> Data:
    """
    Create and save PyG Data object.
    """
    print("\n" + "=" * 60)
    print("Saving graph data...")

    data = Data(
        x=torch.tensor(features, dtype=torch.float32),
        edge_index=edge_index,
        edge_attr=edge_attr,
        edge_time=edge_time,
        y=y,
        train_mask=train_mask,
        val_mask=val_mask,
        test_mask=test_mask,
        predict_mask=predict_mask,
        is_esun_mask=is_esun_mask,
        node_first_txn_time=torch.tensor(node_first_txn_time, dtype=torch.float32),
        node_last_txn_time=torch.tensor(node_last_txn_time, dtype=torch.float32),
        node_alert_time=node_alert_time,
        node_to_acct=node_to_acct,
        acct_to_node=acct_to_node,
        filter_name=filter_name,
    )

    print(f"\n  Data Summary:")
    print(f"    Nodes: {data.num_nodes:,}")
    print(f"    Edges: {data.num_edges:,}")
    print(f"    Features: {data.num_node_features}")
    print(f"    Filter: {filter_name}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    torch.save(data, output_path)
    print(f"\n  Saved to: {output_path}")

    return data


def build_phase2_filtered_graph(
    df_transaction: pd.DataFrame,
    df_alert: pd.DataFrame,
    df_test_groundtruth: pd.DataFrame,
    retained_accounts: Set[str],
    output_path: str,
    filter_name: str = "",
) -> Data:
    """
    Main function to build a filtered graph for Phase 2 (Finals).
    """
    from .feature_computer import compute_all_features

    print("\n" + "=" * 60)
    print(f"BUILDING PHASE 2 GRAPH: {filter_name}")
    print("=" * 60)

    # 1. Build account index
    acct_to_node, node_to_acct, all_accounts = build_account_index(
        df_transaction, retained_accounts
    )

    # 2. Create bank masks
    is_esun_mask, acct_type_dict = create_bank_account_masks(
        df_transaction, all_accounts, acct_to_node
    )

    # 3. Compute features
    node_scaler_path = os.path.join(os.path.dirname(output_path), "scaler_node.json")
    edge_scaler_path = os.path.join(os.path.dirname(output_path), "scaler_edge.json")

    features, node_first_txn_time, node_last_txn_time = compute_all_features(
        df_transaction,
        all_accounts,
        acct_to_node,
        acct_type_dict,
        scaler_save_path=node_scaler_path,
    )

    # 4. Build edges
    edge_index, edge_attr, edge_time = build_edge_index(
        df_transaction,
        acct_to_node,
        scaler_save_path=edge_scaler_path,
    )

    # 5. Create splits (Using Phase 2 Logic)
    y, train_mask, val_mask, test_mask, predict_mask, node_alert_time = (
        create_phase2_split(
            df_alert,
            df_test_groundtruth,
            all_accounts,
            acct_to_node,
            node_first_txn_time,
            is_esun_mask,
        )
    )

    # 6. Save
    data = save_graph_data(
        output_path,
        features,
        edge_index,
        edge_attr,
        edge_time,
        y,
        train_mask,
        val_mask,
        test_mask,
        predict_mask,
        is_esun_mask,
        node_first_txn_time,
        node_last_txn_time,
        node_alert_time,
        node_to_acct,
        acct_to_node,
        filter_name,
    )

    return data


def build_filtered_graph(
    df_transaction: pd.DataFrame,
    df_alert: pd.DataFrame,
    df_predict: pd.DataFrame,
    retained_accounts: Set[str],
    output_path: str,
    filter_name: str = "",
) -> Data:
    """
    Main function to build a filtered graph.

    This is the entry point for each experiment's preprocessing.
    """
    from .feature_computer import compute_all_features

    print("\n" + "=" * 60)
    print(f"BUILDING FILTERED GRAPH: {filter_name}")
    print("=" * 60)

    # 1. Build account index
    acct_to_node, node_to_acct, all_accounts = build_account_index(
        df_transaction, retained_accounts
    )

    # 2. Create bank masks
    is_esun_mask, acct_type_dict = create_bank_account_masks(
        df_transaction, all_accounts, acct_to_node
    )

    # 3. Compute features
    node_scaler_path = os.path.join(os.path.dirname(output_path), "scaler_node.json")
    edge_scaler_path = os.path.join(os.path.dirname(output_path), "scaler_edge.json")

    features, node_first_txn_time, node_last_txn_time = compute_all_features(
        df_transaction,
        all_accounts,
        acct_to_node,
        acct_type_dict,
        scaler_save_path=node_scaler_path,
    )

    # 4. Build edges
    edge_index, edge_attr, edge_time = build_edge_index(
        df_transaction,
        acct_to_node,
        scaler_save_path=edge_scaler_path,
    )

    # 5. Create splits
    y, train_mask, val_mask, test_mask, predict_mask, node_alert_time = (
        create_train_val_test_split(
            df_alert,
            df_predict,
            all_accounts,
            acct_to_node,
            node_first_txn_time,
            is_esun_mask,
        )
    )

    # 6. Save
    data = save_graph_data(
        output_path,
        features,
        edge_index,
        edge_attr,
        edge_time,
        y,
        train_mask,
        val_mask,
        test_mask,
        predict_mask,
        is_esun_mask,
        node_first_txn_time,
        node_last_txn_time,
        node_alert_time,
        node_to_acct,
        acct_to_node,
        filter_name,
    )

    return data


def build_phase3_graph(
    df_transaction: pd.DataFrame,
    df_alert: pd.DataFrame,
    df_predict: pd.DataFrame,
    retained_accounts: Set[str],
    output_path: str,
    filter_name: str = "",
) -> Data:
    """
    Main function to build a filtered graph for Phase 3 (Day 153-183 Prediction).
    """
    from .feature_computer import compute_all_features

    print("\n" + "=" * 60)
    print(f"BUILDING PHASE 3 GRAPH: {filter_name}")
    print("=" * 60)

    # 1. Build account index
    acct_to_node, node_to_acct, all_accounts = build_account_index(
        df_transaction, retained_accounts
    )

    # 2. Create bank masks
    is_esun_mask, acct_type_dict = create_bank_account_masks(
        df_transaction, all_accounts, acct_to_node
    )

    # 3. Compute features
    node_scaler_path = os.path.join(os.path.dirname(output_path), "scaler_node.json")
    edge_scaler_path = os.path.join(os.path.dirname(output_path), "scaler_edge.json")

    features, node_first_txn_time, node_last_txn_time = compute_all_features(
        df_transaction,
        all_accounts,
        acct_to_node,
        acct_type_dict,
        scaler_save_path=node_scaler_path,
    )

    # 4. Build edges
    edge_index, edge_attr, edge_time = build_edge_index(
        df_transaction,
        acct_to_node,
        scaler_save_path=edge_scaler_path,
    )

    # 5. Create splits (Phase 3 Logic)
    # Use create_train_val_test_split which already handles df_predict
    y, train_mask, val_mask, test_mask, predict_mask, node_alert_time = (
        create_train_val_test_split(
            df_alert,
            df_predict,
            all_accounts,
            acct_to_node,
            node_first_txn_time,
            is_esun_mask,
        )
    )

    # 6. Save
    data = save_graph_data(
        output_path,
        features,
        edge_index,
        edge_attr,
        edge_time,
        y,
        train_mask,
        val_mask,
        test_mask,
        predict_mask,
        is_esun_mask,
        node_first_txn_time,
        node_last_txn_time,
        node_alert_time,
        node_to_acct,
        acct_to_node,
        filter_name,
    )

    return data
