#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature Computer - Compute node features (34 dimensions)

Modular feature computation that can be reused across experiments.

Author: Claude
Date: 2026-01-13
"""

import numpy as np
import pandas as pd
import json
from collections import defaultdict
import networkx as nx
from typing import List, Dict, Tuple, Set, Optional


def _txn_time_series_to_seconds(txn_time_series: pd.Series) -> pd.Series:
    """Convert txn_time to seconds-of-day.

    Supports both "HH:MM:SS" and numeric HHMMSS forms (e.g., 93015, 143000).
    Invalid values fall back to 0.
    """
    raw = txn_time_series.fillna("").astype(str).str.strip()
    seconds = pd.Series(0, index=raw.index, dtype=np.int64)

    colon_mask = raw.str.contains(":", regex=False)
    if colon_mask.any():
        colon_parsed = pd.to_datetime(
            raw[colon_mask], format="%H:%M:%S", errors="coerce"
        )
        colon_seconds = (
            colon_parsed.dt.hour.fillna(0).astype(np.int64) * 3600
            + colon_parsed.dt.minute.fillna(0).astype(np.int64) * 60
            + colon_parsed.dt.second.fillna(0).astype(np.int64)
        )
        seconds.loc[colon_mask] = colon_seconds.to_numpy()

    numeric_mask = ~colon_mask
    if numeric_mask.any():
        digits = raw[numeric_mask].str.replace(r"\D", "", regex=True)
        six = digits.str[-6:].str.zfill(6)

        hh = (
            pd.to_numeric(six.str.slice(0, 2), errors="coerce")
            .fillna(0)
            .astype(np.int64)
        )
        mm = (
            pd.to_numeric(six.str.slice(2, 4), errors="coerce")
            .fillna(0)
            .astype(np.int64)
        )
        ss = (
            pd.to_numeric(six.str.slice(4, 6), errors="coerce")
            .fillna(0)
            .astype(np.int64)
        )

        valid = (hh <= 23) & (mm <= 59) & (ss <= 59)
        numeric_seconds = (hh * 3600 + mm * 60 + ss).where(valid, 0)
        seconds.loc[numeric_mask] = numeric_seconds.to_numpy()

    return seconds


def _within_cutoff(
    acct: str,
    txn_time: float,
    acct_to_node: Dict[str, int],
    cutoff_times: Optional[np.ndarray],
) -> bool:
    if cutoff_times is None:
        return True
    node_idx = acct_to_node.get(acct)
    if node_idx is None:
        return False
    return txn_time <= cutoff_times[node_idx]


def _prepare_txn_vectorized(df_transaction, acct_to_node, cutoff_times=None):
    """Pre-compute node indices and cutoff masks for vectorized operations.

    Returns df with added columns: from_node, to_node, txn_time_val, from_valid, to_valid
    """
    df = df_transaction.copy()
    df["from_node"] = df["from_acct"].map(acct_to_node)
    df["to_node"] = df["to_acct"].map(acct_to_node)

    # Unified timestamp
    if "txn_timestamp" in df.columns:
        df["txn_time_val"] = df["txn_timestamp"]
    elif "txn_date" in df.columns:
        df["txn_time_val"] = df["txn_date"]
    else:
        df["txn_time_val"] = 0

    # Valid = account exists in graph
    df["from_valid"] = df["from_node"].notna()
    df["to_valid"] = df["to_node"].notna()

    # Apply cutoff times
    if cutoff_times is not None:
        from_node_int = df["from_node"].fillna(0).astype(int).values
        to_node_int = df["to_node"].fillna(0).astype(int).values
        from_cutoff = np.where(df["from_valid"], cutoff_times[from_node_int], -np.inf)
        to_cutoff = np.where(df["to_valid"], cutoff_times[to_node_int], -np.inf)
        df["from_valid"] &= df["txn_time_val"].values <= from_cutoff
        df["to_valid"] &= df["txn_time_val"].values <= to_cutoff

    df["from_node"] = df["from_node"].fillna(-1).astype(int)
    df["to_node"] = df["to_node"].fillna(-1).astype(int)

    return df


def compute_basic_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    acct_type_dict: Dict[str, int],
    cutoff_times: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute basic features (12 dimensions).

    Features:
    0-1. degree_in, degree_out
    2-3. total_amt_in, total_amt_out
    4-5. avg_amt_in, avg_amt_out
    6. txn_count
    7. max_txn_timing_ratio
    8. max_txn_amount
    9. flow_ratio
    10. avg_time_interval
    11. is_esun

    Returns:
        features, node_first_txn_time, node_last_txn_time
    """
    print("\n" + "=" * 60)
    print("Computing basic features (12 dimensions)...")

    num_nodes = len(all_accounts)
    features = np.zeros((num_nodes, 12), dtype=np.float32)
    node_first_txn_time = np.full(num_nodes, np.inf, dtype=np.float32)
    node_last_txn_time = np.full(num_nodes, -np.inf, dtype=np.float32)

    print("  Processing transactions (vectorized)...")
    df_prepared = _prepare_txn_vectorized(df_transaction, acct_to_node, cutoff_times)
    df_prepared["order_id"] = np.arange(len(df_prepared), dtype=np.int64)

    print("  Aggregating in/out statistics...")
    df_from = df_prepared[df_prepared["from_valid"]]
    df_to = df_prepared[df_prepared["to_valid"]]

    degree_out = df_from.groupby("from_node").size()
    total_amt_out = df_from.groupby("from_node")["txn_amt"].sum()
    degree_in = df_to.groupby("to_node").size()
    total_amt_in = df_to.groupby("to_node")["txn_amt"].sum()

    print("  Aggregating timing statistics...")
    df_from_txn = df_from[["from_node", "txn_time_val", "txn_amt", "order_id"]].rename(
        columns={"from_node": "node_idx"}
    )
    df_from_txn["side_order"] = 0
    df_to_txn = df_to[["to_node", "txn_time_val", "txn_amt", "order_id"]].rename(
        columns={"to_node": "node_idx"}
    )
    df_to_txn["side_order"] = 1

    df_txn_node = pd.concat([df_from_txn, df_to_txn], ignore_index=True)

    if len(df_txn_node) > 0:
        df_txn_node = df_txn_node.sort_values(
            ["node_idx", "txn_time_val", "order_id", "side_order"], kind="mergesort"
        )
        time_agg = df_txn_node.groupby("node_idx")["txn_time_val"].agg(
            first_time="min", last_time="max", txn_count="size"
        )
        raw_max_amt = (
            df_txn_node.groupby("node_idx")["txn_amt"].max().rename("raw_max_amt")
        )
        max_amt_series = raw_max_amt.clip(lower=0.0).rename("max_amt")
        max_candidates = df_txn_node.join(raw_max_amt, on="node_idx")
        max_candidates = max_candidates[
            max_candidates["txn_amt"] == max_candidates["raw_max_amt"]
        ]
        first_max = max_candidates.groupby("node_idx", sort=False).first()
        max_amt_time = first_max["txn_time_val"].copy()
        non_positive_nodes = raw_max_amt[raw_max_amt <= 0].index
        if len(non_positive_nodes) > 0:
            max_amt_time.loc[non_positive_nodes] = time_agg.loc[
                non_positive_nodes, "first_time"
            ]

        time_span = time_agg["last_time"] - time_agg["first_time"]
        max_txn_timing_ratio = pd.Series(0.0, index=time_agg.index, dtype=np.float64)
        positive_span = time_span > 0
        max_txn_timing_ratio.loc[positive_span] = (
            max_amt_time.loc[positive_span] - time_agg.loc[positive_span, "first_time"]
        ) / time_span.loc[positive_span]
        max_txn_timing_ratio.loc[~positive_span] = 0.5
        avg_time_interval = pd.Series(0.0, index=time_agg.index, dtype=np.float64)
        avg_time_interval.loc[positive_span] = (
            time_span.loc[positive_span] / time_agg.loc[positive_span, "txn_count"]
        )

        first_vals = time_agg["first_time"]
        last_vals = time_agg["last_time"]
    else:
        time_agg = pd.DataFrame(columns=["first_time", "last_time", "txn_count"])
        max_amt_series = pd.Series(dtype=np.float64)
        max_txn_timing_ratio = pd.Series(dtype=np.float64)
        avg_time_interval = pd.Series(dtype=np.float64)
        first_vals = pd.Series(dtype=np.float64)
        last_vals = pd.Series(dtype=np.float64)

    print("  Computing features for each account...")
    node_indices = np.arange(num_nodes)
    deg_in_arr = degree_in.reindex(node_indices, fill_value=0).to_numpy(
        dtype=np.float64
    )
    deg_out_arr = degree_out.reindex(node_indices, fill_value=0).to_numpy(
        dtype=np.float64
    )
    amt_in_arr = total_amt_in.reindex(node_indices, fill_value=0.0).to_numpy(
        dtype=np.float64
    )
    amt_out_arr = total_amt_out.reindex(node_indices, fill_value=0.0).to_numpy(
        dtype=np.float64
    )
    txn_count_arr = deg_in_arr + deg_out_arr

    avg_in_arr = np.divide(
        amt_in_arr,
        deg_in_arr,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=deg_in_arr > 0,
    )
    avg_out_arr = np.divide(
        amt_out_arr,
        deg_out_arr,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=deg_out_arr > 0,
    )

    max_ratio_arr = max_txn_timing_ratio.reindex(node_indices, fill_value=0.0).to_numpy(
        dtype=np.float64
    )
    max_amt_arr = max_amt_series.reindex(node_indices, fill_value=0.0).to_numpy(
        dtype=np.float64
    )
    avg_interval_arr = avg_time_interval.reindex(node_indices, fill_value=0.0).to_numpy(
        dtype=np.float64
    )

    flow_ratio_arr = np.divide(
        deg_out_arr,
        txn_count_arr,
        out=np.full(num_nodes, 0.5, dtype=np.float64),
        where=txn_count_arr > 0,
    )

    node_first_txn_time[:] = first_vals.reindex(
        node_indices, fill_value=np.inf
    ).to_numpy(dtype=np.float32)
    node_last_txn_time[:] = last_vals.reindex(
        node_indices, fill_value=-np.inf
    ).to_numpy(dtype=np.float32)

    is_esun_arr = np.zeros(num_nodes, dtype=np.float64)
    for acct in all_accounts:
        node_idx = acct_to_node[acct]
        is_esun_arr[node_idx] = 1.0 if acct_type_dict.get(acct, 2) == 1 else 0.0

    features[:, 0] = deg_in_arr
    features[:, 1] = deg_out_arr
    features[:, 2] = amt_in_arr
    features[:, 3] = amt_out_arr
    features[:, 4] = avg_in_arr
    features[:, 5] = avg_out_arr
    features[:, 6] = txn_count_arr
    features[:, 7] = max_ratio_arr
    features[:, 8] = max_amt_arr
    features[:, 9] = flow_ratio_arr
    features[:, 10] = avg_interval_arr
    features[:, 11] = is_esun_arr

    print(f"  Done. Shape: {features.shape}")
    return features, node_first_txn_time, node_last_txn_time


def compute_balance_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    cutoff_times: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Compute balance features (5 dimensions).

    Features:
    0. current_balance
    1. max_balance
    2. min_balance
    3. balance_volatility
    4. rapid_drain_flag
    """
    print("\n" + "=" * 60)
    print("Computing balance features (5 dimensions)...")

    num_nodes = len(all_accounts)
    balance_features = np.zeros((num_nodes, 5), dtype=np.float32)

    df_sorted = (
        df_transaction.sort_values("txn_timestamp")
        if "txn_timestamp" in df_transaction.columns
        else df_transaction.sort_values("txn_date")
    )

    print("  Building balance histories (vectorized)...")
    df_prepared = _prepare_txn_vectorized(df_sorted, acct_to_node, cutoff_times)
    df_prepared["order_id"] = np.arange(len(df_prepared), dtype=np.int64)

    df_out_events = df_prepared[df_prepared["from_valid"]][
        ["from_node", "txn_time_val", "txn_amt", "order_id"]
    ].rename(columns={"from_node": "node_idx"})
    df_out_events["signed_amt"] = -df_out_events["txn_amt"]
    df_out_events["side_order"] = 0

    df_in_events = df_prepared[df_prepared["to_valid"]][
        ["to_node", "txn_time_val", "txn_amt", "order_id"]
    ].rename(columns={"to_node": "node_idx"})
    df_in_events["signed_amt"] = df_in_events["txn_amt"]
    df_in_events["side_order"] = 1

    events = pd.concat([df_out_events, df_in_events], ignore_index=True)

    print("  Computing balance features...")
    rapid_drain_count = 0
    if len(events) > 0:
        events = events.sort_values(
            ["node_idx", "txn_time_val", "order_id", "side_order"], kind="mergesort"
        )
        events["balance"] = events.groupby("node_idx")["signed_amt"].cumsum()

        grouped = events.groupby("node_idx")
        current_balance = grouped["balance"].last()
        max_balance = grouped["balance"].max()
        min_balance = grouped["balance"].min()
        balance_std = grouped["balance"].agg(
            lambda x: np.std(x.values) if len(x) > 1 else 0.0
        )
        final_time = grouped["txn_time_val"].last()

        max_candidates = events.join(max_balance.rename("max_balance"), on="node_idx")
        max_candidates = max_candidates[
            max_candidates["balance"] == max_candidates["max_balance"]
        ]
        time_of_max = max_candidates.groupby("node_idx", sort=False)[
            "txn_time_val"
        ].first()

        node_indices = np.arange(num_nodes)
        balance_features[:, 0] = current_balance.reindex(
            node_indices, fill_value=0.0
        ).to_numpy(dtype=np.float32)
        balance_features[:, 1] = max_balance.reindex(
            node_indices, fill_value=0.0
        ).to_numpy(dtype=np.float32)
        balance_features[:, 2] = min_balance.reindex(
            node_indices, fill_value=0.0
        ).to_numpy(dtype=np.float32)
        balance_features[:, 3] = balance_std.reindex(
            node_indices, fill_value=0.0
        ).to_numpy(dtype=np.float32)

        rapid_mask = (
            (max_balance > 10000)
            & (current_balance < 1000)
            & ((final_time - time_of_max.reindex(max_balance.index)) < 7 * 86400)
        )
        rapid_nodes = rapid_mask[rapid_mask].index.astype(int)
        balance_features[rapid_nodes, 4] = 1.0
        rapid_drain_count = len(rapid_nodes)

    print(f"  Done. Rapid drain: {rapid_drain_count}")
    return balance_features


def compute_multiscale_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    cutoff_times: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Compute multi-scale temporal features (9 dimensions).

    Features (for 1d, 7d, 30d windows):
    - txn_count
    - avg_amt
    - unique_counterparties
    """
    print("\n" + "=" * 60)
    print("Computing multi-scale features (9 dimensions)...")

    num_nodes = len(all_accounts)
    multiscale_features = np.zeros((num_nodes, 9), dtype=np.float32)

    print("  Collecting account transactions (vectorized)...")
    df_prepared = _prepare_txn_vectorized(df_transaction, acct_to_node, cutoff_times)

    df_from = df_prepared[df_prepared["from_valid"]][
        ["from_node", "txn_time_val", "txn_amt", "to_acct"]
    ].rename(columns={"from_node": "node_idx", "to_acct": "counterparty"})
    df_to = df_prepared[df_prepared["to_valid"]][
        ["to_node", "txn_time_val", "txn_amt", "from_acct"]
    ].rename(columns={"to_node": "node_idx", "from_acct": "counterparty"})

    events = pd.concat([df_from, df_to], ignore_index=True)

    windows = {"1d": 86400, "7d": 7 * 86400, "30d": 30 * 86400}

    print("  Computing features...")
    if len(events) > 0:
        latest_time = (
            events.groupby("node_idx")["txn_time_val"].max().rename("latest_time")
        )
        events = events.join(latest_time, on="node_idx")

        node_indices = np.arange(num_nodes)
        for window_idx, (window_name, window_seconds) in enumerate(windows.items()):
            print(f"    Window {window_name}...")
            window_start = events["latest_time"] - window_seconds
            window_events = events[events["txn_time_val"] >= window_start]

            if len(window_events) == 0:
                continue

            agg = window_events.groupby("node_idx").agg(
                txn_count=("txn_amt", "size"),
                avg_amt=("txn_amt", "mean"),
                unique_counterparties=("counterparty", "nunique"),
            )

            multiscale_features[:, window_idx * 3] = (
                agg["txn_count"]
                .reindex(node_indices, fill_value=0)
                .to_numpy(dtype=np.float32)
            )
            multiscale_features[:, window_idx * 3 + 1] = (
                agg["avg_amt"]
                .reindex(node_indices, fill_value=0.0)
                .to_numpy(dtype=np.float32)
            )
            multiscale_features[:, window_idx * 3 + 2] = (
                agg["unique_counterparties"]
                .reindex(node_indices, fill_value=0)
                .to_numpy(dtype=np.float32)
            )

    print(f"  Done. Shape: {multiscale_features.shape}")
    return multiscale_features


def compute_graph_structure_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    cutoff_times: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Compute graph structure features (4 dimensions).

    Features:
    0. pagerank
    1. betweenness
    2. clustering_coef
    3. k_core
    """
    print("\n" + "=" * 60)
    print("Computing graph structure features (4 dimensions)...")

    num_nodes = len(all_accounts)
    graph_features = np.zeros((num_nodes, 4), dtype=np.float32)

    all_accounts_set = set(all_accounts)

    print("  Building NetworkX graph...")
    G = nx.DiGraph()
    G.add_nodes_from(range(num_nodes))

    df_filtered = df_transaction[
        df_transaction["from_acct"].isin(all_accounts_set)
        & df_transaction["to_acct"].isin(all_accounts_set)
    ].copy()

    df_filtered["from_node"] = df_filtered["from_acct"].map(acct_to_node)
    df_filtered["to_node"] = df_filtered["to_acct"].map(acct_to_node)

    if cutoff_times is not None:
        txn_time = df_filtered.get("txn_timestamp", df_filtered.get("txn_date", 0))
        from_nodes = df_filtered["from_node"].values
        to_nodes = df_filtered["to_node"].values
        cutoff_src = cutoff_times[from_nodes]
        cutoff_dst = cutoff_times[to_nodes]
        keep_mask = txn_time.values <= np.minimum(cutoff_src, cutoff_dst)
        df_filtered = df_filtered[keep_mask]

    edge_list = list(zip(df_filtered["from_node"], df_filtered["to_node"]))
    G.add_edges_from(edge_list)

    print(f"  Graph: {num_nodes:,} nodes, {len(edge_list):,} edges")

    print("  Computing PageRank...")
    pagerank = nx.pagerank(G, alpha=0.85, max_iter=100)
    for node_idx in range(num_nodes):
        graph_features[node_idx, 0] = pagerank.get(node_idx, 0.0)

    print("  Computing Betweenness (k=100)...")
    betweenness = nx.betweenness_centrality(G, normalized=True, k=min(100, num_nodes))
    for node_idx in range(num_nodes):
        graph_features[node_idx, 1] = betweenness.get(node_idx, 0.0)

    print("  Computing Clustering...")
    G_undirected = G.to_undirected()
    clustering_raw = nx.clustering(G_undirected)
    clustering = clustering_raw if isinstance(clustering_raw, dict) else {}
    for node_idx in range(num_nodes):
        graph_features[node_idx, 2] = clustering.get(node_idx, 0.0)

    print("  Computing K-core...")
    core_number = nx.core_number(G_undirected)
    for node_idx in range(num_nodes):
        graph_features[node_idx, 3] = core_number.get(node_idx, 0)

    print(f"  Done. Shape: {graph_features.shape}")
    return graph_features


def compute_flow_pattern_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    cutoff_times: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Compute flow pattern features (4 dimensions).

    Features:
    0. inflow_concentration
    1. outflow_concentration
    2. flow_reversal_count
    3. same_day_in_out_ratio
    """
    print("\n" + "=" * 60)
    print("Computing flow pattern features (4 dimensions)...")

    num_nodes = len(all_accounts)
    flow_features = np.zeros((num_nodes, 4), dtype=np.float32)

    print("  Collecting flow statistics (vectorized)...")
    df_prepared = _prepare_txn_vectorized(df_transaction, acct_to_node, cutoff_times)

    if "txn_date" in df_prepared.columns:
        txn_date_vals = df_prepared["txn_date"]
    else:
        txn_date_vals = pd.Series(0, index=df_prepared.index)

    df_in = df_prepared[df_prepared["to_valid"]][
        ["to_node", "from_acct", "txn_amt"]
    ].copy()
    df_in["txn_date"] = txn_date_vals[df_prepared["to_valid"]]
    df_in = df_in.rename(columns={"to_node": "node_idx", "from_acct": "partner"})

    df_out = df_prepared[df_prepared["from_valid"]][
        ["from_node", "to_acct", "txn_amt"]
    ].copy()
    df_out["txn_date"] = txn_date_vals[df_prepared["from_valid"]]
    df_out = df_out.rename(columns={"from_node": "node_idx", "to_acct": "partner"})

    print("  Computing same-day in/out patterns...")
    node_indices = np.arange(num_nodes)

    total_inflow = (
        df_in.groupby("node_idx")["txn_amt"].sum()
        if len(df_in) > 0
        else pd.Series(dtype=np.float64)
    )
    total_outflow = (
        df_out.groupby("node_idx")["txn_amt"].sum()
        if len(df_out) > 0
        else pd.Series(dtype=np.float64)
    )

    inflow_by_partner = (
        df_in.groupby(["node_idx", "partner"])["txn_amt"]
        .sum()
        .rename("amt")
        .reset_index()
        if len(df_in) > 0
        else pd.DataFrame(columns=["node_idx", "partner", "amt"])
    )
    outflow_by_partner = (
        df_out.groupby(["node_idx", "partner"])["txn_amt"]
        .sum()
        .rename("amt")
        .reset_index()
        if len(df_out) > 0
        else pd.DataFrame(columns=["node_idx", "partner", "amt"])
    )

    if len(inflow_by_partner) > 0:
        top3_in = inflow_by_partner.sort_values(
            ["node_idx", "amt"], ascending=[True, False]
        )
        top3_in = top3_in.groupby("node_idx").head(3).groupby("node_idx")["amt"].sum()
        inflow_conc = np.divide(
            top3_in.reindex(node_indices, fill_value=0.0).to_numpy(dtype=np.float64),
            total_inflow.reindex(node_indices, fill_value=0.0).to_numpy(
                dtype=np.float64
            ),
            out=np.zeros(num_nodes, dtype=np.float64),
            where=total_inflow.reindex(node_indices, fill_value=0.0).to_numpy(
                dtype=np.float64
            )
            > 0,
        )
    else:
        inflow_conc = np.zeros(num_nodes, dtype=np.float64)

    if len(outflow_by_partner) > 0:
        top3_out = outflow_by_partner.sort_values(
            ["node_idx", "amt"], ascending=[True, False]
        )
        top3_out = top3_out.groupby("node_idx").head(3).groupby("node_idx")["amt"].sum()
        outflow_conc = np.divide(
            top3_out.reindex(node_indices, fill_value=0.0).to_numpy(dtype=np.float64),
            total_outflow.reindex(node_indices, fill_value=0.0).to_numpy(
                dtype=np.float64
            ),
            out=np.zeros(num_nodes, dtype=np.float64),
            where=total_outflow.reindex(node_indices, fill_value=0.0).to_numpy(
                dtype=np.float64
            )
            > 0,
        )
    else:
        outflow_conc = np.zeros(num_nodes, dtype=np.float64)

    in_pairs = (
        inflow_by_partner[["node_idx", "partner"]].drop_duplicates()
        if len(inflow_by_partner) > 0
        else pd.DataFrame(columns=["node_idx", "partner"])
    )
    out_pairs = (
        outflow_by_partner[["node_idx", "partner"]].drop_duplicates()
        if len(outflow_by_partner) > 0
        else pd.DataFrame(columns=["node_idx", "partner"])
    )

    if len(in_pairs) > 0 and len(out_pairs) > 0:
        reversal_pairs = in_pairs.merge(
            out_pairs, on=["node_idx", "partner"], how="inner"
        )
        reversal_count = reversal_pairs.groupby("node_idx").size()
    else:
        reversal_count = pd.Series(dtype=np.int64)

    if len(df_in) > 0 and len(df_out) > 0:
        in_daily = df_in[["node_idx", "txn_date", "partner"]].drop_duplicates()
        out_daily = df_out[["node_idx", "txn_date", "partner"]].drop_duplicates()
        same_day_pairs = in_daily.merge(
            out_daily, on=["node_idx", "txn_date", "partner"], how="inner"
        )
        same_day_count = (
            same_day_pairs.groupby(["node_idx", "txn_date"])
            .size()
            .groupby("node_idx")
            .sum()
        )
    else:
        same_day_count = pd.Series(dtype=np.int64)

    in_partner_count = (
        in_pairs.groupby("node_idx").size()
        if len(in_pairs) > 0
        else pd.Series(dtype=np.int64)
    )
    out_partner_count = (
        out_pairs.groupby("node_idx").size()
        if len(out_pairs) > 0
        else pd.Series(dtype=np.int64)
    )
    total_partner_count = in_partner_count.add(out_partner_count, fill_value=0)

    print("  Computing features...")
    flow_features[:, 0] = inflow_conc.astype(np.float32)
    flow_features[:, 1] = outflow_conc.astype(np.float32)
    flow_features[:, 2] = reversal_count.reindex(node_indices, fill_value=0).to_numpy(
        dtype=np.float32
    )
    flow_features[:, 3] = np.divide(
        same_day_count.reindex(node_indices, fill_value=0).to_numpy(dtype=np.float64),
        total_partner_count.reindex(node_indices, fill_value=0).to_numpy(
            dtype=np.float64
        ),
        out=np.zeros(num_nodes, dtype=np.float64),
        where=total_partner_count.reindex(node_indices, fill_value=0).to_numpy(
            dtype=np.float64
        )
        > 0,
    ).astype(np.float32)

    print(f"  Done. Shape: {flow_features.shape}")
    return flow_features


def compute_ratio_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    cutoff_times: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Compute ratio-based features (6 dimensions).

    Features:
    0. out_cv
    1. out_self_ratio
    2. out_concentration
    3. in_cv
    4. in_self_ratio
    5. in_concentration
    """
    print("\n" + "=" * 60)
    print("Computing ratio features (6 dimensions)...")

    num_nodes = len(all_accounts)
    ratio_features = np.zeros((num_nodes, 6), dtype=np.float32)
    print("  Collecting transaction stats (vectorized)...")
    df_prepared = _prepare_txn_vectorized(df_transaction, acct_to_node, cutoff_times)
    df_prepared["amt_sq"] = df_prepared["txn_amt"] * df_prepared["txn_amt"]

    if "is_self_txn" in df_prepared.columns:
        df_prepared["is_self_flag"] = df_prepared["is_self_txn"].astype(str) == "Y"
    else:
        df_prepared["is_self_flag"] = False

    df_out = df_prepared[df_prepared["from_valid"]][
        ["from_node", "to_acct", "txn_amt", "amt_sq", "is_self_flag"]
    ].rename(columns={"from_node": "node_idx", "to_acct": "partner"})
    df_in = df_prepared[df_prepared["to_valid"]][
        ["to_node", "from_acct", "txn_amt", "amt_sq", "is_self_flag"]
    ].rename(columns={"to_node": "node_idx", "from_acct": "partner"})

    out_agg = (
        df_out.groupby("node_idx").agg(
            count=("txn_amt", "size"),
            amt_sum=("txn_amt", "sum"),
            amt_sq_sum=("amt_sq", "sum"),
            self_count=("is_self_flag", "sum"),
            partner_count=("partner", "nunique"),
        )
        if len(df_out) > 0
        else pd.DataFrame(
            columns=["count", "amt_sum", "amt_sq_sum", "self_count", "partner_count"]
        )
    )
    in_agg = (
        df_in.groupby("node_idx").agg(
            count=("txn_amt", "size"),
            amt_sum=("txn_amt", "sum"),
            amt_sq_sum=("amt_sq", "sum"),
            self_count=("is_self_flag", "sum"),
            partner_count=("partner", "nunique"),
        )
        if len(df_in) > 0
        else pd.DataFrame(
            columns=["count", "amt_sum", "amt_sq_sum", "self_count", "partner_count"]
        )
    )

    print("  Computing ratios...")
    eps = 1e-6
    node_indices = np.arange(num_nodes)

    out_count = (
        out_agg.get("count", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0)
        .to_numpy(dtype=np.float64)
    )
    out_sum = (
        out_agg.get("amt_sum", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0.0)
        .to_numpy(dtype=np.float64)
    )
    out_sum_sq = (
        out_agg.get("amt_sq_sum", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0.0)
        .to_numpy(dtype=np.float64)
    )
    out_self = (
        out_agg.get("self_count", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0)
        .to_numpy(dtype=np.float64)
    )
    out_partner_count = (
        out_agg.get("partner_count", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0)
        .to_numpy(dtype=np.float64)
    )

    in_count = (
        in_agg.get("count", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0)
        .to_numpy(dtype=np.float64)
    )
    in_sum = (
        in_agg.get("amt_sum", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0.0)
        .to_numpy(dtype=np.float64)
    )
    in_sum_sq = (
        in_agg.get("amt_sq_sum", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0.0)
        .to_numpy(dtype=np.float64)
    )
    in_self = (
        in_agg.get("self_count", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0)
        .to_numpy(dtype=np.float64)
    )
    in_partner_count = (
        in_agg.get("partner_count", pd.Series(dtype=np.float64))
        .reindex(node_indices, fill_value=0)
        .to_numpy(dtype=np.float64)
    )

    out_mean = np.divide(
        out_sum,
        out_count,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=out_count > 0,
    )
    out_var = np.maximum(
        np.divide(
            out_sum_sq,
            out_count,
            out=np.zeros(num_nodes, dtype=np.float64),
            where=out_count > 0,
        )
        - out_mean**2,
        0.0,
    )
    out_std = np.sqrt(out_var)

    in_mean = np.divide(
        in_sum, in_count, out=np.zeros(num_nodes, dtype=np.float64), where=in_count > 0
    )
    in_var = np.maximum(
        np.divide(
            in_sum_sq,
            in_count,
            out=np.zeros(num_nodes, dtype=np.float64),
            where=in_count > 0,
        )
        - in_mean**2,
        0.0,
    )
    in_std = np.sqrt(in_var)

    ratio_features[:, 0] = np.divide(
        out_std,
        out_mean + eps,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=out_count > 0,
    ).astype(np.float32)
    ratio_features[:, 1] = np.divide(
        out_self,
        out_count,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=out_count > 0,
    ).astype(np.float32)
    ratio_features[:, 2] = np.divide(
        out_count,
        out_partner_count + eps,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=out_count > 0,
    ).astype(np.float32)

    ratio_features[:, 3] = np.divide(
        in_std,
        in_mean + eps,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=in_count > 0,
    ).astype(np.float32)
    ratio_features[:, 4] = np.divide(
        in_self, in_count, out=np.zeros(num_nodes, dtype=np.float64), where=in_count > 0
    ).astype(np.float32)
    ratio_features[:, 5] = np.divide(
        in_count,
        in_partner_count + eps,
        out=np.zeros(num_nodes, dtype=np.float64),
        where=in_count > 0,
    ).astype(np.float32)

    print("  Standardizing ratio features...")
    means = ratio_features.mean(axis=0)
    stds = ratio_features.std(axis=0)
    stds[stds < 1e-10] = 1.0
    ratio_features = (ratio_features - means) / stds

    print(f"  Done. Shape: {ratio_features.shape}")
    return ratio_features


def compute_time_distribution_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    cutoff_times: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Compute hourly transaction distribution features (6 dimensions).

    Features:
    0. ratio_00_06 (Night/Early Morning)
    1. ratio_06_12 (Morning)
    2. ratio_12_18 (Afternoon)
    3. ratio_18_24 (Evening)
    4. peak_hour (Hour with most transactions, 0-23 normalized to 0-1)
    5. max_amt_hour (Hour with max transaction amount, 0-23 normalized to 0-1)

    Note: Assumes txn_timestamp is available (seconds since epoch or relative).
    """
    print("\n" + "=" * 60)
    print("Computing time distribution features (6 dimensions)...")

    num_nodes = len(all_accounts)
    time_features = np.zeros((num_nodes, 6), dtype=np.float32)

    all_accounts_set = set(all_accounts)

    # Optimized Vectorized Implementation
    print("  Vectorizing time computation...")

    # Ensure txn_timestamp exists
    if "txn_timestamp" not in df_transaction.columns:
        if "txn_time" in df_transaction.columns:
            print("    Parsing txn_time values...")
            seconds = _txn_time_series_to_seconds(df_transaction["txn_time"])
            df_transaction["hour"] = (seconds // 3600).clip(0, 23).astype(int)
        else:
            df_transaction["hour"] = 0
    else:
        # Vectorized hour extraction
        # Assumes txn_timestamp is seconds. Hour = (ts % 86400) // 3600
        # Handle NaN by filling with 0
        ts = df_transaction["txn_timestamp"].fillna(0).astype(int)
        df_transaction["hour"] = ((ts % 86400) // 3600).clip(0, 23).astype(int)

    cutoff_by_acct = None
    if cutoff_times is not None:
        cutoff_by_acct = {acct: cutoff_times[idx] for acct, idx in acct_to_node.items()}

    # 1. Hourly Counts (Pivot Table)
    print("  Aggregating hourly counts...")
    # Filter only relevant accounts
    df_filtered = df_transaction[
        df_transaction["from_acct"].isin(all_accounts_set)
        | df_transaction["to_acct"].isin(all_accounts_set)
    ][["from_acct", "to_acct", "txn_amt", "hour"]].copy()

    df_filtered["txn_time"] = df_transaction.get(
        "txn_timestamp", df_transaction.get("txn_date", 0)
    )

    # We need to count for both from and to
    # Melt to get (acct, hour, amt)
    # This doubles the rows but allows single groupby
    df_from = df_filtered[["from_acct", "hour", "txn_amt", "txn_time"]].rename(
        columns={"from_acct": "acct"}
    )
    df_to = df_filtered[["to_acct", "hour", "txn_amt", "txn_time"]].rename(
        columns={"to_acct": "acct"}
    )

    if cutoff_by_acct is not None:
        df_from["cutoff"] = df_from["acct"].map(cutoff_by_acct)
        df_to["cutoff"] = df_to["acct"].map(cutoff_by_acct)
        df_from = df_from[df_from["txn_time"] <= df_from["cutoff"]]
        df_to = df_to[df_to["txn_time"] <= df_to["cutoff"]]

    df_melt = pd.concat([df_from, df_to])

    # Filter again for valid accounts (in case of None/NaN)
    df_melt = df_melt[df_melt["acct"].isin(all_accounts_set)]

    # Groupby
    # Size = count per hour
    # Max = max amt per hour
    print("  Grouping by account and hour...")
    hourly_stats = (
        df_melt.groupby(["acct", "hour"])
        .agg(count=("txn_amt", "size"), max_amt=("txn_amt", "max"))
        .reset_index()
    )

    # Process into features
    print("  Computing final features...")

    # We can use pivot tables
    count_pivot = hourly_stats.pivot(
        index="acct", columns="hour", values="count"
    ).fillna(0)
    max_amt_pivot = hourly_stats.pivot(
        index="acct", columns="hour", values="max_amt"
    ).fillna(0)

    # Ensure all columns 0-23 exist
    for h in range(24):
        if h not in count_pivot.columns:
            count_pivot[h] = 0
        if h not in max_amt_pivot.columns:
            max_amt_pivot[h] = 0

    # Sort columns
    count_pivot = count_pivot[sorted(count_pivot.columns)]
    max_amt_pivot = max_amt_pivot[sorted(max_amt_pivot.columns)]

    # Iterate accounts to fill feature matrix
    # This is much faster than iterating transactions

    # Pre-map acct indices
    acct_indices = {acct: idx for acct, idx in acct_to_node.items()}

    for acct in count_pivot.index:
        if acct not in acct_indices:
            continue

        node_idx = acct_indices[acct]
        counts = count_pivot.loc[acct].values
        max_amts = max_amt_pivot.loc[acct].values

        total_txns = counts.sum()

        if total_txns > 0:
            # 1. Ratios
            time_features[node_idx, 0] = counts[0:6].sum() / total_txns
            time_features[node_idx, 1] = counts[6:12].sum() / total_txns
            time_features[node_idx, 2] = counts[12:18].sum() / total_txns
            time_features[node_idx, 3] = counts[18:24].sum() / total_txns

            # 2. Peak Hour
            peak_hour = np.argmax(counts)
            time_features[node_idx, 4] = peak_hour / 23.0

            # 3. Max Amount Hour
            max_amt_hour = np.argmax(max_amts)
            time_features[node_idx, 5] = max_amt_hour / 23.0

    print(f"  Done. Shape: {time_features.shape}")
    return time_features


def compute_edge_attributes(
    df_transaction: pd.DataFrame,
    acct_to_node: Dict[str, int],
    standardize: bool = True,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, Dict[str, object]]:
    """Build multi-graph edge tensors from raw transactions.

    Returns:
        src_nodes, dst_nodes, edge_attr, meta
    where each transaction row corresponds to exactly one edge.
    """
    df = df_transaction.copy()

    if "txn_timestamp" not in df.columns:
        if "txn_time" in df.columns:
            seconds = _txn_time_series_to_seconds(df["txn_time"])
            df["txn_timestamp"] = df["txn_date"].astype(float) * 86400.0 + seconds
        else:
            df["txn_timestamp"] = df["txn_date"].astype(float)

    src_nodes = df["from_acct"].map(acct_to_node)
    dst_nodes = df["to_acct"].map(acct_to_node)
    valid_mask = src_nodes.notna() & dst_nodes.notna()
    if not valid_mask.any():
        raise ValueError("No valid transaction edges after account mapping")

    df_valid = df.loc[valid_mask].copy()
    df_valid["src_node"] = src_nodes[valid_mask].astype(np.int64).to_numpy()
    df_valid["dst_node"] = dst_nodes[valid_mask].astype(np.int64).to_numpy()
    df_valid["txn_timestamp"] = df_valid["txn_timestamp"].astype(float)
    df_valid["txn_amt"] = df_valid["txn_amt"].fillna(0.0).astype(float)
    df_valid["_orig_idx"] = np.arange(len(df_valid), dtype=np.int64)

    sort_cols = ["from_acct", "txn_timestamp", "_orig_idx"]
    sorted_view = df_valid.sort_values(sort_cols, kind="mergesort")
    sorted_view["time_delta"] = (
        sorted_view.groupby("from_acct", sort=False)["txn_timestamp"]
        .diff()
        .fillna(0.0)
        .clip(lower=0.0)
    )
    time_delta_map = sorted_view.set_index("_orig_idx")["time_delta"]
    df_valid["time_delta"] = (
        df_valid["_orig_idx"].map(time_delta_map).fillna(0.0).astype(float)
    )

    currency_series = df_valid["currency_type"].fillna("UNK").astype(str)
    channel_series = df_valid["channel_type"].fillna("UNK").astype(str)
    currency_vocab = sorted(currency_series.unique().tolist())
    channel_vocab = sorted(channel_series.unique().tolist())
    currency_map = {v: i for i, v in enumerate(currency_vocab)}
    channel_map = {v: i for i, v in enumerate(channel_vocab)}

    currency_cat = pd.Categorical(currency_series, categories=currency_vocab)
    channel_cat = pd.Categorical(channel_series, categories=channel_vocab)
    currency_one_hot = pd.get_dummies(currency_cat, prefix="currency", dtype=np.float64)
    channel_one_hot = pd.get_dummies(channel_cat, prefix="channel", dtype=np.float64)

    txn_amt_log1p = np.log1p(
        np.clip(df_valid["txn_amt"].to_numpy(dtype=np.float64), 0.0, None)
    )
    time_delta_raw = np.clip(
        df_valid["time_delta"].to_numpy(dtype=np.float64), 0.0, None
    )
    time_delta = np.log1p(time_delta_raw)

    dense_base = np.stack([txn_amt_log1p, time_delta], axis=1)
    edge_attr_raw = np.concatenate(
        [dense_base, currency_one_hot.to_numpy(), channel_one_hot.to_numpy()],
        axis=1,
    )

    means = edge_attr_raw.mean(axis=0)
    stds = edge_attr_raw.std(axis=0)
    stds[stds < 1e-10] = 1.0
    if standardize:
        edge_attr = (edge_attr_raw - means) / stds
    else:
        edge_attr = edge_attr_raw

    meta: Dict[str, object] = {
        "feature_names": ["txn_amt_log1p", "txn_time_delta_log1p"]
        + list(currency_one_hot.columns)
        + list(channel_one_hot.columns),
        "means": means.tolist(),
        "stds": stds.tolist(),
        "categorical_encoding": "one_hot",
        "currency_vocab": currency_vocab,
        "channel_vocab": channel_vocab,
        "currency_mapping": currency_map,
        "channel_mapping": channel_map,
        "num_edges": int(edge_attr.shape[0]),
        "standardized": bool(standardize),
        "raw_time_delta_min": float(time_delta_raw.min())
        if len(time_delta_raw)
        else 0.0,
        "raw_time_delta_max": float(time_delta_raw.max())
        if len(time_delta_raw)
        else 0.0,
        "time_delta_log1p_min": float(time_delta.min()) if len(time_delta) else 0.0,
        "time_delta_log1p_max": float(time_delta.max()) if len(time_delta) else 0.0,
    }

    return (
        df_valid["src_node"].to_numpy(dtype=np.int64),
        df_valid["dst_node"].to_numpy(dtype=np.int64),
        edge_attr.astype(np.float32),
        {
            **meta,
            "edge_time": df_valid["txn_timestamp"].to_numpy(dtype=np.float32),
        },
    )


def compute_all_features(
    df_transaction: pd.DataFrame,
    all_accounts: List[str],
    acct_to_node: Dict[str, int],
    acct_type_dict: Dict[str, int],
    include_time_features: bool = False,
    cutoff_times: Optional[np.ndarray] = None,
    raw_save_path: Optional[str] = None,
    scaler_save_path: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute base features (34 dims) and optionally include time distribution (+6 dims).

    Returns:
        features_std, node_first_txn_time, node_last_txn_time
    """
    print("\n" + "=" * 60)
    if include_time_features:
        print("COMPUTING ALL FEATURES (40 dimensions)")
    else:
        print("COMPUTING BASE FEATURES (34 dimensions)")
    print("=" * 60)

    # Basic features (12)
    basic_features, node_first_txn_time, node_last_txn_time = compute_basic_features(
        df_transaction, all_accounts, acct_to_node, acct_type_dict, cutoff_times
    )

    # Balance features (5)
    balance_features = compute_balance_features(
        df_transaction, all_accounts, acct_to_node, cutoff_times
    )

    # Multi-scale features (9)
    multiscale_features = compute_multiscale_features(
        df_transaction, all_accounts, acct_to_node, cutoff_times
    )

    # Graph structure features (4)
    graph_features = compute_graph_structure_features(
        df_transaction, all_accounts, acct_to_node, cutoff_times
    )

    # Flow pattern features (4)
    flow_features = compute_flow_pattern_features(
        df_transaction, all_accounts, acct_to_node, cutoff_times
    )

    feature_groups = [
        basic_features,
        balance_features,
        multiscale_features,
        graph_features,
        flow_features,
    ]

    time_features = None
    if include_time_features:
        # Time distribution features (6)
        time_features = compute_time_distribution_features(
            df_transaction, all_accounts, acct_to_node, cutoff_times
        )
        feature_groups.append(time_features)

    features_all = np.concatenate(feature_groups, axis=1)

    print("\n" + "=" * 60)
    print("Feature Summary:")
    print(f"  Basic: {basic_features.shape}")
    print(f"  Balance: {balance_features.shape}")
    print(f"  Multi-scale: {multiscale_features.shape}")
    print(f"  Graph structure: {graph_features.shape}")
    print(f"  Flow pattern: {flow_features.shape}")
    if include_time_features:
        assert time_features is not None
        print(f"  Time distribution: {time_features.shape}")
    else:
        print("  Time distribution: [skipped]")
    print(f"  Total: {features_all.shape}")

    # Standardize
    print("\nStandardizing features...")
    if raw_save_path is not None:
        import torch as _torch

        _torch.save(
            _torch.tensor(features_all, dtype=_torch.float32),
            raw_save_path,
        )
        print(f"  Saved raw features: {raw_save_path}")
    feature_means = features_all.mean(axis=0)
    feature_stds = features_all.std(axis=0)
    feature_stds[feature_stds < 1e-10] = 1.0

    features_std = (features_all - feature_means) / feature_stds

    print(f"  Mean: {features_std.mean():.6f}")
    print(f"  Std: {features_std.std():.6f}")

    if scaler_save_path is not None:
        scaler_payload = {
            "feature_means": feature_means.tolist(),
            "feature_stds": feature_stds.tolist(),
            "num_features": int(features_std.shape[1]),
            "standardization": "zscore",
        }
        with open(scaler_save_path, "w", encoding="utf-8") as f:
            json.dump(scaler_payload, f, indent=2)
        print(f"  Saved node scaler params: {scaler_save_path}")

    return features_std, node_first_txn_time, node_last_txn_time
