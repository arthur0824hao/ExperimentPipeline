#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyright: reportMissingImports=false
"""
Base Data Loader - Load and prepare raw data

Author: Claude
Date: 2026-01-13
"""

import os
import pandas as pd
import numpy as np
from collections import defaultdict


def load_csv_files(data_dir):
    """
    Load CSV files from data directory.

    Returns:
        df_transaction, df_alert, df_predict
    """
    print("=" * 60)
    print("Loading CSV files...")

    df_transaction = pd.read_csv(os.path.join(data_dir, "acct_transaction.csv"))
    print(f"  Transactions: {len(df_transaction):,}")

    df_alert = pd.read_csv(os.path.join(data_dir, "acct_alert.csv"))
    print(f"  Alert accounts: {len(df_alert):,}")

    df_predict = pd.read_csv(os.path.join(data_dir, "acct_predict.csv"))
    print(f"  Predict accounts: {len(df_predict):,}")

    return df_transaction, df_alert, df_predict


def load_phase2_csv_files(data_dir):
    """
    Load CSV files from Phase 2 data directory.

    Returns:
        df_transaction, df_alert, df_test_groundtruth
    """
    print("=" * 60)
    print("Loading Phase 2 CSV files...")

    df_transaction = pd.read_csv(os.path.join(data_dir, "acct_transaction.csv"))
    print(f"  Transactions: {len(df_transaction):,}")

    df_alert = pd.read_csv(os.path.join(data_dir, "acct_alert.csv"))
    print(f"  Alert accounts: {len(df_alert):,}")

    # Load Ground Truth for Test Set
    df_test_groundtruth = pd.read_csv(
        os.path.join(data_dir, "acct_test_groundtruth_final.csv")
    )
    print(f"  Test Ground Truth accounts: {len(df_test_groundtruth):,}")

    # Check for expected columns
    if "label" not in df_test_groundtruth.columns:
        print("WARNING: 'label' column not found in ground truth file!")

    return df_transaction, df_alert, df_test_groundtruth


def convert_currency_to_twd(df_transaction):
    """Convert all transaction amounts to TWD."""
    print("\n" + "=" * 60)
    print("Converting currencies to TWD...")

    exchange_rates = {
        "TWD": 1.0,
        "USD": 30.5,
        "CNY": 4.3,
        "EUR": 33.5,
        "JPY": 0.22,
        "HKD": 3.9,
        "GBP": 38.5,
        "SGD": 22.8,
        "AUD": 20.3,
        "CAD": 22.5,
    }

    if "currency_type" in df_transaction.columns:
        df_transaction["txn_amt"] = df_transaction.apply(
            lambda row: row["txn_amt"] * exchange_rates.get(row["currency_type"], 1.0),
            axis=1,
        )
        print("  Done.")

    return df_transaction


def integrate_datetime(df_transaction):
    """Integrate txn_date and txn_time into complete timestamp."""
    print("\n" + "=" * 60)
    print("Integrating datetime...")

    def parse_time_to_seconds(time_str):
        try:
            if pd.isna(time_str):
                return 0
            parts = str(time_str).split(":")
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            return 0
        except:
            return 0

    if "txn_time" in df_transaction.columns:
        df_transaction["txn_timestamp"] = df_transaction.apply(
            lambda row: row["txn_date"] * 86400
            + parse_time_to_seconds(row["txn_time"]),
            axis=1,
        )
    else:
        df_transaction["txn_timestamp"] = df_transaction["txn_date"].astype(float)

    print("  Done.")
    return df_transaction


def compute_other_bank_stats(df_transaction):
    """
    Compute statistics for other bank accounts' interactions with E.SUN.

    This is used for filtering decisions.

    Returns:
        DataFrame with columns:
        - acct: account ID
        - is_esun: whether this is an E.SUN account
        - has_esun_txn: has any transaction with E.SUN
        - esun_txn_count: number of transactions with E.SUN
        - max_esun_txn_amt: max single transaction amount with E.SUN
        - total_esun_amt: total amount with E.SUN
        - has_bidirectional: has both send and receive with E.SUN
        - esun_partner_count: number of unique E.SUN partners
        - last_txn_time: last transaction timestamp
    """
    print("\n" + "=" * 60)
    print("Computing other bank statistics...")

    print("  Processing transactions (vectorized)...")
    if len(df_transaction) == 0:
        df_stats = pd.DataFrame(
            columns=[
                "acct",
                "is_esun",
                "has_esun_txn",
                "esun_txn_count",
                "max_esun_txn_amt",
                "total_esun_amt",
                "has_bidirectional",
                "esun_partner_count",
                "last_txn_time",
            ]
        )
        return df_stats

    # Preserve per-account first-seen order consistent with the original loop
    accts_flat = df_transaction[["from_acct", "to_acct"]].to_numpy().ravel(order="C")
    acct_order = pd.unique(accts_flat)
    df_out = pd.DataFrame(index=pd.Index(acct_order, name="acct"))

    txn_time_col = (
        "txn_timestamp" if "txn_timestamp" in df_transaction.columns else "txn_date"
    )
    if txn_time_col in df_transaction.columns:
        txn_time = df_transaction[txn_time_col]
    else:
        txn_time = pd.Series(0, index=df_transaction.index)

    # is_esun: accounts that appear with acct_type == 1
    esun_from = df_transaction.loc[
        df_transaction["from_acct_type"] == 1, "from_acct"
    ].unique()
    esun_to = df_transaction.loc[
        df_transaction["to_acct_type"] == 1, "to_acct"
    ].unique()
    esun_accts = set(np.concatenate([esun_from, esun_to], axis=0))
    df_out["is_esun"] = df_out.index.isin(esun_accts)

    # last_txn_time: max timestamp across from/to participation
    df_times = pd.concat(
        [
            pd.DataFrame(
                {"acct": df_transaction["from_acct"].values, "t": txn_time.to_numpy()}
            ),
            pd.DataFrame(
                {"acct": df_transaction["to_acct"].values, "t": txn_time.to_numpy()}
            ),
        ],
        ignore_index=True,
    )
    last_txn_time = df_times.groupby("acct", sort=False)["t"].max()
    df_out["last_txn_time"] = last_txn_time.reindex(df_out.index).fillna(0)

    # Cross-bank interactions with E.SUN (other bank acct_type == 2)
    mask_12 = (df_transaction["from_acct_type"] == 1) & (
        df_transaction["to_acct_type"] == 2
    )
    mask_21 = (df_transaction["from_acct_type"] == 2) & (
        df_transaction["to_acct_type"] == 1
    )

    edges_12 = df_transaction.loc[mask_12, ["to_acct", "from_acct", "txn_amt"]].rename(
        columns={"to_acct": "other_acct", "from_acct": "esun_partner"}
    )
    edges_21 = df_transaction.loc[mask_21, ["from_acct", "to_acct", "txn_amt"]].rename(
        columns={"from_acct": "other_acct", "to_acct": "esun_partner"}
    )
    edges_12["_dir_recv"] = True
    edges_21["_dir_send"] = True
    edges = pd.concat([edges_12, edges_21], ignore_index=True, sort=False)

    if len(edges) == 0:
        df_out["has_esun_txn"] = False
        df_out["esun_txn_count"] = 0
        df_out["max_esun_txn_amt"] = 0.0
        df_out["total_esun_amt"] = 0.0
        df_out["has_bidirectional"] = False
        df_out["esun_partner_count"] = 0
    else:
        grp = edges.groupby("other_acct", sort=False)
        esun_txn_count = grp.size()
        max_esun_txn_amt = grp["txn_amt"].max()
        total_esun_amt = grp["txn_amt"].sum()
        esun_partner_count = grp["esun_partner"].nunique()

        has_recv = edges_12["other_acct"].drop_duplicates().to_numpy()
        has_send = edges_21["other_acct"].drop_duplicates().to_numpy()
        has_bidirectional = df_out.index.isin(has_recv) & df_out.index.isin(has_send)

        df_out["has_esun_txn"] = (
            esun_txn_count.reindex(df_out.index).fillna(0).astype(int) > 0
        )
        df_out["esun_txn_count"] = (
            esun_txn_count.reindex(df_out.index).fillna(0).astype(int)
        )
        df_out["max_esun_txn_amt"] = max_esun_txn_amt.reindex(df_out.index).fillna(0.0)
        df_out["total_esun_amt"] = total_esun_amt.reindex(df_out.index).fillna(0.0)
        df_out["has_bidirectional"] = has_bidirectional
        df_out["esun_partner_count"] = (
            esun_partner_count.reindex(df_out.index).fillna(0).astype(int)
        )

    # Final DataFrame
    print("  Building stats DataFrame...")
    df_stats = df_out.reset_index()

    print(f"\n  Account statistics:")
    print(f"    Total accounts: {len(df_stats):,}")
    print(f"    E.SUN accounts: {df_stats['is_esun'].sum():,}")
    print(
        f"    Other bank with E.SUN txn: {(~df_stats['is_esun'] & df_stats['has_esun_txn']).sum():,}"
    )
    print(
        f"    Other bank without E.SUN txn: {(~df_stats['is_esun'] & ~df_stats['has_esun_txn']).sum():,}"
    )
    print(
        f"    Bidirectional with E.SUN: {(~df_stats['is_esun'] & df_stats['has_bidirectional']).sum():,}"
    )

    return df_stats


def get_max_txn_time(df_transaction):
    """Get maximum transaction time (used for recent_days filter)."""
    return df_transaction["txn_date"].max() * 86400


def compute_temporal_cutoff(df_transaction, df_predict, train_ratio=0.6):
    """
    Compute temporal cutoff for train/val/test split.

    Returns the cutoff time where train_ratio of E.SUN trainable accounts
    are before this time.

    Args:
        df_transaction: Transaction DataFrame with txn_date
        df_predict: Predict accounts (excluded from training)
        train_ratio: Ratio for training set (default: 0.6)

    Returns:
        cutoff_time: Temporal cutoff in days since epoch
    """
    import numpy as np

    predict_accounts = set(df_predict["acct"].unique())
    if len(df_transaction) == 0:
        return 0

    # Vectorized: first txn_date per trainable E.SUN account (exclude predict accounts)
    df_from = df_transaction[
        (df_transaction["from_acct_type"] == 1)
        & (~df_transaction["from_acct"].isin(predict_accounts))
    ][["from_acct", "txn_date"]]
    first_from = df_from.groupby("from_acct", sort=False)["txn_date"].min()

    df_to = df_transaction[
        (df_transaction["to_acct_type"] == 1)
        & (~df_transaction["to_acct"].isin(predict_accounts))
    ][["to_acct", "txn_date"]]
    first_to = df_to.groupby("to_acct", sort=False)["txn_date"].min()

    first_to.index.name = "acct"
    first_from.index.name = "acct"
    acct_first_txn = (
        pd.concat([first_from, first_to], axis=0).groupby(level=0, sort=False).min()
    )

    trainable_times = sorted(acct_first_txn.to_list())
    if len(trainable_times) == 0:
        return 0

    cutoff_idx = int(train_ratio * len(trainable_times))
    return trainable_times[cutoff_idx]
