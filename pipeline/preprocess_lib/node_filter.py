#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyright: reportMissingImports=false
"""
Node Filter - Configurable node filtering strategies

Author: Claude
Date: 2026-01-13
"""

from dataclasses import dataclass
from typing import Set, Optional
import pandas as pd


@dataclass
class FilterConfig:
    """Configuration for node filtering."""

    name: str
    description: str

    # Filter flags
    direct_connect_only: bool = False  # F1: Only keep other-bank with direct E.SUN txn
    min_txn_count: int = 0  # F2: Min transactions with E.SUN (0=disabled)
    min_txn_amount: float = 0.0  # F3: Min single txn amount with E.SUN (0=disabled)
    require_bidirectional: bool = (
        False  # F4: Must have both send and receive with E.SUN
    )
    min_esun_partners: int = 0  # F5: Min unique E.SUN partners (0=disabled)
    recent_days: int = 0  # F6: Only keep accounts active in last N days (0=disabled)

    # Combine logic
    combine_logic: str = "AND"  # "AND" or "OR" for combining F2-F5

    # Special modes
    esun_only: bool = False  # PP15: Only keep E.SUN accounts
    khop_from_fraud: int = 0  # PP13: K-hop subgraph from fraud (0=disabled)


# Predefined filter configurations for all experiments
FILTER_PRESETS = {
    # Baseline
    "PP0_Baseline": FilterConfig(
        name="PP0_Baseline",
        description="No filtering (baseline)",
    ),
    # Single factor experiments
    "PP1_DirectConnect": FilterConfig(
        name="PP1_DirectConnect",
        description="F1: Only other-bank with direct E.SUN transaction",
        direct_connect_only=True,
    ),
    "PP2_TxnCount2": FilterConfig(
        name="PP2_TxnCount2",
        description="F2: At least 2 transactions with E.SUN",
        min_txn_count=2,
    ),
    "PP3_Amount10K": FilterConfig(
        name="PP3_Amount10K",
        description="F3: At least one 10K+ TWD transaction with E.SUN",
        min_txn_amount=10000.0,
    ),
    "PP4_Bidirectional": FilterConfig(
        name="PP4_Bidirectional",
        description="F4: Has bidirectional transactions with E.SUN",
        require_bidirectional=True,
    ),
    "PP5_MultiPartner": FilterConfig(
        name="PP5_MultiPartner",
        description="F5: Transacts with 2+ unique E.SUN accounts",
        min_esun_partners=2,
    ),
    "PP6_Recent60D": FilterConfig(
        name="PP6_Recent60D",
        description="F6: Active in last 60 days",
        recent_days=60,
    ),
    # Combination experiments
    "PP7_Direct_TxnCount2": FilterConfig(
        name="PP7_Direct_TxnCount2",
        description="F1+F2: Direct connect AND 2+ transactions",
        direct_connect_only=True,
        min_txn_count=2,
    ),
    "PP8_Direct_Amount10K": FilterConfig(
        name="PP8_Direct_Amount10K",
        description="F1+F3: Direct connect AND 10K+ amount",
        direct_connect_only=True,
        min_txn_amount=10000.0,
    ),
    "PP9_Direct_Bidir": FilterConfig(
        name="PP9_Direct_Bidir",
        description="F1+F4: Direct connect AND bidirectional",
        direct_connect_only=True,
        require_bidirectional=True,
    ),
    "PP10_TxnOrAmt": FilterConfig(
        name="PP10_TxnOrAmt",
        description="F2 OR F3: 2+ txn OR 10K+ amount",
        min_txn_count=2,
        min_txn_amount=10000.0,
        combine_logic="OR",
    ),
    "PP11_Bidir_Multi": FilterConfig(
        name="PP11_Bidir_Multi",
        description="F4+F5: Bidirectional AND 2+ partners",
        require_bidirectional=True,
        min_esun_partners=2,
    ),
    "PP12_Direct_Recent": FilterConfig(
        name="PP12_Direct_Recent",
        description="F1+F6: Direct connect AND recent 60 days",
        direct_connect_only=True,
        recent_days=60,
    ),
    # Aggressive experiments
    "PP13_KHop2": FilterConfig(
        name="PP13_KHop2",
        description="2-hop subgraph from fraud nodes",
        khop_from_fraud=2,
    ),
    "PP14_Strict": FilterConfig(
        name="PP14_Strict",
        description="F1+F2+F3: Direct + 2+ txn + 10K+ amount",
        direct_connect_only=True,
        min_txn_count=2,
        min_txn_amount=10000.0,
    ),
    "PP15_EsunOnly": FilterConfig(
        name="PP15_EsunOnly",
        description="Only E.SUN accounts (no other-bank)",
        esun_only=True,
    ),
    # Q-series: Quick Validation
    "Q1_PP12_FixedBug": FilterConfig(
        name="Q1_PP12_FixedBug",
        description="PP12 rerun with same_day_in_out bug fixed",
        direct_connect_only=True,
        recent_days=60,
    ),
    "Q2_PP13_Fixed": FilterConfig(
        name="Q2_PP13_Fixed",
        description="2-hop subgraph from fraud (training period only, no leakage)",
        khop_from_fraud=2,
    ),
    "Q3_TimeWindow_30d": FilterConfig(
        name="Q3_TimeWindow_30d",
        description="Active in last 30 days",
        recent_days=30,
    ),
    "Q4_TimeWindow_90d": FilterConfig(
        name="Q4_TimeWindow_90d",
        description="Active in last 90 days",
        recent_days=90,
    ),
    "Q5_Amount5K": FilterConfig(
        name="Q5_Amount5K",
        description="At least one 5K+ TWD transaction with E.SUN",
        min_txn_amount=5000.0,
    ),
    "Q6_Amount20K": FilterConfig(
        name="Q6_Amount20K",
        description="At least one 20K+ TWD transaction with E.SUN",
        min_txn_amount=20000.0,
    ),
    "Q7_Hybrid_Custom": FilterConfig(
        name="Q7_Hybrid_Custom",
        description="3+ txn OR 8K+ amount (custom OR logic)",
        min_txn_count=3,
        min_txn_amount=8000.0,
        combine_logic="OR",
    ),
    # S-series: Time Window Optimization
    "S1_TimeWindow_20d": FilterConfig(
        name="S1_TimeWindow_20d",
        description="Active in last 20 days (shorter window)",
        recent_days=20,
    ),
    "S2_TimeWindow_45d": FilterConfig(
        name="S2_TimeWindow_45d",
        description="Active in last 45 days (interpolate 30d-60d)",
        recent_days=45,
    ),
    "S3_TimeWindow_50d": FilterConfig(
        name="S3_TimeWindow_50d",
        description="Active in last 50 days (interpolate 45d-60d)",
        recent_days=50,
    ),
    "S4_Hybrid_30d_5K": FilterConfig(
        name="S4_Hybrid_30d_5K",
        description="30d AND 5K+ amount (dual filter)",
        recent_days=30,
        min_txn_amount=5000.0,
        combine_logic="AND",
    ),
    "S5_Hybrid_45d_3Txn": FilterConfig(
        name="S5_Hybrid_45d_3Txn",
        description="45d AND 3+ transactions",
        recent_days=45,
        min_txn_count=3,
        combine_logic="AND",
    ),
}


def get_filter_config(preset_name: str) -> FilterConfig:
    """Get a predefined filter configuration by name."""
    if preset_name not in FILTER_PRESETS:
        raise ValueError(
            f"Unknown preset: {preset_name}. Available: {list(FILTER_PRESETS.keys())}"
        )
    return FILTER_PRESETS[preset_name]


def apply_node_filter(
    df_stats: pd.DataFrame,
    config: FilterConfig,
    max_txn_time: Optional[float] = None,
    must_include_accts: Optional[Set[str]] = None,
) -> Set[str]:
    """
    Apply node filter based on configuration.

    Args:
        df_stats: DataFrame from compute_other_bank_stats()
        config: FilterConfig object
        max_txn_time: Maximum transaction timestamp (for recent_days filter)
        must_include_accts: Set of account IDs that MUST be retained (e.g. test set)

    Returns:
        Set of account IDs to retain
    """
    print("\n" + "=" * 60)
    print(f"Applying filter: {config.name}")
    print(f"Description: {config.description}")

    # Start with all accounts
    df = df_stats.copy()

    # Special case: E.SUN only
    if config.esun_only:
        retain_mask = df["is_esun"]
        retained = set(df[retain_mask]["acct"])

        if must_include_accts:
            missing = len(must_include_accts - retained)
            retained.update(must_include_accts)
            print(f"  Rescued {missing} must-include accounts.")

        print(f"\n  E.SUN only: {len(retained):,} accounts retained")
        return retained

    # Always keep all E.SUN accounts
    esun_mask = df["is_esun"]
    other_bank_mask = ~df["is_esun"]

    # Build filter for other-bank accounts
    if config.combine_logic == "AND":
        other_retain = pd.Series(True, index=df.index)
    else:  # OR
        other_retain = pd.Series(False, index=df.index)

    conditions_applied = []

    # F1: Direct connect
    if config.direct_connect_only:
        cond = df["has_esun_txn"]
        if config.combine_logic == "AND":
            other_retain &= cond
        else:
            other_retain |= cond
        conditions_applied.append(f"Direct connect: {cond[other_bank_mask].sum():,}")

    # F2: Min transaction count
    if config.min_txn_count > 0:
        cond = df["esun_txn_count"] >= config.min_txn_count
        if config.combine_logic == "AND":
            other_retain &= cond
        else:
            other_retain |= cond
        conditions_applied.append(
            f"Txn count >= {config.min_txn_count}: {cond[other_bank_mask].sum():,}"
        )

    # F3: Min transaction amount
    if config.min_txn_amount > 0:
        cond = df["max_esun_txn_amt"] >= config.min_txn_amount
        if config.combine_logic == "AND":
            other_retain &= cond
        else:
            other_retain |= cond
        conditions_applied.append(
            f"Amount >= {config.min_txn_amount:,.0f}: {cond[other_bank_mask].sum():,}"
        )

    # F4: Bidirectional
    if config.require_bidirectional:
        cond = df["has_bidirectional"]
        if config.combine_logic == "AND":
            other_retain &= cond
        else:
            other_retain |= cond
        conditions_applied.append(f"Bidirectional: {cond[other_bank_mask].sum():,}")

    # F5: Multi-partner
    if config.min_esun_partners > 0:
        cond = df["esun_partner_count"] >= config.min_esun_partners
        if config.combine_logic == "AND":
            other_retain &= cond
        else:
            other_retain |= cond
        conditions_applied.append(
            f"Partners >= {config.min_esun_partners}: {cond[other_bank_mask].sum():,}"
        )

    # F6: Recent days (always AND)
    if config.recent_days > 0 and max_txn_time is not None:
        cutoff_time = max_txn_time - (config.recent_days * 86400)
        cond = df["last_txn_time"] >= cutoff_time
        other_retain &= cond  # Always AND for time filter
        conditions_applied.append(
            f"Recent {config.recent_days}d: {cond[other_bank_mask].sum():,}"
        )

    # Combine: keep all E.SUN + filtered other-bank
    final_retain = esun_mask | (other_bank_mask & other_retain)
    retained = set(df[final_retain]["acct"])

    # Force include specific accounts
    if must_include_accts:
        rescued_count = len(must_include_accts - retained)
        retained.update(must_include_accts)
        print(f"  Rescued {rescued_count:,} must-include accounts (e.g. Test set)")

    # Print summary
    print(f"\n  Filter conditions ({config.combine_logic}):")
    for cond in conditions_applied:
        print(f"    - {cond}")

    original_count = len(df)
    esun_count = esun_mask.sum()
    other_retained = (other_bank_mask & other_retain).sum()

    print(f"\n  Results:")
    print(f"    Original accounts: {original_count:,}")
    print(f"    E.SUN (always kept): {esun_count:,}")
    print(f"    Other-bank retained: {other_retained:,}")
    print(
        f"    Total retained: {len(retained):,} ({len(retained) / original_count * 100:.1f}%)"
    )
    print(f"    Removed: {original_count - len(retained):,}")

    return retained


def apply_khop_filter(
    df_transaction: pd.DataFrame,
    df_alert: pd.DataFrame,
    k: int = 2,
    temporal_cutoff: Optional[float] = None,
) -> Set[str]:
    """
    Extract k-hop subgraph centered on fraud nodes.

    Args:
        df_transaction: Transaction DataFrame
        df_alert: Alert accounts DataFrame
        k: Number of hops
        temporal_cutoff: If provided, only use fraud accounts with event_date <= cutoff
                         (prevents data leakage when using training period only)

    Returns:
        Set of account IDs in k-hop neighborhood of fraud
    """
    print("\n" + "=" * 60)
    print(f"Extracting {k}-hop subgraph from fraud nodes...")

    # Filter fraud accounts by temporal cutoff to prevent data leakage
    if temporal_cutoff is not None:
        fraud_accounts = set(
            df_alert[df_alert["event_date"] <= temporal_cutoff]["acct"].unique()
        )
        print(
            f"  Fraud nodes (seeds, event_date <= {temporal_cutoff}): {len(fraud_accounts):,}"
        )
    else:
        fraud_accounts = set(df_alert["acct"].unique())
        print(
            f"  Fraud nodes (seeds, all periods - WARNING: may leak test data): {len(fraud_accounts):,}"
        )

    # Build adjacency
    import numpy as np

    if len(df_transaction) == 0:
        neighbors = {}
    else:
        # Vectorized adjacency build: (u,v) and (v,u)
        edges = np.concatenate(
            [
                df_transaction[["from_acct", "to_acct"]].to_numpy(),
                df_transaction[["to_acct", "from_acct"]].to_numpy(),
            ],
            axis=0,
        )
        edges_df = pd.DataFrame(edges, columns=["src", "dst"])
        neighbors = edges_df.groupby("src", sort=False)["dst"].apply(set).to_dict()

    # BFS for k hops
    current_layer = fraud_accounts.copy()
    all_nodes = fraud_accounts.copy()

    for hop in range(k):
        next_layer = set()
        for node in current_layer:
            if node in neighbors:
                next_layer.update(neighbors[node])

        next_layer -= all_nodes  # Only new nodes
        all_nodes.update(next_layer)
        current_layer = next_layer
        print(
            f"  Hop {hop + 1}: +{len(next_layer):,} nodes (total: {len(all_nodes):,})"
        )

    print(f"  Final {k}-hop subgraph: {len(all_nodes):,} nodes")
    return all_nodes


# ============================================================================


# ============================================================================
# Milestone Validation Filters (Phase 2 Data)
# ============================================================================

FILTER_PRESETS["M6_TimeWindow30d"] = FilterConfig(
    name="M6_TimeWindow30d",
    description="Milestone 6: Active in last 30 days (Phase 2)",
    recent_days=30,
)

FILTER_PRESETS["M7_TimeWindow50d"] = FilterConfig(
    name="M7_TimeWindow50d",
    description="Milestone 7: Active in last 50 days (Phase 2)",
    recent_days=50,
)

FILTER_PRESETS["M8_Hybrid_AND"] = FilterConfig(
    name="M8_Hybrid_AND",
    description="Milestone 8: 30d window AND 5K+ amount (Phase 2)",
    recent_days=30,
    min_txn_amount=5000.0,
    combine_logic="AND",
)
