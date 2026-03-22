#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Preprocessing Library for Fraud Detection GNN

Provides modular preprocessing with configurable node filtering strategies.

Author: Claude
Date: 2026-01-13
"""

from .data_loader_base import (
    load_csv_files,
    load_phase2_csv_files,
    convert_currency_to_twd,
    integrate_datetime,
    compute_other_bank_stats,
)

from .feature_computer import (
    compute_all_features,
    compute_basic_features,
    compute_balance_features,
    compute_multiscale_features,
    compute_graph_structure_features,
    compute_flow_pattern_features,
)

from .node_filter import (
    FilterConfig,
    apply_node_filter,
    get_filter_config,
    FILTER_PRESETS,
)

from .graph_builder import (
    build_filtered_graph,
    build_phase2_filtered_graph,
    create_train_val_test_split,
    save_graph_data,
)

from .data_loader import (
    create_neighbor_loaders,
    get_default_num_neighbors,
    get_loader_kwargs,
    setup_loaders,
    DEFAULT_NUM_NEIGHBORS_4L,
    DEFAULT_NUM_NEIGHBORS_5L,
)

from .feature_bank import (
    load_data_with_features,
    get_feature_dim,
    list_available_features,
    list_presets,
    print_feature_info,
)

__all__ = [
    "load_csv_files",
    "load_phase2_csv_files",
    "convert_currency_to_twd",
    "integrate_datetime",
    "compute_other_bank_stats",
    "compute_all_features",
    "FilterConfig",
    "apply_node_filter",
    "get_filter_config",
    "FILTER_PRESETS",
    "build_filtered_graph",
    "build_phase2_filtered_graph",
    "save_graph_data",
    # DataLoader factory
    "create_neighbor_loaders",
    "get_default_num_neighbors",
    "get_loader_kwargs",
    "setup_loaders",
    "DEFAULT_NUM_NEIGHBORS_4L",
    "DEFAULT_NUM_NEIGHBORS_5L",
    # Feature Bank
    "load_data_with_features",
    "get_feature_dim",
    "list_available_features",
    "list_presets",
    "print_feature_info",
]
