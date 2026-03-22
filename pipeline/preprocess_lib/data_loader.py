#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Centralized Data Loading for Phase 2 Experiments

Provides:
1. FeatureBankGraphDataset - Dynamic feature assembly from feature bank
2. NeighborLoader factory - Optimized mini-batch sampling

Usage:
    from preprocess_lib.data_loader import load_data_with_features, create_neighbor_loaders

    # Load with specific features
    data = load_data_with_features(features=["base_34dim", "time_decay_3dim"])

    # Or use preset
    data = load_data_with_features(preset="2G1_TimeDecay")

    # Create loaders
    train_loader, val_loader, test_loader = create_neighbor_loaders(data, num_layers=4)

Author: GNN Fraud Detection Team
Date: 2026-01-24
"""

import os
import json
from typing import List, Optional, Tuple, Dict, Any, Union

import torch
from torch_geometric.data import Data
from torch_geometric.loader import NeighborLoader
from torch_geometric.utils import scatter

from preprocess_lib.feature_bank import _resolve_feature_entry


# =============================================================================
# Path Configuration
# =============================================================================

PHASE2_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PHASE2_DIR, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
FEATURE_BANK_DIR = os.path.join(DATA_DIR, "feature_bank")


# =============================================================================
# Feature Bank Graph Dataset
# =============================================================================


class FeatureBankGraphDataset:
    """
    Dynamic graph dataset that assembles features from the feature bank.

    This class loads graph structure from structure.pt and dynamically
    concatenates feature tensors from the feature bank based on configuration.

    Attributes:
        structure: Dict containing edge_index, y, masks (no x!)
        features: List of feature tensors to concatenate
        data: PyG Data object with assembled features

    Example:
        dataset = FeatureBankGraphDataset(
            features=["base_34dim", "time_decay_3dim"]
        )
        data = dataset.get_data()
        # data.x has shape [num_nodes, 37]
    """

    def __init__(
        self,
        features: Optional[List[str]] = None,
        preset: Optional[str] = None,
        structure_path: Optional[str] = None,
        feature_bank_path: Optional[str] = None,
    ):
        """
        Initialize the dataset.

        Args:
            features: List of feature names to load (e.g., ["base_34dim", "time_decay_3dim"])
            preset: Use a predefined feature combination from registry.json
            structure_path: Override path to structure.pt
            feature_bank_path: Override path to feature_bank directory

        Raises:
            FileNotFoundError: If structure.pt or required feature files don't exist
            ValueError: If both features and preset are None
        """
        self.structure_path = structure_path or os.path.join(
            PROCESSED_DIR, "structure.pt"
        )
        self.feature_bank_path = feature_bank_path or FEATURE_BANK_DIR

        # Load registry
        registry_path = os.path.join(self.feature_bank_path, "registry.json")
        if os.path.exists(registry_path):
            with open(registry_path, "r") as f:
                self.registry = json.load(f)
        else:
            self.registry = None

        # Resolve feature list
        if preset is not None:
            if self.registry is None:
                raise FileNotFoundError(f"registry.json not found at {registry_path}")
            if preset not in self.registry.get("presets", {}):
                available = list(self.registry.get("presets", {}).keys())
                raise ValueError(f"Unknown preset '{preset}'. Available: {available}")
            self.feature_names = self.registry["presets"][preset]
        elif features is not None:
            self.feature_names = features
        else:
            # Default to base features only
            self.feature_names = ["base_34dim"]

        # Load structure
        self._load_structure()

        # Load and assemble features
        self._load_features()

        # Build Data object
        self._build_data()

    def _load_structure(self):
        """Load graph structure from structure.pt"""
        if not os.path.exists(self.structure_path):
            raise FileNotFoundError(
                f"structure.pt not found at {self.structure_path}.\n"
                "Run 'python scripts/build_structure.py' first."
            )

        self.structure = torch.load(self.structure_path, weights_only=False)
        self.num_nodes = self.structure["num_nodes"]

        print(
            f"Loaded structure: {self.num_nodes:,} nodes, "
            f"{self.structure['edge_index'].shape[1]:,} edges"
        )

    def _load_features(self):
        """Load and concatenate feature tensors"""
        self.feature_tensors = []
        self.feature_dims = []
        tensor_cache: Dict[str, Any] = {}

        for feat_name in self.feature_names:
            spec = _resolve_feature_entry(
                feat_name, self.registry or {}, self.feature_bank_path
            )
            feat_file = spec.get("file")
            expected_dims = spec.get("dims")
            if not isinstance(feat_file, str) or not feat_file:
                raise ValueError(f"Feature spec missing file: {feat_name}")

            feat_path = os.path.join(self.feature_bank_path, feat_file)

            if not os.path.exists(feat_path):
                raise FileNotFoundError(
                    f"Feature file not found: {feat_path}\n"
                    "Run 'python scripts/build_feature_bank.py' first."
                )

            if feat_path not in tensor_cache:
                tensor_cache[feat_path] = torch.load(feat_path, weights_only=False)
            tensor = tensor_cache[feat_path]

            start_idx = spec.get("start_idx")
            end_idx = spec.get("end_idx")
            if (
                isinstance(tensor, torch.Tensor)
                and start_idx is not None
                and end_idx is not None
                and (start_idx != 0 or end_idx != tensor.shape[1])
            ):
                tensor = tensor[:, int(start_idx) : int(end_idx)]

            # Validate shape
            if tensor.shape[0] != self.num_nodes:
                raise ValueError(
                    f"Feature {feat_name} has {tensor.shape[0]} rows, "
                    f"but structure has {self.num_nodes} nodes. "
                    "Node ordering mismatch - rebuild feature bank."
                )

            if expected_dims and tensor.shape[1] != expected_dims:
                print(
                    f"Warning: {feat_name} has {tensor.shape[1]} dims, "
                    f"expected {expected_dims}"
                )

            self.feature_tensors.append(tensor)
            self.feature_dims.append(tensor.shape[1])

            print(f"  Loaded {feat_name}: {tensor.shape[1]} dims")

        # Concatenate
        self.x = torch.cat(self.feature_tensors, dim=1)
        print(f"  Total features: {self.x.shape[1]} dims")

    def _build_data(self):
        """Build PyG Data object"""
        self.data = Data(
            x=self.x,
            edge_index=self.structure["edge_index"],
            y=self.structure["y"],
            train_mask=self.structure["train_mask"],
            val_mask=self.structure["val_mask"],
            test_mask=self.structure["test_mask"],
            predict_mask=self.structure["predict_mask"],
            is_esun_mask=self.structure["is_esun_mask"],
        )

        if "edge_attr" in self.structure:
            self.data.edge_attr = self.structure["edge_attr"]
        if "edge_time" in self.structure:
            self.data.edge_time = self.structure["edge_time"]

        # Preserve node mappings if available in structure
        if "acct_to_node" in self.structure:
            self.data.acct_to_node = self.structure["acct_to_node"]
        if "node_to_acct" in self.structure:
            self.data.node_to_acct = self.structure["node_to_acct"]

    def get_data(self) -> Data:
        """Get the assembled PyG Data object"""
        return self.data

    def get_feature_info(self) -> Dict[str, Any]:
        """Get information about loaded features"""
        return {
            "feature_names": self.feature_names,
            "feature_dims": self.feature_dims,
            "total_dims": self.x.shape[1],
            "num_nodes": self.num_nodes,
        }


def load_data_with_features(
    features: Optional[List[str]] = None,
    preset: Optional[str] = None,
    legacy_path: Optional[str] = None,
) -> Data:
    """
    Load graph data with specified features.

    This is the main entry point for loading data in experiments.

    Args:
        features: List of feature names (e.g., ["base_34dim", "time_decay_3dim"])
        preset: Use a predefined preset (e.g., "2G1_TimeDecay")
        legacy_path: Load from legacy graph_data.pt (for backward compatibility)

    Returns:
        PyG Data object with assembled features

    Examples:
        # Use base features only (34 dims)
        data = load_data_with_features(features=["base_34dim"])

        # Use preset for 2G1 experiment (37 dims)
        data = load_data_with_features(preset="2G1_TimeDecay")

        # Legacy mode (load from graph_data.pt)
        data = load_data_with_features(legacy_path="data/processed/graph_data.pt")
    """
    # Legacy mode: load directly from graph_data.pt
    if legacy_path is not None:
        if not os.path.isabs(legacy_path):
            legacy_path = os.path.join(PHASE2_DIR, legacy_path)

        if not os.path.exists(legacy_path):
            raise FileNotFoundError(f"Legacy graph data not found: {legacy_path}")

        print(f"Loading legacy graph data from {legacy_path}")
        data = torch.load(legacy_path, weights_only=False)
        return data

    # Feature bank mode
    dataset = FeatureBankGraphDataset(features=features, preset=preset)
    return dataset.get_data()


# =============================================================================
# NeighborLoader Configuration
# =============================================================================

# Optimized neighbor sampling: [10, 10, 5, 5] for 4 layers
DEFAULT_NUM_NEIGHBORS_4L = [10, 10, 5, 5]
DEFAULT_NUM_NEIGHBORS_5L = [10, 10, 5, 5, 5]

# DataLoader worker settings (0 = single-process to avoid CUDA multiprocess issues)
DEFAULT_NUM_WORKERS = 8
DEFAULT_BATCH_SIZE = 1024
DEFAULT_PREFETCH_FACTOR = 2


def get_default_num_neighbors(num_layers: int) -> List[int]:
    """Get default neighbor sampling configuration for given number of layers."""
    if num_layers == 4:
        return DEFAULT_NUM_NEIGHBORS_4L.copy()
    elif num_layers == 5:
        return DEFAULT_NUM_NEIGHBORS_5L.copy()
    elif num_layers <= 3:
        return [10] * num_layers
    else:
        return [10, 10] + [5] * (num_layers - 2)


def get_loader_kwargs(
    batch_size: Optional[int] = None,
    num_workers: Optional[int] = None,
    pin_memory: bool = True,
    persistent_workers: bool = True,
    prefetch_factor: Optional[int] = None,
) -> Dict[str, Any]:
    """Get optimized DataLoader keyword arguments."""
    if batch_size is None:
        batch_size = int(os.environ.get("BATCH_SIZE", DEFAULT_BATCH_SIZE))

    if num_workers is None:
        num_workers = DEFAULT_NUM_WORKERS

    if prefetch_factor is None:
        prefetch_factor = DEFAULT_PREFETCH_FACTOR

    kwargs = {
        "batch_size": batch_size,
        "num_workers": num_workers,
        "pin_memory": pin_memory,
    }

    if num_workers > 0:
        kwargs["persistent_workers"] = persistent_workers
        kwargs["prefetch_factor"] = prefetch_factor

    return kwargs


def create_neighbor_loaders(
    data: Data,
    num_layers: int = 4,
    num_neighbors: Optional[List[int]] = None,
    batch_size: Optional[int] = None,
    num_workers: Optional[int] = None,
    **extra_kwargs,
) -> Tuple[NeighborLoader, NeighborLoader, NeighborLoader]:
    """
    Create train, validation, and test NeighborLoaders with optimized settings.

    Args:
        data: PyG Data object with train_mask, val_mask, test_mask
        num_layers: Number of GNN layers (determines neighbor sampling)
        num_neighbors: Override default neighbor sampling (optional)
        batch_size: Override batch size (optional)
        num_workers: Override number of workers (optional)
        **extra_kwargs: Additional kwargs passed to NeighborLoader

    Returns:
        Tuple of (train_loader, val_loader, test_loader)
    """
    if num_neighbors is None:
        num_neighbors = get_default_num_neighbors(num_layers)

    if len(num_neighbors) != num_layers:
        raise ValueError(
            f"num_neighbors length ({len(num_neighbors)}) must match "
            f"num_layers ({num_layers})"
        )

    filter_per_worker = extra_kwargs.pop("filter_per_worker", True)
    use_temporal_sampling = extra_kwargs.pop("use_temporal_sampling", None)
    temporal_strategy = extra_kwargs.pop("temporal_strategy", "last")
    time_attr = extra_kwargs.pop("time_attr", "edge_time")
    kwargs = get_loader_kwargs(batch_size=batch_size, num_workers=num_workers)
    kwargs.update(extra_kwargs)

    temporal_kwargs: Dict[str, Any] = {}
    node_time: Optional[torch.Tensor] = None
    if use_temporal_sampling is None:
        use_temporal_sampling = hasattr(data, time_attr)
    if use_temporal_sampling:
        if not hasattr(data, time_attr):
            raise ValueError(
                f"Temporal sampling requested but data has no '{time_attr}' attribute"
            )
        edge_time = getattr(data, time_attr)
        if edge_time.dtype != torch.long:
            edge_time = edge_time.long()
            setattr(data, time_attr, edge_time)

        edge_index = data.edge_index
        if edge_index is None:
            raise ValueError(
                "Temporal sampling requested but data.edge_index is missing"
            )
        src, dst = edge_index
        num_nodes = int(edge_index.max().item()) + 1
        node_time_src = scatter(
            edge_time,
            src,
            dim=0,
            dim_size=num_nodes,
            reduce="max",
        )
        node_time_dst = scatter(
            edge_time,
            dst,
            dim=0,
            dim_size=num_nodes,
            reduce="max",
        )
        node_time = torch.maximum(node_time_src, node_time_dst).long()
        temporal_kwargs = {
            "time_attr": time_attr,
            "temporal_strategy": temporal_strategy,
            "is_sorted": False,
        }

    def _make_loader(input_nodes, shuffle: bool) -> NeighborLoader:
        loader_input_time = None
        if use_temporal_sampling and node_time is not None:
            if input_nodes is None:
                raise ValueError("input_nodes cannot be None for temporal loader")
            if (
                isinstance(input_nodes, torch.Tensor)
                and input_nodes.dtype == torch.bool
            ):
                node_ids = torch.where(input_nodes)[0]
            elif isinstance(input_nodes, torch.Tensor):
                node_ids = input_nodes.long()
            else:
                node_ids = torch.as_tensor(input_nodes, dtype=torch.long)
            loader_input_time = node_time[node_ids]

        try:
            return NeighborLoader(
                data,
                input_nodes=input_nodes,
                input_time=loader_input_time,
                num_neighbors=num_neighbors,
                shuffle=shuffle,
                filter_per_worker=filter_per_worker,
                **temporal_kwargs,
                **kwargs,
            )
        except ValueError as e:
            if use_temporal_sampling and "time_attr" in str(e):
                raise RuntimeError(
                    "Temporal NeighborLoader requires pyg-lib. "
                    "Install pyg-lib or disable temporal sampling explicitly."
                ) from e
            raise

    import time as _t

    _t0 = _t.time()
    print("[Loader] Creating train loader...", flush=True)
    train_loader = _make_loader(data.train_mask, shuffle=True)
    print(f"[Loader] Train loader ready ({_t.time() - _t0:.1f}s)", flush=True)
    _t1 = _t.time()
    print("[Loader] Creating val loader...", flush=True)
    val_loader = _make_loader(data.val_mask, shuffle=False)
    print(f"[Loader] Val loader ready ({_t.time() - _t1:.1f}s)", flush=True)
    _t2 = _t.time()
    print("[Loader] Creating test loader...", flush=True)
    test_loader = _make_loader(data.test_mask, shuffle=False)
    print(
        f"[Loader] Test loader ready ({_t.time() - _t2:.1f}s), total={_t.time() - _t0:.1f}s",
        flush=True,
    )

    return train_loader, val_loader, test_loader


def print_loader_config(
    num_neighbors: List[int],
    batch_size: int,
    num_workers: int = DEFAULT_NUM_WORKERS,
):
    """Print loader configuration for logging."""
    total_expansion = 1
    for n in num_neighbors:
        total_expansion *= n

    print(f"DataLoader Config:")
    print(f"  Neighbors: {num_neighbors} (max expansion: {total_expansion:,})")
    print(f"  Batch size: {batch_size}")
    print(f"  Workers: {num_workers}")
    print(f"  Pin memory: True")


def setup_loaders(
    data: Data,
    num_layers: int = 4,
    verbose: bool = True,
) -> Tuple[NeighborLoader, NeighborLoader, NeighborLoader, List[int]]:
    """
    Quick setup for standard experiments.

    Returns:
        Tuple of (train_loader, val_loader, test_loader, num_neighbors)
    """
    num_neighbors = get_default_num_neighbors(num_layers)
    batch_size = int(os.environ.get("BATCH_SIZE", DEFAULT_BATCH_SIZE))

    if verbose:
        print_loader_config(num_neighbors, batch_size)

    train_loader, val_loader, test_loader = create_neighbor_loaders(
        data,
        num_layers=num_layers,
        num_neighbors=num_neighbors,
    )

    return train_loader, val_loader, test_loader, num_neighbors


# =============================================================================
# CLI for Testing
# =============================================================================


if __name__ == "__main__":
    print("=" * 60)
    print("Feature Bank Data Loader Test")
    print("=" * 60)

    # Check if feature bank exists
    structure_path = os.path.join(PROCESSED_DIR, "structure.pt")
    registry_path = os.path.join(FEATURE_BANK_DIR, "registry.json")

    print(f"\nChecking paths:")
    print(
        f"  Structure: {structure_path} - {'EXISTS' if os.path.exists(structure_path) else 'NOT FOUND'}"
    )
    print(
        f"  Registry: {registry_path} - {'EXISTS' if os.path.exists(registry_path) else 'NOT FOUND'}"
    )

    if os.path.exists(registry_path):
        with open(registry_path, "r") as f:
            registry = json.load(f)

        print(f"\nAvailable features:")
        for name, info in registry.get("features", {}).items():
            file_path = os.path.join(FEATURE_BANK_DIR, info["file"])
            exists = "EXISTS" if os.path.exists(file_path) else "NOT FOUND"
            print(f"  {name}: {info['dims']} dims [{exists}]")

        print(f"\nAvailable presets:")
        for preset, features in registry.get("presets", {}).items():
            total_dims = sum(
                registry["features"].get(f, {}).get("dims", 0) for f in features
            )
            print(f"  {preset}: {features} -> {total_dims} dims")

    # Try loading if structure exists
    if os.path.exists(structure_path):
        print("\n" + "=" * 60)
        print("Testing data loading...")
        print("=" * 60)

        try:
            data = load_data_with_features(features=["base_34dim"])
            print(f"\nLoaded successfully!")
            assert data.x is not None
            assert data.edge_index is not None
            print(f"  x: {data.x.shape}")
            print(f"  edge_index: {data.edge_index.shape}")
            print(f"  train: {data.train_mask.sum().item()}")
            print(f"  val: {data.val_mask.sum().item()}")
            print(f"  test: {data.test_mask.sum().item()}")
        except Exception as e:
            print(f"\nError loading data: {e}")
    else:
        print("\nRun 'python scripts/build_structure.py' first to create structure.pt")
