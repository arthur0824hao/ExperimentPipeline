#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature Bank - Dynamic feature assembly for experiments.

This module provides utilities to:
1. Load graph structure from structure.pt
2. Dynamically assemble features from the feature bank
3. Create DataLoaders with custom feature configurations

Supports BOTH legacy (graph_data.pt) and new (structure.pt + feature_bank) modes.

Usage:
    from preprocess_lib.feature_bank import load_data_with_features

    # New mode: Load from feature bank
    data = load_data_with_features(features=["base_34dim", "time_decay_3dim"])

    # Or use preset
    data = load_data_with_features(preset="2G1_TimeDecay")

    # Legacy mode: Load from graph_data.pt
    data = load_data_with_features(features=["base"])  # Falls back to legacy
"""

import os
import json
from typing import List, Optional, Dict, Any, Tuple

import torch
from torch_geometric.data import Data


# =============================================================================
# Paths
# =============================================================================


def get_phase2_dir():
    """Get Phase2 directory path."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_feature_bank_dir():
    """Get feature bank directory path."""
    return os.path.join(get_phase2_dir(), "data/feature_bank")


def get_structure_path():
    """Get structure.pt path (new architecture)."""
    return os.path.join(get_phase2_dir(), "data/processed/structure.pt")


def get_legacy_graph_path():
    """Get legacy graph_data.pt path."""
    return os.path.join(get_phase2_dir(), "data/processed/graph_data.pt")


# =============================================================================
# Registry
# =============================================================================


def load_registry() -> Dict[str, Any]:
    """Load feature registry."""
    registry_path = os.path.join(get_feature_bank_dir(), "registry.json")

    if not os.path.exists(registry_path):
        # Return minimal registry for legacy mode
        return {
            "features": {
                "base": {
                    "file": None,
                    "dims": 34,
                    "description": "Legacy base features",
                }
            },
            "presets": {"base": ["base"]},
        }

    with open(registry_path, "r") as f:
        return json.load(f)


def get_preset_features(preset_name: str) -> List[str]:
    """Get feature list for a preset."""
    registry = load_registry()
    presets = registry.get("presets", {})

    if preset_name not in presets:
        available = list(presets.keys())
        raise ValueError(f"Unknown preset '{preset_name}'. Available: {available}")

    return presets[preset_name]


def list_available_features() -> List[str]:
    """List all available feature groups."""
    registry = load_registry()
    return list(registry.get("features", {}).keys())


def list_presets() -> List[str]:
    """List all available presets."""
    registry = load_registry()
    return list(registry.get("presets", {}).keys())


# =============================================================================
# Feature Loading
# =============================================================================


def _use_new_architecture(structure_file: str = "structure.pt") -> bool:
    structure_path = os.path.join(get_phase2_dir(), "data/processed", structure_file)
    feature_bank_dir = get_feature_bank_dir()
    if not os.path.exists(structure_path) or not os.path.isdir(feature_bank_dir):
        return False
    if os.path.exists(os.path.join(feature_bank_dir, "registry.json")):
        return True
    try:
        return any(name.endswith(".pt") for name in os.listdir(feature_bank_dir))
    except OSError:
        return False


def _resolve_feature_file(
    feat_name: str, registry: Dict[str, Any], feature_bank_dir: str
) -> tuple[Optional[str], Optional[int]]:
    feature_info = registry.get("features", {})
    if feat_name in feature_info:
        info = feature_info[feat_name]
        feat_file = info.get("file")
        expected_dims = info.get("dims")
        return feat_file, expected_dims

    direct_path = os.path.join(feature_bank_dir, f"{feat_name}.pt")
    if os.path.exists(direct_path):
        return f"{feat_name}.pt", None

    return None, None


def _resolve_feature_entry(
    feat_name: str, registry: Dict[str, Any], feature_bank_dir: str
) -> Dict[str, Any]:
    feature_info = registry.get("features", {})
    artifacts = registry.get("artifacts", {})

    if feat_name in feature_info:
        info = dict(feature_info[feat_name])
        artifact_id = info.get("artifact_id")
        if artifact_id:
            artifact = artifacts.get(artifact_id)
            if artifact is None:
                raise ValueError(
                    f"Feature {feat_name} references missing artifact_id={artifact_id}"
                )
            file_name = artifact.get("path")
            if not file_name:
                raise ValueError(
                    f"Artifact {artifact_id} missing path for feature {feat_name}"
                )
            start_idx = int(info.get("start_idx", 0) or 0)
            end_idx = int(info.get("end_idx", artifact.get("total_dim", 0)) or 0)
            dims = int(info.get("dims", max(0, end_idx - start_idx)) or 0)
            return {
                "name": feat_name,
                "file": file_name,
                "dims": dims,
                "start_idx": start_idx,
                "end_idx": end_idx,
                "artifact_id": artifact_id,
                "kind": info.get("kind", "feature"),
            }

        feat_file = info.get("file")
        expected_dims = info.get("dims")
        return {
            "name": feat_name,
            "file": feat_file,
            "dims": expected_dims,
            "start_idx": None,
            "end_idx": None,
            "artifact_id": None,
            "kind": info.get("kind", "feature"),
        }

    direct_path = os.path.join(feature_bank_dir, f"{feat_name}.pt")
    if os.path.exists(direct_path):
        return {
            "name": feat_name,
            "file": f"{feat_name}.pt",
            "dims": None,
            "start_idx": None,
            "end_idx": None,
            "artifact_id": None,
            "kind": "direct",
        }

    raise ValueError(f"Unknown feature: {feat_name}")


def resolve_feature_spec(feat_name: str) -> Dict[str, Any]:
    registry = load_registry()
    return _resolve_feature_entry(feat_name, registry, get_feature_bank_dir())


def _load_tensor_feature(
    spec: Dict[str, Any],
    feature_bank_dir: str,
    tensor_cache: Dict[str, Any],
) -> Any:
    feat_file = spec.get("file")
    if not feat_file:
        raise ValueError(f"Feature spec missing file: {spec}")

    feat_path = os.path.join(feature_bank_dir, feat_file)
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
        return tensor[:, int(start_idx) : int(end_idx)]
    return tensor


def _load_legacy_data(verbose: bool = True) -> Data:
    """Load data from legacy graph_data.pt."""
    graph_path = get_legacy_graph_path()

    if not os.path.exists(graph_path):
        raise FileNotFoundError(
            f"Neither structure.pt nor graph_data.pt found.\n"
            f"Run 'python scripts/build_structure.py && python scripts/build_feature_bank.py' first."
        )

    if verbose:
        print(f"Loading legacy graph from {graph_path}...")

    return torch.load(graph_path, weights_only=False)


def _load_new_architecture(
    feature_names: List[str], verbose: bool = True, structure_file: str = "structure.pt"
) -> Data:
    """Load data from new architecture (structure.pt + feature_bank)."""
    structure_path = os.path.join(get_phase2_dir(), "data/processed", structure_file)

    if verbose:
        print(f"Loading structure from {structure_path}...")

    structure = torch.load(structure_path, weights_only=False)
    if isinstance(structure, dict):
        num_nodes = int(structure.get("num_nodes", structure["y"].shape[0]))
    else:
        num_nodes = int(structure.num_nodes)

    # Load and concatenate features
    feature_tensors = []
    feature_bank_dir = get_feature_bank_dir()
    registry = load_registry()
    tensor_cache: Dict[str, Any] = {}

    for feat_name in feature_names:
        # Handle legacy "base" -> "base_34dim" mapping
        if feat_name == "base":
            feat_name = "base_34dim"

        spec = _resolve_feature_entry(feat_name, registry, feature_bank_dir)
        tensor = _load_tensor_feature(spec, feature_bank_dir, tensor_cache)
        expected_dims = spec.get("dims")

        if isinstance(tensor, torch.Tensor):
            if tensor.shape[0] != num_nodes:
                raise ValueError(
                    f"Feature {feat_name} has {tensor.shape[0]} rows, "
                    f"but structure has {num_nodes} nodes."
                )
            if expected_dims is not None and tensor.shape[1] != expected_dims:
                raise ValueError(
                    f"Feature {feat_name} has {tensor.shape[1]} dims, expected {expected_dims}."
                )
            feature_tensors.append(tensor)
            if verbose:
                print(f"  Loaded {feat_name}: {tensor.shape[1]} dims")
        else:
            # Handle non-tensor features (e.g. dictionaries for sequences)
            # Store them temporarily to attach to Data later
            temp_other = registry.setdefault("_temp_other_features", {})
            if isinstance(temp_other, dict):
                temp_other[feat_name] = tensor
            if verbose:
                print(f"  Loaded complex feature {feat_name} ({type(tensor).__name__})")

    # Concatenate features
    if len(feature_tensors) == 0:
        raise ValueError("No features loaded!")

    x = torch.cat(feature_tensors, dim=1)
    if verbose:
        print(f"  Total features: {x.shape[1]} dims")

    # Build Data object
    data = Data(
        x=x,
        edge_index=structure["edge_index"],
        y=structure["y"],
        train_mask=structure["train_mask"],
        val_mask=structure["val_mask"],
        test_mask=structure["test_mask"],
        predict_mask=structure["predict_mask"],
        is_esun_mask=structure["is_esun_mask"],
    )

    # Attach temporal & identity fields from structure (critical for window filtering & p2 eval)
    _OPTIONAL_FIELDS = [
        "node_alert_time",
        "node_first_txn_time",
        "node_last_txn_time",
        "acct_to_node",
    ]
    for field in _OPTIONAL_FIELDS:
        if field in structure:
            setattr(data, field, structure[field])
            if verbose:
                val = structure[field]
                if isinstance(val, torch.Tensor):
                    print(f"  Loaded {field}: {val.shape} {val.dtype}")
                elif isinstance(val, dict):
                    print(f"  Loaded {field}: dict ({len(val)} items)")

    # Attach other features (dicts, etc)
    _attach_other_features(data, registry)

    # Load edge attributes if available (for GATv2/TGN)
    if "edge_attr" in structure:
        data.edge_attr = structure["edge_attr"]
        if verbose:
            print(f"  Loaded edge_attr: {data.edge_attr.shape}")
    if "edge_time" in structure:
        data.edge_time = structure["edge_time"]
        if verbose:
            print(f"  Loaded edge_time: {data.edge_time.shape}")

    data.feature_groups = feature_names
    data.feature_dim = x.shape[1]

    return data


def _attach_other_features(data: Data, registry: Dict):
    """Attach non-tensor features to Data object."""
    temp_other = registry.get("_temp_other_features")
    if isinstance(temp_other, dict):
        for name, value in temp_other.items():
            setattr(data, name, value)
            if not hasattr(data, "other_features"):
                data.other_features = []
            data.other_features.append(name)
        # Clean up
        registry.pop("_temp_other_features", None)


def _normalize_feature_names(features: List[str]) -> List[str]:
    """Normalize feature names (handle legacy naming)."""
    mapping = {
        "base": "base_34dim",
        "time_decay": "time_decay_3dim",
        "balance_volatility": "balance_vol_4dim",
        "balance_vol": "balance_vol_4dim",
        "ego_network": "ego_network_6dim",
        "ego": "ego_network_6dim",
        "velocity": "velocity_3dim",
        "velocity_features": "velocity_3dim",
        "burst": "burst_3dim",
        "burst_features": "burst_3dim",
        "acceleration": "acceleration_3dim",
        "interaction": "interaction_3dim",
        "short_window": "short_window_6dim",
    }
    return [mapping.get(f, f) for f in features]


# =============================================================================
# Main API
# =============================================================================


def load_data_with_features(
    features: Optional[List[str]] = None,
    preset: Optional[str] = None,
    verbose: bool = True,
    structure_file: str = "structure.pt",
) -> Data:
    """
    Load graph data with dynamically assembled features.

    Args:
        features: List of feature groups to include
        preset: Use a preset configuration
        verbose: Print loading info
        structure_file: Filename of structure file in data/processed/ (default: structure.pt)
    """
    # Validate arguments
    if features is None and preset is None:
        features = ["base_34dim"]  # Default to baseline
    elif features is not None and preset is not None:
        raise ValueError("Specify either 'features' or 'preset', not both.")
    elif preset is not None:
        features = get_preset_features(preset)

    if features is None:
        raise ValueError("features could not be resolved")

    # Normalize feature names
    features = _normalize_feature_names(features)

    # Check which architecture to use
    use_new = _use_new_architecture(structure_file)

    if use_new:
        if verbose:
            print(f"Using new Feature Bank architecture ({structure_file})")
        return _load_new_architecture(features, verbose, structure_file)
    else:
        # Legacy mode: load from graph_data.pt
        if verbose:
            print("Using legacy graph_data.pt (new architecture not built)")

        # Only base features supported in legacy mode
        if features != ["base_34dim"] and features != ["base"]:
            raise ValueError(
                f"Legacy mode only supports base features. "
                f"Requested: {features}. "
                f"Run 'python scripts/build_structure.py && python scripts/build_feature_bank.py' "
                f"to enable feature bank."
            )

        data = _load_legacy_data(verbose)
        if data.x is None:
            raise ValueError("Legacy graph_data.pt missing node features (x)")
        data.feature_groups = ["base"]
        data.feature_dim = data.x.shape[1]
        return data


def get_feature_dim(features: List[str]) -> int:
    """
    Get total feature dimension for a feature configuration.

    Args:
        features: List of feature groups

    Returns:
        Total feature dimension
    """
    registry = load_registry()
    # Normalize names
    features = _normalize_feature_names(features)

    total_dim = 0
    for group in features:
        spec = _resolve_feature_entry(group, registry, get_feature_bank_dir())
        dims = spec.get("dims")
        if dims is None:
            raise ValueError(f"Unknown feature group: {group}")
        total_dim += int(dims)

    return total_dim


# =============================================================================
# Utility
# =============================================================================


def print_feature_info():
    """Print information about available features."""
    registry = load_registry()

    print("\n" + "=" * 60)
    print("Feature Bank Information")
    print("=" * 60)

    print(
        f"\nArchitecture: {'New (Feature Bank)' if _use_new_architecture() else 'Legacy (graph_data.pt)'}"
    )

    print("\nFeature Groups:")
    for name, info in registry.get("features", {}).items():
        dims = info.get("dims", "?")
        desc = info.get("description", "")
        print(f"  - {name}: {dims} dims")
        if desc:
            print(f"    {desc}")

    print("\nPresets:")
    for name, features in registry.get("presets", {}).items():
        try:
            total_dims = get_feature_dim(features)
        except:
            total_dims = "?"
        print(f"  - {name}: {features} -> {total_dims} dims")

    print()


if __name__ == "__main__":
    print_feature_info()
