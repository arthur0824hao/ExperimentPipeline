from __future__ import annotations

import json
import math
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional


PHASE3_ROOT = Path(__file__).resolve().parents[1]
FEATURE_REGISTRY_PATH = PHASE3_ROOT / "data" / "feature_bank" / "registry.json"
STRUCTURE_PATH = PHASE3_ROOT / "data" / "processed" / "structure_edge_temporal.pt"

FORMULA_VERSION = "static_vram_estimator_v1_1"
CARD_TOTAL_VRAM_MB = 24 * 1024
DEFAULT_VRAM_RESERVE_MB = 1800
MB = 1024.0 * 1024.0
BYTES_PER_FP32 = 4.0
INDEX_BYTES = 8.0
LABEL_MASK_BYTES_PER_NODE = 12.0
PREDICT_SCORE_BYTES_PER_NODE = 8.0
PREDICT_POSTPROC_BUFFER_MB = 96.0
EVAL_ACTIVATION_RATIO = 0.42
PREDICT_ACTIVATION_RATIO = 0.42
ZEBRA_DEFAULT_FEATURE_DIM = 34
ZEBRA_FEATURE_LABEL = "zebra_phase1faithful_default34"
DEFAULT_HOLDOUT_FEATURES = ["base_basic12_cut_d152"]

FULLBATCH_ALPHA = {
    "GRAPHSAGE": 0.75,
    "GCN": 0.70,
    "GIN": 0.82,
    "GAT": 1.05,
    "GATV2": 1.05,
    "DEFAULT": 0.78,
}
FULLBATCH_BETA = {
    "GRAPHSAGE": 1.35,
    "GCN": 1.26,
    "GIN": 1.42,
    "GAT": 1.72,
    "GATV2": 1.72,
    "DEFAULT": 1.38,
}
FULLBATCH_SAFETY = {
    "GRAPHSAGE": 2300.0,
    "GCN": 2100.0,
    "GIN": 2400.0,
    "GAT": 2800.0,
    "GATV2": 2800.0,
    "DEFAULT": 2300.0,
}

NEIGHBOR_ALPHA = 0.55
NEIGHBOR_BETA = 0.95
NEIGHBOR_SAFETY_MB = 1400.0
TEMPORAL_ALPHA = 0.70
TEMPORAL_BETA = 1.10
TEMPORAL_SAFETY_MB = 1700.0
NO_BATCH_ALPHA = 1.05
NO_BATCH_BETA = 1.55
NO_BATCH_SAFETY_MB = 3200.0


def _mb(num_bytes: float) -> float:
    return float(num_bytes) / MB


def _round_mb(value: float) -> int:
    return int(math.ceil(float(value)))


@lru_cache(maxsize=1)
def _load_feature_registry(registry_path: str) -> Dict[str, Any]:
    path = Path(registry_path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _load_structure_stats(structure_path: str) -> Dict[str, int]:
    import torch

    payload = torch.load(structure_path, weights_only=False)
    if not isinstance(payload, dict):
        raise TypeError(f"Unexpected structure payload type: {type(payload)!r}")

    edge_index = payload.get("edge_index")
    edge_attr = payload.get("edge_attr")
    num_nodes = int(payload.get("num_nodes", 0) or 0)
    if edge_index is None or not hasattr(edge_index, "size"):
        raise ValueError("structure payload missing edge_index")
    num_edges = int(edge_index.size(1))
    edge_attr_dim = 0
    if edge_attr is not None and hasattr(edge_attr, "dim"):
        edge_attr_dim = int(edge_attr.size(1)) if edge_attr.dim() > 1 else 1
    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "edge_attr_dim": edge_attr_dim,
    }


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _entry_json_path(exp: Dict[str, Any], phase3_root: Path) -> Path:
    name = str(exp.get("name") or "").strip()
    return phase3_root / "experiments" / name / "entry.json"


def _load_entry_json(exp: Dict[str, Any], phase3_root: Path) -> Dict[str, Any]:
    return _read_json(_entry_json_path(exp, phase3_root))


def _load_runtime_meta(exp: Dict[str, Any], phase3_root: Path) -> Dict[str, Any]:
    results_path = phase3_root / "results_db" / f"{exp.get('name', '')}.json"
    payload = _read_json(results_path)
    runtime_meta = payload.get("runtime_meta")
    if isinstance(runtime_meta, dict):
        merged = dict(runtime_meta)
        for key in (
            "hidden_dim",
            "num_layers",
            "model_variant",
            "batch_size",
            "eval_batch_size",
            "num_neighbors",
            "temporal_strategy",
            "loss_type",
        ):
            if key in payload and key not in merged:
                merged[key] = payload[key]
        return merged
    return payload


def _resolve_feature_context(
    exp: Dict[str, Any],
    runtime_meta: Dict[str, Any],
    registry: Dict[str, Any],
    phase3_root: Path,
    arch: str,
) -> tuple[List[str], Dict[str, int], int]:
    features = [str(item) for item in exp.get("features", []) if str(item).strip()]
    if not features:
        entry_payload = _load_entry_json(exp, phase3_root)
        features = [
            str(item) for item in entry_payload.get("features", []) if str(item).strip()
        ]
    if not features:
        features = [
            str(item) for item in runtime_meta.get("features", []) if str(item).strip()
        ]

    feature_dims = _feature_dims(features, registry) if features else {}
    total_feature_dim = int(sum(feature_dims.values()))
    if total_feature_dim > 0:
        return features, feature_dims, total_feature_dim

    if arch == "ZEBRA":
        return (
            [],
            {ZEBRA_FEATURE_LABEL: ZEBRA_DEFAULT_FEATURE_DIM},
            ZEBRA_DEFAULT_FEATURE_DIM,
        )

    raise ValueError(
        f"unable to resolve feature dims for {features or [exp.get('name', '')]}"
    )


def _load_script_text(exp: Dict[str, Any], phase3_root: Path) -> str:
    script = exp.get("script")
    if script:
        script_path = Path(script)
        if not script_path.is_absolute():
            script_path = phase3_root / script_path
    else:
        name = str(exp.get("name") or "").strip()
        script_path = phase3_root / "experiments" / name / "scripts" / "train.py"
    try:
        return script_path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _feature_dims(features: List[str], registry: Dict[str, Any]) -> Dict[str, int]:
    feature_map = registry.get("features", {}) if isinstance(registry, dict) else {}
    dims: Dict[str, int] = {}
    for feature_name in features:
        info = feature_map.get(feature_name) or {}
        dims[feature_name] = int(info.get("dims", 0) or 0)
    return dims


def _sum_feature_dims(features: List[str], registry: Dict[str, Any]) -> int:
    return int(sum(_feature_dims(features, registry).values()))


def _parse_first_int(pattern: str, text: str) -> Optional[int]:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _parse_neighbors(raw: Any) -> List[int]:
    if isinstance(raw, list):
        values = []
        for item in raw:
            try:
                values.append(int(item))
            except (TypeError, ValueError):
                continue
        return values
    if isinstance(raw, str):
        cleaned = raw.strip()
        if not cleaned or cleaned.lower() == "none":
            return []
        if cleaned.startswith("[") and cleaned.endswith("]"):
            cleaned = cleaned[1:-1]
        values = []
        for token in cleaned.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                values.append(int(token))
            except ValueError:
                continue
        return values
    return []


def _infer_hidden_dim(
    exp: Dict[str, Any], runtime_meta: Dict[str, Any], script_text: str
) -> int:
    env = exp.get("env") or {}
    for key in ("HIDDEN_DIM", "HIDDEN_CHANNELS", "hidden_dim", "hidden_channels"):
        value = env.get(key) if isinstance(env, dict) else None
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass

    for key in ("hidden_dim", "hidden_channels"):
        try:
            value = runtime_meta.get(key)
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass

    name = str(exp.get("name") or "")
    parsed = _parse_first_int(r"(?:^|[_-])H(\d+)(?:$|[_-])", name)
    if parsed is not None:
        return parsed

    parsed = _parse_first_int(r"hidden_dim\s*=\s*(\d+)", script_text)
    if parsed is not None:
        return parsed
    return 64


def _infer_num_layers(
    exp: Dict[str, Any], runtime_meta: Dict[str, Any], script_text: str
) -> int:
    env = exp.get("env") or {}
    for key in ("NUM_LAYERS", "num_layers"):
        value = env.get(key) if isinstance(env, dict) else None
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass
    try:
        value = runtime_meta.get("num_layers")
        if value is not None:
            return int(value)
    except (TypeError, ValueError):
        pass
    parsed = _parse_first_int(r"num_layers\s*=\s*(\d+)", script_text)
    if parsed is not None:
        return parsed
    return 2


def _infer_batch_size(
    exp: Dict[str, Any], runtime_meta: Dict[str, Any]
) -> Optional[int]:
    env = exp.get("env") or {}
    candidates = []
    if isinstance(env, dict):
        candidates.extend([env.get("BATCH_SIZE"), env.get("batch_size")])
    candidates.extend([exp.get("batch_size"), runtime_meta.get("batch_size")])
    for value in candidates:
        if isinstance(value, str) and value.strip().lower() == "full-batch":
            return None
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _detect_architecture(
    exp: Dict[str, Any], runtime_meta: Dict[str, Any], script_text: str
) -> str:
    haystack = "\n".join(
        [
            str(exp.get("name") or ""),
            str(runtime_meta.get("model_variant") or ""),
            script_text,
        ]
    ).upper()
    if "ZEBRA" in haystack:
        return "ZEBRA"
    if "GRAPHSAGE" in haystack or "SAGECONV" in haystack:
        return "GRAPHSAGE"
    if "GATV2" in haystack:
        return "GATV2"
    if "GAT" in haystack:
        return "GAT"
    if "GIN" in haystack:
        return "GIN"
    if "GCN" in haystack:
        return "GCN"
    if "TGN" in haystack:
        return "TGN"
    return "DEFAULT"


def _classify_family(
    exp: Dict[str, Any], runtime_meta: Dict[str, Any], script_text: str, arch: str
) -> Dict[str, Any]:
    name = str(exp.get("name") or "").upper()
    temporal_strategy = str(runtime_meta.get("temporal_strategy") or "").strip().lower()
    neighbors = _parse_neighbors(runtime_meta.get("num_neighbors"))
    if not neighbors:
        neighbors = _parse_neighbors((exp.get("env") or {}).get("NUM_NEIGHBORS"))
    script_upper = script_text.upper()

    if "ZEBRA" in name or "SENIOR" in name or arch == "ZEBRA":
        return {
            "memory_family": "no_batch_path_child",
            "execution_mode": "fullgraph_no_batch_path",
            "memory_mode": "n/a",
            "runtime_batch_adjustable": False,
            "neighborloader_applicable": False,
            "neighborloader_recommended": False,
            "fallback_mode": "none",
            "oom_policy_mode": "not_applicable",
        }

    if (
        "TGN" in name
        or "TEMPORAL" in name
        or temporal_strategy not in {"", "disabled", "none"}
        or "TEMPORALDATALOADER" in script_upper
        or "LASTNEIGHBORLOADER" in script_upper
    ):
        return {
            "memory_family": "temporal_edge_batch",
            "execution_mode": "temporal_batch",
            "memory_mode": "temporal_batch",
            "runtime_batch_adjustable": True,
            "neighborloader_applicable": False,
            "neighborloader_recommended": False,
            "fallback_mode": "reduce_batch_size",
            "oom_policy_mode": "batch_adjustable",
        }

    if (
        neighbors
        or "NEIGHBORLOADER" in script_upper
        or "LINKNEIGHBORLOADER" in script_upper
    ):
        return {
            "memory_family": "neighborloader_gnn",
            "execution_mode": "neighborloader",
            "memory_mode": "neighborloader",
            "runtime_batch_adjustable": True,
            "neighborloader_applicable": True,
            "neighborloader_recommended": False,
            "fallback_mode": "reduce_batch_size",
            "oom_policy_mode": "batch_adjustable",
        }

    return {
        "memory_family": "fullbatch_sparse_gnn",
        "execution_mode": "fullbatch",
        "memory_mode": "fullbatch",
        "runtime_batch_adjustable": False,
        "neighborloader_applicable": arch
        in {"GRAPHSAGE", "GCN", "GIN", "GAT", "GATV2"},
        "neighborloader_recommended": False,
        "fallback_mode": "switch_to_neighborloader"
        if arch in {"GRAPHSAGE", "GCN", "GIN", "GAT", "GATV2"}
        else "reduce_hidden_first",
        "oom_policy_mode": "execution_mode_switchable",
    }


def _estimate_param_count(
    arch: str, in_dim: int, hidden_dim: int, out_dim: int, num_layers: int
) -> int:
    in_proj = in_dim * hidden_dim + hidden_dim
    out_proj = hidden_dim * out_dim + out_dim
    bn_params = num_layers * hidden_dim * 2
    if arch in {"GRAPHSAGE", "GCN"}:
        conv_params = num_layers * ((2 * hidden_dim * hidden_dim) + (2 * hidden_dim))
    elif arch == "GIN":
        conv_params = num_layers * ((2 * hidden_dim * hidden_dim) + (2 * hidden_dim))
    elif arch in {"GAT", "GATV2"}:
        conv_params = num_layers * ((4 * hidden_dim * hidden_dim) + (4 * hidden_dim))
    elif arch == "ZEBRA":
        conv_params = num_layers * ((5 * hidden_dim * hidden_dim) + (4 * hidden_dim))
    else:
        conv_params = num_layers * ((2 * hidden_dim * hidden_dim) + (2 * hidden_dim))
    return int(in_proj + conv_params + bn_params + out_proj)


def _sampled_frontier(batch_size: int, neighbors: List[int]) -> Dict[str, int]:
    frontier = 1
    sampled_nodes = 1
    sampled_edges = 0
    for k in neighbors:
        frontier *= max(int(k), 1)
        sampled_nodes += frontier
        sampled_edges += frontier
    return {
        "sampled_nodes": int(batch_size * sampled_nodes),
        "sampled_edges": int(batch_size * sampled_edges),
    }


def _policy_summary(
    family: str,
    upper_mb: int,
    usable_vram_mb: int,
    neighborloader_applicable: bool,
) -> Dict[str, Any]:
    if family == "no_batch_path_child":
        if upper_mb > int(0.85 * usable_vram_mb):
            return {
                "decision": "diagnostic_only_high_risk",
                "neighborloader_recommended": False,
                "reason": "no-batch-path family exceeds 85% of usable VRAM; explicit diagnostic run only",
            }
        return {
            "decision": "not_applicable",
            "neighborloader_recommended": False,
            "reason": "no-batch-path family stays under the conservative 85% usable VRAM threshold",
        }

    if family in {"neighborloader_gnn", "temporal_edge_batch"}:
        if upper_mb > int(0.90 * usable_vram_mb):
            return {
                "decision": "reduce_hidden_first",
                "neighborloader_recommended": family == "neighborloader_gnn",
                "reason": "sampled execution still exceeds 90% of usable VRAM; reduce hidden size first",
            }
        return {
            "decision": "not_applicable",
            "neighborloader_recommended": family == "neighborloader_gnn",
            "reason": "sampled execution is already selected; no additional mode switch needed",
        }

    if upper_mb <= int(0.70 * usable_vram_mb):
        return {
            "decision": "allow_fullbatch",
            "neighborloader_recommended": False,
            "reason": "conservative upper estimate is within 70% of usable VRAM",
        }
    if upper_mb <= int(0.90 * usable_vram_mb):
        return {
            "decision": "warn_allow_fullbatch",
            "neighborloader_recommended": False,
            "reason": "conservative upper estimate is between 70% and 90% of usable VRAM",
        }
    return {
        "decision": "prefer_neighborloader"
        if neighborloader_applicable
        else "reduce_hidden_first",
        "neighborloader_recommended": bool(neighborloader_applicable),
        "reason": "conservative upper estimate exceeds 90% of usable VRAM",
    }


def _estimate_confidence(
    family: str, arch: str, hidden_dim: int, num_layers: int, feature_dim_total: int
) -> Dict[str, Any]:
    if family == "no_batch_path_child":
        return {
            "estimate_confidence": "unreliable_for_decision",
            "can_gate_decide": False,
            "confidence_reason": "no-batch-path family is only safe for diagnostic upper-bound guidance",
        }
    if (
        arch == "GRAPHSAGE"
        and num_layers == 2
        and hidden_dim == 64
        and feature_dim_total in {12, 44}
    ):
        return {
            "estimate_confidence": "calibrated",
            "can_gate_decide": True,
            "confidence_reason": "matches the primary GraphSAGE calibration pair",
        }
    if arch == "GRAPHSAGE":
        return {
            "estimate_confidence": "review",
            "can_gate_decide": True,
            "confidence_reason": "same family as calibration pair but hidden-dim/layer axis still needs calibration",
        }
    return {
        "estimate_confidence": "review",
        "can_gate_decide": True,
        "confidence_reason": "family is estimable but not directly anchored by the primary calibration pair",
    }


def estimate_experiment_memory_contract(
    exp: Dict[str, Any], phase3_root: Path | str = PHASE3_ROOT
) -> Dict[str, Any]:
    phase3_root = Path(phase3_root)
    registry = _load_feature_registry(
        str(phase3_root / FEATURE_REGISTRY_PATH.relative_to(PHASE3_ROOT))
    )
    stats = _load_structure_stats(
        str(phase3_root / STRUCTURE_PATH.relative_to(PHASE3_ROOT))
    )
    runtime_meta = _load_runtime_meta(exp, phase3_root)
    script_text = _load_script_text(exp, phase3_root)
    arch = _detect_architecture(exp, runtime_meta, script_text)
    features, feature_dims, total_feature_dim = _resolve_feature_context(
        exp, runtime_meta, registry, phase3_root, arch
    )
    family_info = _classify_family(exp, runtime_meta, script_text, arch)
    hidden_dim = _infer_hidden_dim(exp, runtime_meta, script_text)
    num_layers = _infer_num_layers(exp, runtime_meta, script_text)
    out_dim = 2
    param_count = _estimate_param_count(
        arch=arch,
        in_dim=total_feature_dim,
        hidden_dim=hidden_dim,
        out_dim=out_dim,
        num_layers=num_layers,
    )

    num_nodes = int(stats["num_nodes"])
    num_edges = int(stats["num_edges"])
    edge_attr_dim = int(stats["edge_attr_dim"])
    edge_attr_mem_mb = _mb(num_edges * edge_attr_dim * BYTES_PER_FP32)
    label_mask_mem_mb = _mb(num_nodes * LABEL_MASK_BYTES_PER_NODE)
    param_mem_mb = _mb(param_count * BYTES_PER_FP32)
    optimizer_mem_mb = _mb(param_count * 8.0)

    family = family_info["memory_family"]
    batch_size = _infer_batch_size(exp, runtime_meta)
    neighbors = _parse_neighbors(runtime_meta.get("num_neighbors"))
    if not neighbors:
        neighbors = _parse_neighbors((exp.get("env") or {}).get("NUM_NEIGHBORS"))

    est_context: Dict[str, Any] = {}
    if family == "neighborloader_gnn":
        effective_batch = batch_size or 1024
        effective_neighbors = neighbors or [15, 10]
        sampled = _sampled_frontier(effective_batch, effective_neighbors)
        sampled_nodes = sampled["sampled_nodes"]
        sampled_edges = sampled["sampled_edges"]
        x_mem_mb = _mb(sampled_nodes * total_feature_dim * BYTES_PER_FP32)
        edge_index_mem_mb = _mb(2 * sampled_edges * INDEX_BYTES)
        sampled_edge_attr_mem_mb = _mb(sampled_edges * edge_attr_dim * BYTES_PER_FP32)
        activation_proxy_mb = _mb(
            NEIGHBOR_ALPHA * sampled_nodes * hidden_dim * num_layers * BYTES_PER_FP32
        )
        message_proxy_mb = _mb(
            NEIGHBOR_BETA * sampled_edges * hidden_dim * num_layers * BYTES_PER_FP32
        )
        safety_buffer_mb = NEIGHBOR_SAFETY_MB
        est_context = {
            "estimated_sampled_nodes": sampled_nodes,
            "estimated_sampled_edges": sampled_edges,
            "batch_size": effective_batch,
            "num_neighbors": effective_neighbors,
        }
        edge_attr_component_mb = sampled_edge_attr_mem_mb
    elif family == "temporal_edge_batch":
        effective_batch = batch_size or 4096
        effective_neighbors = neighbors or [20, 10]
        sampled = _sampled_frontier(effective_batch, effective_neighbors)
        sampled_nodes = max(sampled["sampled_nodes"], effective_batch * 2)
        sampled_edges = max(sampled["sampled_edges"], effective_batch)
        x_mem_mb = _mb(sampled_nodes * total_feature_dim * BYTES_PER_FP32)
        edge_index_mem_mb = _mb(2 * sampled_edges * INDEX_BYTES)
        sampled_edge_attr_mem_mb = _mb(sampled_edges * edge_attr_dim * BYTES_PER_FP32)
        activation_proxy_mb = _mb(
            TEMPORAL_ALPHA * sampled_nodes * hidden_dim * num_layers * BYTES_PER_FP32
        )
        message_proxy_mb = _mb(
            TEMPORAL_BETA * sampled_edges * hidden_dim * num_layers * BYTES_PER_FP32
        )
        safety_buffer_mb = TEMPORAL_SAFETY_MB
        est_context = {
            "estimated_sampled_nodes": sampled_nodes,
            "estimated_sampled_edges": sampled_edges,
            "batch_size": effective_batch,
            "num_neighbors": effective_neighbors,
        }
        edge_attr_component_mb = sampled_edge_attr_mem_mb
    else:
        x_mem_mb = _mb(num_nodes * total_feature_dim * BYTES_PER_FP32)
        edge_index_mem_mb = _mb(2 * num_edges * INDEX_BYTES)
        if family == "no_batch_path_child":
            activation_proxy_mb = _mb(
                NO_BATCH_ALPHA * num_nodes * hidden_dim * num_layers * BYTES_PER_FP32
            )
            message_proxy_mb = _mb(
                NO_BATCH_BETA * num_edges * hidden_dim * num_layers * BYTES_PER_FP32
            )
            safety_buffer_mb = NO_BATCH_SAFETY_MB
        else:
            alpha = FULLBATCH_ALPHA.get(arch, FULLBATCH_ALPHA["DEFAULT"])
            beta = FULLBATCH_BETA.get(arch, FULLBATCH_BETA["DEFAULT"])
            safety_buffer_mb = FULLBATCH_SAFETY.get(arch, FULLBATCH_SAFETY["DEFAULT"])
            activation_proxy_mb = _mb(
                alpha * num_nodes * hidden_dim * num_layers * BYTES_PER_FP32
            )
            message_proxy_mb = _mb(
                beta * num_edges * hidden_dim * num_layers * BYTES_PER_FP32
            )
        edge_attr_component_mb = edge_attr_mem_mb

    lower_mb = (
        x_mem_mb
        + edge_index_mem_mb
        + edge_attr_component_mb
        + label_mask_mem_mb
        + param_mem_mb
        + optimizer_mem_mb
        + activation_proxy_mb
        + message_proxy_mb
    )
    upper_mb = lower_mb + safety_buffer_mb
    eval_mb = (
        x_mem_mb
        + edge_index_mem_mb
        + edge_attr_component_mb
        + label_mask_mem_mb
        + param_mem_mb
        + activation_proxy_mb * EVAL_ACTIVATION_RATIO
        + message_proxy_mb
    )
    predict_score_mb = _mb(num_nodes * PREDICT_SCORE_BYTES_PER_NODE)
    predict_mb = (
        x_mem_mb
        + edge_index_mem_mb
        + edge_attr_component_mb
        + param_mem_mb
        + activation_proxy_mb * PREDICT_ACTIVATION_RATIO
        + message_proxy_mb
        + predict_score_mb
        + PREDICT_POSTPROC_BUFFER_MB
    )
    lower_mb_i = _round_mb(lower_mb)
    eval_mb_i = _round_mb(eval_mb)
    predict_mb_i = _round_mb(predict_mb)
    upper_mb_i = _round_mb(upper_mb)

    usable_vram_mb = CARD_TOTAL_VRAM_MB - DEFAULT_VRAM_RESERVE_MB
    policy = _policy_summary(
        family=family,
        upper_mb=upper_mb_i,
        usable_vram_mb=usable_vram_mb,
        neighborloader_applicable=bool(family_info["neighborloader_applicable"]),
    )
    confidence = _estimate_confidence(
        family=family,
        arch=arch,
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        feature_dim_total=total_feature_dim,
    )

    reason_parts = [
        f"family={family}",
        f"arch={arch}",
        f"N={num_nodes}",
        f"E={num_edges}",
        f"F={total_feature_dim}",
        f"H={hidden_dim}",
        f"L={num_layers}",
        f"execution_mode={family_info['execution_mode']}",
        policy["reason"],
    ]
    if est_context:
        if "estimated_sampled_nodes" in est_context:
            reason_parts.append(
                f"sampled_nodes={est_context['estimated_sampled_nodes']}"
            )
        if "estimated_sampled_edges" in est_context:
            reason_parts.append(
                f"sampled_edges={est_context['estimated_sampled_edges']}"
            )

    contract: Dict[str, Any] = {
        "memory_family": family,
        "execution_mode": family_info["execution_mode"],
        "memory_mode": family_info["memory_mode"],
        "runtime_batch_adjustable": bool(family_info["runtime_batch_adjustable"]),
        "neighborloader_applicable": bool(family_info["neighborloader_applicable"]),
        "neighborloader_recommended": bool(policy["neighborloader_recommended"]),
        "fallback_mode": str(family_info["fallback_mode"]),
        "est_mem_formula_version": FORMULA_VERSION,
        "est_mem_lower_mb": lower_mb_i,
        "est_train_mem_mb": lower_mb_i,
        "est_eval_mem_mb": eval_mb_i,
        "est_predict_mem_mb": predict_mb_i,
        "est_mem_upper_mb": upper_mb_i,
        "est_mem_initial_mb": upper_mb_i,
        "est_mem_decision_mb": max(lower_mb_i, eval_mb_i, predict_mb_i, upper_mb_i),
        "est_mem_reason": "; ".join(reason_parts),
        "safety_buffer_mb": int(math.ceil(safety_buffer_mb)),
        "oom_policy_mode": str(family_info["oom_policy_mode"]),
        "card_total_vram_mb": CARD_TOTAL_VRAM_MB,
        "usable_vram_mb": usable_vram_mb,
        "decision": policy["decision"],
        "estimate_confidence": confidence["estimate_confidence"],
        "can_gate_decide": confidence["can_gate_decide"],
        "confidence_reason": confidence["confidence_reason"],
        "graph_stats": {
            "num_nodes": num_nodes,
            "num_edges": num_edges,
            "edge_attr_dim": edge_attr_dim,
        },
        "feature_dims": feature_dims,
        "feature_dim_total": total_feature_dim,
        "hidden_dim": hidden_dim,
        "num_layers": num_layers,
        "model_arch": arch,
        "component_mb": {
            "x_mem_mb": round(x_mem_mb, 2),
            "edge_index_mem_mb": round(edge_index_mem_mb, 2),
            "edge_attr_mem_mb": round(edge_attr_component_mb, 2),
            "label_mask_mem_mb": round(label_mask_mem_mb, 2),
            "param_mem_mb": round(param_mem_mb, 2),
            "optimizer_mem_mb": round(optimizer_mem_mb, 2),
            "activation_proxy_mb": round(activation_proxy_mb, 2),
            "message_passing_proxy_mb": round(message_proxy_mb, 2),
            "predict_score_mb": round(predict_score_mb, 2),
        },
    }
    contract.update(est_context)
    return contract


def build_calibration_row(
    exp_name: str,
    contract: Dict[str, Any],
    observed_peak_mb: float,
    note: str = "",
) -> Dict[str, Any]:
    graph_stats = contract.get("graph_stats") or {}
    static_est_mb = float(
        contract.get("est_mem_decision_mb") or contract.get("est_mem_upper_mb") or 0
    )
    observed_peak_mb = float(observed_peak_mb)
    abs_error = abs(static_est_mb - observed_peak_mb)
    rel_error = abs_error / observed_peak_mb if observed_peak_mb > 0 else 0.0
    return {
        "exp_name": exp_name,
        "N": int(graph_stats.get("num_nodes") or 0),
        "E": int(graph_stats.get("num_edges") or 0),
        "F": int(contract.get("feature_dim_total") or 0),
        "H": int(contract.get("hidden_dim") or 0),
        "family": str(contract.get("memory_family") or ""),
        "static_est_mb": round(static_est_mb, 2),
        "observed_peak_mb": round(observed_peak_mb, 2),
        "abs_error_mb": round(abs_error, 2),
        "rel_error": round(rel_error, 4),
        "notes": note,
    }


def build_holdout_row(
    exp_name: str,
    contract: Dict[str, Any],
    observed_peak_mb: float,
    verdict: str = "",
) -> Dict[str, Any]:
    observed_peak_mb = float(observed_peak_mb)
    est_train = float(contract.get("est_train_mem_mb") or 0)
    abs_error = abs(est_train - observed_peak_mb)
    rel_error = abs_error / observed_peak_mb if observed_peak_mb > 0 else 0.0
    resolved_verdict = verdict or ("pass" if rel_error <= 0.15 else "review")
    return {
        "exp_name": exp_name,
        "family": str(contract.get("memory_family") or ""),
        "F": int(contract.get("feature_dim_total") or 0),
        "H": int(contract.get("hidden_dim") or 0),
        "L": int(contract.get("num_layers") or 0),
        "est_train": round(est_train, 2),
        "est_eval": round(float(contract.get("est_eval_mem_mb") or 0), 2),
        "observed_peak": round(observed_peak_mb, 2),
        "abs_error": round(abs_error, 2),
        "rel_error": round(rel_error, 4),
        "verdict": resolved_verdict,
    }


def infer_memory_contract_for_exp(
    exp: Dict[str, Any], phase3_root: Path | str = PHASE3_ROOT
) -> Dict[str, Any]:
    existing = exp.get("memory_contract")
    if isinstance(existing, dict) and existing:
        return existing
    return estimate_experiment_memory_contract(exp, phase3_root)


def compute_zebra_static_upper_bounds(
    phase3_root: Path | str = PHASE3_ROOT,
) -> List[Dict[str, Any]]:
    phase3_root = Path(phase3_root)
    rows: List[Dict[str, Any]] = []
    for hidden in (10, 20, 30, 56):
        exp_name = f"EX_SENIOR_ZEBRA_M1Diag_H{hidden}"
        exp = {
            "name": exp_name,
            "features": [],
            "script": f"experiments/{exp_name}/scripts/train.py",
            "env": {"HIDDEN_DIM": hidden},
            "description": f"virtual zebra upper-bound hidden={hidden}",
        }
        contract = estimate_experiment_memory_contract(exp, phase3_root)
        rows.append(
            {
                "label": f"ZEBRA_H{hidden}_static_estimate",
                "exp_name": exp_name,
                "hidden_dim": hidden,
                "family": str(contract.get("memory_family") or ""),
                "execution_mode": str(contract.get("execution_mode") or ""),
                "est_train": float(contract.get("est_train_mem_mb") or 0),
                "est_eval": float(contract.get("est_eval_mem_mb") or 0),
                "est_predict": float(contract.get("est_predict_mem_mb") or 0),
                "est_upper": float(contract.get("est_mem_upper_mb") or 0),
                "est_decision": float(contract.get("est_mem_decision_mb") or 0),
                "decision": str(contract.get("decision") or ""),
                "reason": str(contract.get("est_mem_reason") or ""),
                "virtual_only": hidden == 56,
            }
        )
    return rows


def compute_graphsage_calibration(
    phase3_root: Path | str = PHASE3_ROOT,
) -> Dict[str, Any]:
    phase3_root = Path(phase3_root)
    baseline_exp = {
        "name": "EX_PHASE3_GraphSAGE_Baseline_LeakSafe",
        "features": ["base_basic12_cut_d152"],
        "script": "scripts/train_phase3_graphsage_targeted.py",
    }
    regen_exp = {
        "name": "EX_PHASE3_GraphSAGE_BaselinePlus_Regenerated32",
        "features": [
            "base_basic12_cut_d152",
            "base_regen22_cut_d152",
            "balance_vol_4dim_cut_d152",
            "velocity_3dim_cut_d152",
            "burst_3dim_cut_d152",
        ],
        "script": "scripts/train_phase3_graphsage_targeted.py",
    }
    baseline_results = _read_json(
        phase3_root / "results_db" / "EX_PHASE3_GraphSAGE_Baseline_LeakSafe.json"
    )
    regen_results = _read_json(
        phase3_root
        / "results_db"
        / "EX_PHASE3_GraphSAGE_BaselinePlus_Regenerated32.json"
    )
    baseline_contract = estimate_experiment_memory_contract(baseline_exp, phase3_root)
    regen_contract = estimate_experiment_memory_contract(regen_exp, phase3_root)
    feature_delta_mb = _mb(
        baseline_contract["graph_stats"]["num_nodes"]
        * (regen_contract["feature_dim_total"] - baseline_contract["feature_dim_total"])
        * BYTES_PER_FP32
    )
    observed_delta_mb = float(regen_results.get("peak_memory_mb", 0.0)) - float(
        baseline_results.get("peak_memory_mb", 0.0)
    )
    return {
        "rows": [
            build_calibration_row(
                baseline_exp["name"],
                baseline_contract,
                float(baseline_results.get("peak_memory_mb", 0.0)),
                note="primary calibration sample",
            ),
            build_calibration_row(
                regen_exp["name"],
                regen_contract,
                float(regen_results.get("peak_memory_mb", 0.0)),
                note="primary calibration sample (+32 feature dims)",
            ),
        ],
        "feature_delta_theoretical_mb": round(feature_delta_mb, 2),
        "feature_delta_observed_mb": round(observed_delta_mb, 2),
        "delta_gap_mb": round(abs(observed_delta_mb - feature_delta_mb), 2),
    }
