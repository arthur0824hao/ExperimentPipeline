#!/usr/bin/env python3
"""Shared experiment registration logic extracted from preprocess.py (T10)."""

from __future__ import annotations

from datetime import datetime
from typing import Dict


def build_experiment_config(exp: Dict, batch_id: str) -> Dict:
    name = exp["name"]
    config = {
        "name": name,
        "script": exp.get("script", f"experiments/{name}/scripts/train.py"),
        "description": exp.get("description", "Auto-registered from ready.json"),
        "batch_id": batch_id,
        "status": "READY",
        "priority": exp.get("priority", 0),
        "registered_at": datetime.now().isoformat(),
        "running_on": None,
        "completed_at": None,
        "error_info": None,
        "parent_experiment": exp.get("parent_experiment"),
        "role": exp.get("role"),
    }

    for key in (
        "max_retries",
        "preferred_worker",
        "batch_size",
        "eval_batch_size",
        "display_order",
        "group_id",
        "main_experiment",
        "condition_parent",
        "gate_type",
        "gate_evidence_ref",
    ):
        if key in exp:
            config[key] = exp.get(key)

    memory_contract = exp.get("memory_contract")
    if isinstance(memory_contract, dict) and memory_contract:
        config["memory_contract"] = dict(memory_contract)

    env_overrides = exp.get("env")
    if isinstance(env_overrides, dict) and env_overrides:
        config["env"] = {
            str(k): str(v)
            for k, v in env_overrides.items()
            if k is not None and v is not None
        }

    return config


def register_experiment(exp: Dict, exp_data: Dict, batch_id: str) -> Dict:
    experiments = exp_data.get("experiments", [])
    existing_names = {e["name"] for e in experiments}

    if exp["name"] in existing_names:
        print(f"  Experiment {exp['name']} already registered, skipping...")
        return exp_data

    config = build_experiment_config(exp, batch_id)
    experiments.append(config)
    exp_data["experiments"] = experiments
    print(f"  Registered: {exp['name']} (batch: {batch_id})")
    return exp_data
