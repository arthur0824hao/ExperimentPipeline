#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence


def load_machine_constraints(machine_paths: Sequence[Path]) -> Dict[str, Dict[str, Any]]:
    for config_path in machine_paths:
        try:
            if not config_path.exists():
                continue
            data = json.loads(config_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {str(k): v for k, v in data.items() if isinstance(v, dict)}
        except Exception:
            continue
    return {}


def load_worker_whitelist(machine_paths: Sequence[Path]) -> list[str]:
    return sorted(load_machine_constraints(machine_paths).keys())


def normalize_worker_whitelist(workers: Iterable[str]) -> list[str]:
    return sorted(
        {str(worker or "").strip() for worker in workers if str(worker or "").strip()}
    )


def filter_worker_heartbeats(
    heartbeats: Mapping[str, Dict[str, Any]],
    whitelist: Iterable[str],
    *,
    fail_closed: bool = False,
) -> Dict[str, Dict[str, Any]]:
    allowed = set(normalize_worker_whitelist(whitelist))
    if not allowed:
        if fail_closed:
            return {}
        return {
            str(worker_id): dict(payload)
            for worker_id, payload in heartbeats.items()
            if isinstance(payload, dict)
        }

    filtered: Dict[str, Dict[str, Any]] = {}
    for worker_id, payload in heartbeats.items():
        worker_token = str(worker_id or "").strip()
        if not worker_token or worker_token not in allowed:
            continue
        if isinstance(payload, dict):
            filtered[worker_token] = dict(payload)
    return filtered


def get_worker_heartbeat(
    heartbeats: Mapping[str, Dict[str, Any]],
    worker_id: str,
    whitelist: Iterable[str],
    *,
    fail_closed: bool = False,
) -> Dict[str, Any]:
    worker_token = str(worker_id or "").strip()
    if not worker_token:
        return {}
    filtered = filter_worker_heartbeats(heartbeats, whitelist, fail_closed=fail_closed)
    payload = filtered.get(worker_token)
    return dict(payload) if isinstance(payload, dict) else {}
