#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Register experiments into ready.json with unit-test gate."""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

PHASE3_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PHASE3_ROOT))

import preprocess as preprocess


def _sync_added_entries_to_db(added_entries: List[Dict[str, Any]]) -> None:
    if not added_entries:
        return

    try:
        import psycopg2
        from psycopg2.extras import Json
        from db_registry import _get_dsn
    except Exception as e:
        raise RuntimeError(f"DB sync import failed: {e}") from e

    try:
        conn = psycopg2.connect(_get_dsn())
        cur = conn.cursor()
        cur.execute(
            "ALTER TABLE exp_registry.experiments "
            "ADD COLUMN IF NOT EXISTS parent_experiment TEXT"
        )
        cur.execute(
            "ALTER TABLE exp_registry.experiments "
            "ADD COLUMN IF NOT EXISTS doc_processed_at TIMESTAMPTZ"
        )
        cur.execute(
            "SELECT COALESCE(MAX(display_order), 0) FROM exp_registry.experiments"
        )
        row = cur.fetchone()
        current_max = int(row[0]) if row is not None and row[0] is not None else 0
        next_order = current_max + 1

        for exp in added_entries:
            extra = {
                "priority": int(exp.get("priority", 0) or 0),
                "description": str(exp.get("description", "")),
                "parent_experiment": str(exp.get("parent_experiment", "") or ""),
                "role": str(exp.get("role", "") or ""),
                "main_experiment": str(exp.get("main_experiment", "") or ""),
            }
            for key in ("condition_parent", "gate_type", "gate_evidence_ref"):
                value = exp.get(key)
                if value is None:
                    continue
                text = str(value).strip()
                if text:
                    extra[key] = text
            memory_contract = exp.get("memory_contract")
            if isinstance(memory_contract, dict) and memory_contract:
                extra["memory_contract"] = memory_contract
            cur.execute(
                """
                INSERT INTO exp_registry.experiments
                (name, batch_id, status, script_path, display_order, extra)
                VALUES (%s, %s, 'NEEDS_RERUN', %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                  batch_id = EXCLUDED.batch_id,
                  status = 'NEEDS_RERUN',
                  script_path = EXCLUDED.script_path,
                  display_order = EXCLUDED.display_order,
                  extra = EXCLUDED.extra,
                  updated_at = NOW()
                """,
                (
                    str(exp["name"]),
                    str(exp.get("batch_id", "")),
                    str(exp.get("script", "")),
                    next_order,
                    Json(extra),
                ),
            )
            parent = str(exp.get("parent_experiment", "") or "").strip()
            if parent:
                cur.execute(
                    "UPDATE exp_registry.experiments SET parent_experiment=%s WHERE name=%s",
                    (parent, str(exp["name"])),
                )
            next_order += 1

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise RuntimeError(f"DB sync failed: {e}") from e


READY_FILE = PHASE3_ROOT / "ready.json"


def _load_entries(entry_path: Path) -> List[Dict[str, Any]]:
    with open(entry_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("Entry JSON must be a dict or list")


def _normalize_ready_data(raw: Any, batch_id: str) -> Dict[str, Any]:
    if isinstance(raw, list):
        data = {"ready_to_process": 1, "batch_id": "legacy", "experiments": raw}
    elif isinstance(raw, dict):
        data = raw
    else:
        data = {"ready_to_process": 0, "batch_id": batch_id, "experiments": []}

    if not isinstance(data.get("experiments"), list):
        data["experiments"] = []
    if "ready_to_process" not in data:
        data["ready_to_process"] = 0
    if not data.get("batch_id"):
        data["batch_id"] = (
            batch_id or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
    return data


def _is_valid_v3_main_name(name: str) -> bool:
    return bool(
        re.match(
            r"^[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+$", name
        )
    )


def _is_valid_v3_sub_name(name: str) -> bool:
    return bool(re.match(r"^SUB_[A-Za-z0-9]+_[A-Za-z0-9_]+$", name))


def _is_valid_frozen_exp_name(name: str) -> bool:
    return bool(
        re.match(
            r"^EXP_P[13]_(GS|ZB)_H[0-9]+_(ORIGIN|YOUR32|SENIOR10|COMBINED)_V[0-9]+$",
            name,
        )
    )


def _validate_entry(exp: Dict[str, Any], enforce_v3_naming: bool) -> List[str]:
    missing = []
    name = str(exp.get("name") or "").strip()
    if not name:
        missing.append("name")
    if not exp.get("features"):
        missing.append("features")

    if enforce_v3_naming and name:
        role = str(exp.get("role") or "").strip().lower()
        if role == "child" or name.startswith("SUB_"):
            if not _is_valid_v3_sub_name(name):
                missing.append(
                    "name(v3): child must match SUB_[ParentShortName]_[Variant]"
                )
        elif not (_is_valid_v3_main_name(name) or _is_valid_frozen_exp_name(name)):
            missing.append(
                "name(v3): main must match [Phase]_[ModelArch]_[LossType]_[FeatureVariant]_[HyperParam] or frozen EXP_* contract"
            )
    return missing


def _build_known_names_and_status(
    raw_registry: Any,
) -> tuple[set, list, dict]:
    known_names: set = set()
    if isinstance(raw_registry, dict):
        active_registry = raw_registry.get("experiments", [])
        for key in ("experiments", "completed", "archived"):
            for item in raw_registry.get(key, []):
                if isinstance(item, dict) and item.get("name"):
                    known_names.add(str(item["name"]))
    elif isinstance(raw_registry, list):
        active_registry = raw_registry
        for item in raw_registry:
            if isinstance(item, dict) and item.get("name"):
                known_names.add(str(item["name"]))
    else:
        active_registry = []
    active_status_by_name = {
        str(e.get("name", "")): str(e.get("status", "")).upper()
        for e in active_registry
        if isinstance(e, dict) and e.get("name")
    }
    return known_names, active_registry, active_status_by_name


def _classify_entries(
    entries: List[Dict[str, Any]],
    known_names: set,
    active_status_by_name: dict,
    existing_names: set,
) -> tuple[list, list]:
    would_add: list = []
    would_block: list = []

    for exp in entries:
        name = exp.get("name", "")
        active_status = active_status_by_name.get(name)
        if active_status == "RUNNING":
            would_block.append({"name": name, "reason": "RUNNING in registry"})
            continue

        enforce_v3_naming = name not in known_names
        missing = _validate_entry(exp, enforce_v3_naming)
        if missing:
            would_block.append(
                {
                    "name": name or "<unknown>",
                    "reason": f"Missing: {', '.join(missing)}",
                }
            )
            continue

        would_add.append(name)

    return would_add, would_block


def build_parser() -> argparse.ArgumentParser:
    from cli_shared import add_common_args

    parser = argparse.ArgumentParser(
        description="Register experiments to ready.json",
        epilog="Examples:\n"
        "  %(prog)s --entry entries/my_exp.json --set-ready\n"
        "  %(prog)s --entry entries/my_exp.json --dry-run --output json\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_common_args(parser)
    parser.add_argument(
        "--entry",
        action="append",
        required=True,
        help="Path to JSON entry (dict) or list of entries",
    )
    parser.add_argument("--batch-id", default="", help="Set batch_id if missing")
    parser.add_argument(
        "--set-ready",
        action="store_true",
        help="Set ready_to_process=1 after adding",
    )
    return parser


def main(args: Any | None = None) -> int:
    from cli_shared import (
        EXIT_CODES,
        emit_result,
        setup_logging,
    )

    if args is None:
        args = build_parser().parse_args()
    setup_logging(args)

    dry_run = getattr(args, "dry_run", False)

    raw_ready = preprocess.load_json(READY_FILE)
    ready_data = _normalize_ready_data(raw_ready, args.batch_id)

    raw_registry = preprocess.load_json(preprocess.EXPERIMENTS_FILE)
    known_names, active_registry, active_status_by_name = _build_known_names_and_status(
        raw_registry
    )

    existing_names = {e.get("name") for e in ready_data.get("experiments", [])}

    entries: List[Dict[str, Any]] = []
    for entry_path in args.entry:
        entries.extend(_load_entries(Path(entry_path)))

    if dry_run:
        would_add, would_block = _classify_entries(
            entries, known_names, active_status_by_name, existing_names
        )
        report = {
            "dry_run": True,
            "would_add": would_add,
            "would_block": would_block,
            "batch_id": ready_data.get("batch_id", ""),
            "would_set_ready": bool(args.set_ready),
            "would_sync_db": len(would_add) > 0,
        }
        emit_result(args, report, status="ok")
        return EXIT_CODES["SUCCESS"]

    if args.batch_id and ready_data.get("batch_id") != args.batch_id:
        print(
            f"[WARN] ready.json batch_id='{ready_data.get('batch_id')}' kept; "
            f"--batch-id '{args.batch_id}' ignored."
        )

    added = []
    blocked = []

    for exp in entries:
        name = exp.get("name", "")

        active_status = active_status_by_name.get(name)
        if active_status == "RUNNING":
            blocked.append(
                (
                    name,
                    "Experiment is RUNNING in registry. Use a new experiment name (recommended EX_*).",
                )
            )
            continue

        if name in existing_names:
            ready_data["experiments"] = [
                e for e in ready_data["experiments"] if e.get("name") != name
            ]
            print(f"[INFO] Updated queued experiment entry: {name}")

        enforce_v3_naming = name not in known_names
        missing = _validate_entry(exp, enforce_v3_naming)
        if missing:
            blocked.append(
                (name or "<unknown>", f"Missing fields: {', '.join(missing)}")
            )
            continue

        gate = preprocess.run_experiment_gate(exp)
        if not gate["passed"]:
            blocked.append((name, f"{gate['status']}: {gate['message']}"))
            continue

        exp["gate_status"] = gate["status"]
        exp["gate_checked_at"] = datetime.now().isoformat()
        if isinstance(gate.get("memory_contract"), dict):
            exp["memory_contract"] = dict(gate["memory_contract"])
        ready_data["experiments"].append(exp)
        existing_names.add(name)
        added.append(name)

    if args.set_ready:
        ready_data["ready_to_process"] = 1

    if added:
        preprocess.save_json(READY_FILE, ready_data)
        print(f"Added {len(added)} experiment(s): {added}")
        added_entries = [
            e for e in ready_data.get("experiments", []) if e.get("name") in added
        ]
        try:
            _sync_added_entries_to_db(added_entries)
            print(f"Synced {len(added_entries)} experiment(s) to DB registry")
        except Exception as e:
            print(f"[ERROR] {e}")
            return EXIT_CODES["RUNTIME_ERROR"]
    else:
        print("No experiments added.")

    if blocked:
        print("Blocked entries:")
        for name, reason in blocked:
            print(f"  - {name}: {reason}")
        return EXIT_CODES["GENERAL_ERROR"]

    return EXIT_CODES["SUCCESS"]


if __name__ == "__main__":
    raise SystemExit(main())
