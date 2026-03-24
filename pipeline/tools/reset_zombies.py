#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

PIPELINE_DIR = Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from db_registry import DBExperimentsDB, get_conn, sync_snapshot_to_json

ZOMBIE_RUNNING_NAMES = [
    "EXP_P3_GS_H56_ORIGIN_V1",
    "EXP_P3_GS_H56_SENIOR10_V1",
    "EXP_P3_GS_H56_YOUR32_V1",
]

STALE_SCRIPT_ERROR_NAMES = [
    "EXP_P1_GS_H56_SENIOR10_V1",
    "EXP_P1_GS_H56_YOUR32_V1",
    "EXP_P1_ZB_H30_COMBINED_V1",
    "EXP_P1_ZB_H30_ORIGIN_V1",
    "EXP_P3_ZB_H30_SENIOR10_V1",
    "EXP_P3_ZB_H30_YOUR32_V1",
]


def _fetch_name_state(cur, names: List[str]) -> Dict[str, Dict[str, object]]:
    cur.execute(
        """
        SELECT name, status, run_id, error_type
        FROM exp_registry.experiments
        WHERE name = ANY(%s)
        """,
        (names,),
    )
    rows = cur.fetchall()
    out: Dict[str, Dict[str, object]] = {}
    for name, status, run_id, error_type in rows:
        out[str(name)] = {
            "status": status,
            "run_id": run_id,
            "error_type": error_type,
        }
    return out


def _insert_status_log(
    cur,
    name: str,
    old_status: str,
    new_status: str,
    reason: str,
    run_id,
) -> None:
    cur.execute(
        """
        INSERT INTO exp_registry.status_log(
            experiment_name, old_status, new_status, changed_by, reason, run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (name, old_status, new_status, "bundle-b010-cleanup", reason, run_id),
    )


def cleanup(dsn: str, json_path: Path, dry_run: bool = False) -> Dict[str, object]:
    zombie_before: Dict[str, Dict[str, object]] = {}
    stale_before: Dict[str, Dict[str, object]] = {}

    with get_conn(dsn) as conn:
        with conn.cursor() as cur:
            zombie_before = _fetch_name_state(cur, ZOMBIE_RUNNING_NAMES)
            stale_before = _fetch_name_state(cur, STALE_SCRIPT_ERROR_NAMES)

            zombie_to_reset = [
                name
                for name in ZOMBIE_RUNNING_NAMES
                if str(zombie_before.get(name, {}).get("status") or "") == "RUNNING"
            ]
            stale_to_clear = [
                name
                for name in STALE_SCRIPT_ERROR_NAMES
                if str(stale_before.get(name, {}).get("error_type") or "") == "SCRIPT_ERROR"
            ]

            if not dry_run and zombie_to_reset:
                cur.execute(
                    """
                    UPDATE exp_registry.experiments
                    SET status = 'NEEDS_RERUN',
                        run_id = NULL,
                        worker_id = NULL,
                        gpu_id = NULL,
                        pid = NULL,
                        started_at = NULL,
                        retry_count = 0,
                        oom_retry_count = 0
                    WHERE name = ANY(%s)
                      AND status = 'RUNNING'
                    """,
                    (zombie_to_reset,),
                )
                for name in zombie_to_reset:
                    old_status = str(zombie_before.get(name, {}).get("status") or "RUNNING")
                    _insert_status_log(
                        cur,
                        name,
                        old_status,
                        "NEEDS_RERUN",
                        "bundle B-010 zombie reset",
                        zombie_before.get(name, {}).get("run_id"),
                    )

            if not dry_run and stale_to_clear:
                cur.execute(
                    """
                    UPDATE exp_registry.experiments
                    SET error_type = NULL,
                        error_message = NULL,
                        failed_at = NULL,
                        is_true_oom = FALSE,
                        error_peak_mb = 0
                    WHERE name = ANY(%s)
                      AND error_type = 'SCRIPT_ERROR'
                    """,
                    (stale_to_clear,),
                )
                for name in stale_to_clear:
                    old_status = str(stale_before.get(name, {}).get("status") or "NEEDS_RERUN")
                    _insert_status_log(
                        cur,
                        name,
                        old_status,
                        old_status,
                        "bundle B-010 stale SCRIPT_ERROR cleared",
                        stale_before.get(name, {}).get("run_id"),
                    )

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'RUNNING') AS running_count,
                    COUNT(*) FILTER (WHERE status = 'NEEDS_RERUN') AS rerun_count,
                    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_count,
                    COUNT(*) FILTER (WHERE error_type IS NOT NULL) AS nonnull_error_count,
                    COUNT(*) AS total_count
                FROM exp_registry.experiments
                """
            )
            row = cur.fetchone()

    if not dry_run:
        sync_snapshot_to_json(json_path=json_path, dsn=dsn)

    return {
        "dry_run": dry_run,
        "zombie_candidates": ZOMBIE_RUNNING_NAMES,
        "stale_error_candidates": STALE_SCRIPT_ERROR_NAMES,
        "zombies_reset": [
            name
            for name in ZOMBIE_RUNNING_NAMES
            if str(zombie_before.get(name, {}).get("status") or "") == "RUNNING"
        ],
        "stale_errors_cleared": [
            name
            for name in STALE_SCRIPT_ERROR_NAMES
            if str(stale_before.get(name, {}).get("error_type") or "") == "SCRIPT_ERROR"
        ],
        "final_counts": {
            "running": int(row[0]),
            "needs_rerun": int(row[1]),
            "completed": int(row[2]),
            "error_type_nonnull": int(row[3]),
            "total": int(row[4]),
        },
        "snapshot_json": str(json_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset B-010 zombie/stale experiment state")
    parser.add_argument("--dry-run", action="store_true", help="Show planned changes only")
    parser.add_argument(
        "--json-path",
        default=str((Path(__file__).resolve().parents[1] / "experiments.json")),
        help="Path to experiments snapshot JSON",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db = DBExperimentsDB()
    result = cleanup(
        dsn=db.dsn,
        json_path=Path(args.json_path),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(result, ensure_ascii=True, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
