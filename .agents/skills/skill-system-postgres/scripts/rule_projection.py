#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[3]
WORKFLOW_TICKETS_PATH = (
    ROOT_DIR / "skills" / "skill-system-workflow" / "scripts" / "tickets.py"
)
RULE_SOURCE_CONFIG = [
    {
        "rule_scope": "skill_system",
        "source_path": ROOT_DIR / "AGENTS.md",
        "priority": 10,
        "projection_path": ROOT_DIR / "note" / "skill_system_rules.md",
        "title": "Skill System Rules",
    },
    {
        "rule_scope": "project",
        "source_path": ROOT_DIR / "note" / "note_rules.md",
        "priority": 20,
        "projection_path": ROOT_DIR / "note" / "project_rules.md",
        "title": "Project Rules",
    },
    {
        "rule_scope": "compat",
        "source_path": ROOT_DIR / "review" / "REVIEW_AGENT_PROTOCOL.md",
        "priority": 30,
        "projection_path": ROOT_DIR / "note" / "compat_rules.md",
        "title": "Compatibility Rules",
    },
]


def load_workflow_module():
    spec = importlib.util.spec_from_file_location(
        "rule_projection_workflow", WORKFLOW_TICKETS_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {WORKFLOW_TICKETS_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def connect_rule_db():
    workflow_mod = load_workflow_module()
    return workflow_mod.connect_workflow_db()


def display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def extract_rule_entries(text: str) -> list[str]:
    entries: list[str] = []
    in_code_fence = False
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_code_fence = not in_code_fence
            continue
        if in_code_fence or not stripped:
            continue

        candidate = ""
        if stripped.startswith(("- ", "* ")):
            candidate = stripped[2:].strip()
        elif re.match(r"^\d+\.\s+", stripped):
            candidate = re.sub(r"^\d+\.\s+", "", stripped).strip()
        elif stripped.startswith(">"):
            candidate = stripped[1:].strip()

        if candidate:
            entries.append(candidate)
    return dedupe_preserve_order(entries)


def build_rule_bundle(repo_root: Path = ROOT_DIR) -> dict[str, Any]:
    rule_sets: list[dict[str, Any]] = []
    for config in RULE_SOURCE_CONFIG:
        source_path = config["source_path"]
        text = source_path.read_text(encoding="utf-8") if source_path.exists() else ""
        entries = extract_rule_entries(text)
        rule_sets.append(
            {
                "rule_scope": config["rule_scope"],
                "source_path": display_path(source_path, repo_root),
                "priority": config["priority"],
                "projection_path": display_path(config["projection_path"], repo_root),
                "title": config["title"],
                "entries": [
                    {
                        "rule_text": entry,
                        "rule_hash": stable_hash(entry),
                        "enabled": True,
                    }
                    for entry in entries
                ],
            }
        )
    return {
        "merge_priority": [
            item["rule_scope"]
            for item in sorted(rule_sets, key=lambda item: item["priority"])
        ],
        "rule_sets": rule_sets,
    }


def sync_rule_bundle(conn: Any, bundle: dict[str, Any]) -> dict[str, Any]:
    synced_sets: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        for rule_set in sorted(bundle["rule_sets"], key=lambda item: item["priority"]):
            cur.execute(
                """
                INSERT INTO skill_system.rule_sets (rule_scope, source_path, priority)
                VALUES (%s, %s, %s)
                ON CONFLICT (rule_scope, source_path) DO UPDATE SET
                  priority = EXCLUDED.priority,
                  updated_at = NOW()
                RETURNING rule_set_id, created_at, updated_at
                """,
                (
                    rule_set["rule_scope"],
                    rule_set["source_path"],
                    rule_set["priority"],
                ),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Failed to upsert rule_set")
            rule_set_id, created_at, updated_at = row
            cur.execute(
                "DELETE FROM skill_system.rule_entries WHERE rule_set_id = %s",
                (rule_set_id,),
            )
            for entry in rule_set["entries"]:
                cur.execute(
                    """
                    INSERT INTO skill_system.rule_entries (
                      rule_set_id, rule_text, rule_hash, enabled
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (
                        rule_set_id,
                        entry["rule_text"],
                        entry["rule_hash"],
                        entry["enabled"],
                    ),
                )
            synced_sets.append(
                {
                    **rule_set,
                    "rule_set_id": int(rule_set_id),
                    "created_at": str(created_at),
                    "updated_at": str(updated_at),
                }
            )
    return {
        "merge_priority": bundle["merge_priority"],
        "rule_sets": synced_sets,
    }


def read_rule_model(conn: Any) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              rs.rule_set_id,
              rs.rule_scope,
              rs.source_path,
              rs.priority,
              rs.created_at,
              rs.updated_at,
              re.rule_id,
              re.rule_text,
              re.rule_hash,
              re.enabled
            FROM skill_system.rule_sets rs
            LEFT JOIN skill_system.rule_entries re
              ON re.rule_set_id = rs.rule_set_id
            ORDER BY rs.priority ASC, rs.rule_scope ASC, rs.source_path ASC, re.rule_text ASC
            """
        )
        rows = cur.fetchall()

    rule_sets: list[dict[str, Any]] = []
    by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        rule_set_id = int(row[0])
        rule_set = by_id.get(rule_set_id)
        if rule_set is None:
            rule_set = {
                "rule_set_id": rule_set_id,
                "rule_scope": row[1],
                "source_path": row[2],
                "priority": int(row[3]),
                "created_at": str(row[4]),
                "updated_at": str(row[5]),
                "entries": [],
            }
            by_id[rule_set_id] = rule_set
            rule_sets.append(rule_set)
        if row[6] is not None:
            rule_set["entries"].append(
                {
                    "rule_id": int(row[6]),
                    "rule_text": row[7],
                    "rule_hash": row[8],
                    "enabled": bool(row[9]),
                }
            )

    return {
        "merge_priority": [
            item["rule_scope"]
            for item in sorted(rule_sets, key=lambda item: item["priority"])
        ],
        "rule_sets": rule_sets,
        "source_paths": [item["source_path"] for item in rule_sets],
    }


def render_rule_projection(rule_set: dict[str, Any]) -> str:
    heading = rule_set["rule_scope"].upper() + "_RULES"
    lines = [
        f"# {heading}",
        "",
        f"- source_of_truth: `skill_system.rule_sets` + `skill_system.rule_entries`",
        f"- rule_scope: `{rule_set['rule_scope']}`",
        f"- source_path: `{rule_set['source_path']}`",
        f"- priority: `{rule_set['priority']}`",
        f"- rule_count: `{len(rule_set.get('entries', []))}`",
        "",
        "## Rules",
        "",
    ]
    entries = rule_set.get("entries", [])
    if not entries:
        lines.append("- (none)")
    else:
        for entry in entries:
            lines.append(f"- {entry['rule_text']}")
    lines.append("")
    return "\n".join(lines)


def write_rule_projection_files(
    rule_model: dict[str, Any],
    repo_root: Path = ROOT_DIR,
    output_dir: Path | None = None,
) -> list[str]:
    target_root = output_dir or repo_root / "note"
    target_root.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    projection_names = {
        "skill_system": "skill_system_rules.md",
        "project": "project_rules.md",
        "compat": "compat_rules.md",
    }
    for rule_set in rule_model["rule_sets"]:
        path = target_root / projection_names[rule_set["rule_scope"]]
        path.write_text(render_rule_projection(rule_set), encoding="utf-8")
        written.append(display_path(path, repo_root))
    return written


def build_rule_model_summary(rule_model: dict[str, Any]) -> dict[str, Any]:
    return {
        "rule_sets": [
            {
                "rule_scope": item["rule_scope"],
                "source_path": item["source_path"],
                "priority": item["priority"],
                "rule_count": len(item.get("entries", [])),
            }
            for item in rule_model["rule_sets"]
        ],
        "merge_priority": rule_model["merge_priority"],
        "source_paths": rule_model.get("source_paths")
        or [item["source_path"] for item in rule_model["rule_sets"]],
    }


def run_sync(
    write_files: bool, repo_root: Path = ROOT_DIR, output_dir: Path | None = None
) -> dict[str, Any]:
    conn, db_target, target_source = connect_rule_db()
    try:
        bundle = build_rule_bundle(repo_root)
        sync_rule_bundle(conn, bundle)
        rule_model = read_rule_model(conn)
        written_files = (
            write_rule_projection_files(rule_model, repo_root, output_dir)
            if write_files
            else []
        )
        conn.commit()
        return {
            "status": "ok",
            "db_target": db_target,
            "target_source": target_source,
            "rule_model": build_rule_model_summary(rule_model),
            "written_files": written_files,
        }
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync canonical rule hierarchy into PostgreSQL and deterministic markdown projections."
    )
    parser.add_argument(
        "--write-files",
        action="store_true",
        help="Write markdown projections into note/ after syncing PostgreSQL state.",
    )
    parser.add_argument(
        "--output-dir",
        help="Optional output directory override for projection files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = run_sync(
        write_files=args.write_files,
        output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
    )
    print(json.dumps(payload, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
