#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[3]
RULE_PROJECTION_PATH = Path(__file__).with_name("rule_projection.py")
ARCHITECTURE_GRAPH_PATH = Path(__file__).with_name("architecture_graph.py")
GENERATED_START = "<!-- GENERATED_START -->"
GENERATED_END = "<!-- GENERATED_END -->"
NOTE_FEEDBACK_PATH = ROOT_DIR / "note" / "note_feedback.md"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def wrap_generated_block(body: str) -> str:
    return f"{GENERATED_START}\n{body.rstrip()}\n{GENERATED_END}\n"


def replace_generated_block(
    existing_text: str, body: str, *, append_if_missing: bool
) -> str:
    block = wrap_generated_block(body)
    if GENERATED_START in existing_text and GENERATED_END in existing_text:
        prefix, remainder = existing_text.split(GENERATED_START, 1)
        _, suffix = remainder.split(GENERATED_END, 1)
        normalized_prefix = prefix.rstrip()
        joiner = "\n\n" if normalized_prefix else ""
        return normalized_prefix + joiner + block + suffix.lstrip("\n")
    if append_if_missing and existing_text.strip():
        return existing_text.rstrip() + "\n\n" + block
    return block


def build_projection_documents(repo_root: Path = ROOT_DIR) -> dict[str, str]:
    rule_mod = load_module(RULE_PROJECTION_PATH, "projection_engine_rules")
    architecture_mod = load_module(
        ARCHITECTURE_GRAPH_PATH, "projection_engine_architecture"
    )
    conn, _, _ = rule_mod.connect_rule_db()
    try:
        rule_model = rule_mod.read_rule_model(conn)
        architecture_graph = architecture_mod.read_architecture_graph(conn)
    finally:
        conn.rollback()
        conn.close()

    documents = {
        "note/skill_system_rules.md": rule_mod.render_rule_projection(
            next(
                item
                for item in rule_model["rule_sets"]
                if item["rule_scope"] == "skill_system"
            )
        ),
        "note/project_rules.md": rule_mod.render_rule_projection(
            next(
                item
                for item in rule_model["rule_sets"]
                if item["rule_scope"] == "project"
            )
        ),
        "note/compat_rules.md": rule_mod.render_rule_projection(
            next(
                item
                for item in rule_model["rule_sets"]
                if item["rule_scope"] == "compat"
            )
        ),
        "note/architecture_map.md": architecture_mod.render_architecture_map(
            architecture_graph
        ),
        "note/note_feedback.md": "\n".join(
            [
                "## Projection Status",
                "",
                "- source_of_truth: `skill_system.rule_sets` + `skill_system.rule_entries` + `skill_system.project_nodes` + `skill_system.project_edges`",
                "- projected_files:",
                "  - note/skill_system_rules.md",
                "  - note/project_rules.md",
                "  - note/compat_rules.md",
                "  - note/architecture_map.md",
                "  - note/note_feedback.md",
                f"- rule_set_count: {len(rule_model['rule_sets'])}",
                f"- architecture_node_count: {len(architecture_graph['nodes'])}",
                f"- architecture_edge_count: {len(architecture_graph['edges'])}",
            ]
        ),
    }
    return documents


def write_projection_documents(
    repo_root: Path = ROOT_DIR, target_paths: dict[str, Path] | None = None
) -> list[str]:
    documents = build_projection_documents(repo_root)
    written: list[str] = []
    for relative_path, body in documents.items():
        path = (target_paths or {}).get(relative_path, repo_root / relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
        updated = replace_generated_block(
            existing,
            body,
            append_if_missing=relative_path == "note/note_feedback.md",
        )
        path.write_text(updated, encoding="utf-8")
        written.append(relative_path)
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write deterministic PostgreSQL-backed markdown projections using managed blocks."
    )
    return parser.parse_args()


def main() -> int:
    parse_args()
    written = write_projection_documents()
    print(json.dumps({"status": "ok", "written_files": written}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
