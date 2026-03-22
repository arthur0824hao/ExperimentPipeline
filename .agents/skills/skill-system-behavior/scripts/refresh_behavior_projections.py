#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh behavior_* projection tables from SKILL.spec.yaml files."
    )
    parser.add_argument("--skills-dir", default="./skills", help="Skills directory")
    parser.add_argument(
        "--format", choices=["json", "sql"], default="json", help="Output format"
    )
    return parser.parse_args()


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_json(value) -> str:
    return sql_literal(json.dumps(value, ensure_ascii=False)) + "::jsonb"


def build_projection_bundle(skills_dir: Path) -> dict:
    skills_dir = Path(skills_dir)
    specs = sorted(skills_dir.rglob("SKILL.spec.yaml"))
    bundle = {"sources": [], "nodes": [], "edges": [], "snapshots": []}
    seen_nodes: set[tuple[str, str, str]] = set()

    def add_node(
        skill_id: str, node_type: str, node_key: str, title: str, description: str = ""
    ):
        key = (skill_id, node_type, node_key)
        if key in seen_nodes:
            return
        seen_nodes.add(key)
        bundle["nodes"].append(
            {
                "skill_id": skill_id,
                "node_type": node_type,
                "node_key": node_key,
                "title": title,
                "description": description,
            }
        )

    for spec_path in specs:
        spec = load_yaml(spec_path) or {}
        skill_id = spec.get("skill_name")
        if not isinstance(skill_id, str) or not skill_id.strip():
            continue

        skill_id = skill_id.strip()
        content = spec_path.read_text(encoding="utf-8")
        content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()

        source = {
            "skill_id": skill_id,
            "source_path": str(spec_path),
            "source_kind": "skill-spec",
            "content_hash": content_hash,
            "parser_version": "1.0",
            "status": "active",
        }
        bundle["sources"].append(source)

        add_node(skill_id, "skill", skill_id, skill_id, spec.get("description", ""))

        edges_for_skill = []
        for op in spec.get("operations", []) or []:
            if not isinstance(op, dict):
                continue
            op_name = op.get("name")
            if not isinstance(op_name, str) or not op_name:
                continue
            add_node(skill_id, "operation", op_name, op_name, op.get("intent", ""))
            edges_for_skill.append(
                {
                    "skill_id": skill_id,
                    "to_skill_id": skill_id,
                    "from_node_key": skill_id,
                    "to_node_key": op_name,
                    "edge_type": "implements",
                    "label": None,
                }
            )

        for dep in spec.get("depends_on", []) or []:
            if isinstance(dep, str) and dep.strip():
                dep = dep.strip()
                add_node(dep, "skill", dep, dep, "")
                edges_for_skill.append(
                    {
                        "skill_id": skill_id,
                        "to_skill_id": dep,
                        "from_node_key": skill_id,
                        "to_node_key": dep,
                        "edge_type": "depends_on",
                        "label": None,
                    }
                )

        for delegate in spec.get("delegates_to", []) or []:
            if isinstance(delegate, str) and delegate.strip():
                delegate = delegate.strip()
                add_node(delegate, "skill", delegate, delegate, "")
                edges_for_skill.append(
                    {
                        "skill_id": skill_id,
                        "to_skill_id": delegate,
                        "from_node_key": skill_id,
                        "to_node_key": delegate,
                        "edge_type": "delegates_to",
                        "label": None,
                    }
                )

        bundle["edges"].extend(edges_for_skill)
        bundle["snapshots"].append(
            {
                "skill_id": skill_id,
                "snapshot_tag": content_hash[:12],
                "graph_json": {
                    "skill_id": skill_id,
                    "nodes": [n for n in bundle["nodes"] if n["skill_id"] == skill_id],
                    "edges": edges_for_skill,
                },
                "mermaid": _to_mermaid(skill_id, edges_for_skill),
                "source_hash_set": [content_hash],
            }
        )

    return bundle


def _to_mermaid(skill_id: str, edges: list[dict]) -> str:
    lines = ["flowchart TD", f'  skill["{skill_id}"]']
    seen = set()
    for edge in edges:
        dst = edge["to_node_key"]
        if dst not in seen:
            seen.add(dst)
            lines.append(f'  n_{len(seen)}["{dst}"]')
    for idx, edge in enumerate(edges, start=1):
        connector = "-->" if edge["edge_type"] != "delegates_to" else "-.->"
        lines.append(f"  skill {connector}|{edge['edge_type']}| n_{idx}")
    return "\n".join(lines)


def to_sql(bundle: dict) -> str:
    lines = ["BEGIN;"]
    for source in bundle["sources"]:
        skill_id = source["skill_id"]
        source_path = source["source_path"]
        lines.append(
            f"DELETE FROM behavior_edges WHERE source_id IN (SELECT id FROM behavior_sources WHERE skill_id = {sql_literal(skill_id)} AND source_path = {sql_literal(source_path)});"
        )
        lines.append(
            f"DELETE FROM behavior_nodes WHERE source_id IN (SELECT id FROM behavior_sources WHERE skill_id = {sql_literal(skill_id)} AND source_path = {sql_literal(source_path)});"
        )
        lines.append(
            "INSERT INTO behavior_sources (skill_id, source_path, source_kind, content_hash, parser_version, status, metadata, last_seen_at, last_parsed_at, updated_at) "
            f"VALUES ({sql_literal(skill_id)}, {sql_literal(source_path)}, {sql_literal(source['source_kind'])}, {sql_literal(source['content_hash'])}, {sql_literal(source['parser_version'])}, {sql_literal(source['status'])}, {sql_json({'source': 'refresh_behavior_projections'})}, NOW(), NOW(), NOW()) "
            "ON CONFLICT (skill_id, source_path) DO UPDATE SET "
            "content_hash = EXCLUDED.content_hash, parser_version = EXCLUDED.parser_version, status = EXCLUDED.status, last_seen_at = NOW(), last_parsed_at = NOW(), updated_at = NOW();"
        )

    for node in bundle["nodes"]:
        source = next(
            (s for s in bundle["sources"] if s["skill_id"] == node["skill_id"]), None
        )
        source_expr = (
            f"(SELECT id FROM behavior_sources WHERE skill_id = {sql_literal(source['skill_id'])} AND source_path = {sql_literal(source['source_path'])})"
            if source
            else "NULL"
        )
        lines.append(
            "INSERT INTO behavior_nodes (skill_id, source_id, node_type, node_key, title, description, metadata) "
            f"VALUES ({sql_literal(node['skill_id'])}, {source_expr}, {sql_literal(node['node_type'])}, {sql_literal(node['node_key'])}, {sql_literal(node['title'])}, {sql_literal(node['description'])}, {sql_json({'source': 'refresh_behavior_projections'})}) "
            "ON CONFLICT (skill_id, node_type, node_key) DO UPDATE SET title = EXCLUDED.title, description = EXCLUDED.description, updated_at = NOW();"
        )

    for edge in bundle["edges"]:
        lines.append(
            "INSERT INTO behavior_edges (source_id, from_node_id, to_node_id, edge_type, label, confidence, metadata) VALUES ("
            f"(SELECT id FROM behavior_sources WHERE skill_id = {sql_literal(edge['skill_id'])} ORDER BY id DESC LIMIT 1), "
            f"(SELECT id FROM behavior_nodes WHERE skill_id = {sql_literal(edge['skill_id'])} AND node_key = {sql_literal(edge['from_node_key'])} ORDER BY id DESC LIMIT 1), "
            f"(SELECT id FROM behavior_nodes WHERE skill_id = {sql_literal(edge['to_skill_id'])} AND node_key = {sql_literal(edge['to_node_key'])} ORDER BY id DESC LIMIT 1), "
            f"{sql_literal(edge['edge_type'])}, {sql_literal(edge['label'])}, 1.0, {sql_json({'source': 'refresh_behavior_projections'})});"
        )

    for snapshot in bundle["snapshots"]:
        lines.append(
            "INSERT INTO behavior_snapshots (memory_id, skill_id, snapshot_tag, graph_json, mermaid, source_hash_set) VALUES ("
            "NULL, "
            f"{sql_literal(snapshot['skill_id'])}, {sql_literal(snapshot['snapshot_tag'])}, {sql_json(snapshot['graph_json'])}, {sql_literal(snapshot['mermaid'])}, {sql_json(snapshot['source_hash_set'])}) "
            "ON CONFLICT (skill_id, snapshot_tag) DO UPDATE SET graph_json = EXCLUDED.graph_json, mermaid = EXCLUDED.mermaid, source_hash_set = EXCLUDED.source_hash_set;"
        )

    lines.append("COMMIT;")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    bundle = build_projection_bundle(Path(args.skills_dir))
    if args.format == "sql":
        print(to_sql(bundle))
    else:
        print(json.dumps(bundle, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
